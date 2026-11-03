//! `bitcoin-gateway`: the binary. CLI, config load, listeners, signals.
use bitcoin::hex::DisplayHex;
use bitcoin_core_rpc::{Auth, Client};
use bitcoin_core_zmq::{Topic, ZmqEvents};
use bitcoin_gateway::config::{Config, ConfigError};
use bitcoin_gateway::node::{Events, Node, Upstream};
use bitcoin_gateway::server::{Tls, check_listen, serve_with};
use bitcoin_gateway::{Gateway, Settings, auth};
use clap::{Parser, Subcommand};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "gRPC, REST and WebSocket in front of Bitcoin Core")]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
    /// The TOML config.
    #[arg(short, long, default_value = "gateway.toml")]
    config: PathBuf,
    /// gRPC listener; overrides `[listen] grpc`.
    #[arg(long)]
    grpc: Option<SocketAddr>,
    /// HTTP listener (REST under /v1, WebSocket at /ws); overrides `[listen] http`.
    #[arg(long)]
    http: Option<SocketAddr>,
    /// Metrics and health listener; overrides `[listen] metrics`.
    #[arg(long)]
    metrics: Option<SocketAddr>,
    /// Allow non-loopback listeners without `[tls]` (a proxy terminates TLS).
    #[arg(long)]
    insecure: bool,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generates a token and prints it with the hash the config stores.
    HashToken,
    /// Checks the config and exits.
    CheckConfig,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Some(Cmd::HashToken) => {
            let token = auth::new_token();
            let hash = auth::hash_token(&token).to_lower_hex_string();
            println!("token        = \"{token}\"  # give to the client; not stored anywhere");
            println!("token_sha256 = \"{hash}\"  # put in the [[key]]");
            return ExitCode::SUCCESS;
        }
        Some(Cmd::CheckConfig) => {
            return match Config::load(&cli.config) {
                Ok(_) => {
                    println!("{}: ok", cli.config.display());
                    ExitCode::SUCCESS
                }
                Err(e) => config_error(&cli.config, &e),
            };
        }
        None => {}
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    match rt.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bitcoin-gateway: {e}");
            ExitCode::FAILURE
        }
    }
}

fn config_error(path: &Path, e: &ConfigError) -> ExitCode {
    match e.line {
        Some(l) => eprintln!("{}:{l}: {}", path.display(), e.message),
        None => eprintln!("{}: {}", path.display(), e.message),
    }
    ExitCode::from(2)
}

async fn run(cli: Cli) -> Result<(), String> {
    let src = std::fs::read_to_string(&cli.config)
        .map_err(|e| format!("can't read {}: {e}", cli.config.display()))?;
    let config = Config::parse(&src).map_err(|e| located(&cli.config, &e))?;
    let listen = [
        ("grpc", cli.grpc.or(config.listen.grpc)),
        ("http", cli.http.or(config.listen.http)),
        ("metrics", cli.metrics.or(config.listen.metrics)),
    ];
    if listen.iter().all(|(_, a)| a.is_none()) {
        return Err("no listeners: set [listen] or --grpc/--http/--metrics".into());
    }
    for (name, addr) in &listen {
        if let Some(a) = addr {
            check_listen(name, *a, config.tls.is_some(), cli.insecure)?;
        }
    }
    let tls = config.tls.as_ref().map(Tls::load).transpose()?;

    let shutdown = CancellationToken::new();
    let mut term = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut int = signal(SignalKind::interrupt()).map_err(|e| e.to_string())?;
    let mut hup = signal(SignalKind::hangup()).map_err(|e| e.to_string())?;
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
            tracing::info!("shutting down");
            shutdown.cancel();
        });
    }

    let n = &config.node;
    let auth = match (&n.cookie, &n.rpc_user, &n.rpc_password) {
        (Some(c), _, _) => Auth::Cookie(c.clone()),
        (None, Some(u), Some(p)) => Auth::UserPass(u.get_ref().clone(), p.clone()),
        _ => unreachable!("Config::parse checked [node]"),
    };
    let builder = || Client::builder(n.rpc_url.get_ref()).auth(auth.clone());
    let Some(client) = retry("node", &shutdown, || builder().connect()).await else {
        return Ok(());
    };
    tracing::info!(version = client.version().0, "connected to Core");
    let events = match &n.zmq {
        Some(z) => {
            let z = z.get_ref();
            let Some(zmq) =
                retry("zmq", &shutdown, || ZmqEvents::connect(z, Topic::Sequence)).await
            else {
                return Ok(());
            };
            Events::Zmq(zmq)
        }
        None => {
            tracing::warn!(
                "no [node] zmq: events poll waitfornewblock (block events only, one rpcthread)"
            );
            Events::Rpc(client.clone())
        }
    };
    check_wallets(&client, &config, &src)
        .await
        .map_err(|e| located(&cli.config, &e))?;

    let node = Node {
        rpc: client.clone(),
        events,
    };
    let settings = Settings {
        http: config.http.clone(),
        grpc: config.grpc.clone(),
        ws: config.ws.clone(),
        events: config.events.clone(),
        limits: config.limits.clone(),
    };
    let keys = config
        .key_specs(&src)
        .map_err(|e| located(&cli.config, &e))?;
    let gw = Gateway::new(
        node,
        Arc::new(client.clone()) as Arc<dyn Upstream>,
        keys,
        settings,
        shutdown.clone(),
    );

    let mut tasks = Vec::new();
    for (name, addr) in listen {
        let Some(addr) = addr else { continue };
        let app = match name {
            "grpc" => gw.grpc_router(),
            "http" => gw.http_router(),
            _ => gw.metrics_router(),
        };
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| format!("can't listen on {addr} ({name}): {e}"))?;
        tracing::info!(%addr, name, tls = tls.is_some(), "listening");
        tasks.push(tokio::spawn(serve_with(
            listener,
            app,
            tls.clone(),
            shutdown.clone(),
            gw.serve_limits(),
        )));
    }

    // SIGHUP: reload [[key]] and the certificate; listeners need a restart.
    {
        let (gw, path, client, tls) = (gw.clone(), cli.config.clone(), client, tls.clone());
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = hup.recv() => {}
                    _ = shutdown.cancelled() => return,
                }
                match reload_keys(&path, &client).await {
                    Ok(keys) => {
                        tracing::info!(keys = keys.len(), "reloaded keys");
                        gw.keys.replace(keys);
                    }
                    Err(e) => tracing::error!("reload failed, keeping the old keys: {e}"),
                }
                if let Some(t) = &tls {
                    match t.reload() {
                        Ok(()) => tracing::info!("reloaded the TLS certificate"),
                        Err(e) => tracing::error!("TLS reload failed, keeping the old one: {e}"),
                    }
                }
            }
        });
    }
    for t in tasks {
        let _ = t.await;
    }
    Ok(())
}

fn located(path: &Path, e: &ConfigError) -> String {
    match e.line {
        Some(l) => format!("{}:{l}: {}", path.display(), e.message),
        None => format!("{}: {}", path.display(), e.message),
    }
}

/// Retries `f` with backoff until it succeeds or shutdown; `None` on shutdown.
async fn retry<T, F, Fut>(what: &str, shutdown: &CancellationToken, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bitcoin_node_api::ApiResult<T>>,
{
    let mut wait = Duration::from_millis(500);
    loop {
        match f().await {
            Ok(v) => return Some(v),
            Err(e) => tracing::warn!(error = %e, retry_in = ?wait, "can't reach {what}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.cancelled() => return None,
        }
        wait = (wait * 2).min(Duration::from_secs(30));
    }
}

/// Fails if a wallet scope names a wallet that isn't in Core's wallet dir.
async fn check_wallets(client: &Client, config: &Config, src: &str) -> Result<(), ConfigError> {
    let scopes = config.wallet_scopes(src);
    if scopes.is_empty() {
        return Ok(());
    }
    let names: HashSet<String> =
        match Upstream::call(client, None, "listwalletdir", serde_json::json!([])).await {
            Ok(v) => v["wallets"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|w| w["name"].as_str().map(str::to_owned))
                .collect(),
            Err(e) => {
                return Err(ConfigError {
                    line: scopes[0].1.line,
                    message: format!("wallet scopes, but listwalletdir failed: {e}"),
                });
            }
        };
    match scopes.into_iter().find(|(w, _)| !names.contains(w)) {
        Some((_, e)) => Err(e),
        None => Ok(()),
    }
}

async fn reload_keys(
    path: &Path,
    client: &Client,
) -> Result<Vec<bitcoin_gateway::config::KeySpec>, String> {
    let src = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let config = Config::parse(&src).map_err(|e| located(path, &e))?;
    check_wallets(client, &config, &src)
        .await
        .map_err(|e| located(path, &e))?;
    config.key_specs(&src).map_err(|e| located(path, &e))
}
