//! I4: the loopback rule, graceful shutdown, and the binary's startup.
mod common;

use axum::routing::get;
use bitcoin_gateway::server::serve;
use common::FakeCore;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

const EXE: &str = env!("CARGO_BIN_EXE_bitcoin-gateway");

#[tokio::test]
async fn shutdown_drains_calls_in_flight() {
    let app = axum::Router::new().route(
        "/slow",
        get(|| async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            "done"
        }),
    );
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let token = CancellationToken::new();
    let server = tokio::spawn(serve(l, app, None, token.clone()));

    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.write_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();

    // the call in flight finishes
    let mut buf = Vec::new();
    conn.read_to_end(&mut buf).await.unwrap();
    let reply = String::from_utf8_lossy(&buf);
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    assert!(reply.ends_with("done"), "{reply}");
    // then the server stops, and new connections are refused
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert!(TcpStream::connect(addr).await.is_err());
}

fn write_config(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let p = dir.join("gateway.toml");
    std::fs::write(&p, body).unwrap();
    p
}

fn tmpdir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("bitcoin-gateway-{name}-{}", std::process::id()))
}

#[test]
fn non_loopback_without_tls_refuses_to_start() {
    let p = write_config(
        &tmpdir("noloop"),
        "[node]\nrpc_url = \"http://127.0.0.1:1\"\nrpc_user = \"u\"\nrpc_password = \"p\"\n\
         [listen]\nhttp = \"0.0.0.0:0\"\n",
    );
    let out = Command::new(EXE).arg("-c").arg(&p).output().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("without TLS"), "{err}");

    // --insecure lets it past the check (it then waits for Core: kill it)
    let mut child = Command::new(EXE)
        .arg("-c")
        .arg(&p)
        .arg("--insecure")
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert!(child.try_wait().unwrap().is_none(), "exited early");
    child.kill().unwrap();
    let _ = child.wait();
}

#[test]
fn bad_config_names_the_line() {
    let p = write_config(
        &tmpdir("badline"),
        "[node]\nrpc_url = \"http://127.0.0.1:1\"\ncookie = \"/c\"\n\n[[key]]\nname = \"k\"\n\
         token_sha256 = \"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08\"\n\
         allow = [\"chain:raed\"]\n",
    );
    let out = Command::new(EXE)
        .arg("-c")
        .arg(&p)
        .arg("check-config")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("gateway.toml:8: unknown scope `chain:raed`"),
        "{err}"
    );
}

#[test]
fn hash_token_prints_a_matching_pair() {
    let out = Command::new(EXE).arg("hash-token").output().unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    let field = |name: &str| {
        s.lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split('"').nth(1))
            .unwrap()
            .to_owned()
    };
    let (token, hash) = (field("token "), field("token_sha256"));
    use bitcoin::hex::DisplayHex;
    assert_eq!(
        bitcoin_gateway::auth::hash_token(&token).to_lower_hex_string(),
        hash
    );
}

#[tokio::test]
async fn sigterm_exits_cleanly() {
    let core = FakeCore::start(|_, _, _| Ok(Value::Null)).await;
    let p = write_config(
        &tmpdir("sigterm"),
        &format!(
            "[node]\nrpc_url = \"{}\"\nrpc_user = \"u\"\nrpc_password = \"p\"\n\
             [listen]\nhttp = \"127.0.0.1:0\"\nmetrics = \"127.0.0.1:0\"\n",
            core.url
        ),
    );
    let mut child = Command::new(EXE)
        .arg("-c")
        .arg(&p)
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // wait for both listeners (logs go to stdout)
    let stdout = child.stdout.take().unwrap();
    let ready = tokio::task::spawn_blocking(move || {
        let mut seen = 0;
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("listening") {
                seen += 1;
                if seen == 2 {
                    return true;
                }
            }
        }
        false
    });
    assert!(
        tokio::time::timeout(Duration::from_secs(20), ready)
            .await
            .unwrap()
            .unwrap()
    );
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(status.success());
    let done = tokio::task::spawn_blocking(move || child.wait().unwrap());
    let status = tokio::time::timeout(Duration::from_secs(15), done)
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "{status}");
}

mod tls {
    use super::*;
    use bitcoin_gateway::config::TlsConfig;
    use bitcoin_gateway::server::Tls;
    use rustls::pki_types::{CertificateDer, ServerName};
    use std::sync::Arc;
    use tokio_rustls::TlsConnector;

    /// A self-signed cert for `localhost`, written to `dir`; returns it in DER.
    fn self_signed(dir: &std::path::Path) -> CertificateDer<'static> {
        let c = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("cert.pem"), c.cert.pem()).unwrap();
        std::fs::write(dir.join("key.pem"), c.signing_key.serialize_pem()).unwrap();
        c.cert.der().clone()
    }

    async fn get_healthz(
        addr: std::net::SocketAddr,
        root: &CertificateDer<'static>,
    ) -> std::io::Result<String> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(root.clone()).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tcp = TcpStream::connect(addr).await?;
        let name = ServerName::try_from("localhost").unwrap();
        let mut s = TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await?;
        s.write_all(b"GET /healthz HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
            .await?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    #[tokio::test]
    async fn serves_tls_and_reloads_the_certificate() {
        let dir = tmpdir("tls");
        let first = self_signed(&dir);
        let tls = Tls::load(&TlsConfig {
            cert: dir.join("cert.pem"),
            key: dir.join("key.pem"),
        })
        .unwrap();
        let app = axum::Router::new().route("/healthz", get(|| async { "ok" }));
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let token = CancellationToken::new();
        tokio::spawn(serve(l, app, Some(tls.clone()), token.clone()));

        let reply = get_healthz(addr, &first).await.unwrap();
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");

        // SIGHUP: a new certificate on disk is picked up by new connections
        let second = self_signed(&dir);
        tls.reload().unwrap();
        assert!(get_healthz(addr, &first).await.is_err());
        let reply = get_healthz(addr, &second).await.unwrap();
        assert!(reply.ends_with("ok"), "{reply}");

        // a broken file keeps the old certificate
        std::fs::write(dir.join("cert.pem"), "garbage").unwrap();
        assert!(tls.reload().is_err());
        assert!(get_healthz(addr, &second).await.is_ok());
        token.cancel();
    }

    #[tokio::test]
    async fn a_stalled_tls_handshake_is_closed() {
        use bitcoin_gateway::server::{ServeLimits, serve_with};
        let dir = tmpdir("tls-stall");
        self_signed(&dir);
        let tls = Tls::load(&TlsConfig {
            cert: dir.join("cert.pem"),
            key: dir.join("key.pem"),
        })
        .unwrap();
        let app = axum::Router::new().route("/healthz", get(|| async { "ok" }));
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let token = CancellationToken::new();
        let limits = ServeLimits {
            handshake_timeout: Duration::from_millis(300),
            ..ServeLimits::default()
        };
        tokio::spawn(serve_with(l, app, Some(tls), token.clone(), limits));

        // The start of a ClientHello, then nothing.
        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.write_all(&[0x16, 0x03, 0x01]).await.unwrap();
        let start = std::time::Instant::now();
        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), conn.read_to_end(&mut buf))
            .await
            .expect("closed")
            .ok();
        assert!(start.elapsed() >= Duration::from_millis(250));
        token.cancel();
    }
}
