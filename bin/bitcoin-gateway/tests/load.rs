//! Load: the gateway binary in front of a regtest `bitcoind`, under
//! concurrent clients. `#[ignore]`; numbers mean something only in release:
//!
//! ```sh
//! BITCOIND_EXE=$(scripts/fetch-bitcoind.sh 31.2) \
//!   cargo test --release -p bitcoin-gateway --test load -- --ignored --nocapture
//! ```
//!
//! `LOAD_SECS` sets each timed phase (default 10). It prints throughput,
//! latency and the gateway's memory, and checks what must hold under load:
//! no 5xx, the per-key and connection limits answer as configured, memory
//! doesn't grow between two identical rounds, every subscriber gets every
//! block.
use bitcoin::hex::DisplayHex;
use bitcoin_core_rpc::{Auth, Client, CreateWalletOptions};
use bitcoin_gateway::auth::hash_token;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use test_node::{RPC_PASSWORD, RPC_USER, TestNode, free_port};
use tokio::net::TcpSocket;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const EXE: &str = env!("CARGO_BIN_EXE_bitcoin-gateway");

fn secs() -> Duration {
    Duration::from_secs(
        std::env::var("LOAD_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10),
    )
}

/// The gateway process; killed on drop.
struct Gateway {
    child: Child,
    http: SocketAddr,
    metrics: SocketAddr,
}

impl Gateway {
    fn rss_mib(&self) -> f64 {
        let s = std::fs::read_to_string(format!("/proc/{}/status", self.child.id())).unwrap();
        let kb: f64 = s
            .lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))
            .and_then(|v| v.split_whitespace().next())
            .and_then(|v| v.parse().ok())
            .unwrap();
        kb / 1024.0
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.http)
    }

    async fn metric(&self, name: &str) -> f64 {
        let text = reqwest::get(format!("http://{}/metrics", self.metrics))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(' '))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0)
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Outcomes of one timed phase.
#[derive(Default)]
struct Tally {
    statuses: BTreeMap<u16, u64>,
    latencies: Vec<Duration>,
}

impl Tally {
    fn total(&self) -> u64 {
        self.statuses.values().sum()
    }

    fn count(&self, pred: impl Fn(u16) -> bool) -> u64 {
        self.statuses
            .iter()
            .filter(|(s, _)| pred(**s))
            .map(|(_, n)| n)
            .sum()
    }

    fn pct(&mut self, p: f64) -> Duration {
        self.latencies.sort();
        let i = ((self.latencies.len() as f64 - 1.0) * p).round() as usize;
        self.latencies.get(i).copied().unwrap_or_default()
    }

    fn report(&mut self, name: &str, took: Duration) {
        let rps = self.total() as f64 / took.as_secs_f64();
        let (p50, p99) = (self.pct(0.50), self.pct(0.99));
        println!(
            "{name:<28} {:>8} req  {rps:>8.0} req/s  p50 {:>7.2} ms  p99 {:>7.2} ms  {:?}",
            self.total(),
            p50.as_secs_f64() * 1e3,
            p99.as_secs_f64() * 1e3,
            self.statuses
        );
    }
}

/// `clients` tasks, each a keep-alive connection, sending `next(i)` GETs
/// with `token` until the phase ends.
async fn hammer(
    gw: &Gateway,
    clients: usize,
    token: &str,
    paths: Arc<Vec<String>>,
) -> (Tally, Duration) {
    let tally = Arc::new(Mutex::new(Tally::default()));
    let deadline = Instant::now() + secs();
    let start = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..clients {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(1)
            .build()
            .unwrap();
        let (tally, paths, auth) = (tally.clone(), paths.clone(), format!("Bearer {token}"));
        let base = gw.url("");
        tasks.push(tokio::spawn(async move {
            let mut i = c;
            let mut local = Tally::default();
            while Instant::now() < deadline {
                let path = &paths[i % paths.len()];
                i += clients;
                let t = Instant::now();
                let status = match client
                    .get(format!("{base}{path}"))
                    .header("authorization", &auth)
                    .send()
                    .await
                {
                    Ok(r) => {
                        let s = r.status().as_u16();
                        let _ = r.bytes().await;
                        s
                    }
                    Err(_) => 0, // connection error
                };
                local.latencies.push(t.elapsed());
                *local.statuses.entry(status).or_default() += 1;
            }
            let mut t = tally.lock().unwrap();
            t.latencies.extend(local.latencies);
            for (s, n) in local.statuses {
                *t.statuses.entry(s).or_default() += n;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let took = start.elapsed();
    let t = std::mem::take(&mut *tally.lock().unwrap());
    (t, took)
}

/// A TCP connection to `to` from loopback address `127.0.0.<from>`.
async fn connect_from(from: u8, to: SocketAddr) -> tokio::net::TcpStream {
    let s = TcpSocket::new_v4().unwrap();
    s.bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, from)),
        0,
    ))
    .unwrap();
    s.connect(to).await.unwrap()
}

/// The gateway closed it (a refused connection is closed right after
/// accept, so a read sees EOF or a reset).
async fn closed(conn: &tokio::net::TcpStream) -> bool {
    let mut b = [0u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_millis(20), conn.peek(&mut b)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs bitcoind; run with --release"]
async fn gateway_under_load() {
    // Core, with a wallet, 110 blocks and a mempool.
    let zmq = format!("tcp://127.0.0.1:{}", free_port());
    let node = TestNode::start(&[&format!("-zmqpubsequence={zmq}"), "-fallbackfee=0.0001"]);
    let auth = Auth::UserPass(RPC_USER.into(), RPC_PASSWORD.into());
    let mut core = None;
    for _ in 0..100 {
        if let Ok(c) = Client::connect(&node.rpc_url(), auth.clone()).await {
            core = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let core = core.unwrap_or_else(|| panic!("node never answered:\n{}", node.log_tail(30)));
    let w = core
        .create_wallet("w", CreateWalletOptions::default())
        .await
        .unwrap()
        .wallet;
    let addr = w
        .get_new_address(None, None)
        .await
        .unwrap()
        .assume_checked()
        .to_string();
    let mine = |n: u32| {
        let core = core.clone();
        let addr = addr.clone();
        async move {
            let v: Vec<String> = core
                .raw_call("generatetoaddress", json!([n, addr]))
                .await
                .unwrap();
            v
        }
    };
    let hashes = mine(110).await;
    let mut txids = Vec::new();
    for _ in 0..20 {
        let t: String = w
            .raw_call("sendtoaddress", json!([addr, 0.01]))
            .await
            .unwrap();
        txids.push(t);
    }

    // The gateway binary.
    let (http, metrics) = (
        SocketAddr::from(([127, 0, 0, 1], free_port())),
        SocketAddr::from(([127, 0, 0, 1], free_port())),
    );
    let dir = std::env::temp_dir().join(format!("bitcoin-gateway-load-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let h = |t: &str| hash_token(t).to_lower_hex_string();
    let config = format!(
        r#"
[node]
rpc_url = "{}"
rpc_user = "{RPC_USER}"
rpc_password = "{RPC_PASSWORD}"
zmq = "{zmq}"

[listen]
http = "{http}"
metrics = "{metrics}"

[limits]
max_connections = 256
max_connections_per_ip = 64

[[key]]
name = "load"
token_sha256 = "{}"
allow = ["chain:read", "mempool:read", "fees:read", "events"]
max_in_flight = 128

[[key]]
name = "small"
token_sha256 = "{}"
allow = ["chain:read"]
max_in_flight = 4

[[key]]
name = "ws"
token_sha256 = "{}"
allow = ["chain:read", "mempool:read"]
"#,
        node.rpc_url(),
        h("load-token"),
        h("small-token"),
        h("ws-token"),
    );
    let path = dir.join("gateway.toml");
    std::fs::write(&path, config).unwrap();
    let gw = Gateway {
        child: Command::new(EXE)
            .arg("-c")
            .arg(&path)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
        http,
        metrics,
    };
    let mut ready = false;
    for _ in 0..100 {
        if let Ok(r) = reqwest::get(format!("http://{metrics}/readyz")).await
            && r.status() == 200
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "gateway never became ready");
    println!(
        "\nphase                             total        rate          latency          statuses"
    );
    println!("{:<28} rss {:.1} MiB", "idle", gw.rss_mib());

    // A: mixed reads, twice; memory must not grow between the rounds.
    let mut mix = vec!["/v1/chain/tip".to_owned(), "/v1/mempool/txids".to_owned()];
    for (i, b) in hashes.iter().enumerate() {
        mix.push(format!("/v1/blocks/{b}"));
        mix.push(format!("/v1/blocks/{b}/header"));
        mix.push(format!("/v1/chain/heights/{}", i + 1));
    }
    for t in &txids {
        mix.push(format!("/v1/mempool/{t}"));
    }
    mix.push("/v1/fees/estimate?blocks=6".to_owned());
    let mix = Arc::new(mix);
    let mut rss = Vec::new();
    for round in 1..=2 {
        let (mut t, took) = hammer(&gw, 64, "load-token", mix.clone()).await;
        t.report(&format!("A{round}: reads x64"), took);
        assert_eq!(t.count(|s| s >= 500 || s == 0), 0, "errors under load");
        assert_eq!(t.count(|s| s == 429), 0, "under the key's limit");
        rss.push(gw.rss_mib());
        println!("{:<28} rss {:.1} MiB", "", rss[round - 1]);
    }
    assert!(
        rss[1] <= rss[0] * 1.25 + 8.0,
        "memory grew between identical rounds: {rss:?} MiB"
    );

    // B: a key with max_in_flight = 4 under 32 clients: 429s, never 5xx.
    let blocks = Arc::new(hashes.iter().map(|b| format!("/v1/blocks/{b}")).collect());
    let (mut t, took) = hammer(&gw, 32, "small-token", blocks).await;
    t.report("B: key limit 4, x32", took);
    assert!(t.count(|s| s == 429) > 0, "the key's limit never applied");
    assert!(t.count(|s| s == 200) > 0, "nothing got through");
    assert_eq!(t.count(|s| s >= 500 || s == 0), 0);

    // C: connection flood from other loopback addresses, opened at once
    // (well inside handshake_timeout, which closes silent ones after 10 s).
    let before = gw.metric("gateway_connections_rejected_total").await;
    let mut one_ip = Vec::new();
    for _ in 0..100 {
        one_ip.push(connect_from(2, http).await);
    }
    let mut many_ips = Vec::new();
    for ip in 3..=10u8 {
        for _ in 0..64 {
            many_ips.push(connect_from(ip, http).await);
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut refused_per_ip = 0;
    for c in &one_ip {
        refused_per_ip += closed(c).await as usize;
    }
    let mut refused_total = 0;
    for c in &many_ips {
        refused_total += closed(c).await as usize;
    }
    let open = one_ip.len() + many_ips.len() - refused_per_ip - refused_total;
    // A new client now: over max_connections too, so it is refused.
    let during = connect_from(1, http).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let refused_during = closed(&during).await;
    let rejected = gw.metric("gateway_connections_rejected_total").await - before;
    println!(
        "{:<28} 100 from one IP: {refused_per_ip} refused; 512 from 8 more: {refused_total} refused; {open} open; metric +{rejected}",
        "C: connection flood",
    );
    assert_eq!(refused_per_ip, 36, "max_connections_per_ip = 64");
    assert!(open <= 256, "max_connections = 256, {open} open");
    assert!(refused_total > 0, "max_connections never applied");
    assert!(refused_during, "a new client while full");
    assert!(rejected >= (refused_per_ip + refused_total + 1) as f64);
    let held = (one_ip, many_ips, during);
    drop(held);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let s = reqwest::Client::new()
        .get(gw.url("/v1/chain/tip"))
        .header("authorization", "Bearer load-token")
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(s, 200, "served again once the flood ends");
    println!("{:<28} rss {:.1} MiB", "", gw.rss_mib());

    // D: 16 SSE subscribers each get every block; a 17th is refused.
    let client = reqwest::Client::new();
    let mut subs = Vec::new();
    for _ in 0..16 {
        let r = client
            .get(gw.url("/v1/events"))
            .header("authorization", "Bearer load-token")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        subs.push(r);
    }
    let extra = client
        .get(gw.url("/v1/events"))
        .header("authorization", "Bearer load-token")
        .send()
        .await
        .unwrap();
    assert_eq!(extra.status(), 429, "max_subscriptions");
    drop(extra);
    let start = Instant::now();
    let mined = mine(5).await;
    let mut readers = Vec::new();
    for r in subs {
        let mined = mined.clone();
        readers.push(tokio::spawn(async move {
            let mut body = r.bytes_stream();
            let mut buf = String::new();
            let mut seen = 0;
            while seen < mined.len() {
                let chunk = tokio::time::timeout(Duration::from_secs(20), body.next())
                    .await
                    .expect("events in time")
                    .unwrap()
                    .unwrap();
                buf.push_str(&String::from_utf8_lossy(&chunk));
                seen = mined.iter().filter(|h| buf.contains(h.as_str())).count();
            }
        }));
    }
    for r in readers {
        r.await.unwrap();
    }
    println!(
        "{:<28} 16 subscribers got all 5 blocks in {:.0} ms; the 17th: 429",
        "D: SSE fan-out",
        start.elapsed().as_secs_f64() * 1e3
    );

    // E: WebSocket pass-through, 16 connections.
    let deadline = Instant::now() + secs();
    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let url = format!("ws://{http}/ws");
        tasks.push(tokio::spawn(async move {
            let mut req = url.into_client_request().unwrap();
            req.headers_mut()
                .insert("authorization", "Bearer ws-token".parse().unwrap());
            let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
            let (mut ok, mut errors, mut lat) = (0u64, 0u64, Vec::new());
            let mut id = 0;
            while Instant::now() < deadline {
                id += 1;
                let t = Instant::now();
                let call =
                    json!({"jsonrpc": "2.0", "id": id, "method": "getblockcount", "params": []});
                ws.send(Message::text(call.to_string())).await.unwrap();
                let reply = loop {
                    match ws.next().await.unwrap().unwrap() {
                        Message::Text(t) => break serde_json::from_str::<Value>(&t).unwrap(),
                        _ => continue,
                    }
                };
                lat.push(t.elapsed());
                if reply.get("result").is_some() {
                    ok += 1;
                } else {
                    errors += 1;
                }
            }
            (ok, errors, lat)
        }));
    }
    let mut t = Tally::default();
    let mut errors = 0;
    for task in tasks {
        let (ok, e, lat) = task.await.unwrap();
        *t.statuses.entry(200).or_default() += ok;
        errors += e;
        t.latencies.extend(lat);
    }
    t.report("E: WS getblockcount x16", start.elapsed());
    assert_eq!(errors, 0, "pass-through errors under load");

    let in_flight = gw.metric("gateway_node_calls_in_flight").await;
    println!(
        "{:<28} rss {:.1} MiB; node calls in flight now {in_flight}",
        "end",
        gw.rss_mib()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
