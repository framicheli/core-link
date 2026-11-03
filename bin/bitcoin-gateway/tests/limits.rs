//! `[limits]`: connections per listener and per IP, slow or silent clients,
//! calls in flight per key and in total, subscriptions per key.
mod common;

use axum::routing::get;
use bitcoin_gateway::Settings;
use bitcoin_gateway::config::{KeySpec, LimitsConfig};
use bitcoin_gateway::server::{ServeLimits, serve_with};
use common::{FakeCore, TestGateway, keys};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// A plain listener with `limits`, serving `/ok`.
async fn listener(limits: ServeLimits) -> (std::net::SocketAddr, CancellationToken) {
    let app = axum::Router::new().route("/ok", get(|| async { "ok" }));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let token = CancellationToken::new();
    tokio::spawn(serve_with(l, app, None, token.clone(), limits));
    (addr, token)
}

fn limits(max: usize, per_ip: usize, timeout_ms: u64) -> ServeLimits {
    ServeLimits {
        max_connections: max,
        max_connections_per_ip: per_ip,
        handshake_timeout: Duration::from_millis(timeout_ms),
        ..ServeLimits::default()
    }
}

/// Reads until the server closes; how long that took. Panics after 5 s.
async fn closed_after(conn: &mut TcpStream) -> Duration {
    let start = Instant::now();
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), conn.read_to_end(&mut buf))
        .await
        .expect("the server should close the connection")
        .ok();
    start.elapsed()
}

/// `GET /ok` on `conn`, keep-alive; the reply's status line.
async fn get_ok(conn: &mut TcpStream) -> String {
    conn.write_all(b"GET /ok HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 512];
    let n = conn.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .next()
        .unwrap_or("")
        .to_owned()
}

#[tokio::test]
async fn connections_over_the_limit_are_closed() {
    let l = limits(2, 64, 5_000);
    let rejected = l.rejected.clone();
    let (addr, _t) = listener(l).await;
    let mut a = TcpStream::connect(addr).await.unwrap();
    let mut b = TcpStream::connect(addr).await.unwrap();
    assert!(get_ok(&mut a).await.contains("200"));
    assert!(get_ok(&mut b).await.contains("200"));

    let mut c = TcpStream::connect(addr).await.unwrap();
    assert!(closed_after(&mut c).await < Duration::from_secs(1));
    assert_eq!(rejected.load(Ordering::Relaxed), 1);

    // A closed connection frees its place.
    drop(a);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut d = TcpStream::connect(addr).await.unwrap();
    assert!(get_ok(&mut d).await.contains("200"));
}

#[tokio::test]
async fn connections_per_ip() {
    let l = limits(100, 1, 5_000);
    let rejected = l.rejected.clone();
    let (addr, _t) = listener(l).await;
    let mut a = TcpStream::connect(addr).await.unwrap();
    assert!(get_ok(&mut a).await.contains("200"));
    let mut b = TcpStream::connect(addr).await.unwrap();
    assert!(closed_after(&mut b).await < Duration::from_secs(1));
    assert_eq!(rejected.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn silent_slow_and_idle_clients_are_closed() {
    let (addr, _t) = listener(limits(100, 100, 300)).await;

    // Connects and sends nothing.
    let mut silent = TcpStream::connect(addr).await.unwrap();
    let t = closed_after(&mut silent).await;
    assert!(t >= Duration::from_millis(250), "{t:?}");

    // Sends half a request, then stalls.
    let mut slow = TcpStream::connect(addr).await.unwrap();
    slow.write_all(b"GET /ok HTTP/1.1\r\nhost").await.unwrap();
    let t = closed_after(&mut slow).await;
    assert!(t >= Duration::from_millis(250), "{t:?}");

    // One request, then idle on keep-alive.
    let mut idle = TcpStream::connect(addr).await.unwrap();
    assert!(get_ok(&mut idle).await.contains("200"));
    let t = closed_after(&mut idle).await;
    assert!(t >= Duration::from_millis(250), "{t:?}");

    // A client that keeps asking is served.
    let mut busy = TcpStream::connect(addr).await.unwrap();
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(get_ok(&mut busy).await.contains("200"));
    }
}

fn settings(l: LimitsConfig) -> Settings {
    Settings {
        limits: l,
        ..Settings::default()
    }
}

/// A gateway with `l`, its keys' limits from `l` as `Config` would set them.
async fn gateway(l: LimitsConfig) -> TestGateway {
    let core = FakeCore::start(|_, _, _| Ok(Value::Null)).await;
    let keys = keys()
        .into_iter()
        .map(|k| KeySpec {
            max_in_flight: l.key_max_in_flight,
            max_subscriptions: l.max_subscriptions,
            ..k
        })
        .collect();
    TestGateway::with(keys, settings(l), core).await
}

async fn rest_tip(gw: &TestGateway, token: &str) -> (u16, Value) {
    let r = reqwest::Client::new()
        .get(format!("{}/v1/chain/tip", gw.http_url()))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap())
}

#[tokio::test]
async fn calls_in_flight_per_key_and_per_node() {
    let gw = gateway(LimitsConfig {
        key_max_in_flight: 1,
        node_max_in_flight: 1,
        node_queue_secs: 1,
        ..LimitsConfig::default()
    })
    .await;
    let reader = gw
        .gw
        .keys
        .authenticate(Some("Bearer reader-token"))
        .unwrap();
    let root = gw.gw.keys.authenticate(Some("Bearer root-token")).unwrap();

    // The key's one slot is taken: its next call is rate limited at once.
    let held = gw.gw.call_slots(&reader).await.unwrap();
    let (s, v) = rest_tip(&gw, "reader-token").await;
    assert_eq!(s, 429, "{v}");
    assert_eq!(v["error"]["code"], "rate_limited");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("in flight")
    );

    // The node's one slot is taken too: another key waits, then gets 503.
    let start = Instant::now();
    let (s, v) = rest_tip(&gw, "root-token").await;
    assert_eq!(s, 503, "{v}");
    assert!(start.elapsed() >= Duration::from_millis(900));

    // Readiness doesn't need a slot.
    let ready = reqwest::get(format!("http://{}/readyz", gw.metrics))
        .await
        .unwrap();
    assert_eq!(ready.status(), 200);
    let m = reqwest::get(format!("http://{}/metrics", gw.metrics))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(m.contains("gateway_node_calls_in_flight 1"), "{m}");

    // A call waiting for the node gets it when it frees up.
    let waiting = {
        let gw = gw.gw.clone();
        let root = root.clone();
        tokio::spawn(async move { gw.call_slots(&root).await.is_ok() })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(held);
    assert!(waiting.await.unwrap());
    let (s, _) = rest_tip(&gw, "reader-token").await;
    assert_eq!(s, 200);
}

#[tokio::test]
async fn subscriptions_per_key() {
    let gw = gateway(LimitsConfig {
        max_subscriptions: 2,
        ..LimitsConfig::default()
    })
    .await;
    let reader = gw
        .gw
        .keys
        .authenticate(Some("Bearer reader-token"))
        .unwrap();
    let a = gw.gw.subscribe(&reader).await.unwrap();
    let _b = gw.gw.subscribe(&reader).await.unwrap();

    // The third, over SSE: 429.
    let r = reqwest::Client::new()
        .get(format!("{}/v1/events", gw.http_url()))
        .header("authorization", "Bearer reader-token")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    let v: Value = r.json().await.unwrap();
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("subscriptions")
    );

    // Another key has its own.
    let root = gw.gw.keys.authenticate(Some("Bearer root-token")).unwrap();
    assert!(gw.gw.subscribe(&root).await.is_ok());

    // Ending one frees its slot.
    drop(a);
    assert!(gw.gw.subscribe(&reader).await.is_ok());
}

#[tokio::test]
async fn rejected_connections_are_counted_in_metrics() {
    let rejected = Arc::new(AtomicU64::new(0));
    let l = ServeLimits {
        rejected: rejected.clone(),
        ..limits(1, 64, 5_000)
    };
    let (addr, _t) = listener(l).await;
    let _a = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut b = TcpStream::connect(addr).await.unwrap();
    closed_after(&mut b).await;
    assert_eq!(rejected.load(Ordering::Relaxed), 1);

    // And the gateway's listeners share its metrics counter.
    let gw = gateway(LimitsConfig::default()).await;
    assert!(Arc::ptr_eq(
        &gw.gw.serve_limits().rejected,
        &gw.gw.metrics.connections_rejected
    ));
}

#[tokio::test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "binds 127.0.0.2/.3: only Linux routes all of 127/8 to lo"
)]
async fn per_ip_counts_each_source_address() {
    use tokio::net::TcpSocket;
    let l = limits(100, 3, 5_000);
    let rejected = l.rejected.clone();
    let (addr, _t) = listener(l).await;
    let from = |ip: [u8; 4]| async move {
        let s = TcpSocket::new_v4().unwrap();
        s.bind(SocketAddr::from((ip, 0))).unwrap();
        s.connect(addr).await.unwrap()
    };
    let mut held = Vec::new();
    for _ in 0..5 {
        held.push(from([127, 0, 0, 2]).await);
    }
    for _ in 0..2 {
        held.push(from([127, 0, 0, 3]).await);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        rejected.load(Ordering::Relaxed),
        2,
        "two over the limit from .2"
    );
    let mut closed = 0;
    for c in &mut held {
        let mut b = [0u8; 1];
        if let Ok(Ok(0)) = tokio::time::timeout(Duration::from_millis(100), c.read(&mut b)).await {
            closed += 1;
        }
    }
    assert_eq!(closed, 2);
}
