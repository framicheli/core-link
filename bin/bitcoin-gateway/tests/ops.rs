//! L1: metrics, /healthz and /readyz.
mod common;

use bitcoin_gateway::Settings;
use common::{FakeCore, TestGateway, keys};
use serde_json::json;

async fn get(url: String) -> (u16, String) {
    let r = reqwest::get(url).await.unwrap();
    (r.status().as_u16(), r.text().await.unwrap())
}

#[tokio::test]
async fn scrape_shows_the_counters() {
    let gw = TestGateway::start().await;
    gw.node.mine_empty(1);
    let c = reqwest::Client::new();
    let tip = format!("{}/v1/chain/tip", gw.http_url());
    for auth in [
        "Bearer reader-token",
        "Bearer reader-token",
        "Bearer sender-token",
    ] {
        c.get(&tip)
            .header("authorization", auth)
            .send()
            .await
            .unwrap();
    }
    let _sub = gw.gw.fanout.subscribe();
    let (s, m) = get(format!("http://{}/metrics", gw.metrics)).await;
    assert_eq!(s, 200);
    for line in [
        "gateway_requests_total{key=\"reader\",method=\"Chain/Tip\",outcome=\"ok\"} 2",
        "gateway_requests_total{key=\"sender\",method=\"Chain/Tip\",outcome=\"permission_denied\"} 1",
        "gateway_request_duration_seconds_count{method=\"Chain/Tip\"} 3",
        "gateway_core_reachable 1",
        "gateway_event_subscribers 1",
        "gateway_lagged_sent_total 0",
        "gateway_fanout_resubscriptions_total 0",
    ] {
        assert!(m.contains(line), "{line} not in\n{m}");
    }
}

#[tokio::test]
async fn health_and_readiness() {
    let gw = TestGateway::start().await;
    assert_eq!(get(format!("http://{}/healthz", gw.metrics)).await.0, 200);
    assert_eq!(
        get(format!("http://{}/readyz", gw.metrics)).await,
        (200, "ready\n".into())
    );

    // Core down: alive, not ready
    let dead = FakeCore {
        url: "http://127.0.0.1:1".into(),
        calls: Default::default(),
    };
    let down = TestGateway::with(keys(), Settings::default(), dead).await;
    assert_eq!(get(format!("http://{}/healthz", down.metrics)).await.0, 200);
    let (s, body) = get(format!("http://{}/readyz", down.metrics)).await;
    assert_eq!(s, 503);
    assert!(body.contains("unreachable"), "{body}");
    let (_, m) = get(format!("http://{}/metrics", down.metrics)).await;
    assert!(m.contains("gateway_core_reachable 0"));
    assert!(!*down.gw.health.borrow());

    // Core in initial block download: not ready
    let ibd = FakeCore::start(|_, m, _| match m {
        "getblockchaininfo" => Ok(json!({ "initialblockdownload": true })),
        _ => Ok(json!(null)),
    })
    .await;
    let ibd = TestGateway::with(keys(), Settings::default(), ibd).await;
    let (s, body) = get(format!("http://{}/readyz", ibd.metrics)).await;
    assert_eq!(s, 503);
    assert!(body.contains("initial block download"), "{body}");
}
