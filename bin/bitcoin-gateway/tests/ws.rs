//! K3/K4: WebSocket pass-through, wallet routes, batches, events, limits.
mod common;

use bitcoin_gateway::Settings;
use common::{FakeCore, TestGateway, key, keys};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A fake Core with a little state: wallet `hot` exists.
async fn core() -> FakeCore {
    FakeCore::start(|wallet, method, params| match (wallet, method) {
        (None, "getblockcount") => Ok(json!(3)),
        (None, "echo") => Ok(params.clone()),
        (None, "sendrawtransaction") => Err((-26, "bad-txns-inputs-missingorspent".into())),
        (Some("hot"), "getbalance") => Ok(json!(1.5)),
        (Some(w), _) if w != "hot" => Err((
            -18,
            format!("Requested wallet does not exist or is not loaded: {w}"),
        )),
        (_, "getnewthing") => Ok(json!("new")),
        _ => Ok(Value::Null),
    })
    .await
}

async fn start(settings: Settings) -> TestGateway {
    let mut ks = keys();
    ks.push(key(
        "payments",
        "pay-token",
        &["wallet:hot:read", "wallet:hot:write", "chain:read"],
    ));
    let mut any = key("any", "any-token", &["admin"]);
    any.allow_unknown = true;
    ks.push(any);
    TestGateway::with(ks, settings, core().await).await
}

async fn connect(gw: &TestGateway, path: &str, token: &str) -> Ws {
    let mut req = format!("ws://{}{path}", gw.http)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

async fn recv(ws: &mut Ws) -> Value {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("a reply in time")
            .unwrap()
            .unwrap();
        if let Message::Text(t) = m {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

async fn call(ws: &mut Ws, req: Value) -> Value {
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    recv(ws).await
}

fn rpc(id: u64, method: &str, params: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

#[tokio::test]
async fn pass_through_returns_core_unchanged() {
    let gw = start(Settings::default()).await;
    let mut ws = connect(&gw, "/ws", "reader-token").await;
    let r = call(&mut ws, rpc(1, "getblockcount", json!([]))).await;
    assert_eq!(r, json!({ "jsonrpc": "2.0", "id": 1, "result": 3 }));

    // Core's error comes back unchanged
    let mut ws = connect(&gw, "/ws", "sender-token").await;
    let r = call(&mut ws, rpc(2, "sendrawtransaction", json!(["00"]))).await;
    assert_eq!(
        r,
        json!({ "jsonrpc": "2.0", "id": 2, "error": { "code": -26, "message": "bad-txns-inputs-missingorspent" } })
    );

    // named params pass through too; ids may be strings
    let mut ws = connect(&gw, "/ws", "hot-token").await;
    let r = call(
        &mut ws,
        json!({ "jsonrpc": "2.0", "id": "x", "method": "echo", "params": { "arg0": "hi" } }),
    )
    .await;
    assert_eq!(r["id"], "x");
    assert_eq!(r["result"], json!({ "arg0": "hi" }));
}

#[tokio::test]
async fn wallet_scope_is_per_wallet() {
    let gw = start(Settings::default()).await;
    let mut hot = connect(&gw, "/ws/wallet/hot", "hot-token").await;
    let r = call(&mut hot, rpc(1, "getbalance", json!([]))).await;
    assert_eq!(r["result"], 1.5);
    // the hot key reads only
    let r = call(&mut hot, rpc(2, "sendtoaddress", json!(["bcrt1q...", 1]))).await;
    assert_eq!(r["error"]["code"], -32001);
    // and only wallet hot
    let mut cold = connect(&gw, "/ws/wallet/cold", "hot-token").await;
    let r = call(&mut cold, rpc(3, "getbalance", json!([]))).await;
    assert_eq!(r["error"]["code"], -32001);
    // wallet methods on the node route need admin
    let mut node = connect(&gw, "/ws", "pay-token").await;
    let r = call(&mut node, rpc(4, "getbalance", json!([]))).await;
    assert_eq!(r["error"]["code"], -32001);
    assert!(
        r["error"]["message"]
            .as_str()
            .unwrap()
            .contains("/ws/wallet/")
    );
    // the write scope can spend, on the wallet's own endpoint
    let mut pay = connect(&gw, "/ws/wallet/hot", "pay-token").await;
    let r = call(&mut pay, rpc(5, "sendtoaddress", json!(["bcrt1q...", 1]))).await;
    assert_eq!(r["result"], Value::Null);
    assert!(r.get("error").is_none());
    let calls = gw.core.calls.lock().unwrap().clone();
    assert!(calls.contains(&(Some("hot".into()), "sendtoaddress".into())));
    assert!(calls.contains(&(Some("hot".into()), "getbalance".into())));
    // nothing denied reached Core
    assert!(!calls.iter().any(|(w, _)| w.as_deref() == Some("cold")));
}

#[tokio::test]
async fn batch_with_one_denied_item() {
    let gw = start(Settings::default()).await;
    let mut ws = connect(&gw, "/ws", "reader-token").await;
    let r = call(
        &mut ws,
        json!([
            rpc(1, "getblockcount", json!([])),
            rpc(2, "stop", json!([])),
            { "jsonrpc": "2.0", "method": "getblockcount" }, // a notification: no reply
            rpc(3, "getnewthing", json!([])),
        ]),
    )
    .await;
    let r = r.as_array().unwrap();
    assert_eq!(r.len(), 3);
    assert_eq!(r[0]["result"], 3);
    assert_eq!(r[1]["id"], 2);
    assert_eq!(r[1]["error"]["code"], -32001);
    assert!(
        r[1]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("admin only")
    );
    assert_eq!(r[2]["error"]["code"], -32001);
    assert!(
        !gw.core
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(_, m)| m == "stop")
    );

    // unknown methods only for admin + allow_unknown
    let mut any = connect(&gw, "/ws", "any-token").await;
    assert_eq!(
        call(&mut any, rpc(1, "getnewthing", json!([]))).await["result"],
        "new"
    );

    // malformed frames
    ws.send(Message::Text("{not json".into())).await.unwrap();
    assert_eq!(recv(&mut ws).await["error"]["code"], -32700);
    assert_eq!(
        call(&mut ws, json!({ "id": 9 })).await["error"]["code"],
        -32600
    );
    assert_eq!(call(&mut ws, json!([])).await["error"]["code"], -32600);
}

#[tokio::test]
async fn events_subscribe_and_unsubscribe() {
    let gw = start(Settings::default()).await;
    let mut ws = connect(&gw, "/ws", "sender-token").await;
    assert_eq!(
        call(&mut ws, rpc(1, "events.subscribe", json!([]))).await["error"]["code"],
        -32001
    );

    let mut ws = connect(&gw, "/ws", "reader-token").await;
    let r = call(&mut ws, rpc(1, "events.subscribe", json!([]))).await;
    assert_eq!(r["result"], "sub-1");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let hash = gw.node.mine(&[]);
    let note = recv(&mut ws).await;
    assert_eq!(
        note,
        json!({ "jsonrpc": "2.0", "method": "events.event", "params": {
            "subscription": "sub-1",
            "event": { "type": "block_connected", "hash": hash.to_string() }
        }})
    );
    gw.node.lag();
    assert_eq!(
        recv(&mut ws).await["params"]["event"],
        json!({ "type": "lagged" })
    );

    let r = call(&mut ws, rpc(2, "events.unsubscribe", json!(["sub-1"]))).await;
    assert_eq!(r["result"], true);
    let r = call(&mut ws, rpc(3, "events.unsubscribe", json!(["sub-1"]))).await;
    assert_eq!(r["error"]["code"], -32602);
    // no more events after unsubscribing
    gw.node.mine(&[]);
    let r = call(&mut ws, rpc(4, "getblockcount", json!([]))).await;
    assert_eq!(r["id"], 4);
}

#[tokio::test]
async fn oversize_frame_closes_with_1009() {
    let mut settings = Settings::default();
    settings.ws.max_message_bytes = 1024;
    let gw = start(settings).await;
    let mut ws = connect(&gw, "/ws", "reader-token").await;
    let big = rpc(1, "echo", json!(["x".repeat(4096)]));
    ws.send(Message::Text(big.to_string().into()))
        .await
        .unwrap();
    let close = loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
        {
            Some(Ok(Message::Close(f))) => break f,
            Some(Ok(_)) => continue,
            Some(Err(WsError::ConnectionClosed)) | None => panic!("closed without a frame"),
            Some(Err(e)) => panic!("{e}"),
        }
    };
    assert_eq!(close.unwrap().code, CloseCode::Size);
}

#[tokio::test]
async fn rate_breach_is_32002() {
    let mut k = key("slow", "slow-token", &["chain:read"]);
    k.rate = Some(bitcoin_gateway::config::Rate {
        per_second: 0.001,
        burst: 2,
    });
    let gw = TestGateway::with(vec![k], Settings::default(), core().await).await;
    let mut ws = connect(&gw, "/ws", "slow-token").await;
    let r = call(
        &mut ws,
        json!([
            rpc(1, "getblockcount", json!([])),
            rpc(2, "getblockcount", json!([])),
            rpc(3, "getblockcount", json!([])),
        ]),
    )
    .await;
    assert_eq!(r[0]["result"], 3);
    assert_eq!(r[1]["result"], 3);
    assert_eq!(r[2]["error"]["code"], -32002);
}

#[tokio::test]
async fn upstream_down_is_32003() {
    let dead = FakeCore {
        url: "http://127.0.0.1:1".into(),
        calls: Default::default(),
    };
    let gw = TestGateway::with(keys(), Settings::default(), dead).await;
    let mut ws = connect(&gw, "/ws", "reader-token").await;
    let r = call(&mut ws, rpc(1, "getblockcount", json!([]))).await;
    assert_eq!(r["error"]["code"], -32003);
}

#[tokio::test]
async fn upgrade_needs_a_token() {
    let gw = start(Settings::default()).await;
    let req = format!("ws://{}/ws", gw.http)
        .into_client_request()
        .unwrap();
    match tokio_tungstenite::connect_async(req).await {
        Err(WsError::Http(r)) => assert_eq!(r.status(), 401),
        other => panic!("{other:?}"),
    }
    // browsers: the token as a subprotocol
    let mut req = format!("ws://{}/ws", gw.http)
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        "bitcoin-gateway, bearer.reader-token".parse().unwrap(),
    );
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(resp.headers()["sec-websocket-protocol"], "bitcoin-gateway");
    assert_eq!(
        call(&mut ws, rpc(1, "getblockcount", json!([]))).await["result"],
        3
    );
}

#[tokio::test]
async fn key_reload_closes_connections() {
    let gw = start(Settings::default()).await;
    let mut ws = connect(&gw, "/ws/wallet/hot", "pay-token").await;
    assert_eq!(
        call(&mut ws, rpc(1, "getbalance", json!([]))).await["result"],
        1.5
    );
    // The operator revokes every key: the socket closes rather than
    // keeping the key it authenticated with.
    gw.gw.keys.replace(vec![]);
    let m = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    match m {
        Some(Ok(Message::Close(Some(f)))) => assert_eq!(f.code, CloseCode::Restart),
        other => panic!("{other:?}"),
    }
    let req = format!("ws://{}/ws/wallet/hot", gw.http)
        .into_client_request()
        .unwrap();
    let mut req = req;
    req.headers_mut()
        .insert("authorization", "Bearer pay-token".parse().unwrap());
    match tokio_tungstenite::connect_async(req).await {
        Err(WsError::Http(r)) => assert_eq!(r.status(), 401),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn shutdown_closes_connections() {
    let gw = start(Settings::default()).await;
    let mut ws = connect(&gw, "/ws", "reader-token").await;
    gw.shutdown.cancel();
    let m = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    match m {
        Some(Ok(Message::Close(Some(f)))) => assert_eq!(f.code, CloseCode::Away),
        other => panic!("{other:?}"),
    }
}
