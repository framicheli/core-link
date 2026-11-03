//! L2: the gateway in front of a real `bitcoind` with ZMQ. `#[ignore]`:
//! `cargo test -- --ignored` (see `test-node` for which binary). CI runs it
//! on every supported Core.
//!
//! gRPC and REST tip, block and broadcast; gRPC and SSE event streams
//! through a reorg; a WebSocket wallet send with and without the write scope.
mod common;

use bitcoin::consensus::encode::{deserialize, serialize, serialize_hex};
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, Transaction};
use bitcoin_core_rpc::{Auth, Client, CreateWalletOptions, Wallet};
use bitcoin_core_zmq::{Topic, ZmqEvents};
use bitcoin_gateway::grpc::pb;
use bitcoin_gateway::node::{Events, Node, Upstream};
use bitcoin_gateway::server::serve;
use bitcoin_gateway::{Gateway, Settings};
use common::key;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use test_node::{RPC_PASSWORD, RPC_USER, TestNode, free_port};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;

async fn mine(c: &Client, n: u32, to: &str) -> Vec<BlockHash> {
    let v: Vec<String> = c
        .raw_call("generatetoaddress", json!([n, to]))
        .await
        .unwrap();
    v.iter().map(|h| BlockHash::from_str(h).unwrap()).collect()
}

/// A signed tx paying 0.1 BTC back to the wallet, not broadcast.
async fn signed_tx(w: &Wallet, to: &str) -> Transaction {
    let funded: Value = w
        .raw_call(
            "walletcreatefundedpsbt",
            json!([[], [{ to: 0.1 }], 0, { "lockUnspents": true }]),
        )
        .await
        .unwrap();
    let processed: Value = w
        .raw_call("walletprocesspsbt", json!([funded["psbt"]]))
        .await
        .unwrap();
    let fin: Value = w
        .client()
        .raw_call("finalizepsbt", json!([processed["psbt"]]))
        .await
        .unwrap();
    bitcoin::consensus::encode::deserialize_hex(fin["hex"].as_str().unwrap()).unwrap()
}

fn req<T>(msg: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut()
        .insert("authorization", "Bearer reader-token".parse().unwrap());
    r
}

async fn next_block_event(s: &mut tonic::Streaming<pb::NodeEvent>) -> pb::node_event::Event {
    use pb::node_event::Event;
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an event in time")
            .unwrap()
            .unwrap()
            .event
            .unwrap();
        match ev {
            Event::TxAdded(_) | Event::TxRemoved(_) => continue,
            e => return e,
        }
    }
}

#[tokio::test]
#[ignore = "needs bitcoind"]
async fn gateway_in_front_of_core() {
    let zmq = format!("tcp://127.0.0.1:{}", free_port());
    let node = TestNode::start(&[&format!("-zmqpubsequence={zmq}"), "-fallbackfee=0.0001"]);
    let auth = Auth::UserPass(RPC_USER.into(), RPC_PASSWORD.into());
    let mut client = None;
    for _ in 0..100 {
        if let Ok(c) = Client::connect(&node.rpc_url(), auth.clone()).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let client = client.unwrap_or_else(|| panic!("node never answered:\n{}", node.log_tail(30)));
    let hot = client
        .create_wallet("hot", CreateWalletOptions::default())
        .await
        .unwrap()
        .wallet;
    let addr = hot
        .get_new_address(None, None)
        .await
        .unwrap()
        .assume_checked()
        .to_string();
    mine(&client, 101, &addr).await;

    let events = ZmqEvents::connect(&zmq, Topic::Sequence).await.unwrap();
    let keys = vec![
        key(
            "reader",
            "reader-token",
            &["chain:read", "mempool:read", "events"],
        ),
        key("sender", "sender-token", &["broadcast"]),
        key("hot-read", "hot-read-token", &["wallet:hot:read"]),
        key(
            "hot-write",
            "hot-write-token",
            &["wallet:hot:read", "wallet:hot:write"],
        ),
    ];
    let shutdown = CancellationToken::new();
    let gw = Gateway::new(
        Node {
            rpc: client.clone(),
            events: Events::Zmq(events),
        },
        Arc::new(client.clone()) as Arc<dyn Upstream>,
        keys,
        Settings::default(),
        shutdown.clone(),
    );
    let grpc_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (grpc, http) = (grpc_l.local_addr().unwrap(), http_l.local_addr().unwrap());
    tokio::spawn(serve(grpc_l, gw.grpc_router(), None, shutdown.clone()));
    tokio::spawn(serve(http_l, gw.http_router(), None, shutdown.clone()));
    gw.check_ready().await.unwrap();

    let channel = tonic::transport::Channel::from_shared(format!("http://{grpc}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut chain = pb::chain_client::ChainClient::new(channel.clone());
    let http_c = reqwest::Client::new();
    let rest = |path: &str, token: &str| {
        http_c
            .get(format!("http://{http}{path}"))
            .header("authorization", format!("Bearer {token}"))
    };

    // tip and block, gRPC and REST
    let best = client.get_best_block_hash().await.unwrap();
    let tip = chain
        .tip(req(pb::TipRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (tip.height, tip.hash.as_slice()),
        (101, best.as_byte_array().as_slice())
    );
    let v: Value = rest("/v1/chain/tip", "reader-token")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!({ "height": 101, "hash": best.to_string() }));
    let block = client.get_block(best).await.unwrap();
    let b = chain
        .block(req(pb::HashRequest {
            hash: best.to_byte_array().to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deserialize::<bitcoin::Block>(&b.block).unwrap(), block);
    let bin = rest(&format!("/v1/blocks/{best}"), "reader-token")
        .header("accept", "application/octet-stream")
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(bin.as_ref(), serialize(&block).as_slice());

    // broadcast over gRPC, then over REST
    let mut bc = pb::broadcast_client::BroadcastClient::new(channel.clone());
    let tx1 = signed_tx(&hot, &addr).await;
    let mut r = tonic::Request::new(pb::TxRequest {
        transaction: serialize(&tx1),
    });
    r.metadata_mut()
        .insert("authorization", "Bearer sender-token".parse().unwrap());
    let id = bc.broadcast(r).await.unwrap().into_inner();
    assert_eq!(id.txid, tx1.compute_txid().to_byte_array());
    let tx2 = signed_tx(&hot, &addr).await;
    let v: Value = http_c
        .post(format!("http://{http}/v1/tx"))
        .header("authorization", "Bearer sender-token")
        .json(&json!({ "hex": serialize_hex(&tx2) }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["txid"], tx2.compute_txid().to_string());
    let (s, _) = {
        let r = rest(
            &format!("/v1/mempool/{}", tx2.compute_txid()),
            "reader-token",
        )
        .send()
        .await
        .unwrap();
        (r.status(), ())
    };
    assert_eq!(s, 200);

    // event streams through a reorg: gRPC and SSE
    let mut events = pb::events_client::EventsClient::new(channel);
    let mut grpc_events = events
        .subscribe(req(pb::SubscribeRequest {}))
        .await
        .unwrap()
        .into_inner();
    let sse = rest("/v1/events", "reader-token").send().await.unwrap();
    let mut sse = sse.bytes_stream();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let a = mine(&client, 1, &addr).await[0];
    client.invalidate_block(a).await.unwrap();
    // to another address, so the new block isn't the invalidated one again
    let other = hot
        .get_new_address(None, None)
        .await
        .unwrap()
        .assume_checked()
        .to_string();
    let new = mine(&client, 2, &other).await;

    use pb::node_event::Event;
    let h = |b: BlockHash| b.to_byte_array().to_vec();
    assert_eq!(
        next_block_event(&mut grpc_events).await,
        Event::BlockConnected(pb::BlockConnected { hash: h(a) })
    );
    assert_eq!(
        next_block_event(&mut grpc_events).await,
        Event::BlockDisconnected(pb::BlockDisconnected { hash: h(a) })
    );
    for b in &new {
        assert_eq!(
            next_block_event(&mut grpc_events).await,
            Event::BlockConnected(pb::BlockConnected { hash: h(*b) })
        );
    }
    let want = [
        json!({ "type": "block_connected", "hash": a.to_string() }),
        json!({ "type": "block_disconnected", "hash": a.to_string() }),
        json!({ "type": "block_connected", "hash": new[0].to_string() }),
        json!({ "type": "block_connected", "hash": new[1].to_string() }),
    ];
    let mut text = String::new();
    let mut blocks = Vec::new();
    while blocks.len() < want.len() {
        let chunk = tokio::time::timeout(Duration::from_secs(20), sse.next())
            .await
            .expect("SSE in time")
            .unwrap()
            .unwrap();
        text.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(i) = text.find("\n\n") {
            let ev: String = text.drain(..i + 2).collect();
            if let Some(d) = ev.lines().find_map(|l| l.strip_prefix("data: ")) {
                let v: Value = serde_json::from_str(d).unwrap();
                assert_ne!(v["type"], "lagged");
                if v["type"].as_str().unwrap().starts_with("block") {
                    blocks.push(v);
                }
            }
        }
    }
    assert_eq!(blocks, want);

    // a wallet send over WebSocket, without and with the write scope
    let ws = |token: &'static str| {
        let mut r = format!("ws://{http}/ws/wallet/hot")
            .into_client_request()
            .unwrap();
        r.headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        async move { tokio_tungstenite::connect_async(r).await.unwrap().0 }
    };
    let send =
        json!({ "jsonrpc": "2.0", "id": 1, "method": "sendtoaddress", "params": [addr, 0.5] });
    let ask = |mut s: tokio_tungstenite::WebSocketStream<_>, v: Value| async move {
        s.send(Message::Text(v.to_string().into())).await.unwrap();
        loop {
            if let Message::Text(t) = s.next().await.unwrap().unwrap() {
                return serde_json::from_str::<Value>(&t).unwrap();
            }
        }
    };
    let r = ask(ws("hot-read-token").await, send.clone()).await;
    assert_eq!(r["error"]["code"], -32001);
    let balance = ask(
        ws("hot-read-token").await,
        json!({ "jsonrpc": "2.0", "id": 2, "method": "getbalance", "params": [] }),
    )
    .await;
    assert!(balance["result"].as_f64().unwrap() > 0.0, "{balance}");
    let r = ask(ws("hot-write-token").await, send).await;
    let txid =
        bitcoin::Txid::from_str(r["result"].as_str().unwrap_or_else(|| panic!("{r}"))).unwrap();
    let mempool: Vec<bitcoin::Txid> = client.raw_call("getrawmempool", json!([])).await.unwrap();
    assert!(mempool.contains(&txid));
    // Core's own error, unchanged: an amount over the balance
    let r = ask(
        ws("hot-write-token").await,
        json!({ "jsonrpc": "2.0", "id": 3, "method": "sendtoaddress", "params": [addr, 1_000_000] }),
    )
    .await;
    assert_eq!(r["error"]["code"], -6, "{r}");
    shutdown.cancel();
}
