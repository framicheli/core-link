//! K1/K2: every REST route against the mock, encodings, errors, caching,
//! SSE, CORS and the OpenAPI document.
mod common;

use bitcoin::consensus::encode::{deserialize, serialize, serialize_hex};
use bitcoin::{BlockHash, FeeRate};
use bitcoin_gateway::rest::ROUTES;
use bitcoin_node_api::{FeeMode, FeeTarget, NodeAPIError, RpcErrorCode};
use common::{TestGateway, coinbase_out, spend};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_stream::StreamExt;

const R: &str = "Bearer reader-token";
const S: &str = "Bearer sender-token";

struct Http {
    base: String,
    c: Client,
}

impl Http {
    fn new(gw: &TestGateway) -> Http {
        Http {
            base: gw.http_url(),
            c: Client::new(),
        }
    }
    fn get(&self, path: &str, auth: &str) -> reqwest::RequestBuilder {
        self.c
            .get(format!("{}{path}", self.base))
            .header("authorization", auth)
    }
    fn post(&self, path: &str, auth: &str) -> reqwest::RequestBuilder {
        self.c
            .post(format!("{}{path}", self.base))
            .header("authorization", auth)
    }
    async fn json(&self, path: &str) -> (StatusCode, Value) {
        let r = self.get(path, R).send().await.unwrap();
        let s = r.status();
        (s, r.json().await.unwrap())
    }
}

#[tokio::test]
async fn chain_routes() {
    let gw = TestGateway::start().await;
    let hashes = gw.node.mine_empty(3);
    let h = Http::new(&gw);

    let (s, tip) = h.json("/v1/chain/tip").await;
    assert_eq!(s, 200);
    assert_eq!(tip, json!({ "height": 3, "hash": hashes[2].to_string() }));

    let (_, v) = h.json("/v1/chain/heights/1").await;
    assert_eq!(v, json!({ "hash": hashes[0].to_string() }));
    let (s, v) = h.json("/v1/chain/heights/99").await;
    assert_eq!(s, 400); // Core's -8
    assert_eq!(v["error"]["core_code"], -8);
    assert_eq!(v["error"]["code"], "invalid_parameter");
    assert_eq!(v["error"]["type"], "rpc");

    let block = gw.node.block_at(2).unwrap();
    let (_, v) = h.json(&format!("/v1/blocks/{}", hashes[1])).await;
    assert_eq!(v["hex"], serialize_hex(&block));
    let bin = h
        .get(&format!("/v1/blocks/{}", hashes[1]), R)
        .header("accept", "application/octet-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(bin.headers()["content-type"], "application/octet-stream");
    assert_eq!(
        deserialize::<bitcoin::Block>(&bin.bytes().await.unwrap()).unwrap(),
        block
    );

    let bin = h
        .get(&format!("/v1/blocks/{}/header", hashes[1]), R)
        .header("accept", "application/octet-stream")
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(bin.len(), 80);
    let want = bitcoin_node_api::ChainSource::block_filter(&gw.node, hashes[1])
        .await
        .unwrap();
    let (s, v) = h.json(&format!("/v1/blocks/{}/filter", hashes[1])).await;
    assert_eq!(s, 200);
    use bitcoin::hex::DisplayHex;
    assert_eq!(v["filter"], want.content.to_lower_hex_string());
    let bin = h
        .get(&format!("/v1/blocks/{}/filter", hashes[1]), R)
        .header("accept", "application/octet-stream")
        .send()
        .await
        .unwrap();
    assert!(
        bin.headers()["cache-control"]
            .to_str()
            .unwrap()
            .contains("immutable")
    );
    assert_eq!(bin.bytes().await.unwrap().as_ref(), want.content.as_slice());

    let (_, v) = h.json(&format!("/v1/blocks/{}/header", hashes[1])).await;
    assert_eq!(
        v,
        json!({
            "hash": hashes[1].to_string(),
            "hex": serialize_hex(&block.header),
            "height": 2,
            "chain_work": format!("{:064x}", 6),
        })
    );

    let cb = &block.txdata[0];
    let (_, v) = h
        .json(&format!("/v1/tx/{}?block={}", cb.compute_txid(), hashes[1]))
        .await;
    assert_eq!(v["hex"], serialize_hex(cb));
    let (_, v) = h.json(&format!("/v1/tx/{}", cb.compute_txid())).await;
    assert_eq!(v["txid"], cb.compute_txid().to_string());

    let (s, u) = h
        .json(&format!("/v1/utxos/{}/0?mempool=true", cb.compute_txid()))
        .await;
    assert_eq!(s, 200);
    assert_eq!(u["value"], 5_000_000_000u64);
    assert_eq!(u["coinbase"], true);
    // None from a lookup is 404
    let (s, v) = h.json(&format!("/v1/utxos/{}/5", cb.compute_txid())).await;
    assert_eq!(s, 404);
    assert_eq!(v["error"]["code"], "not_found");
    // unknown block: Core's -5 is 404 with the code alongside
    let (s, v) = h
        .json(&format!(
            "/v1/blocks/{}",
            BlockHash::from_byte_array([7; 32])
        ))
        .await;
    assert_eq!(s, 404);
    assert_eq!(v["error"]["core_code"], -5);
    // bad input
    let (s, _) = h.json("/v1/blocks/nothex").await;
    assert_eq!(s, 400);
}

use bitcoin::hashes::Hash;

#[tokio::test]
async fn mempool_fees_broadcast_routes() {
    let gw = TestGateway::start().await;
    gw.node.mine_empty(101);
    let h = Http::new(&gw);
    let tx1 = spend(coinbase_out(&gw.node, 1), 1000);
    let tx2 = spend(coinbase_out(&gw.node, 2), 1000);
    let tx3 = spend(coinbase_out(&gw.node, 3), 1000);

    // test, then broadcast as JSON and as bytes
    let r: Value = h
        .post("/v1/tx/test", S)
        .json(&json!({ "txs": [serialize_hex(&tx1)] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["results"][0]["allowed"], true);
    let r: Value = h
        .post("/v1/tx", S)
        .json(&json!({ "hex": serialize_hex(&tx1) }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["txid"], tx1.compute_txid().to_string());
    let r: Value = h
        .post("/v1/tx", S)
        .header("content-type", "application/octet-stream")
        .body(serialize(&tx2))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["txid"], tx2.compute_txid().to_string());
    // a package as concatenated bytes
    let mut both = serialize(&tx2);
    both.extend(serialize(&tx3));
    let r: Value = h
        .post("/v1/packages", S)
        .header("content-type", "application/octet-stream")
        .body(both)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["results"][0]["outcome"]["type"], "already_in_mempool");
    assert_eq!(r["results"][1]["outcome"]["type"], "accepted");

    let (_, v) = h.json("/v1/mempool/txids").await;
    assert_eq!(v["txids"].as_array().unwrap().len(), 3);
    let (_, v) = h.json("/v1/mempool/snapshot").await;
    assert_eq!(v["txids"].as_array().unwrap().len(), 3);
    assert!(v["sequence"].is_u64());
    let (s, v) = h.json(&format!("/v1/mempool/{}", tx1.compute_txid())).await;
    assert_eq!(s, 200);
    assert_eq!(v["ancestors"], 1);
    let (s, _) = h
        .json(&format!("/v1/mempool/{}", coinbase_out(&gw.node, 9).txid))
        .await;
    assert_eq!(s, 404);

    // no estimate is 200 with null
    let (s, v) = h.json("/v1/fees/estimate?blocks=6").await;
    assert_eq!((s, v), (StatusCode::OK, json!({ "fee_rate": null })));
    gw.node.set_fee(
        FeeTarget {
            blocks: 3,
            mode: FeeMode::Conservative,
        },
        Some(FeeRate::from_sat_per_kwu(500)),
    );
    let (_, v) = h.json("/v1/fees/estimate?blocks=3&mode=conservative").await;
    assert_eq!(v, json!({ "fee_rate": 500 }));
    let (s, _) = h.json("/v1/fees/estimate?mode=fast").await;
    assert_eq!(s, 400);

    // Core's rejection: 422
    gw.node.fail_next(
        "broadcast",
        NodeAPIError::Rpc {
            code: RpcErrorCode::VerifyRejected,
            message: "bad-txns".into(),
        },
    );
    let r = h
        .post("/v1/tx", S)
        .json(&json!({ "hex": serialize_hex(&tx3) }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 422);
    let v: Value = r.json().await.unwrap();
    assert_eq!(
        v,
        json!({"error": {"type": "rpc", "code": "verify_rejected", "core_code": -26, "message": "bad-txns"}})
    );
}

#[tokio::test]
async fn auth_scopes_and_rate() {
    let gw = TestGateway::start().await;
    let h = Http::new(&gw);
    let r =
        h.c.get(format!("{}/v1/chain/tip", h.base))
            .send()
            .await
            .unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(r.headers()["www-authenticate"], "Bearer");
    assert_eq!(
        h.get("/v1/chain/tip", "Bearer bad")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        h.get("/v1/chain/tip", S).send().await.unwrap().status(),
        403
    );
    assert_eq!(
        h.post("/v1/tx", R)
            .json(&json!({"hex": "00"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );

    let mut k = common::key("slow", "slow-token", &["chain:read"]);
    k.rate = Some(bitcoin_gateway::config::Rate {
        per_second: 0.001,
        burst: 1,
    });
    gw.gw.keys.replace(vec![k]);
    let auth = "Bearer slow-token";
    assert_eq!(
        h.get("/v1/chain/tip", auth).send().await.unwrap().status(),
        200
    );
    let r = h.get("/v1/chain/tip", auth).send().await.unwrap();
    assert_eq!(r.status(), 429);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["error"]["code"], "rate_limited");
}

#[tokio::test]
async fn caching() {
    let gw = TestGateway::start().await;
    let hash = gw.node.mine(&[]);
    let h = Http::new(&gw);
    let path = format!("/v1/blocks/{hash}");
    let r = h.get(&path, R).send().await.unwrap();
    assert_eq!(
        r.headers()["cache-control"],
        "private, max-age=31536000, immutable"
    );
    let etag = r.headers()["etag"].to_str().unwrap().to_owned();
    assert_eq!(etag, format!("\"{hash}\""));
    assert_eq!(r.headers()["vary"], "Accept");
    // the binary form has its own tag
    let r = h
        .get(&path, R)
        .header("accept", "application/octet-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["etag"], format!("\"{hash}.bin\"").as_str());

    // If-None-Match: 304 without asking the node...
    gw.node
        .fail_next("block", NodeAPIError::Decode("would fail".into()));
    let r = h
        .get(&path, R)
        .header("if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 304);
    assert_eq!(r.headers()["etag"], etag.as_str());
    // ...but still checking the token and scope
    let r = h
        .get(&path, S)
        .header("if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    gw.node.block(hash).await.ok(); // drain the queued failure

    // changing answers are no-store
    for p in ["/v1/chain/tip", "/v1/mempool/txids", "/v1/fees/estimate"] {
        let r = h.get(p, R).send().await.unwrap();
        assert_eq!(r.headers()["cache-control"], "no-store", "{p}");
        assert!(r.headers().get("etag").is_none(), "{p}");
    }
    let cb = gw.node.block_at(1).unwrap().txdata[0].compute_txid();
    let r = h.get(&format!("/v1/tx/{cb}"), R).send().await.unwrap();
    assert_eq!(r.headers()["cache-control"], "no-store");
    let r = h
        .get(&format!("/v1/tx/{cb}?block={hash}"), R)
        .send()
        .await
        .unwrap();
    assert!(
        r.headers()["cache-control"]
            .to_str()
            .unwrap()
            .contains("immutable")
    );
    assert!(r.headers().get("etag").is_some());

    // public_cache switches private to public
    let mut settings = bitcoin_gateway::Settings::default();
    settings.http.public_cache = true;
    let core = common::FakeCore::start(|_, _, _| Ok(Value::Null)).await;
    let gw2 = TestGateway::with(common::keys(), settings, core).await;
    let hash = gw2.node.mine(&[]);
    let r = Http::new(&gw2)
        .get(&format!("/v1/blocks/{hash}/header"), R)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
}

use bitcoin_node_api::ChainSource;

#[tokio::test]
async fn sse_events() {
    let gw = TestGateway::start().await;
    let h = Http::new(&gw);
    assert_eq!(h.get("/v1/events", S).send().await.unwrap().status(), 403);
    let r = h.get("/v1/events", R).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let mut body = r.bytes_stream();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let hash = gw.node.mine(&[]);
    gw.node.lag();
    let mut text = String::new();
    while !text.contains("lagged") {
        let chunk = tokio::time::timeout(Duration::from_secs(5), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        text.push_str(&String::from_utf8_lossy(&chunk));
    }
    let data: Vec<Value> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(|d| serde_json::from_str(d).unwrap())
        .collect();
    assert_eq!(
        data,
        [
            json!({ "type": "block_connected", "hash": hash.to_string() }),
            json!({ "type": "lagged" })
        ]
    );
}

#[tokio::test]
async fn cors_only_for_listed_origins() {
    let gw = TestGateway::start().await;
    let r = Http::new(&gw)
        .get("/v1/chain/tip", R)
        .header("origin", "https://explorer.example")
        .send()
        .await
        .unwrap();
    assert!(r.headers().get("access-control-allow-origin").is_none());

    let mut settings = bitcoin_gateway::Settings::default();
    settings.http.cors_origins = vec!["https://explorer.example".into()];
    let core = common::FakeCore::start(|_, _, _| Ok(Value::Null)).await;
    let gw = TestGateway::with(common::keys(), settings, core).await;
    let h = Http::new(&gw);
    let r = h
        .get("/v1/chain/tip", R)
        .header("origin", "https://explorer.example")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.headers()["access-control-allow-origin"],
        "https://explorer.example"
    );
    let r = h
        .get("/v1/chain/tip", R)
        .header("origin", "https://evil.example")
        .send()
        .await
        .unwrap();
    assert!(r.headers().get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn openapi_validates_and_covers_every_route() {
    let gw = TestGateway::start().await;
    let r = Client::new()
        .get(format!("{}/v1/openapi.json", gw.http_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let text = r.text().await.unwrap();
    let doc: openapiv3::OpenAPI = serde_json::from_str(&text).expect("a valid OpenAPI 3 document");
    assert_eq!(doc.openapi, "3.0.3");
    let mut ops = 0;
    for (path, item) in doc.paths.paths.iter() {
        let item = item.as_item().unwrap();
        ops += item.iter().count();
        // every path parameter is declared
        for seg in path.split('/').filter(|s| s.starts_with('{')) {
            let name = &seg[1..seg.len() - 1];
            for (_, op) in item.iter() {
                assert!(
                    op.parameters
                        .iter()
                        .any(|p| p.as_item().unwrap().parameter_data_ref().name == name),
                    "{path}: {name} undeclared"
                );
            }
        }
    }
    assert_eq!(ops, ROUTES.len());
}

/// Every gRPC method in the proto has a REST route, and every route
/// answers (not the router's own 404/405).
#[tokio::test]
async fn every_grpc_method_has_a_route() {
    use prost::Message;
    let set = prost_types::FileDescriptorSet::decode(bitcoin_gateway::grpc::DESCRIPTOR).unwrap();
    let mut grpc = Vec::new();
    for f in &set.file {
        for s in &f.service {
            for m in &s.method {
                grpc.push(format!("{}/{}", s.name(), m.name()));
            }
        }
    }
    assert_eq!(grpc.len(), bitcoin_gateway::api::METHODS.len());
    for m in &grpc {
        assert!(
            bitcoin_gateway::api::METHODS.contains(&m.as_str()),
            "{m} not in api::METHODS"
        );
        assert!(ROUTES.iter().any(|r| r.grpc == m), "no REST route for {m}");
    }

    let gw = TestGateway::start().await;
    let hash = gw.node.mine(&[]);
    let txid = gw.node.block_at(1).unwrap().txdata[0].compute_txid();
    let c = Client::new();
    for r in ROUTES {
        if r.grpc == "Events/Subscribe" {
            continue; // a stream; tested above
        }
        let path = r
            .path
            .replace("{hash}", &hash.to_string())
            .replace("{txid}", &txid.to_string())
            .replace("{height}", "1")
            .replace("{vout}", "0");
        let url = format!("{}{path}", gw.http_url());
        let req = match r.method {
            "GET" => c.get(url),
            _ => c.post(url).json(&json!({ "hex": "", "txs": [] })),
        };
        let resp = req
            .header("authorization", "Bearer root-token")
            .send()
            .await
            .unwrap();
        // a router miss is a bare 404/405 with no JSON error body
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert!(
            status.is_success() || body.contains("\"error\""),
            "{} {}: {status} {body}",
            r.method,
            r.path
        );
    }
}
