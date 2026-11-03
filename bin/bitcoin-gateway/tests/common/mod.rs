//! Shared by the integration tests: a fake Core over HTTP, and a gateway
//! over `bitcoin-node-mock` on ephemeral ports.
#![allow(dead_code)]
use axum::Json;
use axum::extract::{Path, State};
use axum::routing::post;
use bitcoin_core_rpc::{Auth, Client, Version};
use bitcoin_gateway::auth::hash_token;
use bitcoin_gateway::config::KeySpec;
use bitcoin_gateway::node::Upstream;
use bitcoin_gateway::scopes::Scope;
use bitcoin_gateway::server::serve_with;
use bitcoin_gateway::{Gateway, Settings};
use bitcoin_node_mock::MockNode;
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// What a fake Core answers: `Ok(result)` or `Err((code, message))`.
pub type Answer = Result<Value, (i64, String)>;
/// `(wallet, method)` of every call, in order.
pub type Calls = Arc<Mutex<Vec<(Option<String>, String)>>>;
type Handler = Arc<dyn Fn(Option<&str>, &str, &Value) -> Answer + Send + Sync>;

/// A JSON-RPC server that speaks just enough of Core's dialect.
pub struct FakeCore {
    pub url: String,
    /// `(wallet, method)` of every call, in order.
    pub calls: Calls,
}

#[derive(Clone)]
struct FakeState {
    handler: Handler,
    calls: Calls,
}

fn reply(s: &FakeState, wallet: Option<&str>, req: &Value) -> Value {
    let method = req["method"].as_str().unwrap_or_default();
    s.calls
        .lock()
        .unwrap()
        .push((wallet.map(str::to_owned), method.to_owned()));
    let params = req.get("params").cloned().unwrap_or(json!([]));
    let answer = match method {
        "getnetworkinfo" => Ok(json!({"version": 310000})),
        _ => (s.handler)(wallet, method, &params),
    };
    match answer {
        Ok(r) => json!({"result": r, "error": null, "id": req["id"]}),
        Err((code, message)) => {
            json!({"result": null, "error": {"code": code, "message": message}, "id": req["id"]})
        }
    }
}

async fn node_route(State(s): State<FakeState>, Json(req): Json<Value>) -> Json<Value> {
    Json(answer_all(&s, None, &req))
}

async fn wallet_route(
    State(s): State<FakeState>,
    Path(name): Path<String>,
    Json(req): Json<Value>,
) -> Json<Value> {
    Json(answer_all(&s, Some(&name), &req))
}

fn answer_all(s: &FakeState, wallet: Option<&str>, req: &Value) -> Value {
    match req {
        Value::Array(items) => Value::Array(items.iter().map(|r| reply(s, wallet, r)).collect()),
        r => reply(s, wallet, r),
    }
}

impl FakeCore {
    /// Serves `handler` on an ephemeral port. `getnetworkinfo` answers Core 31.
    pub async fn start(
        handler: impl Fn(Option<&str>, &str, &Value) -> Answer + Send + Sync + 'static,
    ) -> FakeCore {
        let state = FakeState {
            handler: Arc::new(handler),
            calls: Arc::default(),
        };
        let calls = state.calls.clone();
        let app = axum::Router::new()
            .route("/", post(node_route))
            .route("/wallet/{name}", post(wallet_route))
            .with_state(state);
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        FakeCore {
            url: format!("http://{addr}"),
            calls,
        }
    }

    /// A client for it.
    pub fn client(&self) -> Client {
        Client::with_version(
            &self.url,
            Auth::UserPass("u".into(), "p".into()),
            Version(310000),
        )
    }
}

/// A key with `token` and `allow`.
pub fn key(name: &str, token: &str, allow: &[&str]) -> KeySpec {
    KeySpec {
        name: name.into(),
        token_sha256: hash_token(token),
        scopes: allow.iter().map(|s| Scope::parse(s).unwrap()).collect(),
        rate: None,
        allow_unknown: false,
        max_in_flight: 32,
        max_subscriptions: 16,
    }
}

/// The keys most tests use.
pub fn keys() -> Vec<KeySpec> {
    vec![
        key(
            "reader",
            "reader-token",
            &["chain:read", "mempool:read", "fees:read", "events"],
        ),
        key("sender", "sender-token", &["broadcast"]),
        key("hot", "hot-token", &["wallet:hot:read", "util:read"]),
        key("root", "root-token", &["admin"]),
    ]
}

/// A gateway on the mock, its listeners on ephemeral ports.
pub struct TestGateway {
    pub node: MockNode,
    pub gw: Arc<Gateway<MockNode>>,
    pub grpc: SocketAddr,
    pub http: SocketAddr,
    pub metrics: SocketAddr,
    pub shutdown: CancellationToken,
    pub core: FakeCore,
}

impl TestGateway {
    /// Starts with [`keys`], default settings, and a fake Core whose every
    /// method returns `null`.
    pub async fn start() -> TestGateway {
        TestGateway::with(
            keys(),
            Settings::default(),
            FakeCore::start(|_, _, _| Ok(Value::Null)).await,
        )
        .await
    }

    /// Starts with these keys, settings and pass-through Core.
    pub async fn with(keys: Vec<KeySpec>, settings: Settings, core: FakeCore) -> TestGateway {
        let node = MockNode::regtest();
        let shutdown = CancellationToken::new();
        let upstream = Arc::new(core.client()) as Arc<dyn Upstream>;
        let gw = Gateway::new(node.clone(), upstream, keys, settings, shutdown.clone());
        let mut addrs = Vec::new();
        for app in [gw.grpc_router(), gw.http_router(), gw.metrics_router()] {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(l.local_addr().unwrap());
            tokio::spawn(serve_with(
                l,
                app,
                None,
                shutdown.clone(),
                gw.serve_limits(),
            ));
        }
        TestGateway {
            node,
            gw,
            grpc: addrs[0],
            http: addrs[1],
            metrics: addrs[2],
            shutdown,
            core,
        }
    }

    /// `http://<http listener>`.
    pub fn http_url(&self) -> String {
        format!("http://{}", self.http)
    }

    /// `http://<grpc listener>`.
    pub fn grpc_url(&self) -> String {
        format!("http://{}", self.grpc)
    }
}

impl Drop for TestGateway {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// A tx spending `prev` to an OP_RETURN; the mock doesn't check scripts.
pub fn spend(prev: bitcoin::OutPoint, sats: u64) -> bitcoin::Transaction {
    use bitcoin::{Amount, ScriptBuf, Sequence, TxIn, TxOut, Witness, absolute, transaction};
    bitcoin::Transaction {
        version: transaction::Version::TWO,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: prev,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: ScriptBuf::new_op_return([2]),
        }],
    }
}

/// Output 0 of the coinbase at `height`.
pub fn coinbase_out(node: &MockNode, height: u64) -> bitcoin::OutPoint {
    bitcoin::OutPoint {
        txid: node.block_at(height).unwrap().txdata[0].compute_txid(),
        vout: 0,
    }
}
