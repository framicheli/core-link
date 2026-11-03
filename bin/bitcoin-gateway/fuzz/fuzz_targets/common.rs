//! One gateway on the mock, built once per fuzzing process, with a stub
//! Core for the WebSocket pass-through and readiness.
use bitcoin_gateway::auth::{Key, hash_token};
use bitcoin_gateway::config::KeySpec;
use bitcoin_gateway::node::{BoxFut, Upstream};
use bitcoin_gateway::scopes::Scope;
use bitcoin_gateway::{Gateway, Settings};
use bitcoin_node_api::ApiResult;
use bitcoin_node_mock::MockNode;
use serde_json::{Value, json};
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

/// Answers every call: readiness gets "not in IBD", the rest `null`.
struct StubCore;

impl Upstream for StubCore {
    fn call<'a>(
        &'a self,
        _wallet: Option<&'a str>,
        method: &'a str,
        _params: Value,
    ) -> BoxFut<'a, ApiResult<Value>> {
        Box::pin(async move {
            Ok(match method {
                "getblockchaininfo" => json!({ "initialblockdownload": false }),
                _ => Value::Null,
            })
        })
    }
}

pub const TOKENS: [&str; 4] = ["root-token", "reader-token", "hot-token", "sender-token"];

fn key(name: &str, token: &str, allow: &[&str], allow_unknown: bool) -> KeySpec {
    KeySpec {
        name: name.into(),
        token_sha256: hash_token(token),
        scopes: allow.iter().map(|s| Scope::parse(s).unwrap()).collect(),
        rate: None,
        allow_unknown,
        max_in_flight: 32,
        max_subscriptions: 16,
    }
}

pub struct Harness {
    pub rt: Runtime,
    pub gw: Arc<Gateway<MockNode>>,
}

impl Harness {
    /// The key for `TOKENS[i % 4]`.
    #[allow(dead_code)] // the rest target authenticates by header
    pub fn key(&self, i: u8) -> Arc<Key> {
        let t = TOKENS[i as usize % TOKENS.len()];
        self.gw
            .keys
            .authenticate(Some(&format!("Bearer {t}")))
            .unwrap()
    }
}

/// A node with some blocks and a mempool tx, so lookups find things.
pub fn harness() -> &'static Harness {
    static H: OnceLock<Harness> = OnceLock::new();
    H.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let node = MockNode::regtest();
        node.mine_empty(3);
        let gw = rt.block_on(async {
            Gateway::new(
                node,
                Arc::new(StubCore) as Arc<dyn Upstream>,
                vec![
                    key("root", TOKENS[0], &["admin"], true),
                    key(
                        "reader",
                        TOKENS[1],
                        &["chain:read", "mempool:read", "fees:read", "events"],
                        false,
                    ),
                    key(
                        "hot",
                        TOKENS[2],
                        &["wallet:hot:read", "wallet:hot:write"],
                        false,
                    ),
                    key("sender", TOKENS[3], &["broadcast"], false),
                ],
                Settings::default(),
                CancellationToken::new(),
            )
        });
        Harness { rt, gw }
    })
}
