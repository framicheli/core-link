//! WebSocket at `/ws` and `/ws/wallet/<name>`: Core's JSON-RPC passed
//! through, filtered by the key's scopes, plus `events.subscribe`
//! (PHASE_2.md §5).
//!
//! Method names, params, results and errors are Core's, unchanged. Errors
//! the gateway adds use JSON-RPC's server range:
//!
//! | Code     | Meaning                                       |
//! | -------- | --------------------------------------------- |
//! | `-32001` | the key may not call this method              |
//! | `-32002` | the key's rate is exceeded                    |
//! | `-32003` | the node is unreachable                       |
//!
//! A message larger than `[ws] max_message_bytes` closes the connection with
//! close code 1009 (message too big).
//!
//! Browsers can't set `Authorization` on a WebSocket, so the token may also
//! come as a subprotocol: `new WebSocket(url, ["bitcoin-gateway",
//! "bearer." + token])`. Never a query parameter: URLs end up in logs.
use crate::Gateway;
use crate::auth::Key;
use crate::error::{ApiError, Kind};
use crate::node::Backend;
use axum::Router;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bitcoin_node_api::{NodeAPIError, TransportError};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;

/// The subprotocol the gateway answers with when the token came as one.
pub const PROTOCOL: &str = "bitcoin-gateway";
const BEARER_PROTOCOL: &str = "bearer.";

/// The key may not call this method.
pub const UNAUTHORIZED: i64 = -32001;
/// The key's rate is exceeded.
pub const RATE_LIMITED: i64 = -32002;
/// The node is unreachable.
pub const UPSTREAM_UNAVAILABLE: i64 = -32003;

fn token_from(headers: &HeaderMap) -> (Option<String>, bool) {
    if let Some(a) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        return (Some(a.to_owned()), false);
    }
    let token = headers
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .find_map(|p| p.trim().strip_prefix(BEARER_PROTOCOL).map(str::to_owned));
    (token.map(|t| format!("Bearer {t}")), true)
}

async fn node_route<N: Backend>(
    State(gw): State<Arc<Gateway<N>>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    upgrade(gw, headers, ws, None)
}

async fn wallet_route<N: Backend>(
    State(gw): State<Arc<Gateway<N>>>,
    Path(wallet): Path<String>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    upgrade(gw, headers, ws, Some(wallet))
}

fn upgrade<N: Backend>(
    gw: Arc<Gateway<N>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    wallet: Option<String>,
) -> Response {
    let (auth, via_protocol) = token_from(&headers);
    let key = match gw.keys.authenticate(auth.as_deref()) {
        Ok(k) => k,
        Err(e) => return e.into_response(),
    };
    let ws = if via_protocol {
        ws.protocols([PROTOCOL])
    } else {
        ws
    };
    let limit = gw.settings.ws.max_message_bytes;
    ws.max_message_size(limit)
        .max_frame_size(limit)
        .on_upgrade(move |socket| Conn::run(gw, key, wallet, socket))
}

/// One connection's state.
struct Conn<N> {
    gw: Arc<Gateway<N>>,
    key: Arc<Key>,
    wallet: Option<String>,
    out: mpsc::Sender<Message>,
    subs: Mutex<HashMap<String, JoinHandle<()>>>,
    next_sub: Mutex<u64>,
}

impl<N: Backend> Conn<N> {
    async fn run(gw: Arc<Gateway<N>>, key: Arc<Key>, wallet: Option<String>, socket: WebSocket) {
        let (mut sink, mut stream) = socket.split();
        let (out, mut rx) = mpsc::channel::<Message>(256);
        let writer = tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                let close = matches!(m, Message::Close(_));
                if sink.send(m).await.is_err() || close {
                    break;
                }
            }
            let _ = sink.close().await;
        });
        let in_flight = Arc::new(Semaphore::new(gw.settings.ws.max_in_flight));
        let shutdown = gw.shutdown.clone();
        let conn = Arc::new(Conn {
            gw,
            key,
            wallet,
            out: out.clone(),
            subs: Mutex::default(),
            next_sub: Mutex::new(0),
        });
        loop {
            let msg = tokio::select! {
                m = stream.next() => m,
                _ = shutdown.cancelled() => {
                    let _ = out.send(close(close_code::AWAY, "shutting down")).await;
                    break;
                }
            };
            let text = match msg {
                Some(Ok(Message::Text(t))) => t,
                Some(Ok(Message::Binary(_))) => {
                    let reply = error(Value::Null, -32600, "send JSON-RPC as text frames");
                    let _ = out.send(text_msg(&reply)).await;
                    continue;
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => continue, // ping/pong: answered by the library
                Some(Err(e)) => {
                    // Oversize messages land here.
                    tracing::debug!(error = %e, "websocket read failed");
                    let _ = out.send(close(close_code::SIZE, "message too big")).await;
                    break;
                }
            };
            // Backpressure: stop reading while the connection's calls in
            // flight are at the limit.
            let Ok(permit) = in_flight.clone().acquire_owned().await else {
                break;
            };
            let conn = conn.clone();
            tokio::spawn(async move {
                if let Some(reply) = conn.frame(&text).await {
                    let _ = conn.out.send(text_msg(&reply)).await;
                }
                drop(permit);
            });
        }
        for (_, h) in conn.subs.lock().unwrap_or_else(|e| e.into_inner()).drain() {
            h.abort();
        }
        drop(out);
        drop(conn);
        let _ = writer.await;
    }

    /// One frame: a call or a batch. `None` when nothing is owed (only
    /// notifications).
    async fn frame(self: &Arc<Self>, text: &str) -> Option<Value> {
        let v: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => return Some(error(Value::Null, -32700, &format!("parse error: {e}"))),
        };
        match v {
            Value::Array(items) if items.is_empty() => {
                Some(error(Value::Null, -32600, "empty batch"))
            }
            Value::Array(items) => {
                let mut replies = Vec::new();
                for item in items {
                    if let Some(r) = self.call(item).await {
                        replies.push(r);
                    }
                }
                (!replies.is_empty()).then_some(Value::Array(replies))
            }
            v => self.call(v).await,
        }
    }

    async fn call(self: &Arc<Self>, req: Value) -> Option<Value> {
        let Value::Object(mut req) = req else {
            return Some(error(Value::Null, -32600, "a call must be an object"));
        };
        // No id: a notification, owed no reply.
        let id = req.remove("id");
        let reply_id = id.clone().unwrap_or(Value::Null);
        let Some(Value::String(method)) = req.remove("method") else {
            return Some(error(reply_id, -32600, "missing method"));
        };
        let params = req.remove("params").unwrap_or_else(|| json!([]));
        if !(params.is_array() || params.is_object()) {
            return Some(error(reply_id, -32600, "params must be an array or object"));
        }
        let start = Instant::now();
        let result = match method.as_str() {
            "events.subscribe" => self.subscribe().await,
            "events.unsubscribe" => self.unsubscribe(&params),
            _ => self.pass_through(&method, params).await,
        };
        let api_err = match &result {
            Err(Fail::Gateway(e)) => Some(e.clone()),
            _ => None,
        };
        // Method names come from clients: only known ones become labels.
        let known = crate::scopes::method_area(&method).is_some() || method.starts_with("events.");
        let label = if known { method.as_str() } else { "unknown" };
        self.gw.observe(&self.key, label, start, api_err.as_ref());
        id.as_ref()?;
        Some(match result {
            Ok(v) => json!({ "jsonrpc": "2.0", "id": reply_id, "result": v }),
            Err(Fail::Core { code, message }) => error(reply_id, code, &message),
            Err(Fail::Gateway(e)) => {
                let code = match e.kind {
                    Kind::ResourceExhausted => RATE_LIMITED,
                    Kind::Unavailable | Kind::DeadlineExceeded => UPSTREAM_UNAVAILABLE,
                    Kind::PermissionDenied | Kind::Unauthenticated => UNAUTHORIZED,
                    Kind::InvalidArgument | Kind::NotFound => -32602,
                    _ => -32603,
                };
                error(reply_id, code, &e.message)
            }
        })
    }

    async fn pass_through(&self, method: &str, params: Value) -> Result<Value, Fail> {
        self.key
            .require_method(method, self.wallet.as_deref())
            .map_err(Fail::Gateway)?;
        let _slots = self.gw.call_slots(&self.key).await.map_err(Fail::Gateway)?;
        match self
            .gw
            .upstream
            .call(self.wallet.as_deref(), method, params)
            .await
        {
            Ok(v) => Ok(v),
            // Core's error, unchanged.
            Err(NodeAPIError::Rpc { code, message }) => Err(Fail::Core {
                code: code.code(),
                message,
            }),
            Err(NodeAPIError::Transport(TransportError::Connect(_) | TransportError::Timeout)) => {
                Err(Fail::Gateway(ApiError::unavailable("node unreachable")))
            }
            Err(e) => Err(Fail::Gateway(ApiError::from(e))),
        }
    }

    async fn subscribe(self: &Arc<Self>) -> Result<Value, Fail> {
        let mut sub = self.gw.subscribe(&self.key).await.map_err(Fail::Gateway)?;
        let id = {
            let mut n = self.next_sub.lock().unwrap_or_else(|e| e.into_inner());
            *n += 1;
            format!("sub-{n}")
        };
        let (out, sid) = (self.out.clone(), id.clone());
        // Registered before the task runs, so an unsubscribe can't miss it.
        let mut subs = self.subs.lock().unwrap_or_else(|e| e.into_inner());
        let task = tokio::spawn(async move {
            while let Some(ev) = sub.next().await {
                let note = json!({
                    "jsonrpc": "2.0",
                    "method": "events.event",
                    "params": { "subscription": sid, "event": ev },
                });
                if out.send(text_msg(&note)).await.is_err() {
                    return;
                }
            }
        });
        subs.insert(id.clone(), task);
        Ok(Value::String(id))
    }

    fn unsubscribe(&self, params: &Value) -> Result<Value, Fail> {
        let id = params
            .get(0)
            .or_else(|| params.get("subscription"))
            .and_then(Value::as_str)
            .ok_or_else(|| Fail::Gateway(ApiError::bad_request("expected [subscription id]")))?;
        match self
            .subs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id)
        {
            Some(h) => {
                h.abort();
                Ok(Value::Bool(true))
            }
            None => Err(Fail::Gateway(ApiError::bad_request(format!(
                "no subscription {id:?}"
            )))),
        }
    }
}

enum Fail {
    /// Core's own error: passed on unchanged.
    Core { code: i64, message: String },
    /// The gateway's.
    Gateway(ApiError),
}

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn text_msg(v: &Value) -> Message {
    Message::Text(Utf8Bytes::from(v.to_string()))
}

fn close(code: u16, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: Utf8Bytes::from_static(reason),
    }))
}

/// One text message through the JSON-RPC handling, as a connection of `key`
/// on `wallet`'s route would handle it: the reply owed, if any. For the fuzz
/// targets; not part of the API. Subscriptions it opens end on return.
#[doc(hidden)]
pub async fn handle_text<N: Backend>(
    gw: Arc<Gateway<N>>,
    key: Arc<Key>,
    wallet: Option<String>,
    text: &str,
) -> Option<Value> {
    let (out, _rx) = mpsc::channel(256);
    let conn = Arc::new(Conn {
        gw,
        key,
        wallet,
        out,
        subs: Mutex::default(),
        next_sub: Mutex::new(0),
    });
    let reply = conn.frame(text).await;
    for (_, h) in conn.subs.lock().unwrap_or_else(|e| e.into_inner()).drain() {
        h.abort();
    }
    reply
}

/// The WebSocket routes.
pub fn router<N: Backend>(gw: Arc<Gateway<N>>) -> Router {
    Router::new()
        .route("/ws", get(node_route::<N>))
        .route("/ws/wallet/{name}", get(wallet_route::<N>))
        .with_state(gw)
}
