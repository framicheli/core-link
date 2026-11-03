//! A tiny fake bitcoind: HTTP/1.1 JSON-RPC over a `TcpListener`.
//! A handler answers first; otherwise `tests/fixtures/<method>.json` is the result.
//! Paths are kept: wallet calls go to `/wallet/<name>`.
#![allow(dead_code)]
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct Reply {
    pub status: u16,
    pub body: String,
    pub delay: Duration,
}

pub fn ok(result: Value) -> Option<Reply> {
    Some(Reply {
        status: 200,
        body: json!({ "jsonrpc": "2.0", "result": result, "id": 0 }).to_string(),
        delay: Duration::ZERO,
    })
}

pub fn rpc_err(code: i64, message: &str) -> Option<Reply> {
    Some(Reply {
        status: 200,
        body: json!({ "jsonrpc": "2.0", "error": { "code": code, "message": message }, "id": 0 })
            .to_string(),
        delay: Duration::ZERO,
    })
}

pub fn fixture(method: &str) -> Option<Value> {
    let path = format!(
        "{}/tests/fixtures/{method}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let s = std::fs::read_to_string(path).ok()?;
    Some(serde_json::from_str(&s).expect("fixture is JSON"))
}

/// One request: path, method, params.
type Call = (String, String, Value);

/// One JSON-RPC call as the handler sees it. A batch is one of these per item.
pub struct Request {
    pub path: String,
    pub method: String,
    pub params: Value,
    /// Lowercased header names.
    pub headers: Vec<(String, String)>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

pub struct Fake {
    pub url: String,
    calls: Arc<Mutex<Vec<Call>>>,
    requests: Arc<AtomicUsize>,
}

impl Fake {
    /// Params of every request for `method`, in arrival order.
    pub fn calls(&self, method: &str) -> Vec<Value> {
        self.calls_at(method).into_iter().map(|(_, p)| p).collect()
    }

    /// Path and params of every request for `method`, in arrival order.
    pub fn calls_at(&self, method: &str) -> Vec<(String, Value)> {
        let calls = self.calls.lock().unwrap();
        calls
            .iter()
            .filter(|(_, m, _)| m == method)
            .map(|(path, _, p)| (path.clone(), p.clone()))
            .collect()
    }

    /// HTTP requests received; a batch counts once.
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

/// Serves fixtures only.
pub async fn fake() -> Fake {
    fake_with(|_, _| None).await
}

pub async fn fake_with(
    handler: impl Fn(&str, &Value) -> Option<Reply> + Send + Sync + 'static,
) -> Fake {
    fake_at(move |_, method, params| handler(method, params)).await
}

/// `fake_with`, with the request path as the handler's first argument.
pub async fn fake_at(
    handler: impl Fn(&str, &str, &Value) -> Option<Reply> + Send + Sync + 'static,
) -> Fake {
    fake_req(move |r| handler(&r.path, &r.method, &r.params)).await
}

/// The general fake: the handler sees headers too. A JSON array body is a
/// batch: each item is handled on its own and the replies come back in
/// reverse order (JSON-RPC allows any), with the item's `id`.
pub async fn fake_req(handler: impl Fn(&Request) -> Option<Reply> + Send + Sync + 'static) -> Fake {
    fake_on(TcpListener::bind("127.0.0.1:0").await.unwrap(), handler)
}

/// `fake_req` on a listener the caller bound.
pub fn fake_on(
    listener: TcpListener,
    handler: impl Fn(&Request) -> Option<Reply> + Send + Sync + 'static,
) -> Fake {
    let url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let requests = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(handler);
    let (log, count) = (calls.clone(), requests.clone());
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let (handler, log, count) = (handler.clone(), log.clone(), count.clone());
            tokio::spawn(async move {
                let Some((path, headers, body)) = read_request(&mut sock).await else {
                    return;
                };
                count.fetch_add(1, Ordering::SeqCst);
                let answer = |item: &Value| {
                    let req = Request {
                        path: path.clone(),
                        method: item["method"].as_str().unwrap().to_owned(),
                        params: item["params"].clone(),
                        headers: headers.clone(),
                    };
                    log.lock().unwrap().push((
                        path.clone(),
                        req.method.clone(),
                        req.params.clone(),
                    ));
                    handler(&req)
                        .or_else(|| fixture(&req.method).and_then(ok))
                        .or_else(|| rpc_err(-32601, "Method not found"))
                        .unwrap()
                };
                let reply = match &body {
                    Value::Array(items) => {
                        let mut delay = Duration::ZERO;
                        let mut out = Vec::new();
                        for item in items.iter().rev() {
                            let r = answer(item);
                            delay = delay.max(r.delay);
                            let mut v: Value = serde_json::from_str(&r.body).unwrap();
                            v["id"] = item["id"].clone();
                            out.push(v);
                        }
                        Reply {
                            status: 200,
                            body: Value::Array(out).to_string(),
                            delay,
                        }
                    }
                    item => answer(item),
                };
                tokio::time::sleep(reply.delay).await;
                let resp = format!(
                    "HTTP/1.1 {} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    reply.status,
                    reply.body.len(),
                    reply.body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    Fake {
        url,
        calls,
        requests,
    }
}

/// The request's path, lowercased headers and JSON body.
async fn read_request(sock: &mut TcpStream) -> Option<(String, Vec<(String, String)>, Value)> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0u8; 4096];
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        let s = String::from_utf8_lossy(&buf);
        if let Some(i) = s.find("\r\n\r\n") {
            let headers: Vec<(String, String)> = s[..i]
                .lines()
                .skip(1)
                .filter_map(|l| {
                    let (n, v) = l.split_once(':')?;
                    Some((n.trim().to_ascii_lowercase(), v.trim().to_owned()))
                })
                .collect();
            let len: usize = headers
                .iter()
                .find(|(n, _)| n == "content-length")?
                .1
                .parse()
                .ok()?;
            if buf.len() >= i + 4 + len {
                // `POST /wallet/w HTTP/1.1`
                let path = s.split(' ').nth(1)?.to_owned();
                let body = serde_json::from_slice(&buf[i + 4..i + 4 + len]).ok()?;
                return Some((path, headers, body));
            }
        }
    }
}

/// Every `.rs` file under `src/`, subdirectories (`wallet/`) included.
pub fn source_files() -> Vec<std::path::PathBuf> {
    let mut dirs = vec![std::path::PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src"
    ))];
    let mut files = Vec::new();
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                files.push(path);
            }
        }
    }
    files
}

/// One request/reply pair from a recorded session.
pub type Exchange = Value;

/// Forwards every request to `upstream` (a real node) and logs
/// `{method, params, reply}` in order, `reply` being Core's `{result, error}`.
/// Requests to a path other than `/` (wallets) also log `path`.
pub async fn recorder(
    upstream: String,
    user: String,
    pass: String,
) -> (String, Arc<Mutex<Vec<Exchange>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let log = Arc::new(Mutex::new(Vec::new()));
    let http = reqwest::Client::new();
    let session = log.clone();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let (http, log) = (http.clone(), session.clone());
            let (upstream, user, pass) = (upstream.clone(), user.clone(), pass.clone());
            tokio::spawn(async move {
                let Some((path, _, req)) = read_request(&mut sock).await else {
                    return;
                };
                // Dropping the connection reads as a transport error, e.g. while the node starts.
                let Ok(resp) = http
                    .post(format!("{upstream}{path}"))
                    .basic_auth(user, Some(pass))
                    .json(&req)
                    .timeout(Duration::from_secs(600))
                    .send()
                    .await
                else {
                    return;
                };
                let status = resp.status().as_u16();
                let mut reply: Value = resp.json().await.unwrap();
                let reply =
                    json!({ "result": reply["result"].take(), "error": reply["error"].take() });
                let mut exchange = json!({
                    "method": req["method"], "params": req["params"], "reply": reply,
                });
                if path != "/" {
                    exchange["path"] = json!(path);
                }
                log.lock().unwrap().push(exchange);
                let body = json!({ "jsonrpc": "2.0", "result": reply["result"], "error": reply["error"], "id": req["id"] }).to_string();
                let resp = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    (url, log)
}

/// Serves a recorded session: each request gets the next unused reply
/// recorded for the same path, method and params; anything else is an error.
pub async fn replay(session: Vec<Exchange>) -> Fake {
    let session = Mutex::new(session);
    fake_at(move |path, method, params| {
        let mut s = session.lock().unwrap();
        let Some(i) = s.iter().position(|e| {
            e["path"].as_str().unwrap_or("/") == path
                && e["method"] == method
                && e["params"] == *params
        }) else {
            return rpc_err(
                -32601,
                &format!("not in session: {path} {method} {params}"),
            );
        };
        let reply = s.remove(i)["reply"].take();
        Some(Reply {
            status: 200,
            body: json!({ "jsonrpc": "2.0", "result": reply["result"], "error": reply["error"], "id": 0 })
                .to_string(),
            delay: Duration::ZERO,
        })
    })
    .await
}
