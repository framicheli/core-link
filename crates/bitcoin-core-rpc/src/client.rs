use crate::auth::{Credentials, UserPass};
use crate::methods::{RetryPolicy, method_class};
#[cfg(feature = "tracing")]
use crate::trace;
use crate::{Auth, Feature, Version};
use bitcoin_node_api::{ApiResult, NodeAPIError, RpcErrorCode, Template, TransportError};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

const TIMEOUT: Duration = Duration::from_secs(30);
/// For methods that block or scan, and `getblocktemplate` longpolls.
const LONG_TIMEOUT: Duration = Duration::from_secs(600);
const LONG_METHODS: &[&str] = &[
    "waitforblock",
    "waitforblockheight",
    "waitfornewblock",
    "scanblocks",
    "scantxoutset",
    "gettxoutsetinfo",
    "dumptxoutset",
    "loadtxoutset",
    "importmempool",
    "verifychain",
    "generatetoaddress",
    "generatetodescriptor",
    "generateblock",
    "rescanblockchain",
    "importdescriptors",
    "restorewallet",
    "loadwallet",
    "migratewallet",
    "importprunedfunds",
];
const MIN_MAJOR: u32 = 29;
const BATCH_CHUNK: usize = 100;

pub(crate) type Longpoll = watch::Receiver<Option<ApiResult<Template>>>;

/// A Bitcoin Core JSON-RPC client. Cheap to clone; clones share the
/// connection pool and the credentials.
#[derive(Clone, Debug)]
pub struct Client {
    http: reqwest::Client,
    url: String,
    creds: Arc<Credentials>,
    version: Version,
    timeout: Duration,
    long_timeout: Duration,
    retry: Option<RetryPolicy>,
    batch_chunk: usize,
    next_id: Arc<AtomicU64>,
    /// One in-flight GBT longpoll per `longpollid`. Held only to look up or
    /// insert; the wait happens on the `watch`.
    pub(crate) longpolls: Arc<Mutex<HashMap<String, Longpoll>>>,
}

/// Settings for a [`Client`], from [`Client::builder`].
///
/// ```no_run
/// # async fn example() -> bitcoin_node_api::ApiResult<()> {
/// use bitcoin_core_rpc::{Auth, Client, RetryPolicy};
/// use std::time::Duration;
///
/// let client = Client::builder("http://127.0.0.1:8332")
///     .auth(Auth::cookie("/home/me/.bitcoin/.cookie").expect("cookie"))
///     .timeout(Duration::from_secs(10))
///     .retry(RetryPolicy::default())
///     .connect()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub struct Builder {
    url: String,
    auth: Auth,
    timeout: Duration,
    long_timeout: Duration,
    http: Option<reqwest::Client>,
    retry: Option<RetryPolicy>,
    batch_chunk: usize,
}

impl Builder {
    /// Default: `Auth::None`.
    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = auth;
        self
    }

    /// Per-request timeout. Default 30 s.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Timeout for methods that block or scan (`waitfornewblock`,
    /// `scantxoutset`, `rescanblockchain`, ...) and template longpolls.
    /// Default 600 s.
    pub fn long_timeout(mut self, timeout: Duration) -> Self {
        self.long_timeout = timeout;
        self
    }

    /// The HTTP client to send with, for a proxy, TLS roots or pool limits.
    /// It keeps its own settings; the per-request timeouts above still apply.
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Retry failed requests where that is safe; see [`RetryPolicy`].
    /// Default: no retries.
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = Some(policy);
        self
    }

    /// Items per HTTP request in [`Client::batch`]. Default 100, at least 1.
    pub fn batch_chunk(mut self, n: usize) -> Self {
        self.batch_chunk = n.max(1);
        self
    }

    /// Runs `getnetworkinfo` to learn the version. Fails fast on bad URL or
    /// auth, and on Core older than 29.
    pub async fn connect(self) -> ApiResult<Client> {
        #[derive(Deserialize)]
        struct NetworkInfo {
            version: u32,
        }
        let mut client = self.build(Version(0));
        let info: NetworkInfo = client.call("getnetworkinfo", json!([])).await?;
        client.version = Version(info.version);
        if client.version.major() < MIN_MAJOR {
            return Err(NodeAPIError::Unsupported(format!(
                "Core {} is below the supported minimum {MIN_MAJOR}",
                client.version.major()
            )));
        }
        Ok(client)
    }

    /// No I/O: the caller asserts the node's version.
    pub fn build(self, version: Version) -> Client {
        Client {
            http: self.http.unwrap_or_default(),
            url: self.url,
            creds: Arc::new(Credentials::new(self.auth)),
            version,
            timeout: self.timeout,
            long_timeout: self.long_timeout,
            retry: self.retry,
            batch_chunk: self.batch_chunk,
            next_id: Arc::default(),
            longpolls: Arc::default(),
        }
    }
}

#[derive(Deserialize)]
struct RpcResponse {
    #[serde(default)]
    result: Value,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct BatchReply {
    #[serde(default)]
    id: Value,
    #[serde(default)]
    result: Value,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn into_api(self) -> NodeAPIError {
        NodeAPIError::Rpc {
            code: self.code.into(),
            message: self.message,
        }
    }
}

impl Client {
    /// Settings beyond URL and auth: timeouts, HTTP client, retries.
    pub fn builder(url: &str) -> Builder {
        Builder {
            url: url.to_owned(),
            auth: Auth::None,
            timeout: TIMEOUT,
            long_timeout: LONG_TIMEOUT,
            http: None,
            retry: None,
            batch_chunk: BATCH_CHUNK,
        }
    }

    /// Connects and runs `getnetworkinfo` to learn the version. Fails fast on bad URL/auth.
    /// Shorthand for `Client::builder(url).auth(auth).connect()`.
    pub async fn connect(url: &str, auth: Auth) -> ApiResult<Client> {
        Client::builder(url).auth(auth).connect().await
    }

    /// Skip detection; caller asserts the version. For tests and fixed deployments.
    /// Shorthand for `Client::builder(url).auth(auth).build(version)`.
    pub fn with_version(url: &str, auth: Auth, version: Version) -> Client {
        Client::builder(url).auth(auth).build(version)
    }

    /// Core version, detected at connect or given to `with_version`.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Whether the node's version has `f`.
    pub fn supports(&self, f: Feature) -> bool {
        self.version.supports(f)
    }

    /// The credentials this client was built with.
    pub fn auth(&self) -> &Auth {
        self.creds.auth()
    }

    /// Escape hatch for RPCs added after this crate.
    pub async fn raw_call<T: DeserializeOwned>(&self, method: &str, params: Value) -> ApiResult<T> {
        self.call(method, params).await
    }

    /// Calls `method` once per entry of `params`, in JSON-RPC batches of
    /// `batch_chunk` (default 100) items per HTTP request. Results are in
    /// input order. The outer `Err` means a whole request failed; an inner
    /// one is that item's own error (`-5` for one unknown hash).
    pub async fn batch<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> ApiResult<Vec<ApiResult<T>>> {
        let timeout = self.timeout_for(method);
        let mut out = Vec::with_capacity(params.len());
        for chunk in params.chunks(self.batch_chunk) {
            let first = self
                .next_id
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            let body: Vec<Value> = chunk
                .iter()
                .zip(first..)
                .map(|(p, id)| request_body(id, method, p.clone()))
                .collect();
            let body = Value::Array(body);
            let replies = self.retrying(method, true, || {
                self.batch_once(method, &body, timeout, first, chunk.len())
            });
            #[cfg(feature = "tracing")]
            let replies = trace::in_span(
                tracing::debug_span!(
                    "rpc_batch",
                    method,
                    first_id = first,
                    items = chunk.len(),
                    duration_ms = tracing::field::Empty,
                    error = tracing::field::Empty,
                ),
                replies,
            );
            let replies = replies.await?;
            out.extend(replies.into_iter().map(|r| {
                r.and_then(|v| {
                    serde_json::from_value(v)
                        .map_err(|e| NodeAPIError::Decode(format!("{method}: {e}")))
                })
            }));
        }
        Ok(out)
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> ApiResult<T> {
        self.call_at(&self.url, None, method, params).await
    }

    /// `call`, posted to `url` (`wallet`'s endpoint) instead of the node's.
    pub(crate) async fn call_at<T: DeserializeOwned>(
        &self,
        url: &str,
        wallet: Option<&str>,
        method: &str,
        params: Value,
    ) -> ApiResult<T> {
        let timeout = self.timeout_for(method);
        self.request(url, wallet, method, params, timeout, true)
            .await
    }

    /// A template longpoll: the long timeout, and never retried (the caller
    /// shares one in-flight request per `longpollid`).
    pub(crate) async fn call_long<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> ApiResult<T> {
        self.request(&self.url, None, method, params, self.long_timeout, false)
            .await
    }

    fn timeout_for(&self, method: &str) -> Duration {
        if LONG_METHODS.contains(&method) {
            self.long_timeout
        } else {
            self.timeout
        }
    }

    async fn request<T: DeserializeOwned>(
        &self,
        url: &str,
        wallet: Option<&str>,
        method: &str,
        params: Value,
        timeout: Duration,
        retry: bool,
    ) -> ApiResult<T> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = request_body(id, method, params);
        let call = async {
            let result = self
                .retrying(method, retry, || {
                    self.call_once(url, method, &body, timeout)
                })
                .await?;
            serde_json::from_value(result)
                .map_err(|e| NodeAPIError::Decode(format!("{method}: {e}")))
        };
        #[cfg(feature = "tracing")]
        let call = trace::in_span(
            tracing::debug_span!(
                "rpc",
                method,
                id,
                wallet,
                duration_ms = tracing::field::Empty,
                error = tracing::field::Empty,
            ),
            call,
        );
        #[cfg(not(feature = "tracing"))]
        let _ = wallet;
        call.await
    }

    /// Runs `attempt` until it succeeds, the policy says the error isn't
    /// safe to retry for `method`, or attempts run out.
    async fn retrying<R, F, Fut>(&self, method: &str, retry: bool, attempt: F) -> ApiResult<R>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = ApiResult<R>>,
    {
        let Some(policy) = self.retry.filter(|_| retry) else {
            return attempt().await;
        };
        let class = method_class(method);
        let mut retries = 0;
        loop {
            match attempt().await {
                Err(e)
                    if retries + 1 < policy.max_attempts
                        && RetryPolicy::should_retry(class, &e) =>
                {
                    let backoff = policy.backoff(retries);
                    trace_event!(
                        attempt = retries + 1,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %crate::trace::kind(&e),
                        "retrying"
                    );
                    #[cfg(not(feature = "tracing"))]
                    let _ = &e;
                    tokio::time::sleep(backoff).await;
                    retries += 1;
                }
                r => return r,
            }
        }
    }

    /// One request; the reply's `result`.
    async fn call_once(
        &self,
        url: &str,
        method: &str,
        body: &Value,
        timeout: Duration,
    ) -> ApiResult<Value> {
        let (status, bytes) = self.post(url, body, timeout).await?;
        // Core speaks JSON-RPC 2.0 (28+): errors come back with HTTP 200, so parse regardless of status.
        let resp: RpcResponse = match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(_) if !status.is_success() => {
                return Err(TransportError::Http(status.as_u16()).into());
            }
            Err(e) => return Err(NodeAPIError::Decode(format!("{method}: {e}"))),
        };
        match resp.error {
            Some(e) => Err(e.into_api()),
            None => Ok(resp.result),
        }
    }

    /// One batch request of `n` items with ids `first..first + n`; replies
    /// matched by id, since JSON-RPC lets the server answer in any order.
    async fn batch_once(
        &self,
        method: &str,
        body: &Value,
        timeout: Duration,
        first: u64,
        n: usize,
    ) -> ApiResult<Vec<ApiResult<Value>>> {
        let (status, bytes) = self.post(&self.url, body, timeout).await?;
        let replies: Vec<BatchReply> = match serde_json::from_slice(&bytes) {
            Ok(r) => r,
            Err(e) => {
                // One error object instead of an array: the batch as a whole was refused.
                return Err(match serde_json::from_slice::<RpcResponse>(&bytes) {
                    Ok(RpcResponse { error: Some(e), .. }) => e.into_api(),
                    _ if !status.is_success() => TransportError::Http(status.as_u16()).into(),
                    _ => NodeAPIError::Decode(format!("{method} batch: {e}")),
                });
            }
        };
        let mut slots: Vec<Option<ApiResult<Value>>> = (0..n).map(|_| None).collect();
        for reply in replies {
            let slot = reply
                .id
                .as_u64()
                .and_then(|id| id.checked_sub(first))
                .and_then(|i| slots.get_mut(usize::try_from(i).ok()?));
            if let Some(slot) = slot {
                *slot = Some(match reply.error {
                    Some(e) => Err(e.into_api()),
                    None => Ok(reply.result),
                });
            }
        }
        let results: Vec<_> = slots
            .into_iter()
            .map(|s| {
                s.unwrap_or_else(|| {
                    Err(NodeAPIError::Decode(format!("{method}: no reply in batch")))
                })
            })
            .collect();
        // A node still starting answers every item with -28: retry the batch like one call.
        if let Some(Err(e)) = results.first()
            && results.iter().all(|r| {
                matches!(
                    r,
                    Err(NodeAPIError::Rpc {
                        code: RpcErrorCode::InWarmup,
                        ..
                    })
                )
            })
        {
            return Err(e.clone());
        }
        Ok(results)
    }

    /// POSTs `body` with the current credentials. On a 401/403 with a cookie,
    /// re-reads it and, if it changed, sends once more.
    async fn post(
        &self,
        url: &str,
        body: &Value,
        timeout: Duration,
    ) -> ApiResult<(reqwest::StatusCode, bytes::Bytes)> {
        let mut sent: Option<UserPass> = self.creds.current();
        let mut refreshed = false;
        loop {
            let mut req = self.http.post(url).json(body).timeout(timeout);
            if let Some((u, p)) = &sent {
                req = req.basic_auth(u, Some(p));
            }
            let resp = req.send().await.map_err(transport)?;
            let status = resp.status();
            if status.as_u16() == 401 || status.as_u16() == 403 {
                if !refreshed && let Some(fresh) = self.creds.refresh(&sent) {
                    trace_event!("cookie changed after a {}; sending again", status.as_u16());
                    sent = Some(fresh);
                    refreshed = true;
                    continue;
                }
                trace_event!(status = status.as_u16(), "authentication failed");
                return Err(TransportError::Auth.into());
            }
            let bytes = resp.bytes().await.map_err(transport)?;
            return Ok((status, bytes));
        }
    }
}

/// A JSON-RPC 2.0 request. An omitted optional arg is left out, not sent as
/// `null`: Core 29–30 reject `null` for some (`help`), and nulls are only
/// kept mid-list.
fn request_body(id: u64, method: &str, mut params: Value) -> Value {
    if let Value::Array(a) = &mut params {
        while a.last().is_some_and(Value::is_null) {
            a.pop();
        }
    }
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

fn transport(e: reqwest::Error) -> NodeAPIError {
    if e.is_timeout() {
        TransportError::Timeout
    } else if e.is_connect() {
        TransportError::Connect(e.to_string())
    } else {
        TransportError::Other(e.to_string())
    }
    .into()
}

/// Turns `into_model()` output into an `ApiResult`, whether the conversion is
/// fallible (`Result<T, E>`) or not (`T`).
pub(crate) trait IntoApi<T> {
    fn into_api(self, method: &str) -> ApiResult<T>;
}

impl<T> IntoApi<T> for T {
    fn into_api(self, _: &str) -> ApiResult<T> {
        Ok(self)
    }
}

impl<T, E: std::fmt::Display> IntoApi<T> for Result<T, E> {
    fn into_api(self, method: &str) -> ApiResult<T> {
        self.map_err(|e| NodeAPIError::Decode(format!("{method}: {e}")))
    }
}

/// Calls `$method`, parses the reply with the `corepc_types` struct for the
/// connected Core major and converts it to its `model` type. The only place
/// the supported range (29–31) is spelled out; `connect` refuses older Core.
macro_rules! versioned {
    ($self:ident, $method:literal, $params:expr, $ty:ident) => {{
        use $crate::client::IntoApi;
        let params = $params;
        match $self.version().major() {
            29 => $self
                .call::<corepc_types::v29::$ty>($method, params)
                .await?
                .into_model()
                .into_api($method),
            30 => $self
                .call::<corepc_types::v30::$ty>($method, params)
                .await?
                .into_model()
                .into_api($method),
            _ => $self
                .call::<corepc_types::v31::$ty>($method, params)
                .await?
                .into_model()
                .into_api($method),
        }
    }};
}
