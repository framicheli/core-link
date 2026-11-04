//! REST under `/v1`: hand-written routes over the same [`api`](crate::api)
//! handlers as gRPC.
//!
//! JSON by default, in the api crate's `serde` shape: hashes as hex in
//! display order, transactions as consensus hex, amounts in satoshis.
//! `Accept: application/octet-stream` gets consensus bytes for blocks,
//! headers and transactions. Responses that can never change are cacheable
//! for good, with an `ETag`.
use crate::Gateway;
use crate::auth::Key;
use crate::error::{ApiError, Kind};
use crate::node::Backend;
use crate::scopes::Scope;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, FromRequestParts, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header, request::Parts};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use bitcoin::consensus::encode::{deserialize_partial, serialize, serialize_hex};
use bitcoin::{BlockHash, OutPoint, Transaction, Txid};
use bitcoin_node_api::{FeeMode, FeeTarget};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};

const OCTET: &str = "application/octet-stream";
/// Packages can be large: 25 txs of up to 100 kvB each, hex-encoded.
const BODY_LIMIT: usize = 8 << 20;

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.kind.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = json!({"error": {
            "type": self.source,
            "code": self.code,
            "core_code": self.core_code,
            "message": self.message,
        }});
        let mut r = (status, Json(body)).into_response();
        if self.kind == Kind::Unauthenticated {
            r.headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        r
    }
}

/// The caller's key, from `Authorization: Bearer <token>`.
pub struct Auth(pub Arc<Key>);

impl<N: Backend> FromRequestParts<Arc<Gateway<N>>> for Auth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        gw: &Arc<Gateway<N>>,
    ) -> Result<Self, Self::Rejection> {
        let h = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        gw.keys.authenticate(h).map(Auth)
    }
}

/// One query or path parameter, for the OpenAPI document.
#[derive(Debug, Clone, Copy)]
pub struct Param {
    /// Its name.
    pub name: &'static str,
    /// `path` or `query`.
    pub location: &'static str,
    /// `string`, `integer` or `boolean`.
    pub schema: &'static str,
    /// Shown in the docs.
    pub description: &'static str,
}

/// One route, as documented and as tested.
#[derive(Debug, Clone, Copy)]
pub struct Route {
    /// `GET` or `POST`.
    pub method: &'static str,
    /// The axum and OpenAPI path.
    pub path: &'static str,
    /// The gRPC method (`Service/Method`) it mirrors.
    pub grpc: &'static str,
    /// The scope it needs.
    pub scope: &'static str,
    /// What it does.
    pub summary: &'static str,
    /// Its parameters.
    pub params: &'static [Param],
    /// Serves consensus bytes with `Accept: application/octet-stream`.
    pub binary: bool,
}

const fn path(name: &'static str, description: &'static str) -> Param {
    Param {
        name,
        location: "path",
        schema: "string",
        description,
    }
}

const HASH: Param = path("hash", "Block hash, hex in display order");
const TXID: Param = path("txid", "Txid, hex in display order");

/// Every REST route. A test checks each gRPC method has one.
pub const ROUTES: &[Route] = &[
    Route {
        method: "GET",
        path: "/v1/chain/tip",
        grpc: "Chain/Tip",
        scope: "chain:read",
        summary: "The best block: {height, hash}",
        params: &[],
        binary: false,
    },
    Route {
        method: "GET",
        path: "/v1/chain/heights/{height}",
        grpc: "Chain/BlockHash",
        scope: "chain:read",
        summary: "Hash of the active-chain block at a height: {hash}",
        params: &[Param {
            name: "height",
            location: "path",
            schema: "integer",
            description: "Block height; genesis is 0",
        }],
        binary: false,
    },
    Route {
        method: "GET",
        path: "/v1/blocks/{hash}",
        grpc: "Chain/Block",
        scope: "chain:read",
        summary: "A block: {hash, hex}, or consensus bytes. Immutable",
        params: &[HASH],
        binary: true,
    },
    Route {
        method: "GET",
        path: "/v1/blocks/{hash}/header",
        grpc: "Chain/Header",
        scope: "chain:read",
        summary: "A block header: {hash, hex, height, chain_work}, or 80 bytes. Immutable",
        params: &[HASH],
        binary: true,
    },
    Route {
        method: "GET",
        path: "/v1/blocks/{hash}/filter",
        grpc: "Chain/BlockFilter",
        scope: "chain:read",
        summary: "A block's BIP 158 basic filter: {hash, filter}, or its bytes. Immutable; 501 without -blockfilterindex",
        params: &[HASH],
        binary: true,
    },
    Route {
        method: "GET",
        path: "/v1/tx/{txid}",
        grpc: "Chain/Transaction",
        scope: "chain:read",
        summary: "A transaction: {txid, hex}, or consensus bytes. Immutable with `block`",
        params: &[
            TXID,
            Param {
                name: "block",
                location: "query",
                schema: "string",
                description: "The block holding it; needed for confirmed txs unless Core runs -txindex",
            },
        ],
        binary: true,
    },
    Route {
        method: "GET",
        path: "/v1/utxos/{txid}/{vout}",
        grpc: "Chain/Utxo",
        scope: "chain:read",
        summary: "An unspent output; 404 if spent or unknown",
        params: &[
            TXID,
            Param {
                name: "vout",
                location: "path",
                schema: "integer",
                description: "Output index",
            },
            Param {
                name: "mempool",
                location: "query",
                schema: "boolean",
                description: "Also consider the mempool (default false)",
            },
        ],
        binary: false,
    },
    Route {
        method: "GET",
        path: "/v1/mempool/txids",
        grpc: "Mempool/Txids",
        scope: "mempool:read",
        summary: "Every mempool txid: {txids}",
        params: &[],
        binary: false,
    },
    Route {
        method: "GET",
        path: "/v1/mempool/snapshot",
        grpc: "Mempool/Snapshot",
        scope: "mempool:read",
        summary: "Mempool txids with the event sequence they match: {txids, sequence}",
        params: &[],
        binary: false,
    },
    Route {
        method: "GET",
        path: "/v1/mempool/{txid}",
        grpc: "Mempool/Entry",
        scope: "mempool:read",
        summary: "A mempool entry; 404 if not in the mempool",
        params: &[TXID],
        binary: false,
    },
    Route {
        method: "GET",
        path: "/v1/fees/estimate",
        grpc: "Fees/EstimateFee",
        scope: "fees:read",
        summary: "A fee rate in sat/kwu: {fee_rate}, null if the node has no estimate yet",
        params: &[
            Param {
                name: "blocks",
                location: "query",
                schema: "integer",
                description: "Confirmation target in blocks (default 6)",
            },
            Param {
                name: "mode",
                location: "query",
                schema: "string",
                description: "economical (default) or conservative",
            },
        ],
        binary: false,
    },
    Route {
        method: "POST",
        path: "/v1/tx",
        grpc: "Broadcast/Broadcast",
        scope: "broadcast",
        summary: "Broadcasts a transaction, {hex} or consensus bytes: {txid}",
        params: &[],
        binary: true,
    },
    Route {
        method: "POST",
        path: "/v1/tx/test",
        grpc: "Broadcast/TestAccept",
        scope: "broadcast",
        summary: "Checks transactions without submitting them, {txs: [hex]} or concatenated consensus bytes: {results}",
        params: &[],
        binary: true,
    },
    Route {
        method: "POST",
        path: "/v1/packages",
        grpc: "Broadcast/BroadcastPackage",
        scope: "broadcast",
        summary: "Submits a package, {txs: [hex]} or concatenated consensus bytes: {results}",
        params: &[],
        binary: true,
    },
    Route {
        method: "GET",
        path: "/v1/events",
        grpc: "Events/Subscribe",
        scope: "events",
        summary: "Server-Sent Events, one NodeEvent JSON per data line, lagged included",
        params: &[],
        binary: false,
    },
];

fn parse<T: FromStr>(s: &str, what: &str) -> Result<T, ApiError> {
    s.parse()
        .map_err(|_| ApiError::bad_request(format!("bad {what}: {s:?}")))
}

fn wants_binary(h: &HeaderMap) -> bool {
    h.get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains(OCTET))
}

fn octet(bytes: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, OCTET)], bytes).into_response()
}

fn no_store(mut r: Response) -> Response {
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// Each representation has its own tag, so a cache never swaps them.
fn etag(id: &str, binary: bool) -> String {
    if binary {
        format!("\"{id}.bin\"")
    } else {
        format!("\"{id}\"")
    }
}

fn cache_headers(r: &mut Response, etag: &str, public: bool) {
    let cc = if public {
        "public, max-age=31536000, immutable"
    } else {
        "private, max-age=31536000, immutable"
    };
    let h = r.headers_mut();
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cc));
    if let Ok(v) = HeaderValue::from_str(etag) {
        h.insert(header::ETAG, v);
    }
    h.insert(header::VARY, HeaderValue::from_static("Accept"));
}

fn not_modified(h: &HeaderMap, etag: &str) -> bool {
    h.get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|t| t.trim() == etag || t.trim() == "*"))
}

/// An immutable resource: `304` if the client has it (scope still
/// checked, the node not asked), else `fetch`'s response with cache headers.
async fn immutable<N: Backend>(
    gw: &Gateway<N>,
    key: &Key,
    method: &'static str,
    headers: &HeaderMap,
    tag: String,
    fetch: impl Future<Output = Result<Response, ApiError>>,
) -> Result<Response, ApiError> {
    let public = gw.settings.http.public_cache;
    if not_modified(headers, &tag) {
        gw.authorize(key, method, Scope::ChainRead)?;
        let mut r = StatusCode::NOT_MODIFIED.into_response();
        cache_headers(&mut r, &tag, public);
        return Ok(r);
    }
    let mut r = fetch.await?;
    cache_headers(&mut r, &tag, public);
    Ok(r)
}

type Gw<N> = State<Arc<Gateway<N>>>;

async fn tip<N: Backend>(State(gw): Gw<N>, Auth(key): Auth) -> Result<Response, ApiError> {
    Ok(no_store(Json(gw.tip(&key).await?).into_response()))
}

async fn height<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    Path(height): Path<String>,
) -> Result<Response, ApiError> {
    let h = gw.block_hash(&key, parse(&height, "height")?).await?;
    Ok(no_store(Json(json!({ "hash": h })).into_response()))
}

async fn block<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let hash: BlockHash = parse(&hash, "block hash")?;
    let bin = wants_binary(&headers);
    let fetch = async {
        let b = gw.block(&key, hash).await?;
        Ok(if bin {
            octet(serialize(&b))
        } else {
            Json(json!({ "hash": hash, "hex": serialize_hex(&b) })).into_response()
        })
    };
    let tag = etag(&hash.to_string(), bin);
    immutable(&gw, &key, "Chain/Block", &headers, tag, fetch).await
}

async fn header<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let hash: BlockHash = parse(&hash, "block hash")?;
    let bin = wants_binary(&headers);
    let fetch = async {
        let i = gw.header(&key, hash).await?;
        Ok(if bin {
            octet(serialize(&i.header))
        } else {
            Json(json!({
                "hash": hash,
                "hex": serialize_hex(&i.header),
                "height": i.height,
                "chain_work": i.chain_work,
            }))
            .into_response()
        })
    };
    let tag = etag(&format!("{hash}.header"), bin);
    immutable(&gw, &key, "Chain/Header", &headers, tag, fetch).await
}

async fn filter<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let hash: BlockHash = parse(&hash, "block hash")?;
    let bin = wants_binary(&headers);
    let fetch = async {
        let f = gw.block_filter(&key, hash).await?;
        Ok(if bin {
            octet(f.content)
        } else {
            Json(json!({ "hash": hash, "filter": bitcoin::hex::DisplayHex::to_lower_hex_string(f.content.as_slice()) })).into_response()
        })
    };
    // A block's filter is fixed by the block and what it spends.
    let tag = etag(&format!("{hash}.filter"), bin);
    immutable(&gw, &key, "Chain/BlockFilter", &headers, tag, fetch).await
}

#[derive(Deserialize)]
struct TxQuery {
    block: Option<String>,
}

async fn transaction<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    Path(txid): Path<String>,
    Query(q): Query<TxQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let txid: Txid = parse(&txid, "txid")?;
    let block: Option<BlockHash> = q.block.as_deref().map(|b| parse(b, "block")).transpose()?;
    let bin = wants_binary(&headers);
    let fetch = async {
        let t = gw.transaction(&key, txid, block).await?;
        Ok(if bin {
            octet(serialize(&t))
        } else {
            Json(json!({ "txid": txid, "hex": serialize_hex(&t) })).into_response()
        })
    };
    match block {
        // In a named block a tx never changes; without one, Core looks in
        // the mempool first, so the answer can change.
        Some(b) => {
            let tag = etag(&format!("{txid}.{b}"), bin);
            immutable(&gw, &key, "Chain/Transaction", &headers, tag, fetch).await
        }
        None => Ok(no_store(fetch.await?)),
    }
}

async fn utxo<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    Path((txid, vout)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let op = OutPoint::new(parse(&txid, "txid")?, parse(&vout, "vout")?);
    let mempool = match q.get("mempool").map(String::as_str) {
        None | Some("false") | Some("0") => false,
        Some("true") | Some("1") | Some("") => true,
        Some(v) => return Err(ApiError::bad_request(format!("bad mempool: {v:?}"))),
    };
    match gw.utxo(&key, op, mempool).await? {
        Some(u) => Ok(no_store(Json(u).into_response())),
        None => Err(ApiError::not_found("no such unspent output")),
    }
}

async fn txids<N: Backend>(State(gw): Gw<N>, Auth(key): Auth) -> Result<Response, ApiError> {
    let ids = gw.txids(&key).await?;
    Ok(no_store(Json(json!({ "txids": ids })).into_response()))
}

async fn snapshot<N: Backend>(State(gw): Gw<N>, Auth(key): Auth) -> Result<Response, ApiError> {
    Ok(no_store(Json(gw.snapshot(&key).await?).into_response()))
}

async fn entry<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    Path(txid): Path<String>,
) -> Result<Response, ApiError> {
    match gw.entry(&key, parse(&txid, "txid")?).await? {
        Some(e) => Ok(no_store(Json(e).into_response())),
        None => Err(ApiError::not_found("not in the mempool")),
    }
}

async fn fees<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let blocks = match q.get("blocks") {
        Some(b) => parse(b, "blocks")?,
        None => 6,
    };
    let mode = match q.get("mode").map(String::as_str) {
        None | Some("economical") => FeeMode::Economical,
        Some("conservative") => FeeMode::Conservative,
        Some(m) => return Err(ApiError::bad_request(format!("bad mode: {m:?}"))),
    };
    let rate = gw.estimate_fee(&key, FeeTarget { blocks, mode }).await?;
    // "No estimate yet" is an answer, not a missing resource.
    Ok(no_store(
        Json(json!({ "fee_rate": rate.map(|r| r.to_sat_per_kwu()) })).into_response(),
    ))
}

/// A key without the scope gets 403 before its body is even parsed.
fn may(key: &Key, scope: Scope) -> Result<(), ApiError> {
    if key.has(&scope) {
        Ok(())
    } else {
        Err(ApiError::permission_denied(format!(
            "this key lacks the {scope} scope"
        )))
    }
}

fn is_binary_body(h: &HeaderMap) -> bool {
    h.get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|c| c.starts_with(OCTET))
}

fn tx_hex(s: &str) -> Result<Transaction, ApiError> {
    bitcoin::consensus::encode::deserialize_hex(s)
        .map_err(|e| ApiError::bad_request(format!("transaction: {e}")))
}

/// Concatenated consensus-encoded transactions.
fn txs_bytes(mut b: &[u8]) -> Result<Vec<Transaction>, ApiError> {
    let mut out = Vec::new();
    while !b.is_empty() {
        let (tx, n) = deserialize_partial::<Transaction>(b)
            .map_err(|e| ApiError::bad_request(format!("transaction {}: {e}", out.len())))?;
        out.push(tx);
        b = &b[n..];
    }
    Ok(out)
}

fn json_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("body: {e}")))
}

#[derive(Deserialize)]
struct HexBody {
    hex: String,
}

#[derive(Deserialize)]
struct TxsBody {
    txs: Vec<String>,
}

fn txs_body(headers: &HeaderMap, body: &[u8]) -> Result<Vec<Transaction>, ApiError> {
    if is_binary_body(headers) {
        txs_bytes(body)
    } else {
        json_body::<TxsBody>(body)?
            .txs
            .iter()
            .map(|h| tx_hex(h))
            .collect()
    }
}

async fn broadcast<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    may(&key, Scope::Broadcast)?;
    let tx = if is_binary_body(&headers) {
        match txs_bytes(&body)?.as_slice() {
            [tx] => tx.clone(),
            _ => return Err(ApiError::bad_request("expected exactly one transaction")),
        }
    } else {
        tx_hex(&json_body::<HexBody>(&body)?.hex)?
    };
    let txid = gw.broadcast(&key, &tx).await?;
    Ok(no_store(Json(json!({ "txid": txid })).into_response()))
}

async fn test_accept<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    may(&key, Scope::Broadcast)?;
    let txs = txs_body(&headers, &body)?;
    let results = gw.test_accept(&key, &txs).await?;
    Ok(no_store(
        Json(json!({ "results": results })).into_response(),
    ))
}

async fn package<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    may(&key, Scope::Broadcast)?;
    let txs = txs_body(&headers, &body)?;
    let results = gw.broadcast_package(&key, &txs).await?;
    Ok(no_store(
        Json(json!({ "results": results })).into_response(),
    ))
}

async fn events<N: Backend>(
    State(gw): Gw<N>,
    Auth(key): Auth,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let sub = gw.subscribe(&key).await?;
    let stream = sub.map(|e| {
        Ok(Event::default()
            .json_data(e)
            .unwrap_or_else(|_| Event::default().data("{\"type\":\"lagged\"}")))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// The OpenAPI 3 document, generated from [`ROUTES`].
pub fn openapi() -> Value {
    let mut paths = serde_json::Map::new();
    for r in ROUTES {
        let params: Vec<Value> = r
            .params
            .iter()
            .map(|p| {
                json!({
                    "name": p.name,
                    "in": p.location,
                    "required": p.location == "path",
                    "description": p.description,
                    "schema": { "type": p.schema },
                })
            })
            .collect();
        let mut ok = json!({ "description": "Success", "content": {
            "application/json": { "schema": { "type": "object" } }
        }});
        if r.binary && r.method == "GET" {
            ok["content"][OCTET] = json!({ "schema": { "type": "string", "format": "binary" } });
        }
        if r.grpc == "Events/Subscribe" {
            ok["content"] = json!({ "text/event-stream": { "schema": { "type": "string" } } });
        }
        let mut op = json!({
            "operationId": r.grpc.replace('/', "_"),
            "summary": r.summary,
            "description": format!("Scope `{}`. Mirrors gRPC `{}`.", r.scope, r.grpc),
            "parameters": params,
            "responses": {
                "200": ok,
                "default": {
                    "description": "Error",
                    "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } }
                }
            },
        });
        if r.method == "POST" {
            let mut content = json!({ "application/json": { "schema": { "type": "object" } } });
            if r.binary {
                content[OCTET] = json!({ "schema": { "type": "string", "format": "binary" } });
            }
            op["requestBody"] = json!({ "required": true, "content": content });
        }
        let entry = paths.entry(r.path.to_owned()).or_insert_with(|| json!({}));
        entry[r.method.to_ascii_lowercase()] = op;
    }
    json!({
        "openapi": "3.0.3",
        "info": {
            "title": "bitcoin-gateway",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "The bitcoin-node-api traits over HTTP. Every route but this one needs `Authorization: Bearer <token>`.",
        },
        "security": [{ "bearer": [] }],
        "paths": paths,
        "components": {
            "securitySchemes": { "bearer": { "type": "http", "scheme": "bearer" } },
            "schemas": { "Error": {
                "type": "object",
                "properties": { "error": {
                    "type": "object",
                    "properties": {
                        "type": { "type": "string" },
                        "code": { "type": "string" },
                        "core_code": { "type": "integer", "nullable": true },
                        "message": { "type": "string" },
                    },
                }},
            }},
        },
    })
}

/// The REST routes.
pub fn router<N: Backend>(gw: Arc<Gateway<N>>) -> Router {
    Router::new()
        .route("/v1/chain/tip", get(tip::<N>))
        .route("/v1/chain/heights/{height}", get(height::<N>))
        .route("/v1/blocks/{hash}", get(block::<N>))
        .route("/v1/blocks/{hash}/header", get(header::<N>))
        .route("/v1/blocks/{hash}/filter", get(filter::<N>))
        .route("/v1/tx/{txid}", get(transaction::<N>))
        .route("/v1/utxos/{txid}/{vout}", get(utxo::<N>))
        .route("/v1/mempool/txids", get(txids::<N>))
        .route("/v1/mempool/snapshot", get(snapshot::<N>))
        .route("/v1/mempool/{txid}", get(entry::<N>))
        .route("/v1/fees/estimate", get(fees::<N>))
        .route("/v1/tx", post(broadcast::<N>))
        .route("/v1/tx/test", post(test_accept::<N>))
        .route("/v1/packages", post(package::<N>))
        .route("/v1/events", get(events::<N>))
        .route("/v1/openapi.json", get(|| async { Json(openapi()) }))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(gw)
}
