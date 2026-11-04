# bitcoin-gateway

gRPC, REST and WebSocket in front of Bitcoin Core, behind per-key
permissions, built on the `bitcoin-node-api` traits and the
`bitcoin-core-rpc` / `bitcoin-core-zmq` backends.

```sh
bitcoin-gateway hash-token                   # a token and its hash for a [[key]]
bitcoin-gateway -c gateway.toml check-config
bitcoin-gateway -c gateway.toml
```

**Status: experimental.** The library crates are the product; the gateway
stays in this workspace, built and tested in CI, until someone depends on it.
Every setting is in [`gateway.example.toml`](gateway.example.toml).

`bitcoind` speaks JSON-RPC, REST, ZMQ and IPC, not gRPC or WebSocket. The
gateway is one binary built on these crates: it talks to Core on one side
and serves gRPC, REST and WebSocket on the other, behind per-key
permissions, so port 8332 stays private.

Library before gateway: wallets, Lightning nodes and pools embed the
library. The gateway is for those that can't or won't: other languages,
browsers, several services sharing one node, operators who want per-client
permissions. It is one gateway in front of one Core, not a load balancer
or a cache across nodes.

## Running it

```sh
bitcoin-gateway hash-token                  # make a token for a [[key]]
bitcoin-gateway -c gateway.toml check-config
bitcoin-gateway -c gateway.toml             # --grpc/--http/--metrics override [listen]
```

[`bin/bitcoin-gateway/gateway.example.toml`](gateway.example.toml)
documents every setting. A config error stops startup with the line that's
wrong (``gateway.toml:8: unknown scope `chain:raed` ``), and so does a wallet
scope naming a wallet Core doesn't have. `SIGHUP` reloads the `[[key]]`
entries and the TLS certificate; listener changes need a restart. On
`SIGTERM` the gateway stops accepting, gives calls in flight 10 seconds,
and ends event streams cleanly.

## Keys and tokens

Clients send `Authorization: Bearer <token>`: an HTTP header for REST and
WebSocket, metadata for gRPC. Never a query parameter, because URLs end up
in logs.

**Tokens are stored only as SHA-256 hashes**, and compared in constant
time against every key's hash. Why:

- Config files leak: backups, git repositories, container images, support
  tickets. A hash found in a leaked config can't be used to log in.
- Plain SHA-256 is enough. `hash-token` generates tokens as 256-bit random
  values. Slow hashes like argon2 protect guessable passwords; nobody can
  guess a random 256-bit token, so a fast hash is safe and costs almost
  nothing per request.
- It is the same idea as Core's own `rpcauth`, which stores a salted HMAC
  of the password rather than the password.
- The cost: a lost token can't be recovered, only reissued, and operators
  can't choose their own tokens, which is a feature.

## Scopes

Every scope is split read/write, so nobody has to put one scope spanning
`getbalance` and `sendtoaddress` in front of a hot wallet:

| Scope                     | Unlocks                                                                      |
| ------------------------- | ---------------------------------------------------------------------------- |
| `chain:read` / `:write`   | blockchain reads / `invalidateblock`, `reconsiderblock`, `pruneblockchain`…  |
| `mempool:read`            | mempool reads                                                                |
| `broadcast`               | `sendrawtransaction`, `submitpackage`, `testmempoolaccept`                   |
| `fees:read`               | `estimatesmartfee`                                                           |
| `events`                  | event streams                                                                |
| `mining:read` / `:write`  | `getblocktemplate`, `getmininginfo` / `submitblock`, `prioritisetransaction` |
| `network:read` / `:write` | `getpeerinfo`… / `addnode`, `setban`, `disconnectnode`…                      |
| `util:read`               | `validateaddress`, `decoderawtransaction`, `deriveaddresses`…                |
| `wallet:<name>:read`      | reads on that wallet only                                                    |
| `wallet:<name>:write`     | spending, signing, address and label changes on that wallet                  |
| `admin`                   | everything, including the admin-only methods                                 |

One table (`bin/bitcoin-gateway/src/scopes.rs`) gives every method
`bitcoin-core-rpc` types an area; its read/write class comes from the
library's `method_class` table. A test fails if a typed RPC has no area.
Some rules worth knowing:

- **A method in neither table is denied**, even for `admin`, unless the key
  also sets `allow_unknown = true`.
- **Admin only**: methods that take a path on the node's filesystem
  (`backupwallet`, `dumptxoutset`, `loadwallet`, `createwallet`…), that
  change the process (`stop`, `logging`, `setnetworkactive`,
  `encryptwallet`…), that run an external signer on the node host, or that
  list every wallet on the node.
- `listdescriptors` and `gethdkeys` can export private keys, so they need
  the wallet's `write` scope although Core counts them as reads.
- Wallet methods must be called on the wallet's own route; on the node
  route Core would pick the wallet, so only `admin` may.
- A key may have a rate, `rate = { per_second = 50, burst = 100 }`, shared
  by every API the key uses.
- A key has a number of calls to Core it may have running at once
  (`[limits] key_max_in_flight`, 32, or its own `max_in_flight`) and of event
  subscriptions it may hold (`max_subscriptions`, 16). Over either, the call
  is rate limited at once (429, `RESOURCE_EXHAUSTED`, `-32002`): a client
  that opens more should slow down, not queue.

## The composite backend

Config decides at startup which backend serves what: `bitcoin-core-rpc`'s
`Client` serves chain, mempool, broadcast, fees and pass-through; events
come from `bitcoin-core-zmq` when `[node] zmq` is set, and from `Client`'s
`waitfornewblock` polling otherwise (block events only).

**It is an enum, not `dyn` traits:**

```rust
enum Events { Zmq(ZmqEvents), Rpc(Client) }   // EventSource by match
struct Node { rpc: Client, events: Events }   // the five traits by delegation
```

The backends are known at compile time; only the choice is made at
runtime. An enum makes that choice without `Box<dyn ChainSource>`, which
the traits don't support (they return `impl Future`), so the traits stay
as they are. Everything above `Node` is generic over the traits, which is
how the gateway's tests run on `bitcoin-node-mock`.

## Event fan-out

The gateway opens one upstream subscription and copies it to every
client. Without that, every subscriber would open its own ZMQ socket or,
worse, hold one of Core's `rpcthreads` with its own `waitfornewblock` poll.
A client that falls behind gets `Lagged` and keeps going; upstream `Lagged`
is forwarded to everyone; if the upstream stream ends or fails, the
gateway resubscribes with backoff and sends everyone `Lagged`, since events
may have been lost. Clients resync with `tip()` (and `snapshot()` for a
mempool copy), the same recipe as the library.

## gRPC

Services mirror the traits, generated from
[`proto/bitcoin/node/v1/node.proto`](proto/bitcoin/node/v1/node.proto):
`Chain`, `Mempool`, `Broadcast`, `Fees`, and `Events` (a server stream).
All are read-only except `Broadcast`. Hashes and txids are 32 bytes in
internal byte order (what `bitcoin::BlockHash` holds); transactions, blocks
and headers are consensus-encoded bytes; amounts are satoshis; fee rates
are sat/kwu, like `bitcoin::FeeRate`. A lookup that finds nothing (`utxo`,
mempool `entry`, a fee estimate the node doesn't have yet) is an unset
`optional` field, not an error. `NodeEvent` is a `oneof` with `Lagged`, and
clients must handle it.

```sh
grpcurl -H "authorization: Bearer $TOKEN" localhost:50051 bitcoin.node.v1.Chain/Tip
```

Also served: the standard health service (it follows `/readyz`), server
reflection (so `grpcurl` needs no proto file), and gRPC-Web for browsers
when `[grpc] web = true`. The generated code is committed and a CI job
regenerates it (`cargo run -p grpc-codegen`, with a vendored `protoc`) and
fails on any diff, so building the gateway needs no `protoc`.

Errors map one table onto gRPC status codes and HTTP statuses, and Core's
error code travels along (`x-core-code` metadata on gRPC, `core_code` in
REST bodies) so clients can still branch on it:

| Library error                                               | gRPC                  | HTTP |
| ----------------------------------------------------------- | --------------------- | ---- |
| `Rpc { InvalidAddressOrKey (-5) }`                          | `NOT_FOUND`           | 404  |
| `Rpc { VerifyRejected, VerifyError, VerifyAlreadyInChain }` | `FAILED_PRECONDITION` | 422  |
| `Rpc { InvalidParameter, Type, Deserialization }`           | `INVALID_ARGUMENT`    | 400  |
| `Rpc { InWarmup }`, `Transport(Connect)`                    | `UNAVAILABLE`         | 503  |
| `Transport(Timeout)`                                        | `DEADLINE_EXCEEDED`   | 504  |
| `Unsupported`                                               | `UNIMPLEMENTED`       | 501  |
| `Decode`, `Transport(Auth)` (to Core), other                | `INTERNAL`            | 500  |
| no or bad token                                             | `UNAUTHENTICATED`     | 401  |
| scope missing                                               | `PERMISSION_DENIED`   | 403  |
| the key's rate exceeded                                     | `RESOURCE_EXHAUSTED`  | 429  |

A failed login to Core is the operator's problem, not the client's: it is
`INTERNAL` with a generic message, and a log line for the operator.

**No block templates over the network.** `TemplateSource` is on neither
gRPC nor REST. Why:

- Latency. For mining, the critical path is "new tip → fresh work on the
  hashers". Going through the gateway adds a network hop and a re-encoding
  of every template, and IPC exists precisely to remove those.
- State. A `Template` carries a live handle to a template held in Core.
  Over the network the gateway would have to keep every client's templates
  alive, give them ids, expire them and release them: IPC's
  `BlockTemplate` capability rebuilt, badly, across a network.
- Ecosystem. Stratum v2's Template Distribution Protocol is the standard
  for "template provider → pool", and pool software already speaks it. A
  gateway template API would be a protocol nobody's software uses.
- It is cheap to add later: a new service doesn't break existing clients,
  while removing one after people depend on it would.

## REST

The same trait API over plain HTTP, under `/v1`, next to gRPC:

| Route                                             | Trait method                   | Scope          |
| ------------------------------------------------- | ------------------------------ | -------------- |
| `GET  /v1/chain/tip`                              | `ChainSource::tip`             | `chain:read`   |
| `GET  /v1/chain/heights/{height}`                 | `block_hash`                   | `chain:read`   |
| `GET  /v1/blocks/{hash}`                          | `block`                        | `chain:read`   |
| `GET  /v1/blocks/{hash}/header`                   | `header_info`                  | `chain:read`   |
| `GET  /v1/blocks/{hash}/filter`                   | `block_filter` (BIP 158)       | `chain:read`   |
| `GET  /v1/tx/{txid}?block={hash}`                 | `transaction`                  | `chain:read`   |
| `GET  /v1/utxos/{txid}/{vout}?mempool=true`       | `utxo`                         | `chain:read`   |
| `GET  /v1/mempool/txids`                          | `MempoolSource::txids`         | `mempool:read` |
| `GET  /v1/mempool/snapshot`                       | `snapshot`                     | `mempool:read` |
| `GET  /v1/mempool/{txid}`                         | `entry`                        | `mempool:read` |
| `GET  /v1/fees/estimate?blocks=6&mode=economical` | `FeeEstimator::estimate_fee`   | `fees:read`    |
| `POST /v1/tx`                                     | `Broadcaster::broadcast`       | `broadcast`    |
| `POST /v1/tx/test`                                | `test_accept`                  | `broadcast`    |
| `POST /v1/packages`                               | `broadcast_package`            | `broadcast`    |
| `GET  /v1/events`                                 | `EventSource::subscribe` (SSE) | `events`       |

```sh
curl -H "authorization: Bearer $TOKEN" localhost:8080/v1/chain/tip
curl -H "authorization: Bearer $TOKEN" -H "accept: application/octet-stream" \
     localhost:8080/v1/blocks/$HASH > block.bin
curl -N -H "authorization: Bearer $TOKEN" localhost:8080/v1/events
```

- **JSON** by default, in `bitcoin-node-api`'s `serde` shape: hashes as hex
  in display order (as Core's JSON and explorers show them), transactions
  as consensus hex, amounts in satoshis, fee rates in sat/kwu. **Binary**
  with `Accept: application/octet-stream`: consensus bytes for blocks,
  headers and transactions. A header's JSON also carries the block's
  `height` and `chain_work` (as gRPC's `HeaderReply` does). `POST /v1/tx` takes `{"hex": "…"}` or raw
  bytes, by `Content-Type`; the other POSTs take `{"txs": ["…"]}` or the
  transactions' bytes back to back.
- **A lookup that finds nothing is 404** (`utxo`, mempool `entry`), but an
  absent fee estimate is `200` with `"fee_rate": null`: "no estimate yet" is
  an answer, not a missing resource.
- **Errors** have one body, with the status from the table above:
  `{"error": {"type": "rpc", "code": "verify_rejected", "core_code": -26, "message": "…"}}`.
- **Events** are Server-Sent Events, one event JSON per `data:` line,
  `lagged` included. Browsers' `EventSource` can't send an `Authorization`
  header, so browsers use WebSocket for events; SSE is for `curl` and
  server-side clients.
- **Caching.** A block or header fetched by hash, or a transaction fetched
  with its block, never changes: those responses are
  `max-age=31536000, immutable` with an `ETag`, and `If-None-Match` gets a
  `304` without asking Core (the token and scope are still checked).
  Everything else is `no-store`. Responses are `private` by default,
  because requests carry a token and a shared cache serving them to others
  would bypass scopes; `[http] public_cache = true` makes the immutable
  ones `public`, for an operator who puts a CDN in front of chain data
  they consider public anyway. A block reorged out keeps its hash and
  content, so caching it stays correct.
- **CORS** only for the origins in `[http] cors_origins`.
- **`GET /v1/openapi.json`**: an OpenAPI 3 document generated from the
  route table, so clients in any language can be generated.

**Why REST next to gRPC, with hand-written routes over the same handlers:**

- Reach: `curl`, shell scripts, languages with weak gRPC support, and
  browsers without gRPC-Web.
- HTTP caching: a block or header fetched by hash never changes, so it can
  be cached for good. Explorers get that from REST; gRPC and WebSocket
  can't offer it.
- No second API to keep in step: each route calls the same handler as its
  gRPC method, with the same scopes and error table. A test fails if a
  gRPC method (read from the proto's descriptor) has no route.
- Push stays where it fits: events are Server-Sent Events on REST, and a
  gRPC stream or WebSocket otherwise.

## WebSocket: Core's JSON-RPC, passed through

Everything Core-specific (the ~160 RPCs that aren't trait methods) goes
through WebSocket. `/ws` routes to the node and `/ws/wallet/<name>` to that
wallet, mirroring Core's own URLs. Frames are JSON-RPC 2.0 objects or
batches with Core's method names and params; each call is checked against
the key's scopes (wallet calls against `wallet:<name>:*`), then sent to
Core, and Core's `result` or `error` comes back as is.

```sh
websocat -H "authorization: Bearer $TOKEN" ws://localhost:8080/ws
{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":[]}
```

Errors the gateway adds use JSON-RPC's server range: `-32001` the key may
not call the method, `-32002` rate limited, `-32003` the node is
unreachable. A batch is answered item by item, so one denied call doesn't
fail the others. Per connection: messages up to `[ws] max_message_bytes`
(1 MiB; larger closes the connection with code 1009), `[ws] max_in_flight`
calls at once (16; the gateway stops reading until one finishes), and the
key's rate per call. Browsers can't set headers on a WebSocket, so the
token may also be sent as a subprotocol:
`new WebSocket(url, ["bitcoin-gateway", "bearer." + token])`.

Events, for browsers and anyone already on the socket:

```json
→ {"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":[]}
← {"jsonrpc":"2.0","id":1,"result":"sub-1"}
← {"jsonrpc":"2.0","method":"events.event","params":{"subscription":"sub-1","event":{"type":"block_connected","hash":"…"}}}
→ {"jsonrpc":"2.0","id":2,"method":"events.unsubscribe","params":["sub-1"]}
```

The `event` body is the same JSON as SSE's `data:` and REST's bodies:
`bitcoin-node-api`'s `serde` shape, now **v1 and frozen**, so changing it
is a breaking change of that crate.

**Why pass Core's JSON through unchanged:**

- It works across Core versions. The gateway returns whatever Core said, so
  every supported Core works, and RPCs added in a future Core release work
  without a gateway release (for an `admin` key with `allow_unknown`).
- No new surface to maintain. Typed results would mean designing and
  freezing a JSON schema for about 160 replies, the version-drifting
  surface this project lists as a non-goal. Core's own docs and existing
  client libraries in every language already describe its JSON.
- Nothing is lost. Re-encoding through the library's `model` types could
  drop fields Core sends.
- Clients who want stability have gRPC and REST: the typed,
  version-independent API, covering the traits. WebSocket is the escape
  hatch for everything Core-specific.

## Listeners and TLS

Three listeners, each optional: gRPC, HTTP (REST under `/v1`, WebSocket at
`/ws`) and metrics. With `[tls]` every listener serves TLS (rustls, HTTP/2
and HTTP/1.1 by ALPN). The gateway refuses to listen on a non-loopback
address without `[tls]`, unless started with `--insecure` for TLS
terminated by a proxy in front.

**Limits** (`[limits]`), so nobody, with or without a token, can tie up the
gateway or Core:

- **Connections**: at most `max_connections` (1024) per listener and
  `max_connections_per_ip` (64); more are closed on accept, before TLS, and
  counted in `gateway_connections_rejected_total`.
- **Slow or silent clients**: a TLS handshake, a connection's first bytes,
  each HTTP/1.1 request's headers (an idle keep-alive connection
  included) and each REST request's body get `handshake_timeout_secs`
  (10). HTTP/2 connections are
  pinged every 30 seconds and dropped if the answer doesn't come, and carry
  at most `http2_max_streams` (100) calls at once. Tokens are checked per
  request, so without these a client with no token could hold connections
  open for ever.
- **Core's threads**: at most `node_max_in_flight` (8) calls go to Core at
  once, all keys together; the rest wait up to `node_queue_secs` (10), then
  fail as unavailable (503, `UNAVAILABLE`, `-32003`). Core answers RPCs on
  `-rpcthreads` threads with a `-rpcworkqueue` queue behind them, and
  refuses work past that; keeping the gateway below it keeps Core
  answering. Readiness checks and the event feed don't need a slot, so they
  get through when clients fill them.
- **Per key**: calls in flight and subscriptions, under Scopes above.

A reload (`SIGHUP`) starts each key's counts afresh. Calls and event
streams already open finish on the key they started with; WebSocket
connections, which authenticate once at upgrade, are closed with 1012 so
every call after the reload is checked against the new keys.

## Configuration reference

[`gateway.example.toml`](gateway.example.toml) is the
annotated version. Every section but `[node]` is optional.

| Setting                            | Default             | Meaning                                                                |
| ---------------------------------- | ------------------- | ---------------------------------------------------------------------- |
| `[node] rpc_url`                   | (required)          | Core's RPC, `http://` or `https://`                                    |
| `[node] cookie`                    |                     | Core's `.cookie`, re-read after a 401; or:                             |
| `[node] rpc_user`, `rpc_password`  |                     | `-rpcuser`/`-rpcpassword` or an `-rpcauth` user                        |
| `[node] zmq`                       | none: RPC polling   | Core's `-zmqpubsequence` endpoint, `tcp://…`                           |
| `[listen] grpc`, `http`, `metrics` | none                | addresses; `--grpc`, `--http`, `--metrics` override them               |
| `[http] cors_origins`              | `[]`                | origins that get CORS headers (REST, WebSocket, gRPC-Web)              |
| `[http] public_cache`              | `false`             | immutable responses `public` instead of `private`                      |
| `[grpc] web`                       | `false`             | also accept gRPC-Web                                                   |
| `[ws] max_message_bytes`           | `1048576`           | largest WebSocket message; larger closes with 1009                     |
| `[ws] max_in_flight`               | `16`                | calls one WebSocket connection may have running                        |
| `[events] buffer`                  | `1024`              | events buffered per subscriber before it gets `Lagged`                 |
| `[limits] max_connections`         | `1024`              | open connections per listener                                          |
| `[limits] max_connections_per_ip`  | `64`                | open connections per client IP, per listener                           |
| `[limits] handshake_timeout_secs`  | `10`                | TLS handshake, first bytes, each HTTP/1.1 request's headers and body   |
| `[limits] http2_max_streams`       | `100`               | concurrent HTTP/2 streams (gRPC calls) per connection                  |
| `[limits] max_subscriptions`       | `16`                | event subscriptions per key, all APIs together                         |
| `[limits] key_max_in_flight`       | `32`                | calls to Core per key at once                                          |
| `[limits] node_max_in_flight`      | `8`                 | calls to Core at once, all keys; below `-rpcthreads` + `-rpcworkqueue` |
| `[limits] node_queue_secs`         | `10`                | how long a call waits for one of those                                 |
| `[tls] cert`, `key`                | none                | PEM files; every listener serves TLS; reloaded on `SIGHUP`             |
| `[[key]] name`                     | (required)          | shown in logs and metrics                                              |
| `[[key]] token_sha256`             | (required)          | from `bitcoin-gateway hash-token`                                      |
| `[[key]] allow`                    | (required)          | scopes, see above                                                      |
| `[[key]] rate`                     | unlimited           | `{ per_second = 50, burst = 100 }`, shared by every API                |
| `[[key]] allow_unknown`            | `false`             | an `admin` key may call methods the gateway doesn't know               |
| `[[key]] max_in_flight`            | `key_max_in_flight` | this key's calls to Core at once                                       |

## Operations

- **Metrics** (Prometheus text format at `/metrics` on the metrics
  listener, which has no auth: keep it private): requests by key, method
  and outcome; latency histograms by method; errors from Core by kind;
  whether Core answered the last readiness check; event subscribers;
  `Lagged` sent; fan-out resubscriptions; calls to Core in flight;
  connections refused by `[limits]`. WebSocket method names that the
  gateway doesn't know are counted as `unknown`, so clients can't create
  labels.
- **Health**: `/healthz` answers while the process is up; `/readyz` answers
  `200` only when Core answers and is out of initial block download, `503`
  otherwise. The gateway checks every 5 seconds; `/readyz` and the gRPC
  health service report that last check and never call Core themselves,
  so the unauthenticated metrics listener can't be used to flood the node.
- **Logs** through `tracing` (`RUST_LOG`, default `info`), with the
  library crates' `tracing` feature on, plus one line per request with the
  key name, method, outcome and duration. Never params or results.
- **Upstream outages**: `Client` reconnects per request and ZMQ by itself;
  meanwhile calls fail with `UNAVAILABLE` / `503` / `-32003`, `/readyz`
  fails, and event streams get `Lagged` when the feed comes back. At
  startup the gateway waits for Core (and ZMQ), retrying with backoff;
  an error waiting can't fix (wrong credentials, an unsupported Core, a
  reply it can't parse) stops it with that error instead.
- **Packaging**: the release workflow
  ([`.github/workflows/release.yml`](../../.github/workflows/release.yml)) builds
  Linux x86_64 and aarch64 binaries on a `v*` tag, attaches them to the
  GitHub release with the example config and a hardened
  [systemd unit](packaging/bitcoin-gateway.service),
  and pushes a multi-arch distroless image to
  `ghcr.io/<owner>/bitcoin-gateway`. Run by hand, or on a pull request that
  changes the packaging, it does all of that but publish, and runs each
  binary and the image once. `bin/bitcoin-gateway/Dockerfile` builds the
  same image from source.

```sh
docker run -v ./gateway.toml:/etc/bitcoin-gateway/gateway.toml:ro \
  -p 8080:8080 ghcr.io/framicheli/bitcoin-gateway --insecure   # TLS at your proxy
```

Still open outside the gateway: proposing `bitcoin-core-rpc` to the
rust-bitcoin `corepc` maintainers, the first crates.io release, and whether
BIP 158 filters belong on a trait.
