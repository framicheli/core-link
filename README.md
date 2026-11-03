# bitcoin_rpc

An async Rust library for interacting with Bitcoin Core.

- **`bitcoin-core-rpc`**: a complete, typed, async JSON-RPC client. One
  method per public RPC in Core 31 (164 RPCs across 9 sections), a
  `Wallet` handle for the wallet RPCs, the regtest/hidden RPCs, automatic
  Core version detection, and a `raw_call` escape hatch.
- **`bitcoin-node-api`**: a small set of backend-agnostic capability traits
  (`ChainSource`, `MempoolSource`, `Broadcaster`, `FeeEstimator`,
  `TemplateSource`, `EventSource`) that libraries can program against
  without caring how they reach the node.
- **`bitcoin-core-zmq`**: Core's ZMQ `sequence` notifications as an
  `EventSource` stream.
- **`bitcoin-core-ipc`**: Core 30+'s Cap'n Proto mining interface as a
  `TemplateSource`, for miners who care about milliseconds.
- **`bitcoin-node-mock`**: an in-memory node implementing all six traits,
  for testing code written against them without a `bitcoind`.
- **`bitcoin-node-ldk`**: LDK's `lightning-block-sync` `BlockSource` over
  any `ChainSource`, the first external adapter built on the traits.
- **`bitcoin-node-bdk`**: BDK chain sync over any `ChainSource` and
  `MempoolSource`: block by block through reorgs, the mempool with
  evictions, or by compact block filters (fetching only the blocks that
  touch the wallet), as the events `bdk_wallet` applies.
- **`bitcoin-gateway`** (a binary, not a crate): Core over gRPC, REST and
  WebSocket, behind per-key permissions
  ([§16](#16-bitcoin-gateway-core-over-grpc-rest-and-websocket)).

Supported Bitcoin Core versions: **29, 30 and 31**, each tested live in CI
against the real release binary. The rule: the major versions Bitcoin Core
itself maintains (the latest three). When Core ships a new major, it is
added and the one Core retires is dropped.

Status: pre-release (`0.1.0`), not yet on crates.io. Stages A–E of the plan
are done, Phase 1.5's F (production `Client`), G (mock, tracing) and H1
(docs, metadata, CHANGELOG), and Phase 2 (I–L, the gateway; M, the LDK
adapter). No release has been tagged yet.

---

## Contents

1. [Why this project exists](#1-why-this-project-exists)
2. [Who it is for](#2-who-it-is-for)
3. [Architecture: two layers](#3-architecture-two-layers)
4. [Workspace layout](#4-workspace-layout)
5. [Quick start](#5-quick-start)
6. [`bitcoin-node-api`: the traits](#6-bitcoin-node-api-the-traits)
7. [`bitcoin-core-rpc`: the client](#7-bitcoin-core-rpc-the-client)
8. [`bitcoin-core-zmq`: push events](#8-bitcoin-core-zmq-push-events)
9. [`bitcoin-core-ipc`: mining over Cap'n Proto](#9-bitcoin-core-ipc-mining-over-capn-proto)
10. [`consumers`: proving the traits](#10-consumers-proving-the-traits)
11. [Support tooling: `test-node`, `ipc-codegen`, scripts](#11-support-tooling-test-node-ipc-codegen-scripts)
12. [Testing strategy](#12-testing-strategy)
13. [Continuous integration](#13-continuous-integration)
14. [Development workflow](#14-development-workflow)
15. [How the project was built](#15-how-the-project-was-built)
16. [`bitcoin-gateway`: Core over gRPC, REST and WebSocket](#16-bitcoin-gateway-core-over-grpc-rest-and-websocket)
17. [Further reading](#17-further-reading)
18. [License](#18-license)

---

## 1. Why this project exists

`bitcoind` is the reference implementation of Bitcoin, and almost every
serious Bitcoin service (wallets, Lightning nodes, mining pools, indexers,
explorers, custody backends) needs to talk to it. In Rust, at the time this
project started, doing that well was surprisingly hard:

- **There is no maintained async production client.**
  `rust-bitcoincore-rpc` was archived in November 2025. `corepc-client` is
  blocking and documented as a testing tool; the corepc project explicitly
  tells production users to write their own client on top of
  `corepc-types`.
- **Version handling is left to the application.** Core's JSON replies
  change between releases. `corepc-types` has one set of structs per
  version (`v17` … `v31`), but picking the right one at runtime is the
  caller's problem.
- **Everyone re-implements the same layer privately.** LDK has
  `lightning-block-sync` (a `BlockSource` trait with RPC and REST
  backends), BDK has `bdk_bitcoind_rpc` and `bdk_bitcoind_client`,
  p2poolv2 has its own `NodeBackend`. None of these are reusable outside
  their project.
- **Almost nothing in Rust talks to Core's IPC mining interface
  generically.** Core 30 introduced a Cap'n Proto interface that lets a
  miner get new block templates with far less latency than JSON-RPC, but
  the only existing wrapper (`bitcoin_core_sv2`) is shaped around Stratum
  v2 messages and drags in a large dependency tree.

This project fills those gaps: one complete, async, version-aware client,
plus a thin trait layer so that libraries can be written once and run on
JSON-RPC, ZMQ, IPC, a mock, or (later) a remote gateway.

### Prior art, and what was reused

| Piece                 | State before this project                                                         | Here                                         |
| --------------------- | --------------------------------------------------------------------------------- | -------------------------------------------- |
| Async JSON-RPC client | None maintained                                                                   | `bitcoin-core-rpc`                           |
| Response types        | `corepc-types`: per-version structs plus a `model` layer in `rust-bitcoin` types | **Used, not forked**                         |
| IPC mining            | `bitcoin_core_sv2` (Sv2-shaped, heavy deps)                                       | `bitcoin-core-ipc`, written directly on capnp |
| Chain-source trait    | LDK, BDK, p2poolv2: each private to its project                                   | `bitcoin-node-api`                           |
| gRPC / WS gateway     | `btc-buf` (Go, early), `btc-rpc-proxy` (auth proxy)                               | `bitcoin-gateway`                            |

### Scope decisions

- **Full RPC coverage** for Core 29–31, not a curated subset. A client that
  covers "the common calls" forces users back to stringly-typed raw calls
  the moment they need anything else.
- **Traits stay small.** They don't mirror Core; they name capabilities
  that several backends can provide. A trait only grows when a second
  backend could implement the method, or a consumer needs to be generic
  over it, never just because `Client` has a method.
- **Library before gateway.** Wallets, Lightning nodes and pools embed a
  client; the gateway is a separate product built on top.

### Non-goals

- A protobuf message per Core RPC in the gateway (a large, version-drifting
  surface for unclear demand). gRPC will expose the traits only.
- Other node implementations (btcd, Knots, …) until a real user needs them.
- Core < 28. `corepc-types` goes back to v17, so widening is cheap if asked.
- Legacy (BDB) wallets. Only descriptor wallets get typed methods; the 12
  legacy-only RPCs remain reachable through `Client::raw_call`.

---

## 2. Who it is for

Any Rust developer who wants a complete, typed, async client for Core
29–31 (nothing maintained fills that role today, see
[§1](#1-why-this-project-exists)), and in particular:

- **Wallet-like applications** (wallets, Lightning nodes, indexers,
  explorers, custody backends) that sync chain state and track
  transactions and UTXOs. They use `bitcoin-core-rpc` and
  `bitcoin-core-zmq`.
- **Mining software** (pools, solo miners) that builds block templates and
  submits blocks, where milliseconds matter. They use `bitcoin-core-ipc`,
  with RPC as a fallback.
- **Library authors** (LDK, BDK, p2pool and the like) who today each keep a
  private node-access layer. They can program against the
  `bitcoin-node-api` traits once and run on RPC, ZMQ, IPC or the mock. The
  `consumers` crate ([§10](#10-consumers-proving-the-traits)) proves the
  traits hold up, `bitcoin-node-ldk` runs LDK's block sync on them, and
  `bitcoin-node-bdk` keeps a `bdk_wallet` in sync over them.
- **Anyone testing code written against the traits.** `bitcoin-node-mock`
  implements all six traits in memory, so tests don't need a `bitcoind`.
- **Services that can't embed a Rust library** (other languages, browsers,
  several services sharing one node, operators who want per-key permissions
  in front of port 8332). The `bitcoin-gateway` binary
  ([§16](#16-bitcoin-gateway-core-over-grpc-rest-and-websocket)) serves them over gRPC, REST and
  WebSocket.

The first two drove the design, and they need very different things:

|                | Wallet-like (wallets, LN nodes, indexers)                                   | Mining (pools, solo miners)                        |
| -------------- | --------------------------------------------------------------------------- | -------------------------------------------------- |
| Main job       | Sync chain state, find relevant transactions/UTXOs                          | Build a block template, submit blocks              |
| Key calls      | blocks/headers, `getrawtransaction`, `gettxout`, `sendrawtransaction`, fees | `getblocktemplate`, `submitblock`                  |
| Events         | New block, mempool transaction                                              | New tip, as fast as possible; template updates     |
| Latency        | Seconds are fine                                                            | Milliseconds matter (stale work is lost money)     |
| Best transport | JSON-RPC + ZMQ                                                              | IPC (`createNewBlock`, `waitNext`), RPC as fallback |

The overlap between them is small (tip, block fetch, broadcast, events) but
real, and that overlap is exactly what the trait layer contains.

What `bitcoind` actually speaks: JSON-RPC over HTTP, REST (read-only), ZMQ
(push notifications) and, from Core 30, Cap'n Proto IPC (mining-focused,
schema still unstable). It does **not** speak WebSocket or gRPC, which is
why those belong in a separate gateway process (see
[§16](#16-bitcoin-gateway-core-over-grpc-rest-and-websocket)).

---

## 3. Architecture: two layers

```
                 consumers (wallet, pool, indexer, gateway)
                        │                       │
            generic over traits          needs Core specifics
                        │                       │
   ┌────────────────────▼────────┐    ┌─────────▼──────────────────┐
   │ bitcoin-node-api            │    │ bitcoin-core-rpc::Client   │
   │ ChainSource, Broadcaster,   │◄───│ one async method per RPC,  │
   │ FeeEstimator, MempoolSource,│impl│ Wallet handle, version     │
   │ TemplateSource, EventSource │    │ detection, raw_call        │
   └──────────▲──────────────────┘    └─────────┬──────────────────┘
              │ impl                            │ JSON-RPC/HTTP
   ┌──────────┴──────────┐                      ▼
   │ bitcoin-core-zmq    │                   bitcoind
   │ bitcoin-core-ipc    │
   └─────────────────────┘
```

- **Layer 1, `Client`** is complete and Core-shaped. Every public RPC is a
  typed async method named after the RPC. This is what you reach for when
  you know Core and want Core.
- **Layer 2, the traits** are small and backend-agnostic (about 17 methods,
  built from the RPCs marked `core` in [`RpcInventory.md`](RpcInventory.md)).
  This is what libraries program against, so they can swap `Client` for
  IPC, ZMQ, a mock, or a remote gateway.

### Backend support matrix

|                    |  Chain   | Mempool | Broadcaster | Fee |      Template      |                  Event                  |
| ------------------ | :------: | :-----: | :---------: | :-: | :----------------: | :-------------------------------------: |
| `bitcoin-core-rpc` |    ✓     |    ✓    |      ✓      |  ✓  | ✓ (GBT + longpoll) | ✓ (`waitfornewblock` poll, blocks only) |
| `bitcoin-core-zmq` |          |         |             |     |                    |                    ✓                    |
| `bitcoin-core-ipc` | tip only |         |             |     |         ✓          |    ✓ (`waitTipChanged`, tip only)     |

A typical deployment combines them: RPC for chain/mempool/broadcast/fees,
ZMQ for events, and IPC for templates when the node is Core 30+.

---

## 4. Workspace layout

```
bitcoin_rpc/
├── Cargo.toml              virtual workspace; shared deps and package metadata
├── DESIGN.md               the full design, with decisions and findings
├── PHASE_2.md              the gateway's plan, and the LDK adapter's
├── RpcInventory.md         every Core 31 RPC, classified core / typed / ?
├── CHANGELOG.md            changes to the published crates
├── scripts/
│   └── fetch-bitcoind.sh   downloads a pinned, hash-checked Core release
├── .github/workflows/      ci.yml on every push and PR, release.yml on v* tags
├── crates/
│   ├── bitcoin-node-api/   traits, events, errors          deps: bitcoin, futures-core (serde optional)
│   ├── bitcoin-core-rpc/   Client + Wallet                 deps: api, corepc-types, reqwest, serde, tokio
│   ├── bitcoin-core-zmq/   EventSource over ZMQ            deps: api, zeromq, tokio
│   ├── bitcoin-core-ipc/   mining over Cap'n Proto         deps: api, capnp, capnp-rpc, tokio
│   ├── bitcoin-node-mock/  in-memory node, all six traits  deps: api, tokio (sync, time)
│   ├── bitcoin-node-ldk/   LDK BlockSource on ChainSource  deps: api, lightning-block-sync
│   ├── bitcoin-node-bdk/   BDK sync on Chain/MempoolSource deps: api, bdk_core
│   ├── consumers/          two example consumers (publish = false)
│   ├── test-node/          throwaway regtest bitcoind for live tests (publish = false)
│   ├── ipc-codegen/        regenerates bitcoin-core-ipc's capnp code (publish = false)
│   └── grpc-codegen/       regenerates the gateway's gRPC code (publish = false)
└── bin/
    └── bitcoin-gateway/    gRPC, REST and WebSocket in front of Core (release binaries, not crates.io)
```

Rules the workspace follows, and why:

- **`bitcoin-node-api` has no I/O dependencies and no `corepc-types`.** It
  must be cheap to depend on for any library, and its signatures use only
  `bitcoin::*` types or types it defines itself.
- **Backends depend on the api crate, never on each other.** Live tests
  may dev-depend on `bitcoin-core-rpc` to drive the node.
- **`bitcoin = "0.32"`** is pinned once in `[workspace.dependencies]`,
  matching `corepc-types 0.16`. The two are bumped together.
- **Edition 2024, `rust-version = 1.89`** (the floor set by `zeromq` 0.7).
  A CI job checks that promise stays true.
- **Every public type derives `Debug, Clone, PartialEq, Eq`**, small ones
  add `Copy`. Enums that will grow (`NodeAPIError`, `RpcErrorCode`,
  `TransportError`, `NodeEvent`, `Feature`) are `#[non_exhaustive]`, so a
  new variant is not a semver break.

---

## 5. Quick start

The crates are not published yet; depend on them by git:

```toml
[dependencies]
bitcoin-core-rpc = { git = "https://github.com/framicheli/bitcoin_rpc" }
bitcoin-node-api = { git = "https://github.com/framicheli/bitcoin_rpc" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Talk to Core directly

```rust
use bitcoin_core_rpc::{Auth, Client};

#[tokio::main]
async fn main() -> bitcoin_node_api::ApiResult<()> {
    let auth = Auth::cookie("/home/me/.bitcoin/.cookie").expect("cookie");
    let client = Client::connect("http://127.0.0.1:8332", auth).await?;

    println!("Core {}", client.version().major());
    let info = client.get_blockchain_info().await?;
    println!("{info:?}");
    Ok(())
}
```

`Client::connect` runs `getnetworkinfo` once to learn the Core version, and
fails fast on a bad URL, bad credentials, or a Core older than 28.

For anything beyond URL and auth, use the builder:

```rust
let client = Client::builder("http://127.0.0.1:8332")
    .auth(Auth::cookie(path)?)               // re-read after a 401: survives node restarts
    .timeout(Duration::from_secs(10))        // default 30 s
    .long_timeout(Duration::from_secs(900))  // waitfor*, scans, longpolls; default 600 s
    .http_client(my_reqwest_client)          // proxy, TLS roots, pool limits
    .retry(RetryPolicy::default())           // off unless set; safe per method, see below
    .connect().await?;                       // or .build(version): no I/O
```

Retries follow one table, `method_class(name)`, that classes every typed
RPC as `Read`, `Idempotent` (`sendrawtransaction`, `submitblock`, ...) or
`Write`. A refused connection or `-28` (warmup) is retried for any method;
a timeout only for reads, since a timed-out `sendtoaddress` may have paid.
Unknown methods count as writes.

Catching up on blocks? `client.get_block_hashes(0..=199)` and
`client.get_blocks(&hashes)` send JSON-RPC batches (100 items per request);
`client.batch::<T>(method, params)` does the same for any method, with a
per-item `Result`.

### Program against the traits

```rust
use bitcoin_node_api::{ApiResult, ChainSource};

/// Works with `bitcoin_core_rpc::Client`, an IPC client (tip only), a mock…
async fn print_tip<C: ChainSource>(node: &C) -> ApiResult<()> {
    let tip = node.tip().await?;
    let header = node.header(tip.hash).await?;
    println!("height {} time {}", tip.height, header.time);
    Ok(())
}
```

### Test against the mock

```rust
use bitcoin_node_api::{ChainSource, EventSource};
use bitcoin_node_mock::MockNode;

let node = MockNode::regtest();          // genesis tip
let mut events = node.subscribe().await?;
node.mine_empty(3);                      // BlockConnected ×3
node.reorg(2, 3);                        // BlockDisconnected ×2, then BlockConnected ×3
node.add_to_mempool(tx);                 // TxAdded, with a mempool sequence
node.fail_next("block", TransportError::Timeout.into());
node.lag();                              // Lagged to every subscriber
print_tip(&node).await?;                 // anything generic over the traits
```

It behaves like the RPC backend where callers can tell (unknown lookups,
error codes, snapshot sequences, mined txs leaving the mempool silently,
`wait_next` waking on a new tip or a mempool change) and validates almost
nothing else. `submit_solution` checks the header against the template and
the proof of work. The `consumers` crate runs both of its scenarios against
it offline (`crates/consumers/tests/mock.rs`).

### `tracing` feature

`bitcoin-core-rpc`, `bitcoin-core-zmq` and `bitcoin-core-ipc` have an
optional `tracing` feature. RPC: a `rpc` span per request (`method`, `id`,
`wallet`, `duration_ms`, `error`), with retries and cookie re-reads as
events inside it, and a `rpc_batch` span per batch request. ZMQ: connect,
disconnect, and each `Lagged` with its cause (sequence gap, full channel,
reconnect). IPC: connect, the protocol picked, each Core thread created.
**Params and results are never recorded**: they hold passphrases, private
descriptors and raw transactions; errors are logged by kind and code only.

### Use a wallet (regtest)

```rust
use bitcoin::{Amount, FeeRate};
use bitcoin_core_rpc::{CreateWalletOptions, FundOptions, SendOptions};

let hot = client.create_wallet("hot", CreateWalletOptions::default()).await?.wallet;
let mine = hot.get_new_address(None, None).await?.assume_checked();
client.generate_to_address(101, &mine).await?; // one mature coinbase

let to = hot.get_new_address(None, None).await?.assume_checked();
let fee = FundOptions::default().fee_rate(FeeRate::from_sat_per_vb_u32(2));
hot.send(&[(to, Amount::ONE_BTC)], None, SendOptions::default().fund(fee)).await?;
```

A full PSBT example (watch-only wallet builds, hot wallet signs) is in the
crate docs at the top of `crates/bitcoin-core-rpc/src/lib.rs`.

### Subscribe to events over ZMQ

```rust
use bitcoin_core_zmq::{Topic, ZmqEvents};
use bitcoin_node_api::EventSource;

let zmq = ZmqEvents::connect("tcp://127.0.0.1:28333", Topic::Sequence).await?;
let mut events = zmq.subscribe().await?;
// Now sync (tip, mempool snapshot), then apply events as they arrive.
```

### Get block templates over IPC (Core 30+)

```rust
use bitcoin_core_ipc::IpcClient;
use bitcoin_node_api::{TemplateSource, WaitOptions};

let ipc = IpcClient::connect("/home/me/.bitcoin/regtest/node.sock").await?;
let tmpl = ipc.template().await?;
let next = ipc.wait_next(&tmpl, WaitOptions::default()).await?;
```

---

## 6. `bitcoin-node-api`: the traits

`crates/bitcoin-node-api`. Pure types and traits, no I/O.

```
src/
├── lib.rs       re-exports, ApiResult, the Fut<T> trait alias
├── error.rs     NodeAPIError, RpcErrorCode, TransportError
├── chain.rs     Tip, Utxo, ChainSource
├── mempool.rs   MempoolEntry, MempoolSnapshot, MempoolSource
├── tx.rs        MempoolAccept, PackageResult, FeeTarget, Broadcaster, FeeEstimator
├── mining.rs    Template, Coinbase, TemplateTx, TemplateToken, Solution, WaitOptions, TemplateSource
├── event.rs     NodeEvent, EventSource
└── wire.rs      serde mirror types (feature "serde" only)
```

### Errors

One error type for every backend:

```rust
#[non_exhaustive]
pub enum NodeAPIError {
    Rpc { code: RpcErrorCode, message: String }, // the node answered with an error
    Transport(TransportError),                   // couldn't reach the node
    Decode(String),                              // reached it, couldn't parse the reply
    Unsupported(String),                         // this backend / Core version can't do it
}
```

- `RpcErrorCode` mirrors Core's `src/rpc/protocol.h` (JSON-RPC, general,
  P2P and wallet codes). `Other(i64)` keeps any code newer than this crate,
  and `From<i64>` / `code()` round-trip exactly.
- `TransportError` is `Connect`, `Timeout`, `Auth` (HTTP 401/403),
  `Http(status)` or `Other`; `is_retryable()` is true for `Connect` and
  `Timeout` only.
- Lookups map Core's `-5` ("not found") to `Ok(None)` (`utxo`,
  `MempoolSource::entry`), so "not there" is a value, not an error.

### The `Fut<T>` alias

Trait methods return `impl Fut<T>`, a trait alias for
`Future<Output = ApiResult<T>> + Send`. This uses native async-in-trait (no
`async-trait` boxing). The traits are therefore not `dyn`-compatible;
consumers are generic (`fn run<N: ChainSource>(n: &N)`). That will be
revisited if someone needs `Box<dyn ChainSource>`.

### The traits

| Trait            | Methods                                                                     | Purpose                                     |
| ---------------- | --------------------------------------------------------------------------- | ------------------------------------------- |
| `ChainSource`    | `tip`, `block_hash`, `header`, `header_info`, `block_filter`, `block`, `transaction`, `utxo` | Read the best chain        |
| `MempoolSource`  | `txids`, `snapshot`, `entry`                                                | Read the mempool                            |
| `Broadcaster`    | `broadcast`, `broadcast_package`, `test_accept`                             | Send transactions                           |
| `FeeEstimator`   | `estimate_fee(FeeTarget) -> Option<FeeRate>`                                | Fee estimates (`None` = no estimate yet)    |
| `TemplateSource` | `template`, `transactions`, `wait_next`, `submit_solution`                  | Mining work                                 |
| `EventSource`    | `subscribe() -> EventStream`                                                | Block and mempool events                    |

Design choices worth knowing:

- **`MempoolSource` is separate from `ChainSource`** because some backends
  (REST, IPC) can serve chain data but not mempool data.
- **`MempoolSource::snapshot()`** returns the txids *and* the mempool
  sequence number the node will assign next. That is what makes a gap-free
  mempool copy possible: subscribe to events, snapshot, then skip tx events
  whose sequence is below the snapshot's. Its default implementation calls
  `txids()` with no sequence; the RPC backend overrides it with
  `getrawmempool` + `mempool_sequence`. (Added after the E4 consumers
  needed it; see [§10](#10-consumers-proving-the-traits).)
- **Reorgs need no special method.** After `Lagged` (or at start), compare
  `block_hash(h)` against the hashes you hold, top down; the first match is
  the fork, found in depth + 1 calls. A `BlockConnected` whose parent isn't
  your tip means walking `header(h).prev_blockhash` back to a block you
  know.

### Mining types: why `Template` is lean

The mining critical path is "new tip → fresh work on the hashers". A
Stratum v2 style pool needs only the header fields, coinbase requirements
and coinbase merkle path for that. So:

- **`Template`** holds `prev_hash`, `height`, `version`, `bits`,
  `min_time`, `cur_time`, a `Coinbase` (what Core requires of the
  coinbase: version, sequence, scriptSig prefix, witness reserved value,
  `reward_remaining`, `required_outputs`, lock time), and
  `coinbase_merkle_path`. **No transactions.**
- **`transactions(&tmpl)`** fetches/decodes the transactions only when
  asked, avoiding decode, allocation and (on IPC) a full-block transfer that
  a miner may never need.
- **Submission takes a `Solution`** (`version`, `time`, `nonce`, complete
  `coinbase`), not a `Block`: a miner that never fetched the transactions
  cannot build one, and IPC's `submitSolution` doesn't want one anyway.
- **`wait_next(&prev, opts)` returns the new template**, because both
  longpoll and IPC `waitNext` already produce it; returning `()` would
  force a second fetch.
- **`Template` carries its own `TemplateToken`**, an opaque backend handle
  (`Arc<dyn Any + Send + Sync>`). Both backends are per-template (longpoll
  needs the id of the template you fetched; IPC calls methods on a
  `BlockTemplate` capability), so a context-free `wait_next` would break as
  soon as a pool holds two templates.
- **Pure helpers** build the block from a template, with no I/O:
  `Template::coinbase_tx(payout, extra_nonce)`, `merkle_root(&coinbase)`
  and `header(&solution)`. One rule on both backends: your outputs first,
  then `required_outputs` (the witness commitment), exactly once.

### Events

```rust
#[non_exhaustive]
pub enum NodeEvent {
    BlockConnected(BlockHash),
    BlockDisconnected(BlockHash),
    TxAdded   { txid: Txid, mempool_sequence: Option<u64> },
    TxRemoved { txid: Txid, mempool_sequence: Option<u64> },
    Lagged,   // events were lost: resync from ChainSource::tip()
}
```

Event streams are lossy by nature (ZMQ high-water marks, reconnects, slow
consumers). Every `EventSource` emits `Lagged` when it knows it dropped
something; consumers treat it as "resync", never as fatal. Block events
carry only a hash because ZMQ carries only a hash; adding a height would
cost an RPC call per event.

### `serde` feature

Off by default. Adds `Serialize`/`Deserialize` on every api type, for the
gateway's JSON and for consumers that log or forward values. Enums
are tagged `{"type": "snake_case_variant", ...}`; hashes, scripts and
transactions are hex; amounts are satoshis. `Template` is `Serialize` only,
since its token is a live backend handle. **The JSON shape is v1 and
frozen**: the gateway's REST and WebSocket clients see it, so changing it is
a breaking change (DESIGN.md §3.5).

---

## 7. `bitcoin-core-rpc`: the client

`crates/bitcoin-core-rpc`. Async JSON-RPC over HTTP (`reqwest` with
rustls), one typed method per RPC, plus the trait implementations.

### Modules: one file per Core section

```
src/
├── lib.rs              re-exports: Client, Wallet, Auth, Version, Feature, model, options
├── client.rs           Client, Builder, connect, with_version, raw_call, batch, versioned! macro
├── auth.rs             Auth::{None, UserPass, Cookie}, shared credentials, cookie re-read
├── methods.rs          method_class table, RetryPolicy
├── batch.rs            get_block_hashes, get_blocks
├── version.rs          Version, Feature
├── core.rs             Core-shaped JSON structs where corepc has no model type
├── traits.rs           impl of every bitcoin-node-api trait for Client
├── blockchain.rs       == Blockchain ==
├── control.rs          == Control ==
├── mining.rs           == Mining ==
├── network.rs          == Network ==
├── raw_transactions.rs == Rawtransactions == (all but fundrawtransaction)
├── signer.rs           == Signer ==
├── util.rs             == Util ==
├── zmq.rs              == Zmq ==
├── regtest.rs          generate*, invalidate/reconsiderblock, setmocktime, echo…
└── wallet/
    ├── mod.rs          Wallet, Loaded, Client::wallet, URL routing, create/load/restore/migrate/list
    ├── options.rs      options structs and builders for the wallet RPCs
    ├── lifecycle.rs    unload, info, backup, flags, keypool, encryption
    ├── addresses.rs    new/change addresses, address info, labels
    ├── coins.rs        balances, listunspent, locks, groupings
    ├── history.rs      gettransaction (+ verbose), list*, getreceivedby*
    ├── spending.rs     send*, PSBTs, bumpfee, abandon, simulate, fundrawtransaction
    └── descriptors.rs  import/list descriptors, HD keys, rescan, pruned funds, signmessage
```

The split follows Core's own `help` sections so every method is easy to find
by comparing against Core's documentation.

### `Client`

```rust
impl Client {
    pub fn builder(url: &str) -> Builder;                               // timeouts, http client, retry
    pub async fn connect(url: &str, auth: Auth) -> ApiResult<Client>;   // detects the version
    pub fn with_version(url: &str, auth: Auth, version: Version) -> Client; // caller asserts it
    pub async fn batch<T>(&self, method: &str, params: Vec<Value>) -> ApiResult<Vec<ApiResult<T>>>;
    pub fn version(&self) -> Version;
    pub fn supports(&self, f: Feature) -> bool;
    pub fn wallet(&self, name: &str) -> Wallet;
    pub async fn raw_call<T: DeserializeOwned>(&self, method: &str, params: Value) -> ApiResult<T>;
    // …then one method per RPC
}
```

`Client` is `Clone` and cheap to clone (it shares one `reqwest::Client` and
its connection pool). `with_version` exists so tests skip the
`getnetworkinfo` round trip and so a fixed deployment can pin a version.

### Naming and signatures

- **Method name = RPC name in snake_case**: `getblockstats` →
  `get_block_stats`, `walletcreatefundedpsbt` → `wallet_create_funded_psbt`.
  No renaming cleverness, so everything is greppable against Core's docs.
- **Inputs use `bitcoin::*` types** (`BlockHash`, `Txid`, `&Transaction`,
  `Address`, `Amount`, `FeeRate`), never `&str` for something that has a type.
- **Required params are positional; optional ones are `Option<T>`**, or an
  options struct when there are many.
- **Flags that change the shape of the reply become separate methods**:
  `get_block(hash) -> Block`, `get_block_verbose_one(hash)`,
  `get_block_verbose_two(hash)`, …, rather than an enum return type.

### Response types and version dispatch

Response types come from [`corepc-types`](https://crates.io/crates/corepc-types)
rather than being hand-written:

- **Public return types are `model::*`** (corepc's version-agnostic structs
  using `rust-bitcoin` types, re-exported as `bitcoin_core_rpc::model`) **or
  plain `bitcoin::*`** types. Never a version-specific `vN` type.
- Internally, each method deserializes into the `v29`…`v31` struct that
  matches the connected node and converts it with `into_model()`. A small
  `versioned!` macro keeps each method to a few lines and is the only place
  the supported range is spelled out.
- **Hex-capable RPCs** (`getblock 0`, `getblockheader false`,
  `getrawtransaction false`, `gettxoutproof`) skip corepc and go straight to
  `bitcoin::consensus::deserialize_hex`: smaller, faster, no version branch.
- About 33 RPCs have no corepc `model` type (`getpeerinfo`,
  `listdescriptors`, `getzmqnotifications`, …). These return the `v31`
  struct, re-exported from `bitcoin_core_rpc::core` and documented as
  Core-shaped JSON. If a method's JSON changed within 29–31, `core` defines
  its own struct that parses every supported version.

### Versions and features

```rust
pub struct Version(pub u32);   // getnetworkinfo.version, e.g. 310100; major() = 31

#[non_exhaustive]
pub enum Feature { SubmitPackage, ClusterMempool, PrivateBroadcast, TxVersionArg }
```

Methods that exist only on some versions check `supports()` and return
`NodeAPIError::Unsupported("… needs Core 31+")` instead of letting Core
answer with a bare "method not found". `connect` rejects Core < 28 with
`Unsupported`. A `Feature` is only added when a method actually branches on it.

### Wallets

```rust
let w = client.wallet("hot");           // routes to <url>/wallet/hot
let loaded = client.load_wallet("cold", None).await?; // Loaded { wallet, warnings }
```

- **Every wallet call is routed to the wallet's own endpoint**
  (`/wallet/<name>`, the name percent-encoded as one path segment so names
  with spaces, `/` or non-ASCII work). Nothing relies on "exactly one
  wallet loaded", which would otherwise fail with `-19` as soon as a second
  wallet loads.
- 57 wallet RPCs as 58 typed methods (`gettransaction` has a verbose
  twin), descriptor wallets only. `fundrawtransaction` lives on `Wallet`
  even though Core lists it under raw transactions, because it needs a wallet.
  `descriptorprocesspsbt` stays on `Client`, because it works with
  `-disablewallet`.
- RPCs with many optional arguments take an **options struct**
  (`CreateWalletOptions`, `FundOptions`, `SendOptions`, `SendAllOptions`,
  `SendToAddressOptions`, `SendManyOptions`, `BumpFeeOptions`,
  `ListUnspentQuery`, `ImportDescriptor`): `#[non_exhaustive]`, `Default`,
  one setter per field, so Core can add options without breaking callers.
  Unset options are left out of the request (never sent as `null`), fee
  rates go as sat/vB, deprecated no-op flags are never sent.
- **Passphrases** are `&str`, never stored by `Wallet`, and hidden from `Debug`.

### Trait implementations on `Client`

`traits.rs` implements every `bitcoin-node-api` trait over JSON-RPC:

- `ChainSource`: `getblockchaininfo`, `getblockhash`, `getblockheader`,
  `getblock`, `getrawtransaction`, `gettxout` (with `-5` → `None`).
- `MempoolSource`: `getrawmempool`, `getmempoolentry`, and `snapshot()` via
  `getrawmempool` with `mempool_sequence`.
- `Broadcaster`: `sendrawtransaction`, `submitpackage`, `testmempoolaccept`.
- `FeeEstimator`: `estimatesmartfee`; an `errors` array (no estimate yet)
  becomes `Ok(None)`.
- `TemplateSource`: `getblocktemplate` with segwit rules. The coinbase
  merkle path is computed from the GBT txids; the raw transactions stay in
  the token, still hex, until `transactions()` decodes them.
  `submit_solution` rebuilds the full block from the token and calls
  `submitblock`.
- `EventSource`: a background task polling `waitfornewblock` (10 s per
  poll). Blocks only, no tx events. A new tip that isn't a direct child of
  the previous one (a reorg, or several blocks at once) becomes `Lagged`.
  For low-latency or mempool events, use `bitcoin-core-zmq`.

### Wire details

- Requests are JSON-RPC 2.0. Core 28+ returns HTTP 200 with an `error`
  object on RPC errors, so the body is parsed regardless of status.
  401/403 → `TransportError::Auth`; timeout → `Timeout`; connection failure
  → `Connect`; any other non-JSON body → `Http(status)`.
- Basic auth on every request. `Auth::cookie(path)` reads Core's `.cookie`
  when the client is built and again after a 401/403; if it changed, that
  request is sent once more. Clones and `Wallet`s share the credentials.
- 30 s default timeout (`Builder::timeout`); blocking or scanning methods (`waitfor*`,
  `scantxoutset`, `gettxoutsetinfo`, `rescanblockchain`,
  `importdescriptors`, `generate*`, `loadwallet`, …) and GBT longpolls get 10
  minutes (`Builder::long_timeout`).
- Batches: one JSON array per chunk, replies matched by `id` (any order).
  A batch whose every item is `-28` is retried like a single call.

### The longpoll problem, and how it is solved

A `getblocktemplate` longpoll holds one of Core's `rpcthreads` (default 4)
for its whole wait, and Core does **not** notice a dropped HTTP connection:
the loop in `rpc/mining.cpp` exits only on a tip change, a mempool check
(first after 1 minute, then every 10 s) or shutdown. A client-side timeout
therefore does not free the thread, and a few abandoned longpolls can starve
the node of RPC threads, making a `submitblock` queue behind them.

So the RPC backend keeps **at most one longpoll in flight per
`longpollid`**, in a spawned task whose result is published on a
`tokio::sync::watch`. Every `wait_next(prev, opts)` caller subscribes with
its own deadline; a timeout returns `None` and leaves the request running,
and the next `wait_next` on the same template reuses it. Operators sharing
a node with anything else should still raise `rpcthreads`.

### Differences between template backends (documented on the trait)

Over RPC, `wait_next` ignores `fee_threshold` (GBT has no equivalent), Core
holds a longpoll ≥ 60 s after a mempool change, and `getblocktemplate` may
serve a cached template for up to 5 s. Over IPC both are immediate. A miner
on RPC should rely on block events, not on fee wake-ups.

---

## 8. `bitcoin-core-zmq`: push events

`crates/bitcoin-core-zmq`. Core's ZMQ notifications as an `EventSource`,
built on the pure-Rust [`zeromq`](https://crates.io/crates/zeromq) crate,
so there is no `libzmq` to install.

```rust
let zmq = ZmqEvents::connect("tcp://127.0.0.1:28333", Topic::Sequence).await?;
let events = zmq.subscribe().await?;   // ZmqStream: Stream<Item = ApiResult<NodeEvent>>
```

How and why:

- **Topic `sequence`, not `hashblock` + `hashtx`.** `hashtx` also fires for
  every transaction in a connected block, so it cannot mean "added to the
  mempool". `sequence` carries all four kinds in one ordered stream with one
  counter: `C`/`D` → `BlockConnected`/`BlockDisconnected`, `A`/`R` →
  `TxAdded`/`TxRemoved` with the mempool sequence. `Topic::HashBlock` is a
  fallback for nodes without `-zmqpubsequence` (block connects only).
- **Frames** are `[topic, body, u32 LE counter]`; hashes arrive in display
  order and are reversed. A malformed body is a `Decode` error in the
  stream but still advances the counter.
- **Each `subscribe()` gets its own socket and reader task**, feeding a
  bounded channel (4096 events by default, `with_capacity` to change) with
  `try_send`. The reader never blocks on the consumer, so Core's send
  buffer can't overflow because a consumer is slow.
- **`Lagged`** is emitted for a gap in Core's counter, a disconnect (the
  socket reconnects by itself, 100 ms doubling to 30 s), or a full channel.
  One `Lagged` is owed and delivered before the next event, or promptly
  on its own; dropped events are not queued.
- **Sync after subscribing, never before.** ZMQ drops anything published
  before the subscription reaches Core, so there is no `Lagged` at start.
  The recipe: `subscribe()`, then `tip()`, then for a mempool copy
  `MempoolSource::snapshot()`, keeping tx events whose sequence is at or
  above the snapshot's.
- **Mined transactions leave silently**: Core sends `R` only for
  non-block removals (replaced, expired, evicted), so a mempool copy must
  also remove the transactions of each connected block.
- Endpoints come from `Client::get_zmq_notifications()`; a `0.0.0.0` bind
  must be rewritten to the node's address. The crate deliberately does not
  depend on `bitcoin-core-rpc`.

---

## 9. `bitcoin-core-ipc`: mining over Cap'n Proto

`crates/bitcoin-core-ipc`. Core 30 and 31's multiprocess mining interface
over a Unix socket, implementing `TemplateSource` and `ChainSource::tip`.

Start Core's multiprocess node with a socket:

```sh
bitcoin -m node -ipcbind=unix            # or libexec/bitcoin-node -ipcbind=unix
# listens on <datadir>/<chain>/node.sock
```

### Two protocols

Core 31 renumbered methods (`makeMining` @2→@3, `submitSolution` @9→@7),
added a context argument to `createNewBlock` and `checkBlock`, and turned
`getCoinbaseTx` from a placeholder transaction into a `CoinbaseTx` struct.
So each version has its own generated code, and `version.rs` shares the
common parts with a macro. `IpcClient::connect` asks for Core 31's
`makeMining` first and falls back to Core 30's on `Unimplemented`;
`core_major()` says which one is in use.

### Schemas and generated code

`schema/v30/` and `schema/v31/` hold verbatim copies of Core's
`src/ipc/capnp/*.capnp` and libmultiprocess' `mp/proxy.capnp` at the
matching tag. The generated Rust in `src/generated/` is **committed**, so
building the crate needs no `capnp` compiler. It is regenerated with
`cargo run -p ipc-codegen`, and CI checks the committed code matches byte
for byte (see [§13](#13-continuous-integration)). `bitcoin-capnp-types` was
not used because it runs `capnp` in every build.

### The worker thread

`capnp-rpc` types are not `Send`. So one dedicated OS thread with a tokio
`LocalSet` owns the connection; `IpcClient` is a cheap `Send + Sync` handle
that sends boxed jobs to it over an unbounded channel, each run with
`spawn_local`, so calls run concurrently on the worker. Templates stay on
the worker, keyed by a `u64` id. The `TemplateToken` holds that id and sends
`Release(id)` when its last clone drops, so Core can free the template. A
token from a different `IpcClient` is refused.

### Core threads

Every IPC call names a Core-side thread in its context. Core 30 refuses a
call on a thread that is already running one ("thread busy"), so each call
leases an idle thread from a pool and creates a new one when all are busy.
This is also what keeps a long `waitNext` from blocking a `submitSolution`.

### Mapping to `TemplateSource`

- `template()`: `createNewBlock`, with `getBlockHeader`, `getCoinbaseTx`
  (plus `getCoinbaseCommitment` on Core 30) and `getCoinbaseMerklePath`
  **pipelined** on its promised result: one round trip. Height comes from
  the coinbase's BIP 34 push; `min_time` is the header's time (IPC sends no
  minimum). Core 30's placeholder coinbase is reshaped into `Coinbase`.
- `transactions()`: `getBlock` + `getTxFees` + `getTxSigops`.
- `wait_next()`: `waitNext` with both options always set (Core 30's schema
  defaults them to 0); a null result is `None`.
- `submit_solution()`: `submitSolution` with the complete coinbase; `false`
  is a rejection, "already known" is `Ok`.
- `ChainSource::tip` uses `getTip`; the other chain methods return
  `Unsupported`.
- `EventSource`: `waitTipChanged` in a loop (10 s per call), tip only. One
  block higher is `BlockConnected`; any other tip change is `Lagged` (IPC
  gives no parent hash, so resync from `tip()`). A transport error ends the
  stream.

Measured on empty regtest: warm `template()` is ~1.0–1.2 ms over IPC vs
~0.9 ms over RPC. IPC's advantage is in `wait_next` reacting immediately to
new tips and fee changes, and in not paying for transactions you don't fetch.

`bitcoin_core_sv2` was read but not wrapped: its API is Stratum v2 messages
and it pulls in `stratum-core`, tokio "full" and `tracing-subscriber`.

---

## 10. `consumers`: proving the traits

`crates/consumers` (not published). Traits are only good if real code can
be written against them, so two consumers were written using **only** the
`bitcoin-node-api` traits; their binaries are the only place a backend is
named.

- **`feed`** (`src/feed.rs`, binary `template-feed`): a miner's loop.
  Builds the coinbase and header with `Template`'s helpers, grinds a nonce,
  submits, and learns of new work from `wait_next` or a block event,
  whichever comes first.

  ```sh
  cargo run -p consumers --bin template-feed -- \
      --rpc http://127.0.0.1:18443 --user u --pass p \
      [--ipc <datadir>/regtest/node.sock] [--zmq tcp://127.0.0.1:28333] \
      [--mine N --payout <address>]
  ```

- **`follower`** (`src/follower.rs`, binary `chain-follower`): follows the
  best chain and mempool the way a wallet backend would, handling reorgs by
  walking `prev_blockhash`.

  ```sh
  cargo run -p consumers --bin chain-follower -- \
      --rpc http://127.0.0.1:18443 --user u --pass p \
      [--zmq tcp://127.0.0.1:28333] [--start HEIGHT]
  ```

Both run live on Core 29–31, with templates over RPC and IPC and events over
ZMQ and RPC polling. What they exposed fed back into the API (stage E4,
detailed in `DESIGN.md` §3.3 "E4 findings"):

1. Building the block from a `Template` was everyone's job → pure helpers
   `coinbase_tx`, `merkle_root`, `header` on `Template`.
2. `MempoolSource` couldn't give a gap-free snapshot →
   `MempoolSource::snapshot()` with a sequence.
3. Block events carry only a hash → documented; consumers confirm with `tip()`.
4. Reorg detection needs no new method → both patterns documented on
   `ChainSource`.
5. Template backends differ in timing → documented on `TemplateSource`.
6. Mined transactions leave the mempool silently → sync recipe documented
   on `MempoolSource`.

They also found a real bug: `bitcoin-core-zmq` delivered an owed `Lagged`
only with Core's next message, possibly minutes after a burst.

---

## 11. Support tooling: `test-node`, `ipc-codegen`, scripts

### `test-node`

A throwaway regtest node for live tests. `TestNode::start(extra_args)`
launches `bitcoind` from `$BITCOIND_EXE` (else `PATH`) on a fresh datadir
and free port, with `-txindex -blockfilterindex` and no P2P; the process and
datadir are removed on drop. `TestNode::start_ipc` launches the same
release's `libexec/bitcoin-node -ipcbind=unix` and returns `None` before
Core 30. `log_tail()` helps diagnose failures.

### `ipc-codegen`

`cargo run -p ipc-codegen` regenerates `bitcoin-core-ipc/src/generated/vNN/`
from `bitcoin-core-ipc/schema/vNN/` using `capnpc`. It needs the `capnp`
compiler on `PATH`, any version (e.g. `brew install capnp` or
`dnf install capnproto`); the version is recorded in each generated file's
header.

### `grpc-codegen`

`cargo run -p grpc-codegen` regenerates the gateway's gRPC code
(`bin/bitcoin-gateway/src/grpc/generated/`) from its proto with
`tonic-prost-build`. It brings its own `protoc` (`protoc-bin-vendored`), so
it needs nothing installed and its output depends only on `Cargo.lock`.

### `scripts/fetch-bitcoind.sh`

```sh
scripts/fetch-bitcoind.sh 31.2   # prints the path of the bitcoind binary
```

Downloads a Bitcoin Core release (Linux x86_64) into
`$BITCOIND_DIR` (default `target/bitcoind`), checks it against a pinned
SHA256 (taken from the release's `SHA256SUMS`, checked against the Guix
builder signatures when added), and prints the `bitcoind` path. Pinned
versions: 29.5, 30.4, 31.0, 31.2. A new version needs a new hash
line.

---

## 12. Testing strategy

The goal: every typed method is checked against a real Core reply, on every
supported version, without requiring a node for everyday `cargo test`.
Three layers make that work.

### 1. Fake bitcoind (offline, fast)

`crates/bitcoin-core-rpc/tests/common/mod.rs` is a small hand-written
HTTP/1.1 responder on a `TcpListener` that maps method → canned JSON and
counts requests (needed to prove that two `wait_next` callers share one
longpoll). Fixtures in `tests/fixtures/*.json` are real Core 31 output.

- `tests/rpc.rs`: the trait implementations, happy and error paths.
- `tests/typed.rs`: per-version dispatch, the `core` structs, methods that
  build non-trivial params.
- `tests/wallet.rs`: `/wallet/<name>` routing, the params each wallet
  method builds, replies from different Core versions.

### 2. Live (real Core, `#[ignore]`)

`tests/live.rs` starts a real node with `test-node` and runs
`tests/scenario/`, which calls **every** typed method, building chain and
wallet state with the regtest RPCs. The scenario is version-aware: on older
Core it expects `Unsupported` or "method not found" for newer RPCs. The
wallet scenario runs on its own node with `-fallbackfee`. Other crates have
their own live tests (ZMQ: RBF replacement, mining, `invalidateblock`; IPC:
templates, `wait_next` timeouts and fee wake-ups, mining a block built from
the merkle path, bad PoW rejected, a submit during a pending wait; consumers:
both loops end to end).

### 3. Replay (offline, from real Core)

With `RECORD=1` on Core 31, the live test saves every request and reply to
`tests/fixtures/core31/session.json` and `wallet-session.json`.
`tests/replay.rs` runs the same scenario against the fake, serving the
recorded replies by path + method + params. So in plain `cargo test`, every
typed method parses a real Core 31 reply and sends exactly the params Core
accepted. It also fails if a `pub async fn` is missing from the scenario,
so a new method can't land untested.

### Other unit tests

- `bitcoin-node-api/tests/serde.rs`: JSON shapes and round-trips.
- `bitcoin-node-api/tests/template.rs`: the block-building helpers.
- `bitcoin-core-zmq/tests/fake.rs`: a `PubSocket` stands in for Core (all
  labels, byte order, gaps, counter wrap, slow consumer, bad bodies,
  publisher restart).
- `bitcoin-node-ldk/tests/mock.rs`: LDK's `synchronize_listeners` and
  `SpvClient` on the mock, through reorgs.
- `bitcoin-node-bdk/tests/mock.rs`: `bdk_bitcoind_rpc`'s emitter scenarios
  (reorgs, start heights, mempool re-emission and evictions) and a
  `bdk_wallet` through a payment, a reorg and an eviction, on the mock;
  `tests/filters.rs`: filter sync fetching only matching blocks through
  reorgs, and ending where block-by-block sync does. Live, the mock's
  filters are checked byte for byte against Core's.
- `bin/bitcoin-gateway/tests/`: config, auth, scopes, fan-out, listeners,
  every gRPC method, REST route and WebSocket feature, against the mock
  (and a fake Core for the pass-through).
- Unit tests inside the crates (merkle path vs block root, Core 30
  coinbase reshaping, BIP 34 heights, `RpcErrorCode` round-trips…).

### Fuzzing (the gateway)

`bin/bitcoin-gateway/fuzz` has libFuzzer targets for everything the gateway
parses from outside: the config file, scope strings, WebSocket messages and
REST requests (method, path, headers, body), the last two through the real
handlers on the mock. CI runs each for 30 seconds; longer runs are local
(`cargo +nightly fuzz run <target>`, see its README). The first long run
found that `wallet::read`, a scope no route can use, was accepted.

### Load (the gateway)

`bin/bitcoin-gateway/tests/load.rs` (`#[ignore]`) runs the gateway binary in
front of a regtest node and puts it under concurrent clients: mixed REST
reads (twice, checking memory doesn't grow between identical rounds), a key
over its `max_in_flight`, a connection flood from several addresses, 16 SSE
subscribers plus one too many, and WebSocket pass-through.

```sh
BITCOIND_EXE=$(scripts/fetch-bitcoind.sh 31.2) \
  cargo test --release -p bitcoin-gateway --test load -- --ignored --nocapture
```

One run on a 16-thread laptop, Core 31.2,
default limits except `max_connections = 256`:

| Phase                                   | Result                                                         |
| --------------------------------------- | -------------------------------------------------------------- |
| REST reads, 64 clients                  | ~22,000 req/s, p50 2.9 ms, p99 3.4 ms, no errors               |
| A key with `max_in_flight = 4`, 32 clients | ~7,400 req/s served, the rest 429 at once; no 5xx           |
| 100 connections from one IP, 512 from 8 more | 36 and 320 refused; 256 open; new clients refused until it ends |
| 16 SSE subscribers                      | every block to every one within ~10 ms; the 17th gets 429      |
| WebSocket `getblockcount`, 16 connections | ~21,000 calls/s, p99 1.2 ms                                  |
| Memory                                  | 12 MiB idle, 22 MiB under load, flat between rounds            |

REST reads are bounded by `node_max_in_flight` (8 calls to Core at a time):
the latency is mostly the wait for a slot, which is the point.

### Running them

```sh
cargo test                       # unit tests, fakes, and the recorded Core 31 replay
cargo test --all-features        # also the serde tests
```

Live tests (Linux x86_64), for one crate or the whole workspace:

```sh
BITCOIND_EXE=$(scripts/fetch-bitcoind.sh 31.2) \
  cargo test -p bitcoin-core-rpc --test live -- --ignored

BITCOIND_EXE=$(scripts/fetch-bitcoind.sh 31.2) \
  cargo test --workspace --test live -- --ignored --nocapture
```

The IPC live tests skip themselves before Core 30. To refresh the replay
fixtures after changing the scenario:

```sh
RECORD=1 BITCOIND_EXE=$(scripts/fetch-bitcoind.sh 31.2) \
  cargo test -p bitcoin-core-rpc --test live -- --ignored
```

---

## 13. Continuous integration

`.github/workflows/ci.yml` runs these jobs on every push to `main` and every
pull request:

| Job       | What it does                                                                                                                            |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `check`   | `cargo fmt --check`, `cargo clippy --all-targets --all-features -D warnings`, `cargo test --all-features` (fakes + replay)              |
| `codegen` | Reads the `capnp` version from the generated files' headers, builds that exact `capnp` (cached), runs `ipc-codegen`, fails on any diff |
| `grpc-codegen` | Runs `grpc-codegen` (vendored `protoc`) and fails on any diff in the gateway's generated gRPC code                          |
| `msrv`    | `cargo check --workspace --all-targets --all-features` on Rust 1.89, so `rust-version` stays true                                       |
| `fuzz`    | The gateway's fuzz targets on nightly, 30 seconds each                                                                                  |
| `live`    | Matrix over Core 29.5, 30.4, 31.2: downloads (cached) and runs every crate's live tests                                          |

A second workflow, `release.yml`, publishes on `v*` tags: the gateway's
release binaries and container image (see §16, Operations). On a pull
request that touches the packaging, or run by hand, it is a dry run: the
same builds and image, smoke-tested, nothing published.

The `codegen` job exists because different `capnp` versions encode schemas
differently. Rather than pinning a version for contributors, CI follows
whatever version produced the committed files: after upgrading `capnp`
locally, just regenerate and commit.

---

## 14. Development workflow

- **Build**: `cargo build` (no system dependencies; `capnp` only needed to
  regenerate IPC code).
- **Before committing**: `cargo fmt --all`,
  `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`.
- **Adding an RPC method**: add it to the file for its Core section, use
  `versioned!` (or hex decoding) for the reply, add it to
  `tests/scenario/`, re-record on Core 31 with `RECORD=1`, and run the live
  test on the other versions. The replay test fails until the scenario
  covers it.
- **Changing an IPC schema / adding a Core version**: copy the schemas into
  `schema/vNN/`, add `vNN` to `ipc-codegen`'s list, `cargo run -p
  ipc-codegen`, wire it into `version.rs`, commit the generated code.
- **Changing the gateway's proto**: edit
  `bin/bitcoin-gateway/proto/bitcoin/node/v1/node.proto`, run
  `cargo run -p grpc-codegen`, commit the generated code.
- **Adding a Core release to test**: add its hash to
  `scripts/fetch-bitcoind.sh` and to the CI matrix.
- **Generated code** under `crates/bitcoin-core-ipc/src/generated/` and
  `bin/bitcoin-gateway/src/grpc/generated/` is marked `linguist-generated`
  in `.gitattributes`, so it is collapsed in GitHub diffs.

---

## 15. How the project was built

The work was planned in [`DESIGN.md`](DESIGN.md) and carried out in stages,
each one a pull request, with the rule that the trait layer should become
usable early so the other backends could start.

| Stage | What                                                                                                   |
| ----- | ------------------------------------------------------------------------------------------------------ |
| —     | Research: why the library should exist, prior art, the full RPC inventory (`RpcInventory.md`)          |
| A     | Foundation: CI, error types, `Client`/`Auth`/`Version`/`Feature`, the fake bitcoind and fixtures       |
| B     | The `core` methods: every trait implemented over RPC, including the shared GBT longpoll                |
| C     | Typed methods by section (blockchain, mining, network, control, zmq, raw tx, util, signer, regtest), then live tests against Core 31 with record/replay |
| E1    | `bitcoin-core-zmq`                                                                                     |
| E2    | Live tests on Core 28, 29, 30, 31 in CI                                                                |
| E3    | `bitcoin-core-ipc` and `ipc-codegen`                                                                   |
| E4    | The two consumers; their findings applied back to `bitcoin-node-api`                                   |
| D     | The wallet: `Wallet` handle and lifecycle (D1), addresses/coins/history (D2), spending/PSBTs/descriptors (D3) |
| F–H   | Phase 1.5: method classes and retries, batching, the mock, `tracing`, docs and release prep            |
| I     | Gateway skeleton: config, hashed tokens, scopes, the composite backend, event fan-out, listeners and TLS |
| J     | gRPC: the proto and committed codegen, one handler per trait method, events, health, reflection        |
| K     | REST (encodings, caching, SSE, OpenAPI) and the WebSocket pass-through; the `serde` shape frozen as v1 |
| L     | Metrics, health and readiness, the live gateway test on Core 28–31, release packaging                  |
| M     | `bitcoin-node-ldk`: LDK's `BlockSource` over the traits, and what it taught about them                 |
| —     | Hardening: `header_info`, gateway limits, fuzzing, a load test                                          |
| N     | `bitcoin-node-bdk`: BDK's emitter and filter sync over the traits; `ChainSource::block_filter`        |

Every step followed the same loop: write the code, add a fixture and a test,
`cargo test && cargo clippy --all-targets`, commit. Facts about Core were
taken from the release binaries' `help` output and from running them, not
from memory, and where a live run disagreed with the plan, the code
followed Core and the design document recorded why. Some examples of what
the live runs taught:

- Creating a wallet that already exists is `-4`, not `-36`.
- `lockunspent` unlocks everything only when the outpoint list is omitted,
  not when it is `[]`.
- `send` and `sendall` return a `txid` whenever the transaction is
  complete, even with `add_to_wallet: false`.
- A 0-input transaction (what `fundrawtransaction` takes) didn't
  round-trip between rust-bitcoin and Core; fixed in both directions.
- corepc-types mismatches (`unloadwallet` returning `{warnings}`,
  verbose `gettransaction` sending `decoded` as an object) are handled in
  the client.
- ZMQ's mempool sequence from `getrawmempool` is the *next* one Core
  assigns, so events at or above it are new.

---

## 16. `bitcoin-gateway`: Core over gRPC, REST and WebSocket

`bitcoind` speaks JSON-RPC, REST, ZMQ and IPC, not gRPC or WebSocket. The
gateway is one binary built on these crates: it talks to Core on one side
and serves gRPC, REST and WebSocket on the other, behind per-key
permissions, so port 8332 stays private. The full plan is
[`PHASE_2.md`](PHASE_2.md).

Library before gateway: wallets, Lightning nodes and pools embed the
library. The gateway is for those that can't or won't: other languages,
browsers, several services sharing one node, operators who want per-client
permissions. It is one gateway in front of one Core, not a load balancer
or a cache across nodes.

### Running it

```sh
bitcoin-gateway hash-token                  # make a token for a [[key]]
bitcoin-gateway -c gateway.toml check-config
bitcoin-gateway -c gateway.toml             # --grpc/--http/--metrics override [listen]
```

[`bin/bitcoin-gateway/gateway.example.toml`](bin/bitcoin-gateway/gateway.example.toml)
documents every setting. A config error stops startup with the line that's
wrong (``gateway.toml:8: unknown scope `chain:raed` ``), and so does a wallet
scope naming a wallet Core doesn't have. `SIGHUP` reloads the `[[key]]`
entries and the TLS certificate; listener changes need a restart. On
`SIGTERM` the gateway stops accepting, gives calls in flight 10 seconds,
and ends event streams cleanly.

### Keys and tokens

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

### Scopes

Every scope is split read/write, so nobody has to put one scope spanning
`getbalance` and `sendtoaddress` in front of a hot wallet:

| Scope                     | Unlocks                                                                 |
| ------------------------- | ----------------------------------------------------------------------- |
| `chain:read` / `:write`   | blockchain reads / `invalidateblock`, `reconsiderblock`, `pruneblockchain`… |
| `mempool:read`            | mempool reads                                                           |
| `broadcast`               | `sendrawtransaction`, `submitpackage`, `testmempoolaccept`              |
| `fees:read`               | `estimatesmartfee`                                                      |
| `events`                  | event streams                                                           |
| `mining:read` / `:write`  | `getblocktemplate`, `getmininginfo` / `submitblock`, `prioritisetransaction` |
| `network:read` / `:write` | `getpeerinfo`… / `addnode`, `setban`, `disconnectnode`…                 |
| `util:read`               | `validateaddress`, `decoderawtransaction`, `deriveaddresses`…           |
| `wallet:<name>:read`      | reads on that wallet only                                               |
| `wallet:<name>:write`     | spending, signing, address and label changes on that wallet             |
| `admin`                   | everything, including the admin-only methods                            |

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

### The composite backend

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

### Event fan-out

The gateway opens one upstream subscription and copies it to every
client. Without that, every subscriber would open its own ZMQ socket or,
worse, hold one of Core's `rpcthreads` with its own `waitfornewblock` poll.
A client that falls behind gets `Lagged` and keeps going; upstream `Lagged`
is forwarded to everyone; if the upstream stream ends or fails, the
gateway resubscribes with backoff and sends everyone `Lagged`, since events
may have been lost. Clients resync with `tip()` (and `snapshot()` for a
mempool copy), the same recipe as the library.

### gRPC

Services mirror the traits, generated from
[`proto/bitcoin/node/v1/node.proto`](bin/bitcoin-gateway/proto/bitcoin/node/v1/node.proto):
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

### REST

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

### WebSocket: Core's JSON-RPC, passed through

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

### Listeners and TLS

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
  and each HTTP/1.1 request's headers (an idle keep-alive connection
  included) get `handshake_timeout_secs` (10). HTTP/2 connections are
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

A reload (`SIGHUP`) starts each key's counts afresh; calls and streams
already open keep running.

### Configuration reference

[`gateway.example.toml`](bin/bitcoin-gateway/gateway.example.toml) is the
annotated version. Every section but `[node]` is optional.

| Setting                     | Default         | Meaning                                                          |
| --------------------------- | --------------- | ---------------------------------------------------------------- |
| `[node] rpc_url`            | (required)      | Core's RPC, `http://` or `https://`                              |
| `[node] cookie`             |                 | Core's `.cookie`, re-read after a 401; or:                       |
| `[node] rpc_user`, `rpc_password` |           | `-rpcuser`/`-rpcpassword` or an `-rpcauth` user                  |
| `[node] zmq`                | none: RPC polling | Core's `-zmqpubsequence` endpoint, `tcp://…`                   |
| `[listen] grpc`, `http`, `metrics` | none     | addresses; `--grpc`, `--http`, `--metrics` override them         |
| `[http] cors_origins`       | `[]`            | origins that get CORS headers (REST, WebSocket, gRPC-Web)        |
| `[http] public_cache`       | `false`         | immutable responses `public` instead of `private`                |
| `[grpc] web`                | `false`         | also accept gRPC-Web                                             |
| `[ws] max_message_bytes`    | `1048576`       | largest WebSocket message; larger closes with 1009               |
| `[ws] max_in_flight`        | `16`            | calls one WebSocket connection may have running                  |
| `[events] buffer`           | `1024`          | events buffered per subscriber before it gets `Lagged`           |
| `[limits] max_connections`  | `1024`          | open connections per listener                                    |
| `[limits] max_connections_per_ip` | `64`      | open connections per client IP, per listener                     |
| `[limits] handshake_timeout_secs` | `10`      | TLS handshake, first bytes, each HTTP/1.1 request's headers      |
| `[limits] http2_max_streams` | `100`          | concurrent HTTP/2 streams (gRPC calls) per connection            |
| `[limits] max_subscriptions` | `16`           | event subscriptions per key, all APIs together                   |
| `[limits] key_max_in_flight` | `32`           | calls to Core per key at once                                    |
| `[limits] node_max_in_flight` | `8`           | calls to Core at once, all keys; below `-rpcthreads` + `-rpcworkqueue` |
| `[limits] node_queue_secs`  | `10`            | how long a call waits for one of those                           |
| `[tls] cert`, `key`         | none            | PEM files; every listener serves TLS; reloaded on `SIGHUP`       |
| `[[key]] name`              | (required)      | shown in logs and metrics                                        |
| `[[key]] token_sha256`      | (required)      | from `bitcoin-gateway hash-token`                                |
| `[[key]] allow`             | (required)      | scopes, see above                                                |
| `[[key]] rate`              | unlimited       | `{ per_second = 50, burst = 100 }`, shared by every API          |
| `[[key]] allow_unknown`     | `false`         | an `admin` key may call methods the gateway doesn't know         |
| `[[key]] max_in_flight`     | `key_max_in_flight` | this key's calls to Core at once                             |

### Operations

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
  with the reason otherwise. The gateway checks every 5 seconds, and the
  gRPC health service reports the same.
- **Logs** through `tracing` (`RUST_LOG`, default `info`), with the
  library crates' `tracing` feature on, plus one line per request with the
  key name, method, outcome and duration. Never params or results.
- **Upstream outages**: `Client` reconnects per request and ZMQ by itself;
  meanwhile calls fail with `UNAVAILABLE` / `503` / `-32003`, `/readyz`
  fails, and event streams get `Lagged` when the feed comes back. At
  startup the gateway waits for Core (and ZMQ), retrying with backoff.
- **Packaging**: the release workflow
  ([`.github/workflows/release.yml`](.github/workflows/release.yml)) builds
  Linux x86_64 and aarch64 binaries on a `v*` tag, attaches them to the
  GitHub release with the example config and a hardened
  [systemd unit](bin/bitcoin-gateway/packaging/bitcoin-gateway.service),
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
BIP 158 filters belong on a trait (PHASE_2.md §8).

---

## 17. Further reading

- [`DESIGN.md`](DESIGN.md): the complete design, every decision with its
  reason, and the findings from each stage.
- [`PHASE_2.md`](PHASE_2.md): the gateway's and the LDK adapter's plan,
  stage by stage.
- [`CHANGELOG.md`](CHANGELOG.md): what changed in the published crates.
- [`RpcInventory.md`](RpcInventory.md): every Core 31 RPC by section,
  classified `core` (backs a trait), `typed` (Core-specific) or `?`.
- Crate-level docs: `cargo doc --workspace --no-deps --open`.

---

## 18. License

MIT. See [`LICENSE`](LICENSE). The Cap'n Proto schemas under
`crates/bitcoin-core-ipc/schema/` are verbatim copies from Bitcoin Core and
libmultiprocess, also MIT.
