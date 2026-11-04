<div align="center">

# core-link

**Async Rust for Bitcoin Core: typed RPC, ZMQ, IPC mining, and a gRPC/REST/WebSocket gateway.**

[![CI](https://github.com/framicheli/core-link/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/framicheli/core-link/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.89-orange?logo=rust)](https://blog.rust-lang.org/)
[![Rust 2024](https://img.shields.io/badge/edition-2024-orange?logo=rust)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![Bitcoin Core](https://img.shields.io/badge/Bitcoin%20Core-29%20%7C%2030%20%7C%2031-f7931a?logo=bitcoin&logoColor=white)](https://bitcoincore.org/en/releases/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](#18-license)

</div>

---

## What's inside

| Crate                            | What it gives you                                                                                                                                                                                                     |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`bitcoin-core-rpc`**           | A complete, typed, async JSON-RPC client: one method per public RPC in Core 31 (164 RPCs across 9 sections), a `Wallet` handle, regtest/hidden RPCs, automatic Core version detection, and a `raw_call` escape hatch. |
| **`bitcoin-node-api`**           | Backend-agnostic capability traits (`ChainSource`, `MempoolSource`, `Broadcaster`, `FeeEstimator`, `TemplateSource`, `EventSource`) that libraries program against without caring how they reach the node.            |
| **`bitcoin-core-zmq`**           | Core's ZMQ `sequence` notifications as an `EventSource` stream.                                                                                                                                                       |
| **`bitcoin-core-ipc`**           | Core 30+'s Cap'n Proto mining interface as a `TemplateSource`, for miners who care about milliseconds.                                                                                                                |
| **`bitcoin-node-mock`**          | An in-memory node implementing all six traits: test your code without a `bitcoind`.                                                                                                                                   |
| **`bitcoin-node-ldk`**           | LDK's `lightning-block-sync` `BlockSource` over any `ChainSource`.                                                                                                                                                    |
| **`bitcoin-node-bdk`**           | BDK chain sync over any `ChainSource` + `MempoolSource`: block by block through reorgs, the mempool with evictions, or compact block filters, as the events `bdk_wallet` applies.                                     |
| **`bitcoin-gateway`** _(binary)_ | Core over gRPC, REST and WebSocket, behind per-key permissions ([§16](#16-bitcoin-gateway-core-over-grpc-rest-and-websocket)).                                                                                        |

## Supported Bitcoin Core

**29, 30 and 31**, each tested live in CI against the real release binary.
The rule: the major versions Bitcoin Core itself maintains (the latest
three). When Core ships a new major, it is added and the one Core retires is
dropped.

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

| Piece                 | State before this project                                                        | Here                                          |
| --------------------- | -------------------------------------------------------------------------------- | --------------------------------------------- |
| Async JSON-RPC client | None maintained                                                                  | `bitcoin-core-rpc`                            |
| Response types        | `corepc-types`: per-version structs plus a `model` layer in `rust-bitcoin` types | **Used, not forked**                          |
| IPC mining            | `bitcoin_core_sv2` (Sv2-shaped, heavy deps)                                      | `bitcoin-core-ipc`, written directly on capnp |
| Chain-source trait    | LDK, BDK, p2poolv2: each private to its project                                  | `bitcoin-node-api`                            |
| gRPC / WS gateway     | `btc-buf` (Go, early), `btc-rpc-proxy` (auth proxy)                              | `bitcoin-gateway`                             |

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
- Core < 29. `corepc-types` goes back to v17, so widening is cheap if asked.
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

|                | Wallet-like (wallets, LN nodes, indexers)                                   | Mining (pools, solo miners)                         |
| -------------- | --------------------------------------------------------------------------- | --------------------------------------------------- |
| Main job       | Sync chain state, find relevant transactions/UTXOs                          | Build a block template, submit blocks               |
| Key calls      | blocks/headers, `getrawtransaction`, `gettxout`, `sendrawtransaction`, fees | `getblocktemplate`, `submitblock`                   |
| Events         | New block, mempool transaction                                              | New tip, as fast as possible; template updates      |
| Latency        | Seconds are fine                                                            | Milliseconds matter (stale work is lost money)      |
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
- **Layer 2, the traits** are small and backend-agnostic (about 17 methods).
  This is what libraries program against, so they can swap `Client` for
  IPC, ZMQ, a mock, or a remote gateway.

### Backend support matrix

|                    |  Chain   | Mempool | Broadcaster | Fee |      Template      |                  Event                  |
| ------------------ | :------: | :-----: | :---------: | :-: | :----------------: | :-------------------------------------: |
| `bitcoin-core-rpc` |    ✓     |    ✓    |      ✓      |  ✓  | ✓ (GBT + longpoll) | ✓ (`waitfornewblock` poll, blocks only) |
| `bitcoin-core-zmq` |          |         |             |     |                    |                    ✓                    |
| `bitcoin-core-ipc` | tip only |         |             |     |         ✓          |     ✓ (`waitTipChanged`, tip only)      |

A typical deployment combines them: RPC for chain/mempool/broadcast/fees,
ZMQ for events, and IPC for templates when the node is Core 30+.

---

## 4. Workspace layout

```
core-link/
├── Cargo.toml              virtual workspace; shared deps and package metadata
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
  matching `corepc-types 0.16`. The two are bumped together. The LDK and
  BDK adapters are the exception: they use `lightning::bitcoin` and
  `bdk_core::bitcoin` on their ecosystem's side and convert at the boundary
  (a move while the versions match, a consensus re-encode when they don't),
  so LDK, BDK and this workspace can each bump `bitcoin` on their own.
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
bitcoin-core-rpc = { git = "https://github.com/framicheli/core-link" }
bitcoin-node-api = { git = "https://github.com/framicheli/core-link" }
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
fails fast on a bad URL, bad credentials, or a Core older than 29.

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

The error type, the `Fut<T>` alias, each trait, the mining types, events and the `serde` feature: [`crates/bitcoin-node-api/README.md`](crates/bitcoin-node-api/README.md).

---

## 7. `bitcoin-core-rpc`: the client

Modules, `Client`, naming, version dispatch, wallets, the trait implementations, wire details and the longpoll design: [`crates/bitcoin-core-rpc/README.md`](crates/bitcoin-core-rpc/README.md).

---

## 8. `bitcoin-core-zmq`: push events

Topics, framing, `Lagged`, and the sync recipe: [`crates/bitcoin-core-zmq/README.md`](crates/bitcoin-core-zmq/README.md).

---

## 9. `bitcoin-core-ipc`: mining over Cap'n Proto

The two protocols, schemas and codegen, the worker thread, Core threads and the `TemplateSource` mapping: [`crates/bitcoin-core-ipc/README.md`](crates/bitcoin-core-ipc/README.md).

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
ZMQ and RPC polling. What they exposed fed back into the API (stage E4):

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

| Phase                                        | Result                                                          |
| -------------------------------------------- | --------------------------------------------------------------- |
| REST reads, 64 clients                       | ~22,000 req/s, p50 2.9 ms, p99 3.4 ms, no errors                |
| A key with `max_in_flight = 4`, 32 clients   | ~7,400 req/s served, the rest 429 at once; no 5xx               |
| 100 connections from one IP, 512 from 8 more | 36 and 320 refused; 256 open; new clients refused until it ends |
| 16 SSE subscribers                           | every block to every one within ~10 ms; the 17th gets 429       |
| WebSocket `getblockcount`, 16 connections    | ~21,000 calls/s, p99 1.2 ms                                     |
| Memory                                       | 12 MiB idle, 22 MiB under load, flat between rounds             |

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

| Job            | What it does                                                                                                                           |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `check`        | `cargo fmt --check`, `cargo clippy --all-targets --all-features -D warnings`, `cargo test --all-features` (fakes + replay)             |
| `codegen`      | Reads the `capnp` version from the generated files' headers, builds that exact `capnp` (cached), runs `ipc-codegen`, fails on any diff |
| `grpc-codegen` | Runs `grpc-codegen` (vendored `protoc`) and fails on any diff in the gateway's generated gRPC code                                     |
| `msrv`         | `cargo check --workspace --all-targets --all-features` on Rust 1.89, so `rust-version` stays true                                      |
| `fuzz`         | The gateway's fuzz targets on nightly, 30 seconds each                                                                                 |
| `coverage`     | `cargo llvm-cov` over the workspace tests (generated code excluded), uploaded to Codecov                                               |
| `live`         | Matrix over Core 29.5, 30.4, 31.2: downloads (cached) and runs every crate's live tests                                                |

A second workflow, `release.yml`, publishes on `v*` tags: the gateway's
release binaries and container image (see the [gateway README](bin/bitcoin-gateway/README.md), Operations). On a pull
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

The work was planned up front and carried out locally in stages, with the
rule that the trait layer should become usable early so the other backends
could start. The public history starts at the finished workspace.

| Stage | What                                                                                                                                                    |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| —     | Research: why the library should exist, prior art, the full RPC inventory                                                                               |
| A     | Foundation: CI, error types, `Client`/`Auth`/`Version`/`Feature`, the fake bitcoind and fixtures                                                        |
| B     | The `core` methods: every trait implemented over RPC, including the shared GBT longpoll                                                                 |
| C     | Typed methods by section (blockchain, mining, network, control, zmq, raw tx, util, signer, regtest), then live tests against Core 31 with record/replay |
| E1    | `bitcoin-core-zmq`                                                                                                                                      |
| E2    | Live tests on Core 28, 29, 30, 31 in CI                                                                                                                 |
| E3    | `bitcoin-core-ipc` and `ipc-codegen`                                                                                                                    |
| E4    | The two consumers; their findings applied back to `bitcoin-node-api`                                                                                    |
| D     | The wallet: `Wallet` handle and lifecycle (D1), addresses/coins/history (D2), spending/PSBTs/descriptors (D3)                                           |
| F–H   | Phase 1.5: method classes and retries, batching, the mock, `tracing`, docs and release prep                                                             |
| I     | Gateway skeleton: config, hashed tokens, scopes, the composite backend, event fan-out, listeners and TLS                                                |
| J     | gRPC: the proto and committed codegen, one handler per trait method, events, health, reflection                                                         |
| K     | REST (encodings, caching, SSE, OpenAPI) and the WebSocket pass-through; the `serde` shape frozen as v1                                                  |
| L     | Metrics, health and readiness, the live gateway test on Core 28–31, release packaging                                                                   |
| M     | `bitcoin-node-ldk`: LDK's `BlockSource` over the traits, and what it taught about them                                                                  |
| —     | Hardening: `header_info`, gateway limits, fuzzing, a load test                                                                                          |
| N     | `bitcoin-node-bdk`: BDK's emitter and filter sync over the traits; `ChainSource::block_filter`                                                          |

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
- ZMQ's mempool sequence from `getrawmempool` is the _next_ one Core
  assigns, so events at or above it are new.

---

## 16. `bitcoin-gateway`: Core over gRPC, REST and WebSocket

Running it, keys and scopes, the composite backend, event fan-out, gRPC, REST, WebSocket, listeners and TLS, configuration and operations: [`bin/bitcoin-gateway/README.md`](bin/bitcoin-gateway/README.md).

---

## 17. Further reading

- Crate-level docs: `cargo doc --workspace --no-deps --open`.

---

## 18. License

Apache License 2.0. See [`LICENSE`](LICENSE). The Cap'n Proto schemas under
`crates/bitcoin-core-ipc/schema/` are verbatim copies from Bitcoin Core and
libmultiprocess and remain under their original MIT license.
