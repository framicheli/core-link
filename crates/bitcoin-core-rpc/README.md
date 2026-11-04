# bitcoin-core-rpc

A complete, typed, async JSON-RPC client for Bitcoin Core 29–31 (the
versions Core maintains), and the
JSON-RPC backend for the [`bitcoin-node-api`](https://crates.io/crates/bitcoin-node-api)
traits.

- One method per public RPC (`getblockstats` → `get_block_stats`), with
  `bitcoin::*` types in and out, and a `Wallet` handle for wallet RPCs.
- Core version detected on connect; replies parsed for that version.
- Cookie auth that survives a node restart, configurable timeouts and HTTP
  client, opt-in retries that never resend a payment, and JSON-RPC batching.
- `raw_call` for anything newer than this crate.

```rust
use bitcoin_core_rpc::{Auth, Client, RetryPolicy};

let client = Client::builder("http://127.0.0.1:8332")
    .auth(Auth::cookie("/home/me/.bitcoin/.cookie")?)
    .retry(RetryPolicy::default())
    .connect()
    .await?;
let info = client.get_blockchain_info().await?;
let hashes = client.get_block_hashes(0..=99).await?; // one batched request
```

Retries follow `method_class(name)`: reads and idempotent submits
(`sendrawtransaction`, `submitblock`) are retried after a timeout; writes
like `sendtoaddress` only when the request never left.

Feature `tracing`: a span per request (method, id, wallet, duration, error
kind), never params or results.

Async JSON-RPC over HTTP (`reqwest` with
rustls), one typed method per RPC, plus the trait implementations.

## Modules: one file per Core section

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

## `Client`

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

## Naming and signatures

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

## Response types and version dispatch

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

## Versions and features

```rust
pub struct Version(pub u32);   // getnetworkinfo.version, e.g. 310100; major() = 31

#[non_exhaustive]
pub enum Feature { SubmitPackage, ClusterMempool, PrivateBroadcast, TxVersionArg }
```

Methods that exist only on some versions check `supports()` and return
`NodeAPIError::Unsupported("… needs Core 31+")` instead of letting Core
answer with a bare "method not found". `connect` rejects Core < 29 with
`Unsupported`. A `Feature` is only added when a method actually branches on it.

## Wallets

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

## Trait implementations on `Client`

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

## Wire details

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

## The longpoll problem, and how it is solved

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

## Differences between template backends (documented on the trait)

Over RPC, `wait_next` ignores `fee_threshold` (GBT has no equivalent), Core
holds a longpoll ≥ 60 s after a mempool change, and `getblocktemplate` may
serve a cached template for up to 5 s. Over IPC both are immediate. A miner
on RPC should rely on block events, not on fee wake-ups.

License: Apache-2.0.
