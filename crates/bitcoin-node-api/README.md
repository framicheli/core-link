# bitcoin-node-api

Small, backend-agnostic capability traits for talking to a Bitcoin node.
Libraries program against these and stay independent of how the node is
reached (JSON-RPC, ZMQ, IPC, a mock, a remote gateway).

| Trait            | What it gives                                              |
| ---------------- | ---------------------------------------------------------- |
| `ChainSource`    | tip, block hashes, headers, filters, blocks, txs, UTXOs    |
| `MempoolSource`  | txids, entries, a snapshot with Core's mempool sequence    |
| `Broadcaster`    | broadcast, package broadcast, test-accept                  |
| `FeeEstimator`   | fee rate for a confirmation target                         |
| `TemplateSource` | block templates, waiting for new ones, submitting a solution |
| `EventSource`    | a stream of block, mempool and `Lagged` events             |

```rust
use bitcoin_node_api::{ApiResult, ChainSource};

async fn print_tip<C: ChainSource>(node: &C) -> ApiResult<()> {
    let tip = node.tip().await?;
    println!("{} at height {}", tip.hash, tip.height);
    Ok(())
}
```

No I/O dependencies: `bitcoin`, `futures-core`, and `serde` behind the
optional `serde` feature (JSON shape v1, frozen).

Implementations: [`bitcoin-core-rpc`](https://crates.io/crates/bitcoin-core-rpc),
[`bitcoin-core-zmq`](https://crates.io/crates/bitcoin-core-zmq),
[`bitcoin-core-ipc`](https://crates.io/crates/bitcoin-core-ipc),
[`bitcoin-node-mock`](https://crates.io/crates/bitcoin-node-mock).
Adapters: [`bitcoin-node-ldk`](https://crates.io/crates/bitcoin-node-ldk)
(LDK's `BlockSource` over any `ChainSource`),
[`bitcoin-node-bdk`](https://crates.io/crates/bitcoin-node-bdk) (BDK chain
sync).

Pure types and traits, no I/O.

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

## Errors

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

## The `Fut<T>` alias

Trait methods return `impl Fut<T>`, a trait alias for
`Future<Output = ApiResult<T>> + Send`. This uses native async-in-trait (no
`async-trait` boxing). The traits are therefore not `dyn`-compatible;
consumers are generic (`fn run<N: ChainSource>(n: &N)`). That will be
revisited if someone needs `Box<dyn ChainSource>`.

## The traits

| Trait            | Methods                                                                                      | Purpose                                  |
| ---------------- | -------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `ChainSource`    | `tip`, `block_hash`, `header`, `header_info`, `block_filter`, `block`, `transaction`, `utxo` | Read the best chain                      |
| `MempoolSource`  | `txids`, `snapshot`, `entry`                                                                 | Read the mempool                         |
| `Broadcaster`    | `broadcast`, `broadcast_package`, `test_accept`                                              | Send transactions                        |
| `FeeEstimator`   | `estimate_fee(FeeTarget) -> Option<FeeRate>`                                                 | Fee estimates (`None` = no estimate yet) |
| `TemplateSource` | `template`, `transactions`, `wait_next`, `submit_solution`                                   | Mining work                              |
| `EventSource`    | `subscribe() -> EventStream`                                                                 | Block and mempool events                 |

Design choices worth knowing:

- **`MempoolSource` is separate from `ChainSource`** because some backends
  (REST, IPC) can serve chain data but not mempool data.
- **`MempoolSource::snapshot()`** returns the txids _and_ the mempool
  sequence number the node will assign next. That is what makes a gap-free
  mempool copy possible: subscribe to events, snapshot, then skip tx events
  whose sequence is below the snapshot's. Its default implementation calls
  `txids()` with no sequence; the RPC backend overrides it with
  `getrawmempool` + `mempool_sequence`. (Added after the E4 consumers
  needed it; see [§10](https://github.com/framicheli/core-link#10-consumers-proving-the-traits).)
- **Reorgs need no special method.** After `Lagged` (or at start), compare
  `block_hash(h)` against the hashes you hold, top down; the first match is
  the fork, found in depth + 1 calls. A `BlockConnected` whose parent isn't
  your tip means walking `header(h).prev_blockhash` back to a block you
  know.

## Mining types: why `Template` is lean

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

## Events

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

## `serde` feature

Off by default. Adds `Serialize`/`Deserialize` on every api type, for the
gateway's JSON and for consumers that log or forward values. Enums
are tagged `{"type": "snake_case_variant", ...}`; hashes, scripts and
transactions are hex; amounts are satoshis. `Template` is `Serialize` only,
since its token is a live backend handle. **The JSON shape is v1 and
frozen**: the gateway's REST and WebSocket clients see it, so changing it is
a breaking change.

License: Apache-2.0.
