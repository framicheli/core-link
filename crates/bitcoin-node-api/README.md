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
sync). Design and rationale: the [repository README](https://github.com/framicheli/core-link).

License: MIT.
