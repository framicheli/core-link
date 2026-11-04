# bitcoin-node-ldk

LDK's [`lightning-block-sync`](https://crates.io/crates/lightning-block-sync)
`BlockSource` over any
[`bitcoin-node-api`](https://crates.io/crates/bitcoin-node-api)
`ChainSource`: Bitcoin Core over RPC, the in-memory mock, or any other
backend. Generic, no backend-specific code.

```rust
use bitcoin_node_ldk::ChainBlockSource;
use lightning_block_sync::{init, poll::ChainPoller, SpvClient, UnboundedCache};

let source = ChainBlockSource::new(rpc_client);   // or a MockNode, ...
let mut cache = UnboundedCache::new();
let tip = init::synchronize_listeners(&source, Network::Bitcoin, &mut cache, listeners).await?;
let poller = ChainPoller::new(&source, Network::Bitcoin);
let mut spv = SpvClient::new(tip, poller, &mut cache, &listener);
spv.poll_best_tip().await?;
```

Built against `lightning-block-sync` 0.2.0 (re-exported as
`bitcoin_node_ldk::lightning_block_sync`). The types LDK sees are
`lightning::bitcoin`'s and the backend's are `bitcoin_node_api::bitcoin`'s,
converted at the boundary, so the two can pin different `bitcoin` versions.

| `BlockSource`    | `ChainSource`                                                  |
| ---------------- | -------------------------------------------------------------- |
| `get_best_block` | `tip()`, height included                                       |
| `get_block`      | `block(hash)`, always a full block                             |
| `get_header`     | `header_info(hash)`: the header, its height and chainwork      |

**Height and chainwork** are the node's own (`getblockheader` on Core), so
they are exact on every network, testnet3/4 included, and LDK's height
hint isn't needed.

**Errors**: transport failures and RPC errors other than "unknown block"
(-5) are transient (LDK polls again); unknown block, decode errors and
`Unsupported` are persistent. `into_inner()` gives back the
`NodeAPIError`.

Example: `cargo run -p bitcoin-node-ldk --example spv` (on the mock), or
`-- http://127.0.0.1:8332 user pass` to follow a Core node.

License: Apache-2.0.
