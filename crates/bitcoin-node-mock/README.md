# bitcoin-node-mock

An in-memory Bitcoin node implementing all six
[`bitcoin-node-api`](https://crates.io/crates/bitcoin-node-api) traits, for
testing code written against them without running `bitcoind`.

```rust
use bitcoin_node_api::{ChainSource, EventSource};
use bitcoin_node_mock::MockNode;

let node = MockNode::regtest();               // genesis tip
let mut events = node.subscribe().await?;
node.mine_empty(3);                           // BlockConnected ×3
node.reorg(2, 3);                             // BlockDisconnected ×2, BlockConnected ×3
let txid = node.add_to_mempool(tx);           // TxAdded with a mempool sequence
node.fail_next("block", TransportError::Timeout.into());
node.lag();                                   // Lagged to every subscriber
```

It behaves like the RPC backend where callers can tell: Core's error codes
for unknown blocks, `None` for unknown UTXOs and mempool entries, snapshot
sequences, mined transactions leaving the mempool without `TxRemoved`, RBF
replacement, `wait_next` waking on a new tip or mempool change. It is a test
double, not a node: scripts, amounts and consensus rules are not checked;
`submit_solution` checks only the header and its proof of work.

License: Apache-2.0.
