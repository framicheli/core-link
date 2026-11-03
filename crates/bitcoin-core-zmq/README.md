# bitcoin-core-zmq

Bitcoin Core's ZMQ `sequence` notifications as a
[`bitcoin-node-api`](https://crates.io/crates/bitcoin-node-api) `EventSource`:
block connects and disconnects, mempool adds and removes with Core's
mempool sequence, and an explicit `Lagged` whenever messages were lost
(sequence gap, slow consumer, reconnect), so consumers know to resync.

```rust
use bitcoin_core_zmq::{Topic, ZmqEvents};
use bitcoin_node_api::EventSource;

// bitcoind -zmqpubsequence=tcp://127.0.0.1:28332
let zmq = ZmqEvents::connect("tcp://127.0.0.1:28332", Topic::Sequence).await?;
let mut events = zmq.subscribe().await?;
```

Pure Rust (`zeromq`), no libzmq. Feature `tracing`: connect, disconnect
and each `Lagged` with its cause.

More: the [repository README](https://github.com/framicheli/bitcoin_rpc).

License: MIT.
