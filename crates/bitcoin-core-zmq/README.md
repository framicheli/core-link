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

Core's ZMQ notifications as an `EventSource`,
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

License: Apache-2.0.
