# bitcoin-node-bdk

BDK chain sync over any
[`bitcoin-node-api`](https://crates.io/crates/bitcoin-node-api) backend:
Bitcoin Core over RPC, the in-memory mock, or anything else that implements
`ChainSource` and `MempoolSource`.

`Emitter` is `bdk_bitcoind_rpc`'s `Emitter`, async and generic. It emits
every block of the best chain in order, from the wallet's latest checkpoint
and through reorgs, and the mempool with evictions. The events are the
shapes `bdk_wallet` applies:

```rust
use bitcoin_node_bdk::Emitter;

let mut emitter = Emitter::new(client.clone(), wallet.latest_checkpoint(), 0, unconfirmed);
while let Some(ev) = emitter.next_block().await? {
    wallet.apply_block_connected_to(&ev.block, ev.block_height(), ev.connected_to())?;
}
let mempool = emitter.mempool().await?;
wallet.apply_unconfirmed_txs(mempool.update);
wallet.apply_evicted_txs(mempool.evicted);
```

| `bdk_bitcoind_rpc` asks Core              | `Emitter` asks the backend                         |
| ----------------------------------------- | -------------------------------------------------- |
| `getblockhash`, `getblock` (`confirmations`, `nextblockhash`) | `block_hash`, `block`; on the best chain = `block_hash(height)` is its hash |
| `getblockcount`, `getblockhash`, `getrawmempool` | `tip`, `txids`, `tip` (a mempool read at one tip) |
| `getrawtransaction`                       | `transaction(txid, None)`                          |

Two calls per new block, as with Core directly. Evictions are reported
only once the emitter has reached the tip the mempool was read at, so a
mined transaction isn't mistaken for an evicted one. To poll only when
something changed, subscribe to an `EventSource` and call `next_block` and
`mempool` on each event.

**Compact block filters**: `bip158::FilterIter` is
`bdk_bitcoind_rpc::bip158::FilterIter`, async. Each block's BIP 158 filter
(`ChainSource::block_filter`; Core needs `-blockfilterindex`) is matched
against the wallet's scripts, and only matching blocks are fetched; every
block's checkpoint still reaches the wallet, and reorgs rewind the scan.

```rust
use bitcoin_node_bdk::bip158::FilterIter;

let mut iter = FilterIter::new(client.clone(), wallet.latest_checkpoint(), spks);
while let Some(ev) = iter.next().await? {
    match &ev.block {
        Some(block) => wallet.apply_block_connected_to(block, ev.height(), ev.connected_to())?,
        None => wallet.apply_update(Update { chain: Some(ev.cp.clone()), ..Default::default() })?,
    }
}
```

Built against `bdk_core` 0.6 (`bdk_wallet` 3.x, `bdk_chain` 0.23). The types
the wallet sees are `bdk_core::bitcoin`'s and the backend's are
`bitcoin_node_api::bitcoin`'s, converted at the boundary, so the two can pin
different `bitcoin` versions.

License: MIT.
