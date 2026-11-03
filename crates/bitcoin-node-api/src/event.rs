//! Push notifications: blocks connected or disconnected, mempool changes.

use crate::{ApiResult, Fut};
use bitcoin::{BlockHash, Txid};
use futures_core::Stream;

/// A change on the node, as pushed by an [`EventSource`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(
        from = "crate::wire::NodeEventWire",
        into = "crate::wire::NodeEventWire"
    )
)]
pub enum NodeEvent {
    /// Only a hash, as Core's ZMQ sends it: it may be older than your state
    /// (your own just-submitted block can arrive late). Confirm with
    /// `ChainSource::tip()`, or `header()` for its parent. ZMQ announces a
    /// reorg's disconnects first; RPC polling reports it as `Lagged`.
    BlockConnected(BlockHash),
    /// The block left the active chain (reorg).
    BlockDisconnected(BlockHash),
    /// `mempool_sequence` is Core's mempool sequence number, when the source
    /// has it (ZMQ). To build a gap-free mempool copy: subscribe, then
    /// `MempoolSource::snapshot()`, then skip tx events below its `sequence`.
    TxAdded {
        /// The tx that entered the mempool.
        txid: Txid,
        /// Core's mempool sequence for this event.
        mempool_sequence: Option<u64>,
    },
    /// Left the mempool for a reason other than being mined (replaced,
    /// expired, evicted, conflicted). Mined txs are covered by `BlockConnected`.
    TxRemoved {
        /// The tx that left the mempool.
        txid: Txid,
        /// Core's mempool sequence for this event.
        mempool_sequence: Option<u64>,
    },
    /// Events were lost (ZMQ HWM drop, reconnect, slow consumer). Resync from `ChainSource::tip()`.
    Lagged,
}

/// A push feed of [`NodeEvent`]s.
pub trait EventSource {
    /// Associated type rather than `impl Stream` so consumers can name it.
    type EventStream: Stream<Item = ApiResult<NodeEvent>> + Send + Unpin;
    /// Opens a new event stream.
    fn subscribe(&self) -> impl Fut<Self::EventStream>;
}
