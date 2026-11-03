//! Mempool reads.

use crate::Fut;
use bitcoin::{Amount, Txid, Weight};

/// One mempool tx, as `getmempoolentry` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MempoolEntry {
    /// Virtual size in vbytes, sigop-adjusted as Core uses for fee rate.
    pub vsize: u32,
    /// BIP141 weight.
    pub weight: Weight,
    /// The tx's own fee, without ancestors or descendants.
    pub fee: Amount,
    /// When it entered the mempool. Seconds since the epoch, node's clock.
    pub time: u64,
    /// Chain height when it entered the mempool.
    pub height: u64,
    /// In-mempool ancestors. Includes the tx itself, as Core counts.
    pub ancestors: u32,
    /// In-mempool descendants. Includes the tx itself, as Core counts.
    pub descendants: u32,
}

/// The mempool's txids at one moment, and where its event stream stood.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MempoolSnapshot {
    /// Every tx in the mempool, in no particular order.
    pub txids: Vec<Txid>,
    /// The next mempool sequence the node will assign, if the source has
    /// one: tx events below it are already in `txids`; skip them.
    pub sequence: Option<u64>,
}

/// To keep a mempool copy: subscribe to events first, then `snapshot()`,
/// then apply tx events, skipping those below `snapshot.sequence`. Mined txs
/// leave without a `TxRemoved` (Core reports only other removals), so also
/// drop the txs of each connected block. Some event sources (RPC polling)
/// send no tx events at all; re-snapshot on `Lagged`.
pub trait MempoolSource {
    /// Every tx in the mempool, in no particular order.
    fn txids(&self) -> impl Fut<Vec<Txid>>;
    /// `None` if the tx is not in the mempool.
    fn entry(&self, txid: Txid) -> impl Fut<Option<MempoolEntry>>;
    /// `txids` with the event sequence they match. The default has no
    /// sequence, so events racing it may be applied twice or missed.
    fn snapshot(&self) -> impl Fut<MempoolSnapshot>
    where
        Self: Sync,
    {
        async move {
            Ok(MempoolSnapshot {
                txids: self.txids().await?,
                sequence: None,
            })
        }
    }
}
