//! Broadcasting transactions and estimating fees.

use crate::Fut;
use bitcoin::{Amount, FeeRate, Transaction, Txid};

/// One tx's result from [`Broadcaster::test_accept`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MempoolAccept {
    /// The tx this result is for.
    pub txid: Txid,
    /// Whether the mempool would accept it.
    pub allowed: bool,
    /// Core's reason when not allowed.
    pub reject_reason: Option<String>,
}

/// One tx's result from [`Broadcaster::broadcast_package`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PackageResult {
    /// The tx this result is for.
    pub txid: Txid,
    /// What happened to it.
    pub outcome: PackageOutcome,
}

/// What a package submission did with one tx.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(
        from = "crate::wire::PackageOutcomeWire",
        into = "crate::wire::PackageOutcomeWire"
    )
)]
pub enum PackageOutcome {
    /// Entered the mempool.
    Accepted {
        /// The tx's own fee.
        fee: Amount,
    },
    /// Was already there (same txid, or same txid with a different witness).
    AlreadyInMempool,
    /// Not accepted; Core's reason.
    Rejected(String),
}

/// Core's `estimatesmartfee` estimate mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum FeeMode {
    /// Uses a longer history; slower to drop, less likely to underpay.
    Conservative,
    /// Reacts faster to short-term drops in fees.
    Economical,
}

/// What to estimate a fee rate for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FeeTarget {
    /// Confirm within this many blocks.
    pub blocks: u16,
    /// How the estimate trades safety for cost.
    pub mode: FeeMode,
}

/// Sends transactions to the node's mempool.
pub trait Broadcaster {
    /// Submits one tx and relays it.
    fn broadcast(&self, tx: &Transaction) -> impl Fut<Txid>;
    /// One result per input tx, in input order. `Unsupported` on Core < 28.
    fn broadcast_package(&self, txs: &[Transaction]) -> impl Fut<Vec<PackageResult>>;
    /// Checks the txs against the mempool without submitting them. One result
    /// per input tx, in input order.
    fn test_accept(&self, txs: &[Transaction]) -> impl Fut<Vec<MempoolAccept>>;
}

/// Fee rate estimates.
pub trait FeeEstimator {
    /// `None`: the node has no estimate yet (fresh node, regtest). Not an error.
    fn estimate_fee(&self, target: FeeTarget) -> impl Fut<Option<FeeRate>>;
}
