//! Chain reads: tip, headers, blocks, transactions, UTXOs.

use crate::Fut;
use bitcoin::bip158::BlockFilter;
use bitcoin::block::Header;
use bitcoin::{Amount, Block, BlockHash, OutPoint, ScriptBuf, Transaction, Txid, Work};

/// The best block of the active chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Tip {
    /// Genesis is 0.
    pub height: u64,
    /// Hash of the block at `height`.
    pub hash: BlockHash,
}

/// A header with its place in the chain, as `getblockheader` reports it.
///
/// `height` and `chain_work` follow from the header's ancestry, so for a
/// given hash they never change, active chain or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeaderInfo {
    /// The header, as consensus hex in JSON.
    #[cfg_attr(feature = "serde", serde(with = "crate::wire::consensus_hex"))]
    pub header: Header,
    /// Genesis is 0.
    pub height: u64,
    /// Total work of the chain up to and including this block (Core's
    /// `chainwork`; 64 hex digits in JSON).
    pub chain_work: Work,
}

/// An unspent output, as `gettxout` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Utxo {
    /// The output's value.
    pub value: Amount,
    /// The script that locks it.
    pub script_pubkey: ScriptBuf,
    /// 0 for an output of a mempool tx; 1 once in the tip block.
    pub confirmations: u32,
    /// Created by a coinbase tx, so unspendable until 100 confirmations.
    pub coinbase: bool,
}

/// Read access to the chain.
///
/// Reorgs need no special call. After `Lagged` (or at start), compare
/// `block_hash(h)` with the hashes you hold from your tip down; the first
/// match is the fork, found in depth + 1 calls. A `BlockConnected` whose
/// parent isn't your tip (from a source that reports connects only) means
/// walking `header(h).prev_blockhash` back to a block you know.
pub trait ChainSource {
    /// The current best block.
    fn tip(&self) -> impl Fut<Tip>;
    /// Hash of the active-chain block at `height`. Above the tip: `Rpc` with
    /// `InvalidParameter` (-8), as Core answers.
    fn block_hash(&self, height: u64) -> impl Fut<BlockHash>;
    /// Header of a block the node knows, active chain or not.
    fn header(&self, hash: BlockHash) -> impl Fut<Header>;
    /// `header`, with the block's height and chainwork. Unknown hash: `Rpc`
    /// with `InvalidAddressOrKey` (-5), as Core answers.
    fn header_info(&self, hash: BlockHash) -> impl Fut<HeaderInfo>;
    /// The block's BIP 158 basic filter (BIP 157's `getcfilters`): its
    /// output scripts and the scripts it spends, so a wallet can tell from
    /// a few hundred bytes whether to fetch the block. Core needs
    /// `-blockfilterindex`; without it, and on backends without filters,
    /// `Unsupported`.
    fn block_filter(&self, hash: BlockHash) -> impl Fut<BlockFilter>;
    /// The full block; fails if the node pruned it.
    fn block(&self, hash: BlockHash) -> impl Fut<Block>;
    /// `block` is required for confirmed txs unless the node runs `-txindex`.
    fn transaction(&self, txid: Txid, block: Option<BlockHash>) -> impl Fut<Transaction>;
    /// `None` if spent or unknown.
    fn utxo(&self, outpoint: OutPoint, include_mempool: bool) -> impl Fut<Option<Utxo>>;
}
