//! Compact block filter sync ([BIP 157]/[BIP 158]): `bdk_bitcoind_rpc`'s
//! `bip158::FilterIter`, async and over any [`ChainSource`] with
//! [`block_filter`](ChainSource::block_filter) (Core with
//! `-blockfilterindex`, the mock).
//!
//! Each block's filter (a few hundred bytes) is matched against the
//! wallet's scripts; only a block that may pay or spend them is fetched.
//! False positives happen (about one in 784,931 per script) and cost a block
//! download; misses don't. Every block yields an [`Event`] whose checkpoint
//! keeps the wallet's chain current, matched or not.
//!
//! ```no_run
//! # async fn run(node: bitcoin_node_mock::MockNode, mut wallet: bdk_wallet::Wallet,
//! #     spks: Vec<bitcoin::ScriptBuf>) -> Result<(), Box<dyn std::error::Error>> {
//! use bitcoin_node_bdk::bip158::FilterIter;
//!
//! let mut iter = FilterIter::new(node.clone(), wallet.latest_checkpoint(), spks);
//! while let Some(ev) = iter.next().await? {
//!     match &ev.block {
//!         Some(block) => wallet.apply_block_connected_to(block, ev.height(), ev.connected_to())?,
//!         None => wallet.apply_update(bdk_wallet::Update { chain: Some(ev.cp.clone()), ..Default::default() })?,
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! Blocks only: for the mempool, use an [`Emitter`](crate::Emitter).
//!
//! [BIP 157]: https://github.com/bitcoin/bips/blob/master/bip-0157.mediawiki
//! [BIP 158]: https://github.com/bitcoin/bips/blob/master/bip-0158.mediawiki

use crate::convert;
use bdk_core::bitcoin::{Block, ScriptBuf};
use bdk_core::{BlockId, CheckPoint};
use bitcoin_node_api::{ChainSource, NodeAPIError, bitcoin};
use core::fmt;

/// Walks the best chain from a checkpoint, block by block, fetching only
/// the blocks whose filter matches the watched scripts.
#[derive(Debug)]
pub struct FilterIter<C> {
    chain: C,
    spks: Vec<ScriptBuf>,
    /// The scanned chain: the starting checkpoint plus every block scanned.
    cp: CheckPoint,
    /// The last block scanned, while the next is expected right after it.
    last: Option<BlockId>,
}

/// One scanned block, from [`FilterIter::next`].
#[derive(Debug, Clone)]
pub struct Event {
    /// The scanned chain up to this block, connected to the starting
    /// checkpoint: apply it to the wallet's chain.
    pub cp: CheckPoint,
    /// The block, if its filter matched a watched script.
    pub block: Option<Block>,
}

impl Event {
    /// The filter matched: [`block`](Self::block) is set.
    pub fn is_match(&self) -> bool {
        self.block.is_some()
    }

    /// The block's height.
    pub fn height(&self) -> u32 {
        self.cp.height()
    }

    /// The block before it in [`cp`](Self::cp). For
    /// `Wallet::apply_block_connected_to`.
    pub fn connected_to(&self) -> BlockId {
        match self.cp.prev() {
            Some(prev) => prev.block_id(),
            None => self.cp.block_id(),
        }
    }
}

/// Why [`FilterIter::next`] failed.
#[derive(Debug)]
pub enum FilterError {
    /// The backend failed; `Unsupported` if it has no filters (Core without
    /// `-blockfilterindex`).
    Node(NodeAPIError),
    /// A filter didn't decode.
    Filter(bitcoin::bip158::Error),
    /// No block of the starting checkpoint is on the node's best chain: a
    /// different network, or a reorg deeper than the checkpoint reaches.
    NoCommonAncestor,
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterError::Node(e) => write!(f, "node: {e}"),
            FilterError::Filter(e) => write!(f, "block filter: {e}"),
            FilterError::NoCommonAncestor => {
                f.write_str("no block of the checkpoint is on the node's best chain")
            }
        }
    }
}

impl std::error::Error for FilterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FilterError::Node(e) => Some(e),
            FilterError::Filter(e) => Some(e),
            FilterError::NoCommonAncestor => None,
        }
    }
}

impl From<NodeAPIError> for FilterError {
    fn from(e: NodeAPIError) -> Self {
        FilterError::Node(e)
    }
}

impl<C> FilterIter<C> {
    /// Scans from `cp` (the wallet's latest checkpoint) for blocks touching
    /// `spks`.
    pub fn new(chain: C, cp: CheckPoint, spks: impl IntoIterator<Item = ScriptBuf>) -> Self {
        FilterIter {
            chain,
            spks: spks.into_iter().collect(),
            cp,
            last: None,
        }
    }

    /// Watches `spk` too, from the next block on.
    pub fn add_spk(&mut self, spk: ScriptBuf) {
        self.spks.push(spk);
    }

    /// The backend.
    pub fn inner(&self) -> &C {
        &self.chain
    }
}

impl<C: ChainSource + Sync> FilterIter<C> {
    /// The next block of the best chain, or `None` at the tip.
    ///
    /// Three calls per block (`block_hash`, `header` to follow reorgs,
    /// `block_filter`), and `block` on a match. After a reorg the scan
    /// resumes after the newest checkpoint still on the best chain, and the
    /// event's checkpoint replaces the blocks reorged out.
    pub async fn next(&mut self) -> Result<Option<Event>, FilterError> {
        loop {
            let base = match self.last {
                Some(id) => id,
                None => self.find_base().await?,
            };
            let height = base.height + 1;
            let hash = match self.chain.block_hash(u64::from(height)).await {
                Ok(h) => h,
                Err(e) if crate::above_tip(&e) => {
                    if crate::on_best_chain(&self.chain, base).await? {
                        self.last = None;
                        return Ok(None);
                    }
                    self.last = None; // reorged onto a shorter chain
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            let header = match self.chain.header(hash).await {
                Ok(h) => h,
                Err(e) if crate::not_found(&e) => continue, // raced a reorg
                Err(e) => return Err(e.into()),
            };
            if header.prev_blockhash != convert::hash_in(base.hash) {
                self.last = None; // `base` was reorged out
                continue;
            }
            let filter = self.chain.block_filter(hash).await?;
            let matched = !self.spks.is_empty()
                && filter
                    .match_any(&hash, self.spks.iter().map(|s| s.as_bytes()))
                    .map_err(FilterError::Filter)?;
            let block = if matched {
                Some(convert::block(self.chain.block(hash).await?)?)
            } else {
                None
            };
            let id = BlockId {
                height,
                hash: convert::hash_out(hash),
            };
            self.cp = self.cp.clone().insert(id);
            self.last = Some(id);
            return Ok(Some(Event {
                cp: self.cp.clone(),
                block,
            }));
        }
    }

    /// The newest block of the scanned chain still on the best chain.
    async fn find_base(&self) -> Result<BlockId, FilterError> {
        for cp in self.cp.iter() {
            if crate::on_best_chain(&self.chain, cp.block_id()).await? {
                return Ok(cp.block_id());
            }
        }
        Err(FilterError::NoCommonAncestor)
    }
}
