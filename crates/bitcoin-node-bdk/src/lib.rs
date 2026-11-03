//! BDK chain sync over any [`bitcoin_node_api`] backend: Bitcoin Core over
//! RPC, the in-memory mock, or anything else that implements the traits.
//!
//! [`Emitter`] is `bdk_bitcoind_rpc::Emitter`, async and generic: the same
//! events, the same rules, so code written for one reads like the other. It
//! emits every block of the best chain in order, from where the wallet left
//! off, through reorgs, and the mempool with evictions.
//!
//! ```no_run
//! # async fn run(node: bitcoin_node_mock::MockNode) -> Result<(), Box<dyn std::error::Error>> {
//! use bdk_wallet::{KeychainKind, Wallet};
//! use bitcoin_node_bdk::Emitter;
//!
//! # let (desc, change) = ("", "");
//! let mut wallet = Wallet::create(desc, change)
//!     .network(bitcoin::Network::Regtest)
//!     .create_wallet_no_persist()?;
//! let mut emitter = Emitter::new(node.clone(), wallet.latest_checkpoint(), 0, wallet
//!     .transactions()
//!     .filter(|t| t.chain_position.is_unconfirmed())
//!     .map(|t| t.tx_node.tx.clone()));
//! while let Some(ev) = emitter.next_block().await? {
//!     wallet.apply_block_connected_to(&ev.block, ev.block_height(), ev.connected_to())?;
//! }
//! let mempool = emitter.mempool().await?;
//! wallet.apply_unconfirmed_txs(mempool.update);
//! wallet.apply_evicted_txs(mempool.evicted);
//! # Ok(()) }
//! ```
//!
//! # What it asks of the node
//!
//! | `bdk_bitcoind_rpc` (Core RPC)               | here                                     |
//! | ------------------------------------------- | ---------------------------------------- |
//! | `getblockhash`, `getblock` (with `nextblockhash`, `confirmations`) | `block_hash`, `block`: a block is on the best chain when `block_hash(its height)` is its hash |
//! | `getblockcount` + `getblockhash` around `getrawmempool` | `tip`, `txids`, `tip`              |
//! | `getrawtransaction`                         | `transaction(txid, None)`                |
//!
//! Two calls per new block (`block_hash`, `block`), as with Core directly.
//! No trait method beyond `ChainSource` and `MempoolSource`, so any backend
//! with those works. To poll only when something changed, subscribe to an
//! [`EventSource`](bitcoin_node_api::EventSource) and call `next_block`
//! and `mempool` on each event.
//!
//! Compact block filter sync, fetching only the blocks that may touch the
//! wallet, is [`bip158::FilterIter`].
//!
//! # Errors
//!
//! Every method returns the backend's [`NodeAPIError`]. A transaction that
//! leaves the mempool between `txids` and `transaction` is skipped, and a
//! block reorged out mid-poll is retried from the new best chain, as in
//! `bdk_bitcoind_rpc`.
#![warn(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bdk_core::{BlockId, CheckPoint};
use bitcoin::{Block, BlockHash, Transaction, Txid};
use bitcoin_node_api::{ApiResult, ChainSource, MempoolSource, NodeAPIError, RpcErrorCode};

pub use bdk_core;

pub mod bip158;

/// Emits the best chain's blocks and the mempool from a [`ChainSource`] (and
/// a [`MempoolSource`] for [`mempool`](Emitter::mempool)), connected to the
/// checkpoint it was started from.
#[derive(Debug)]
pub struct Emitter<C> {
    chain: C,
    start_height: u32,
    /// The last emitted block that is on the best chain, linked back to the
    /// checkpoint the emitter started from. Popped back on a reorg.
    last_cp: CheckPoint,
    /// The last emitted block, while the next one is expected right after
    /// it. `None` sends the next poll looking for where the chains agree.
    last_block: Option<BlockId>,
    /// The mempool as last emitted, minus confirmed transactions: what an
    /// eviction is measured against, and a cache of transactions.
    mempool_snapshot: HashMap<Txid, Arc<Transaction>>,
}

/// No unconfirmed transactions to start with, for [`Emitter::new`].
pub const NO_EXPECTED_MEMPOOL_TXS: core::iter::Empty<Arc<Transaction>> = core::iter::empty();

impl<C> Emitter<C> {
    /// An emitter continuing from `last_cp` (the wallet's latest
    /// checkpoint). Blocks below `start_height` are skipped unless a reorg
    /// reaches below it. `expected_mempool_txs`: the wallet's unconfirmed
    /// transactions, so their eviction is reported.
    pub fn new(
        chain: C,
        last_cp: CheckPoint,
        start_height: u32,
        expected_mempool_txs: impl IntoIterator<Item = impl Into<Arc<Transaction>>>,
    ) -> Self {
        Emitter {
            chain,
            start_height,
            last_cp,
            last_block: None,
            mempool_snapshot: expected_mempool_txs
                .into_iter()
                .map(|tx| {
                    let tx: Arc<Transaction> = tx.into();
                    (tx.compute_txid(), tx)
                })
                .collect(),
        }
    }

    /// The backend.
    pub fn inner(&self) -> &C {
        &self.chain
    }

    /// Unwraps the backend.
    pub fn into_inner(self) -> C {
        self.chain
    }
}

/// What one poll found.
enum Poll {
    /// The next block on the best chain.
    Block(BlockId, Block),
    /// At the tip.
    NoMoreBlocks,
    /// The last emitted block was reorged out: look for agreement.
    NotInBestChain,
    /// The newest checkpoint still on the best chain.
    Agreement(CheckPoint),
    /// None of the checkpoints is: start again from genesis.
    NoAgreement(BlockHash),
}

/// `block_hash` above the tip: Core's -8, and the mock's.
fn above_tip(e: &NodeAPIError) -> bool {
    matches!(
        e,
        NodeAPIError::Rpc {
            code: RpcErrorCode::InvalidParameter,
            ..
        }
    )
}

/// An unknown block or transaction: Core's -5.
fn not_found(e: &NodeAPIError) -> bool {
    matches!(
        e,
        NodeAPIError::Rpc {
            code: RpcErrorCode::InvalidAddressOrKey,
            ..
        }
    )
}

impl<C: ChainSource + Sync> Emitter<C> {
    /// The next block of the best chain, or `None` at the tip.
    ///
    /// Its [`BlockEvent::checkpoint`] connects to the checkpoint the emitter
    /// started from; after a reorg, the next block is the first one past
    /// where the chains agree.
    pub async fn next_block(&mut self) -> ApiResult<Option<BlockEvent<Block>>> {
        loop {
            match self.poll_once().await? {
                Poll::Block(id, block) => {
                    let checkpoint = self
                        .last_cp
                        .clone()
                        .push(id)
                        .expect("a block is above the checkpoint it extends");
                    self.last_cp = checkpoint.clone();
                    self.last_block = Some(id);
                    // Confirmed: no longer a candidate for eviction.
                    for tx in &block.txdata {
                        self.mempool_snapshot.remove(&tx.compute_txid());
                    }
                    return Ok(Some(BlockEvent { block, checkpoint }));
                }
                Poll::NoMoreBlocks => {
                    self.last_block = None;
                    return Ok(None);
                }
                Poll::NotInBestChain => self.last_block = None,
                Poll::Agreement(cp) => {
                    // A reorg below `start_height` must revisit the heights
                    // it invalidated.
                    let h = cp.height();
                    if h < self.start_height && h < self.last_cp.height() {
                        self.start_height = h;
                    }
                    self.last_block = Some(cp.block_id());
                    self.last_cp = cp;
                }
                Poll::NoAgreement(genesis) => {
                    self.last_cp = CheckPoint::new(BlockId {
                        height: 0,
                        hash: genesis,
                    });
                    self.last_block = None;
                }
            }
        }
    }

    async fn poll_once(&self) -> ApiResult<Poll> {
        let c = &self.chain;
        let Some(last) = self.last_block else {
            for cp in self.last_cp.iter() {
                if self.on_best_chain(cp.block_id()).await? {
                    return Ok(Poll::Agreement(cp));
                }
            }
            return Ok(Poll::NoAgreement(c.block_hash(0).await?));
        };
        let jump = last.height + 1 < self.start_height;
        let height = if jump {
            self.start_height
        } else {
            last.height + 1
        };
        let hash = match c.block_hash(u64::from(height)).await {
            Ok(h) => h,
            Err(e) if above_tip(&e) => {
                // Nothing new, unless what we emitted last was reorged out
                // onto a shorter chain.
                return Ok(if self.on_best_chain(last).await? {
                    Poll::NoMoreBlocks
                } else {
                    Poll::NotInBestChain
                });
            }
            Err(e) => return Err(e),
        };
        if jump && !self.on_best_chain(last).await? {
            return Ok(Poll::NotInBestChain);
        }
        let block = match c.block(hash).await {
            Ok(b) => b,
            // Reorged out between the two calls.
            Err(e) if not_found(&e) => return Ok(Poll::NotInBestChain),
            Err(e) => return Err(e),
        };
        if !jump && block.header.prev_blockhash != last.hash {
            return Ok(Poll::NotInBestChain);
        }
        Ok(Poll::Block(BlockId { height, hash }, block))
    }

    async fn on_best_chain(&self, id: BlockId) -> ApiResult<bool> {
        on_best_chain(&self.chain, id).await
    }
}

/// `id` is the best chain's block at its height.
async fn on_best_chain<C: ChainSource>(chain: &C, id: BlockId) -> ApiResult<bool> {
    match chain.block_hash(u64::from(id.height)).await {
        Ok(h) => Ok(h == id.hash),
        Err(e) if above_tip(&e) => Ok(false),
        Err(e) => Err(e),
    }
}

impl<C: ChainSource + MempoolSource + Sync> Emitter<C> {
    /// The mempool now, stamped with the current time; see
    /// [`mempool_at`](Self::mempool_at).
    pub async fn mempool(&mut self) -> ApiResult<MempoolEvent> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.mempool_at(now).await
    }

    /// Every mempool transaction, and those that left it without being
    /// mined, stamped with `sync_time` (unix seconds).
    ///
    /// Evictions are reported only once [`next_block`](Self::next_block)
    /// has reached the tip the mempool was read at: before that, a
    /// transaction that left the mempool may be in a block not emitted yet.
    pub async fn mempool_at(&mut self, sync_time: u64) -> ApiResult<MempoolEvent> {
        let c = &self.chain;
        // A mempool consistent with one tip: read it between two equal tips.
        let (tip, txids) = loop {
            let tip = c.tip().await?;
            let txids = c.txids().await?;
            if c.tip().await? == tip {
                break (tip, txids);
            }
        };
        let mut update = Vec::with_capacity(txids.len());
        for txid in &txids {
            let tx = match self.mempool_snapshot.get(txid) {
                Some(tx) => tx.clone(),
                None => match c.transaction(*txid, None).await {
                    Ok(tx) => {
                        let tx = Arc::new(tx);
                        self.mempool_snapshot.insert(*txid, tx.clone());
                        tx
                    }
                    // Mined or evicted since `txids`.
                    Err(e) if not_found(&e) => continue,
                    Err(e) => return Err(e),
                },
            };
            update.push((tx, sync_time));
        }
        let at_tip =
            tip.height == u64::from(self.last_cp.height()) && tip.hash == self.last_cp.hash();
        let mut evicted = Vec::new();
        if at_tip {
            let now: HashSet<Txid> = txids.into_iter().collect();
            evicted = self
                .mempool_snapshot
                .keys()
                .filter(|t| !now.contains(*t))
                .map(|t| (*t, sync_time))
                .collect();
            self.mempool_snapshot = update
                .iter()
                .map(|(tx, _)| (tx.compute_txid(), tx.clone()))
                .collect();
        }
        Ok(MempoolEvent { update, evicted })
    }
}

/// The mempool, from [`Emitter::mempool`].
#[derive(Debug, Default, Clone)]
pub struct MempoolEvent {
    /// Every transaction in the mempool, with when it was seen (unix
    /// seconds). For `Wallet::apply_unconfirmed_txs`.
    pub update: Vec<(Arc<Transaction>, u64)>,
    /// Transactions that left the mempool unmined, with when that was
    /// noticed. For `Wallet::apply_evicted_txs`.
    pub evicted: Vec<(Txid, u64)>,
}

/// A block, from [`Emitter::next_block`].
#[derive(Debug, Clone)]
pub struct BlockEvent<B> {
    /// The block.
    pub block: B,
    /// The block's checkpoint, linked to every block emitted before it and
    /// to the checkpoint the emitter started from, all one chain.
    pub checkpoint: CheckPoint,
}

impl<B> BlockEvent<B> {
    /// The block's height.
    pub fn block_height(&self) -> u32 {
        self.checkpoint.height()
    }

    /// The block's hash.
    pub fn block_hash(&self) -> BlockHash {
        self.checkpoint.hash()
    }

    /// The block it connects to: the previous checkpoint, emitted or the
    /// one the emitter started from. For `Wallet::apply_block_connected_to`.
    pub fn connected_to(&self) -> BlockId {
        match self.checkpoint.prev() {
            Some(prev) => prev.block_id(),
            None => self.checkpoint.block_id(),
        }
    }
}
