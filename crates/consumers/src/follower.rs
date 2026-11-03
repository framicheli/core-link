//! Follows the best chain and the mempool against `ChainSource`,
//! `MempoolSource` and `NodeEvent`s only, the way a wallet backend would.
//!
//! Reorgs are found by walking parents: `header(hash).prev_blockhash` until a
//! block we know. ZMQ announces the disconnects first, so the walk only does
//! real work with sources that report connects alone (RPC polling) or after
//! `Lagged`.
use bitcoin::{BlockHash, Txid};
use bitcoin_node_api::{ApiResult, ChainSource, MempoolSource, NodeEvent, Tip};
use std::collections::{HashMap, HashSet};

/// Deeper than this, a reorg is handled by a full resync.
const MAX_WALK: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Connected(Tip),
    Disconnected(Tip),
    /// The chain switched branches below the old tip.
    Reorg {
        fork_height: u64,
        /// Blocks dropped from the old branch.
        depth: u64,
        tip: Tip,
    },
    /// Re-read from the source after `Lagged` or a too-deep reorg.
    Resynced(Option<Tip>),
    TxAdded(Txid),
    TxRemoved(Txid),
    /// Nothing to do: already known, or older than our snapshot.
    Ignored,
}

pub struct Follower<C> {
    src: C,
    /// Height of `chain[0]`.
    start: u64,
    chain: Vec<BlockHash>,
    heights: HashMap<BlockHash, u64>,
    mempool: HashSet<Txid>,
    /// From the last `snapshot()`: tx events below it are already in.
    mempool_seq: Option<u64>,
}

impl<C: ChainSource + MempoolSource + Sync> Follower<C> {
    /// Subscribe to events before calling this, so nothing falls in between.
    pub async fn start(src: C, start: u64) -> ApiResult<Follower<C>> {
        let mut f = Follower {
            src,
            start,
            chain: Vec::new(),
            heights: HashMap::new(),
            mempool: HashSet::new(),
            mempool_seq: None,
        };
        f.resync().await?;
        Ok(f)
    }

    pub fn tip(&self) -> Option<Tip> {
        self.chain.last().map(|&hash| Tip {
            height: self.start + self.chain.len() as u64 - 1,
            hash,
        })
    }

    pub fn block_hash(&self, height: u64) -> Option<BlockHash> {
        let i = height.checked_sub(self.start)?;
        self.chain.get(usize::try_from(i).ok()?).copied()
    }

    pub fn mempool(&self) -> &HashSet<Txid> {
        &self.mempool
    }

    /// Re-reads the chain from the source: drops blocks that are no longer on
    /// it (top down), then appends up to the source's tip. Then the mempool.
    pub async fn resync(&mut self) -> ApiResult<Change> {
        let tip = self.src.tip().await?;
        while let Some(ours) = self.tip() {
            let theirs = if ours.height <= tip.height {
                Some(self.src.block_hash(ours.height).await?)
            } else {
                None
            };
            if theirs == Some(ours.hash) {
                break;
            }
            self.pop();
        }
        let next = self.tip().map_or(self.start, |t| t.height + 1);
        for height in next..=tip.height {
            let hash = self.src.block_hash(height).await?;
            self.push(hash);
        }
        let snap = self.src.snapshot().await?;
        self.mempool = snap.txids.into_iter().collect();
        self.mempool_seq = snap.sequence;
        Ok(Change::Resynced(self.tip()))
    }

    pub async fn apply(&mut self, ev: NodeEvent) -> ApiResult<Change> {
        match ev {
            NodeEvent::BlockConnected(hash) => self.connected(hash).await,
            NodeEvent::BlockDisconnected(hash) => Ok(match self.tip() {
                Some(t) if t.hash == hash => {
                    self.pop();
                    Change::Disconnected(t)
                }
                _ => Change::Ignored,
            }),
            NodeEvent::TxAdded {
                mempool_sequence, ..
            }
            | NodeEvent::TxRemoved {
                mempool_sequence, ..
            } if self.before_snapshot(mempool_sequence) => Ok(Change::Ignored),
            NodeEvent::TxAdded { txid, .. } => Ok(if self.mempool.insert(txid) {
                Change::TxAdded(txid)
            } else {
                Change::Ignored
            }),
            NodeEvent::TxRemoved { txid, .. } => Ok(if self.mempool.remove(&txid) {
                Change::TxRemoved(txid)
            } else {
                Change::Ignored
            }),
            NodeEvent::Lagged => self.resync().await,
            _ => Ok(Change::Ignored),
        }
    }

    async fn connected(&mut self, hash: BlockHash) -> ApiResult<Change> {
        if self.heights.contains_key(&hash) {
            return Ok(Change::Ignored);
        }
        // Walk back to a block we know. Headers carry no height, so the new
        // block's height comes from the fork point and the walk's length.
        let mut branch = vec![hash];
        let mut prev = self.src.header(hash).await?.prev_blockhash;
        let fork_height = loop {
            if let Some(&h) = self.heights.get(&prev) {
                break h;
            }
            if branch.len() >= MAX_WALK || self.chain.is_empty() {
                return self.resync().await;
            }
            branch.push(prev);
            prev = self.src.header(prev).await?.prev_blockhash;
        };
        let old_top = self.tip().map_or(fork_height, |t| t.height);
        while self.tip().is_some_and(|t| t.height > fork_height) {
            self.pop();
        }
        for &h in branch.iter().rev() {
            self.push(h);
        }
        // Mined txs leave the mempool without a `TxRemoved` (Core reports
        // only non-block removals), so drop them by reading the blocks.
        for &h in &branch {
            for tx in self.src.block(h).await?.txdata {
                self.mempool.remove(&tx.compute_txid());
            }
        }
        let tip = self.tip().expect("just pushed");
        let depth = old_top - fork_height;
        // Nothing dropped: catching up (several blocks if events were missed).
        Ok(if depth == 0 {
            Change::Connected(tip)
        } else {
            Change::Reorg {
                fork_height,
                depth,
                tip,
            }
        })
    }

    /// Already reflected in the last snapshot. Without sequences (a source
    /// that has none), every event is applied.
    fn before_snapshot(&self, seq: Option<u64>) -> bool {
        matches!((seq, self.mempool_seq), (Some(s), Some(snap)) if s < snap)
    }

    fn push(&mut self, hash: BlockHash) {
        let height = self.start + self.chain.len() as u64;
        self.chain.push(hash);
        self.heights.insert(hash, height);
    }

    fn pop(&mut self) {
        if let Some(h) = self.chain.pop() {
            self.heights.remove(&h);
        }
    }
}
