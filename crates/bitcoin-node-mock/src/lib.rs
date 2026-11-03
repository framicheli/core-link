//! An in-memory Bitcoin node implementing every `bitcoin-node-api` trait,
//! for testing code written against the traits without a `bitcoind`.
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")] async fn main() {
//! use bitcoin_node_api::{ChainSource, EventSource, NodeEvent};
//! use bitcoin_node_mock::MockNode;
//!
//! let node = MockNode::regtest();
//! let mut events = node.subscribe().await.unwrap();
//! let hash = node.mine(&[]);
//! assert_eq!(node.tip().await.unwrap().height, 1);
//! # let _ = (hash, &mut events);
//! # }
//! ```
//!
//! It behaves like the RPC backend where callers can tell: unknown lookups
//! are `None` (`utxo`, `entry`) or Core's error codes (`-5`, `-8`),
//! `snapshot()` carries a mempool sequence, mined transactions leave the
//! mempool without a `TxRemoved`, a reorg announces its disconnects first
//! (like ZMQ), and `wait_next` returns on a new tip or, with a
//! `fee_threshold`, on a mempool change that raises fees by that much.
//!
//! It validates almost nothing: scripts, amounts and consensus rules are not
//! checked. `submit_solution` checks only that the header matches the
//! template and meets its proof-of-work target.
#![warn(missing_docs)]
use bitcoin::block::{Header, Version as BlockVersion};
use bitcoin::blockdata::constants::genesis_block;
use bitcoin::hashes::{Hash, HashEngine};
use bitcoin::script::Builder;
use bitcoin::{
    Amount, Block, BlockHash, FeeRate, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
    TxMerkleNode, TxOut, Txid, Weight, Witness, absolute, transaction,
};
use bitcoin_node_api::*;
use futures_core::Stream;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

/// Regtest: 50 BTC, halving every 150 blocks.
const HALVING: u64 = 150;

/// A node in memory. `Clone` shares the state: keep one handle to drive the
/// chain and give clones to the code under test.
#[derive(Clone)]
pub struct MockNode {
    state: Arc<Mutex<State>>,
    /// Bumped on every tip or mempool change; `wait_next` watches it.
    changes: Arc<watch::Sender<u64>>,
}

impl std::fmt::Debug for MockNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.lock();
        f.debug_struct("MockNode")
            .field("height", &(s.chain.len() - 1))
            .field("mempool", &s.mempool.len())
            .finish()
    }
}

struct PoolTx {
    tx: Transaction,
    fee: Amount,
    time: u64,
    height: u64,
}

#[derive(Default)]
struct State {
    /// The active chain; index = height.
    chain: Vec<BlockHash>,
    /// Every block ever seen, stale ones included.
    blocks: HashMap<BlockHash, Block>,
    /// Arrival order.
    mempool: Vec<Txid>,
    pool: HashMap<Txid, PoolTx>,
    /// The next mempool sequence to assign.
    sequence: u64,
    fees: Vec<(FeeTarget, FeeRate)>,
    failures: HashMap<String, VecDeque<NodeAPIError>>,
    subscribers: Vec<mpsc::UnboundedSender<ApiResult<NodeEvent>>>,
    /// Makes every mined block distinct.
    nonce: u64,
}

/// What a `Template` from this node points back to.
struct MockToken {
    txs: Vec<TemplateTx>,
    /// `changes` when the template was made.
    generation: u64,
}

impl MockNode {
    /// A regtest chain with only the genesis block, and an empty mempool.
    pub fn regtest() -> MockNode {
        let genesis = genesis_block(Network::Regtest);
        let hash = genesis.block_hash();
        let state = State {
            chain: vec![hash],
            blocks: HashMap::from([(hash, genesis)]),
            ..State::default()
        };
        MockNode {
            state: Arc::new(Mutex::new(state)),
            changes: Arc::new(watch::channel(0).0),
        }
    }

    /// Mines a block with `txs` on the tip and returns its hash. Txs from the
    /// mempool leave it without a `TxRemoved`; others needn't be in it.
    /// Sends `BlockConnected`.
    pub fn mine(&self, txs: &[Transaction]) -> BlockHash {
        let mut s = self.lock();
        let hash = s.mine(txs.to_vec());
        drop(s);
        self.changed();
        hash
    }

    /// Mines `n` empty blocks; returns their hashes.
    pub fn mine_empty(&self, n: usize) -> Vec<BlockHash> {
        (0..n).map(|_| self.mine(&[])).collect()
    }

    /// Disconnects the top `disconnect` blocks, then mines `connect` new
    /// empty ones. Sends a `BlockDisconnected` per block (tip first), then a
    /// `BlockConnected` per block, as ZMQ does. Transactions of disconnected
    /// blocks go back to the mempool (`TxAdded`), as in Core.
    ///
    /// # Panics
    /// If `disconnect` would remove the genesis block.
    pub fn reorg(&self, disconnect: usize, connect: usize) {
        let mut s = self.lock();
        assert!(disconnect < s.chain.len(), "can't disconnect genesis");
        let mut returned = Vec::new();
        for _ in 0..disconnect {
            let hash = s.chain.pop().expect("checked above");
            s.send(NodeEvent::BlockDisconnected(hash));
            returned.extend(s.blocks[&hash].txdata.iter().skip(1).cloned());
        }
        for tx in returned.into_iter().rev() {
            s.add(tx);
        }
        for _ in 0..connect {
            s.mine(Vec::new());
        }
        drop(s);
        self.changed();
    }

    /// Adds `tx` to the mempool, as `broadcast` does (`TxAdded`). Mempool txs
    /// spending the same outputs are replaced (`TxRemoved`), with their
    /// descendants. The fee is inputs minus outputs when the inputs are known,
    /// else zero.
    pub fn add_to_mempool(&self, tx: Transaction) -> Txid {
        let txid = self.lock().add(tx);
        self.changed();
        txid
    }

    /// Removes `txid` and its descendants from the mempool with `TxRemoved`,
    /// as an expiry or eviction would. `false` if it wasn't there.
    pub fn remove_from_mempool(&self, txid: Txid) -> bool {
        let removed = self.lock().evict(txid);
        if removed {
            self.changed();
        }
        removed
    }

    /// What `estimate_fee(target)` answers; `None` means "no estimate".
    pub fn set_fee(&self, target: FeeTarget, rate: Option<FeeRate>) {
        let mut s = self.lock();
        s.fees.retain(|(t, _)| *t != target);
        if let Some(r) = rate {
            s.fees.push((target, r));
        }
    }

    /// The next call to trait method `method` (e.g. `"block"`, `"tip"`,
    /// `"broadcast"`, `"subscribe"`) fails with `err`. Queued per method.
    pub fn fail_next(&self, method: &str, err: NodeAPIError) {
        self.lock()
            .failures
            .entry(method.to_owned())
            .or_default()
            .push_back(err);
    }

    /// Sends `Lagged` to every subscriber, as a dropped ZMQ message would.
    pub fn lag(&self) {
        self.lock().send(NodeEvent::Lagged);
    }

    /// The active chain's block at `height`, if any.
    pub fn block_at(&self, height: u64) -> Option<Block> {
        let s = self.lock();
        let hash = s.chain.get(usize::try_from(height).ok()?)?;
        Some(s.blocks[hash].clone())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    fn changed(&self) {
        self.changes.send_modify(|g| *g += 1);
    }

    /// A queued failure for `method`, if any.
    fn check(&self, method: &str) -> ApiResult<()> {
        match self
            .lock()
            .failures
            .get_mut(method)
            .and_then(VecDeque::pop_front)
        {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

fn rpc_err(code: RpcErrorCode, message: &str) -> NodeAPIError {
    NodeAPIError::Rpc {
        code,
        message: message.into(),
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn subsidy(height: u64) -> Amount {
    match height / HALVING {
        h if h >= 64 => Amount::ZERO,
        h => Amount::from_sat(Amount::from_int_btc(50).to_sat() >> h),
    }
}

/// BIP34: the height as the scriptSig's first push.
fn height_push(height: u64) -> ScriptBuf {
    Builder::new().push_int(height as i64).into_script()
}

/// The witness commitment output for a block whose non-coinbase txs are `txs`.
fn witness_commitment(txs: &[Transaction]) -> TxOut {
    let dummy = Block {
        header: genesis_block(Network::Regtest).header,
        txdata: std::iter::once(genesis_block(Network::Regtest).txdata[0].clone())
            .chain(txs.iter().cloned())
            .collect(),
    };
    let root = dummy.witness_root().expect("has a coinbase");
    let commitment = Block::compute_witness_commitment(&root, &[0u8; 32]);
    let mut data = vec![0xaa, 0x21, 0xa9, 0xed];
    data.extend_from_slice(commitment.as_byte_array());
    let push: &bitcoin::script::PushBytes = data.as_slice().try_into().expect("36 bytes");
    TxOut {
        value: Amount::ZERO,
        script_pubkey: Builder::new()
            .push_opcode(bitcoin::opcodes::all::OP_RETURN)
            .push_slice(push)
            .into_script(),
    }
}

/// Siblings of the coinbase (index 0) from leaf to root, given the other txids in block order.
fn coinbase_merkle_path(txids: impl Iterator<Item = Txid>) -> Vec<TxMerkleNode> {
    let mut level: Vec<TxMerkleNode> = txids
        .map(|t| TxMerkleNode::from_raw_hash(t.to_raw_hash()))
        .collect();
    let mut path = Vec::new();
    while let Some(&sibling) = level.first() {
        path.push(sibling);
        level = level[1..]
            .chunks(2)
            .map(|c| {
                let mut e = TxMerkleNode::engine();
                e.input(c[0].as_byte_array());
                e.input(c[c.len() - 1].as_byte_array());
                TxMerkleNode::from_engine(e)
            })
            .collect();
    }
    path
}

impl State {
    fn tip(&self) -> Tip {
        Tip {
            height: self.chain.len() as u64 - 1,
            hash: *self.chain.last().expect("genesis"),
        }
    }

    fn tip_header(&self) -> Header {
        self.blocks[&self.tip().hash].header
    }

    fn send(&mut self, ev: NodeEvent) {
        self.subscribers.retain(|s| s.send(Ok(ev)).is_ok());
    }

    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence - 1
    }

    /// The output `op` creates, on the active chain or in the mempool.
    fn output(&self, op: OutPoint) -> Option<TxOut> {
        let tx = match self.pool.get(&op.txid) {
            Some(p) => &p.tx,
            None => self.find_confirmed(op.txid)?.0,
        };
        tx.output.get(op.vout as usize).cloned()
    }

    /// A tx on the active chain, and its height.
    fn find_confirmed(&self, txid: Txid) -> Option<(&Transaction, u64)> {
        self.chain.iter().enumerate().find_map(|(h, hash)| {
            self.blocks[hash]
                .txdata
                .iter()
                .find(|t| t.compute_txid() == txid)
                .map(|t| (t, h as u64))
        })
    }

    fn add(&mut self, tx: Transaction) -> Txid {
        let txid = tx.compute_txid();
        if self.pool.contains_key(&txid) {
            return txid;
        }
        let conflicts: Vec<Txid> = self
            .mempool
            .iter()
            .filter(|t| {
                self.pool[*t].tx.input.iter().any(|i| {
                    tx.input
                        .iter()
                        .any(|j| j.previous_output == i.previous_output)
                })
            })
            .copied()
            .collect();
        for c in conflicts {
            self.evict(c);
        }
        let inputs: Option<Amount> = tx
            .input
            .iter()
            .map(|i| self.output(i.previous_output).map(|o| o.value))
            .sum();
        let outputs: Amount = tx.output.iter().map(|o| o.value).sum();
        let fee = inputs
            .and_then(|i| i.checked_sub(outputs))
            .unwrap_or(Amount::ZERO);
        let height = self.tip().height;
        self.pool.insert(
            txid,
            PoolTx {
                tx,
                fee,
                time: now(),
                height,
            },
        );
        self.mempool.push(txid);
        let mempool_sequence = Some(self.next_sequence());
        self.send(NodeEvent::TxAdded {
            txid,
            mempool_sequence,
        });
        txid
    }

    /// Mempool txs spending an output of `txid`.
    fn children(&self, txid: Txid) -> Vec<Txid> {
        self.mempool
            .iter()
            .filter(|t| {
                self.pool[*t]
                    .tx
                    .input
                    .iter()
                    .any(|i| i.previous_output.txid == txid)
            })
            .copied()
            .collect()
    }

    /// Mempool txs `txid` spends from.
    fn parents(&self, txid: Txid) -> Vec<Txid> {
        let mut p: Vec<Txid> = self.pool[&txid]
            .tx
            .input
            .iter()
            .map(|i| i.previous_output.txid)
            .filter(|t| self.pool.contains_key(t))
            .collect();
        p.dedup();
        p
    }

    /// `txid` and everything reachable through `step`, `txid` included.
    fn closure(&self, txid: Txid, step: impl Fn(&State, Txid) -> Vec<Txid>) -> Vec<Txid> {
        let mut seen = vec![txid];
        let mut i = 0;
        while i < seen.len() {
            for t in step(self, seen[i]) {
                if !seen.contains(&t) {
                    seen.push(t);
                }
            }
            i += 1;
        }
        seen
    }

    /// Removes `txid` and its descendants with `TxRemoved`.
    fn evict(&mut self, txid: Txid) -> bool {
        if !self.pool.contains_key(&txid) {
            return false;
        }
        for t in self.closure(txid, State::children) {
            self.pool.remove(&t);
            self.mempool.retain(|m| *m != t);
            let mempool_sequence = Some(self.next_sequence());
            self.send(NodeEvent::TxRemoved {
                txid: t,
                mempool_sequence,
            });
        }
        true
    }

    /// A coinbase for the block on the tip; `tag` keeps blocks distinct.
    fn coinbase(&self, fees: Amount, txs: &[Transaction], tag: u64) -> Transaction {
        let height = self.tip().height + 1;
        let mut script_sig = height_push(height).into_bytes();
        script_sig.extend(Builder::new().push_int(tag as i64).as_bytes());
        Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(script_sig),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[[0u8; 32]]),
            }],
            output: vec![
                TxOut {
                    value: subsidy(height) + fees,
                    script_pubkey: ScriptBuf::new_op_return([]),
                },
                witness_commitment(txs),
            ],
        }
    }

    fn mine(&mut self, txs: Vec<Transaction>) -> BlockHash {
        self.nonce += 1;
        let fees = txs
            .iter()
            .filter_map(|t| self.pool.get(&t.compute_txid()).map(|p| p.fee))
            .sum();
        let coinbase = self.coinbase(fees, &txs, self.nonce);
        let prev = self.tip_header();
        let mut block = Block {
            header: Header {
                version: BlockVersion::from_consensus(0x2000_0000),
                prev_blockhash: self.tip().hash,
                merkle_root: TxMerkleNode::all_zeros(),
                time: prev.time.max(now() as u32).max(prev.time + 1),
                bits: prev.bits,
                nonce: 0,
            },
            txdata: std::iter::once(coinbase).chain(txs).collect(),
        };
        block.header.merkle_root = block.compute_merkle_root().expect("has txs");
        while !block.header.target().is_met_by(block.block_hash()) {
            block.header.nonce += 1;
        }
        self.connect(block)
    }

    /// Appends `block` to the active chain; its txs leave the mempool
    /// silently (and so do their conflicts, with `TxRemoved`).
    fn connect(&mut self, block: Block) -> BlockHash {
        let hash = block.block_hash();
        for tx in block.txdata.iter().skip(1) {
            let txid = tx.compute_txid();
            if self.pool.remove(&txid).is_some() {
                self.mempool.retain(|m| *m != txid);
            }
            let conflicts: Vec<Txid> = self
                .mempool
                .iter()
                .filter(|t| {
                    self.pool[*t].tx.input.iter().any(|i| {
                        tx.input
                            .iter()
                            .any(|j| j.previous_output == i.previous_output)
                    })
                })
                .copied()
                .collect();
            for c in conflicts {
                self.evict(c);
            }
        }
        self.chain.push(hash);
        self.blocks.insert(hash, block);
        self.send(NodeEvent::BlockConnected(hash));
        hash
    }

    fn template(&self, generation: u64) -> Template {
        let tip = self.tip();
        let prev = self.tip_header();
        let txs: Vec<TemplateTx> = self
            .mempool
            .iter()
            .map(|t| {
                let p = &self.pool[t];
                TemplateTx {
                    tx: p.tx.clone(),
                    fee: p.fee,
                    sigops: 0,
                }
            })
            .collect();
        let fees = txs.iter().map(|t| t.fee).sum();
        let plain: Vec<Transaction> = txs.iter().map(|t| t.tx.clone()).collect();
        let height = tip.height + 1;
        let min_time = prev.time + 1;
        Template {
            prev_hash: tip.hash,
            height,
            version: BlockVersion::from_consensus(0x2000_0000),
            bits: prev.bits,
            min_time,
            cur_time: (now() as u32).max(min_time),
            coinbase: Coinbase {
                version: transaction::Version::TWO,
                sequence: Sequence::MAX,
                script_sig_prefix: height_push(height),
                witness: Some([0; 32]),
                reward_remaining: subsidy(height) + fees,
                required_outputs: vec![witness_commitment(&plain)],
                lock_time: absolute::LockTime::ZERO,
            },
            coinbase_merkle_path: coinbase_merkle_path(plain.iter().map(|t| t.compute_txid())),
            token: TemplateToken::new(MockToken { txs, generation }),
        }
    }
}

fn token(t: &Template) -> ApiResult<&MockToken> {
    t.token
        .downcast_ref()
        .ok_or_else(|| NodeAPIError::Unsupported("template is not from a MockNode".into()))
}

impl ChainSource for MockNode {
    async fn tip(&self) -> ApiResult<Tip> {
        self.check("tip")?;
        Ok(self.lock().tip())
    }

    async fn block_hash(&self, height: u64) -> ApiResult<BlockHash> {
        self.check("block_hash")?;
        let s = self.lock();
        usize::try_from(height)
            .ok()
            .and_then(|h| s.chain.get(h).copied())
            .ok_or_else(|| rpc_err(RpcErrorCode::InvalidParameter, "Block height out of range"))
    }

    async fn header(&self, hash: BlockHash) -> ApiResult<Header> {
        self.check("header")?;
        let s = self.lock();
        s.blocks
            .get(&hash)
            .map(|b| b.header)
            .ok_or_else(|| rpc_err(RpcErrorCode::InvalidAddressOrKey, "Block not found"))
    }

    /// Height and chainwork by walking the parents: the mock's chains are short.
    async fn header_info(&self, hash: BlockHash) -> ApiResult<HeaderInfo> {
        self.check("header_info")?;
        let s = self.lock();
        let not_found = || rpc_err(RpcErrorCode::InvalidAddressOrKey, "Block not found");
        let header = s.blocks.get(&hash).ok_or_else(not_found)?.header;
        let (mut height, mut chain_work) = (0u64, header.work());
        let mut prev = header.prev_blockhash;
        while let Some(b) = s.blocks.get(&prev) {
            height += 1;
            chain_work = chain_work + b.header.work();
            prev = b.header.prev_blockhash;
        }
        Ok(HeaderInfo {
            header,
            height,
            chain_work,
        })
    }

    /// Built from the block and the scripts it spends, as Core's index
    /// does. A spent output the mock never saw counts as an empty script,
    /// which BIP 158 leaves out.
    async fn block_filter(&self, hash: BlockHash) -> ApiResult<bitcoin::bip158::BlockFilter> {
        self.check("block_filter")?;
        let s = self.lock();
        let block = s
            .blocks
            .get(&hash)
            .ok_or_else(|| rpc_err(RpcErrorCode::InvalidAddressOrKey, "Block not found"))?;
        let script = |op: &OutPoint| -> Result<ScriptBuf, bitcoin::bip158::Error> {
            let tx = block
                .txdata
                .iter()
                .find(|t| t.compute_txid() == op.txid)
                .or_else(|| s.find_confirmed(op.txid).map(|(t, _)| t));
            Ok(tx
                .and_then(|t| t.output.get(op.vout as usize))
                .map(|o| o.script_pubkey.clone())
                .unwrap_or_default())
        };
        bitcoin::bip158::BlockFilter::new_script_filter(block, script)
            .map_err(|e| NodeAPIError::Decode(format!("block filter: {e}")))
    }

    async fn block(&self, hash: BlockHash) -> ApiResult<Block> {
        self.check("block")?;
        let s = self.lock();
        s.blocks
            .get(&hash)
            .cloned()
            .ok_or_else(|| rpc_err(RpcErrorCode::InvalidAddressOrKey, "Block not found"))
    }

    /// As with `-txindex`: `block` is optional.
    async fn transaction(&self, txid: Txid, block: Option<BlockHash>) -> ApiResult<Transaction> {
        self.check("transaction")?;
        let s = self.lock();
        let found = match block {
            Some(b) => s
                .blocks
                .get(&b)
                .and_then(|b| b.txdata.iter().find(|t| t.compute_txid() == txid))
                .cloned(),
            None => s
                .pool
                .get(&txid)
                .map(|p| p.tx.clone())
                .or_else(|| s.find_confirmed(txid).map(|(t, _)| t.clone())),
        };
        found.ok_or_else(|| {
            rpc_err(
                RpcErrorCode::InvalidAddressOrKey,
                "No such mempool or blockchain transaction",
            )
        })
    }

    async fn utxo(&self, outpoint: OutPoint, include_mempool: bool) -> ApiResult<Option<Utxo>> {
        self.check("utxo")?;
        let s = self.lock();
        let spent_in = |txs: &mut dyn Iterator<Item = &Transaction>| {
            txs.flat_map(|t| &t.input)
                .any(|i| i.previous_output == outpoint)
        };
        let chain_txs = || s.chain.iter().flat_map(|h| &s.blocks[h].txdata);
        if spent_in(&mut chain_txs()) {
            return Ok(None);
        }
        let pool_txs = || s.pool.values().map(|p| &p.tx);
        if include_mempool && spent_in(&mut pool_txs()) {
            return Ok(None);
        }
        let (tx, confirmations) = match s.find_confirmed(outpoint.txid) {
            Some((tx, h)) => (tx, (s.tip().height - h + 1) as u32),
            None => match s.pool.get(&outpoint.txid) {
                Some(p) if include_mempool => (&p.tx, 0),
                _ => return Ok(None),
            },
        };
        Ok(tx.output.get(outpoint.vout as usize).map(|o| Utxo {
            value: o.value,
            script_pubkey: o.script_pubkey.clone(),
            confirmations,
            coinbase: tx.is_coinbase(),
        }))
    }
}

impl MempoolSource for MockNode {
    async fn txids(&self) -> ApiResult<Vec<Txid>> {
        self.check("txids")?;
        Ok(self.lock().mempool.clone())
    }

    async fn entry(&self, txid: Txid) -> ApiResult<Option<MempoolEntry>> {
        self.check("entry")?;
        let s = self.lock();
        let Some(p) = s.pool.get(&txid) else {
            return Ok(None);
        };
        let weight: Weight = p.tx.weight();
        Ok(Some(MempoolEntry {
            vsize: weight.to_vbytes_ceil() as u32,
            weight,
            fee: p.fee,
            time: p.time,
            height: p.height,
            // Both include the tx itself, as Core's counts do.
            ancestors: s.closure(txid, State::parents).len() as u32,
            descendants: s.closure(txid, State::children).len() as u32,
        }))
    }

    async fn snapshot(&self) -> ApiResult<MempoolSnapshot> {
        self.check("snapshot")?;
        let s = self.lock();
        Ok(MempoolSnapshot {
            txids: s.mempool.clone(),
            sequence: Some(s.sequence),
        })
    }
}

impl Broadcaster for MockNode {
    async fn broadcast(&self, tx: &Transaction) -> ApiResult<Txid> {
        self.check("broadcast")?;
        let txid = tx.compute_txid();
        if self.lock().find_confirmed(txid).is_some() {
            return Err(rpc_err(
                RpcErrorCode::VerifyAlreadyInChain,
                "Transaction outputs already in utxo set",
            ));
        }
        Ok(self.add_to_mempool(tx.clone()))
    }

    async fn broadcast_package(&self, txs: &[Transaction]) -> ApiResult<Vec<PackageResult>> {
        self.check("broadcast_package")?;
        let mut out = Vec::new();
        for tx in txs {
            let txid = tx.compute_txid();
            let outcome = if self.lock().pool.contains_key(&txid) {
                PackageOutcome::AlreadyInMempool
            } else {
                self.add_to_mempool(tx.clone());
                PackageOutcome::Accepted {
                    fee: self.lock().pool[&txid].fee,
                }
            };
            out.push(PackageResult { txid, outcome });
        }
        Ok(out)
    }

    async fn test_accept(&self, txs: &[Transaction]) -> ApiResult<Vec<MempoolAccept>> {
        self.check("test_accept")?;
        let s = self.lock();
        Ok(txs
            .iter()
            .map(|tx| {
                let txid = tx.compute_txid();
                let reject_reason = if s.pool.contains_key(&txid) {
                    Some("txn-already-in-mempool".to_owned())
                } else if s.find_confirmed(txid).is_some() {
                    Some("txn-already-known".to_owned())
                } else {
                    None
                };
                MempoolAccept {
                    txid,
                    allowed: reject_reason.is_none(),
                    reject_reason,
                }
            })
            .collect())
    }
}

impl FeeEstimator for MockNode {
    async fn estimate_fee(&self, target: FeeTarget) -> ApiResult<Option<FeeRate>> {
        self.check("estimate_fee")?;
        Ok(self
            .lock()
            .fees
            .iter()
            .find(|(t, _)| *t == target)
            .map(|(_, r)| *r))
    }
}

impl TemplateSource for MockNode {
    async fn template(&self) -> ApiResult<Template> {
        self.check("template")?;
        let generation = *self.changes.borrow();
        Ok(self.lock().template(generation))
    }

    async fn transactions(&self, tmpl: &Template) -> ApiResult<Vec<TemplateTx>> {
        self.check("transactions")?;
        Ok(token(tmpl)?.txs.clone())
    }

    /// Returns on a new tip, or on a mempool change that raises the
    /// template's fees by `fee_threshold`. `None`: only a new tip counts, as
    /// over IPC.
    async fn wait_next(&self, prev: &Template, opts: WaitOptions) -> ApiResult<Option<Template>> {
        self.check("wait_next")?;
        let mut seen = token(prev)?.generation;
        let prev_fees: Amount = token(prev)?.txs.iter().map(|t| t.fee).sum();
        let mut rx = self.changes.subscribe();
        let wait = async {
            loop {
                let generation = *rx
                    .wait_for(|g| *g != seen)
                    .await
                    .expect("the sender lives in self");
                seen = generation;
                let next = self.lock().template(generation);
                let fees: Amount = token(&next)?.txs.iter().map(|t| t.fee).sum();
                let enough = opts.fee_threshold.is_some_and(|t| fees >= prev_fees + t);
                if next.prev_hash != prev.prev_hash || enough {
                    return Ok(next);
                }
            }
        };
        match opts.timeout {
            None => wait.await.map(Some),
            Some(t) => match tokio::time::timeout(t, wait).await {
                Ok(r) => r.map(Some),
                Err(_) => Ok(None),
            },
        }
    }

    /// Checks the header against the template and its proof of work, then
    /// connects the block. A block on a parent that is no longer the tip is
    /// stored, not connected (Core's "inconclusive"); already known is `Ok`.
    async fn submit_solution(&self, tmpl: &Template, sol: &Solution) -> ApiResult<()> {
        self.check("submit_solution")?;
        let tok = token(tmpl)?;
        let header = tmpl.header(sol);
        if sol.time < tmpl.min_time {
            return Err(rpc_err(RpcErrorCode::VerifyError, "time-too-old"));
        }
        if header.validate_pow(header.target()).is_err() {
            return Err(rpc_err(RpcErrorCode::VerifyError, "high-hash"));
        }
        let block = Block {
            header,
            txdata: std::iter::once(sol.coinbase.clone())
                .chain(tok.txs.iter().map(|t| t.tx.clone()))
                .collect(),
        };
        if block.compute_merkle_root() != Some(header.merkle_root) {
            return Err(rpc_err(RpcErrorCode::VerifyError, "bad-txnmrklroot"));
        }
        let mut s = self.lock();
        let hash = block.block_hash();
        if s.blocks.contains_key(&hash) {
            return Ok(());
        }
        if s.tip().hash != tmpl.prev_hash {
            s.blocks.insert(hash, block);
            return Ok(());
        }
        s.connect(block);
        drop(s);
        self.changed();
        Ok(())
    }
}

/// A `MockNode` subscription. Unbounded: events are never dropped, so
/// `Lagged` comes only from [`MockNode::lag`].
#[derive(Debug)]
pub struct MockEvents(mpsc::UnboundedReceiver<ApiResult<NodeEvent>>);

impl Stream for MockEvents {
    type Item = ApiResult<NodeEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

impl EventSource for MockNode {
    type EventStream = MockEvents;

    /// Every block and tx event from now on, ZMQ-style (sequences included).
    async fn subscribe(&self) -> ApiResult<MockEvents> {
        self.check("subscribe")?;
        let (tx, rx) = mpsc::unbounded_channel();
        self.lock().subscribers.push(tx);
        Ok(MockEvents(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subsidy_halves() {
        assert_eq!(subsidy(0), Amount::from_int_btc(50));
        assert_eq!(subsidy(150), Amount::from_int_btc(25));
        assert_eq!(subsidy(150 * 64), Amount::ZERO);
    }

    #[test]
    fn mined_blocks_are_valid_shape() {
        let node = MockNode::regtest();
        // Heights 1-16 are OP_1..OP_16, as in Core; rust-bitcoin parses pushes only.
        let h = *node.mine_empty(17).last().unwrap();
        let b = node.block_at(17).unwrap();
        assert_eq!(b.block_hash(), h);
        assert_eq!(b.bip34_block_height().unwrap(), 17);
        assert!(b.check_merkle_root());
        assert!(b.check_witness_commitment());
        assert!(b.header.validate_pow(b.header.target()).is_ok());
    }
}
