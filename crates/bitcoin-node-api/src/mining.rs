//! Block templates for mining, and submitting solved blocks.

use crate::Fut;
use bitcoin::block::Header;
use bitcoin::hashes::{Hash, HashEngine};
use bitcoin::script::{self, PushBytes};
use bitcoin::{
    Amount, BlockHash, CompactTarget, OutPoint, Script, ScriptBuf, Sequence, Transaction, TxIn,
    TxMerkleNode, TxOut, Witness, absolute, block, transaction,
};
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Opaque backend handle. Backends downcast; consumers only pass it back.
/// `Clone` is an `Arc` clone; `PartialEq` is `Arc::ptr_eq`.
#[derive(Clone)]
pub struct TemplateToken(Arc<dyn Any + Send + Sync>);

impl TemplateToken {
    /// Wraps a backend's per-template state.
    pub fn new<T: Any + Send + Sync>(inner: T) -> Self {
        TemplateToken(Arc::new(inner))
    }

    /// `None` if the token came from another backend.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

impl fmt::Debug for TemplateToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TemplateToken(..)")
    }
}

impl PartialEq for TemplateToken {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for TemplateToken {}

/// What Core requires of the coinbase. Mirrors `node::CoinbaseTx`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Coinbase {
    /// The coinbase tx's version.
    pub version: transaction::Version,
    /// nSequence of the coinbase input.
    pub sequence: Sequence,
    /// Must start the scriptSig (BIP34 height today). Total scriptSig ≤ 100 bytes.
    pub script_sig_prefix: ScriptBuf,
    /// Sole witness stack item of the coinbase input (BIP141 reserved value). None = no witness data.
    #[cfg_attr(feature = "serde", serde(with = "crate::wire::hex32"))]
    pub witness: Option<[u8; 32]>,
    /// Subsidy plus fees minus `required_outputs`.
    pub reward_remaining: Amount,
    /// Append these, in order, as the LAST outputs of the coinbase. Include them
    /// exactly once; do not add a witness commitment of your own.
    pub required_outputs: Vec<TxOut>,
    /// The coinbase tx's nLockTime.
    pub lock_time: absolute::LockTime,
}

/// Everything needed to build a coinbase and start hashing. No transactions.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Template {
    /// The block this one builds on.
    pub prev_hash: BlockHash,
    /// Height of the block being built.
    pub height: u64,
    /// Header version the node suggests.
    pub version: block::Version,
    /// The block's target, in header form.
    pub bits: CompactTarget,
    /// Earliest valid block time. Exact over RPC; over IPC, which sends no
    /// minimum, the template header's own time (valid, but conservative).
    pub min_time: u32,
    /// Block time the node suggests. Seconds since the epoch.
    pub cur_time: u32,
    /// What the coinbase must contain.
    pub coinbase: Coinbase,
    /// Siblings of the coinbase from leaf to root.
    pub coinbase_merkle_path: Vec<TxMerkleNode>,
    /// A live handle into the backend: not serialized, so `Template` is
    /// `Serialize` only.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub token: TemplateToken,
}

/// A non-coinbase tx of a template, in block order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TemplateTx {
    /// The transaction.
    #[cfg_attr(feature = "serde", serde(with = "crate::wire::consensus_hex"))]
    pub tx: Transaction,
    /// Inputs minus outputs.
    pub fee: Amount,
    /// Sigop cost, as counted against the block limit.
    pub sigops: u32,
}

/// What the miner changed. Coinbase must be complete, witness included.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Solution {
    /// Header version, with any version bits the miner rolled.
    pub version: block::Version,
    /// Header time. Seconds since the epoch.
    pub time: u32,
    /// Header nonce.
    pub nonce: u32,
    /// The full coinbase tx, e.g. from [`Template::coinbase_tx`].
    #[cfg_attr(feature = "serde", serde(with = "crate::wire::consensus_hex"))]
    pub coinbase: Transaction,
}

/// Options for [`TemplateSource::wait_next`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WaitOptions {
    /// Give up after this long. `None` waits indefinitely.
    #[cfg_attr(feature = "serde", serde(with = "crate::wire::millis"))]
    pub timeout: Option<Duration>,
    /// Return on a same-tip template only if its fees rise by at least this
    /// much. `None`: only a new tip counts. Ignored over RPC.
    pub fee_threshold: Option<Amount>,
}

impl Template {
    /// The coinbase a miner submits: `payout` gets `reward_remaining`, then
    /// the `required_outputs`; the scriptSig is the prefix followed by a push
    /// of `extra_nonce`. Core rejects a scriptSig over 100 bytes.
    ///
    /// # Panics
    /// If `extra_nonce` is over 520 bytes (no script push is that long).
    pub fn coinbase_tx(&self, payout: &Script, extra_nonce: &[u8]) -> Transaction {
        let c = &self.coinbase;
        let push = <&PushBytes>::try_from(extra_nonce).expect("extra_nonce over 520 bytes");
        let mut script_sig = c.script_sig_prefix.to_bytes();
        script_sig.extend(script::Builder::new().push_slice(push).as_bytes());
        let mut output = vec![TxOut {
            value: c.reward_remaining,
            script_pubkey: payout.into(),
        }];
        output.extend(c.required_outputs.iter().cloned());
        Transaction {
            version: c.version,
            lock_time: c.lock_time,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(script_sig),
                sequence: c.sequence,
                witness: c
                    .witness
                    .map_or_else(Witness::new, |w| Witness::from_slice(&[w])),
            }],
            output,
        }
    }

    /// The block's merkle root, from `coinbase` and `coinbase_merkle_path`:
    /// no need for the template's transactions.
    pub fn merkle_root(&self, coinbase: &Transaction) -> TxMerkleNode {
        let mut node = TxMerkleNode::from_raw_hash(coinbase.compute_txid().to_raw_hash());
        for sibling in &self.coinbase_merkle_path {
            let mut e = TxMerkleNode::engine();
            e.input(node.as_byte_array());
            e.input(sibling.as_byte_array());
            node = TxMerkleNode::from_engine(e);
        }
        node
    }

    /// The header `sol` makes; mine on its `block_hash()`.
    pub fn header(&self, sol: &Solution) -> Header {
        Header {
            version: sol.version,
            prev_blockhash: self.prev_hash,
            merkle_root: self.merkle_root(&sol.coinbase),
            time: sol.time,
            bits: self.bits,
            nonce: sol.nonce,
        }
    }
}

/// Backends differ in freshness: over IPC a new template comes straight from
/// Core's `waitNext`; over RPC see `template` and `wait_next`. A miner on RPC
/// should treat block events, not fee wake-ups, as its signal for new work.
pub trait TemplateSource {
    /// Over RPC, `getblocktemplate` serves a cached template for up to 5s
    /// after a mempool change.
    fn template(&self) -> impl Fut<Template>;
    /// On demand: decode and allocate only when asked.
    fn transactions(&self, tmpl: &Template) -> impl Fut<Vec<TemplateTx>>;
    /// Blocks until `prev` is stale, then returns the new template. `None` on timeout.
    ///
    /// Over RPC, `fee_threshold` is ignored and Core answers a longpoll on a
    /// new tip, or only ≥60s after a mempool change.
    fn wait_next(&self, prev: &Template, opts: WaitOptions) -> impl Fut<Option<Template>>;
    /// "Already known" is `Ok`.
    fn submit_solution(&self, tmpl: &Template, sol: &Solution) -> impl Fut<()>;
}
