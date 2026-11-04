//! The api's `bitcoin` types as BDK's, and back.
//!
//! `bitcoin-node-api` pins the `bitcoin` that `corepc-types` uses; BDK
//! pins its own (`bdk_core::bitcoin`). While both are one crate, every
//! conversion here is a move. When they diverge, blocks and transactions
//! are re-encoded through consensus bytes and hashes copied byte for byte,
//! so either side can bump without the other.
use bdk_core::bitcoin as bdk;
// Both `Hash` imports are needed only once the two `bitcoin`s differ.
#[allow(unused_imports)]
use bdk_core::bitcoin::hashes::Hash as _;
use bitcoin_node_api::bitcoin;
#[allow(unused_imports)]
use bitcoin_node_api::bitcoin::hashes::Hash as _;
use bitcoin_node_api::{ApiResult, NodeAPIError};
use std::any::Any;

/// `a` as a `B`: a move when `A` is `B` (one `bitcoin`), else `reencode`.
fn same_or<A: Any, B: Any>(a: A, reencode: impl FnOnce(A) -> B) -> B {
    match (Box::new(a) as Box<dyn Any>).downcast::<B>() {
        Ok(b) => *b,
        Err(a) => reencode(*a.downcast::<A>().expect("it went in as an A")),
    }
}

/// Consensus bytes out of one `bitcoin`, into the other.
fn reencode<A, B>(what: &str, a: A) -> ApiResult<B>
where
    A: bitcoin::consensus::Encodable,
    B: bdk::consensus::Decodable,
{
    bdk::consensus::deserialize(&bitcoin::consensus::serialize(&a))
        .map_err(|e| NodeAPIError::Decode(format!("{what} doesn't re-encode for BDK: {e}")))
}

pub fn hash_in(h: bdk::BlockHash) -> bitcoin::BlockHash {
    bitcoin::BlockHash::from_byte_array(h.to_byte_array())
}

pub fn hash_out(h: bitcoin::BlockHash) -> bdk::BlockHash {
    bdk::BlockHash::from_byte_array(h.to_byte_array())
}

pub fn txid_in(t: bdk::Txid) -> bitcoin::Txid {
    bitcoin::Txid::from_byte_array(t.to_byte_array())
}

pub fn txid_out(t: bitcoin::Txid) -> bdk::Txid {
    bdk::Txid::from_byte_array(t.to_byte_array())
}

pub fn block(b: bitcoin::Block) -> ApiResult<bdk::Block> {
    same_or(b, |b| reencode("block", b))
}

pub fn tx(t: bitcoin::Transaction) -> ApiResult<bdk::Transaction> {
    same_or(t, |t| reencode("transaction", t))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Network;
    use bitcoin::constants::genesis_block;

    #[test]
    fn same_type_moves_other_reencodes() {
        assert_eq!(same_or::<u8, u8>(1, |_| unreachable!()), 1);
        assert_eq!(same_or::<u8, u16>(1, |a| u16::from(a) + 1), 2);
    }

    /// The re-encode path, which `same_or` skips while both sides are one crate.
    #[test]
    fn reencode_round_trips() {
        let g = genesis_block(Network::Bitcoin);
        let cb = g.txdata[0].clone();
        assert_eq!(reencode::<_, bdk::Block>("block", g.clone()).unwrap(), g);
        assert_eq!(
            reencode::<_, bdk::Transaction>("transaction", cb.clone()).unwrap(),
            cb
        );
        assert_eq!(hash_in(hash_out(g.block_hash())), g.block_hash());
        assert_eq!(txid_in(txid_out(cb.compute_txid())), cb.compute_txid());
    }
}
