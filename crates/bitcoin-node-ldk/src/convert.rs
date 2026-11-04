//! The api's `bitcoin` types as LDK's, and back.
//!
//! `bitcoin-node-api` pins the `bitcoin` that `corepc-types` uses; LDK
//! pins its own (`lightning::bitcoin`). While both are one crate, every
//! conversion here is a move. When they diverge, blocks and headers are
//! re-encoded through consensus bytes and hashes copied byte for byte, so
//! either side can bump without the other.
use bitcoin_node_api::bitcoin;
#[allow(unused_imports)]
use bitcoin_node_api::bitcoin::hashes::Hash as _;
use bitcoin_node_api::{ApiResult, NodeAPIError};
use lightning::bitcoin as ldk;
// Both `Hash` imports are needed only once the two `bitcoin`s differ.
#[allow(unused_imports)]
use lightning::bitcoin::hashes::Hash as _;
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
    B: ldk::consensus::Decodable,
{
    ldk::consensus::deserialize(&bitcoin::consensus::serialize(&a))
        .map_err(|e| NodeAPIError::Decode(format!("{what} doesn't re-encode for LDK: {e}")))
}

pub fn hash_in(h: &ldk::BlockHash) -> bitcoin::BlockHash {
    bitcoin::BlockHash::from_byte_array(h.to_byte_array())
}

pub fn hash_out(h: bitcoin::BlockHash) -> ldk::BlockHash {
    ldk::BlockHash::from_byte_array(h.to_byte_array())
}

pub fn work(w: bitcoin::Work) -> ldk::Work {
    ldk::Work::from_be_bytes(w.to_be_bytes())
}

pub fn header(h: bitcoin::block::Header) -> ApiResult<ldk::block::Header> {
    same_or(h, |h| reencode("header", h))
}

pub fn block(b: bitcoin::Block) -> ApiResult<ldk::Block> {
    same_or(b, |b| reencode("block", b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::constants::genesis_block;
    use bitcoin::{Network, Work};

    #[test]
    fn same_type_moves_other_reencodes() {
        assert_eq!(same_or::<u8, u8>(1, |_| unreachable!()), 1);
        assert_eq!(same_or::<u8, u16>(1, |a| u16::from(a) + 1), 2);
    }

    /// The re-encode path, which `same_or` skips while both sides are one crate.
    #[test]
    fn reencode_round_trips() {
        let g = genesis_block(Network::Bitcoin);
        assert_eq!(reencode::<_, ldk::Block>("block", g.clone()).unwrap(), g);
        assert_eq!(
            reencode::<_, ldk::block::Header>("header", g.header).unwrap(),
            g.header
        );
        assert_eq!(hash_in(&hash_out(g.block_hash())), g.block_hash());
        let w = Work::from_be_bytes([7; 32]);
        assert_eq!(work(w).to_be_bytes(), w.to_be_bytes());
    }
}
