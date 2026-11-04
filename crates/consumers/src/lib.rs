//! Two consumers of `bitcoin-node-api`, written against its traits only:
//! `feed` (a miner's loop) and `follower` (chain and mempool following).
//! The binaries pick backends; this code never names one.
pub mod feed;
pub mod follower;

use futures_core::Stream;
use std::pin::Pin;

/// The next item of an event stream.
pub async fn next_event<S: Stream + Unpin>(s: &mut S) -> Option<S::Item> {
    std::future::poll_fn(|cx| Pin::new(&mut *s).poll_next(cx)).await
}
