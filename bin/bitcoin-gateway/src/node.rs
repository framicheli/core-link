//! The composite backend (PHASE_2.md §2.2): `Client` serves everything but
//! events, which come from ZMQ when it's configured.
//!
//! An enum, not `Box<dyn ...>`: the backends are known at compile time and
//! only the choice is made at runtime, so the traits stay as they are.
use bitcoin::block::Header;
use bitcoin::{Block, BlockHash, FeeRate, OutPoint, Transaction, Txid};
use bitcoin_core_rpc::{BlockEvents, Client};
use bitcoin_core_zmq::{ZmqEvents, ZmqStream};
use bitcoin_node_api::{
    ApiResult, Broadcaster, ChainSource, EventSource, FeeEstimator, FeeTarget, HeaderInfo,
    MempoolAccept, MempoolEntry, MempoolSnapshot, MempoolSource, NodeEvent, PackageResult, Tip,
    Utxo,
};
use futures_core::Stream;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Everything the trait API needs from a backend. [`Node`] in production,
/// `bitcoin-node-mock` in tests.
pub trait Backend:
    ChainSource
    + MempoolSource
    + Broadcaster
    + FeeEstimator
    + EventSource
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<T> Backend for T where
    T: ChainSource
        + MempoolSource
        + Broadcaster
        + FeeEstimator
        + EventSource
        + Clone
        + Send
        + Sync
        + 'static
{
}

/// A boxed future, for the object-safe [`Upstream`].
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Untyped JSON-RPC to Core: WebSocket pass-through and readiness checks.
pub trait Upstream: Send + Sync + 'static {
    /// Calls `method` on the node, or on `wallet`'s endpoint; Core's
    /// `result` as is.
    fn call<'a>(
        &'a self,
        wallet: Option<&'a str>,
        method: &'a str,
        params: Value,
    ) -> BoxFut<'a, ApiResult<Value>>;
}

impl Upstream for Client {
    fn call<'a>(
        &'a self,
        wallet: Option<&'a str>,
        method: &'a str,
        params: Value,
    ) -> BoxFut<'a, ApiResult<Value>> {
        Box::pin(async move {
            match wallet {
                Some(w) => self.wallet(w).raw_call(method, params).await,
                None => self.raw_call(method, params).await,
            }
        })
    }
}

/// Where events come from.
#[derive(Debug, Clone)]
pub enum Events {
    /// Core's `sequence` topic.
    Zmq(ZmqEvents),
    /// `waitfornewblock` polling: block events only.
    Rpc(Client),
}

/// [`Events`]' stream.
#[derive(Debug)]
pub enum EventStream {
    /// From ZMQ.
    Zmq(ZmqStream),
    /// From RPC polling.
    Rpc(BlockEvents),
}

impl Stream for EventStream {
    type Item = ApiResult<NodeEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            EventStream::Zmq(s) => Pin::new(s).poll_next(cx),
            EventStream::Rpc(s) => Pin::new(s).poll_next(cx),
        }
    }
}

impl EventSource for Events {
    type EventStream = EventStream;

    async fn subscribe(&self) -> ApiResult<EventStream> {
        Ok(match self {
            Events::Zmq(z) => EventStream::Zmq(z.subscribe().await?),
            Events::Rpc(c) => EventStream::Rpc(c.subscribe().await?),
        })
    }
}

/// The backend the gateway serves: the five traits by delegation.
#[derive(Debug, Clone)]
pub struct Node {
    /// Chain, mempool, broadcast, fees, pass-through.
    pub rpc: Client,
    /// Events.
    pub events: Events,
}

impl ChainSource for Node {
    async fn tip(&self) -> ApiResult<Tip> {
        self.rpc.tip().await
    }
    async fn block_hash(&self, height: u64) -> ApiResult<BlockHash> {
        self.rpc.block_hash(height).await
    }
    async fn header(&self, hash: BlockHash) -> ApiResult<Header> {
        self.rpc.header(hash).await
    }
    async fn header_info(&self, hash: BlockHash) -> ApiResult<HeaderInfo> {
        self.rpc.header_info(hash).await
    }
    async fn block_filter(&self, hash: BlockHash) -> ApiResult<bitcoin::bip158::BlockFilter> {
        self.rpc.block_filter(hash).await
    }
    async fn block(&self, hash: BlockHash) -> ApiResult<Block> {
        self.rpc.block(hash).await
    }
    async fn transaction(&self, txid: Txid, block: Option<BlockHash>) -> ApiResult<Transaction> {
        self.rpc.transaction(txid, block).await
    }
    async fn utxo(&self, outpoint: OutPoint, include_mempool: bool) -> ApiResult<Option<Utxo>> {
        self.rpc.utxo(outpoint, include_mempool).await
    }
}

impl MempoolSource for Node {
    async fn txids(&self) -> ApiResult<Vec<Txid>> {
        self.rpc.txids().await
    }
    async fn entry(&self, txid: Txid) -> ApiResult<Option<MempoolEntry>> {
        self.rpc.entry(txid).await
    }
    async fn snapshot(&self) -> ApiResult<MempoolSnapshot>
    where
        Self: Sync,
    {
        self.rpc.snapshot().await
    }
}

impl Broadcaster for Node {
    async fn broadcast(&self, tx: &Transaction) -> ApiResult<Txid> {
        self.rpc.broadcast(tx).await
    }
    async fn broadcast_package(&self, txs: &[Transaction]) -> ApiResult<Vec<PackageResult>> {
        self.rpc.broadcast_package(txs).await
    }
    async fn test_accept(&self, txs: &[Transaction]) -> ApiResult<Vec<MempoolAccept>> {
        self.rpc.test_accept(txs).await
    }
}

impl FeeEstimator for Node {
    async fn estimate_fee(&self, target: FeeTarget) -> ApiResult<Option<FeeRate>> {
        self.rpc.estimate_fee(target).await
    }
}

impl EventSource for Node {
    type EventStream = EventStream;

    async fn subscribe(&self) -> ApiResult<EventStream> {
        self.events.subscribe().await
    }
}
