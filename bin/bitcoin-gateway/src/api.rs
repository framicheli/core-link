//! One handler per trait method, shared by gRPC and REST: the scope check,
//! the rate, the call, and `NodeAPIError` -> [`ApiError`]. The front ends
//! only decode requests and encode replies.
use crate::Gateway;
use crate::auth::Key;
use crate::error::ApiError;
use crate::fanout::Subscription;
use crate::node::Backend;
use crate::scopes::Scope;
use bitcoin::bip158::BlockFilter;
use bitcoin::{Block, BlockHash, FeeRate, OutPoint, Transaction, Txid};
use bitcoin_node_api::{
    ApiResult, FeeTarget, HeaderInfo, MempoolAccept, MempoolEntry, MempoolSnapshot, PackageResult,
    Tip, Utxo,
};
use std::time::Instant;

/// Every trait method the API serves, as `Service/Method`: the gRPC name,
/// and the key REST routes are checked against.
pub const METHODS: &[&str] = &[
    "Chain/Tip",
    "Chain/BlockHash",
    "Chain/Header",
    "Chain/BlockFilter",
    "Chain/Block",
    "Chain/Transaction",
    "Chain/Utxo",
    "Mempool/Txids",
    "Mempool/Snapshot",
    "Mempool/Entry",
    "Broadcast/Broadcast",
    "Broadcast/BroadcastPackage",
    "Broadcast/TestAccept",
    "Fees/EstimateFee",
    "Events/Subscribe",
];

impl<N: Backend> Gateway<N> {
    async fn handle<T>(
        &self,
        key: &Key,
        method: &'static str,
        scope: Scope,
        call: impl Future<Output = ApiResult<T>>,
    ) -> Result<T, ApiError> {
        let start = Instant::now();
        let result = match key.require(scope) {
            Ok(()) => match self.call_slots(key).await {
                Ok(_slots) => call.await.map_err(ApiError::from),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        self.observe(key, method, start, result.as_ref().err());
        result
    }

    /// The scope check and rate alone, recorded like a call: for a REST
    /// `304 Not Modified`, which needs nothing from the node.
    pub fn authorize(&self, key: &Key, method: &'static str, scope: Scope) -> Result<(), ApiError> {
        let start = Instant::now();
        let result = key.require(scope);
        self.observe(key, method, start, result.as_ref().err());
        result
    }

    /// `ChainSource::tip`.
    pub async fn tip(&self, key: &Key) -> Result<Tip, ApiError> {
        self.handle(key, "Chain/Tip", Scope::ChainRead, self.node.tip())
            .await
    }

    /// `ChainSource::block_hash`.
    pub async fn block_hash(&self, key: &Key, height: u64) -> Result<BlockHash, ApiError> {
        let call = self.node.block_hash(height);
        self.handle(key, "Chain/BlockHash", Scope::ChainRead, call)
            .await
    }

    /// `ChainSource::header_info`: the header, its height and chainwork.
    pub async fn header(&self, key: &Key, hash: BlockHash) -> Result<HeaderInfo, ApiError> {
        let call = self.node.header_info(hash);
        self.handle(key, "Chain/Header", Scope::ChainRead, call)
            .await
    }

    /// `ChainSource::block_filter`.
    pub async fn block_filter(&self, key: &Key, hash: BlockHash) -> Result<BlockFilter, ApiError> {
        let call = self.node.block_filter(hash);
        self.handle(key, "Chain/BlockFilter", Scope::ChainRead, call)
            .await
    }

    /// `ChainSource::block`.
    pub async fn block(&self, key: &Key, hash: BlockHash) -> Result<Block, ApiError> {
        let call = self.node.block(hash);
        self.handle(key, "Chain/Block", Scope::ChainRead, call)
            .await
    }

    /// `ChainSource::transaction`.
    pub async fn transaction(
        &self,
        key: &Key,
        txid: Txid,
        block: Option<BlockHash>,
    ) -> Result<Transaction, ApiError> {
        let call = self.node.transaction(txid, block);
        self.handle(key, "Chain/Transaction", Scope::ChainRead, call)
            .await
    }

    /// `ChainSource::utxo`.
    pub async fn utxo(
        &self,
        key: &Key,
        outpoint: OutPoint,
        include_mempool: bool,
    ) -> Result<Option<Utxo>, ApiError> {
        let call = self.node.utxo(outpoint, include_mempool);
        self.handle(key, "Chain/Utxo", Scope::ChainRead, call).await
    }

    /// `MempoolSource::txids`.
    pub async fn txids(&self, key: &Key) -> Result<Vec<Txid>, ApiError> {
        self.handle(key, "Mempool/Txids", Scope::MempoolRead, self.node.txids())
            .await
    }

    /// `MempoolSource::snapshot`.
    pub async fn snapshot(&self, key: &Key) -> Result<MempoolSnapshot, ApiError> {
        let call = self.node.snapshot();
        self.handle(key, "Mempool/Snapshot", Scope::MempoolRead, call)
            .await
    }

    /// `MempoolSource::entry`.
    pub async fn entry(&self, key: &Key, txid: Txid) -> Result<Option<MempoolEntry>, ApiError> {
        let call = self.node.entry(txid);
        self.handle(key, "Mempool/Entry", Scope::MempoolRead, call)
            .await
    }

    /// `Broadcaster::broadcast`.
    pub async fn broadcast(&self, key: &Key, tx: &Transaction) -> Result<Txid, ApiError> {
        let call = self.node.broadcast(tx);
        self.handle(key, "Broadcast/Broadcast", Scope::Broadcast, call)
            .await
    }

    /// `Broadcaster::broadcast_package`.
    pub async fn broadcast_package(
        &self,
        key: &Key,
        txs: &[Transaction],
    ) -> Result<Vec<PackageResult>, ApiError> {
        let call = self.node.broadcast_package(txs);
        self.handle(key, "Broadcast/BroadcastPackage", Scope::Broadcast, call)
            .await
    }

    /// `Broadcaster::test_accept`.
    pub async fn test_accept(
        &self,
        key: &Key,
        txs: &[Transaction],
    ) -> Result<Vec<MempoolAccept>, ApiError> {
        let call = self.node.test_accept(txs);
        self.handle(key, "Broadcast/TestAccept", Scope::Broadcast, call)
            .await
    }

    /// `FeeEstimator::estimate_fee`.
    pub async fn estimate_fee(
        &self,
        key: &Key,
        target: FeeTarget,
    ) -> Result<Option<FeeRate>, ApiError> {
        let call = self.node.estimate_fee(target);
        self.handle(key, "Fees/EstimateFee", Scope::FeesRead, call)
            .await
    }

    /// `EventSource::subscribe`, through the fan-out.
    /// Holds one of the key's subscription slots until it is dropped.
    pub async fn subscribe(&self, key: &Key) -> Result<Subscription, ApiError> {
        let start = Instant::now();
        let result = key
            .require(Scope::Events)
            .and_then(|()| key.subscription_slot())
            .map(|slot| self.fanout.subscribe().holding(slot));
        self.observe(key, "Events/Subscribe", start, result.as_ref().err());
        result
    }

    /// Records one request. Logs key name, method, outcome and duration;
    /// never params or results.
    pub(crate) fn observe(
        &self,
        key: &Key,
        method: &str,
        start: Instant,
        error: Option<&ApiError>,
    ) {
        self.metrics
            .request(&key.name, method, start.elapsed(), error);
        let outcome = error.map_or("ok", |e| e.code.as_str());
        tracing::info!(
            key = %key.name,
            method,
            outcome,
            duration_ms = start.elapsed().as_millis() as u64,
            "request"
        );
    }
}
