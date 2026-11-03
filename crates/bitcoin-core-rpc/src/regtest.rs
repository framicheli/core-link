//! Hidden and test-only RPCs. Plain methods: Core rejects them where they don't apply.
use crate::Client;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::{Address, BlockHash, Transaction};
use bitcoin_node_api::ApiResult;
use corepc_types::model;
use serde_json::{Value, json};

impl Client {
    /// `generatetoaddress`: mines `n_blocks` blocks immediately, paying to `address`.
    pub async fn generate_to_address(
        &self,
        n_blocks: u32,
        address: &Address,
    ) -> ApiResult<model::GenerateToAddress> {
        versioned!(
            self,
            "generatetoaddress",
            json!([n_blocks, address]),
            GenerateToAddress
        )
    }

    /// `generatetodescriptor`: mines `n_blocks` blocks immediately, paying to `descriptor`.
    pub async fn generate_to_descriptor(
        &self,
        n_blocks: u32,
        descriptor: &str,
    ) -> ApiResult<model::GenerateToDescriptor> {
        let params = json!([n_blocks, descriptor]);
        versioned!(self, "generatetodescriptor", params, GenerateToDescriptor)
    }

    /// Mines one block paying to `output` (address or descriptor) containing exactly `txs`.
    pub async fn generate_block(
        &self,
        output: &str,
        txs: &[Transaction],
    ) -> ApiResult<model::GenerateBlock> {
        let txs: Vec<_> = txs.iter().map(serialize_hex).collect();
        versioned!(self, "generateblock", json!([output, txs]), GenerateBlock)
    }

    /// `invalidateblock`: marks `hash` and its descendants invalid, reorging away from them.
    pub async fn invalidate_block(&self, hash: BlockHash) -> ApiResult<()> {
        self.call("invalidateblock", json!([hash])).await
    }

    /// `reconsiderblock`: clears the invalid status of `hash`, its ancestors and descendants.
    pub async fn reconsider_block(&self, hash: BlockHash) -> ApiResult<()> {
        self.call("reconsiderblock", json!([hash])).await
    }

    /// Unix seconds; 0 goes back to the system clock.
    pub async fn set_mock_time(&self, time: u64) -> ApiResult<()> {
        self.call("setmocktime", json!([time])).await
    }

    /// Core 29+.
    pub async fn get_orphan_txs(&self) -> ApiResult<model::GetOrphanTxs> {
        versioned!(self, "getorphantxs", json!([]), GetOrphanTxs)
    }

    /// Returns `args` back. For connection tests.
    pub async fn echo(&self, args: &[Value]) -> ApiResult<Value> {
        self.call("echo", Value::from(args)).await
    }

    /// Like `echo`, but Core parses each arg as JSON.
    pub async fn echo_json(&self, args: &[Value]) -> ApiResult<Value> {
        self.call("echojson", Value::from(args)).await
    }
}
