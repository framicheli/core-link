//! `== Mining ==`
use crate::Client;
use bitcoin::block::Header;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::{Block, SignedAmount, Txid};
use bitcoin_node_api::ApiResult;
use corepc_types::model;
use serde_json::json;

impl Client {
    /// Template request with the `segwit` rule. Longpolling lives on `TemplateSource::wait_next`.
    pub async fn get_block_template(&self) -> ApiResult<model::GetBlockTemplate> {
        let params = json!([{ "rules": ["segwit"] }]);
        versioned!(self, "getblocktemplate", params, GetBlockTemplate)
    }

    /// Asks Core to fully validate `block` without submitting it (BIP 23
    /// proposal mode). `None` if valid; otherwise Core's reason (`"bad-txnmrklroot"`,
    /// `"inconclusive-not-best-prevblk"`, ...). Proof of work is not checked.
    pub async fn get_block_template_proposal(&self, block: &Block) -> ApiResult<Option<String>> {
        let params = json!([{ "mode": "proposal", "data": serialize_hex(block) }]);
        self.call("getblocktemplate", params).await
    }

    /// `getmininginfo`: tip, difficulty, network hashrate and mempool size.
    pub async fn get_mining_info(&self) -> ApiResult<model::GetMiningInfo> {
        versioned!(self, "getmininginfo", json!([]), GetMiningInfo)
    }

    /// Defaults: last 120 blocks, at the tip.
    pub async fn get_network_hash_ps(
        &self,
        n_blocks: Option<i64>,
        height: Option<i64>,
    ) -> ApiResult<f64> {
        self.call("getnetworkhashps", json!([n_blocks, height]))
            .await
    }

    /// `getprioritisedtransactions`: fee deltas set with `prioritisetransaction`, by txid.
    pub async fn get_prioritised_transactions(
        &self,
    ) -> ApiResult<model::GetPrioritisedTransactions> {
        versioned!(
            self,
            "getprioritisedtransactions",
            json!([]),
            GetPrioritisedTransactions
        )
    }

    /// `prioritisetransaction`: adds `fee_delta` to `txid`'s fee for block template
    /// selection and eviction; the fee actually paid is unchanged.
    pub async fn prioritise_transaction(
        &self,
        txid: Txid,
        fee_delta: SignedAmount,
    ) -> ApiResult<bool> {
        // The middle argument is a dummy Core requires to be null or 0.
        let params = json!([txid, null, fee_delta.to_sat()]);
        self.call("prioritisetransaction", params).await
    }

    /// `None` if accepted; otherwise Core's reason (`"duplicate"`, `"high-hash"`, ...).
    pub async fn submit_block(&self, block: &Block) -> ApiResult<Option<String>> {
        self.call("submitblock", json!([serialize_hex(block)]))
            .await
    }

    /// `submitheader`: validates `header` and adds it to the block tree. Its parent
    /// must be known.
    pub async fn submit_header(&self, header: &Header) -> ApiResult<()> {
        self.call("submitheader", json!([serialize_hex(header)]))
            .await
    }
}
