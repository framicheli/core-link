//! Transactions and what was received.
use super::Wallet;
use crate::client::IntoApi;
use bitcoin::{Address, Amount, BlockHash, Txid};
use bitcoin_node_api::{ApiResult, NodeAPIError};
use corepc_types::model;
use serde_json::{Value, json};

impl Wallet {
    /// `gettransaction`: a wallet tx with amount, fee, confirmations and per-address details.
    pub async fn get_transaction(&self, txid: Txid) -> ApiResult<model::GetTransaction> {
        versioned!(self, "gettransaction", json!([txid]), GetTransaction)
    }

    /// Also fills `decoded`. Core's `decoded` is `decoderawtransaction`
    /// JSON, which a `Transaction` already holds, so it is the decoded `hex`.
    pub async fn get_transaction_verbose(&self, txid: Txid) -> ApiResult<model::GetTransaction> {
        let mut v: Value = self
            .call("gettransaction", json!([txid, null, true]))
            .await?;
        // corepc types `decoded` as a string; Core sends an object.
        if let Some(o) = v.as_object_mut() {
            o.remove("decoded");
        }
        // Core 29–31 share corepc's v28 shape.
        let r: corepc_types::v28::GetTransaction = serde_json::from_value(v)
            .map_err(|e| NodeAPIError::Decode(format!("gettransaction: {e}")))?;
        let mut tx: model::GetTransaction = r.into_model().into_api("gettransaction")?;
        tx.decoded = Some(tx.tx.clone());
        Ok(tx)
    }

    /// Most recent last. `label` filters incoming payments (`"*"`: all);
    /// `count` defaults to 10.
    pub async fn list_transactions(
        &self,
        label: Option<&str>,
        count: Option<u32>,
        skip: Option<u32>,
    ) -> ApiResult<model::ListTransactions> {
        let params = json!([label, count, skip]);
        versioned!(self, "listtransactions", params, ListTransactions)
    }

    /// Everything since `block` (all when `None`), plus txs removed by
    /// reorgs. Pass `last_block` back next time to sync.
    pub async fn list_since_block(
        &self,
        block: Option<BlockHash>,
        target_confirmations: Option<u32>,
        include_removed: Option<bool>,
        include_change: Option<bool>,
        label: Option<&str>,
    ) -> ApiResult<model::ListSinceBlock> {
        let params = json!([
            block,
            target_confirmations,
            null, // include_watchonly: no-op
            include_removed,
            include_change,
            label
        ]);
        versioned!(self, "listsinceblock", params, ListSinceBlock)
    }

    /// `listreceivedbyaddress`: amount received per address. `min_conf` defaults to 1.
    pub async fn list_received_by_address(
        &self,
        min_conf: Option<u32>,
        include_empty: Option<bool>,
        address_filter: Option<&Address>,
        include_immature_coinbase: Option<bool>,
    ) -> ApiResult<model::ListReceivedByAddress> {
        let params = json!([
            min_conf,
            include_empty,
            null, // include_watchonly: no-op
            address_filter,
            include_immature_coinbase
        ]);
        versioned!(self, "listreceivedbyaddress", params, ListReceivedByAddress)
    }

    /// `listreceivedbylabel`: amount received per label. `min_conf` defaults to 1.
    pub async fn list_received_by_label(
        &self,
        min_conf: Option<u32>,
        include_empty: Option<bool>,
        include_immature_coinbase: Option<bool>,
    ) -> ApiResult<model::ListReceivedByLabel> {
        let params = json!([min_conf, include_empty, null, include_immature_coinbase]);
        versioned!(self, "listreceivedbylabel", params, ListReceivedByLabel)
    }

    /// `min_conf` defaults to 1.
    pub async fn get_received_by_address(
        &self,
        address: &Address,
        min_conf: Option<u32>,
        include_immature_coinbase: Option<bool>,
    ) -> ApiResult<Amount> {
        let params = json!([address, min_conf, include_immature_coinbase]);
        let r: model::GetReceivedByAddress =
            versioned!(self, "getreceivedbyaddress", params, GetReceivedByAddress)?;
        Ok(r.0)
    }

    /// `label` `""` is the default label. `min_conf` defaults to 1.
    pub async fn get_received_by_label(
        &self,
        label: &str,
        min_conf: Option<u32>,
        include_immature_coinbase: Option<bool>,
    ) -> ApiResult<Amount> {
        let params = json!([label, min_conf, include_immature_coinbase]);
        let r: model::GetReceivedByLabel =
            versioned!(self, "getreceivedbylabel", params, GetReceivedByLabel)?;
        Ok(r.0)
    }
}
