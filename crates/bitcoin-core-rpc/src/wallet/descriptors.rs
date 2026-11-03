//! Descriptors and HD keys, rescans, pruned funds, message signing, signers.
use super::{AddressType, ImportDescriptor, Wallet};
use crate::core;
use bitcoin::bip32::Xpub;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::sign_message::MessageSignature;
use bitcoin::{Address, MerkleBlock, Transaction, Txid};
use bitcoin_node_api::ApiResult;
use corepc_types::model;
use serde_json::{Map, json};

impl Wallet {
    /// One result per request, in order. Rescans from the earliest
    /// timestamp, which can take minutes.
    pub async fn import_descriptors(
        &self,
        requests: &[ImportDescriptor],
    ) -> ApiResult<core::ImportDescriptors> {
        let requests: Vec<_> = requests.iter().map(ImportDescriptor::to_json).collect();
        self.call("importdescriptors", json!([requests])).await
    }

    /// `private` includes private keys; the wallet must be unlocked.
    pub async fn list_descriptors(&self, private: bool) -> ApiResult<core::ListDescriptors> {
        self.call("listdescriptors", json!([private])).await
    }

    /// Adds active descriptors of `address_type` from an HD key the wallet
    /// holds (`hdkey`, default the one in use): both external and internal
    /// unless `internal` picks one.
    pub async fn create_wallet_descriptor(
        &self,
        address_type: AddressType,
        internal: Option<bool>,
        hdkey: Option<&Xpub>,
    ) -> ApiResult<core::CreateWalletDescriptor> {
        let mut o = Map::new();
        super::options::put(&mut o, "internal", internal);
        super::options::put(&mut o, "hdkey", hdkey.map(Xpub::to_string));
        let params = json!([address_type.as_str(), super::options::object_or_null(o)]);
        self.call("createwalletdescriptor", params).await
    }

    /// `private` adds the xprvs; the wallet must be unlocked.
    pub async fn get_hd_keys(
        &self,
        active_only: bool,
        private: bool,
    ) -> ApiResult<model::GetHdKeys> {
        let mut o = Map::new();
        super::options::put(&mut o, "active_only", active_only.then_some(true));
        super::options::put(&mut o, "private", private.then_some(true));
        let params = json!([super::options::object_or_null(o)]);
        versioned!(self, "gethdkeys", params, GetHdKeys)
    }

    /// From `start_height` (default 0) to `stop_height` (default the tip).
    pub async fn rescan_blockchain(
        &self,
        start_height: Option<u32>,
        stop_height: Option<u32>,
    ) -> ApiResult<core::RescanBlockchain> {
        self.call("rescanblockchain", json!([start_height, stop_height]))
            .await
    }

    /// `false` when no rescan was running.
    pub async fn abort_rescan(&self) -> ApiResult<bool> {
        self.call("abortrescan", json!([])).await
    }

    /// For pruned nodes: adds `tx`, which pays this wallet, with its proof
    /// from `get_tx_out_proof`.
    pub async fn import_pruned_funds(
        &self,
        tx: &Transaction,
        proof: &MerkleBlock,
    ) -> ApiResult<()> {
        let params = json!([serialize_hex(tx), serialize_hex(proof)]);
        self.call("importprunedfunds", params).await
    }

    /// `removeprunedfunds`: deletes a tx added with `importprunedfunds` from the wallet.
    pub async fn remove_pruned_funds(&self, txid: Txid) -> ApiResult<()> {
        self.call("removeprunedfunds", json!([txid])).await
    }

    /// Legacy (P2PKH) addresses only.
    pub async fn sign_message(
        &self,
        address: &Address,
        message: &str,
    ) -> ApiResult<MessageSignature> {
        let r: model::SignMessage =
            versioned!(self, "signmessage", json!([address, message]), SignMessage)?;
        Ok(r.0)
    }

    /// Shows `address` on the external signer. Needs `-signer`.
    pub async fn wallet_display_address(
        &self,
        address: &Address,
    ) -> ApiResult<model::WalletDisplayAddress> {
        versioned!(
            self,
            "walletdisplayaddress",
            json!([address]),
            WalletDisplayAddress
        )
    }
}
