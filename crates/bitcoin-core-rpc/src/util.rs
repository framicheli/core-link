//! `== Util ==`
use crate::{Client, core};
use bitcoin::sign_message::MessageSignature;
use bitcoin::{Address, PrivateKey, PublicKey};
use bitcoin_node_api::{ApiResult, FeeMode};
use corepc_types::model;
use serde_json::json;

impl Client {
    /// `createmultisig`: an `n_required`-of-`keys` multisig address, redeem script and
    /// descriptor, with Core's default `legacy` (P2SH) address type.
    pub async fn create_multisig(
        &self,
        n_required: u32,
        keys: &[PublicKey],
    ) -> ApiResult<model::CreateMultisig> {
        let keys: Vec<_> = keys.iter().map(PublicKey::to_string).collect();
        versioned!(
            self,
            "createmultisig",
            json!([n_required, keys]),
            CreateMultisig
        )
    }

    /// `range` (inclusive) is required for ranged descriptors.
    pub async fn derive_addresses(
        &self,
        descriptor: &str,
        range: Option<(u32, u32)>,
    ) -> ApiResult<model::DeriveAddresses> {
        let params = match range {
            Some((a, b)) => json!([descriptor, [a, b]]),
            None => json!([descriptor]),
        };
        versioned!(self, "deriveaddresses", params, DeriveAddresses)
    }

    /// For `<0;1>` descriptors: one address list per path. Core 29+.
    pub async fn derive_addresses_multipath(
        &self,
        descriptor: &str,
        range: Option<(u32, u32)>,
    ) -> ApiResult<model::DeriveAddressesMultipath> {
        let params = match range {
            Some((a, b)) => json!([descriptor, [a, b]]),
            None => json!([descriptor]),
        };
        versioned!(self, "deriveaddresses", params, DeriveAddressesMultipath)
    }

    /// `mode` defaults to Core's (`CONSERVATIVE`).
    pub async fn estimate_smart_fee(
        &self,
        blocks: u16,
        mode: Option<FeeMode>,
    ) -> ApiResult<model::EstimateSmartFee> {
        let mode = mode.map(|m| match m {
            FeeMode::Conservative => "CONSERVATIVE",
            FeeMode::Economical => "ECONOMICAL",
        });
        versioned!(
            self,
            "estimatesmartfee",
            json!([blocks, mode]),
            EstimateSmartFee
        )
    }

    /// `getdescriptorinfo`: `descriptor` with its checksum, and whether it is ranged,
    /// solvable and has private keys.
    pub async fn get_descriptor_info(
        &self,
        descriptor: &str,
    ) -> ApiResult<core::GetDescriptorInfo> {
        self.call("getdescriptorinfo", json!([descriptor])).await
    }

    /// `getindexinfo`: each enabled index, whether it is synced and its best height.
    pub async fn get_index_info(&self) -> ApiResult<core::GetIndexInfo> {
        self.call("getindexinfo", json!([])).await
    }

    /// `signmessagewithprivkey`: signs `message` with `key`. Never send keys to a node
    /// you don't run.
    pub async fn sign_message_with_priv_key(
        &self,
        key: &PrivateKey,
        message: &str,
    ) -> ApiResult<model::SignMessageWithPrivKey> {
        let params = json!([key.to_wif(), message]);
        versioned!(
            self,
            "signmessagewithprivkey",
            params,
            SignMessageWithPrivKey
        )
    }

    /// Takes a string: the point is to check one you don't trust yet.
    pub async fn validate_address(&self, address: &str) -> ApiResult<model::ValidateAddress> {
        versioned!(self, "validateaddress", json!([address]), ValidateAddress)
    }

    /// `verifymessage`: checks `signature` of `message` by `address` (P2PKH only).
    pub async fn verify_message(
        &self,
        address: &Address,
        signature: &MessageSignature,
        message: &str,
    ) -> ApiResult<bool> {
        let params = json!([address, signature.to_base64(), message]);
        self.call("verifymessage", params).await
    }
}
