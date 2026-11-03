//! New and change addresses, address info, labels.
use super::{AddressType, Wallet};
use bitcoin::Address;
use bitcoin::address::NetworkUnchecked;
use bitcoin_node_api::ApiResult;
use corepc_types::model;
use serde_json::json;

impl Wallet {
    /// `label` defaults to `""`; `address_type` to `-addresstype`.
    pub async fn get_new_address(
        &self,
        label: Option<&str>,
        address_type: Option<AddressType>,
    ) -> ApiResult<Address<NetworkUnchecked>> {
        let params = json!([label, address_type.map(AddressType::as_str)]);
        let r: model::GetNewAddress = versioned!(self, "getnewaddress", params, GetNewAddress)?;
        Ok(r.0)
    }

    /// `address_type` defaults to `-changetype`.
    pub async fn get_raw_change_address(
        &self,
        address_type: Option<AddressType>,
    ) -> ApiResult<Address<NetworkUnchecked>> {
        let params = json!([address_type.map(AddressType::as_str)]);
        let r: model::GetRawChangeAddress =
            versioned!(self, "getrawchangeaddress", params, GetRawChangeAddress)?;
        Ok(r.0)
    }

    /// `is_watch_only` is always false on Core 30+ (deprecated).
    pub async fn get_address_info(&self, address: &Address) -> ApiResult<model::GetAddressInfo> {
        versioned!(self, "getaddressinfo", json!([address]), GetAddressInfo)
    }

    /// `getaddressesbylabel`: the addresses with `label`, and their purpose.
    pub async fn get_addresses_by_label(
        &self,
        label: &str,
    ) -> ApiResult<model::GetAddressesByLabel> {
        versioned!(
            self,
            "getaddressesbylabel",
            json!([label]),
            GetAddressesByLabel
        )
    }

    /// `setlabel`: sets `address`'s label.
    pub async fn set_label(&self, address: &Address, label: &str) -> ApiResult<()> {
        self.call("setlabel", json!([address, label])).await
    }

    /// All labels, or only those used for `purpose`.
    pub async fn list_labels(
        &self,
        purpose: Option<model::AddressPurpose>,
    ) -> ApiResult<Vec<String>> {
        let purpose = purpose.map(|p| match p {
            model::AddressPurpose::Send => "send",
            model::AddressPurpose::Receive => "receive",
        });
        self.call("listlabels", json!([purpose])).await
    }
}
