//! `== Wallet ==`: the `Wallet` handle, and the node-level calls that create,
//! load and list wallets. Descriptor wallets only.
mod addresses;
mod coins;
mod descriptors;
mod history;
mod lifecycle;
pub(crate) mod options;
mod spending;

pub use options::{
    AddressType, BumpFeeOptions, CreateWalletOptions, FundOptions, ImportDescriptor,
    ListUnspentQuery, LockAction, SendAllOptions, SendManyOptions, SendOptions,
    SendToAddressOptions, SolvingData, Timestamp,
};

use crate::{Client, Version, core};
use bitcoin_node_api::ApiResult;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

/// One loaded wallet: every call goes to Core's `/wallet/<name>` endpoint,
/// so it never depends on which other wallets are loaded.
#[derive(Debug, Clone)]
pub struct Wallet {
    client: Client,
    name: String,
    url: String,
}

/// A wallet that was just created, loaded or restored, with Core's warnings.
#[derive(Debug, Clone)]
pub struct Loaded {
    /// The wallet.
    pub wallet: Wallet,
    /// Core's warnings; empty if none.
    pub warnings: Vec<String>,
}

/// `{name, warnings}`, from `createwallet`, `loadwallet` and `restorewallet`.
#[derive(Deserialize)]
struct NameWarnings {
    name: String,
    #[serde(default)]
    warnings: Vec<String>,
}

impl NameWarnings {
    fn loaded(self, client: &Client) -> Loaded {
        Loaded {
            wallet: client.wallet(&self.name),
            warnings: self.warnings,
        }
    }
}

/// `<base>/wallet/<name>`, the name percent-encoded as one path segment.
fn wallet_url(base: &str, name: &str) -> String {
    match reqwest::Url::parse(base) {
        Ok(mut url) if !url.cannot_be_a_base() => {
            url.path_segments_mut()
                .expect("checked above")
                .pop_if_empty()
                .extend(["wallet", name]);
            url.into()
        }
        // Not a URL: every call fails the same way the node's own calls do.
        _ => base.to_owned(),
    }
}

impl Client {
    /// No I/O: routes later calls to `<url>/wallet/<name>`.
    pub fn wallet(&self, name: &str) -> Wallet {
        Wallet {
            client: self.clone(),
            name: name.to_owned(),
            url: wallet_url(self.url(), name),
        }
    }

    /// Always a descriptor wallet.
    pub async fn create_wallet(&self, name: &str, opts: CreateWalletOptions) -> ApiResult<Loaded> {
        let r: NameWarnings = self.call("createwallet", opts.params(name)).await?;
        Ok(r.loaded(self))
    }

    /// `name` may also be a path, absolute or relative to `-walletdir`.
    /// `load_on_startup`: `None` leaves the startup list unchanged.
    pub async fn load_wallet(
        &self,
        name: &str,
        load_on_startup: Option<bool>,
    ) -> ApiResult<Loaded> {
        let r: NameWarnings = self
            .call("loadwallet", json!([name, load_on_startup]))
            .await?;
        Ok(r.loaded(self))
    }

    /// `backup_file` is a path on the node's filesystem.
    pub async fn restore_wallet(
        &self,
        name: &str,
        backup_file: &str,
        load_on_startup: Option<bool>,
    ) -> ApiResult<Loaded> {
        let params = json!([name, backup_file, load_on_startup]);
        let r: NameWarnings = self.call("restorewallet", params).await?;
        Ok(r.loaded(self))
    }

    /// Names of the loaded wallets.
    pub async fn list_wallets(&self) -> ApiResult<Vec<String>> {
        self.call("listwallets", json!([])).await
    }

    /// Wallets in `-walletdir`, loaded or not.
    pub async fn list_wallet_dir(&self) -> ApiResult<core::ListWalletDir> {
        self.call("listwalletdir", json!([])).await
    }

    /// Migrates a legacy wallet to descriptors. Core 30+ can't load legacy
    /// wallets otherwise; a descriptor wallet is refused.
    pub async fn migrate_wallet(
        &self,
        name: &str,
        passphrase: Option<&str>,
    ) -> ApiResult<core::MigrateWallet> {
        self.call("migratewallet", json!([name, passphrase])).await
    }
}

impl Wallet {
    /// Wallet name, as in `listwallets`; `""` is the default wallet.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The node-level client, for non-wallet calls.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Escape hatch for wallet RPCs added after this crate, posted to this
    /// wallet's endpoint like every other `Wallet` call.
    pub async fn raw_call<T: DeserializeOwned>(&self, method: &str, params: Value) -> ApiResult<T> {
        self.call(method, params).await
    }

    pub(crate) fn version(&self) -> Version {
        self.client.version()
    }

    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> ApiResult<T> {
        self.client
            .call_at(&self.url, Some(&self.name), method, params)
            .await
    }
}
