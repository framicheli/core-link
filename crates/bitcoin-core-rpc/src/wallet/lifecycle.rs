//! Unload, info, backup, flags, keypool, encryption.
use super::Wallet;
use crate::core;
use bitcoin_node_api::ApiResult;
use corepc_types::model;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;

/// corepc says `unloadwallet` returns nothing; Core 29–31 send `{warnings}`.
#[derive(Deserialize)]
struct Warnings {
    #[serde(default)]
    warnings: Vec<String>,
}

impl Wallet {
    /// Returns Core's warnings. `load_on_startup`: `None` leaves the startup
    /// list unchanged.
    pub async fn unload_wallet(&self, load_on_startup: Option<bool>) -> ApiResult<Vec<String>> {
        let r: Warnings = self
            .call("unloadwallet", json!([null, load_on_startup]))
            .await?;
        Ok(r.warnings)
    }

    /// Core 30 dropped the balance and legacy fields; they are `None` there.
    pub async fn get_wallet_info(&self) -> ApiResult<model::GetWalletInfo> {
        versioned!(self, "getwalletinfo", json!([]), GetWalletInfo)
    }

    /// `destination` is a path on the node's filesystem.
    pub async fn backup_wallet(&self, destination: &str) -> ApiResult<()> {
        self.call("backupwallet", json!([destination])).await
    }

    /// `flag` is a Core wallet flag; today only `avoid_reuse` can change.
    pub async fn set_wallet_flag(&self, flag: &str, value: bool) -> ApiResult<core::SetWalletFlag> {
        self.call("setwalletflag", json!([flag, value])).await
    }

    /// `new_size` defaults to `-keypool` (1000).
    pub async fn keypool_refill(&self, new_size: Option<u32>) -> ApiResult<()> {
        self.call("keypoolrefill", json!([new_size])).await
    }

    /// Returns Core's note. The wallet is locked afterwards.
    pub async fn encrypt_wallet(&self, passphrase: &str) -> ApiResult<String> {
        self.call("encryptwallet", json!([passphrase])).await
    }

    /// Unlocks for `timeout` (whole seconds; Core caps it at ~3 years).
    pub async fn wallet_passphrase(&self, passphrase: &str, timeout: Duration) -> ApiResult<()> {
        self.call("walletpassphrase", json!([passphrase, timeout.as_secs()]))
            .await
    }

    /// `walletpassphrasechange`: changes the encryption passphrase from `old` to `new`.
    pub async fn wallet_passphrase_change(&self, old: &str, new: &str) -> ApiResult<()> {
        self.call("walletpassphrasechange", json!([old, new])).await
    }

    /// `walletlock`: removes the decryption key from memory, locking the wallet.
    pub async fn wallet_lock(&self) -> ApiResult<()> {
        self.call("walletlock", json!([])).await
    }
}
