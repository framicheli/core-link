//! Balances, unspent outputs, locks, groupings.
use super::{ListUnspentQuery, LockAction, Wallet};
use bitcoin::{Address, Amount, OutPoint};
use bitcoin_node_api::ApiResult;
use corepc_types::model;
use serde_json::json;

impl Wallet {
    /// `min_conf` defaults to 0. `avoid_reuse` (default true) only applies
    /// to wallets with the `avoid_reuse` flag.
    pub async fn get_balance(
        &self,
        min_conf: Option<u32>,
        avoid_reuse: Option<bool>,
    ) -> ApiResult<Amount> {
        // `dummy` and the no-op `include_watchonly` stay null.
        let params = json!([null, min_conf, null, avoid_reuse]);
        let r: model::GetBalance = versioned!(self, "getbalance", params, GetBalance)?;
        Ok(r.0)
    }

    /// `watch_only` is gone on Core 30+.
    pub async fn get_balances(&self) -> ApiResult<model::GetBalances> {
        versioned!(self, "getbalances", json!([]), GetBalances)
    }

    /// `min_conf` defaults to 1, `max_conf` to 9999999, `include_unsafe` to
    /// true. An empty `addresses` doesn't filter.
    pub async fn list_unspent(
        &self,
        min_conf: Option<u32>,
        max_conf: Option<u32>,
        addresses: &[Address],
        include_unsafe: Option<bool>,
        query: ListUnspentQuery,
    ) -> ApiResult<model::ListUnspent> {
        let addresses = (!addresses.is_empty()).then_some(addresses);
        let params = json!([
            min_conf,
            max_conf,
            addresses,
            include_unsafe,
            query.to_json()
        ]);
        versioned!(self, "listunspent", params, ListUnspent)
    }

    /// Locks are in memory unless `persistent` (ignored when unlocking).
    /// Unlocking with no outpoints unlocks everything.
    pub async fn lock_unspent(
        &self,
        action: LockAction,
        outpoints: &[OutPoint],
        persistent: Option<bool>,
    ) -> ApiResult<bool> {
        // Core unlocks everything only when the list is left out, not for `[]`.
        let outpoints = (!outpoints.is_empty()).then(|| {
            outpoints
                .iter()
                .map(|o| json!({ "txid": o.txid, "vout": o.vout }))
                .collect::<Vec<_>>()
        });
        let unlock = action == LockAction::Unlock;
        self.call("lockunspent", json!([unlock, outpoints, persistent]))
            .await
    }

    /// `listlockunspent`: outputs locked with `lockunspent`.
    pub async fn list_lock_unspent(&self) -> ApiResult<Vec<OutPoint>> {
        let r: model::ListLockUnspent =
            versioned!(self, "listlockunspent", json!([]), ListLockUnspent)?;
        Ok(r.0
            .into_iter()
            .map(|o| OutPoint {
                txid: o.txid,
                vout: o.vout,
            })
            .collect())
    }

    /// `listaddressgroupings`: addresses grouped by common ownership made public by
    /// spending them together.
    pub async fn list_address_groupings(&self) -> ApiResult<model::ListAddressGroupings> {
        versioned!(
            self,
            "listaddressgroupings",
            json!([]),
            ListAddressGroupings
        )
    }
}
