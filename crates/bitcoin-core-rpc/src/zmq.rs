//! `== Zmq ==`
use crate::{Client, core};
use bitcoin_node_api::ApiResult;
use serde_json::json;

impl Client {
    /// Active ZMQ endpoints; `bitcoin-core-zmq` can discover them from this.
    pub async fn get_zmq_notifications(&self) -> ApiResult<Vec<core::GetZmqNotifications>> {
        self.call("getzmqnotifications", json!([])).await
    }
}
