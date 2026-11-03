//! `== Control ==`
use crate::{Client, core};
use bitcoin_node_api::ApiResult;
use serde_json::json;

impl Client {
    /// `getmemoryinfo` (`stats` mode): locked memory pool usage.
    pub async fn get_memory_info(&self) -> ApiResult<core::GetMemoryInfoStats> {
        self.call("getmemoryinfo", json!(["stats"])).await
    }

    /// `getrpcinfo`: running RPC commands and the debug log path.
    pub async fn get_rpc_info(&self) -> ApiResult<core::GetRpcInfo> {
        self.call("getrpcinfo", json!([])).await
    }

    /// All commands, or help for one.
    pub async fn help(&self, command: Option<&str>) -> ApiResult<String> {
        self.call("help", json!([command])).await
    }

    /// Turns log categories on/off; empty slices just read the current state.
    pub async fn logging(&self, include: &[&str], exclude: &[&str]) -> ApiResult<core::Logging> {
        self.call("logging", json!([include, exclude])).await
    }

    /// `stop`: asks the node to shut down. The reply comes before it exits.
    pub async fn stop(&self) -> ApiResult<String> {
        self.call("stop", json!([])).await
    }

    /// Seconds since the node started.
    pub async fn uptime(&self) -> ApiResult<u64> {
        self.call("uptime", json!([])).await
    }
}
