//! `== Network ==`
use crate::{Client, core};
use bitcoin_node_api::ApiResult;
use corepc_types::model;
use serde_json::json;
use std::time::Duration;

/// `addnode` command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddNodeCommand {
    /// Add to the added-node list and keep connecting to it.
    Add,
    /// Remove from the added-node list.
    Remove,
    /// Connect once, without adding to the list.
    OneTry,
}

/// `setban` command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetBanCommand {
    /// Ban the subnet.
    Add,
    /// Lift the ban.
    Remove,
}

impl Client {
    /// `node` is `host:port`. `v2transport` defaults to the node's `-v2transport`.
    pub async fn add_node(
        &self,
        node: &str,
        command: AddNodeCommand,
        v2transport: Option<bool>,
    ) -> ApiResult<()> {
        let command = match command {
            AddNodeCommand::Add => "add",
            AddNodeCommand::Remove => "remove",
            AddNodeCommand::OneTry => "onetry",
        };
        self.call("addnode", json!([node, command, v2transport]))
            .await
    }

    /// `clearbanned`: lifts all bans.
    pub async fn clear_banned(&self) -> ApiResult<()> {
        self.call("clearbanned", json!([])).await
    }

    /// `address` is `host:port`.
    pub async fn disconnect_node(&self, address: &str) -> ApiResult<()> {
        self.call("disconnectnode", json!([address])).await
    }

    /// `disconnectnode` by peer id (from `getpeerinfo`).
    pub async fn disconnect_node_by_id(&self, node_id: u64) -> ApiResult<()> {
        self.call("disconnectnode", json!(["", node_id])).await
    }

    /// All added nodes, or just `node`.
    pub async fn get_added_node_info(
        &self,
        node: Option<&str>,
    ) -> ApiResult<core::GetAddedNodeInfo> {
        let params = match node {
            Some(n) => json!([n]),
            None => json!([]),
        };
        self.call("getaddednodeinfo", params).await
    }

    /// `getaddrmaninfo`: address manager counts (new, tried, total) per network.
    pub async fn get_addr_man_info(&self) -> ApiResult<core::GetAddrManInfo> {
        self.call("getaddrmaninfo", json!([])).await
    }

    /// `getconnectioncount`: number of peer connections.
    pub async fn get_connection_count(&self) -> ApiResult<u64> {
        self.call("getconnectioncount", json!([])).await
    }

    /// `getnettotals`: bytes sent and received, and the upload target state.
    pub async fn get_net_totals(&self) -> ApiResult<core::GetNetTotals> {
        self.call("getnettotals", json!([])).await
    }

    /// `getnetworkinfo`: version, services, networks, local addresses and relay fees.
    pub async fn get_network_info(&self) -> ApiResult<model::GetNetworkInfo> {
        versioned!(self, "getnetworkinfo", json!([]), GetNetworkInfo)
    }

    /// `count` defaults to 1 (0 = all); `network` filters (`ipv4`, `onion`, ...).
    pub async fn get_node_addresses(
        &self,
        count: Option<u32>,
        network: Option<&str>,
    ) -> ApiResult<core::GetNodeAddresses> {
        self.call("getnodeaddresses", json!([count, network])).await
    }

    /// `getpeerinfo`: one entry per connected peer.
    pub async fn get_peer_info(&self) -> ApiResult<Vec<core::PeerInfo>> {
        self.call("getpeerinfo", json!([])).await
    }

    /// `listbanned`: banned subnets with their ban creation and expiry times.
    pub async fn list_banned(&self) -> ApiResult<core::ListBanned> {
        self.call("listbanned", json!([])).await
    }

    /// `ping`: queues a ping to every peer; results show in `getpeerinfo`.
    pub async fn ping(&self) -> ApiResult<()> {
        self.call("ping", json!([])).await
    }

    /// `subnet` is an IP or IP/netmask. `ban_time` defaults to 24h.
    pub async fn set_ban(
        &self,
        subnet: &str,
        command: SetBanCommand,
        ban_time: Option<Duration>,
    ) -> ApiResult<()> {
        let command = match command {
            SetBanCommand::Add => "add",
            SetBanCommand::Remove => "remove",
        };
        let params = json!([subnet, command, ban_time.map(|t| t.as_secs())]);
        self.call("setban", params).await
    }

    /// Returns the new state.
    pub async fn set_network_active(&self, active: bool) -> ApiResult<bool> {
        self.call("setnetworkactive", json!([active])).await
    }
}
