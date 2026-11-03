//! Core-shaped JSON for methods `corepc-types` has no `model` type for.
//! Re-exported from `corepc_types::v31` unless the JSON changed within Core 29–31,
//! in which case the struct is defined here so it parses every supported version.
use serde::Deserialize;
use std::collections::BTreeMap;

pub use corepc_types::v31::{
    AbortRescan, AddedNode, AddedNodeAddress, AddrManInfoNetwork, Banned, ConnectionType,
    CreateWalletDescriptor, DescriptorInfo, EnumerateSigners, GetAddedNodeInfo, GetAddrManInfo,
    GetDescriptorInfo, GetIndexInfo, GetIndexInfoName, GetMemoryInfoStats, GetNetTotals,
    GetNodeAddresses, GetRpcInfo, GetZmqNotifications, ImportDescriptors, ImportDescriptorsResult,
    ListBanned, ListDescriptors, ListWalletDir, ListWalletDirWallet, MigrateWallet, NodeAddress,
    RescanBlockchain, SaveMempool, ScanBlocksStatus, ScanTxOutSetStatus, SetWalletFlag, Signers,
    TransportProtocolType, UploadTarget,
};

/// `getpeerinfo` entry. v31's struct requires `inv_to_send` and
/// `last_inv_sequence`, which Core 29–30 don't send.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct PeerInfo {
    /// Peer index.
    pub id: u32,
    /// `host:port` of the peer.
    pub addr: String,
    /// Bind address of the connection to the peer.
    pub addrbind: Option<String>,
    /// Local address as reported by the peer.
    pub addrlocal: Option<String>,
    /// Network (`ipv4`, `ipv6`, `onion`, `i2p`, `cjdns`, `not_publicly_routable`).
    pub network: String,
    /// AS number of the peer, from `-asmap`.
    pub mapped_as: Option<u32>,
    /// Services offered, hex.
    pub services: String,
    /// Services offered, by name.
    pub servicesnames: Vec<String>,
    /// Whether we relay txs to this peer.
    pub relaytxes: bool,
    /// Unix time of the last send.
    pub lastsend: i64,
    /// Unix time of the last receive.
    pub lastrecv: i64,
    /// Unix time of the last valid tx received from this peer.
    pub last_transaction: i64,
    /// Unix time of the last block received from this peer.
    pub last_block: i64,
    /// Total bytes sent.
    pub bytessent: u64,
    /// Total bytes received.
    pub bytesrecv: u64,
    /// Unix time of the connection.
    pub conntime: i64,
    /// Peer's time offset, seconds.
    pub timeoffset: i64,
    /// Last ping time, seconds.
    pub pingtime: Option<f64>,
    /// Minimum observed ping time, seconds.
    pub minping: Option<f64>,
    /// Time an outstanding ping has been waiting, seconds.
    pub pingwait: Option<f64>,
    /// Peer's protocol version, e.g. 70016.
    pub version: u32,
    /// Peer's user agent.
    pub subver: String,
    /// Inbound (`true`) or outbound (`false`).
    pub inbound: bool,
    /// Whether we selected the peer as a BIP 152 high-bandwidth peer.
    pub bip152_hb_to: bool,
    /// Whether the peer selected us as a BIP 152 high-bandwidth peer.
    pub bip152_hb_from: bool,
    /// Height of header presync with this peer; -1 if none in progress.
    pub presynced_headers: Option<i64>,
    /// Last header we have in common with this peer.
    pub synced_headers: Option<i64>,
    /// Last block we have in common with this peer.
    pub synced_blocks: Option<i64>,
    /// Heights of blocks we're requesting from this peer.
    pub inflight: Option<Vec<u64>>,
    /// Whether we participate in address relay with this peer.
    pub addr_relay_enabled: Option<bool>,
    /// Addresses processed, excluding those dropped by rate limiting.
    pub addr_processed: Option<u64>,
    /// Addresses dropped by rate limiting.
    pub addr_rate_limited: Option<u64>,
    /// Permissions granted to this peer (`noban`, `relay`, ...).
    pub permissions: Vec<String>,
    /// Minimum fee rate the peer accepts, BTC/kvB.
    pub minfeefilter: f64,
    /// Bytes sent by message type.
    pub bytessent_per_msg: BTreeMap<String, u64>,
    /// Bytes received by message type.
    pub bytesrecv_per_msg: BTreeMap<String, u64>,
    /// Core 31+.
    pub inv_to_send: Option<u64>,
    /// Core 31+.
    pub last_inv_sequence: Option<u64>,
    /// Type of connection.
    pub connection_type: Option<ConnectionType>,
    /// Transport protocol: detecting, v1 or v2.
    pub transport_protocol_type: TransportProtocolType,
    /// BIP 324 session id, or `""` for v1.
    pub session_id: String,
}

/// `logging`: category -> enabled. A map because Core adds categories
/// (v31 added `kernel` and `privatebroadcast`).
pub type Logging = BTreeMap<String, bool>;
