//! JSON shapes behind the `serde` feature: v1, frozen (DESIGN.md §3.5). The
//! gateway's REST and WebSocket clients see them; `tests/serde.rs` pins them.
//!
//! Enums are `{"type": "snake_case_variant", ...fields}`. serde can't put a tag
//! next to a bare value (`BlockConnected(hash)`), so those enums convert
//! through a mirror with named fields. Hashes, scripts and transactions are
//! hex; amounts are satoshis.
use crate::{NodeAPIError, NodeEvent, PackageOutcome, RpcErrorCode, TransportError};
use bitcoin::{Amount, BlockHash, Txid};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum NodeEventWire {
    BlockConnected {
        hash: BlockHash,
    },
    BlockDisconnected {
        hash: BlockHash,
    },
    TxAdded {
        txid: Txid,
        mempool_sequence: Option<u64>,
    },
    TxRemoved {
        txid: Txid,
        mempool_sequence: Option<u64>,
    },
    Lagged,
}

impl From<NodeEvent> for NodeEventWire {
    fn from(e: NodeEvent) -> Self {
        match e {
            NodeEvent::BlockConnected(hash) => NodeEventWire::BlockConnected { hash },
            NodeEvent::BlockDisconnected(hash) => NodeEventWire::BlockDisconnected { hash },
            NodeEvent::TxAdded {
                txid,
                mempool_sequence,
            } => NodeEventWire::TxAdded {
                txid,
                mempool_sequence,
            },
            NodeEvent::TxRemoved {
                txid,
                mempool_sequence,
            } => NodeEventWire::TxRemoved {
                txid,
                mempool_sequence,
            },
            NodeEvent::Lagged => NodeEventWire::Lagged,
        }
    }
}

impl From<NodeEventWire> for NodeEvent {
    fn from(e: NodeEventWire) -> Self {
        match e {
            NodeEventWire::BlockConnected { hash } => NodeEvent::BlockConnected(hash),
            NodeEventWire::BlockDisconnected { hash } => NodeEvent::BlockDisconnected(hash),
            NodeEventWire::TxAdded {
                txid,
                mempool_sequence,
            } => NodeEvent::TxAdded {
                txid,
                mempool_sequence,
            },
            NodeEventWire::TxRemoved {
                txid,
                mempool_sequence,
            } => NodeEvent::TxRemoved {
                txid,
                mempool_sequence,
            },
            NodeEventWire::Lagged => NodeEvent::Lagged,
        }
    }
}

/// `code` is Core's number, so codes this crate doesn't name survive.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum NodeAPIErrorWire {
    Rpc { code: i64, message: String },
    Transport { error: TransportError },
    Decode { message: String },
    Unsupported { message: String },
}

impl From<NodeAPIError> for NodeAPIErrorWire {
    fn from(e: NodeAPIError) -> Self {
        match e {
            NodeAPIError::Rpc { code, message } => NodeAPIErrorWire::Rpc {
                code: code.code(),
                message,
            },
            NodeAPIError::Transport(error) => NodeAPIErrorWire::Transport { error },
            NodeAPIError::Decode(message) => NodeAPIErrorWire::Decode { message },
            NodeAPIError::Unsupported(message) => NodeAPIErrorWire::Unsupported { message },
        }
    }
}

impl From<NodeAPIErrorWire> for NodeAPIError {
    fn from(e: NodeAPIErrorWire) -> Self {
        match e {
            NodeAPIErrorWire::Rpc { code, message } => NodeAPIError::Rpc {
                code: RpcErrorCode::from(code),
                message,
            },
            NodeAPIErrorWire::Transport { error } => NodeAPIError::Transport(error),
            NodeAPIErrorWire::Decode { message } => NodeAPIError::Decode(message),
            NodeAPIErrorWire::Unsupported { message } => NodeAPIError::Unsupported(message),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum TransportErrorWire {
    Connect { message: String },
    Timeout,
    Auth,
    Http { status: u16 },
    Other { message: String },
}

impl From<TransportError> for TransportErrorWire {
    fn from(e: TransportError) -> Self {
        match e {
            TransportError::Connect(message) => TransportErrorWire::Connect { message },
            TransportError::Timeout => TransportErrorWire::Timeout,
            TransportError::Auth => TransportErrorWire::Auth,
            TransportError::Http(status) => TransportErrorWire::Http { status },
            TransportError::Other(message) => TransportErrorWire::Other { message },
        }
    }
}

impl From<TransportErrorWire> for TransportError {
    fn from(e: TransportErrorWire) -> Self {
        match e {
            TransportErrorWire::Connect { message } => TransportError::Connect(message),
            TransportErrorWire::Timeout => TransportError::Timeout,
            TransportErrorWire::Auth => TransportError::Auth,
            TransportErrorWire::Http { status } => TransportError::Http(status),
            TransportErrorWire::Other { message } => TransportError::Other(message),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PackageOutcomeWire {
    Accepted { fee: Amount },
    AlreadyInMempool,
    Rejected { reason: String },
}

impl From<PackageOutcome> for PackageOutcomeWire {
    fn from(o: PackageOutcome) -> Self {
        match o {
            PackageOutcome::Accepted { fee } => PackageOutcomeWire::Accepted { fee },
            PackageOutcome::AlreadyInMempool => PackageOutcomeWire::AlreadyInMempool,
            PackageOutcome::Rejected(reason) => PackageOutcomeWire::Rejected { reason },
        }
    }
}

impl From<PackageOutcomeWire> for PackageOutcome {
    fn from(o: PackageOutcomeWire) -> Self {
        match o {
            PackageOutcomeWire::Accepted { fee } => PackageOutcome::Accepted { fee },
            PackageOutcomeWire::AlreadyInMempool => PackageOutcome::AlreadyInMempool,
            PackageOutcomeWire::Rejected { reason } => PackageOutcome::Rejected(reason),
        }
    }
}

/// A transaction or header as consensus hex, like Core's `hex` fields.
pub(crate) mod consensus_hex {
    use bitcoin::consensus::encode::{Decodable, Encodable, deserialize_hex, serialize_hex};
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub(crate) fn serialize<T: Encodable, S: Serializer>(v: &T, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&serialize_hex(v))
    }

    pub(crate) fn deserialize<'de, T: Decodable, D: Deserializer<'de>>(
        d: D,
    ) -> Result<T, D::Error> {
        deserialize_hex(&String::deserialize(d)?).map_err(D::Error::custom)
    }
}

/// `Option<[u8; 32]>` as a hex string or `null`.
pub(crate) mod hex32 {
    use bitcoin::hex::{DisplayHex, FromHex};
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub(crate) fn serialize<S: Serializer>(v: &Option<[u8; 32]>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(b) => s.serialize_some(&b.to_lower_hex_string()),
            None => s.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<[u8; 32]>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|h| <[u8; 32]>::from_hex(&h).map_err(D::Error::custom))
            .transpose()
    }
}

/// `Option<Duration>` as whole milliseconds or `null`.
pub(crate) mod millis {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub(crate) fn serialize<S: Serializer>(v: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(d) => s.serialize_some(&(d.as_millis() as u64)),
            None => s.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Duration>, D::Error> {
        Ok(Option::<u64>::deserialize(d)?.map(Duration::from_millis))
    }
}
