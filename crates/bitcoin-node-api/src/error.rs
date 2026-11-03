//! Errors: RPC errors with Core's codes, transport and decode failures.

use std::fmt;

/// Every failure a trait method can return.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(
        from = "crate::wire::NodeAPIErrorWire",
        into = "crate::wire::NodeAPIErrorWire"
    )
)]
pub enum NodeAPIError {
    /// The node answered with an RPC error.
    Rpc {
        /// Core's error code.
        code: RpcErrorCode,
        /// Core's error message, verbatim.
        message: String,
    },
    /// Couldn't reach the node.
    Transport(TransportError),
    /// Reached the node, couldn't parse the reply.
    Decode(String),
    /// This backend or Core version can't do this.
    Unsupported(String),
}

impl fmt::Display for NodeAPIError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeAPIError::Rpc { code, message } => {
                write!(f, "rpc error {}: {message}", code.code())
            }
            NodeAPIError::Transport(e) => write!(f, "transport error: {e}"),
            NodeAPIError::Decode(e) => write!(f, "decode error: {e}"),
            NodeAPIError::Unsupported(e) => write!(f, "unsupported: {e}"),
        }
    }
}

impl std::error::Error for NodeAPIError {}

impl From<TransportError> for NodeAPIError {
    fn from(e: TransportError) -> Self {
        NodeAPIError::Transport(e)
    }
}

/// Spells each variant and its wire value once, so `code()` and `From<i64>` can't drift.
macro_rules! rpc_error_codes {
    ($($(#[$doc:meta])* $name:ident = $code:literal,)*) => {
        /// Mirrors Core's `src/rpc/protocol.h`. `Other` keeps the raw code for
        /// anything newer than this crate.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum RpcErrorCode {
            $($(#[$doc])* $name,)*
            /// A code this crate doesn't name.
            Other(i64),
        }

        impl RpcErrorCode {
            /// The wire value; `Other(n)` -> n.
            pub fn code(self) -> i64 {
                match self {
                    $(RpcErrorCode::$name => $code,)*
                    RpcErrorCode::Other(n) => n,
                }
            }
        }

        impl From<i64> for RpcErrorCode {
            fn from(n: i64) -> Self {
                match n {
                    $($code => RpcErrorCode::$name,)*
                    n => RpcErrorCode::Other(n),
                }
            }
        }
    };
}

rpc_error_codes! {
    // JSON-RPC 2.0
    /// Malformed JSON-RPC request.
    InvalidRequest = -32600,
    /// No such method on this node.
    MethodNotFound = -32601,
    /// Invalid method parameters.
    InvalidParams = -32602,
    /// Internal JSON-RPC error.
    Internal = -32603,
    /// The request body is not valid JSON.
    Parse = -32700,
    // general
    /// Uncaught exception in the command handler; Core's catch-all.
    Misc = -1,
    /// A parameter has the wrong JSON type.
    Type = -3,
    /// Invalid address or key; also an unknown block or tx.
    InvalidAddressOrKey = -5,
    /// Ran out of memory during the operation.
    OutOfMemory = -7,
    /// Invalid, missing or duplicate parameter.
    InvalidParameter = -8,
    /// Database error.
    Database = -20,
    /// Couldn't parse or validate a raw structure (tx or block hex).
    Deserialization = -22,
    /// General error submitting a tx or block (e.g. missing inputs).
    VerifyError = -25,
    /// The tx or block was rejected by consensus or policy rules.
    VerifyRejected = -26,
    /// The tx is already in the chain.
    VerifyAlreadyInChain = -27,
    /// The node is still starting up.
    InWarmup = -28,
    /// The method is deprecated.
    MethodDeprecated = -32,
    // p2p
    /// No connection to the P2P network.
    ClientNotConnected = -9,
    /// Still in initial block download.
    ClientInInitialDownload = -10,
    /// Node already added.
    ClientNodeAlreadyAdded = -23,
    /// Node was not added with `addnode`.
    ClientNodeNotAdded = -24,
    /// Node to disconnect is not connected.
    ClientNodeNotConnected = -29,
    /// Invalid IP or subnet.
    ClientInvalidIpOrSubnet = -30,
    /// P2P networking is disabled.
    ClientP2pDisabled = -31,
    /// The node has no mempool.
    ClientMempoolDisabled = -33,
    /// Already at the maximum of outbound or block-relay connections.
    ClientNodeCapacityReached = -34,
    // wallet
    /// Unspecified wallet error.
    WalletError = -4,
    /// Not enough funds in the wallet.
    WalletInsufficientFunds = -6,
    /// Invalid label name.
    WalletInvalidLabelName = -11,
    /// Keypool ran out; call `keypoolrefill` first.
    WalletKeypoolRanOut = -12,
    /// Wallet is locked; call `walletpassphrase` first.
    WalletUnlockNeeded = -13,
    /// Wrong wallet passphrase.
    WalletPassphraseIncorrect = -14,
    /// Command given in the wrong encryption state (e.g. encrypting an encrypted wallet).
    WalletWrongEncState = -15,
    /// Failed to encrypt the wallet.
    WalletEncryptionFailed = -16,
    /// Wallet is already unlocked.
    WalletAlreadyUnlocked = -17,
    /// No wallet by that name, or it isn't loaded.
    WalletNotFound = -18,
    /// Several wallets are loaded and the request names none.
    WalletNotSpecified = -19,
    /// That wallet is already loaded.
    WalletAlreadyLoaded = -35,
    /// A wallet with that name already exists.
    WalletAlreadyExists = -36,
}

/// Failure to get a reply from the node.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(
        from = "crate::wire::TransportErrorWire",
        into = "crate::wire::TransportErrorWire"
    )
)]
pub enum TransportError {
    /// Refused, DNS, TLS.
    Connect(String),
    /// No reply in time.
    Timeout,
    /// HTTP 401/403.
    Auth,
    /// Other non-2xx with a non-JSON body.
    Http(u16),
    /// Anything else, as text.
    Other(String),
}

impl TransportError {
    /// `Connect` and `Timeout` are worth retrying; the rest are not.
    pub fn is_retryable(&self) -> bool {
        matches!(self, TransportError::Connect(_) | TransportError::Timeout)
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Connect(e) => write!(f, "connect: {e}"),
            TransportError::Timeout => write!(f, "timeout"),
            TransportError::Auth => write!(f, "authentication failed"),
            TransportError::Http(s) => write!(f, "http status {s}"),
            TransportError::Other(e) => write!(f, "{e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_round_trip() {
        for n in [-32601, -5, -26, -36, -4, 12345] {
            assert_eq!(RpcErrorCode::from(n).code(), n);
        }
        assert_eq!(RpcErrorCode::from(-5), RpcErrorCode::InvalidAddressOrKey);
        assert_eq!(RpcErrorCode::from(12345), RpcErrorCode::Other(12345));
    }
}
