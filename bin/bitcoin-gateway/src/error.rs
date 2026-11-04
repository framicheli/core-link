//! One error type for every front end: gRPC, REST and WebSocket each render
//! an [`ApiError`] their own way.
use bitcoin_node_api::{NodeAPIError, RpcErrorCode, TransportError};
use std::fmt;

/// What went wrong, as a client can act on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Unknown block, tx or key.
    NotFound,
    /// Rejected by consensus or policy.
    FailedPrecondition,
    /// A malformed argument.
    InvalidArgument,
    /// Core is unreachable or warming up.
    Unavailable,
    /// Core didn't answer in time.
    DeadlineExceeded,
    /// The backend can't do this.
    Unimplemented,
    /// Anything else, including the gateway's own login to Core failing.
    Internal,
    /// No token, or a token no key has.
    Unauthenticated,
    /// The key lacks the scope.
    PermissionDenied,
    /// The key's rate is exceeded.
    ResourceExhausted,
}

impl Kind {
    /// The HTTP status REST answers with.
    pub fn http_status(self) -> u16 {
        match self {
            Kind::NotFound => 404,
            Kind::FailedPrecondition => 422,
            Kind::InvalidArgument => 400,
            Kind::Unavailable => 503,
            Kind::DeadlineExceeded => 504,
            Kind::Unimplemented => 501,
            Kind::Internal => 500,
            Kind::Unauthenticated => 401,
            Kind::PermissionDenied => 403,
            Kind::ResourceExhausted => 429,
        }
    }
}

/// An error on its way to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// How clients should treat it.
    pub kind: Kind,
    /// Where it came from: `rpc`, `transport`, `decode`, `unsupported`,
    /// `auth`, `scope`, `rate`, `request`.
    pub source: &'static str,
    /// A snake_case name: Core's error code name for `rpc`
    /// (`verify_rejected`), else a short label.
    pub code: String,
    /// Core's numeric code, for `rpc` errors.
    pub core_code: Option<i64>,
    /// Shown to the client.
    pub message: String,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.code, self.source, self.message)
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    fn new(kind: Kind, source: &'static str, code: &str, message: impl Into<String>) -> ApiError {
        ApiError {
            kind,
            source,
            code: code.to_owned(),
            core_code: None,
            message: message.into(),
        }
    }

    /// No token, or an unknown one.
    pub fn unauthenticated() -> ApiError {
        ApiError::new(
            Kind::Unauthenticated,
            "auth",
            "unauthenticated",
            "missing or unknown bearer token",
        )
    }

    /// The key lacks a scope.
    pub fn permission_denied(message: impl Into<String>) -> ApiError {
        ApiError::new(
            Kind::PermissionDenied,
            "scope",
            "permission_denied",
            message,
        )
    }

    /// The key's rate is exceeded.
    pub fn rate_limited() -> ApiError {
        ApiError::rate_limited_because("rate limit exceeded")
    }

    /// A key's limit reached: its rate, its calls or its subscriptions.
    pub fn rate_limited_because(message: impl Into<String>) -> ApiError {
        ApiError::new(Kind::ResourceExhausted, "rate", "rate_limited", message)
    }

    /// A malformed request the gateway itself rejects.
    pub fn bad_request(message: impl Into<String>) -> ApiError {
        ApiError::new(Kind::InvalidArgument, "request", "bad_request", message)
    }

    /// A resource that doesn't exist (a lookup that returned `None`).
    pub fn not_found(message: impl Into<String>) -> ApiError {
        ApiError::new(Kind::NotFound, "request", "not_found", message)
    }

    /// The gateway is shutting down or has no upstream.
    pub fn unavailable(message: impl Into<String>) -> ApiError {
        ApiError::new(Kind::Unavailable, "transport", "unavailable", message)
    }
}

/// `VerifyRejected` -> `verify_rejected`.
fn snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The snake_case name of Core's error code; `other` for unnamed ones.
pub fn code_name(code: RpcErrorCode) -> String {
    match code {
        RpcErrorCode::Other(_) => "other".into(),
        c => snake(&format!("{c:?}")),
    }
}

impl From<NodeAPIError> for ApiError {
    fn from(e: NodeAPIError) -> ApiError {
        use RpcErrorCode as C;
        match e {
            NodeAPIError::Rpc { code, message } => {
                let kind = match code {
                    C::InvalidAddressOrKey => Kind::NotFound,
                    C::VerifyRejected | C::VerifyError | C::VerifyAlreadyInChain => {
                        Kind::FailedPrecondition
                    }
                    C::InvalidParameter | C::Type | C::Deserialization => Kind::InvalidArgument,
                    C::InWarmup => Kind::Unavailable,
                    _ => Kind::Internal,
                };
                ApiError {
                    kind,
                    source: "rpc",
                    code: code_name(code),
                    core_code: Some(code.code()),
                    message,
                }
            }
            NodeAPIError::Transport(t) => match t {
                TransportError::Connect(_) => ApiError::new(
                    Kind::Unavailable,
                    "transport",
                    "connect",
                    "node unreachable",
                ),
                TransportError::Timeout => ApiError::new(
                    Kind::DeadlineExceeded,
                    "transport",
                    "timeout",
                    "node did not answer in time",
                ),
                // The gateway's own credentials, or Core's HTTP layer: the
                // operator's problem, and nothing the client should see.
                t => {
                    tracing::error!(error = %t, "upstream request failed");
                    ApiError::new(
                        Kind::Internal,
                        "transport",
                        "upstream",
                        "the gateway could not talk to the node",
                    )
                }
            },
            NodeAPIError::Unsupported(m) => {
                ApiError::new(Kind::Unimplemented, "unsupported", "unsupported", m)
            }
            NodeAPIError::Decode(e) => {
                tracing::error!(error = %e, "could not decode the node's reply");
                ApiError::new(
                    Kind::Internal,
                    "decode",
                    "decode",
                    "could not decode the node's reply",
                )
            }
            _ => ApiError::new(Kind::Internal, "internal", "internal", "internal error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rpc(code: i64) -> ApiError {
        NodeAPIError::Rpc {
            code: code.into(),
            message: "m".into(),
        }
        .into()
    }

    #[test]
    fn table() {
        let t = |e: ApiError| (e.kind, e.kind.http_status());
        assert_eq!(t(rpc(-5)), (Kind::NotFound, 404));
        for c in [-26, -25, -27] {
            assert_eq!(t(rpc(c)), (Kind::FailedPrecondition, 422));
        }
        for c in [-8, -3, -22] {
            assert_eq!(t(rpc(c)), (Kind::InvalidArgument, 400));
        }
        assert_eq!(t(rpc(-28)), (Kind::Unavailable, 503));
        assert_eq!(t(rpc(-1)), (Kind::Internal, 500));
        let tr = |e: TransportError| t(NodeAPIError::Transport(e).into());
        assert_eq!(
            tr(TransportError::Connect("x".into())),
            (Kind::Unavailable, 503)
        );
        assert_eq!(tr(TransportError::Timeout), (Kind::DeadlineExceeded, 504));
        assert_eq!(tr(TransportError::Auth), (Kind::Internal, 500));
        assert_eq!(
            t(NodeAPIError::Unsupported("x".into()).into()),
            (Kind::Unimplemented, 501)
        );
        assert_eq!(
            t(NodeAPIError::Decode("x".into()).into()),
            (Kind::Internal, 500)
        );
        assert_eq!(t(ApiError::unauthenticated()), (Kind::Unauthenticated, 401));
        assert_eq!(
            t(ApiError::permission_denied("x")),
            (Kind::PermissionDenied, 403)
        );
        assert_eq!(t(ApiError::rate_limited()), (Kind::ResourceExhausted, 429));
    }

    #[test]
    fn core_code_travels() {
        let e = rpc(-26);
        assert_eq!(e.code, "verify_rejected");
        assert_eq!(e.core_code, Some(-26));
        assert_eq!(rpc(-5).code, "invalid_address_or_key");
        assert_eq!(rpc(-99999).code, "other");
        assert_eq!(rpc(-99999).core_code, Some(-99999));
    }

    #[test]
    fn auth_failure_to_core_hides_detail() {
        let e: ApiError = NodeAPIError::Transport(TransportError::Auth).into();
        assert!(!e.message.contains("auth"));
    }
}
