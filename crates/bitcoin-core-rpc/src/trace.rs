//! The `tracing` feature: a span per request, events for retries and cookie
//! re-reads. Only method names, ids, wallet names, timings and error kinds
//! are recorded, never params or results: they hold passphrases, private
//! descriptors and raw transactions.
use bitcoin_node_api::{NodeAPIError, TransportError};

/// A `tracing::debug!` event with the feature on; nothing without it.
macro_rules! trace_event {
    ($($t:tt)*) => {
        #[cfg(feature = "tracing")]
        tracing::debug!($($t)*);
    };
}

/// What went wrong, without anything from the request or reply: Core's
/// messages and decode errors can quote them.
#[cfg_attr(not(feature = "tracing"), allow(dead_code))]
pub(crate) fn kind(e: &NodeAPIError) -> String {
    match e {
        NodeAPIError::Rpc { code, .. } => format!("rpc {}", code.code()),
        NodeAPIError::Transport(t) => match t {
            TransportError::Connect(_) => "connect".into(),
            TransportError::Timeout => "timeout".into(),
            TransportError::Auth => "auth".into(),
            TransportError::Http(s) => format!("http {s}"),
            _ => "transport".into(),
        },
        NodeAPIError::Decode(_) => "decode".into(),
        _ => "unsupported".into(),
    }
}

/// Runs `fut` in `span`, then records its `duration_ms` and `error`.
#[cfg(feature = "tracing")]
pub(crate) async fn in_span<T>(
    span: tracing::Span,
    fut: impl Future<Output = bitcoin_node_api::ApiResult<T>>,
) -> bitcoin_node_api::ApiResult<T> {
    use tracing::Instrument;
    let start = std::time::Instant::now();
    let r = fut.instrument(span.clone()).await;
    span.record("duration_ms", start.elapsed().as_millis() as u64);
    if let Err(e) = &r {
        span.record("error", kind(e).as_str());
    }
    r
}
