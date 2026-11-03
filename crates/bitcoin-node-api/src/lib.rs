//! Capability traits for talking to a Bitcoin node
//!
//! Feature `serde`: `Serialize`/`Deserialize` on the api types (`Template` is
//! `Serialize` only). The JSON shape is v1, frozen: the gateway's REST and
//! WebSocket clients see it, so changing it is a breaking change. See
//! DESIGN.md §3.5.
#![warn(missing_docs)]
pub mod chain;
pub mod error;
pub mod event;
pub mod mempool;
pub mod mining;
pub mod tx;
#[cfg(feature = "serde")]
mod wire;

pub use bitcoin;
pub use chain::{ChainSource, HeaderInfo, Tip, Utxo};
pub use error::{NodeAPIError, RpcErrorCode, TransportError};
pub use event::{EventSource, NodeEvent};
pub use mempool::{MempoolEntry, MempoolSnapshot, MempoolSource};
pub use mining::{
    Coinbase, Solution, Template, TemplateSource, TemplateToken, TemplateTx, WaitOptions,
};
pub use tx::{
    Broadcaster, FeeEstimator, FeeMode, FeeTarget, MempoolAccept, PackageOutcome, PackageResult,
};

/// What every trait method returns.
pub type ApiResult<T> = core::result::Result<T, NodeAPIError>;

/// Trait alias so trait methods read `-> impl Fut<T>`.
pub trait Fut<T>: Future<Output = ApiResult<T>> + Send {}
impl<T, F> Fut<T> for F where F: Future<Output = ApiResult<T>> + Send {}
