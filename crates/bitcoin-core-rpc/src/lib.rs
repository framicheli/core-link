//! Async JSON-RPC backend for Bitcoin Core
//!
//! # Wallets
//!
//! Wallet RPCs are methods on a [`Wallet`], which posts to the wallet's own
//! endpoint (`/wallet/<name>`). Descriptor wallets only. On regtest:
//!
//! ```no_run
//! # async fn example() -> bitcoin_node_api::ApiResult<()> {
//! use bitcoin::{Amount, FeeRate};
//! use bitcoin_core_rpc::{
//!     Auth, Client, CreateWalletOptions, FundOptions, ImportDescriptor, SendOptions, Timestamp,
//! };
//!
//! let auth = Auth::UserPass("user".into(), "pass".into());
//! let client = Client::connect("http://127.0.0.1:18443", auth).await?;
//! let hot = client.create_wallet("hot", CreateWalletOptions::default()).await?.wallet;
//! let mine = hot.get_new_address(None, None).await?.assume_checked();
//! client.generate_to_address(101, &mine).await?; // one mature coinbase
//!
//! // Send 1 BTC at 2 sat/vB.
//! let to = hot.get_new_address(None, None).await?.assume_checked();
//! let fee = FundOptions::default().fee_rate(FeeRate::from_sat_per_vb_u32(2));
//! hot.send(&[(to.clone(), Amount::ONE_BTC)], None, SendOptions::default().fund(fee))
//!     .await?;
//!
//! // PSBT flow: a watch-only wallet with hot's public descriptors builds the
//! // transaction, the hot wallet signs it.
//! let opts = CreateWalletOptions::default().disable_private_keys(true);
//! let watch = client.create_wallet("watch", opts).await?.wallet;
//! let public = hot.list_descriptors(false).await?;
//! let requests: Vec<_> = public
//!     .descriptors
//!     .iter()
//!     .map(|d| {
//!         let r = ImportDescriptor::new(&d.descriptor, Timestamp::Time(0))
//!             .active(d.active)
//!             .internal(d.internal.unwrap_or(false));
//!         match d.range {
//!             Some([start, end]) => r.range(start as u32, end as u32),
//!             None => r,
//!         }
//!     })
//!     .collect();
//! watch.import_descriptors(&requests).await?;
//! let out = [(to, Amount::ONE_BTC)];
//! let funded = watch
//!     .wallet_create_funded_psbt(&[], &out, None, None, FundOptions::default(), None, None)
//!     .await?;
//! let signed = hot.wallet_process_psbt(&funded.psbt, None, None, None, None).await?;
//! if let Some(tx) = signed.hex {
//!     client.send_raw_transaction(&tx, None).await?;
//! }
//! # Ok(())
//! # }
//! ```
#![warn(missing_docs)]
#[macro_use]
mod trace;
#[macro_use]
mod client;
mod auth;
mod batch;
mod blockchain;
mod control;
pub mod core;
mod methods;
mod mining;
mod network;
pub(crate) mod raw_transactions;
mod regtest;
mod signer;
mod traits;
mod util;
mod version;
mod wallet;
mod zmq;

pub use auth::Auth;
pub use client::{Builder, Client};
pub use corepc_types::model;
pub use methods::{MethodClass, RetryPolicy, classified_methods, method_class};
pub use network::{AddNodeCommand, SetBanCommand};
pub use raw_transactions::RawTransactionWithPrevouts;
pub use traits::BlockEvents;
pub use version::{Feature, Version};
pub use wallet::{
    AddressType, BumpFeeOptions, CreateWalletOptions, FundOptions, ImportDescriptor,
    ListUnspentQuery, Loaded, LockAction, SendAllOptions, SendManyOptions, SendOptions,
    SendToAddressOptions, SolvingData, Timestamp, Wallet,
};

use bitcoin::consensus::encode::{Decodable, deserialize_hex};
use bitcoin_node_api::{ApiResult, NodeAPIError};

/// Decodes a consensus-encoded hex reply.
pub(crate) fn hex<T: Decodable>(s: &str) -> ApiResult<T> {
    deserialize_hex(s).map_err(|e| NodeAPIError::Decode(e.to_string()))
}
