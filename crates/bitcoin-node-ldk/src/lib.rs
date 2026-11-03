//! LDK's [`lightning_block_sync::BlockSource`] over any
//! [`bitcoin_node_api::ChainSource`].
//!
//! ```no_run
//! # async fn run(node: impl bitcoin_node_api::ChainSource + Send + Sync) {
//! use bitcoin::Network;
//! use bitcoin_node_ldk::ChainBlockSource;
//! use lightning_block_sync::poll::ChainPoller;
//! use lightning_block_sync::{SpvClient, UnboundedCache, init};
//!
//! # struct L;
//! # impl lightning::chain::Listen for L {
//! #     fn filtered_block_connected(&self, _: &bitcoin::block::Header,
//! #         _: &lightning::chain::transaction::TransactionData, _: u32) {}
//! #     fn blocks_disconnected(&self, _: lightning::chain::BestBlock) {}
//! # }
//! # let listener = L;
//! let source = ChainBlockSource::new(node);
//! let mut cache = UnboundedCache::new();
//! let tip = init::validate_best_block_header(&source).await.unwrap();
//! let poller = ChainPoller::new(&source, Network::Bitcoin);
//! let mut spv = SpvClient::new(tip, poller, &mut cache, &listener);
//! spv.poll_best_tip().await.unwrap();
//! # }
//! ```
//!
//! # Height and chainwork
//!
//! `BlockHeaderData` wants a height and the total chainwork. Both come from
//! [`ChainSource::header_info`], which Core answers from `getblockheader`:
//! the node's own numbers, exact on every network, testnet3/4's
//! min-difficulty blocks included. LDK's height hint isn't needed.
//!
//! # Errors
//!
//! See [`block_source_error`] for how a [`NodeAPIError`] becomes a
//! [`BlockSourceError`]; the original error is its `into_inner()`.
#![warn(missing_docs)]

use bitcoin::BlockHash;
use bitcoin_node_api::{ChainSource, NodeAPIError, RpcErrorCode};
use lightning_block_sync::{
    AsyncBlockSourceResult, BlockData, BlockHeaderData, BlockSource, BlockSourceError,
};

pub use lightning_block_sync;

/// A [`BlockSource`] backed by a [`ChainSource`]: Core over RPC, the mock, or
/// any other backend with headers.
#[derive(Debug, Clone)]
pub struct ChainBlockSource<C> {
    chain: C,
}

impl<C> ChainBlockSource<C> {
    /// Wraps `chain`.
    pub fn new(chain: C) -> Self {
        ChainBlockSource { chain }
    }

    /// The wrapped source.
    pub fn inner(&self) -> &C {
        &self.chain
    }

    /// Unwraps the source.
    pub fn into_inner(self) -> C {
        self.chain
    }
}

impl<C: ChainSource + Send + Sync> BlockSource for ChainBlockSource<C> {
    /// `header_info`: the header with the node's height and chainwork.
    fn get_header<'a>(
        &'a self,
        header_hash: &'a BlockHash,
        _height_hint: Option<u32>,
    ) -> AsyncBlockSourceResult<'a, BlockHeaderData> {
        Box::pin(async move {
            let i = self
                .chain
                .header_info(*header_hash)
                .await
                .map_err(block_source_error)?;
            Ok(BlockHeaderData {
                header: i.header,
                height: u32::try_from(i.height).map_err(BlockSourceError::persistent)?,
                chainwork: i.chain_work,
            })
        })
    }

    /// `block`: always a full block.
    fn get_block<'a>(
        &'a self,
        header_hash: &'a BlockHash,
    ) -> AsyncBlockSourceResult<'a, BlockData> {
        Box::pin(async move {
            self.chain
                .block(*header_hash)
                .await
                .map(BlockData::FullBlock)
                .map_err(block_source_error)
        })
    }

    /// `tip`, with its height.
    fn get_best_block(&self) -> AsyncBlockSourceResult<'_, (BlockHash, Option<u32>)> {
        Box::pin(async move {
            let tip = self.chain.tip().await.map_err(block_source_error)?;
            let height = u32::try_from(tip.height).map_err(BlockSourceError::persistent)?;
            Ok((tip.hash, Some(height)))
        })
    }
}

/// Maps a trait error to LDK's two kinds. Transient: LDK may poll again;
/// persistent: it won't.
///
/// | `NodeAPIError`                          | kind       | why                                         |
/// | --------------------------------------- | ---------- | ------------------------------------------- |
/// | `Transport(_)`                          | transient  | node down, restarting, slow                 |
/// | `Rpc` `InvalidAddressOrKey` (-5)        | persistent | unknown block: asking again won't help      |
/// | `Rpc`, any other code                   | transient  | warmup, IBD, ...: as LDK's own `RpcClient`  |
/// | `Decode(_)`                             | persistent | the reply won't parse next time either      |
/// | `Unsupported(_)`                        | persistent | the backend can't, ever                     |
///
/// The original error is kept: `into_inner()` downcasts to `NodeAPIError`.
pub fn block_source_error(e: NodeAPIError) -> BlockSourceError {
    match e {
        NodeAPIError::Rpc {
            code: RpcErrorCode::InvalidAddressOrKey,
            ..
        }
        | NodeAPIError::Decode(_)
        | NodeAPIError::Unsupported(_) => BlockSourceError::persistent(e),
        _ => BlockSourceError::transient(e),
    }
}
