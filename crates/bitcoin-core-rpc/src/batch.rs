//! Typed helpers over [`Client::batch`] for catching up on blocks.
use crate::{Client, hex};
use bitcoin::{Block, BlockHash};
use bitcoin_node_api::ApiResult;
use serde_json::json;
use std::ops::RangeInclusive;

impl Client {
    /// `getblockhash` for every height in `heights`, batched. Fails as a
    /// whole: a missing height means the range is past the tip.
    pub async fn get_block_hashes(
        &self,
        heights: RangeInclusive<u64>,
    ) -> ApiResult<Vec<BlockHash>> {
        let params = heights.map(|h| json!([h])).collect();
        self.batch("getblockhash", params)
            .await?
            .into_iter()
            .collect()
    }

    /// `getblock` (raw) for every hash, batched, in input order. One unknown
    /// hash is one inner `Err`; the rest still come back.
    pub async fn get_blocks(&self, hashes: &[BlockHash]) -> ApiResult<Vec<ApiResult<Block>>> {
        let params = hashes.iter().map(|h| json!([h, 0])).collect();
        let raw = self.batch::<String>("getblock", params).await?;
        Ok(raw.into_iter().map(|r| r.and_then(|s| hex(&s))).collect())
    }
}
