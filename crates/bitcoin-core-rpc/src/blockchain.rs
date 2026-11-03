//! `== Blockchain ==`
use crate::client::IntoApi;
use crate::{Client, Feature, core, hex};
use bitcoin::block::Header;
use bitcoin::{Block, BlockHash, MerkleBlock, OutPoint, Txid};
use bitcoin_node_api::{ApiResult, NodeAPIError};
use corepc_types::model;
use serde_json::json;
use std::time::Duration;

fn millis(t: Option<Duration>) -> u64 {
    t.map_or(0, |t| t.as_millis() as u64)
}

impl Client {
    /// `getbestblockhash`: hash of the tip of the most-work chain.
    pub async fn get_best_block_hash(&self) -> ApiResult<BlockHash> {
        self.call("getbestblockhash", json!([])).await
    }

    /// `getblock` verbosity 0: the full block.
    pub async fn get_block(&self, hash: BlockHash) -> ApiResult<Block> {
        hex(&self.call::<String>("getblock", json!([hash, 0])).await?)
    }

    /// `getblock` verbosity 1: header fields plus txids.
    pub async fn get_block_verbose_one(
        &self,
        hash: BlockHash,
    ) -> ApiResult<model::GetBlockVerboseOne> {
        versioned!(self, "getblock", json!([hash, 1]), GetBlockVerboseOne)
    }

    /// `getblock` verbosity 2: header fields plus decoded txs and their fees.
    pub async fn get_block_verbose_two(
        &self,
        hash: BlockHash,
    ) -> ApiResult<model::GetBlockVerboseTwo> {
        versioned!(self, "getblock", json!([hash, 2]), GetBlockVerboseTwo)
    }

    /// `getblock` verbosity 3: as verbosity 2, plus each input's prevout.
    pub async fn get_block_verbose_three(
        &self,
        hash: BlockHash,
    ) -> ApiResult<model::GetBlockVerboseThree> {
        versioned!(self, "getblock", json!([hash, 3]), GetBlockVerboseThree)
    }

    /// `getblockchaininfo`: chain, tip, sync progress and pruning state.
    pub async fn get_blockchain_info(&self) -> ApiResult<model::GetBlockchainInfo> {
        versioned!(self, "getblockchaininfo", json!([]), GetBlockchainInfo)
    }

    /// `getblockcount`: height of the most-work fully validated chain.
    pub async fn get_block_count(&self) -> ApiResult<u64> {
        self.call("getblockcount", json!([])).await
    }

    /// `filter_type` defaults to `"basic"`.
    pub async fn get_block_filter(
        &self,
        hash: BlockHash,
        filter_type: Option<&str>,
    ) -> ApiResult<model::GetBlockFilter> {
        let params = json!([hash, filter_type.unwrap_or("basic")]);
        versioned!(self, "getblockfilter", params, GetBlockFilter)
    }

    /// `getblockfrompeer`: asks peer `peer_id` (from `getpeerinfo`) for a block whose
    /// header we have. Returns once the request is sent.
    pub async fn get_block_from_peer(&self, hash: BlockHash, peer_id: u64) -> ApiResult<()> {
        self.call("getblockfrompeer", json!([hash, peer_id])).await
    }

    /// `getblockhash`: hash of the block at `height` in the best chain.
    pub async fn get_block_hash(&self, height: u64) -> ApiResult<BlockHash> {
        self.call("getblockhash", json!([height])).await
    }

    /// `getblockheader` non-verbose: the 80-byte header.
    pub async fn get_block_header(&self, hash: BlockHash) -> ApiResult<Header> {
        hex(&self
            .call::<String>("getblockheader", json!([hash, false]))
            .await?)
    }

    /// `getblockheader` verbose: header fields plus height, confirmations, chainwork, ...
    pub async fn get_block_header_verbose(
        &self,
        hash: BlockHash,
    ) -> ApiResult<model::GetBlockHeaderVerbose> {
        versioned!(
            self,
            "getblockheader",
            json!([hash, true]),
            GetBlockHeaderVerbose
        )
    }

    /// `getblockstats`: per-block statistics (fees, sizes, feerates, ...) for `hash`.
    pub async fn get_block_stats(&self, hash: BlockHash) -> ApiResult<model::GetBlockStats> {
        versioned!(self, "getblockstats", json!([hash]), GetBlockStats)
    }

    /// Only the named `stats` (`"avgfeerate"`, `"feerate_percentiles"`, ...);
    /// the rest of the result is `None`.
    pub async fn get_block_stats_filtered(
        &self,
        hash: BlockHash,
        stats: &[&str],
    ) -> ApiResult<model::GetBlockStats> {
        versioned!(self, "getblockstats", json!([hash, stats]), GetBlockStats)
    }

    /// `getblockstats` for the block at `height` in the best chain.
    pub async fn get_block_stats_by_height(&self, height: u64) -> ApiResult<model::GetBlockStats> {
        versioned!(self, "getblockstats", json!([height]), GetBlockStats)
    }

    /// `getchainstates`: the chainstates; two while an assumeutxo snapshot is
    /// validated in the background.
    pub async fn get_chain_states(&self) -> ApiResult<model::GetChainStates> {
        versioned!(self, "getchainstates", json!([]), GetChainStates)
    }

    /// `getchaintips`: every known tip in the block tree, including forks.
    pub async fn get_chain_tips(&self) -> ApiResult<model::GetChainTips> {
        versioned!(self, "getchaintips", json!([]), GetChainTips)
    }

    /// `n_blocks` defaults to one month; `hash` to the tip.
    pub async fn get_chain_tx_stats(
        &self,
        n_blocks: Option<u32>,
        hash: Option<BlockHash>,
    ) -> ApiResult<model::GetChainTxStats> {
        versioned!(
            self,
            "getchaintxstats",
            json!([n_blocks, hash]),
            GetChainTxStats
        )
    }

    /// `getdeploymentinfo`: soft fork deployment states at `hash` (default: the tip).
    pub async fn get_deployment_info(
        &self,
        hash: Option<BlockHash>,
    ) -> ApiResult<model::GetDeploymentInfo> {
        let params = match hash {
            Some(h) => json!([h]),
            None => json!([]),
        };
        versioned!(self, "getdeploymentinfo", params, GetDeploymentInfo)
    }

    /// Core 29+. `scan_objects` are output descriptors. A spend's `spend_vout`
    /// is the input index (Core's `spend_vin`).
    pub async fn get_descriptor_activity(
        &self,
        hashes: &[BlockHash],
        scan_objects: &[&str],
        include_mempool: bool,
    ) -> ApiResult<model::GetDescriptorActivity> {
        let params = json!([hashes, scan_objects, include_mempool]);
        let mut r: serde_json::Value = self.call("getdescriptoractivity", params).await?;
        // corepc 0.16 reads `spend_vout`, which Core has never sent. Its v29 struct
        // is the only one (Core 29–31 share it).
        for e in r["activity"].as_array_mut().into_iter().flatten() {
            if let Some(vin) = e.as_object_mut().and_then(|o| o.remove("spend_vin")) {
                e["spend_vout"] = vin;
            }
        }
        let r: corepc_types::v29::GetDescriptorActivity =
            serde_json::from_value(r).into_api("getdescriptoractivity")?;
        r.into_model().into_api("getdescriptoractivity")
    }

    /// `getdifficulty`: proof-of-work difficulty as a multiple of the minimum.
    pub async fn get_difficulty(&self) -> ApiResult<f64> {
        self.call("getdifficulty", json!([])).await
    }

    /// `getmempoolancestors`: txids of `txid`'s in-mempool ancestors.
    pub async fn get_mempool_ancestors(&self, txid: Txid) -> ApiResult<model::GetMempoolAncestors> {
        versioned!(
            self,
            "getmempoolancestors",
            json!([txid, false]),
            GetMempoolAncestors
        )
    }

    /// `getmempoolancestors` verbose: a mempool entry per ancestor.
    pub async fn get_mempool_ancestors_verbose(
        &self,
        txid: Txid,
    ) -> ApiResult<model::GetMempoolAncestorsVerbose> {
        let params = json!([txid, true]);
        versioned!(
            self,
            "getmempoolancestors",
            params,
            GetMempoolAncestorsVerbose
        )
    }

    /// Core 31+ (cluster mempool).
    pub async fn get_mempool_cluster(&self, txid: Txid) -> ApiResult<model::GetMempoolCluster> {
        if !self.supports(Feature::ClusterMempool) {
            return Err(NodeAPIError::Unsupported(
                "getmempoolcluster needs Core 31+".into(),
            ));
        }
        self.call::<corepc_types::v31::GetMempoolCluster>("getmempoolcluster", json!([txid]))
            .await?
            .into_model()
            .into_api("getmempoolcluster")
    }

    /// `getmempooldescendants`: txids of `txid`'s in-mempool descendants.
    pub async fn get_mempool_descendants(
        &self,
        txid: Txid,
    ) -> ApiResult<model::GetMempoolDescendants> {
        let params = json!([txid, false]);
        versioned!(self, "getmempooldescendants", params, GetMempoolDescendants)
    }

    /// `getmempooldescendants` verbose: a mempool entry per descendant.
    pub async fn get_mempool_descendants_verbose(
        &self,
        txid: Txid,
    ) -> ApiResult<model::GetMempoolDescendantsVerbose> {
        let params = json!([txid, true]);
        versioned!(
            self,
            "getmempooldescendants",
            params,
            GetMempoolDescendantsVerbose
        )
    }

    /// `getmempoolentry`: fees, size, ancestry and timing of an in-mempool tx.
    pub async fn get_mempool_entry(&self, txid: Txid) -> ApiResult<model::GetMempoolEntry> {
        versioned!(self, "getmempoolentry", json!([txid]), GetMempoolEntry)
    }

    /// `getmempoolinfo`: mempool size, memory usage, minimum fee rates and policy.
    pub async fn get_mempool_info(&self) -> ApiResult<model::GetMempoolInfo> {
        versioned!(self, "getmempoolinfo", json!([]), GetMempoolInfo)
    }

    /// `getrawmempool`: txids in the mempool.
    pub async fn get_raw_mempool(&self) -> ApiResult<model::GetRawMempool> {
        versioned!(self, "getrawmempool", json!([false]), GetRawMempool)
    }

    /// Txids plus the mempool sequence number, to line up with ZMQ `sequence`.
    pub async fn get_raw_mempool_sequence(&self) -> ApiResult<model::GetRawMempoolSequence> {
        let params = json!([false, true]);
        versioned!(self, "getrawmempool", params, GetRawMempoolSequence)
    }

    /// `getrawmempool` verbose: a mempool entry per tx.
    pub async fn get_raw_mempool_verbose(&self) -> ApiResult<model::GetRawMempoolVerbose> {
        versioned!(self, "getrawmempool", json!([true]), GetRawMempoolVerbose)
    }

    /// `None` if spent or unknown.
    pub async fn get_tx_out(
        &self,
        outpoint: OutPoint,
        include_mempool: bool,
    ) -> ApiResult<Option<model::GetTxOut>> {
        let params = json!([outpoint.txid, outpoint.vout, include_mempool]);
        // `null` when spent, so not `versioned!`; the struct is v17's in all of 29–31.
        self.call::<Option<corepc_types::v31::GetTxOut>>("gettxout", params)
            .await?
            .map(|o| o.into_model())
            .transpose()
            .into_api("gettxout")
    }

    /// Merkle proof that `txids` are in a block. `hash` is needed without `-txindex`.
    pub async fn get_tx_out_proof(
        &self,
        txids: &[Txid],
        hash: Option<BlockHash>,
    ) -> ApiResult<MerkleBlock> {
        let params = match hash {
            Some(h) => json!([txids, h]),
            None => json!([txids]),
        };
        hex(&self.call::<String>("gettxoutproof", params).await?)
    }

    /// `gettxoutsetinfo`: UTXO set statistics, with the default `hash_serialized_3` hash.
    pub async fn get_tx_out_set_info(&self) -> ApiResult<model::GetTxOutSetInfo> {
        versioned!(self, "gettxoutsetinfo", json!([]), GetTxOutSetInfo)
    }

    /// `hash_type` is `"hash_serialized_3"` (the default), `"muhash"` (needs
    /// `-coinstatsindex`) or `"none"` (fastest).
    pub async fn get_tx_out_set_info_hash_type(
        &self,
        hash_type: &str,
    ) -> ApiResult<model::GetTxOutSetInfo> {
        let params = json!([hash_type]);
        versioned!(self, "gettxoutsetinfo", params, GetTxOutSetInfo)
    }

    /// `gettxspendingprevout`: the mempool tx spending each of `outpoints`, if any.
    pub async fn get_tx_spending_prevout(
        &self,
        outpoints: &[OutPoint],
    ) -> ApiResult<model::GetTxSpendingPrevout> {
        let outputs: Vec<_> = outpoints
            .iter()
            .map(|o| json!({ "txid": o.txid, "vout": o.vout }))
            .collect();
        versioned!(
            self,
            "gettxspendingprevout",
            json!([outputs]),
            GetTxSpendingPrevout
        )
    }

    /// `path` is on the node's filesystem.
    pub async fn import_mempool(&self, path: &str) -> ApiResult<()> {
        // Replies `{}`.
        self.call::<serde_json::Value>("importmempool", json!([path]))
            .await
            .map(drop)
    }

    /// Writes a snapshot of the current UTXO set to `path` on the node's filesystem.
    pub async fn dump_tx_out_set(&self, path: &str) -> ApiResult<model::DumpTxOutSet> {
        versioned!(self, "dumptxoutset", json!([path, "latest"]), DumpTxOutSet)
    }

    /// `path` is on the node's filesystem.
    pub async fn load_tx_out_set(&self, path: &str) -> ApiResult<model::LoadTxOutSet> {
        versioned!(self, "loadtxoutset", json!([path]), LoadTxOutSet)
    }

    /// `preciousblock`: treats `hash` as if it was received before others of the same work.
    pub async fn precious_block(&self, hash: BlockHash) -> ApiResult<()> {
        self.call("preciousblock", json!([hash])).await
    }

    /// Returns the height of the last pruned block.
    pub async fn prune_blockchain(&self, height: u64) -> ApiResult<u64> {
        self.call("pruneblockchain", json!([height])).await
    }

    /// `savemempool`: writes the mempool to `mempool.dat` in the data directory.
    pub async fn save_mempool(&self) -> ApiResult<core::SaveMempool> {
        self.call("savemempool", json!([])).await
    }

    /// `scan_objects` are output descriptors. Blocks until the scan is done.
    pub async fn scan_blocks_start(
        &self,
        scan_objects: &[&str],
        start_height: Option<u64>,
        stop_height: Option<u64>,
    ) -> ApiResult<model::ScanBlocksStart> {
        let params = json!(["start", scan_objects, start_height, stop_height]);
        versioned!(self, "scanblocks", params, ScanBlocksStart)
    }

    /// `None` if no scan is running.
    pub async fn scan_blocks_status(&self) -> ApiResult<Option<core::ScanBlocksStatus>> {
        self.call("scanblocks", json!(["status"])).await
    }

    /// `false` if no scan was running.
    pub async fn scan_blocks_abort(&self) -> ApiResult<bool> {
        self.call("scanblocks", json!(["abort"])).await
    }

    /// `descriptors` are output descriptors. Blocks until the scan is done.
    pub async fn scan_tx_out_set_start(
        &self,
        descriptors: &[&str],
    ) -> ApiResult<model::ScanTxOutSetStart> {
        versioned!(
            self,
            "scantxoutset",
            json!(["start", descriptors]),
            ScanTxOutSetStart
        )
    }

    /// `None` if no scan is running.
    pub async fn scan_tx_out_set_status(&self) -> ApiResult<Option<core::ScanTxOutSetStatus>> {
        self.call("scantxoutset", json!(["status"])).await
    }

    /// `false` if no scan was running.
    pub async fn scan_tx_out_set_abort(&self) -> ApiResult<bool> {
        self.call("scantxoutset", json!(["abort"])).await
    }

    /// Defaults: check level 3, last 6 blocks.
    pub async fn verify_chain(
        &self,
        check_level: Option<u32>,
        n_blocks: Option<u32>,
    ) -> ApiResult<bool> {
        self.call("verifychain", json!([check_level, n_blocks]))
            .await
    }

    /// Txids the proof commits to; empty if the proof is for a block not in the best chain.
    pub async fn verify_tx_out_proof(&self, proof: &MerkleBlock) -> ApiResult<Vec<Txid>> {
        let params = json!([bitcoin::consensus::encode::serialize_hex(proof)]);
        let r: model::VerifyTxOutProof =
            versioned!(self, "verifytxoutproof", params, VerifyTxOutProof)?;
        Ok(r.0)
    }

    /// Waits for `hash` to become the tip. `None` timeout waits forever (up to the 10-minute request limit).
    pub async fn wait_for_block(
        &self,
        hash: BlockHash,
        timeout: Option<Duration>,
    ) -> ApiResult<model::WaitForBlock> {
        versioned!(
            self,
            "waitforblock",
            json!([hash, millis(timeout)]),
            WaitForBlock
        )
    }

    /// `waitforblockheight`: waits until the tip is at least `height`. `None` timeout
    /// waits forever (up to the 10-minute request limit).
    pub async fn wait_for_block_height(
        &self,
        height: u64,
        timeout: Option<Duration>,
    ) -> ApiResult<model::WaitForBlockHeight> {
        let params = json!([height, millis(timeout)]);
        versioned!(self, "waitforblockheight", params, WaitForBlockHeight)
    }

    /// `waitfornewblock`: waits for the tip to change. `None` timeout waits forever
    /// (up to the 10-minute request limit).
    pub async fn wait_for_new_block(
        &self,
        timeout: Option<Duration>,
    ) -> ApiResult<model::WaitForNewBlock> {
        versioned!(
            self,
            "waitfornewblock",
            json!([millis(timeout)]),
            WaitForNewBlock
        )
    }
}
