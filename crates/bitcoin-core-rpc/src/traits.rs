//! `bitcoin-node-api` traits on top of JSON-RPC.
use crate::{Client, Feature, hex};
use bitcoin::amount::serde::as_btc;
use bitcoin::bip158::BlockFilter;
use bitcoin::block::Header;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::hashes::{Hash, HashEngine};
use bitcoin::script::Builder;
use bitcoin::{
    Amount, Block, BlockHash, CompactTarget, FeeRate, OutPoint, ScriptBuf, Sequence, Transaction,
    TxMerkleNode, TxOut, Txid, Weight, Wtxid, absolute, block, transaction,
};
use bitcoin_node_api::*;
use futures_core::Stream;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// Lookups map Core's -5 ("not found") to `Ok(None)`.
fn not_found_is_none<T>(r: ApiResult<T>) -> ApiResult<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(NodeAPIError::Rpc {
            code: RpcErrorCode::InvalidAddressOrKey,
            ..
        }) => Ok(None),
        Err(e) => Err(e),
    }
}

fn hexes(txs: &[Transaction]) -> Vec<String> {
    txs.iter().map(serialize_hex).collect()
}

impl ChainSource for Client {
    async fn tip(&self) -> ApiResult<Tip> {
        #[derive(Deserialize)]
        struct Info {
            blocks: u64,
            bestblockhash: BlockHash,
        }
        let i: Info = self.call("getblockchaininfo", json!([])).await?;
        Ok(Tip {
            height: i.blocks,
            hash: i.bestblockhash,
        })
    }

    async fn block_hash(&self, height: u64) -> ApiResult<BlockHash> {
        self.get_block_hash(height).await
    }

    async fn header(&self, hash: BlockHash) -> ApiResult<Header> {
        self.get_block_header(hash).await
    }

    async fn header_info(&self, hash: BlockHash) -> ApiResult<HeaderInfo> {
        let v = self.get_block_header_verbose(hash).await?;
        let header = Header {
            version: v.version,
            prev_blockhash: v.previous_block_hash.unwrap_or_else(BlockHash::all_zeros),
            merkle_root: v.merkle_root,
            time: v.time,
            bits: v.bits,
            nonce: v.nonce,
        };
        // One call instead of two; the hash proves the fields were read right.
        if header.block_hash() != hash {
            return Err(NodeAPIError::Decode(format!(
                "getblockheader {hash}: fields hash to {}",
                header.block_hash()
            )));
        }
        Ok(HeaderInfo {
            header,
            height: v.height.into(),
            chain_work: v.chain_work,
        })
    }

    async fn block_filter(&self, hash: BlockHash) -> ApiResult<BlockFilter> {
        match self.get_block_filter(hash, None).await {
            Ok(f) => Ok(BlockFilter::new(&f.filter)),
            // "Index is not enabled for filtertype basic"
            Err(NodeAPIError::Rpc {
                code: RpcErrorCode::Misc,
                message,
            }) if message.contains("not enabled") => Err(NodeAPIError::Unsupported(format!(
                "getblockfilter: start Core with -blockfilterindex ({message})"
            ))),
            Err(e) => Err(e),
        }
    }

    async fn block(&self, hash: BlockHash) -> ApiResult<Block> {
        self.get_block(hash).await
    }

    async fn transaction(&self, txid: Txid, block: Option<BlockHash>) -> ApiResult<Transaction> {
        self.get_raw_transaction(txid, block).await
    }

    async fn utxo(&self, outpoint: OutPoint, include_mempool: bool) -> ApiResult<Option<Utxo>> {
        #[derive(Deserialize)]
        struct TxOut {
            confirmations: u32,
            #[serde(with = "as_btc")]
            value: Amount,
            #[serde(rename = "scriptPubKey")]
            script_pubkey: Spk,
            coinbase: bool,
        }
        #[derive(Deserialize)]
        struct Spk {
            hex: ScriptBuf,
        }
        // Spent -> `null` result; unknown txid -> -5 on some versions.
        let params = json!([outpoint.txid, outpoint.vout, include_mempool]);
        let out = not_found_is_none(self.call::<Option<TxOut>>("gettxout", params).await)?;
        Ok(out.flatten().map(|o| Utxo {
            value: o.value,
            script_pubkey: o.script_pubkey.hex,
            confirmations: o.confirmations,
            coinbase: o.coinbase,
        }))
    }
}

impl MempoolSource for Client {
    async fn txids(&self) -> ApiResult<Vec<Txid>> {
        self.call("getrawmempool", json!([false])).await
    }

    /// `getrawmempool` with `mempool_sequence`: the sequence ZMQ events carry.
    async fn snapshot(&self) -> ApiResult<MempoolSnapshot>
    where
        Self: Sync,
    {
        let m = self.get_raw_mempool_sequence().await?;
        Ok(MempoolSnapshot {
            txids: m.txids,
            sequence: Some(m.mempool_sequence),
        })
    }

    async fn entry(&self, txid: Txid) -> ApiResult<Option<MempoolEntry>> {
        #[derive(Deserialize)]
        struct Entry {
            vsize: u32,
            weight: u64,
            time: u64,
            height: u64,
            ancestorcount: u32,
            descendantcount: u32,
            fees: Fees,
        }
        #[derive(Deserialize)]
        struct Fees {
            #[serde(with = "as_btc")]
            base: Amount,
        }
        let e = not_found_is_none(self.call::<Entry>("getmempoolentry", json!([txid])).await)?;
        Ok(e.map(|e| MempoolEntry {
            vsize: e.vsize,
            weight: Weight::from_wu(e.weight),
            fee: e.fees.base,
            time: e.time,
            height: e.height,
            ancestors: e.ancestorcount,
            descendants: e.descendantcount,
        }))
    }
}

impl Broadcaster for Client {
    async fn broadcast(&self, tx: &Transaction) -> ApiResult<Txid> {
        self.send_raw_transaction(tx, None).await
    }

    async fn broadcast_package(&self, txs: &[Transaction]) -> ApiResult<Vec<PackageResult>> {
        #[derive(Deserialize)]
        struct Package {
            package_msg: String,
            #[serde(rename = "tx-results")]
            tx_results: HashMap<Wtxid, TxResult>,
        }
        #[derive(Deserialize)]
        struct TxResult {
            #[serde(rename = "other-wtxid")]
            other_wtxid: Option<Wtxid>,
            fees: Option<Fees>,
            error: Option<String>,
        }
        #[derive(Deserialize)]
        struct Fees {
            #[serde(with = "as_btc")]
            base: Amount,
            /// Absent when the tx was already in the mempool.
            #[serde(rename = "effective-feerate")]
            effective_feerate: Option<f64>,
        }
        if !self.supports(Feature::SubmitPackage) {
            return Err(NodeAPIError::Unsupported(
                "submitpackage needs Core 28+".into(),
            ));
        }
        let p: Package = self.call("submitpackage", json!([hexes(txs)])).await?;
        Ok(txs
            .iter()
            .map(|tx| {
                let outcome = match p.tx_results.get(&tx.compute_wtxid()) {
                    // Core stops at the first failure and leaves later txs out.
                    None => PackageOutcome::Rejected(p.package_msg.clone()),
                    Some(TxResult { error: Some(e), .. }) => PackageOutcome::Rejected(e.clone()),
                    Some(TxResult {
                        other_wtxid: Some(_),
                        ..
                    }) => PackageOutcome::AlreadyInMempool,
                    Some(TxResult {
                        fees:
                            Some(Fees {
                                base,
                                effective_feerate: Some(_),
                            }),
                        ..
                    }) => PackageOutcome::Accepted { fee: *base },
                    Some(_) => PackageOutcome::AlreadyInMempool,
                };
                PackageResult {
                    txid: tx.compute_txid(),
                    outcome,
                }
            })
            .collect())
    }

    async fn test_accept(&self, txs: &[Transaction]) -> ApiResult<Vec<MempoolAccept>> {
        #[derive(Deserialize)]
        struct Accept {
            txid: Txid,
            // Missing when a package-wide error stopped validation.
            #[serde(default)]
            allowed: bool,
            #[serde(rename = "reject-reason")]
            reject_reason: Option<String>,
            #[serde(rename = "package-error")]
            package_error: Option<String>,
        }
        let r: Vec<Accept> = self.call("testmempoolaccept", json!([hexes(txs)])).await?;
        Ok(r.into_iter()
            .map(|a| MempoolAccept {
                txid: a.txid,
                allowed: a.allowed,
                reject_reason: a.reject_reason.or(a.package_error),
            })
            .collect())
    }
}

impl FeeEstimator for Client {
    async fn estimate_fee(&self, target: FeeTarget) -> ApiResult<Option<FeeRate>> {
        #[derive(Deserialize)]
        struct Estimate {
            /// BTC/kvB; absent with `errors: [...]` when Core has no estimate.
            #[serde(default, with = "as_btc::opt")]
            feerate: Option<Amount>,
        }
        let mode = match target.mode {
            FeeMode::Conservative => "CONSERVATIVE",
            FeeMode::Economical => "ECONOMICAL",
        };
        let e: Estimate = self
            .call("estimatesmartfee", json!([target.blocks, mode]))
            .await?;
        // sat/kvB -> sat/kwu, rounded up so we never under-pay.
        Ok(e.feerate
            .map(|per_kvb| FeeRate::from_sat_per_kwu(per_kvb.to_sat().div_ceil(4))))
    }
}

#[derive(Deserialize)]
struct Gbt {
    version: i32,
    previousblockhash: BlockHash,
    transactions: Vec<GbtTx>,
    coinbasevalue: u64,
    longpollid: String,
    mintime: u32,
    curtime: u32,
    bits: String,
    height: u64,
    default_witness_commitment: Option<ScriptBuf>,
}

#[derive(Deserialize)]
struct GbtTx {
    data: String,
    txid: Txid,
    fee: u64,
    sigops: u32,
}

/// The RPC `TemplateToken`: the longpoll id plus the GBT txs, still hex.
struct RpcToken {
    longpollid: String,
    txs: Vec<GbtTx>,
}

fn rpc_token(tmpl: &Template) -> ApiResult<&RpcToken> {
    tmpl.token
        .downcast_ref()
        .ok_or_else(|| NodeAPIError::Unsupported("template is from another backend".into()))
}

impl Gbt {
    fn into_template(self) -> ApiResult<Template> {
        let bits = u32::from_str_radix(&self.bits, 16)
            .map_err(|e| NodeAPIError::Decode(format!("bits {:?}: {e}", self.bits)))?;
        let commitment = self.default_witness_commitment;
        Ok(Template {
            prev_hash: self.previousblockhash,
            height: self.height,
            version: block::Version::from_consensus(self.version),
            bits: CompactTarget::from_consensus(bits),
            min_time: self.mintime,
            cur_time: self.curtime,
            coinbase: Coinbase {
                version: transaction::Version::TWO,
                sequence: Sequence::MAX,
                script_sig_prefix: Builder::new().push_int(self.height as i64).into_script(),
                witness: commitment.is_some().then_some([0; 32]),
                reward_remaining: Amount::from_sat(self.coinbasevalue),
                required_outputs: commitment
                    .into_iter()
                    .map(|script_pubkey| TxOut {
                        value: Amount::ZERO,
                        script_pubkey,
                    })
                    .collect(),
                lock_time: absolute::LockTime::ZERO,
            },
            coinbase_merkle_path: coinbase_merkle_path(self.transactions.iter().map(|t| t.txid)),
            token: TemplateToken::new(RpcToken {
                longpollid: self.longpollid,
                txs: self.transactions,
            }),
        })
    }
}

/// Siblings of the coinbase (index 0) from leaf to root, given the other txids in block order.
fn coinbase_merkle_path(txids: impl Iterator<Item = Txid>) -> Vec<TxMerkleNode> {
    let mut level: Vec<TxMerkleNode> = txids
        .map(|t| TxMerkleNode::from_raw_hash(t.to_raw_hash()))
        .collect();
    let mut path = Vec::new();
    while let Some(&sibling) = level.first() {
        path.push(sibling);
        // The coinbase side pairs with `sibling`; the rest pair among themselves, odd one doubled.
        level = level[1..]
            .chunks(2)
            .map(|c| {
                let mut e = TxMerkleNode::engine();
                e.input(c[0].as_byte_array());
                e.input(c[c.len() - 1].as_byte_array());
                TxMerkleNode::from_engine(e)
            })
            .collect();
    }
    path
}

impl Client {
    async fn gbt(&self, longpollid: Option<&str>) -> ApiResult<Template> {
        let gbt: Gbt = match longpollid {
            None => {
                self.call("getblocktemplate", json!([{ "rules": ["segwit"] }]))
                    .await?
            }
            Some(id) => {
                let params = json!([{ "rules": ["segwit"], "longpollid": id }]);
                self.call_long("getblocktemplate", params).await?
            }
        };
        gbt.into_template()
    }
}

impl TemplateSource for Client {
    async fn template(&self) -> ApiResult<Template> {
        self.gbt(None).await
    }

    async fn transactions(&self, tmpl: &Template) -> ApiResult<Vec<TemplateTx>> {
        rpc_token(tmpl)?
            .txs
            .iter()
            .map(|t| {
                Ok(TemplateTx {
                    tx: hex(&t.data)?,
                    fee: Amount::from_sat(t.fee),
                    sigops: t.sigops,
                })
            })
            .collect()
    }

    /// Core keeps a longpoll's rpcthread busy until the tip or mempool changes,
    /// even if we hang up. So at most one request per `longpollid` is in flight;
    /// callers share it through a `watch`, and a caller's timeout never cancels it.
    /// `fee_threshold` is ignored: GBT has no equivalent.
    async fn wait_next(&self, prev: &Template, opts: WaitOptions) -> ApiResult<Option<Template>> {
        let id = rpc_token(prev)?.longpollid.clone();
        let mut rx = {
            let mut polls = self.longpolls.lock().unwrap();
            polls
                .entry(id.clone())
                .or_insert_with(|| {
                    let (tx, rx) = watch::channel(None);
                    let client = self.clone();
                    tokio::spawn(async move {
                        let res = client.gbt(Some(&id)).await;
                        // Publish, then forget: late callers with this id still get the result.
                        let _ = tx.send(Some(res));
                        client.longpolls.lock().unwrap().remove(&id);
                    });
                    rx
                })
                .clone()
        };
        let wait = rx.wait_for(Option::is_some);
        let done = match opts.timeout {
            Some(t) => match tokio::time::timeout(t, wait).await {
                Ok(done) => done,
                Err(_) => return Ok(None),
            },
            None => wait.await,
        };
        match done {
            Ok(res) => res.clone().expect("waited for Some").map(Some),
            Err(_) => {
                Err(TransportError::Other("longpoll task ended without a result".into()).into())
            }
        }
    }

    async fn submit_solution(&self, tmpl: &Template, sol: &Solution) -> ApiResult<()> {
        let mut txdata = vec![sol.coinbase.clone()];
        txdata.extend(self.transactions(tmpl).await?.into_iter().map(|t| t.tx));
        let block = Block {
            header: tmpl.header(sol),
            txdata,
        };
        match self.submit_block(&block).await?.as_deref() {
            // "inconclusive": stored but not (yet) on the best chain. Not a rejection.
            None | Some("duplicate" | "inconclusive" | "duplicate-inconclusive") => Ok(()),
            Some(reason) => Err(NodeAPIError::Rpc {
                code: RpcErrorCode::VerifyError,
                message: reason.into(),
            }),
        }
    }
}

/// How long one `waitfornewblock` blocks. Each subscriber holds one of Core's
/// `rpcthreads` for good; raise `rpcthreads` or use `bitcoin-core-zmq`.
const POLL: Duration = Duration::from_secs(10);

/// Block events from polling `waitfornewblock`. No tx events.
#[derive(Debug)]
pub struct BlockEvents(mpsc::Receiver<ApiResult<NodeEvent>>);

impl Stream for BlockEvents {
    type Item = ApiResult<NodeEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

impl EventSource for Client {
    type EventStream = BlockEvents;

    async fn subscribe(&self) -> ApiResult<BlockEvents> {
        #[derive(Deserialize)]
        struct NewBlock {
            hash: BlockHash,
            height: u64,
        }
        let mut last = self.tip().await?;
        let (tx, rx) = mpsc::channel(64);
        let client = self.clone();
        tokio::spawn(async move {
            while !tx.is_closed() {
                let params = json!([POLL.as_millis() as u64]);
                let ev = match client.call::<NewBlock>("waitfornewblock", params).await {
                    Ok(b) if b.hash == last.hash => continue,
                    // A direct child of the last tip is one connect; anything else
                    // (reorg, missed blocks) is a gap the consumer must resync.
                    Ok(b) => {
                        let child = b.height == last.height + 1
                            && client.header(b.hash).await.map(|h| h.prev_blockhash)
                                == Ok(last.hash);
                        last = Tip {
                            height: b.height,
                            hash: b.hash,
                        };
                        Ok(if child {
                            NodeEvent::BlockConnected(b.hash)
                        } else {
                            NodeEvent::Lagged
                        })
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                if tx.send(ev).await.is_err() {
                    break;
                }
            }
        });
        Ok(BlockEvents(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Network;
    use bitcoin::blockdata::constants::genesis_block;

    #[test]
    fn merkle_path_matches_block_root() {
        let cb = genesis_block(Network::Regtest).txdata[0].clone();
        for n in 0..9u32 {
            // Distinct txs: vary lock time.
            let txs: Vec<Transaction> = (0..n)
                .map(|i| Transaction {
                    lock_time: absolute::LockTime::from_consensus(i + 1),
                    ..cb.clone()
                })
                .collect();
            let path = coinbase_merkle_path(txs.iter().map(Transaction::compute_txid));
            let mut root = TxMerkleNode::from_raw_hash(cb.compute_txid().to_raw_hash());
            for sib in &path {
                let mut e = TxMerkleNode::engine();
                e.input(root.as_byte_array());
                e.input(sib.as_byte_array());
                root = TxMerkleNode::from_engine(e);
            }
            let block = Block {
                header: genesis_block(Network::Regtest).header,
                txdata: std::iter::once(cb.clone()).chain(txs).collect(),
            };
            assert_eq!(Some(root), block.compute_merkle_root(), "n = {n}");
        }
    }
}
