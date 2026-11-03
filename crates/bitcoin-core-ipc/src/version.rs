//! Per-version calls. Core 30 and 31 number their methods differently, so each
//! has its own generated types; `shared!` writes what is the same in both.
use crate::ipc_err;
use bitcoin::block::Header;
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{Amount, Block, BlockHash, Transaction, TxMerkleNode};
use bitcoin_node_api::{ApiResult, NodeAPIError, Solution, Tip, WaitOptions};
use std::cell::RefCell;
use std::future::Future;

/// Core's `MAX_MONEY`: `fee_threshold` when the caller sets none.
const MAX_MONEY: i64 = 21_000_000 * 100_000_000;
/// Core's `DEFAULT_BLOCK_RESERVED_WEIGHT` and coinbase sigops allowance;
/// Core 30's schema has no defaults, so they're always sent.
const RESERVED_WEIGHT: u64 = 8000;
const COINBASE_SIGOPS: u64 = 400;

fn decode<T: bitcoin::consensus::Decodable>(what: &str, b: &[u8]) -> ApiResult<T> {
    deserialize(b).map_err(|e| NodeAPIError::Decode(format!("ipc {what}: {e}")))
}

/// Core's `BlockRef`: the hash in internal byte order, like `BlockHash`'s bytes.
fn block_ref(hash: &[u8], height: i32) -> ApiResult<Tip> {
    let bad = |e: String| NodeAPIError::Decode(format!("ipc tip: {e}"));
    Ok(Tip {
        height: u64::try_from(height).map_err(|_| bad(format!("height {height}")))?,
        hash: BlockHash::from_slice(hash).map_err(|e| bad(e.to_string()))?,
    })
}

macro_rules! shared {
    ($v:ident) => {
        use crate::generated::$v::{
            init_capnp::init,
            mining_capnp::{block_template, mining},
            proxy_capnp::{thread, thread_map},
        };

        pub(crate) type BlockTemplate = block_template::Client;

        pub(crate) struct Caps {
            thread_map: thread_map::Client,
            /// Core threads not running a call. Core 30 refuses a second call
            /// on a busy thread ("thread busy"), so each call leases its own.
            idle: RefCell<Vec<thread::Client>>,
            mining: mining::Client,
        }

        /// A Core thread for one call; back to the pool when dropped.
        struct Lease<'a> {
            caps: &'a Caps,
            thread: Option<thread::Client>,
        }

        impl Lease<'_> {
            fn client(&self) -> thread::Client {
                self.thread.clone().unwrap()
            }
        }

        impl Drop for Lease<'_> {
            fn drop(&mut self) {
                if let Some(t) = self.thread.take() {
                    self.caps.idle.borrow_mut().push(t);
                }
            }
        }

        impl Caps {
            pub(crate) async fn make(
                init: init::Client,
                thread_map: thread_map::Client,
                thread: thread::Client,
            ) -> capnp::Result<Caps> {
                let mut req = init.make_mining_request();
                req.get().init_context().set_thread(thread.clone());
                let mining = req.send().promise.await?.get()?.get_result()?;
                Ok(Caps {
                    thread_map,
                    idle: RefCell::new(vec![thread]),
                    mining,
                })
            }

            /// An idle Core thread, or a new one if all are busy.
            async fn lease(&self) -> ApiResult<Lease<'_>> {
                let idle = self.idle.borrow_mut().pop();
                let thread = match idle {
                    Some(t) => t,
                    None => {
                        let mut req = self.thread_map.make_thread_request();
                        req.get().set_name("bitcoin-core-ipc");
                        let resp = req.send().promise.await.map_err(ipc_err)?;
                        trace_event!("ipc: Core thread created (all busy)");
                        resp.get().and_then(|r| r.get_result()).map_err(ipc_err)?
                    }
                };
                Ok(Lease {
                    caps: self,
                    thread: Some(thread),
                })
            }

            pub(crate) async fn tip(&self) -> ApiResult<Option<Tip>> {
                let mut req = self.mining.get_tip_request();
                let th = self.lease().await?;
                req.get().init_context().set_thread(th.client());
                let resp = req.send().promise.await.map_err(ipc_err)?;
                let r = resp.get().map_err(ipc_err)?;
                if !r.get_has_result() {
                    return Ok(None);
                }
                let b = r.get_result().map_err(ipc_err)?;
                block_ref(b.get_hash().map_err(ipc_err)?, b.get_height()).map(Some)
            }

            /// Waits until the tip is not `current`, at most `timeout`, and
            /// returns the tip then (`current` again on timeout). Returns at
            /// once if the tip has already moved.
            pub(crate) async fn wait_tip_changed(
                &self,
                current: BlockHash,
                timeout: std::time::Duration,
            ) -> ApiResult<Tip> {
                let th = self.lease().await?;
                let mut req = self.mining.wait_tip_changed_request();
                let mut p = req.get();
                p.reborrow().init_context().set_thread(th.client());
                p.set_current_tip(current.as_byte_array());
                // Milliseconds; Core 30's schema has no default, so always set.
                p.set_timeout(timeout.as_secs_f64() * 1000.0);
                let resp = req.send().promise.await.map_err(ipc_err)?;
                let b = resp.get().and_then(|r| r.get_result()).map_err(ipc_err)?;
                block_ref(b.get_hash().map_err(ipc_err)?, b.get_height())
            }

            pub(crate) async fn header(&self, t: &BlockTemplate) -> ApiResult<Header> {
                let mut req = t.get_block_header_request();
                let th = self.lease().await?;
                req.get().init_context().set_thread(th.client());
                let resp = req.send().promise.await.map_err(ipc_err)?;
                decode(
                    "header",
                    resp.get().and_then(|r| r.get_result()).map_err(ipc_err)?,
                )
            }

            pub(crate) async fn merkle_path(
                &self,
                t: &BlockTemplate,
            ) -> ApiResult<Vec<TxMerkleNode>> {
                let mut req = t.get_coinbase_merkle_path_request();
                let th = self.lease().await?;
                req.get().init_context().set_thread(th.client());
                let resp = req.send().promise.await.map_err(ipc_err)?;
                let list = resp.get().and_then(|r| r.get_result()).map_err(ipc_err)?;
                list.iter()
                    .map(|n| {
                        let n = n.map_err(ipc_err)?;
                        TxMerkleNode::from_slice(n)
                            .map_err(|e| NodeAPIError::Decode(format!("ipc merkle path: {e}")))
                    })
                    .collect()
            }

            pub(crate) async fn block(&self, t: &BlockTemplate) -> ApiResult<Block> {
                let mut req = t.get_block_request();
                let th = self.lease().await?;
                req.get().init_context().set_thread(th.client());
                let resp = req.send().promise.await.map_err(ipc_err)?;
                decode(
                    "block",
                    resp.get().and_then(|r| r.get_result()).map_err(ipc_err)?,
                )
            }

            /// Per non-coinbase tx: fee, sigops cost.
            pub(crate) async fn fees_sigops(
                &self,
                t: &BlockTemplate,
            ) -> ApiResult<(Vec<i64>, Vec<i64>)> {
                let mut fees = t.get_tx_fees_request();
                let th = self.lease().await?;
                fees.get().init_context().set_thread(th.client());
                let mut sigops = t.get_tx_sigops_request();
                let th = self.lease().await?;
                sigops.get().init_context().set_thread(th.client());
                let (fees, sigops) = tokio::try_join!(fees.send().promise, sigops.send().promise)
                    .map_err(ipc_err)?;
                let fees = fees.get().and_then(|r| r.get_result()).map_err(ipc_err)?;
                let sigops = sigops.get().and_then(|r| r.get_result()).map_err(ipc_err)?;
                Ok((fees.iter().collect(), sigops.iter().collect()))
            }

            /// `Ok(false)`: Core rejected the block.
            pub(crate) async fn submit(
                &self,
                t: &BlockTemplate,
                sol: &Solution,
            ) -> ApiResult<bool> {
                let mut req = t.submit_solution_request();
                let mut p = req.get();
                let th = self.lease().await?;
                p.reborrow().init_context().set_thread(th.client());
                p.set_version(sol.version.to_consensus() as u32);
                p.set_timestamp(sol.time);
                p.set_nonce(sol.nonce);
                p.set_coinbase(&serialize(&sol.coinbase));
                let resp = req.send().promise.await.map_err(ipc_err)?;
                Ok(resp.get().map_err(ipc_err)?.get_result())
            }

            /// `None` on timeout.
            pub(crate) async fn wait_next(
                &self,
                t: &BlockTemplate,
                opts: WaitOptions,
            ) -> ApiResult<Option<BlockTemplate>> {
                let th = self.lease().await?;
                let mut req = t.wait_next_request();
                let mut p = req.get();
                p.reborrow().init_context().set_thread(th.client());
                // Both fields always: Core 30's schema defaults them to 0.
                let mut o = p.init_options();
                o.set_timeout(opts.timeout.map_or(f64::MAX, |d| d.as_secs_f64() * 1000.0));
                o.set_fee_threshold(opts.fee_threshold.map_or(MAX_MONEY, |a| a.to_sat() as i64));
                let resp = req.send().promise.await.map_err(ipc_err)?;
                let r = resp.get().map_err(ipc_err)?;
                if !r.has_result() {
                    return Ok(None);
                }
                r.get_result().map(Some).map_err(ipc_err)
            }
        }
    };
}

pub(crate) mod v30 {
    use super::*;
    use crate::Parts;
    use bitcoin::{ScriptBuf, TxOut};
    shared!(v30);

    impl Caps {
        /// Pipelined: the template can be called at once; `done` reports
        /// whether `createNewBlock` itself succeeded.
        pub(crate) fn create(&self) -> (BlockTemplate, impl Future<Output = ApiResult<()>>) {
            let mut req = self.mining.create_new_block_request();
            let mut o = req.get().init_options();
            o.set_use_mempool(true);
            o.set_block_reserved_weight(RESERVED_WEIGHT);
            o.set_coinbase_output_max_additional_sigops(COINBASE_SIGOPS);
            let sent = req.send();
            let t = sent.pipeline.get_result();
            (
                t,
                async move { sent.promise.await.map(drop).map_err(ipc_err) },
            )
        }

        /// Core 30 sends a whole placeholder coinbase plus the witness
        /// commitment script; reshape them into Core 31's `CoinbaseTx`.
        pub(crate) async fn coinbase(&self, t: &BlockTemplate) -> ApiResult<Parts> {
            let mut tx = t.get_coinbase_tx_request();
            let th = self.lease().await?;
            tx.get().init_context().set_thread(th.client());
            let mut commitment = t.get_coinbase_commitment_request();
            let th = self.lease().await?;
            commitment.get().init_context().set_thread(th.client());
            let (tx, commitment) =
                tokio::try_join!(tx.send().promise, commitment.send().promise).map_err(ipc_err)?;
            let tx: Transaction = decode(
                "coinbase",
                tx.get().and_then(|r| r.get_result()).map_err(ipc_err)?,
            )?;
            let commitment = commitment
                .get()
                .and_then(|r| r.get_result())
                .map_err(ipc_err)?;
            let required: Vec<TxOut> = if commitment.is_empty() {
                vec![]
            } else {
                vec![TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::from_bytes(commitment.to_vec()),
                }]
            };
            Parts::from_placeholder(tx, required)
        }
    }
}

pub(crate) mod v31 {
    use super::*;
    use crate::Parts;
    use bitcoin::{ScriptBuf, Sequence, TxOut, absolute, transaction};
    shared!(v31);

    impl Caps {
        /// Pipelined: the template can be called at once; `done` reports
        /// whether `createNewBlock` itself succeeded.
        pub(crate) async fn create(
            &self,
        ) -> ApiResult<(BlockTemplate, impl Future<Output = ApiResult<()>> + '_)> {
            let th = self.lease().await?;
            let mut req = self.mining.create_new_block_request();
            let mut p = req.get();
            p.reborrow().init_context().set_thread(th.client());
            let mut o = p.init_options();
            o.set_use_mempool(true);
            o.set_block_reserved_weight(RESERVED_WEIGHT);
            o.set_coinbase_output_max_additional_sigops(COINBASE_SIGOPS);
            let sent = req.send();
            let t = sent.pipeline.get_result();
            let done = async move {
                let _th = th;
                sent.promise.await.map(drop).map_err(ipc_err)
            };
            Ok((t, done))
        }

        pub(crate) async fn coinbase(&self, t: &BlockTemplate) -> ApiResult<Parts> {
            let mut req = t.get_coinbase_tx_request();
            let th = self.lease().await?;
            req.get().init_context().set_thread(th.client());
            let resp = req.send().promise.await.map_err(ipc_err)?;
            let c = resp.get().and_then(|r| r.get_result()).map_err(ipc_err)?;
            let witness = c.get_witness().map_err(ipc_err)?;
            let witness = match witness.len() {
                0 => None,
                32 => Some(witness.try_into().unwrap()),
                n => {
                    return Err(NodeAPIError::Decode(format!(
                        "ipc coinbase witness: {n} bytes"
                    )));
                }
            };
            let required = c
                .get_required_outputs()
                .map_err(ipc_err)?
                .iter()
                .map(|o| decode::<TxOut>("required output", o.map_err(ipc_err)?))
                .collect::<ApiResult<Vec<_>>>()?;
            let reward = c.get_block_reward_remaining();
            Parts::new(bitcoin_node_api::Coinbase {
                version: transaction::Version(c.get_version() as i32),
                sequence: Sequence(c.get_sequence()),
                script_sig_prefix: ScriptBuf::from_bytes(
                    c.get_script_sig_prefix().map_err(ipc_err)?.to_vec(),
                ),
                witness,
                reward_remaining: Amount::from_sat(
                    u64::try_from(reward).map_err(|_| {
                        NodeAPIError::Decode(format!("ipc coinbase reward: {reward}"))
                    })?,
                ),
                required_outputs: required,
                lock_time: absolute::LockTime::from_consensus(c.get_lock_time()),
            })
        }
    }
}
