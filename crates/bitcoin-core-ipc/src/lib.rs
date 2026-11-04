//! Bitcoin Core's mining interface over IPC (Cap'n Proto), Core 30 and 31.
//!
//! Start Core's multiprocess node with a socket, e.g.
//! `bitcoin -m node -ipcbind=unix` (or `libexec/bitcoin-node -ipcbind=unix`),
//! which listens on `<datadir>/<chain>/node.sock`. Core 29 has no IPC.
//!
//! ```no_run
//! # async fn f() -> bitcoin_node_api::ApiResult<()> {
//! use bitcoin_core_ipc::IpcClient;
//! use bitcoin_node_api::{TemplateSource, WaitOptions};
//!
//! let ipc = IpcClient::connect("/home/me/.bitcoin/regtest/node.sock").await?;
//! let tmpl = ipc.template().await?;
//! let next = ipc.wait_next(&tmpl, WaitOptions::default()).await?;
//! # Ok(()) }
//! ```
//!
//! capnp-rpc isn't `Send`, so one worker thread owns the connection and runs
//! every call; `IpcClient` is a cheap `Send + Sync` handle to it. Calls run
//! concurrently on the worker, and each `wait_next` gets its own Core thread,
//! so a `submit_solution` never queues behind a pending wait.
#![warn(missing_docs)]
/// A `tracing::debug!` event with the `tracing` feature on; nothing without
/// it. Only sockets, versions and thread creation: never template contents.
macro_rules! trace_event {
    ($($t:tt)*) => {
        #[cfg(feature = "tracing")]
        tracing::debug!($($t)*);
    };
}

mod generated;
mod version;

use bitcoin::block::Header;
use bitcoin::{Amount, Block, BlockHash, OutPoint, Script, ScriptBuf, Transaction, TxOut, Txid};
use bitcoin_node_api::{
    ApiResult, ChainSource, Coinbase, EventSource, HeaderInfo, NodeAPIError, NodeEvent,
    RpcErrorCode, Solution, Template, TemplateSource, TemplateToken, TemplateTx, Tip,
    TransportError, Utxo, WaitOptions,
};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use version::{v30, v31};

/// Handle to the IPC worker. Clones share one connection.
#[derive(Debug, Clone)]
pub struct IpcClient {
    tx: mpsc::UnboundedSender<Msg>,
    core_major: u32,
}

impl IpcClient {
    /// Connects to Core's `-ipcbind=unix` socket and picks the Core 30 or 31
    /// protocol by asking for the Core 31 mining interface first.
    pub async fn connect(socket: impl AsRef<Path>) -> ApiResult<IpcClient> {
        let socket = socket.as_ref().to_owned();
        let (tx, rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let weak = tx.downgrade();
        std::thread::Builder::new()
            .name("bitcoin-core-ipc".into())
            .spawn(move || worker(socket, rx, weak, ready_tx))
            .map_err(|e| TransportError::Other(format!("ipc worker: {e}")))?;
        let core_major = ready_rx
            .await
            .map_err(|_| TransportError::Other("ipc worker exited".into()))??;
        Ok(IpcClient { tx, core_major })
    }

    /// 30 or 31: the protocol in use.
    pub fn core_major(&self) -> u32 {
        self.core_major
    }

    /// Runs `f` on the worker and waits for its result.
    async fn run<T, F, Fut>(&self, f: F) -> ApiResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Rc<Conn>) -> Fut + Send + 'static,
        Fut: Future<Output = ApiResult<T>> + 'static,
    {
        let (mut done, result) = oneshot::channel();
        let job: Job = Box::new(move |conn| {
            Box::pin(async move {
                // The caller dropping its future drops the call with it:
                // the Core thread it leased is never pooled mid-call, and
                // nothing is described or registered for no one.
                let mut out = None;
                tokio::select! {
                    r = f(conn) => out = Some(r),
                    _ = done.closed() => {}
                }
                if let Some(r) = out {
                    let _ = done.send(r);
                }
            })
        });
        self.tx.send(Msg::Job(job)).map_err(|_| gone())?;
        result.await.map_err(|_| gone())?
    }

    /// The worker-side id of `tmpl`, if it came from this connection.
    fn template_id(&self, tmpl: &Template) -> ApiResult<u64> {
        match tmpl.token.downcast_ref::<Handle>() {
            Some(h) if h.tx.same_channel(&self.tx) => Ok(h.id),
            _ => Err(NodeAPIError::Unsupported(
                "template is not from this IpcClient".into(),
            )),
        }
    }
}

fn gone() -> NodeAPIError {
    TransportError::Other("ipc connection closed".into()).into()
}

pub(crate) fn ipc_err(e: capnp::Error) -> NodeAPIError {
    match e.kind {
        capnp::ErrorKind::Disconnected => TransportError::Other(format!("ipc: {e}")).into(),
        _ => NodeAPIError::Rpc {
            code: RpcErrorCode::from(-1),
            message: format!("ipc: {e}"),
        },
    }
}

type Job = Box<dyn FnOnce(Rc<Conn>) -> Pin<Box<dyn Future<Output = ()>>> + Send>;

enum Msg {
    Job(Job),
    /// The last clone of a template's token was dropped.
    Release(u64),
}

impl std::fmt::Debug for Msg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Msg")
    }
}

/// Inside a `TemplateToken`: frees the worker's template when dropped.
struct Handle {
    id: u64,
    tx: mpsc::UnboundedSender<Msg>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Release(self.id));
    }
}

/// The worker thread: one current-thread runtime and `LocalSet` for capnp-rpc.
fn worker(
    socket: PathBuf,
    mut rx: mpsc::UnboundedReceiver<Msg>,
    weak: mpsc::WeakUnboundedSender<Msg>,
    ready: oneshot::Sender<ApiResult<u32>>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = ready.send(Err(TransportError::Other(e.to_string()).into()));
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let conn = match Conn::open(&socket, weak).await {
            Ok(c) => Rc::new(c),
            Err(e) => {
                let _ = ready.send(Err(e));
                return;
            }
        };
        let _ = ready.send(Ok(conn.core_major()));
        // Ends when every `IpcClient` and template token is gone.
        while let Some(msg) = rx.recv().await {
            match msg {
                Msg::Job(job) => {
                    tokio::task::spawn_local(job(conn.clone()));
                }
                Msg::Release(id) => {
                    conn.templates.borrow_mut().remove(&id);
                }
            }
        }
    });
}

enum Caps {
    V30(v30::Caps),
    V31(v31::Caps),
}

#[derive(Clone)]
enum Tmpl {
    V30(v30::BlockTemplate),
    V31(v31::BlockTemplate),
}

/// Template data besides the header, the same for both versions.
pub(crate) struct Parts {
    coinbase: Coinbase,
    height: u64,
}

impl Parts {
    pub(crate) fn new(coinbase: Coinbase) -> ApiResult<Parts> {
        let height = bip34_height(&coinbase.script_sig_prefix)?;
        Ok(Parts { coinbase, height })
    }

    /// From Core 30's placeholder coinbase: its scriptSig starts with the
    /// height push, and all of the reward sits in non-`required` outputs.
    pub(crate) fn from_placeholder(tx: Transaction, required: Vec<TxOut>) -> ApiResult<Parts> {
        let bad = |what: &str| NodeAPIError::Decode(format!("ipc coinbase: {what}"));
        let input = tx.input.first().ok_or_else(|| bad("no input"))?;
        let script = &input.script_sig;
        // Where the second instruction starts. Not `.nth(1)`: rust-bitcoin
        // 0.32's `InstructionIndices::nth` reports the index before skipping.
        let mut ins = script.instruction_indices();
        ins.next();
        let end = ins
            .next()
            .and_then(Result::ok)
            .map_or(script.len(), |(i, _)| i);
        let witness = match input.witness.len() {
            0 => None,
            1 => Some(
                input.witness[0]
                    .try_into()
                    .map_err(|_| bad("witness is not 32 bytes"))?,
            ),
            _ => return Err(bad("witness has several items")),
        };
        let reward = tx
            .output
            .iter()
            .filter(|o| !required.contains(o))
            .map(|o| o.value)
            .sum();
        Parts::new(Coinbase {
            version: tx.version,
            sequence: input.sequence,
            script_sig_prefix: ScriptBuf::from_bytes(script.as_bytes()[..end].to_vec()),
            witness,
            reward_remaining: reward,
            required_outputs: required,
            lock_time: tx.lock_time,
        })
    }
}

/// The BIP 34 height: the first push of the coinbase scriptSig.
fn bip34_height(prefix: &Script) -> ApiResult<u64> {
    let bad = || NodeAPIError::Decode(format!("ipc coinbase: no BIP34 height in {prefix}"));
    // `script_num` covers both `OP_1`..`OP_16` and pushed numbers.
    let height = match prefix.instructions().next() {
        Some(Ok(i)) => i.script_num().ok_or_else(bad)?,
        _ => return Err(bad()),
    };
    u64::try_from(height).map_err(|_| bad())
}

struct Conn {
    caps: Caps,
    templates: RefCell<HashMap<u64, Tmpl>>,
    next_id: Cell<u64>,
    /// Weak: the worker stops when only it is left.
    tx: mpsc::WeakUnboundedSender<Msg>,
}

/// Runs `$body` with `$c` bound to the version's `Caps`.
macro_rules! on_caps {
    ($conn:expr, |$c:ident| $body:expr) => {
        match &$conn.caps {
            Caps::V30($c) => $body,
            Caps::V31($c) => $body,
        }
    };
}

/// Runs `$body` with the version's `Caps` and the template `$id`.
macro_rules! on_template {
    ($conn:expr, $id:expr, |$c:ident, $t:ident| $body:expr) => {{
        let t = $conn.templates.borrow().get(&$id).cloned();
        match (&$conn.caps, t) {
            (Caps::V30($c), Some(Tmpl::V30($t))) => $body,
            (Caps::V31($c), Some(Tmpl::V31($t))) => $body,
            _ => Err(NodeAPIError::Unsupported("template was released".into())),
        }
    }};
}

impl Conn {
    async fn open(socket: &Path, tx: mpsc::WeakUnboundedSender<Msg>) -> ApiResult<Conn> {
        let stream = tokio::net::UnixStream::connect(socket)
            .await
            .map_err(|e| TransportError::Connect(format!("{}: {e}", socket.display())))?;
        trace_event!(socket = %socket.display(), "ipc connected");
        let (r, w) = stream.into_split();
        let net = twoparty::VatNetwork::new(
            r.compat(),
            w.compat_write(),
            Side::Client,
            Default::default(),
        );
        let mut rpc = RpcSystem::new(Box::new(net), None);
        // `construct` and `makeThread` are numbered alike in 30 and 31.
        let init: generated::v31::init_capnp::init::Client = rpc.bootstrap(Side::Server);
        tokio::task::spawn_local(rpc);
        let thread_map = init
            .construct_request()
            .send()
            .promise
            .await
            .and_then(|r| r.get()?.get_thread_map())
            .map_err(ipc_err)?;
        let mut req = thread_map.make_thread_request();
        req.get().set_name("bitcoin-core-ipc");
        let thread = req
            .send()
            .promise
            .await
            .and_then(|r| r.get()?.get_result())
            .map_err(ipc_err)?;
        trace_event!("ipc: Core thread created");

        let caps = match v31::Caps::make(init.clone(), thread_map.clone(), thread.clone()).await {
            Ok(c) => Caps::V31(c),
            // Core 30 has no method @3 on `Init`; its `makeMining` is @2.
            Err(e) if e.kind == capnp::ErrorKind::Unimplemented => {
                use capnp::capability::FromClientHook;
                let c = v30::Caps::make(
                    FromClientHook::new(init.client.hook),
                    FromClientHook::new(thread_map.client.hook),
                    FromClientHook::new(thread.client.hook),
                )
                .await
                .map_err(ipc_err)?;
                Caps::V30(c)
            }
            Err(e) => return Err(ipc_err(e)),
        };
        trace_event!(
            protocol = match caps {
                Caps::V30(_) => 30,
                Caps::V31(_) => 31,
            },
            "ipc protocol picked"
        );
        Ok(Conn {
            caps,
            templates: RefCell::default(),
            next_id: Cell::new(0),
            tx,
        })
    }

    fn core_major(&self) -> u32 {
        match self.caps {
            Caps::V30(_) => 30,
            Caps::V31(_) => 31,
        }
    }

    /// Fetches what a `Template` needs (one round trip) and keeps `t` alive
    /// until the token's last clone is dropped.
    async fn describe(&self, t: Tmpl) -> ApiResult<Template> {
        let (header, parts, path): (Header, Parts, _) = match (&self.caps, &t) {
            (Caps::V30(c), Tmpl::V30(t)) => {
                tokio::try_join!(c.header(t), c.coinbase(t), c.merkle_path(t))?
            }
            (Caps::V31(c), Tmpl::V31(t)) => {
                tokio::try_join!(c.header(t), c.coinbase(t), c.merkle_path(t))?
            }
            _ => unreachable!("one version per connection"),
        };
        let tx = self.tx.upgrade().ok_or_else(gone)?;
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        self.templates.borrow_mut().insert(id, t);
        Ok(Template {
            prev_hash: header.prev_blockhash,
            height: parts.height,
            version: header.version,
            bits: header.bits,
            // IPC sends no minimum; the header's time is always valid.
            min_time: header.time,
            cur_time: header.time,
            coinbase: parts.coinbase,
            coinbase_merkle_path: path,
            token: TemplateToken::new(Handle { id, tx }),
        })
    }
}

impl TemplateSource for IpcClient {
    async fn template(&self) -> ApiResult<Template> {
        // One round trip: the template's calls ride on `createNewBlock`'s
        // promised result instead of waiting for it.
        self.run(|conn| async move {
            match &conn.caps {
                Caps::V30(c) => {
                    let (t, done) = c.create();
                    let (_, tmpl) = tokio::try_join!(done, conn.describe(Tmpl::V30(t)))?;
                    Ok(tmpl)
                }
                Caps::V31(c) => {
                    let (t, done) = c.create().await?;
                    let (_, tmpl) = tokio::try_join!(done, conn.describe(Tmpl::V31(t)))?;
                    Ok(tmpl)
                }
            }
        })
        .await
    }

    async fn transactions(&self, tmpl: &Template) -> ApiResult<Vec<TemplateTx>> {
        let id = self.template_id(tmpl)?;
        self.run(move |conn| async move {
            let (block, (fees, sigops)): (Block, _) = on_template!(conn, id, |c, t| {
                tokio::try_join!(c.block(&t), c.fees_sigops(&t))
            })?;
            let txs = block.txdata.len().saturating_sub(1);
            if fees.len() != txs || sigops.len() != txs {
                return Err(NodeAPIError::Decode(format!(
                    "ipc: {txs} txs but {} fees and {} sigops",
                    fees.len(),
                    sigops.len()
                )));
            }
            block
                .txdata
                .into_iter()
                .skip(1)
                .zip(fees.into_iter().zip(sigops))
                .map(|(tx, (fee, sigops))| {
                    Ok(TemplateTx {
                        tx,
                        fee: Amount::from_sat(u64::try_from(fee).map_err(|_| {
                            NodeAPIError::Decode(format!("ipc: negative fee {fee}"))
                        })?),
                        sigops: u32::try_from(sigops)
                            .map_err(|_| NodeAPIError::Decode(format!("ipc: sigops {sigops}")))?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn wait_next(&self, prev: &Template, opts: WaitOptions) -> ApiResult<Option<Template>> {
        let id = self.template_id(prev)?;
        self.run(move |conn| async move {
            let next = on_template!(conn, id, |c, t| {
                c.wait_next(&t, opts).await.map(|n| n.map(Tmpl::from))
            })?;
            match next {
                Some(t) => conn.describe(t).await.map(Some),
                None => Ok(None),
            }
        })
        .await
    }

    async fn submit_solution(&self, tmpl: &Template, sol: &Solution) -> ApiResult<()> {
        let id = self.template_id(tmpl)?;
        let sol = sol.clone();
        let accepted = self
            .run(move |conn| async move { on_template!(conn, id, |c, t| c.submit(&t, &sol).await) })
            .await?;
        if accepted {
            Ok(())
        } else {
            // IPC gives no reason, unlike `submitblock`.
            Err(NodeAPIError::Rpc {
                code: RpcErrorCode::VerifyError,
                message: "submitSolution: block rejected".into(),
            })
        }
    }
}

impl From<v30::BlockTemplate> for Tmpl {
    fn from(t: v30::BlockTemplate) -> Tmpl {
        Tmpl::V30(t)
    }
}

impl From<v31::BlockTemplate> for Tmpl {
    fn from(t: v31::BlockTemplate) -> Tmpl {
        Tmpl::V31(t)
    }
}

fn unsupported<T>(what: &str) -> ApiResult<T> {
    Err(NodeAPIError::Unsupported(format!(
        "{what}: Core's IPC mining interface has no such call; use bitcoin-core-rpc"
    )))
}

/// Only `tip` exists over IPC.
impl ChainSource for IpcClient {
    async fn tip(&self) -> ApiResult<Tip> {
        let tip = self
            .run(|conn| async move { on_caps!(conn, |c| c.tip().await) })
            .await?;
        tip.ok_or_else(|| NodeAPIError::Rpc {
            code: RpcErrorCode::InWarmup,
            message: "ipc: no tip yet".into(),
        })
    }

    async fn block_hash(&self, _: u64) -> ApiResult<BlockHash> {
        unsupported("block_hash")
    }

    async fn header(&self, _: BlockHash) -> ApiResult<Header> {
        unsupported("header")
    }

    async fn header_info(&self, _: BlockHash) -> ApiResult<HeaderInfo> {
        unsupported("header_info")
    }

    async fn block_filter(&self, _: BlockHash) -> ApiResult<bitcoin::bip158::BlockFilter> {
        unsupported("block_filter")
    }

    async fn block(&self, _: BlockHash) -> ApiResult<Block> {
        unsupported("block")
    }

    async fn transaction(&self, _: Txid, _: Option<BlockHash>) -> ApiResult<Transaction> {
        unsupported("transaction")
    }

    async fn utxo(&self, _: OutPoint, _: bool) -> ApiResult<Option<Utxo>> {
        unsupported("utxo")
    }
}

/// How long one `waitTipChanged` blocks. Bounds how long a dropped stream
/// keeps its Core thread.
const TIP_WAIT: Duration = Duration::from_secs(10);

/// Tip changes from `waitTipChanged`. No tx events, no disconnects.
#[derive(Debug)]
pub struct TipEvents(mpsc::Receiver<ApiResult<NodeEvent>>);

impl futures_core::Stream for TipEvents {
    type Item = ApiResult<NodeEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// Tip only: IPC tells the new tip's hash and height, not its parent. A tip
/// one block higher is `BlockConnected`; any other change (several blocks at
/// once, a reorg) is `Lagged`, so resync from `tip()`. A reorg that ends
/// exactly one block higher also reads as `BlockConnected`: a consumer that
/// must tell needs a `ChainSource` with headers (`bitcoin-core-rpc`).
///
/// Each subscriber holds one Core thread while it waits. The stream ends
/// after a transport error: `IpcClient` does not reconnect.
impl EventSource for IpcClient {
    type EventStream = TipEvents;

    async fn subscribe(&self) -> ApiResult<TipEvents> {
        let mut last = self.tip().await?;
        let (tx, rx) = mpsc::channel(64);
        let ipc = self.clone();
        tokio::spawn(async move {
            while !tx.is_closed() {
                let current = last.hash;
                let res = ipc
                    .run(move |conn| async move {
                        on_caps!(conn, |c| c.wait_tip_changed(current, TIP_WAIT).await)
                    })
                    .await;
                let ev = match res {
                    Ok(tip) if tip.hash == last.hash => continue,
                    Ok(tip) => {
                        let connect = tip.height == last.height + 1;
                        last = tip;
                        Ok(if connect {
                            NodeEvent::BlockConnected(tip.hash)
                        } else {
                            NodeEvent::Lagged
                        })
                    }
                    Err(e) => {
                        let fatal = matches!(e, NodeAPIError::Transport(_));
                        if tx.send(Err(e)).await.is_err() || fatal {
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
        Ok(TipEvents(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{OutPoint, Sequence, TxIn, Witness, absolute, transaction};

    /// Core 30.4's placeholder coinbase for height 102, as sent over IPC.
    fn core30_placeholder() -> (Transaction, Vec<TxOut>) {
        let commitment = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_hex(
                "6a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf9",
            )
            .unwrap(),
        };
        let tx = Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::from_consensus(101),
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_hex("016600").unwrap(),
                sequence: Sequence(0xffff_fffe),
                witness: Witness::from_slice(&[[0u8; 32]]),
            }],
            output: vec![
                TxOut {
                    value: Amount::from_int_btc(50),
                    script_pubkey: ScriptBuf::from_hex("51").unwrap(),
                },
                commitment.clone(),
            ],
        };
        (tx, vec![commitment])
    }

    #[test]
    fn core30_placeholder_becomes_a_coinbase() {
        let (tx, required) = core30_placeholder();
        let p = Parts::from_placeholder(tx, required.clone()).unwrap();
        assert_eq!(p.height, 102);
        let c = p.coinbase;
        assert_eq!(
            c.script_sig_prefix.as_bytes(),
            [0x01, 0x66],
            "height push only"
        );
        assert_eq!(c.witness, Some([0; 32]));
        assert_eq!(c.reward_remaining, Amount::from_int_btc(50));
        assert_eq!(c.required_outputs, required);
        assert_eq!(c.sequence, Sequence(0xffff_fffe));
        assert_eq!(c.lock_time, absolute::LockTime::from_consensus(101));
    }

    #[test]
    fn bip34_heights() {
        let h = |hex: &str| bip34_height(&ScriptBuf::from_hex(hex).unwrap());
        assert_eq!(h("51").unwrap(), 1, "OP_1");
        assert_eq!(h("60").unwrap(), 16, "OP_16");
        assert_eq!(h("0111").unwrap(), 17);
        assert_eq!(h("03a08601").unwrap(), 100_000);
        assert!(h("").is_err());
    }
}
