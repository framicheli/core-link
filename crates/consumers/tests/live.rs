//! Both consumers on a real node. `#[ignore]`: `cargo test -- --ignored`.
//! Templates over RPC on every Core, and over IPC from Core 30; events over
//! ZMQ and over RPC polling.
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::{Address, Amount, CompressedPublicKey, Network, OutPoint, PrivateKey, Transaction};
use bitcoin_core_ipc::IpcClient;
use bitcoin_core_rpc::{Auth, Client};
use bitcoin_core_zmq::{Topic, ZmqEvents};
use bitcoin_node_api::{
    ApiResult, Broadcaster, ChainSource, EventSource, MempoolSource, NodeEvent, TemplateSource,
};
use consumers::feed::{self, NewWork};
use consumers::follower::{Change, Follower};
use consumers::next_event;
use futures_core::Stream;
use std::collections::HashSet;
use std::time::Duration;
use test_node::{RPC_PASSWORD, RPC_USER, TestNode, free_port};

const FEE: Amount = Amount::from_sat(10_000);

struct Setup {
    _node: TestNode,
    rpc: Client,
    ipc: Option<IpcClient>,
    zmq: String,
    key: PrivateKey,
    addr: Address,
}

/// `bitcoin-node` with IPC when the release has it, else `bitcoind`; ZMQ
/// `sequence` on; 110 blocks mined to our key.
async fn setup() -> Setup {
    let zmq = format!("tcp://127.0.0.1:{}", free_port());
    let arg = format!("-zmqpubsequence={zmq}");
    let node = TestNode::start_ipc(&[&arg]).unwrap_or_else(|| TestNode::start(&[&arg]));
    let auth = Auth::UserPass(RPC_USER.into(), RPC_PASSWORD.into());
    let mut rpc = None;
    for _ in 0..100 {
        match Client::connect(&node.rpc_url(), auth.clone()).await {
            Ok(c) => {
                rpc = Some(c);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let rpc = rpc.expect("node came up");
    eprintln!("Core {}", rpc.version().0);
    let ipc = match node.ipc_socket().exists() {
        true => Some(IpcClient::connect(node.ipc_socket()).await.expect("IPC")),
        false => None,
    };
    let secp = Secp256k1::new();
    let key = PrivateKey::new(
        SecretKey::from_slice(&[0x11; 32]).unwrap(),
        Network::Regtest,
    );
    let addr = Address::p2wpkh(
        &CompressedPublicKey::from_private_key(&secp, &key).unwrap(),
        Network::Regtest,
    );
    rpc.generate_to_address(110, &addr).await.unwrap();
    Setup {
        _node: node,
        rpc,
        ipc,
        zmq,
        key,
        addr,
    }
}

impl Setup {
    /// A tx spending the coinbase at `height` back to us, paying `fee`.
    async fn spend(&self, height: u64, fee: Amount) -> Transaction {
        let cb = &self
            .rpc
            .get_block(self.rpc.get_block_hash(height).await.unwrap())
            .await
            .unwrap()
            .txdata[0];
        let prev = OutPoint {
            txid: cb.compute_txid(),
            vout: 0,
        };
        let tx = self
            .rpc
            .create_raw_transaction(
                &[prev],
                &[(self.addr.clone(), cb.output[0].value - fee)],
                None,
                None,
            )
            .await
            .unwrap();
        self.rpc
            .sign_raw_transaction_with_key(&tx, &[self.key], &[])
            .await
            .unwrap()
            .tx
    }

    /// A ZMQ stream that is known to be live: mine until an event arrives.
    async fn zmq_events(
        &self,
        capacity: usize,
    ) -> impl Stream<Item = ApiResult<NodeEvent>> + Unpin + use<> {
        let zmq = ZmqEvents::connect(&self.zmq, Topic::Sequence)
            .await
            .unwrap()
            .with_capacity(capacity);
        let mut s = zmq.subscribe().await.unwrap();
        for _ in 0..50 {
            self.rpc.generate_to_address(1, &self.addr).await.unwrap();
            if tokio::time::timeout(Duration::from_millis(100), next_event(&mut s))
                .await
                .is_ok()
            {
                return s;
            }
        }
        panic!("no ZMQ event after 50 blocks");
    }
}

/// The feed on one template source: mine, take a broadcast tx, react to
/// someone else's block. `fee_wakeup`: whether `wait_next` honours a fee
/// threshold (IPC) or not (RPC longpoll: at least 60s after a mempool change).
async fn feed_case<S, E>(s: &Setup, src: &S, mut events: E, spend_height: u64, fee_wakeup: bool)
where
    S: TemplateSource + ChainSource,
    E: Stream<Item = ApiResult<NodeEvent>> + Unpin,
{
    let payout = s.addr.script_pubkey();
    for _ in 0..2 {
        let t = src.template().await.unwrap();
        let hash = feed::mine(src, &t, &payout).await.unwrap();
        assert_eq!(s.rpc.get_best_block_hash().await.unwrap(), hash);
    }

    // A broadcast tx: new work with its fee.
    let t = src.template().await.unwrap();
    let tx = s.spend(spend_height, FEE).await;
    let txid = s.rpc.broadcast(&tx).await.unwrap();
    let work = feed::next_work(
        src,
        &t,
        &mut events,
        Some(Amount::from_sat(1)),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let mut t = match (fee_wakeup, work) {
        (true, NewWork::Template(t)) => t,
        (false, NewWork::Timeout) => src.template().await.unwrap(),
        (_, other) => panic!("fee_wakeup {fee_wakeup}: got {other:?}"),
    };
    // `getblocktemplate` serves its cached template for up to 5s after a
    // mempool change; IPC's comes straight from `waitNext`.
    let has_tx =
        |txs: &[bitcoin_node_api::TemplateTx]| txs.iter().any(|x| x.tx.compute_txid() == txid);
    for _ in 0..20 {
        if has_tx(&src.transactions(&t).await.unwrap()) {
            break;
        }
        assert!(!fee_wakeup, "IPC's new template has the tx");
        tokio::time::sleep(Duration::from_millis(500)).await;
        t = src.template().await.unwrap();
    }
    assert!(has_tx(&src.transactions(&t).await.unwrap()));
    assert_eq!(t.coinbase.reward_remaining, Amount::from_int_btc(50) + FEE);
    let hash = feed::mine(src, &t, &payout).await.unwrap();
    let block = s.rpc.get_block(hash).await.unwrap();
    assert!(
        block.txdata.iter().any(|x| x.compute_txid() == txid),
        "tx mined"
    );
    assert_eq!(
        block.txdata[0].output[0].value,
        Amount::from_int_btc(50) + FEE
    );

    // Someone else's block: the feed hears of it and moves on.
    let t = src.template().await.unwrap();
    let theirs = s.rpc.generate_to_address(1, &s.addr).await.unwrap().0[0];
    match feed::next_work(src, &t, &mut events, None, Duration::from_secs(30))
        .await
        .unwrap()
    {
        NewWork::Block(h) => assert_eq!(h, theirs),
        NewWork::Template(next) => assert_eq!(next.prev_hash, theirs),
        // Over RPC polling, our block and theirs can land between two polls:
        // the tip moved by two, so the stream says `Lagged` rather than vouch
        // for a parent it didn't see. The recipe: resync from `tip()`.
        NewWork::Lagged => assert_eq!(src.tip().await.unwrap().hash, theirs),
        other => panic!("expected new work, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs bitcoind"]
async fn template_feed() {
    let s = setup().await;
    // RPC templates, RPC polling events: every Core.
    let events = s.rpc.subscribe().await.unwrap();
    feed_case(&s, &s.rpc, events, 1, false).await;
    // IPC templates, ZMQ events: Core 30+.
    if let Some(ipc) = &s.ipc {
        let events = s.zmq_events(4096).await;
        feed_case(&s, ipc, events, 2, true).await;
    } else {
        eprintln!("IPC part skipped: no bitcoin-node before Core 30");
    }
}

/// Applies events until the follower's tip is the node's, and the mempool
/// matches too when `mempool` (ZMQ only: RPC polling has no tx events).
async fn catch_up<C, E>(
    f: &mut Follower<C>,
    events: &mut E,
    rpc: &Client,
    mempool: bool,
) -> Vec<Change>
where
    C: ChainSource + MempoolSource + Sync,
    E: Stream<Item = ApiResult<NodeEvent>> + Unpin,
{
    let mut changes = Vec::new();
    loop {
        let tip = rpc.tip().await.unwrap();
        let pool: HashSet<_> = rpc.txids().await.unwrap().into_iter().collect();
        if f.tip() == Some(tip) && (!mempool || *f.mempool() == pool) {
            return changes;
        }
        let ev = tokio::time::timeout(Duration::from_secs(30), next_event(events))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "stuck at {:?}, node at {tip:?}; changes {changes:?}",
                    f.tip()
                )
            })
            .expect("stream open")
            .expect("event");
        changes.push(f.apply(ev).await.unwrap());
    }
}

/// Every height the follower has matches the node.
async fn same_chain<C: ChainSource + MempoolSource + Sync>(
    f: &Follower<C>,
    rpc: &Client,
    from: u64,
) {
    let tip = f.tip().unwrap();
    for h in from..=tip.height {
        assert_eq!(
            f.block_hash(h),
            Some(rpc.block_hash(h).await.unwrap()),
            "height {h}"
        );
    }
}

/// Invalidates the top `depth` blocks and mines `depth + 1` others.
async fn reorg(s: &Setup, depth: u64) {
    static N: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
    let tip = s.rpc.tip().await.unwrap();
    let first = s.rpc.block_hash(tip.height - depth + 1).await.unwrap();
    s.rpc.invalidate_block(first).await.unwrap();
    // A fresh payout each time: same parent, height and second would make a
    // block identical to one already invalidated, which Core refuses.
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let other = Address::p2wsh(&bitcoin::ScriptBuf::from(vec![n]), Network::Regtest);
    s.rpc
        .generate_to_address(depth as u32 + 1, &other)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "needs bitcoind"]
async fn chain_follower() {
    let s = setup().await;
    let start = 100;

    // ZMQ: every block and tx event.
    let mut events = s.zmq_events(4096).await;
    let mut f = Follower::start(s.rpc.clone(), start).await.unwrap();
    s.rpc.generate_to_address(3, &s.addr).await.unwrap();
    let changes = catch_up(&mut f, &mut events, &s.rpc, true).await;
    assert!(
        changes
            .iter()
            .all(|c| matches!(c, Change::Connected(_) | Change::Ignored)),
        "{changes:?}"
    );

    // Mempool through an RBF replacement, then mined.
    let tx1 = s.spend(1, Amount::from_sat(1_000)).await;
    s.rpc.broadcast(&tx1).await.unwrap();
    let tx2 = s.spend(1, FEE).await;
    let txid2 = s.rpc.broadcast(&tx2).await.unwrap();
    catch_up(&mut f, &mut events, &s.rpc, true).await;
    assert_eq!(f.mempool(), &HashSet::from([txid2]));
    s.rpc.generate_to_address(1, &s.addr).await.unwrap();
    catch_up(&mut f, &mut events, &s.rpc, true).await;
    assert!(f.mempool().is_empty(), "mined tx left the copy");

    // A 2-block reorg: ZMQ sends the disconnects, then the connects.
    reorg(&s, 2).await;
    let changes = catch_up(&mut f, &mut events, &s.rpc, true).await;
    let dis = changes
        .iter()
        .filter(|c| matches!(c, Change::Disconnected(_)))
        .count();
    assert_eq!(dis, 2, "{changes:?}");
    same_chain(&f, &s.rpc, start).await;

    // The snapshot's sequence: a tx added, then mined, before the follower
    // starts. Its `TxAdded` is still queued, and there's no `TxRemoved` for a
    // mined tx, so without the sequence it would stay in the copy forever.
    let mut late = s.zmq_events(4096).await;
    let tx = s.spend(3, FEE).await;
    let txid = s.rpc.broadcast(&tx).await.unwrap();
    s.rpc.generate_to_address(1, &s.addr).await.unwrap();
    let mut f5 = Follower::start(s.rpc.clone(), start).await.unwrap();
    assert!(f5.mempool().is_empty());
    // Read up to the stale `TxAdded`: it must change nothing.
    loop {
        let ev = next_event(&mut late).await.unwrap().unwrap();
        let stale = matches!(ev, NodeEvent::TxAdded { txid: t, .. } if t == txid);
        let change = f5.apply(ev).await.unwrap();
        if stale {
            assert_eq!(change, Change::Ignored, "below the snapshot's sequence");
            break;
        }
    }
    catch_up(&mut f5, &mut late, &s.rpc, true).await;
    assert!(f5.mempool().is_empty());

    // Lagged: a 1-event channel overflows; the follower resyncs.
    let mut tiny = s.zmq_events(1).await;
    let mut f2 = Follower::start(s.rpc.clone(), start).await.unwrap();
    s.rpc.generate_to_address(5, &s.addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let changes = catch_up(&mut f2, &mut tiny, &s.rpc, false).await;
    assert!(
        changes.iter().any(|c| matches!(c, Change::Resynced(_))),
        "{changes:?}"
    );
    same_chain(&f2, &s.rpc, start).await;

    // RPC polling reports a non-child tip as `Lagged`: the follower resyncs.
    let mut polled = s.rpc.subscribe().await.unwrap();
    let mut f3 = Follower::start(s.rpc.clone(), start).await.unwrap();
    reorg(&s, 2).await;
    let changes = catch_up(&mut f3, &mut polled, &s.rpc, false).await;
    assert!(
        changes.iter().any(|c| matches!(c, Change::Resynced(_))),
        "{changes:?}"
    );
    same_chain(&f3, &s.rpc, start).await;

    // Neither backend makes the follower walk back to a fork (ZMQ sends the
    // disconnects, polling sends `Lagged`). A source that only reports
    // connects would: hand it the new tip directly.
    let mut f4 = Follower::start(s.rpc.clone(), start).await.unwrap();
    let old_tip = f4.tip().unwrap();
    reorg(&s, 2).await;
    let new_tip = s.rpc.tip().await.unwrap();
    let change = f4
        .apply(NodeEvent::BlockConnected(new_tip.hash))
        .await
        .unwrap();
    assert_eq!(
        change,
        Change::Reorg {
            fork_height: old_tip.height - 2,
            depth: 2,
            tip: new_tip,
        }
    );
    same_chain(&f4, &s.rpc, start).await;
}
