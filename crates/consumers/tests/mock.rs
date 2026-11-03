//! Both consumers offline, against `bitcoin-node-mock`: the same scenarios
//! as `live.rs` (mining, new work, a reorg, an RBF replacement, `Lagged`),
//! with no `bitcoind`.
use bitcoin::{
    Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness, absolute, transaction,
};
use bitcoin_node_api::{
    ApiResult, ChainSource, EventSource, MempoolSource, NodeEvent, TemplateSource,
};
use bitcoin_node_mock::MockNode;
use consumers::feed::{self, NewWork};
use consumers::follower::{Change, Follower};
use consumers::next_event;
use futures_core::Stream;
use std::collections::HashSet;
use std::time::Duration;

/// A tx spending output 0 of the coinbase at `height`, paying `fee`.
fn spend(node: &MockNode, height: u64, fee: Amount) -> Transaction {
    let cb = &node.block_at(height).unwrap().txdata[0];
    Transaction {
        version: transaction::Version::TWO,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: cb.compute_txid(),
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: cb.output[0].value - fee,
            script_pubkey: ScriptBuf::new_op_return([1]),
        }],
    }
}

/// Every event already queued, applied.
async fn drain<E>(f: &mut Follower<MockNode>, events: &mut E) -> Vec<Change>
where
    E: Stream<Item = ApiResult<NodeEvent>> + Unpin,
{
    let mut changes = Vec::new();
    while let Ok(Some(ev)) =
        tokio::time::timeout(Duration::from_millis(50), next_event(events)).await
    {
        changes.push(f.apply(ev.unwrap()).await.unwrap());
    }
    changes
}

async fn same_chain(f: &Follower<MockNode>, node: &MockNode, from: u64) {
    let tip = node.tip().await.unwrap();
    assert_eq!(f.tip(), Some(tip));
    for h in from..=tip.height {
        assert_eq!(f.block_hash(h), Some(node.block_hash(h).await.unwrap()));
    }
}

#[tokio::test]
async fn template_feed_on_mock() {
    let node = MockNode::regtest();
    node.mine_empty(110);
    let mut events = node.subscribe().await.unwrap();
    let payout = ScriptBuf::new_op_return([7]);

    // Mine two blocks of our own.
    for _ in 0..2 {
        let t = node.template().await.unwrap();
        let hash = feed::mine(&node, &t, &payout).await.unwrap();
        assert_eq!(node.tip().await.unwrap().hash, hash);
    }

    // A broadcast tx: `wait_next` wakes with a template that has its fee.
    let t = node.template().await.unwrap();
    let fee = Amount::from_sat(10_000);
    let txid = node.add_to_mempool(spend(&node, 1, fee));
    let work = feed::next_work(
        &node,
        &t,
        &mut events,
        Some(Amount::from_sat(1)),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    let NewWork::Template(t) = work else {
        panic!("{work:?}")
    };
    let txs = node.transactions(&t).await.unwrap();
    assert!(txs.iter().any(|x| x.tx.compute_txid() == txid));
    assert_eq!(t.coinbase.reward_remaining, Amount::from_int_btc(50) + fee);
    let hash = feed::mine(&node, &t, &payout).await.unwrap();
    let block = node.block(hash).await.unwrap();
    assert!(block.txdata.iter().any(|x| x.compute_txid() == txid));
    assert!(node.txids().await.unwrap().is_empty());

    // Someone else's block. First drop the events for our own blocks: once
    // the tip moves, a stale one would also (rightly) count as new work, but
    // name an older hash.
    while tokio::time::timeout(Duration::from_millis(20), next_event(&mut events))
        .await
        .is_ok()
    {}
    let t = node.template().await.unwrap();
    let theirs = node.mine(&[]);
    match feed::next_work(&node, &t, &mut events, None, Duration::from_secs(5))
        .await
        .unwrap()
    {
        NewWork::Block(h) => assert_eq!(h, theirs),
        NewWork::Template(next) => assert_eq!(next.prev_hash, theirs),
        other => panic!("{other:?}"),
    }

    // A fee threshold the mempool doesn't reach: timeout.
    let t = node.template().await.unwrap();
    let mut quiet = node.subscribe().await.unwrap();
    node.add_to_mempool(spend(&node, 2, Amount::from_sat(100)));
    let work = feed::next_work(
        &node,
        &t,
        &mut quiet,
        Some(Amount::from_sat(1_000)),
        Duration::from_millis(200),
    )
    .await
    .unwrap();
    assert!(matches!(work, NewWork::Timeout), "{work:?}");

    // Lost events.
    let t = node.template().await.unwrap();
    node.lag();
    let work = feed::next_work(&node, &t, &mut quiet, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        matches!(work, NewWork::Lagged | NewWork::Template(_)),
        "{work:?}"
    );
}

#[tokio::test]
async fn chain_follower_on_mock() {
    let node = MockNode::regtest();
    node.mine_empty(110);
    let start = 100;

    let mut events = node.subscribe().await.unwrap();
    let mut f = Follower::start(node.clone(), start).await.unwrap();
    node.mine_empty(3);
    let changes = drain(&mut f, &mut events).await;
    assert_eq!(changes.len(), 3);
    assert!(
        changes.iter().all(|c| matches!(c, Change::Connected(_))),
        "{changes:?}"
    );
    same_chain(&f, &node, start).await;

    // RBF: the replacement evicts the original (`TxRemoved`), then it's mined.
    let tx1 = spend(&node, 1, Amount::from_sat(1_000));
    let tx2 = spend(&node, 1, Amount::from_sat(10_000));
    node.add_to_mempool(tx1);
    let txid2 = node.add_to_mempool(tx2.clone());
    drain(&mut f, &mut events).await;
    assert_eq!(f.mempool(), &HashSet::from([txid2]));
    node.mine(&[tx2]);
    drain(&mut f, &mut events).await;
    assert!(f.mempool().is_empty(), "mined tx left the copy");

    // A 2-block reorg: disconnects first, then connects.
    node.reorg(2, 3);
    let changes = drain(&mut f, &mut events).await;
    let dis = changes
        .iter()
        .filter(|c| matches!(c, Change::Disconnected(_)))
        .count();
    assert_eq!(dis, 2, "{changes:?}");
    same_chain(&f, &node, start).await;
    // The reorged-out block's tx is back in the mempool.
    assert_eq!(f.mempool(), &HashSet::from([txid2]));
    assert_eq!(node.txids().await.unwrap(), vec![txid2]);

    // The snapshot's sequence: a stale `TxAdded` is ignored.
    let mut late = node.subscribe().await.unwrap();
    let tx = spend(&node, 3, Amount::from_sat(500));
    let txid = node.add_to_mempool(tx.clone());
    node.mine(&[tx]);
    let mut f2 = Follower::start(node.clone(), start).await.unwrap();
    assert!(!f2.mempool().contains(&txid));
    loop {
        let ev = next_event(&mut late).await.unwrap().unwrap();
        let stale = matches!(ev, NodeEvent::TxAdded { txid: t, .. } if t == txid);
        let change = f2.apply(ev).await.unwrap();
        if stale {
            assert_eq!(change, Change::Ignored);
            break;
        }
    }
    drain(&mut f2, &mut late).await;
    assert!(!f2.mempool().contains(&txid));

    // `Lagged`: the follower resyncs, even across a reorg it never saw.
    let mut f3 = Follower::start(node.clone(), start).await.unwrap();
    let mut lagging = node.subscribe().await.unwrap();
    node.reorg(2, 2);
    // Pretend those events were lost: drop them, then lag.
    while tokio::time::timeout(Duration::from_millis(20), next_event(&mut lagging))
        .await
        .is_ok()
    {}
    node.lag();
    let changes = drain(&mut f3, &mut lagging).await;
    assert!(
        changes.iter().any(|c| matches!(c, Change::Resynced(_))),
        "{changes:?}"
    );
    same_chain(&f3, &node, start).await;

    // A source reporting connects only: the follower walks back to the fork.
    let mut f4 = Follower::start(node.clone(), start).await.unwrap();
    let old = f4.tip().unwrap();
    node.reorg(2, 3);
    let new = node.tip().await.unwrap();
    let change = f4.apply(NodeEvent::BlockConnected(new.hash)).await.unwrap();
    assert_eq!(
        change,
        Change::Reorg {
            fork_height: old.height - 2,
            depth: 2,
            tip: new,
        }
    );
    same_chain(&f4, &node, start).await;
}
