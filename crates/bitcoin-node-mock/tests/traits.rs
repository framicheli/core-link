//! `MockNode` behaves like the RPC backend where callers can tell.
use bitcoin::hashes::Hash;
use bitcoin::{
    Amount, FeeRate, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness, absolute,
    transaction,
};
use bitcoin_node_api::*;
use bitcoin_node_mock::MockNode;
use futures_core::Stream;
use std::pin::Pin;
use std::time::Duration;

async fn next<S: Stream + Unpin>(s: &mut S) -> Option<S::Item> {
    std::future::poll_fn(|cx| Pin::new(&mut *s).poll_next(cx)).await
}

fn spend(prev: OutPoint, value: Amount) -> Transaction {
    Transaction {
        version: transaction::Version::TWO,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: prev,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value,
            script_pubkey: ScriptBuf::new_op_return([2]),
        }],
    }
}

fn rpc_code<T: std::fmt::Debug>(r: ApiResult<T>) -> RpcErrorCode {
    match r {
        Err(NodeAPIError::Rpc { code, .. }) => code,
        other => panic!("{other:?}"),
    }
}

fn coinbase_out(node: &MockNode, height: u64) -> OutPoint {
    OutPoint {
        txid: node.block_at(height).unwrap().txdata[0].compute_txid(),
        vout: 0,
    }
}

#[tokio::test]
async fn lookups() {
    let node = MockNode::regtest();
    let hashes = node.mine_empty(3);
    assert_eq!(
        node.tip().await.unwrap(),
        Tip {
            height: 3,
            hash: hashes[2]
        }
    );
    assert_eq!(node.block_hash(1).await.unwrap(), hashes[0]);
    assert_eq!(
        rpc_code(node.block_hash(9).await),
        RpcErrorCode::InvalidParameter
    );
    let unknown = "00".repeat(32).parse().unwrap();
    assert_eq!(
        rpc_code(node.block(unknown).await),
        RpcErrorCode::InvalidAddressOrKey
    );
    assert_eq!(
        node.header(hashes[1]).await.unwrap().prev_blockhash,
        hashes[0]
    );

    let op = coinbase_out(&node, 1);
    let u = node.utxo(op, false).await.unwrap().unwrap();
    assert_eq!((u.confirmations, u.coinbase), (3, true));
    let cb = node.block_at(1).unwrap().txdata[0].clone();
    assert_eq!(node.transaction(op.txid, None).await.unwrap(), cb);

    // Spent in the mempool: gone only with `include_mempool`.
    let tx = spend(op, Amount::from_int_btc(49));
    let txid = node.broadcast(&tx).await.unwrap();
    assert!(node.utxo(op, true).await.unwrap().is_none());
    assert!(node.utxo(op, false).await.unwrap().is_some());
    let child = OutPoint { txid, vout: 0 };
    assert_eq!(
        node.utxo(child, true).await.unwrap().unwrap().confirmations,
        0
    );
    assert!(node.utxo(child, false).await.unwrap().is_none());
    let e = node.entry(txid).await.unwrap().unwrap();
    assert_eq!(e.fee, Amount::from_int_btc(1));
    assert_eq!((e.ancestors, e.descendants), (1, 1));
    assert!(node.entry(op.txid).await.unwrap().is_none());

    // Mined: confirmed, and a rebroadcast is "already in chain".
    node.mine(std::slice::from_ref(&tx));
    assert_eq!(
        node.utxo(child, false)
            .await
            .unwrap()
            .unwrap()
            .confirmations,
        1
    );
    assert_eq!(
        rpc_code(node.broadcast(&tx).await),
        RpcErrorCode::VerifyAlreadyInChain
    );
}

#[tokio::test]
async fn events_and_sequences() {
    let node = MockNode::regtest();
    node.mine_empty(2);
    let mut ev = node.subscribe().await.unwrap();
    let op = coinbase_out(&node, 1);
    let a = spend(op, Amount::from_int_btc(49));
    let b = spend(op, Amount::from_int_btc(48));
    let s = node.snapshot().await.unwrap().sequence.unwrap();
    let ta = node.add_to_mempool(a);
    let tb = node.add_to_mempool(b.clone());
    let want = [
        NodeEvent::TxAdded {
            txid: ta,
            mempool_sequence: Some(s),
        },
        // `b` replaces `a`.
        NodeEvent::TxRemoved {
            txid: ta,
            mempool_sequence: Some(s + 1),
        },
        NodeEvent::TxAdded {
            txid: tb,
            mempool_sequence: Some(s + 2),
        },
    ];
    for w in want {
        assert_eq!(next(&mut ev).await.unwrap().unwrap(), w);
    }
    // Mined: no `TxRemoved`, straight to the block.
    let h = node.mine(&[b]);
    assert_eq!(
        next(&mut ev).await.unwrap().unwrap(),
        NodeEvent::BlockConnected(h)
    );
    assert!(node.txids().await.unwrap().is_empty());

    // A reorg: disconnects (tip first), the tx back in the mempool, connects.
    let old_tip = node.tip().await.unwrap().hash;
    node.reorg(1, 2);
    assert_eq!(
        next(&mut ev).await.unwrap().unwrap(),
        NodeEvent::BlockDisconnected(old_tip)
    );
    assert!(matches!(
        next(&mut ev).await.unwrap().unwrap(),
        NodeEvent::TxAdded { txid, .. } if txid == tb
    ));
    for _ in 0..2 {
        assert!(matches!(
            next(&mut ev).await.unwrap().unwrap(),
            NodeEvent::BlockConnected(_)
        ));
    }
    node.lag();
    assert_eq!(next(&mut ev).await.unwrap().unwrap(), NodeEvent::Lagged);
}

#[tokio::test]
async fn failures_fees_and_packages() {
    let node = MockNode::regtest();
    node.fail_next("tip", TransportError::Timeout.into());
    assert_eq!(
        node.tip().await.unwrap_err(),
        TransportError::Timeout.into()
    );
    assert!(node.tip().await.is_ok(), "only the next call fails");

    let target = FeeTarget {
        blocks: 6,
        mode: FeeMode::Economical,
    };
    assert_eq!(node.estimate_fee(target).await.unwrap(), None);
    let rate = FeeRate::from_sat_per_vb_u32(3);
    node.set_fee(target, Some(rate));
    assert_eq!(node.estimate_fee(target).await.unwrap(), Some(rate));

    node.mine_empty(1);
    let parent = spend(coinbase_out(&node, 1), Amount::from_int_btc(49));
    let child = spend(
        OutPoint {
            txid: parent.compute_txid(),
            vout: 0,
        },
        Amount::from_int_btc(48),
    );
    let accept = node
        .test_accept(std::slice::from_ref(&parent))
        .await
        .unwrap();
    assert!(accept[0].allowed);
    node.broadcast(&parent).await.unwrap();
    let r = node
        .broadcast_package(&[parent.clone(), child.clone()])
        .await
        .unwrap();
    assert_eq!(r[0].outcome, PackageOutcome::AlreadyInMempool);
    assert_eq!(
        r[1].outcome,
        PackageOutcome::Accepted {
            fee: Amount::from_int_btc(1)
        }
    );
    let e = node.entry(child.compute_txid()).await.unwrap().unwrap();
    assert_eq!(e.ancestors, 2);
    // Evicting the parent takes the child too.
    assert!(node.remove_from_mempool(parent.compute_txid()));
    assert!(node.txids().await.unwrap().is_empty());
}

#[tokio::test]
async fn templates_and_solutions() {
    let node = MockNode::regtest();
    let t = node.template().await.unwrap();
    assert_eq!(t.height, 1);
    // Times out with nothing new.
    let opts = WaitOptions {
        timeout: Some(Duration::from_millis(50)),
        fee_threshold: None,
    };
    assert_eq!(node.wait_next(&t, opts).await.unwrap(), None);

    let payout = ScriptBuf::new_op_return([3]);
    let mut sol = Solution {
        version: t.version,
        time: t.cur_time,
        nonce: 0,
        coinbase: t.coinbase_tx(&payout, &[1]),
    };
    let meets = |sol: &Solution| {
        let h = t.header(sol);
        h.target().is_met_by(h.block_hash())
    };
    // A nonce that misses the target is refused.
    while meets(&sol) {
        sol.nonce += 1;
    }
    assert_eq!(
        rpc_code(node.submit_solution(&t, &sol).await),
        RpcErrorCode::VerifyError
    );
    while !meets(&sol) {
        sol.nonce += 1;
    }
    node.submit_solution(&t, &sol).await.unwrap();
    node.submit_solution(&t, &sol).await.unwrap(); // already known
    let tip = node.tip().await.unwrap();
    assert_eq!(tip.hash, t.header(&sol).block_hash());
    let block = node.block(tip.hash).await.unwrap();
    assert!(block.check_witness_commitment());

    // The old template is stale: `wait_next` returns at once.
    let next = node.wait_next(&t, opts).await.unwrap().unwrap();
    assert_eq!(next.prev_hash, tip.hash);
}

#[tokio::test]
async fn header_info_counts_height_and_work() {
    let node = MockNode::regtest();
    node.mine_empty(5);
    // Regtest bits 0x207fffff: work 2 per block.
    let work = |blocks: u8| {
        let mut b = [0; 32];
        b[31] = 2 * blocks;
        bitcoin::Work::from_be_bytes(b)
    };
    for h in [0u64, 1, 5] {
        let hash = node.block_hash(h).await.unwrap();
        let i = node.header_info(hash).await.unwrap();
        assert_eq!(i.header, node.header(hash).await.unwrap());
        assert_eq!((i.height, i.chain_work), (h, work(h as u8 + 1)));
    }

    // A block reorged out keeps its height and work, as with Core.
    let stale = node.tip().await.unwrap().hash;
    node.reorg(2, 3);
    let i = node.header_info(stale).await.unwrap();
    assert_eq!((i.height, i.chain_work), (5, work(6)));
    let tip = node.tip().await.unwrap();
    assert_eq!(node.header_info(tip.hash).await.unwrap().height, 6);

    let unknown = node
        .header_info(bitcoin::BlockHash::from_byte_array([7; 32]))
        .await;
    assert!(matches!(
        unknown,
        Err(NodeAPIError::Rpc {
            code: RpcErrorCode::InvalidAddressOrKey,
            ..
        })
    ));
}
