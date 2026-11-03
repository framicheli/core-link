//! `Emitter` on the mock: the scenarios of `bdk_bitcoind_rpc`'s own emitter
//! tests, then a `bdk_wallet` kept in sync through a payment, a reorg and an
//! eviction.
use bdk_chain::local_chain::LocalChain;
use bdk_core::{BlockId, CheckPoint};
use bdk_wallet::{Balance, KeychainKind, Wallet};
use bitcoin::{
    Amount, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness,
    absolute, transaction,
};
use bitcoin_node_api::{ChainSource, NodeAPIError, TransportError};
use bitcoin_node_bdk::{Emitter, NO_EXPECTED_MEMPOOL_TXS};
use bitcoin_node_mock::MockNode;
use std::collections::BTreeSet;

fn genesis_cp(node: &MockNode) -> CheckPoint {
    CheckPoint::new(BlockId {
        height: 0,
        hash: node.block_at(0).unwrap().block_hash(),
    })
}

/// Emits to the tip, applying each block to `chain`; the heights emitted.
async fn sync(emitter: &mut Emitter<MockNode>, chain: &mut LocalChain) -> Vec<u32> {
    let mut heights = Vec::new();
    while let Some(ev) = emitter.next_block().await.unwrap() {
        chain
            .apply_update(ev.checkpoint.clone())
            .expect("each emission connects");
        heights.push(ev.block_height());
    }
    heights
}

/// `chain` is the node's active chain, block for block.
async fn assert_matches(chain: &LocalChain, node: &MockNode) {
    let tip = node.tip().await.unwrap();
    assert_eq!(u64::from(chain.tip().height()), tip.height);
    for cp in chain.tip().iter() {
        assert_eq!(
            cp.hash(),
            node.block_hash(u64::from(cp.height())).await.unwrap(),
            "height {}",
            cp.height()
        );
    }
}

/// A tx spending `prev` to `spk`; the mock checks no scripts.
fn pay(prev: OutPoint, spk: ScriptBuf, sats: u64) -> Transaction {
    Transaction {
        version: transaction::Version::TWO,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: prev,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: spk,
        }],
    }
}

/// Output 0 of the coinbase at `height`.
fn coinbase(node: &MockNode, height: u64) -> OutPoint {
    OutPoint {
        txid: node.block_at(height).unwrap().txdata[0].compute_txid(),
        vout: 0,
    }
}

#[tokio::test]
async fn emits_in_order_through_reorgs() {
    let node = MockNode::regtest();
    let mut chain = LocalChain::from_genesis_hash(node.block_at(0).unwrap().block_hash()).0;
    let mut emitter = Emitter::new(node.clone(), chain.tip(), 0, NO_EXPECTED_MEMPOOL_TXS);

    node.mine_empty(101);
    assert_eq!(
        sync(&mut emitter, &mut chain).await,
        (1..=101).collect::<Vec<_>>()
    );
    assert_matches(&chain, &node).await;
    assert!(emitter.next_block().await.unwrap().is_none(), "at the tip");

    // Replace the top 6 with 7: emission restarts at the fork, 96.
    node.reorg(6, 7);
    assert_eq!(
        sync(&mut emitter, &mut chain).await,
        (96..=102).collect::<Vec<_>>()
    );
    assert_matches(&chain, &node).await;

    // A reorg onto a shorter chain: nothing new above the fork, but the
    // emitter must notice and re-emit from it.
    node.reorg(5, 3);
    assert_eq!(
        sync(&mut emitter, &mut chain).await,
        (98..=100).collect::<Vec<_>>()
    );
    assert_matches(&chain, &node).await;
}

#[tokio::test]
async fn start_height_skips_to_it_and_connects() {
    let node = MockNode::regtest();
    node.mine_empty(60);
    let mut chain = LocalChain::from_genesis_hash(node.block_at(0).unwrap().block_hash()).0;
    let mut emitter = Emitter::new(node.clone(), chain.tip(), 50, NO_EXPECTED_MEMPOOL_TXS);
    let first = emitter.next_block().await.unwrap().unwrap();
    assert_eq!(first.block_height(), 50);
    assert_eq!(first.connected_to().height, 0, "connects to genesis");
    chain.apply_update(first.checkpoint).unwrap();
    assert_eq!(
        sync(&mut emitter, &mut chain).await,
        (51..=60).collect::<Vec<_>>()
    );
}

/// `bdk_bitcoind_rpc`'s `no_agreement_point`: a reorg below the start
/// height re-emits from the fork.
#[tokio::test]
async fn reorg_below_start_height() {
    let node = MockNode::regtest();
    let mut emitter = Emitter::new(node.clone(), genesis_cp(&node), 98, NO_EXPECTED_MEMPOOL_TXS);
    node.mine_empty(101);
    let b98 = emitter.next_block().await.unwrap().unwrap();
    let b99a = emitter.next_block().await.unwrap().unwrap();
    let b100a = emitter.next_block().await.unwrap().unwrap();
    assert_eq!(
        [
            b98.block_height(),
            b99a.block_height(),
            b100a.block_height()
        ],
        [98, 99, 100]
    );
    node.reorg(3, 3); // 99a..101a out, 99b..101b in
    let b99b = emitter.next_block().await.unwrap().unwrap();
    assert_eq!(b99b.block_height(), 99);
    assert_ne!(b99b.block_hash(), b99a.block_hash());
    assert_eq!(b99b.block.header.prev_blockhash, b98.block_hash());
    assert_eq!(b99b.connected_to(), b98.checkpoint.block_id());
}

/// `bdk_bitcoind_rpc`'s `test_sync_with_new_emitter_after_reorg`: a new
/// emitter from the old tip, started at its height, still connects.
#[tokio::test]
async fn new_emitter_after_reorg_connects() {
    let node = MockNode::regtest();
    let mut chain = LocalChain::from_genesis_hash(node.block_at(0).unwrap().block_hash()).0;
    node.mine_empty(110);
    let mut emitter = Emitter::new(node.clone(), chain.tip(), 0, NO_EXPECTED_MEMPOOL_TXS);
    sync(&mut emitter, &mut chain).await;
    let before = chain.tip();

    node.reorg(6, 6);
    let mut emitter = Emitter::new(
        node.clone(),
        chain.tip(),
        before.height(),
        NO_EXPECTED_MEMPOOL_TXS,
    );
    sync(&mut emitter, &mut chain).await;
    assert_eq!(chain.tip().height(), before.height());
    assert_ne!(chain.tip().hash(), before.hash());
    assert_matches(&chain, &node).await;
}

#[tokio::test]
async fn mempool_new_repeated_confirmed_and_evicted() {
    let node = MockNode::regtest();
    node.mine_empty(110);
    let mut emitter = Emitter::new(node.clone(), genesis_cp(&node), 0, NO_EXPECTED_MEMPOOL_TXS);
    while emitter.next_block().await.unwrap().is_some() {}

    let txs: Vec<Transaction> = (1..=3)
        .map(|h| {
            pay(
                coinbase(&node, h),
                ScriptBuf::new_op_return([h as u8]),
                1000,
            )
        })
        .collect();
    let ids: Vec<Txid> = txs.iter().map(|t| node.add_to_mempool(t.clone())).collect();
    let emitted = |ev: &bitcoin_node_bdk::MempoolEvent| -> BTreeSet<Txid> {
        ev.update.iter().map(|(t, _)| t.compute_txid()).collect()
    };

    // Every tx, every time, stamped with the sync time; nothing evicted.
    for _ in 0..2 {
        let ev = emitter.mempool_at(7).await.unwrap();
        assert_eq!(emitted(&ev), ids.iter().copied().collect());
        assert!(ev.update.iter().all(|(_, t)| *t == 7));
        assert!(ev.evicted.is_empty());
    }

    // Mined: gone from the mempool, not evicted.
    node.mine(std::slice::from_ref(&txs[0]));
    while emitter.next_block().await.unwrap().is_some() {}
    let ev = emitter.mempool_at(8).await.unwrap();
    assert_eq!(emitted(&ev), ids[1..].iter().copied().collect());
    assert!(ev.evicted.is_empty(), "a mined tx is not evicted");

    // Double-spent (replaced) and dropped: both evicted, once.
    let replacement = pay(coinbase(&node, 2), ScriptBuf::new_op_return([9]), 900);
    let rid = node.add_to_mempool(replacement);
    node.remove_from_mempool(ids[2]);
    let ev = emitter.mempool_at(9).await.unwrap();
    assert_eq!(emitted(&ev), BTreeSet::from([rid]));
    let evicted: BTreeSet<Txid> = ev.evicted.iter().map(|(t, _)| *t).collect();
    assert_eq!(evicted, BTreeSet::from([ids[1], ids[2]]));
    assert!(ev.evicted.iter().all(|(_, t)| *t == 9));
    assert!(emitter.mempool_at(10).await.unwrap().evicted.is_empty());
}

/// Behind the tip, a tx that left the mempool may be in a block not emitted
/// yet: no eviction until the emitter catches up.
#[tokio::test]
async fn evictions_wait_for_the_tip() {
    let node = MockNode::regtest();
    node.mine_empty(5);
    let tx = pay(coinbase(&node, 1), ScriptBuf::new_op_return([1]), 1000);
    let mut emitter = Emitter::new(node.clone(), genesis_cp(&node), 0, [tx.clone()]);
    node.mine(std::slice::from_ref(&tx));
    let ev = emitter.mempool().await.unwrap();
    assert!(ev.evicted.is_empty(), "not at the tip yet");
    while emitter.next_block().await.unwrap().is_some() {}
    assert!(
        emitter.mempool().await.unwrap().evicted.is_empty(),
        "it was mined"
    );
}

#[tokio::test]
async fn backend_errors_surface_and_recover() {
    let node = MockNode::regtest();
    node.mine_empty(3);
    let mut emitter = Emitter::new(node.clone(), genesis_cp(&node), 0, NO_EXPECTED_MEMPOOL_TXS);
    node.fail_next("block", TransportError::Timeout.into());
    assert_eq!(
        emitter.next_block().await.unwrap_err(),
        NodeAPIError::Transport(TransportError::Timeout)
    );
    let mut heights = Vec::new();
    while let Some(ev) = emitter.next_block().await.unwrap() {
        heights.push(ev.block_height());
    }
    assert_eq!(heights, [1, 2, 3], "no block skipped or repeated");
}

const EXTERNAL: &str = "tr([83737d5e/86'/1'/0']tpubDDR5GgtoxS8fJyjjvdahN4VzV5DV6jtbcyvVXhEKq2XtpxjxBXmxH3r8QrNbQqHg4bJM1EGkxi7Pjfkgnui9jQWqS7kxHvX6rhUeriLDKxz/0/*)";
const INTERNAL: &str = "tr([83737d5e/86'/1'/0']tpubDDR5GgtoxS8fJyjjvdahN4VzV5DV6jtbcyvVXhEKq2XtpxjxBXmxH3r8QrNbQqHg4bJM1EGkxi7Pjfkgnui9jQWqS7kxHvX6rhUeriLDKxz/1/*)";

/// Brings `wallet` up to date: blocks, then the mempool.
async fn sync_wallet(wallet: &mut Wallet, emitter: &mut Emitter<MockNode>) {
    while let Some(ev) = emitter.next_block().await.unwrap() {
        wallet
            .apply_block_connected_to(&ev.block, ev.block_height(), ev.connected_to())
            .unwrap();
    }
    let mempool = emitter.mempool().await.unwrap();
    wallet.apply_unconfirmed_txs(mempool.update);
    wallet.apply_evicted_txs(mempool.evicted);
}

#[tokio::test]
async fn bdk_wallet_through_payment_reorg_and_eviction() {
    let node = MockNode::regtest();
    node.mine_empty(101);
    let mut wallet = Wallet::create(EXTERNAL, INTERNAL)
        .network(Network::Regtest)
        .create_wallet_no_persist()
        .unwrap();
    let spk = wallet
        .reveal_next_address(KeychainKind::External)
        .script_pubkey();
    let mut emitter = Emitter::new(
        node.clone(),
        wallet.latest_checkpoint(),
        0,
        NO_EXPECTED_MEMPOOL_TXS,
    );
    sync_wallet(&mut wallet, &mut emitter).await;
    assert_eq!(wallet.balance(), Balance::default());

    // Paid, unconfirmed: pending.
    let tx = pay(coinbase(&node, 1), spk.clone(), 50_000);
    node.add_to_mempool(tx.clone());
    sync_wallet(&mut wallet, &mut emitter).await;
    assert_eq!(wallet.balance().untrusted_pending, Amount::from_sat(50_000));

    // Mined: confirmed.
    node.mine(std::slice::from_ref(&tx));
    sync_wallet(&mut wallet, &mut emitter).await;
    assert_eq!(wallet.balance().confirmed, Amount::from_sat(50_000));
    assert_eq!(wallet.latest_checkpoint().height(), 102);

    // Its block reorged out: back to the mempool, pending again.
    node.reorg(1, 2);
    sync_wallet(&mut wallet, &mut emitter).await;
    let b = wallet.balance();
    assert_eq!(b.confirmed, Amount::ZERO);
    assert_eq!(b.untrusted_pending, Amount::from_sat(50_000));
    assert_eq!(wallet.latest_checkpoint().height(), 103);

    // Double-spent away: evicted, and the balance goes with it.
    node.add_to_mempool(pay(
        coinbase(&node, 1),
        ScriptBuf::new_op_return([0]),
        40_000,
    ));
    sync_wallet(&mut wallet, &mut emitter).await;
    assert_eq!(wallet.balance(), Balance::default());
}
