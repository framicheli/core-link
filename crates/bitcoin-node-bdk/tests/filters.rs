//! `bip158::FilterIter` on the mock: only matching blocks are fetched, the
//! chain follows reorgs, and a `bdk_wallet` synced by filters agrees with
//! one synced block by block.
use bdk_chain::local_chain::LocalChain;
use bdk_wallet::{KeychainKind, Update, Wallet};
use bitcoin::{
    Amount, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness, absolute,
    transaction,
};
use bitcoin_node_api::{ChainSource, NodeAPIError};
use bitcoin_node_bdk::bip158::{FilterError, FilterIter};
use bitcoin_node_bdk::{Emitter, NO_EXPECTED_MEMPOOL_TXS};
use bitcoin_node_mock::MockNode;

fn pay(prev: OutPoint, spk: ScriptBuf, sats: u64) -> Transaction {
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
            value: Amount::from_sat(sats),
            script_pubkey: spk,
        }],
    }
}

fn coinbase(node: &MockNode, height: u64) -> OutPoint {
    OutPoint {
        txid: node.block_at(height).unwrap().txdata[0].compute_txid(),
        vout: 0,
    }
}

fn watched(n: u8) -> ScriptBuf {
    ScriptBuf::new_p2wsh(&bitcoin::WScriptHash::from_raw_hash(
        bitcoin::hashes::Hash::hash(&[n]),
    ))
}

/// Scans to the tip; (height, matched) per event, applying each to `chain`.
async fn scan(iter: &mut FilterIter<MockNode>, chain: &mut LocalChain) -> Vec<(u32, bool)> {
    let mut out = Vec::new();
    while let Some(ev) = iter.next().await.unwrap() {
        chain
            .apply_update(ev.cp.clone())
            .expect("each event connects");
        out.push((ev.height(), ev.is_match()));
    }
    out
}

#[tokio::test]
async fn mock_filters_match_paid_and_spent_scripts() {
    let node = MockNode::regtest();
    node.mine_empty(3);
    let a = pay(coinbase(&node, 1), watched(1), 5_000);
    let a_out = OutPoint {
        txid: a.compute_txid(),
        vout: 0,
    };
    let paid = node.mine(&[a]);
    // Spending it: the filter holds the spent script, not just outputs.
    let spent = node.mine(&[pay(a_out, ScriptBuf::new_op_return([0]), 4_000)]);
    let empty = node.mine_empty(1)[0];
    let hit = async |h: bitcoin::BlockHash| {
        node.block_filter(h)
            .await
            .unwrap()
            .match_any(&h, std::iter::once(watched(1).as_bytes()))
            .unwrap()
    };
    assert!(hit(paid).await, "pays the script");
    assert!(hit(spent).await, "spends the script");
    assert!(!hit(empty).await);
}

#[tokio::test]
async fn fetches_only_matching_blocks_and_follows_reorgs() {
    let node = MockNode::regtest();
    node.mine_empty(20);
    let mut chain = LocalChain::from_genesis_hash(node.block_at(0).unwrap().block_hash()).0;
    let mut iter = FilterIter::new(node.clone(), chain.tip(), [watched(1), watched(2)]);

    node.mine(&[pay(coinbase(&node, 1), watched(1), 1_000)]); // 21
    node.mine_empty(2); // 22, 23
    node.mine(&[pay(coinbase(&node, 2), watched(2), 1_000)]); // 24
    let events = scan(&mut iter, &mut chain).await;
    assert_eq!(events.len(), 24);
    let matched: Vec<u32> = events.iter().filter(|e| e.1).map(|e| e.0).collect();
    assert_eq!(matched, [21, 24]);
    assert_eq!(chain.tip().height(), 24);
    assert!(iter.next().await.unwrap().is_none(), "at the tip");

    // Reorg out 23 and 24 (24's tx returns to the mempool), mine 3 more:
    // the scan resumes after 22 and replaces what was reorged out.
    node.reorg(2, 3);
    let events = scan(&mut iter, &mut chain).await;
    assert_eq!(
        events,
        [(23, false), (24, false), (25, false)],
        "the payment isn't mined in the new branch"
    );
    for cp in chain.tip().iter() {
        assert_eq!(
            cp.hash(),
            node.block_hash(u64::from(cp.height())).await.unwrap()
        );
    }

    // Onto a shorter chain.
    node.reorg(3, 1);
    assert_eq!(scan(&mut iter, &mut chain).await, [(23, false)]);
    assert_eq!(chain.tip().height(), 23);
}

#[tokio::test]
async fn no_filters_and_no_common_ancestor() {
    let node = MockNode::regtest();
    node.mine_empty(2);
    let genesis = node.block_at(0).unwrap().block_hash();
    let cp = bdk_core::CheckPoint::new(bdk_core::BlockId {
        height: 0,
        hash: genesis,
    });
    node.fail_next(
        "block_filter",
        NodeAPIError::Unsupported("start Core with -blockfilterindex".into()),
    );
    let mut iter = FilterIter::new(node.clone(), cp, [watched(1)]);
    assert!(matches!(
        iter.next().await,
        Err(FilterError::Node(NodeAPIError::Unsupported(_)))
    ));
    // A checkpoint from another chain.
    let other = bdk_core::CheckPoint::new(bdk_core::BlockId {
        height: 0,
        hash: bitcoin::constants::genesis_block(Network::Bitcoin).block_hash(),
    });
    let mut iter = FilterIter::new(node, other, [watched(1)]);
    assert!(matches!(
        iter.next().await,
        Err(FilterError::NoCommonAncestor)
    ));
}

const EXTERNAL: &str = "tr([83737d5e/86'/1'/0']tpubDDR5GgtoxS8fJyjjvdahN4VzV5DV6jtbcyvVXhEKq2XtpxjxBXmxH3r8QrNbQqHg4bJM1EGkxi7Pjfkgnui9jQWqS7kxHvX6rhUeriLDKxz/0/*)";
const INTERNAL: &str = "tr([83737d5e/86'/1'/0']tpubDDR5GgtoxS8fJyjjvdahN4VzV5DV6jtbcyvVXhEKq2XtpxjxBXmxH3r8QrNbQqHg4bJM1EGkxi7Pjfkgnui9jQWqS7kxHvX6rhUeriLDKxz/1/*)";

fn wallet() -> Wallet {
    Wallet::create(EXTERNAL, INTERNAL)
        .network(Network::Regtest)
        .create_wallet_no_persist()
        .unwrap()
}

/// A wallet synced by filters ends where one synced block by block does.
#[tokio::test]
async fn filter_sync_matches_block_sync() {
    let node = MockNode::regtest();
    node.mine_empty(30);
    let mut by_filter = wallet();
    let mut by_block = wallet();
    let spks: Vec<ScriptBuf> = (0..3)
        .map(|_| {
            let a = by_filter.reveal_next_address(KeychainKind::External);
            by_block.reveal_next_address(KeychainKind::External);
            a.script_pubkey()
        })
        .collect();
    for (i, spk) in spks.iter().enumerate() {
        node.mine_empty(4);
        node.mine(&[pay(coinbase(&node, i as u64 + 1), spk.clone(), 10_000)]);
    }
    node.mine_empty(2);
    node.reorg(1, 2);

    let mut iter = FilterIter::new(node.clone(), by_filter.latest_checkpoint(), spks);
    let mut fetched = 0;
    while let Some(ev) = iter.next().await.unwrap() {
        match &ev.block {
            Some(block) => {
                fetched += 1;
                by_filter
                    .apply_block_connected_to(block, ev.height(), ev.connected_to())
                    .unwrap()
            }
            None => by_filter
                .apply_update(Update {
                    chain: Some(ev.cp.clone()),
                    ..Default::default()
                })
                .unwrap(),
        }
    }
    let mut emitter = Emitter::new(
        node.clone(),
        by_block.latest_checkpoint(),
        0,
        NO_EXPECTED_MEMPOOL_TXS,
    );
    while let Some(ev) = emitter.next_block().await.unwrap() {
        by_block
            .apply_block_connected_to(&ev.block, ev.block_height(), ev.connected_to())
            .unwrap();
    }

    assert_eq!(fetched, 3, "only the three paying blocks");
    assert_eq!(by_filter.balance().confirmed, Amount::from_sat(30_000));
    assert_eq!(by_filter.balance(), by_block.balance());
    assert_eq!(
        by_filter.latest_checkpoint().block_id(),
        by_block.latest_checkpoint().block_id()
    );
    let tip = node.tip().await.unwrap();
    assert_eq!(
        u64::from(by_filter.latest_checkpoint().height()),
        tip.height
    );
}
