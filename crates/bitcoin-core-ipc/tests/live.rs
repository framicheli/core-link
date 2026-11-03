//! `IpcClient` on a real `bitcoin-node -ipcbind=unix`. `#[ignore]`:
//! `cargo test -- --ignored`. Skips before Core 30, which has no IPC.
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::{
    Address, Amount, Block, CompressedPublicKey, Network, OutPoint, PrivateKey, Script,
    TxMerkleNode,
};
use bitcoin_core_ipc::{IpcClient, TipEvents};
use bitcoin_core_rpc::{Auth, Client};
use bitcoin_node_api::{
    ChainSource, EventSource, NodeAPIError, NodeEvent, RpcErrorCode, Solution, Template,
    TemplateSource, WaitOptions,
};
use std::future::poll_fn;
use std::pin::Pin;
use std::time::{Duration, Instant};
use test_node::{RPC_PASSWORD, RPC_USER, TestNode};

const FEE: Amount = Amount::from_sat(10_000);

/// A solution for `t` whose proof of work is valid (`valid`) or not.
fn solve(t: &Template, payout: &Script, valid: bool) -> Solution {
    let mut sol = Solution {
        version: t.version,
        time: t.cur_time,
        nonce: 0,
        coinbase: t.coinbase_tx(payout, &[1]),
    };
    let target = t.header(&sol).target();
    while target.is_met_by(t.header(&sol).block_hash()) != valid {
        sol.nonce += 1;
    }
    sol
}

/// A `bitcoin-node` with IPC and an RPC client on it; `None` before Core 30.
async fn start() -> Option<(TestNode, Client)> {
    let Some(node) = TestNode::start_ipc(&[]) else {
        eprintln!("skipped: this Core release has no bitcoin-node (IPC is Core 30+)");
        return None;
    };
    let auth = Auth::UserPass(RPC_USER.into(), RPC_PASSWORD.into());
    for _ in 0..100 {
        match Client::connect(&node.rpc_url(), auth.clone()).await {
            Ok(c) => return Some((node, c)),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    panic!("bitcoin-node came up");
}

async fn next(s: &mut TipEvents) -> NodeEvent {
    let ev = tokio::time::timeout(
        Duration::from_secs(10),
        poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut *s), cx)),
    );
    ev.await
        .expect("an event within 10s")
        .expect("stream open")
        .expect("no error")
}

#[tokio::test]
#[ignore = "needs bitcoin-node (Core 30+)"]
async fn tip_events_over_ipc() {
    let Some((node, rpc)) = start().await else {
        return;
    };
    let addr = Address::p2wpkh(
        &CompressedPublicKey::from_private_key(
            &Secp256k1::new(),
            &PrivateKey::new(
                SecretKey::from_slice(&[0x11; 32]).unwrap(),
                Network::Regtest,
            ),
        )
        .unwrap(),
        Network::Regtest,
    );
    rpc.generate_to_address(1, &addr).await.unwrap();
    let ipc = IpcClient::connect(node.ipc_socket()).await.unwrap();
    let mut events = ipc.subscribe().await.unwrap();

    // One block on the tip: a connect.
    let mined = rpc.generate_to_address(1, &addr).await.unwrap().0[0];
    assert_eq!(next(&mut events).await, NodeEvent::BlockConnected(mined));

    // The tip goes back a block: IPC can't name the disconnect, so `Lagged`.
    rpc.invalidate_block(mined).await.unwrap();
    assert_eq!(next(&mut events).await, NodeEvent::Lagged);

    // And forward again: a connect.
    rpc.reconsider_block(mined).await.unwrap();
    assert_eq!(next(&mut events).await, NodeEvent::BlockConnected(mined));

    // Nothing else is reported while the tip stays put.
    let quiet = tokio::time::timeout(
        Duration::from_millis(500),
        poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut events), cx)),
    );
    assert!(quiet.await.is_err(), "no event without a tip change");
}

#[tokio::test]
#[ignore = "needs bitcoin-node (Core 30+)"]
async fn mining_over_ipc() {
    let Some((node, rpc)) = start().await else {
        return;
    };
    let major = rpc.version().major();
    eprintln!("Core {}", rpc.version().0);

    let secp = Secp256k1::new();
    let key = PrivateKey::new(
        SecretKey::from_slice(&[0x11; 32]).unwrap(),
        Network::Regtest,
    );
    let addr = Address::p2wpkh(
        &CompressedPublicKey::from_private_key(&secp, &key).unwrap(),
        Network::Regtest,
    );
    let payout = addr.script_pubkey();
    rpc.generate_to_address(101, &addr).await.unwrap();

    let ipc = IpcClient::connect(node.ipc_socket())
        .await
        .expect("connect to node.sock");
    assert_eq!(ipc.core_major(), major, "protocol picked by probing");

    // ChainSource: only the tip.
    let tip = ipc.tip().await.unwrap();
    assert_eq!(tip.hash, rpc.get_best_block_hash().await.unwrap());
    assert_eq!(tip.height, 101);
    assert!(matches!(
        ipc.block_hash(1).await,
        Err(NodeAPIError::Unsupported(_))
    ));

    // A template on the tip, empty mempool.
    let start = Instant::now();
    let t = ipc.template().await.unwrap();
    eprintln!(
        "template(), first: {:?} (creates Core threads)",
        start.elapsed()
    );
    // Warm, the same trait over IPC and over RPC (`getblocktemplate`), for
    // scale. Reported, not asserted: an empty regtest mempool says little.
    let (mut over_ipc, mut over_rpc) = (Duration::ZERO, Duration::ZERO);
    for _ in 0..20 {
        let start = Instant::now();
        ipc.template().await.unwrap();
        over_ipc += start.elapsed();
        let start = Instant::now();
        TemplateSource::template(&rpc).await.unwrap();
        over_rpc += start.elapsed();
    }
    eprintln!(
        "template(), warm average: {:?} over IPC, {:?} over RPC",
        over_ipc / 20,
        over_rpc / 20
    );
    assert_eq!(t.prev_hash, tip.hash);
    assert_eq!(t.height, 102, "from the BIP34 prefix");
    assert_eq!(t.coinbase.reward_remaining, Amount::from_int_btc(50));
    assert!(ipc.transactions(&t).await.unwrap().is_empty());

    // A short wait with nothing new: `None`.
    let quiet = WaitOptions {
        timeout: Some(Duration::from_millis(200)),
        fee_threshold: None,
    };
    assert_eq!(ipc.wait_next(&t, quiet).await.unwrap(), None);

    // A tx enters the mempool: wait_next sees the fee.
    let cb1 = &rpc
        .get_block(rpc.get_block_hash(1).await.unwrap())
        .await
        .unwrap()
        .txdata[0];
    let prev = OutPoint {
        txid: cb1.compute_txid(),
        vout: 0,
    };
    let unsigned = rpc
        .create_raw_transaction(
            &[prev],
            &[(addr.clone(), cb1.output[0].value - FEE)],
            None,
            None,
        )
        .await
        .unwrap();
    let tx = rpc
        .sign_raw_transaction_with_key(&unsigned, &[key], &[])
        .await
        .unwrap()
        .tx;
    let txid = rpc.send_raw_transaction(&tx, None).await.unwrap();
    let rising = WaitOptions {
        timeout: Some(Duration::from_secs(30)),
        fee_threshold: Some(Amount::from_sat(1)),
    };
    let t = ipc
        .wait_next(&t, rising)
        .await
        .unwrap()
        .expect("a template with the new fee");
    let start = Instant::now();
    let txs = ipc.transactions(&t).await.unwrap();
    eprintln!("transactions(): {:?}", start.elapsed());
    assert_eq!(txs.len(), 1);
    assert_eq!(txs[0].tx.compute_txid(), txid);
    assert_eq!(txs[0].fee, FEE);
    assert!(txs[0].sigops > 0);
    assert_eq!(t.coinbase.reward_remaining, Amount::from_int_btc(50) + FEE);

    // The merkle path agrees with the full block.
    let sol = solve(&t, &payout, true);
    let mut block = Block {
        header: Header {
            version: sol.version,
            prev_blockhash: t.prev_hash,
            merkle_root: TxMerkleNode::all_zeros(),
            time: sol.time,
            bits: t.bits,
            nonce: sol.nonce,
        },
        txdata: [sol.coinbase.clone()]
            .into_iter()
            .chain(txs.into_iter().map(|t| t.tx))
            .collect(),
    };
    block.header.merkle_root = block.compute_merkle_root().unwrap();
    assert_eq!(block.header.merkle_root, t.merkle_root(&sol.coinbase));
    assert!(
        block.check_witness_commitment(),
        "required outputs carry the commitment"
    );

    // A bad proof of work is rejected.
    let bad = solve(&t, &payout, false);
    match ipc.submit_solution(&t, &bad).await {
        Err(NodeAPIError::Rpc {
            code: RpcErrorCode::VerifyError,
            ..
        }) => {}
        other => panic!("expected a rejection, got {other:?}"),
    }

    // Submitting while a wait is pending: the submit returns first, then the
    // wait wakes up for the new tip.
    let waiter = {
        let (ipc, t) = (ipc.clone(), t.clone());
        tokio::spawn(async move {
            let next = ipc
                .wait_next(
                    &t,
                    WaitOptions {
                        timeout: Some(Duration::from_secs(30)),
                        fee_threshold: None,
                    },
                )
                .await;
            (next, Instant::now())
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiter.is_finished(), "wait_next is pending");
    let start = Instant::now();
    ipc.submit_solution(&t, &sol).await.unwrap();
    let submitted = Instant::now();
    eprintln!("submit_solution(): {:?}", submitted - start);
    let (next, woke) = waiter.await.unwrap();
    assert!(submitted <= woke, "submit returned before the wait");
    let next = next.unwrap().expect("a template on the new tip");
    assert_eq!(rpc.get_best_block_hash().await.unwrap(), block.block_hash());
    assert_eq!(next.prev_hash, block.block_hash());
    assert_eq!(next.height, 103);

    // Already known: Ok, like `submitblock`'s "duplicate".
    ipc.submit_solution(&t, &sol).await.unwrap();

    // Templates from another backend are refused, not misread.
    let other = IpcClient::connect(node.ipc_socket()).await.unwrap();
    assert!(matches!(
        other.transactions(&next).await,
        Err(NodeAPIError::Unsupported(_))
    ));
}
