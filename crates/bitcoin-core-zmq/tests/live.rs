//! `ZmqEvents` on a real `bitcoind` with `-zmqpubsequence`. `#[ignore]`:
//! `cargo test -- --ignored` (see `test-node` for which binary).
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::{Address, Amount, CompressedPublicKey, Network, OutPoint, PrivateKey, Transaction};
use bitcoin_core_rpc::{Auth, Client};
use bitcoin_core_zmq::{Topic, ZmqEvents, ZmqStream};
use bitcoin_node_api::{EventSource, NodeEvent};
use std::future::poll_fn;
use std::pin::Pin;
use std::time::Duration;
use test_node::{RPC_PASSWORD, RPC_USER, TestNode, free_port};

async fn next(s: &mut ZmqStream) -> NodeEvent {
    let ev = tokio::time::timeout(
        Duration::from_secs(10),
        poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut *s), cx)),
    );
    ev.await
        .expect("an event within 10s")
        .expect("stream open")
        .expect("decodes")
}

/// Reads until `want`, failing on `Lagged`. Returns what came before it.
async fn until(s: &mut ZmqStream, want: NodeEvent) -> Vec<NodeEvent> {
    let mut seen = Vec::new();
    loop {
        match next(s).await {
            NodeEvent::Lagged => panic!("lagged; before it: {seen:?}"),
            ev if ev == want => return seen,
            ev => seen.push(ev),
        }
    }
}

/// Spends `prev` (worth `value`) back to `addr`, paying `fee`.
async fn spend(
    c: &Client,
    key: PrivateKey,
    addr: &Address,
    prev: OutPoint,
    value: Amount,
    fee: Amount,
) -> Transaction {
    let tx = c
        .create_raw_transaction(&[prev], &[(addr.clone(), value - fee)], None, None)
        .await
        .unwrap();
    let signed = c
        .sign_raw_transaction_with_key(&tx, &[key], &[])
        .await
        .unwrap();
    assert!(signed.complete, "{:?}", signed.errors);
    signed.tx
}

#[tokio::test]
#[ignore = "needs bitcoind"]
async fn sequence_events_from_core() {
    let zmq_port = free_port();
    let endpoint = format!("tcp://127.0.0.1:{zmq_port}");
    let node = TestNode::start(&[&format!("-zmqpubsequence={endpoint}")]);
    let auth = Auth::UserPass(RPC_USER.into(), RPC_PASSWORD.into());
    let mut c = None;
    for _ in 0..100 {
        match Client::connect(&node.rpc_url(), auth.clone()).await {
            Ok(client) => {
                c = Some(client);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let c = c.expect("bitcoind came up");
    eprintln!("Core {}", c.version().0);

    let secp = Secp256k1::new();
    let key = PrivateKey::new(
        SecretKey::from_slice(&[0x11; 32]).unwrap(),
        Network::Regtest,
    );
    let cpk = CompressedPublicKey::from_private_key(&secp, &key).unwrap();
    let addr = Address::p2wpkh(&cpk, Network::Regtest);

    let zmq = ZmqEvents::connect(&endpoint, Topic::Sequence)
        .await
        .unwrap();
    let mut s = zmq.subscribe().await.unwrap();
    // Mine until the subscription has landed, then sync as the docs say.
    let mut joined = false;
    for _ in 0..50 {
        c.generate_to_address(1, &addr).await.unwrap();
        if tokio::time::timeout(
            Duration::from_millis(100),
            poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut s), cx)),
        )
        .await
        .is_ok()
        {
            joined = true;
            break;
        }
    }
    assert!(joined, "no event after 50 blocks");
    let snapshot = c.get_raw_mempool_sequence().await.unwrap();

    // Coinbase 1 matures at 101.
    let tip = c.generate_to_address(101, &addr).await.unwrap().0[100];
    until(&mut s, NodeEvent::BlockConnected(tip)).await;
    let cb = &c
        .get_block(c.get_block_hash(1).await.unwrap())
        .await
        .unwrap()
        .txdata[0];
    let prev = OutPoint {
        txid: cb.compute_txid(),
        vout: 0,
    };
    let value = cb.output[0].value;

    // Added, then replaced: the original is removed (R), the replacement added (A).
    let tx1 = spend(&c, key, &addr, prev, value, Amount::from_sat(1_000)).await;
    let txid1 = c.send_raw_transaction(&tx1, None).await.unwrap();
    let tx2 = spend(&c, key, &addr, prev, value, Amount::from_sat(10_000)).await;
    let txid2 = c.send_raw_transaction(&tx2, None).await.unwrap();

    let mut seqs = Vec::new();
    for (want_txid, removed) in [(txid1, false), (txid1, true), (txid2, false)] {
        let ev = next(&mut s).await;
        let (txid, seq) = match ev {
            NodeEvent::TxAdded {
                txid,
                mempool_sequence,
            } if !removed => (txid, mempool_sequence),
            NodeEvent::TxRemoved {
                txid,
                mempool_sequence,
            } if removed => (txid, mempool_sequence),
            other => panic!(
                "expected {} {want_txid}, got {other:?}",
                if removed { "removal" } else { "add" }
            ),
        };
        assert_eq!(txid, want_txid);
        let seq = seq.expect("ZMQ tx events carry the mempool sequence");
        // The snapshot's value is the next one Core assigns.
        assert!(
            seq >= snapshot.mempool_sequence,
            "{seq} before the getrawmempool snapshot {}",
            snapshot.mempool_sequence
        );
        seqs.push(seq);
    }
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "increasing: {seqs:?}");

    // Mined: a connect, and no removal for the mined tx.
    let block = c.generate_to_address(1, &addr).await.unwrap().0[0];
    assert_eq!(next(&mut s).await, NodeEvent::BlockConnected(block));

    // Invalidated: a disconnect, and the tx is back in the mempool.
    c.invalidate_block(block).await.unwrap();
    assert_eq!(next(&mut s).await, NodeEvent::BlockDisconnected(block));
    loop {
        match next(&mut s).await {
            NodeEvent::TxAdded { txid, .. } if txid == txid2 => break,
            NodeEvent::Lagged => panic!("lagged"),
            _ => {}
        }
    }
}
