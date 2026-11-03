//! A `PubSocket` in the test stands in for Core.
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, Txid};
use bitcoin_core_zmq::{Topic, ZmqEvents, ZmqStream};
use bitcoin_node_api::{ApiResult, EventSource, NodeAPIError, NodeEvent};
use std::future::poll_fn;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;
use zeromq::{PubSocket, Socket, SocketSend, ZmqMessage};

const HASH: &str = "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206";

async fn next(s: &mut ZmqStream) -> ApiResult<NodeEvent> {
    let ev = tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut *s), cx)),
    );
    ev.await.expect("an event within 5s").expect("stream open")
}

/// Whatever is already buffered, without waiting for more.
async fn drain(s: &mut ZmqStream) {
    tokio::time::sleep(Duration::from_millis(100)).await;
    while tokio::time::timeout(
        Duration::from_millis(10),
        poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut *s), cx)),
    )
    .await
    .is_ok()
    {}
}

/// Core's frames. `hash` in display order, as Core sends it.
fn msg(topic: &str, body: Vec<u8>, seq: u32) -> ZmqMessage {
    let mut m = ZmqMessage::from(topic.to_owned());
    m.push_back(body.into());
    m.push_back(seq.to_le_bytes().to_vec().into());
    m
}

fn seq_body(hash: [u8; 32], label: u8, mempool_seq: Option<u64>) -> Vec<u8> {
    let mut b = hash.to_vec();
    b.push(label);
    if let Some(s) = mempool_seq {
        b.extend(s.to_le_bytes());
    }
    b
}

fn display(h: [u8; 32]) -> [u8; 32] {
    let mut h = h;
    h.reverse();
    h
}

async fn publisher() -> (PubSocket, String) {
    let mut p = PubSocket::new();
    let ep = p.bind("tcp://127.0.0.1:0").await.unwrap();
    (p, ep.to_string())
}

/// Publishes seq 0 until it arrives (ZMQ drops messages sent before the
/// subscription lands), then drains. The test's own messages start at seq 1.
async fn join(p: &mut PubSocket, s: &mut ZmqStream, topic: Topic) {
    let warmup = match topic {
        Topic::Sequence => msg("sequence", seq_body([0; 32], b'C', None), 0),
        Topic::HashBlock => msg("hashblock", vec![0; 32], 0),
    };
    for _ in 0..250 {
        p.send(warmup.clone()).await.unwrap();
        let got = tokio::time::timeout(
            Duration::from_millis(20),
            poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut *s), cx)),
        );
        if got.await.is_ok() {
            drain(s).await;
            return;
        }
    }
    panic!("subscription never landed");
}

async fn setup(topic: Topic, capacity: usize) -> (PubSocket, String, ZmqStream) {
    let (mut p, ep) = publisher().await;
    let zmq = ZmqEvents::connect(&ep, topic)
        .await
        .unwrap()
        .with_capacity(capacity);
    let mut s = zmq.subscribe().await.unwrap();
    join(&mut p, &mut s, topic).await;
    (p, ep, s)
}

#[tokio::test]
async fn sequence_labels_and_byte_order() {
    let (mut p, _, mut s) = setup(Topic::Sequence, 64).await;
    let block = BlockHash::from_str(HASH).unwrap();
    let txid = Txid::from_str(HASH).unwrap();
    let wire = display(block.to_byte_array());

    p.send(msg("sequence", seq_body(wire, b'C', None), 1))
        .await
        .unwrap();
    p.send(msg("sequence", seq_body(wire, b'D', None), 2))
        .await
        .unwrap();
    p.send(msg("sequence", seq_body(wire, b'A', Some(7)), 3))
        .await
        .unwrap();
    p.send(msg("sequence", seq_body(wire, b'R', Some(8)), 4))
        .await
        .unwrap();

    assert_eq!(
        next(&mut s).await.unwrap(),
        NodeEvent::BlockConnected(block)
    );
    assert_eq!(
        next(&mut s).await.unwrap(),
        NodeEvent::BlockDisconnected(block)
    );
    assert_eq!(
        next(&mut s).await.unwrap(),
        NodeEvent::TxAdded {
            txid,
            mempool_sequence: Some(7)
        }
    );
    assert_eq!(
        next(&mut s).await.unwrap(),
        NodeEvent::TxRemoved {
            txid,
            mempool_sequence: Some(8)
        }
    );
}

#[tokio::test]
async fn hashblock_topic() {
    let (mut p, _, mut s) = setup(Topic::HashBlock, 64).await;
    let block = BlockHash::from_str(HASH).unwrap();
    p.send(msg("hashblock", display(block.to_byte_array()).to_vec(), 1))
        .await
        .unwrap();
    assert_eq!(
        next(&mut s).await.unwrap(),
        NodeEvent::BlockConnected(block)
    );
}

#[tokio::test]
async fn counter_gap_is_lagged() {
    let (mut p, _, mut s) = setup(Topic::Sequence, 64).await;
    let c = |seq| msg("sequence", seq_body([1; 32], b'C', None), seq);
    p.send(c(1)).await.unwrap();
    p.send(c(5)).await.unwrap();
    assert!(matches!(
        next(&mut s).await,
        Ok(NodeEvent::BlockConnected(_))
    ));
    assert_eq!(next(&mut s).await.unwrap(), NodeEvent::Lagged);
    assert!(matches!(
        next(&mut s).await,
        Ok(NodeEvent::BlockConnected(_))
    ));
}

#[tokio::test]
async fn counter_wraps_without_lag() {
    let (mut p, _, mut s) = setup(Topic::Sequence, 64).await;
    let c = |seq| msg("sequence", seq_body([1; 32], b'C', None), seq);
    // Re-anchor the counter near the top: one Lagged for the jump.
    p.send(c(u32::MAX)).await.unwrap();
    p.send(c(0)).await.unwrap();
    assert_eq!(next(&mut s).await.unwrap(), NodeEvent::Lagged);
    assert!(matches!(
        next(&mut s).await,
        Ok(NodeEvent::BlockConnected(_))
    ));
    assert!(matches!(
        next(&mut s).await,
        Ok(NodeEvent::BlockConnected(_))
    ));
}

#[tokio::test]
async fn slow_consumer_gets_one_lagged() {
    let (mut p, _, mut s) = setup(Topic::Sequence, 2).await;
    for seq in 1..=10 {
        p.send(msg("sequence", seq_body([1; 32], b'C', None), seq))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Two fit; 3..=10 were dropped. The `Lagged` follows as soon as there's
    // room, with nothing more published.
    assert!(matches!(
        next(&mut s).await,
        Ok(NodeEvent::BlockConnected(_))
    ));
    assert!(matches!(
        next(&mut s).await,
        Ok(NodeEvent::BlockConnected(_))
    ));
    assert_eq!(next(&mut s).await.unwrap(), NodeEvent::Lagged);
    p.send(msg("sequence", seq_body([2; 32], b'C', None), 11))
        .await
        .unwrap();
    assert_eq!(
        next(&mut s).await.unwrap(),
        NodeEvent::BlockConnected(BlockHash::from_byte_array([2; 32]))
    );
}

#[tokio::test]
async fn malformed_is_an_error_then_continues() {
    let (mut p, _, mut s) = setup(Topic::Sequence, 64).await;
    p.send(msg("sequence", vec![1; 10], 1)).await.unwrap();
    p.send(msg("sequence", seq_body([1; 32], b'A', None), 2))
        .await
        .unwrap();
    p.send(msg("sequence", seq_body([1; 32], b'C', None), 3))
        .await
        .unwrap();
    assert!(matches!(next(&mut s).await, Err(NodeAPIError::Decode(_))));
    assert!(matches!(next(&mut s).await, Err(NodeAPIError::Decode(_))));
    assert!(matches!(
        next(&mut s).await,
        Ok(NodeEvent::BlockConnected(_))
    ));
}

#[tokio::test]
async fn publisher_restart_is_lagged() {
    let (p, ep, mut s) = setup(Topic::Sequence, 64).await;
    p.close().await;
    let mut p = PubSocket::new();
    // Same port, as a restarted Core would.
    let mut bound = None;
    for _ in 0..50 {
        match p.bind(&ep).await {
            Ok(e) => {
                bound = Some(e);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    bound.expect("rebind");
    // A restarted Core counts from 0 again.
    let c = |seq| msg("sequence", seq_body([3; 32], b'C', None), seq);
    let mut events = Vec::new();
    for seq in 0..250 {
        p.send(c(seq)).await.unwrap();
        if let Ok(Some(ev)) = tokio::time::timeout(
            Duration::from_millis(20),
            poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut s), cx)),
        )
        .await
        {
            events.push(ev.unwrap());
            break;
        }
    }
    assert_eq!(
        events,
        [NodeEvent::Lagged],
        "Lagged before the first new event"
    );
}

#[tokio::test]
async fn connect_fails_without_a_publisher() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let err = ZmqEvents::connect(&format!("tcp://127.0.0.1:{port}"), Topic::Sequence)
        .await
        .unwrap_err();
    assert!(matches!(err, NodeAPIError::Transport(_)), "{err}");
}
