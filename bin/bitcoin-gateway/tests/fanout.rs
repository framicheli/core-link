//! I3: one upstream subscription shared by every subscriber.
use bitcoin::hashes::Hash;
use bitcoin_gateway::fanout::Fanout;
use bitcoin_node_api::{ApiResult, EventSource, NodeEvent};
use bitcoin_node_mock::MockNode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

/// The mock, counting `subscribe()` calls.
#[derive(Clone)]
struct Counting(MockNode, Arc<AtomicUsize>);

impl EventSource for Counting {
    type EventStream = <MockNode as EventSource>::EventStream;
    async fn subscribe(&self) -> ApiResult<Self::EventStream> {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.0.subscribe().await
    }
}

async fn next(s: &mut (impl tokio_stream::Stream<Item = NodeEvent> + Unpin)) -> NodeEvent {
    tokio::time::timeout(Duration::from_secs(5), s.next())
        .await
        .expect("an event in time")
        .expect("stream open")
}

/// Lets the upstream task subscribe before the test produces events.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn hundred_subscribers_share_one_upstream() {
    let node = MockNode::regtest();
    let count = Arc::new(AtomicUsize::new(0));
    let fanout = Fanout::new(
        Counting(node.clone(), count.clone()),
        64,
        CancellationToken::new(),
    );
    let mut subs: Vec<_> = (0..100).map(|_| fanout.subscribe()).collect();
    settle().await;
    let hash = node.mine(&[]);
    for s in &mut subs {
        assert_eq!(next(s).await, NodeEvent::BlockConnected(hash));
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(fanout.stats().subscribers.load(Ordering::Relaxed), 100);
    drop(subs);
    assert_eq!(fanout.stats().subscribers.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn upstream_lagged_reaches_everyone() {
    let node = MockNode::regtest();
    let fanout = Fanout::new(node.clone(), 64, CancellationToken::new());
    let mut a = fanout.subscribe();
    let mut b = fanout.subscribe();
    settle().await;
    node.lag();
    assert_eq!(next(&mut a).await, NodeEvent::Lagged);
    assert_eq!(next(&mut b).await, NodeEvent::Lagged);
}

#[tokio::test]
async fn slow_subscriber_gets_lagged_and_keeps_going() {
    let node = MockNode::regtest();
    let fanout = Fanout::new(node.clone(), 4, CancellationToken::new());
    let mut fast = fanout.subscribe();
    let mut slow = fanout.subscribe();
    settle().await;
    let mut hashes = Vec::new();
    for _ in 0..10 {
        hashes.push(node.mine(&[]));
        // the fast one keeps up
        assert_eq!(
            next(&mut fast).await,
            NodeEvent::BlockConnected(*hashes.last().unwrap())
        );
    }
    // the slow one missed six and is told so, then gets the last four
    assert_eq!(next(&mut slow).await, NodeEvent::Lagged);
    for h in &hashes[6..] {
        assert_eq!(next(&mut slow).await, NodeEvent::BlockConnected(*h));
    }
    assert!(fanout.stats().lagged_sent.load(Ordering::Relaxed) >= 1);
    // and it keeps going
    let h = node.mine(&[]);
    assert_eq!(next(&mut slow).await, NodeEvent::BlockConnected(h));
}

/// Upstream streams the test can end.
#[derive(Clone, Default)]
struct Scripted(Arc<Mutex<Vec<mpsc::UnboundedSender<ApiResult<NodeEvent>>>>>);

impl EventSource for Scripted {
    type EventStream = UnboundedReceiverStream<ApiResult<NodeEvent>>;
    async fn subscribe(&self) -> ApiResult<Self::EventStream> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.lock().unwrap().push(tx);
        Ok(UnboundedReceiverStream::new(rx))
    }
}

impl Scripted {
    fn opened(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    fn send(&self, ev: ApiResult<NodeEvent>) {
        self.0.lock().unwrap().last().unwrap().send(ev).unwrap();
    }
    fn end(&self) {
        let mut v = self.0.lock().unwrap();
        let (tx, _) = mpsc::unbounded_channel();
        let last = v.len() - 1;
        v[last] = tx; // drops the live sender: the stream ends
    }
}

async fn wait_for(what: &str, f: impl Fn() -> bool) {
    for _ in 0..100 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn upstream_end_resubscribes_and_sends_lagged() {
    let up = Scripted::default();
    let fanout = Fanout::new(up.clone(), 64, CancellationToken::new());
    let mut s = fanout.subscribe();
    wait_for("first subscribe", || up.opened() == 1).await;
    let h = bitcoin::BlockHash::from_byte_array([1; 32]);
    up.send(Ok(NodeEvent::BlockConnected(h)));
    assert_eq!(next(&mut s).await, NodeEvent::BlockConnected(h));

    up.end();
    wait_for("resubscribe", || up.opened() == 2).await;
    assert_eq!(next(&mut s).await, NodeEvent::Lagged);
    up.send(Ok(NodeEvent::BlockDisconnected(h)));
    assert_eq!(next(&mut s).await, NodeEvent::BlockDisconnected(h));

    // an error item is treated like an end
    up.send(Err(bitcoin_node_api::NodeAPIError::Decode("x".into())));
    wait_for("resubscribe", || up.opened() == 3).await;
    assert_eq!(next(&mut s).await, NodeEvent::Lagged);
    assert_eq!(fanout.stats().resubscriptions.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn shutdown_ends_streams() {
    let node = MockNode::regtest();
    let token = CancellationToken::new();
    let fanout = Fanout::new(node, 64, token.clone());
    let mut s = fanout.subscribe();
    token.cancel();
    let end = tokio::time::timeout(Duration::from_secs(1), s.next()).await;
    assert_eq!(end.unwrap(), None);
}
