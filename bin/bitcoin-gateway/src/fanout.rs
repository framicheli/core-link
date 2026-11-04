//! One upstream event subscription, copied to every client.
//!
//! Without it, every subscriber would open its own ZMQ socket or hold one of
//! Core's `rpcthreads` with its own `waitfornewblock` poll.
//!
//! - A subscriber that falls behind gets `Lagged` and keeps going.
//! - Upstream `Lagged` is forwarded to everyone.
//! - If the upstream stream ends or errors, the fan-out resubscribes with
//!   backoff and sends everyone `Lagged`, since events may have been lost.
use bitcoin_node_api::{EventSource, NodeEvent};
use futures_core::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, broadcast};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

const MIN_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Counters for `/metrics`.
#[derive(Debug, Default)]
pub struct FanoutStats {
    /// Upstream subscriptions opened, the first included.
    pub upstream_subscriptions: AtomicU64,
    /// Upstream subscriptions reopened after an end or error.
    pub resubscriptions: AtomicU64,
    /// `Lagged` events delivered to subscribers.
    pub lagged_sent: AtomicU64,
    /// Subscribers now.
    pub subscribers: AtomicI64,
}

type Start = Box<dyn FnOnce(broadcast::Sender<NodeEvent>) + Send>;

struct Inner {
    tx: broadcast::Sender<NodeEvent>,
    /// The upstream task, started by the first subscriber.
    start: Mutex<Option<Start>>,
    stats: Arc<FanoutStats>,
    shutdown: CancellationToken,
}

/// The shared event feed. Cheap to clone.
#[derive(Clone)]
pub struct Fanout(Arc<Inner>);

impl std::fmt::Debug for Fanout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fanout").finish_non_exhaustive()
    }
}

impl Fanout {
    /// A feed over `source`, buffering `buffer` events per subscriber. The
    /// upstream subscription opens with the first subscriber; streams end
    /// when `shutdown` fires.
    pub fn new<E>(source: E, buffer: usize, shutdown: CancellationToken) -> Fanout
    where
        E: EventSource + Send + Sync + 'static,
    {
        let (tx, _) = broadcast::channel(buffer.max(1));
        let stats = Arc::new(FanoutStats::default());
        let (st, token) = (stats.clone(), shutdown.clone());
        let start: Start = Box::new(move |tx| {
            tokio::spawn(upstream(source, tx, st, token));
        });
        Fanout(Arc::new(Inner {
            tx,
            start: Mutex::new(Some(start)),
            stats,
            shutdown,
        }))
    }

    /// A new subscriber: every event from now on.
    pub fn subscribe(&self) -> Subscription {
        let rx = self.0.tx.subscribe();
        if let Some(start) = self
            .0
            .start
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            start(self.0.tx.clone());
        }
        self.0.stats.subscribers.fetch_add(1, Ordering::Relaxed);
        Subscription {
            rx: BroadcastStream::new(rx),
            stats: self.0.stats.clone(),
            shutdown: Box::pin(self.0.shutdown.clone().cancelled_owned()),
            _slot: None,
        }
    }

    /// The counters.
    pub fn stats(&self) -> &Arc<FanoutStats> {
        &self.0.stats
    }
}

async fn upstream<E: EventSource>(
    source: E,
    tx: broadcast::Sender<NodeEvent>,
    stats: Arc<FanoutStats>,
    shutdown: CancellationToken,
) {
    let mut backoff = MIN_BACKOFF;
    // Events may have been lost since subscribers joined.
    let mut lost = false;
    loop {
        let sub = tokio::select! {
            s = source.subscribe() => s,
            _ = shutdown.cancelled() => return,
        };
        match sub {
            Ok(mut stream) => {
                let n = stats.upstream_subscriptions.fetch_add(1, Ordering::Relaxed);
                if n > 0 {
                    stats.resubscriptions.fetch_add(1, Ordering::Relaxed);
                }
                if lost {
                    let _ = tx.send(NodeEvent::Lagged);
                }
                backoff = MIN_BACKOFF;
                loop {
                    let item = tokio::select! {
                        i = stream.next() => i,
                        _ = shutdown.cancelled() => return,
                    };
                    match item {
                        Some(Ok(ev)) => {
                            // No receivers is fine: nobody is listening now.
                            let _ = tx.send(ev);
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "event stream failed; resubscribing");
                            break;
                        }
                        None => {
                            tracing::warn!("event stream ended; resubscribing");
                            break;
                        }
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, ?backoff, "event subscribe failed"),
        }
        lost = true;
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.cancelled() => return,
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// One subscriber's events. Ends when the gateway shuts down.
pub struct Subscription {
    rx: BroadcastStream<NodeEvent>,
    stats: Arc<FanoutStats>,
    shutdown: Pin<Box<WaitForCancellationFutureOwned>>,
    _slot: Option<OwnedSemaphorePermit>,
}

impl Subscription {
    /// Keeps `slot` (a key's subscription slot) until the subscription ends.
    pub fn holding(mut self, slot: OwnedSemaphorePermit) -> Subscription {
        self._slot = Some(slot);
        self
    }
}

impl Stream for Subscription {
    type Item = NodeEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<NodeEvent>> {
        if self.shutdown.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        let ev = match Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(Some(Ok(ev))) => ev,
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(_)))) => NodeEvent::Lagged,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };
        if ev == NodeEvent::Lagged {
            self.stats.lagged_sent.fetch_add(1, Ordering::Relaxed);
        }
        Poll::Ready(Some(ev))
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.stats.subscribers.fetch_sub(1, Ordering::Relaxed);
    }
}
