//! Bitcoin Core ZMQ notifications as a `bitcoin-node-api` event stream.
//!
//! Point it at Core's `-zmqpubsequence` endpoint. `Client::get_zmq_notifications()`
//! lists it, but as configured: a `tcp://0.0.0.0:...` bind must be rewritten
//! to the node's address.
//!
//! ```no_run
//! # async fn f() -> bitcoin_node_api::ApiResult<()> {
//! use bitcoin_core_zmq::{Topic, ZmqEvents};
//! use bitcoin_node_api::EventSource;
//!
//! let zmq = ZmqEvents::connect("tcp://127.0.0.1:28333", Topic::Sequence).await?;
//! let events = zmq.subscribe().await?;
//! // Now sync: `ChainSource::tip()`, and for a mempool copy `getrawmempool`
//! // with `mempool_sequence`. Skip tx events below that sequence: it's the
//! // next one Core assigns, so the first event after the snapshot carries it.
//! # Ok(()) }
//! ```
//!
//! ZMQ drops what is published before a subscription reaches Core, so sync
//! after `subscribe()` returns, never before. Later losses (a gap in Core's
//! message counter, a reconnect, a consumer too slow for the channel) arrive
//! as one `NodeEvent::Lagged`: resync the same way. The reader never blocks
//! on the consumer, so Core's send buffer doesn't overflow because of it.
#![warn(missing_docs)]
/// A `tracing::debug!` event with the `tracing` feature on; nothing without
/// it. Only endpoints, topics and causes: never message contents.
macro_rules! trace_event {
    ($($t:tt)*) => {
        #[cfg(feature = "tracing")]
        tracing::debug!($($t)*);
    };
}

use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, Txid};
use bitcoin_node_api::{ApiResult, EventSource, NodeAPIError, NodeEvent, TransportError};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use zeromq::{Socket, SocketEvent, SocketOptions, SocketRecv, SubSocket, ZmqMessage};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// ~40 bytes per event; room for a reorg's worth of re-added txs.
const CAPACITY: usize = 4096;

/// What the endpoint publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topic {
    /// `-zmqpubsequence`: block connect/disconnect, tx add/remove.
    Sequence,
    /// `-zmqpubhashblock`: block connects only.
    HashBlock,
}

impl Topic {
    fn name(self) -> &'static str {
        match self {
            Topic::Sequence => "sequence",
            Topic::HashBlock => "hashblock",
        }
    }
}

/// An `EventSource` for one Core ZMQ endpoint; each `subscribe()` opens its own socket.
#[derive(Debug, Clone)]
pub struct ZmqEvents {
    endpoint: String,
    topic: Topic,
    capacity: usize,
}

impl ZmqEvents {
    /// Checks that `endpoint` (`tcp://host:port`) accepts a connection within 5s.
    pub async fn connect(endpoint: &str, topic: Topic) -> ApiResult<ZmqEvents> {
        let sock = open(endpoint).await?;
        sock.close().await;
        Ok(ZmqEvents {
            endpoint: endpoint.to_owned(),
            topic,
            capacity: CAPACITY,
        })
    }

    /// Events buffered per subscriber before it lags. Default 4096.
    pub fn with_capacity(mut self, capacity: usize) -> ZmqEvents {
        self.capacity = capacity.max(1);
        self
    }
}

async fn open(endpoint: &str) -> ApiResult<SubSocket> {
    let mut opts = SocketOptions::default();
    opts.connect_timeout(CONNECT_TIMEOUT);
    let mut sock = SubSocket::with_options(opts);
    sock.connect(endpoint)
        .await
        .map_err(|e| TransportError::Connect(format!("{endpoint}: {e}")))?;
    Ok(sock)
}

/// `subscribe()`'s stream: one socket and reader task per subscriber.
#[derive(Debug)]
pub struct ZmqStream(mpsc::Receiver<ApiResult<NodeEvent>>);

impl Stream for ZmqStream {
    type Item = ApiResult<NodeEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

impl EventSource for ZmqEvents {
    type EventStream = ZmqStream;

    async fn subscribe(&self) -> ApiResult<ZmqStream> {
        let mut sock = open(&self.endpoint).await?;
        let monitor = sock.monitor();
        sock.subscribe(self.topic.name())
            .await
            .map_err(|e| TransportError::Other(e.to_string()))?;
        trace_event!(endpoint = %self.endpoint, topic = self.topic.name(), "zmq connected");
        let (tx, rx) = mpsc::channel(self.capacity);
        tokio::spawn(read(sock, monitor, tx, self.topic));
        Ok(ZmqStream(rx))
    }
}

/// Forwards decoded messages until the subscriber drops the stream.
async fn read(
    mut sock: SubSocket,
    mut monitor: impl Stream<Item = SocketEvent> + Unpin,
    tx: mpsc::Sender<ApiResult<NodeEvent>>,
    topic: Topic,
) {
    let mut out = Out {
        tx,
        lagged: false,
        last_seq: None,
    };
    let mut monitoring = true;
    loop {
        // A second handle, so waiting on the channel doesn't borrow `out`.
        let tx = out.tx.clone();
        tokio::select! {
            _ = tx.closed() => break,
            // An owed `Lagged` goes out as soon as there's room, not only with
            // the next message: after a burst, Core may stay quiet for minutes.
            permit = tx.reserve(), if out.lagged => {
                if let Ok(p) = permit {
                    p.send(Ok(NodeEvent::Lagged));
                    out.lagged = false;
                }
            }
            ev = std::future::poll_fn(|cx| Pin::new(&mut monitor).poll_next(cx)), if monitoring => {
                match ev {
                    // `zeromq` reconnects by itself; anything published meanwhile is gone.
                    Some(SocketEvent::Disconnected(_)) => {
                        trace_event!(topic = topic.name(), "zmq disconnected");
                        out.lost("reconnect");
                    }
                    Some(_) => {}
                    None => monitoring = false,
                }
            }
            msg = sock.recv() => match msg {
                Ok(m) => {
                    // Counted even when the body is bad: it's still Core's message.
                    if let Some(seq) = counter(&m) {
                        out.seq(seq);
                    }
                    out.send(decode(topic, &m));
                }
                // The peer went away mid-read; the reconnect task takes over.
                Err(_) => {
                    out.lost("receive error");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        }
    }
    sock.close().await;
}

struct Out {
    tx: mpsc::Sender<ApiResult<NodeEvent>>,
    /// A `Lagged` is owed before the next event.
    lagged: bool,
    /// Core's per-topic message counter; `None` after a reconnect.
    last_seq: Option<u32>,
}

impl Out {
    /// Owes a `Lagged`; `cause` is logged once per lag, not per lost message.
    fn owe(&mut self, cause: &'static str) {
        if !self.lagged {
            trace_event!(cause, "zmq lagged");
        }
        #[cfg(not(feature = "tracing"))]
        let _ = cause;
        self.lagged = true;
    }

    fn lost(&mut self, cause: &'static str) {
        self.owe(cause);
        self.last_seq = None;
        self.flush();
    }

    fn seq(&mut self, seq: u32) {
        if self
            .last_seq
            .is_some_and(|last| seq != last.wrapping_add(1))
        {
            self.owe("sequence gap");
        }
        self.last_seq = Some(seq);
    }

    /// Delivers an owed `Lagged`; `false` if the channel is still full.
    fn flush(&mut self) -> bool {
        if self.lagged && self.tx.try_send(Ok(NodeEvent::Lagged)).is_err() {
            return false;
        }
        self.lagged = false;
        true
    }

    /// Never waits: a full channel drops `ev` and owes a `Lagged`.
    fn send(&mut self, ev: ApiResult<NodeEvent>) {
        if !self.flush() {
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(ev) {
            self.owe("full channel");
        }
    }
}

/// Core's per-topic message counter: the last of `[topic, body, seq]`.
fn counter(m: &ZmqMessage) -> Option<u32> {
    let seq = m.get(2).filter(|_| m.len() == 3)?;
    Some(u32::from_le_bytes(seq[..].try_into().ok()?))
}

/// The body of `[topic, body, seq]`. Hashes come in display order, so reversed.
fn decode(topic: Topic, m: &ZmqMessage) -> ApiResult<NodeEvent> {
    let bad = |what: &str| NodeAPIError::Decode(format!("zmq {}: {what}", topic.name()));
    let (Some(body), 3) = (m.get(1), m.len()) else {
        return Err(bad("expected 3 frames"));
    };
    if counter(m).is_none() {
        return Err(bad("4-byte counter"));
    }
    let hash = |b: &[u8]| -> ApiResult<[u8; 32]> {
        let mut h: [u8; 32] = b.try_into().map_err(|_| bad("32-byte hash"))?;
        h.reverse();
        Ok(h)
    };
    let mempool_seq = |b: &[u8]| -> ApiResult<u64> {
        Ok(u64::from_le_bytes(
            b.try_into().map_err(|_| bad("8-byte mempool sequence"))?,
        ))
    };
    let ev = match topic {
        Topic::HashBlock => NodeEvent::BlockConnected(BlockHash::from_byte_array(hash(body)?)),
        Topic::Sequence => {
            if body.len() < 33 {
                return Err(bad("short body"));
            }
            let (h, label, rest) = (hash(&body[..32])?, body[32], &body[33..]);
            match (label, rest.len()) {
                (b'C', 0) => NodeEvent::BlockConnected(BlockHash::from_byte_array(h)),
                (b'D', 0) => NodeEvent::BlockDisconnected(BlockHash::from_byte_array(h)),
                (b'A', 8) => NodeEvent::TxAdded {
                    txid: Txid::from_byte_array(h),
                    mempool_sequence: Some(mempool_seq(rest)?),
                },
                (b'R', 8) => NodeEvent::TxRemoved {
                    txid: Txid::from_byte_array(h),
                    mempool_sequence: Some(mempool_seq(rest)?),
                },
                _ => {
                    return Err(bad(&format!(
                        "label {:?}, {} trailing bytes",
                        label as char,
                        rest.len()
                    )));
                }
            }
        }
    };
    Ok(ev)
}
