//! Prometheus metrics, `/healthz` and `/readyz`, on their own listener.
//!
//! The text format is written by hand: a few counters, gauges and one
//! histogram don't need a client library.
use crate::error::ApiError;
use crate::fanout::FanoutStats;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Latency buckets, in seconds.
const BUCKETS: [f64; 12] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0,
];

#[derive(Debug, Default, Clone)]
struct Histogram {
    counts: [u64; BUCKETS.len()],
    sum: f64,
    count: u64,
}

impl Histogram {
    fn observe(&mut self, secs: f64) {
        for (i, b) in BUCKETS.iter().enumerate() {
            if secs <= *b {
                self.counts[i] += 1;
            }
        }
        self.sum += secs;
        self.count += 1;
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// (key, method, outcome) -> count
    requests: BTreeMap<(String, String, String), u64>,
    /// method -> latency
    durations: BTreeMap<String, Histogram>,
    /// kind -> count
    upstream_errors: BTreeMap<String, u64>,
}

/// The gateway's counters.
#[derive(Debug, Default)]
pub struct Metrics {
    inner: Mutex<Inner>,
    /// Core answered the last readiness check.
    pub core_reachable: AtomicBool,
    /// Connections closed on accept by `[limits]`; shared with the listeners.
    pub connections_rejected: Arc<AtomicU64>,
}

impl Metrics {
    /// Records one request.
    pub fn request(&self, key: &str, method: &str, elapsed: Duration, error: Option<&ApiError>) {
        let outcome = error.map_or("ok", |e| e.code.as_str());
        let mut m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        *m.requests
            .entry((key.to_owned(), method.to_owned(), outcome.to_owned()))
            .or_default() += 1;
        m.durations
            .entry(method.to_owned())
            .or_default()
            .observe(elapsed.as_secs_f64());
        if let Some(e) = error {
            let kind = match e.source {
                "transport" => Some(e.code.as_str()),
                s @ ("rpc" | "decode" | "unsupported") => Some(s),
                _ => None, // the client's fault, not the node's
            };
            if let Some(k) = kind {
                *m.upstream_errors.entry(k.to_owned()).or_default() += 1;
            }
        }
    }

    /// The Prometheus text exposition.
    /// `node_in_flight`: calls to Core running now.
    pub fn render(&self, fanout: &FanoutStats, node_in_flight: i64) -> String {
        let m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = String::new();
        let o = &mut out;
        help(
            o,
            "gateway_requests_total",
            "counter",
            "Requests by key, method and outcome.",
        );
        for ((key, method, outcome), n) in &m.requests {
            let _ = writeln!(
                o,
                "gateway_requests_total{{key=\"{}\",method=\"{}\",outcome=\"{}\"}} {n}",
                esc(key),
                esc(method),
                esc(outcome)
            );
        }
        help(
            o,
            "gateway_request_duration_seconds",
            "histogram",
            "Request latency by method.",
        );
        for (method, h) in &m.durations {
            let method = esc(method);
            for (b, c) in BUCKETS.iter().zip(h.counts) {
                let _ = writeln!(
                    o,
                    "gateway_request_duration_seconds_bucket{{method=\"{method}\",le=\"{b}\"}} {c}"
                );
            }
            let _ = writeln!(
                o,
                "gateway_request_duration_seconds_bucket{{method=\"{method}\",le=\"+Inf\"}} {}",
                h.count
            );
            let _ = writeln!(
                o,
                "gateway_request_duration_seconds_sum{{method=\"{method}\"}} {}",
                h.sum
            );
            let _ = writeln!(
                o,
                "gateway_request_duration_seconds_count{{method=\"{method}\"}} {}",
                h.count
            );
        }
        help(
            o,
            "gateway_upstream_errors_total",
            "counter",
            "Errors from Core, by kind.",
        );
        for (kind, n) in &m.upstream_errors {
            let _ = writeln!(
                o,
                "gateway_upstream_errors_total{{kind=\"{}\"}} {n}",
                esc(kind)
            );
        }
        let gauge = |o: &mut String, name: &str, kind: &str, text: &str, v: i64| {
            help(o, name, kind, text);
            let _ = writeln!(o, "{name} {v}");
        };
        gauge(
            o,
            "gateway_core_reachable",
            "gauge",
            "1 if Core answered the last readiness check.",
            self.core_reachable.load(Ordering::Relaxed) as i64,
        );
        gauge(
            o,
            "gateway_event_subscribers",
            "gauge",
            "Event subscribers now.",
            fanout.subscribers.load(Ordering::Relaxed),
        );
        gauge(
            o,
            "gateway_lagged_sent_total",
            "counter",
            "Lagged events delivered to subscribers.",
            fanout.lagged_sent.load(Ordering::Relaxed) as i64,
        );
        gauge(
            o,
            "gateway_fanout_resubscriptions_total",
            "counter",
            "Upstream event subscriptions reopened after an end or error.",
            fanout.resubscriptions.load(Ordering::Relaxed) as i64,
        );
        gauge(
            o,
            "gateway_node_calls_in_flight",
            "gauge",
            "Calls to Core running now (at most [limits] node_max_in_flight).",
            node_in_flight,
        );
        gauge(
            o,
            "gateway_connections_rejected_total",
            "counter",
            "Connections closed on accept: max_connections or max_connections_per_ip reached.",
            self.connections_rejected.load(Ordering::Relaxed) as i64,
        );
        out
    }
}

fn help(o: &mut String, name: &str, kind: &str, text: &str) {
    let _ = writeln!(o, "# HELP {name} {text}");
    let _ = writeln!(o, "# TYPE {name} {kind}");
}

/// A label value, escaped.
fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders() {
        let m = Metrics::default();
        m.request("k\"1", "Chain/Tip", Duration::from_millis(3), None);
        m.request(
            "k\"1",
            "Chain/Tip",
            Duration::from_millis(30),
            Some(&ApiError::rate_limited()),
        );
        let e: ApiError =
            bitcoin_node_api::NodeAPIError::Transport(bitcoin_node_api::TransportError::Timeout)
                .into();
        m.request("k", "Chain/Block", Duration::from_secs(1), Some(&e));
        let s = m.render(&FanoutStats::default(), 3);
        assert!(s.contains(
            "gateway_requests_total{key=\"k\\\"1\",method=\"Chain/Tip\",outcome=\"ok\"} 1"
        ));
        assert!(s.contains("outcome=\"rate_limited\"} 1"));
        assert!(s.contains(
            "gateway_request_duration_seconds_bucket{method=\"Chain/Tip\",le=\"0.005\"} 1"
        ));
        assert!(s.contains(
            "gateway_request_duration_seconds_bucket{method=\"Chain/Tip\",le=\"+Inf\"} 2"
        ));
        assert!(s.contains("gateway_upstream_errors_total{kind=\"timeout\"} 1"));
        // a rate limit is not the node's fault
        assert!(!s.contains("kind=\"rate\""));
        assert!(s.contains("gateway_core_reachable 0"));
        assert!(s.contains("gateway_node_calls_in_flight 3"));
        assert!(s.contains("gateway_connections_rejected_total 0"));
    }
}
