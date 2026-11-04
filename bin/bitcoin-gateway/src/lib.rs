//! `bitcoin-gateway`: gRPC, REST and WebSocket in front of Bitcoin Core,
//! behind per-key permissions. See the README's gateway section.
//!
//! The binary is `main.rs`; this library holds everything else so the
//! integration tests can run the gateway in-process against
//! `bitcoin-node-mock`.
#![warn(missing_docs)]
pub mod api;
pub mod auth;
pub mod config;
pub mod error;
pub mod fanout;
pub mod grpc;
pub mod metrics;
pub mod node;
pub mod rest;
pub mod scopes;
pub mod server;
pub mod ws;

use crate::auth::KeyStore;
use crate::config::{EventsConfig, GrpcConfig, HttpConfig, KeySpec, LimitsConfig, WsConfig};
use crate::fanout::Fanout;
use crate::metrics::Metrics;
use crate::node::{Backend, Upstream};
use crate::server::ServeLimits;
use axum::Router;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// How often the gateway asks Core whether it is ready.
const READINESS_EVERY: Duration = Duration::from_secs(5);
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};

/// The config sections the front ends read.
#[derive(Debug, Clone, Default)]
pub struct Settings {
    /// `[http]`.
    pub http: HttpConfig,
    /// `[grpc]`.
    pub grpc: GrpcConfig,
    /// `[ws]`.
    pub ws: WsConfig,
    /// `[events]`.
    pub events: EventsConfig,
    /// `[limits]`.
    pub limits: LimitsConfig,
}

/// Everything a request handler needs. Generic over the backend so tests run
/// it on `bitcoin-node-mock`.
pub struct Gateway<N> {
    /// The trait API's backend.
    pub node: N,
    /// Untyped calls: WebSocket pass-through, readiness.
    pub upstream: Arc<dyn Upstream>,
    /// API keys.
    pub keys: KeyStore,
    /// The shared event feed.
    pub fanout: Fanout,
    /// Front-end settings.
    pub settings: Settings,
    /// Fires on `SIGTERM`.
    pub shutdown: CancellationToken,
    /// Whether Core answers and is out of initial block download.
    pub health: watch::Sender<bool>,
    /// Counters for `/metrics`.
    pub metrics: Metrics,
    /// Calls to Core at once, all keys together (`[limits]
    /// node_max_in_flight`). Readiness checks and the event feed don't
    /// take one, so they get through when clients fill them.
    pub node_slots: Arc<Semaphore>,
}

/// What a call to Core holds while it runs: one of its key's slots and one
/// of the node's.
#[derive(Debug)]
pub struct CallSlots {
    _key: OwnedSemaphorePermit,
    _node: OwnedSemaphorePermit,
}

impl<N> std::fmt::Debug for Gateway<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway").finish_non_exhaustive()
    }
}

impl<N: Backend> Gateway<N> {
    /// A gateway over `node`.
    pub fn new(
        node: N,
        upstream: Arc<dyn Upstream>,
        keys: Vec<KeySpec>,
        settings: Settings,
        shutdown: CancellationToken,
    ) -> Arc<Gateway<N>> {
        let fanout = Fanout::new(node.clone(), settings.events.buffer, shutdown.clone());
        let node_slots = Arc::new(Semaphore::new(settings.limits.node_max_in_flight));
        let gw = Arc::new(Gateway {
            node_slots,
            node,
            upstream,
            keys: KeyStore::new(keys),
            fanout,
            settings,
            shutdown,
            health: watch::Sender::new(false),
            metrics: Metrics::default(),
        });
        gw.spawn_readiness(READINESS_EVERY);
        gw
    }

    /// Slots for one call to Core: the key's at once (none free: rate
    /// limited), then the node's, waiting up to `[limits] node_queue_secs`
    /// (none free: unavailable).
    pub async fn call_slots(&self, key: &auth::Key) -> Result<CallSlots, error::ApiError> {
        let k = key.call_slot()?;
        let wait = Duration::from_secs(self.settings.limits.node_queue_secs);
        let busy = || error::ApiError::unavailable("the node is busy: no free call slot");
        let n = tokio::time::timeout(wait, self.node_slots.clone().acquire_owned())
            .await
            .map_err(|_| busy())?
            .map_err(|_| busy())?;
        Ok(CallSlots { _key: k, _node: n })
    }

    /// The listeners' limits, from `[limits]`.
    pub fn serve_limits(&self) -> ServeLimits {
        let l = &self.settings.limits;
        ServeLimits {
            max_connections: l.max_connections,
            max_connections_per_ip: l.max_connections_per_ip,
            handshake_timeout: Duration::from_secs(l.handshake_timeout_secs),
            http2_max_streams: l.http2_max_streams,
            rejected: self.metrics.connections_rejected.clone(),
        }
    }

    /// Asks Core whether it is ready: it answers and is out of initial
    /// block download. Updates the gRPC health status and the metric.
    pub async fn check_ready(&self) -> Result<(), String> {
        let r = match self
            .upstream
            .call(None, "getblockchaininfo", serde_json::json!([]))
            .await
        {
            Ok(v) if v["initialblockdownload"].as_bool() == Some(true) => {
                self.metrics.core_reachable.store(true, Ordering::Relaxed);
                Err("Core is in initial block download".to_owned())
            }
            Ok(_) => {
                self.metrics.core_reachable.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.metrics.core_reachable.store(false, Ordering::Relaxed);
                Err(format!("Core is unreachable: {e}"))
            }
        };
        self.health.send_if_modified(|h| {
            let changed = *h != r.is_ok();
            *h = r.is_ok();
            changed
        });
        r
    }

    fn spawn_readiness(self: &Arc<Self>, every: Duration) {
        let gw = Arc::downgrade(self);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                let Some(g) = gw.upgrade() else { return };
                if let Err(e) = g.check_ready().await {
                    tracing::warn!("not ready: {e}");
                }
                drop(g);
                tokio::select! {
                    _ = tokio::time::sleep(every) => {}
                    _ = shutdown.cancelled() => return,
                }
            }
        });
    }

    /// The HTTP listener's routes: REST under `/v1`, WebSocket at `/ws`.
    pub fn http_router(self: &Arc<Self>) -> Router {
        let router = rest::router(self.clone()).merge(ws::router(self.clone()));
        if self.settings.http.cors_origins.is_empty() {
            router
        } else {
            router.layer(cors(&self.settings.http.cors_origins))
        }
    }

    /// The metrics listener's routes: `/metrics`, `/healthz` (the process
    /// is up) and `/readyz` (Core answers and is out of initial block
    /// download).
    pub fn metrics_router(self: &Arc<Self>) -> Router {
        use axum::http::{StatusCode, header};
        use axum::routing::get;
        let gw = self.clone();
        let ready = self.clone();
        Router::new()
            .route("/healthz", get(|| async { "ok\n" }))
            .route(
                "/readyz",
                get(move || async move {
                    match ready.check_ready().await {
                        Ok(()) => (StatusCode::OK, "ready\n".to_owned()),
                        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, format!("{e}\n")),
                    }
                }),
            )
            .route(
                "/metrics",
                get(move || async move {
                    (
                        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                        gw.metrics.render(
                            gw.fanout.stats(),
                            (gw.settings.limits.node_max_in_flight
                                - gw.node_slots.available_permits())
                                as i64,
                        ),
                    )
                }),
            )
    }
}

/// CORS for `origins` only; no headers at all when empty.
pub(crate) fn cors(origins: &[String]) -> CorsLayer {
    let list: Vec<_> = origins.iter().filter_map(|o| o.parse().ok()).collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(list))
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any)
        .expose_headers(tower_http::cors::Any)
}
