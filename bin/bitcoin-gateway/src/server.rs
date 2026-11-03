//! Listeners: TLS, the loopback rule, graceful shutdown.
//!
//! Every listener (gRPC, HTTP, metrics) is an `axum::Router` served by one
//! accept loop: hyper's auto builder speaks HTTP/1.1 and HTTP/2 (gRPC needs
//! the latter), with TLS in front when `[tls]` is set.
use crate::config::TlsConfig;
use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

/// How long calls in flight get to finish on shutdown.
pub const GRACE: Duration = Duration::from_secs(10);

/// Refuses a non-loopback address without TLS, unless `insecure` (TLS
/// terminated by a proxy in front).
pub fn check_listen(name: &str, addr: SocketAddr, tls: bool, insecure: bool) -> Result<(), String> {
    if addr.ip().is_loopback() || tls || insecure {
        Ok(())
    } else {
        Err(format!(
            "refusing to serve {name} on {addr} without TLS: add [tls], listen on a \
             loopback address, or pass --insecure if a proxy terminates TLS"
        ))
    }
}

/// The certificate and key, reloadable on `SIGHUP`.
pub struct Tls {
    cert: PathBuf,
    key: PathBuf,
    acceptor: RwLock<TlsAcceptor>,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tls")
            .field("cert", &self.cert)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl Tls {
    /// Loads `[tls]`.
    pub fn load(c: &TlsConfig) -> Result<Arc<Tls>, String> {
        Ok(Arc::new(Tls {
            acceptor: RwLock::new(acceptor(&c.cert, &c.key)?),
            cert: c.cert.clone(),
            key: c.key.clone(),
        }))
    }

    /// Reads the files again; on error the old certificate stays.
    pub fn reload(&self) -> Result<(), String> {
        let a = acceptor(&self.cert, &self.key)?;
        *self.acceptor.write().unwrap_or_else(|e| e.into_inner()) = a;
        Ok(())
    }

    fn acceptor(&self) -> TlsAcceptor {
        self.acceptor
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

fn acceptor(cert: &Path, key: &Path) -> Result<TlsAcceptor, String> {
    let certs = CertificateDer::pem_file_iter(cert)
        .and_then(|i| i.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("{}: {e}", cert.display()))?;
    if certs.is_empty() {
        return Err(format!("{}: no certificate", cert.display()));
    }
    let key = PrivateKeyDer::from_pem_file(key).map_err(|e| format!("{}: {e}", key.display()))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .and_then(|b| b.with_no_client_auth().with_single_cert(certs, key))
        .map_err(|e| format!("tls: {e}"))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// What one listener lets clients hold (`[limits]`).
#[derive(Debug, Clone)]
pub struct ServeLimits {
    /// Open connections; more are closed on accept.
    pub max_connections: usize,
    /// Open connections from one IP address.
    pub max_connections_per_ip: usize,
    /// For the TLS handshake, the first bytes of a connection, and each
    /// HTTP/1.1 request's headers (idle keep-alive included).
    pub handshake_timeout: Duration,
    /// Concurrent HTTP/2 streams per connection.
    pub http2_max_streams: u32,
    /// Counts connections closed on accept.
    pub rejected: Arc<AtomicU64>,
}

impl Default for ServeLimits {
    /// `[limits]`'s defaults.
    fn default() -> Self {
        let l = crate::config::LimitsConfig::default();
        ServeLimits {
            max_connections: l.max_connections,
            max_connections_per_ip: l.max_connections_per_ip,
            handshake_timeout: Duration::from_secs(l.handshake_timeout_secs),
            http2_max_streams: l.http2_max_streams,
            rejected: Arc::default(),
        }
    }
}

/// HTTP/2 keep-alive: a ping this often, and the connection is dropped if
/// the answer doesn't come within [`PING_TIMEOUT`].
const PING_EVERY: Duration = Duration::from_secs(30);
const PING_TIMEOUT: Duration = Duration::from_secs(20);

/// [`serve_with`] and `[limits]`'s defaults.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<Tls>>,
    shutdown: CancellationToken,
) {
    serve_with(listener, app, tls, shutdown, ServeLimits::default()).await
}

/// Serves `app` until `shutdown`, then stops accepting and gives open
/// connections [`GRACE`] to finish. Upgraded connections (WebSocket) and
/// event streams watch `shutdown` themselves.
///
/// A connection over `limits` is closed at once. One that hasn't finished
/// its TLS handshake or sent a byte within `handshake_timeout` is closed,
/// and so is an HTTP/1.1 connection whose next request's headers don't
/// arrive in time.
pub async fn serve_with(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<Tls>>,
    shutdown: CancellationToken,
    limits: ServeLimits,
) {
    let graceful = GracefulShutdown::new();
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.handshake_timeout);
    builder
        .http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(limits.http2_max_streams)
        .keep_alive_interval(PING_EVERY)
        .keep_alive_timeout(PING_TIMEOUT);
    let slots = Arc::new(Semaphore::new(limits.max_connections));
    let per_ip: Arc<Mutex<HashMap<IpAddr, usize>>> = Arc::default();
    loop {
        let (stream, peer) = tokio::select! {
            r = listener.accept() => match r {
                Ok(c) => c,
                Err(e) => {
                    // Out of file descriptors, usually: wait, don't spin.
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = shutdown.cancelled() => break,
        };
        let Ok(slot) = slots.clone().try_acquire_owned() else {
            limits.rejected.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(%peer, "connection refused: max_connections");
            continue;
        };
        let Some(ip_slot) = IpSlot::take(&per_ip, peer.ip(), limits.max_connections_per_ip) else {
            limits.rejected.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(%peer, "connection refused: max_connections_per_ip");
            continue;
        };
        let svc = TowerToHyperService::new(app.clone());
        let builder = builder.clone();
        let watcher = graceful.watcher();
        let tls = tls.as_ref().map(|t| t.acceptor());
        let first_bytes = limits.handshake_timeout;
        tokio::spawn(async move {
            let _held = (slot, ip_slot);
            let result = match tls {
                Some(acceptor) => {
                    let s = match tokio::time::timeout(first_bytes, acceptor.accept(stream)).await {
                        Ok(Ok(s)) => s,
                        Ok(Err(e)) => {
                            tracing::debug!(%peer, error = %e, "tls handshake failed");
                            return;
                        }
                        Err(_) => {
                            tracing::debug!(%peer, "tls handshake timed out");
                            return;
                        }
                    };
                    let (io, read) = FirstRead::new(s);
                    let conn = builder.serve_connection_with_upgrades(TokioIo::new(io), svc);
                    until_silent(watcher.watch(conn.into_owned()), read, first_bytes).await
                }
                None => {
                    let (io, read) = FirstRead::new(stream);
                    let conn = builder.serve_connection_with_upgrades(TokioIo::new(io), svc);
                    until_silent(watcher.watch(conn.into_owned()), read, first_bytes).await
                }
            };
            match result {
                Some(Err(e)) => {
                    tracing::debug!(%peer, error = %e, "connection ended with an error")
                }
                None => tracing::debug!(%peer, "closed: sent nothing"),
                Some(Ok(())) => {}
            }
        });
    }
    drop(listener);
    tokio::select! {
        _ = graceful.shutdown() => {}
        _ = tokio::time::sleep(GRACE) => tracing::warn!("calls still in flight after the grace period"),
    }
}

/// Runs `conn`, unless no byte arrives within `wait`: then drops it, which
/// closes the connection. (hyper's own timeouts start only once it knows
/// the protocol, and it learns that from the first bytes.)
async fn until_silent<F: Future>(
    conn: F,
    read: Arc<AtomicBool>,
    wait: Duration,
) -> Option<F::Output> {
    let silent = async {
        tokio::time::sleep(wait).await;
        if read.load(Ordering::Relaxed) {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        r = conn => Some(r),
        _ = silent => None,
    }
}

/// One connection's place in the per-IP count, given back on drop.
struct IpSlot {
    map: Arc<Mutex<HashMap<IpAddr, usize>>>,
    ip: IpAddr,
}

impl IpSlot {
    fn take(map: &Arc<Mutex<HashMap<IpAddr, usize>>>, ip: IpAddr, max: usize) -> Option<IpSlot> {
        let mut m = map.lock().unwrap_or_else(|e| e.into_inner());
        let n = m.entry(ip).or_insert(0);
        if *n >= max {
            return None;
        }
        *n += 1;
        Some(IpSlot {
            map: map.clone(),
            ip,
        })
    }
}

impl Drop for IpSlot {
    fn drop(&mut self) {
        let mut m = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = m.get_mut(&self.ip) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.ip);
            }
        }
    }
}

/// An I/O stream that notes when its first byte has been read.
struct FirstRead<T> {
    inner: T,
    read: Arc<AtomicBool>,
}

impl<T> FirstRead<T> {
    fn new(inner: T) -> (FirstRead<T>, Arc<AtomicBool>) {
        let read = Arc::new(AtomicBool::new(false));
        (
            FirstRead {
                inner,
                read: read.clone(),
            },
            read,
        )
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for FirstRead<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        if buf.filled().len() > before {
            self.read.store(true, Ordering::Relaxed);
        }
        r
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for FirstRead<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_rule() {
        let a = |s: &str| s.parse::<SocketAddr>().unwrap();
        assert!(check_listen("http", a("127.0.0.1:1"), false, false).is_ok());
        assert!(check_listen("http", a("[::1]:1"), false, false).is_ok());
        assert!(check_listen("http", a("0.0.0.0:1"), false, false).is_err());
        assert!(check_listen("http", a("192.168.1.2:1"), false, false).is_err());
        assert!(check_listen("http", a("0.0.0.0:1"), true, false).is_ok());
        assert!(check_listen("http", a("0.0.0.0:1"), false, true).is_ok());
    }
}
