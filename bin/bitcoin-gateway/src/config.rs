//! The TOML config, and the checks that fail startup with the line that's wrong.
use crate::scopes::Scope;
use bitcoin::hex::FromHex;
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::{Path, PathBuf};
use toml::Spanned;

/// The whole config file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where Core is.
    pub node: NodeConfig,
    /// Listener addresses; each may be overridden on the command line.
    #[serde(default)]
    pub listen: Listen,
    /// REST and WebSocket settings.
    #[serde(default)]
    pub http: HttpConfig,
    /// gRPC settings.
    #[serde(default)]
    pub grpc: GrpcConfig,
    /// WebSocket limits.
    #[serde(default)]
    pub ws: WsConfig,
    /// Event fan-out.
    #[serde(default)]
    pub events: EventsConfig,
    /// Connection and concurrency limits.
    #[serde(default)]
    pub limits: LimitsConfig,
    /// Certificate and key for every listener.
    pub tls: Option<TlsConfig>,
    /// API keys.
    #[serde(default, rename = "key")]
    pub keys: Vec<KeyConfig>,
}

/// `[node]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// `http://127.0.0.1:8332`.
    pub rpc_url: Spanned<String>,
    /// Core's `.cookie`; or `rpc_user` and `rpc_password`.
    pub cookie: Option<PathBuf>,
    /// `-rpcuser`, or an `-rpcauth` user.
    pub rpc_user: Option<Spanned<String>>,
    /// Its password.
    pub rpc_password: Option<String>,
    /// Core's `-zmqpubsequence` endpoint; events poll RPC without it.
    pub zmq: Option<Spanned<String>>,
}

/// `[listen]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listen {
    /// gRPC.
    pub grpc: Option<SocketAddr>,
    /// REST under `/v1`, WebSocket at `/ws`.
    pub http: Option<SocketAddr>,
    /// Prometheus `/metrics`, `/healthz`, `/readyz`.
    pub metrics: Option<SocketAddr>,
}

/// `[http]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    /// Origins that get CORS headers; empty: none.
    #[serde(default)]
    pub cors_origins: Vec<String>,
    /// Immutable responses are `public` instead of `private`.
    #[serde(default)]
    pub public_cache: bool,
}

/// `[grpc]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    /// Also accept gRPC-Web, for browsers.
    #[serde(default)]
    pub web: bool,
}

/// `[ws]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WsConfig {
    /// Largest message a client may send.
    pub max_message_bytes: usize,
    /// Calls one connection may have in flight.
    pub max_in_flight: usize,
}

impl Default for WsConfig {
    fn default() -> Self {
        WsConfig {
            max_message_bytes: 1 << 20,
            max_in_flight: 16,
        }
    }
}

/// `[events]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EventsConfig {
    /// Events buffered per subscriber before it gets `Lagged`.
    pub buffer: usize,
}

impl Default for EventsConfig {
    fn default() -> Self {
        EventsConfig { buffer: 1024 }
    }
}

/// `[limits]`: what one client, one key or everyone together may hold.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LimitsConfig {
    /// Open connections per listener; more are closed on accept.
    pub max_connections: usize,
    /// Open connections per client IP address, per listener.
    pub max_connections_per_ip: usize,
    /// Seconds for a TLS handshake, and for a request's headers to arrive
    /// (an idle HTTP/1.1 connection is closed after it too).
    pub handshake_timeout_secs: u64,
    /// Concurrent HTTP/2 streams (gRPC calls) per connection.
    pub http2_max_streams: u32,
    /// Event subscriptions per key, over SSE, gRPC and WebSocket together.
    pub max_subscriptions: usize,
    /// Calls to Core per key at once; a `[[key]]` may set its own.
    pub key_max_in_flight: usize,
    /// Calls to Core at once, all keys together. Keep it below Core's
    /// `-rpcthreads` plus `-rpcworkqueue`.
    pub node_max_in_flight: usize,
    /// Seconds a call waits for one of those slots before it fails as
    /// unavailable.
    pub node_queue_secs: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        LimitsConfig {
            max_connections: 1024,
            max_connections_per_ip: 64,
            handshake_timeout_secs: 10,
            http2_max_streams: 100,
            max_subscriptions: 16,
            key_max_in_flight: 32,
            node_max_in_flight: 8,
            node_queue_secs: 10,
        }
    }
}

/// `[tls]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain.
    pub cert: PathBuf,
    /// PEM private key.
    pub key: PathBuf,
}

/// One `[[key]]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyConfig {
    /// Shown in logs and metrics.
    pub name: Spanned<String>,
    /// SHA-256 of the token, hex.
    pub token_sha256: Spanned<String>,
    /// Scopes.
    pub allow: Spanned<Vec<Spanned<String>>>,
    /// Requests per second, with bursts.
    pub rate: Option<Spanned<Rate>>,
    /// Lets an `admin` key call methods the gateway doesn't know.
    pub allow_unknown: Option<Spanned<bool>>,
    /// Calls to Core at once; default `[limits] key_max_in_flight`.
    pub max_in_flight: Option<Spanned<usize>>,
}

/// A key's rate limit: a token bucket.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rate {
    /// Sustained requests per second.
    pub per_second: f64,
    /// Requests allowed at once after a quiet spell.
    pub burst: u32,
}

/// A checked key.
#[derive(Debug, Clone, PartialEq)]
pub struct KeySpec {
    /// Its name.
    pub name: String,
    /// SHA-256 of its token.
    pub token_sha256: [u8; 32],
    /// What it may do.
    pub scopes: HashSet<Scope>,
    /// Its rate limit.
    pub rate: Option<Rate>,
    /// See [`KeyConfig::allow_unknown`].
    pub allow_unknown: bool,
    /// Calls to Core at once.
    pub max_in_flight: usize,
    /// Event subscriptions at once.
    pub max_subscriptions: usize,
}

/// A config error, with the 1-based line it's on when there is one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    /// The line.
    pub line: Option<usize>,
    /// What's wrong.
    pub message: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line {
            Some(l) => write!(f, "line {l}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Turns spans into line numbers.
struct Src<'a>(&'a str);

impl Src<'_> {
    fn err(&self, span: Range<usize>, message: impl Into<String>) -> ConfigError {
        let start = span.start.min(self.0.len());
        ConfigError {
            line: Some(self.0[..start].matches('\n').count() + 1),
            message: message.into(),
        }
    }
}

impl Config {
    /// Reads and checks `path`.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let src = std::fs::read_to_string(path).map_err(|e| ConfigError {
            line: None,
            message: format!("can't read {}: {e}", path.display()),
        })?;
        Config::parse(&src)
    }

    /// Parses and checks a config.
    pub fn parse(src: &str) -> Result<Config, ConfigError> {
        let s = Src(src);
        let config: Config = toml::from_str(src).map_err(|e| match e.span() {
            Some(span) => s.err(span, e.message()),
            None => ConfigError {
                line: None,
                message: e.message().to_owned(),
            },
        })?;
        config.check(&s)?;
        Ok(config)
    }

    fn check(&self, s: &Src) -> Result<(), ConfigError> {
        let n = &self.node;
        let url = n.rpc_url.get_ref();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(s.err(
                n.rpc_url.span(),
                "rpc_url must start with http:// or https://",
            ));
        }
        match (&n.cookie, &n.rpc_user, &n.rpc_password) {
            (Some(_), None, None) | (None, Some(_), Some(_)) => {}
            (Some(_), _, _) => {
                return Err(s.err(
                    n.rpc_url.span(),
                    "[node] takes cookie or rpc_user/rpc_password, not both",
                ));
            }
            (None, Some(u), None) => {
                return Err(s.err(u.span(), "rpc_user without rpc_password"));
            }
            _ => {
                return Err(s.err(
                    n.rpc_url.span(),
                    "[node] needs cookie, or rpc_user and rpc_password",
                ));
            }
        }
        if let Some(z) = &n.zmq
            && !z.get_ref().starts_with("tcp://")
        {
            return Err(s.err(z.span(), "zmq must be a tcp:// endpoint"));
        }
        if self.ws.max_message_bytes == 0 || self.ws.max_in_flight == 0 {
            return Err(ConfigError {
                line: None,
                message: "[ws] limits must be at least 1".into(),
            });
        }
        let l = &self.limits;
        if [
            l.max_connections,
            l.max_connections_per_ip,
            l.handshake_timeout_secs as usize,
            l.http2_max_streams as usize,
            l.max_subscriptions,
            l.key_max_in_flight,
            l.node_max_in_flight,
            l.node_queue_secs as usize,
        ]
        .contains(&0)
        {
            return Err(ConfigError {
                line: None,
                message: "[limits] values must be at least 1".into(),
            });
        }
        if self.events.buffer == 0 {
            return Err(ConfigError {
                line: None,
                message: "[events] buffer must be at least 1".into(),
            });
        }
        self.key_specs_at(s)?;
        Ok(())
    }

    /// The `[[key]]` entries, checked.
    pub fn key_specs(&self, src: &str) -> Result<Vec<KeySpec>, ConfigError> {
        self.key_specs_at(&Src(src))
    }

    fn key_specs_at(&self, s: &Src) -> Result<Vec<KeySpec>, ConfigError> {
        let mut names = HashSet::new();
        let mut hashes = HashSet::new();
        let mut out = Vec::new();
        for k in &self.keys {
            let name = k.name.get_ref();
            if name.is_empty() {
                return Err(s.err(k.name.span(), "empty key name"));
            }
            if !names.insert(name.clone()) {
                return Err(s.err(k.name.span(), format!("duplicate key name `{name}`")));
            }
            let hash = <[u8; 32]>::from_hex(k.token_sha256.get_ref()).map_err(|_| {
                s.err(
                    k.token_sha256.span(),
                    "token_sha256 must be 64 hex characters (bitcoin-gateway hash-token)",
                )
            })?;
            if !hashes.insert(hash) {
                return Err(s.err(k.token_sha256.span(), "duplicate token"));
            }
            let mut scopes = HashSet::new();
            for a in k.allow.get_ref() {
                let scope = Scope::parse(a.get_ref())
                    .ok_or_else(|| s.err(a.span(), format!("unknown scope `{}`", a.get_ref())))?;
                scopes.insert(scope);
            }
            if scopes.is_empty() {
                return Err(s.err(k.allow.span(), "a key needs at least one scope"));
            }
            let allow_unknown = match &k.allow_unknown {
                Some(a) if *a.get_ref() && !scopes.contains(&Scope::Admin) => {
                    return Err(s.err(a.span(), "allow_unknown needs the admin scope"));
                }
                Some(a) => *a.get_ref(),
                None => false,
            };
            let rate = match &k.rate {
                Some(r) => {
                    let v = *r.get_ref();
                    if !(v.per_second > 0.0 && v.per_second.is_finite()) || v.burst == 0 {
                        return Err(s.err(r.span(), "rate needs per_second > 0 and burst >= 1"));
                    }
                    Some(v)
                }
                None => None,
            };
            let max_in_flight = match &k.max_in_flight {
                Some(m) if *m.get_ref() == 0 => {
                    return Err(s.err(m.span(), "max_in_flight must be at least 1"));
                }
                Some(m) => *m.get_ref(),
                None => self.limits.key_max_in_flight,
            };
            out.push(KeySpec {
                name: name.clone(),
                token_sha256: hash,
                scopes,
                rate,
                allow_unknown,
                max_in_flight,
                max_subscriptions: self.limits.max_subscriptions,
            });
        }
        Ok(out)
    }

    /// Each wallet scope's wallet name and line, to check against the node.
    pub fn wallet_scopes<'a>(&'a self, src: &'a str) -> Vec<(String, ConfigError)> {
        let s = Src(src);
        let mut out = Vec::new();
        for k in &self.keys {
            for a in k.allow.get_ref() {
                if let Some(w) = Scope::parse(a.get_ref()).as_ref().and_then(Scope::wallet) {
                    out.push((
                        w.to_owned(),
                        s.err(a.span(), format!("no wallet `{w}` on the node")),
                    ));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../gateway.example.toml");

    #[test]
    fn example_loads() {
        let c = Config::parse(EXAMPLE).unwrap();
        let keys = c.key_specs(EXAMPLE).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys[1].scopes.contains(&Scope::WalletWrite("hot".into())));
        assert_eq!(
            keys[1].rate,
            Some(Rate {
                per_second: 50.0,
                burst: 100
            })
        );
        assert_eq!(c.ws.max_in_flight, 16);
    }

    const BASE: &str = r#"
[node]
rpc_url = "http://127.0.0.1:8332"
cookie = "/tmp/.cookie"
"#;

    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn err(src: &str) -> ConfigError {
        Config::parse(src).unwrap_err()
    }

    #[test]
    fn each_invalid_case_names_its_line() {
        let key = |allow: &str, extra: &str| {
            format!(
                "{BASE}\n[[key]]\nname = \"k\"\ntoken_sha256 = \"{HASH}\"\nallow = [{allow}]\n{extra}"
            )
        };
        // line 8 is `token_sha256`, 9 `allow`, 10 the extra line
        let cases = [
            (key("\"chian:read\"", ""), 9, "unknown scope `chian:read`"),
            (key("", ""), 9, "at least one scope"),
            (
                key("\"chain:read\"", "allow_unknown = true"),
                10,
                "needs the admin",
            ),
            (
                key("\"admin\"", "rate = { per_second = 0, burst = 1 }"),
                10,
                "rate",
            ),
            (key("\"admin\"", "colour = 1"), 10, "unknown field"),
            (key("\"admin\"", "max_in_flight = 0"), 10, "max_in_flight"),
            (
                format!("{BASE}\n[[key]]\nname = \"k\"\ntoken_sha256 = \"abc\"\nallow = []"),
                8,
                "64 hex",
            ),
            (
                format!(
                    "{}\n[[key]]\nname = \"j\"\ntoken_sha256 = \"{HASH}\"\nallow = [\"admin\"]",
                    key("\"admin\"", "")
                ),
                13,
                "duplicate token",
            ),
            (
                "[node]\nrpc_url = \"127.0.0.1:8332\"\ncookie = \"c\"".into(),
                2,
                "http://",
            ),
            (
                "[node]\nrpc_url = \"http://h\"\nrpc_user = \"u\"".into(),
                3,
                "rpc_password",
            ),
            (format!("{BASE}zmq = \"ipc:///tmp/z\""), 5, "tcp://"),
            (
                format!("{BASE}\n[listen]\ngrpc = \"nonsense\""),
                7,
                "invalid",
            ),
        ];
        for (src, line, needle) in cases {
            let e = err(&src);
            assert_eq!(e.line, Some(line), "{e} in\n{src}");
            assert!(e.message.contains(needle), "{e:?} lacks {needle:?}");
        }
    }

    #[test]
    fn wallet_scopes_have_lines() {
        let c = Config::parse(EXAMPLE).unwrap();
        let w = c.wallet_scopes(EXAMPLE);
        assert_eq!(w.len(), 2);
        assert_eq!(w[0].0, "hot");
        assert!(w[0].1.line.is_some());
    }

    #[test]
    fn limits_default_override_and_check() {
        let c = Config::parse(BASE).unwrap();
        assert_eq!(c.limits, LimitsConfig::default());
        assert_eq!(c.limits.node_max_in_flight, 8);

        let src = format!(
            "{BASE}\n[limits]\nkey_max_in_flight = 4\nmax_subscriptions = 2\n\
             [[key]]\nname = \"a\"\ntoken_sha256 = \"{HASH}\"\nallow = [\"admin\"]\n\
             [[key]]\nname = \"b\"\ntoken_sha256 = \"{}\"\nallow = [\"admin\"]\nmax_in_flight = 9\n",
            "00".repeat(32)
        );
        let c = Config::parse(&src).unwrap();
        let keys = c.key_specs(&src).unwrap();
        assert_eq!((keys[0].max_in_flight, keys[0].max_subscriptions), (4, 2));
        assert_eq!((keys[1].max_in_flight, keys[1].max_subscriptions), (9, 2));

        let e = err(&format!("{BASE}\n[limits]\nnode_max_in_flight = 0"));
        assert!(e.message.contains("[limits]"), "{e}");
        let e = err(&format!("{BASE}\n[limits]\nmax_conections = 5"));
        assert!(e.message.contains("unknown field"), "{e}");
    }
}
