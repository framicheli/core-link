//! Bearer tokens, keys, scope checks and per-key rate limits.
//!
//! Tokens are 256-bit random values; the config holds only their SHA-256
//! (PHASE_2.md §1.1). A presented token is hashed and compared against every
//! key's hash in constant time.
use crate::config::{KeySpec, Rate};
use crate::error::ApiError;
use crate::scopes::{Need, Scope, required};
use bitcoin::hashes::{Hash, sha256};
use bitcoin::hex::DisplayHex;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use subtle::ConstantTimeEq;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// SHA-256 of a token, as the config stores it.
pub fn hash_token(token: &str) -> [u8; 32] {
    sha256::Hash::hash(token.as_bytes()).to_byte_array()
}

/// A fresh token: 32 random bytes, hex.
pub fn new_token() -> String {
    let mut b = [0u8; 32];
    getrandom::fill(&mut b).expect("the OS random source failed");
    b.to_lower_hex_string()
}

/// One API key.
#[derive(Debug)]
pub struct Key {
    /// From the config; shown in logs and metrics, never the token.
    pub name: String,
    hash: [u8; 32],
    scopes: HashSet<Scope>,
    allow_unknown: bool,
    bucket: Option<Mutex<Bucket>>,
    /// Calls to Core the key may have running.
    in_flight: Arc<Semaphore>,
    /// Event subscriptions the key may hold.
    subscriptions: Arc<Semaphore>,
}

impl Key {
    /// A key from a checked config entry.
    pub fn new(spec: KeySpec) -> Key {
        Key {
            name: spec.name,
            hash: spec.token_sha256,
            scopes: spec.scopes,
            allow_unknown: spec.allow_unknown,
            bucket: spec.rate.map(|r| Mutex::new(Bucket::new(r))),
            in_flight: Arc::new(Semaphore::new(spec.max_in_flight)),
            subscriptions: Arc::new(Semaphore::new(spec.max_subscriptions)),
        }
    }

    /// Holds `scope`, or `admin`.
    pub fn has(&self, scope: &Scope) -> bool {
        self.scopes.contains(&Scope::Admin) || self.scopes.contains(scope)
    }

    /// May use what `need` names.
    pub fn permits(&self, need: &Need) -> bool {
        match need {
            Need::Scope(s) => self.has(s),
            Need::Admin => self.scopes.contains(&Scope::Admin),
            Need::Unknown => self.scopes.contains(&Scope::Admin) && self.allow_unknown,
        }
    }

    /// Checks `scope`, then takes one request from the key's rate.
    pub fn require(&self, scope: Scope) -> Result<(), ApiError> {
        if !self.has(&scope) {
            return Err(ApiError::permission_denied(format!(
                "this key lacks the {scope} scope"
            )));
        }
        self.admit()
    }

    /// Checks a Core method by name (WebSocket pass-through), then the rate.
    /// `wallet` is the wallet route's wallet.
    pub fn require_method(&self, method: &str, wallet: Option<&str>) -> Result<(), ApiError> {
        let need = required(method, wallet);
        if !self.permits(&need) {
            let why = match need {
                Need::Scope(s) => format!("`{method}` needs the {s} scope"),
                Need::Admin if wallet.is_none() && is_wallet_method(method) => {
                    format!("`{method}` is a wallet method: call it on /ws/wallet/<name>")
                }
                Need::Admin => format!("`{method}` is admin only"),
                Need::Unknown => format!("`{method}` is unknown to the gateway"),
            };
            return Err(ApiError::permission_denied(why));
        }
        self.admit()
    }

    /// One of the key's call slots, held until the call ends. None free:
    /// rate limited, at once (the client should slow down, not queue).
    pub fn call_slot(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        self.in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::rate_limited_because("too many calls in flight for this key"))
    }

    /// One of the key's subscription slots, held by the subscription.
    pub fn subscription_slot(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        self.subscriptions.clone().try_acquire_owned().map_err(|_| {
            ApiError::rate_limited_because("too many event subscriptions for this key")
        })
    }

    /// Takes one request from the key's rate.
    pub fn admit(&self) -> Result<(), ApiError> {
        match &self.bucket {
            Some(b) if !b.lock().unwrap_or_else(|e| e.into_inner()).take() => {
                Err(ApiError::rate_limited())
            }
            _ => Ok(()),
        }
    }
}

fn is_wallet_method(method: &str) -> bool {
    crate::scopes::method_area(method) == Some(crate::scopes::Area::Wallet)
}

/// A token bucket.
#[derive(Debug)]
struct Bucket {
    rate: Rate,
    tokens: f64,
    at: Instant,
}

impl Bucket {
    fn new(rate: Rate) -> Bucket {
        Bucket {
            rate,
            tokens: rate.burst as f64,
            at: Instant::now(),
        }
    }

    fn take(&mut self) -> bool {
        let now = Instant::now();
        let refill = now.duration_since(self.at).as_secs_f64() * self.rate.per_second;
        self.tokens = (self.tokens + refill).min(self.rate.burst as f64);
        self.at = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// The keys in force; replaced whole on `SIGHUP`.
#[derive(Debug, Default)]
pub struct KeyStore(RwLock<Arc<Vec<Arc<Key>>>>);

impl KeyStore {
    /// Keys from checked config entries.
    pub fn new(specs: Vec<KeySpec>) -> KeyStore {
        let store = KeyStore::default();
        store.replace(specs);
        store
    }

    /// Swaps in a new set; requests in flight keep the key they started with.
    pub fn replace(&self, specs: Vec<KeySpec>) {
        let keys = specs.into_iter().map(|s| Arc::new(Key::new(s))).collect();
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(keys);
    }

    /// The key for an `Authorization: Bearer <token>` value.
    pub fn authenticate(&self, authorization: Option<&str>) -> Result<Arc<Key>, ApiError> {
        let token = authorization
            .and_then(|h| {
                let (scheme, token) = h.trim().split_once(' ')?;
                scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
            })
            .filter(|t| !t.is_empty())
            .ok_or_else(ApiError::unauthenticated)?;
        let hash = hash_token(token);
        let keys = self.0.read().unwrap_or_else(|e| e.into_inner()).clone();
        // Every key is compared, so timing doesn't tell which one matched.
        let mut found = None;
        for k in keys.iter() {
            if bool::from(k.hash.ct_eq(&hash)) {
                found = Some(k.clone());
            }
        }
        found.ok_or_else(ApiError::unauthenticated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Kind;

    fn spec(name: &str, token: &str, allow: &[&str]) -> KeySpec {
        KeySpec {
            name: name.into(),
            token_sha256: hash_token(token),
            scopes: allow.iter().map(|s| Scope::parse(s).unwrap()).collect(),
            rate: None,
            allow_unknown: false,
            max_in_flight: 32,
            max_subscriptions: 16,
        }
    }

    fn store() -> KeyStore {
        KeyStore::new(vec![
            spec("explorer", "t1", &["chain:read", "events"]),
            spec("hot", "t2", &["wallet:hot:read", "broadcast"]),
            spec("root", "t3", &["admin"]),
            KeySpec {
                allow_unknown: true,
                ..spec("root-any", "t4", &["admin"])
            },
        ])
    }

    fn kind<T: std::fmt::Debug>(r: Result<T, ApiError>) -> Kind {
        r.unwrap_err().kind
    }

    #[test]
    fn tokens() {
        let t = new_token();
        assert_eq!(t.len(), 64);
        assert_ne!(t, new_token());
        assert_eq!(
            hash_token("test").to_lower_hex_string(),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }

    #[test]
    fn no_or_bad_token() {
        let s = store();
        assert_eq!(kind(s.authenticate(None)), Kind::Unauthenticated);
        assert_eq!(
            kind(s.authenticate(Some("Bearer nope"))),
            Kind::Unauthenticated
        );
        assert_eq!(
            kind(s.authenticate(Some("Basic dDE="))),
            Kind::Unauthenticated
        );
        assert_eq!(kind(s.authenticate(Some("Bearer "))), Kind::Unauthenticated);
        assert_eq!(s.authenticate(Some("Bearer t1")).unwrap().name, "explorer");
        assert_eq!(s.authenticate(Some("bearer  t2 ")).unwrap().name, "hot");
    }

    #[test]
    fn wrong_scope() {
        let s = store();
        let k = s.authenticate(Some("Bearer t1")).unwrap();
        k.require(Scope::ChainRead).unwrap();
        assert_eq!(kind(k.require(Scope::Broadcast)), Kind::PermissionDenied);
        k.require_method("getblock", None).unwrap();
        assert_eq!(
            kind(k.require_method("sendrawtransaction", None)),
            Kind::PermissionDenied
        );
    }

    #[test]
    fn wallet_scopes_are_per_wallet() {
        let s = store();
        let k = s.authenticate(Some("Bearer t2")).unwrap();
        k.require_method("getbalance", Some("hot")).unwrap();
        assert_eq!(
            kind(k.require_method("getbalance", Some("cold"))),
            Kind::PermissionDenied
        );
        assert_eq!(
            kind(k.require_method("sendtoaddress", Some("hot"))),
            Kind::PermissionDenied
        );
        // the node route can't tell which wallet Core would pick
        let e = k.require_method("getbalance", None).unwrap_err();
        assert!(e.message.contains("/ws/wallet/"), "{e}");
    }

    #[test]
    fn unknown_and_admin_only() {
        let s = store();
        let explorer = s.authenticate(Some("Bearer t1")).unwrap();
        let root = s.authenticate(Some("Bearer t3")).unwrap();
        let any = s.authenticate(Some("Bearer t4")).unwrap();
        for k in [&explorer, &root] {
            assert_eq!(
                kind(k.require_method("getnewthing", None)),
                Kind::PermissionDenied
            );
        }
        any.require_method("getnewthing", None).unwrap();
        assert_eq!(
            kind(explorer.require_method("stop", None)),
            Kind::PermissionDenied
        );
        assert_eq!(
            kind(explorer.require_method("backupwallet", Some("hot"))),
            Kind::PermissionDenied
        );
        root.require_method("stop", None).unwrap();
        root.require_method("sendtoaddress", Some("any")).unwrap();
        root.require(Scope::Events).unwrap();
    }

    #[test]
    fn rate_limit() {
        let s = KeyStore::new(vec![KeySpec {
            rate: Some(Rate {
                per_second: 0.001,
                burst: 3,
            }),
            ..spec("slow", "t", &["chain:read"])
        }]);
        let k = s.authenticate(Some("Bearer t")).unwrap();
        for _ in 0..3 {
            k.require(Scope::ChainRead).unwrap();
        }
        assert_eq!(kind(k.require(Scope::ChainRead)), Kind::ResourceExhausted);
    }

    #[test]
    fn reload_replaces_keys() {
        let s = store();
        let old = s.authenticate(Some("Bearer t1")).unwrap();
        s.replace(vec![spec("new", "t9", &["fees:read"])]);
        assert_eq!(
            kind(s.authenticate(Some("Bearer t1"))),
            Kind::Unauthenticated
        );
        assert_eq!(s.authenticate(Some("Bearer t9")).unwrap().name, "new");
        // a request in flight keeps its key
        old.require(Scope::ChainRead).unwrap();
    }
}
