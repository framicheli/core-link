//! Stage F: cookie re-reads, the builder, retries and batching, against the
//! fake bitcoind in `common`.
mod common;

use bitcoin::base64::Engine;
use bitcoin::base64::engine::general_purpose::STANDARD;
use bitcoin::blockdata::constants::genesis_block;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::{BlockHash, Network};
use bitcoin_core_rpc::{Auth, Client, MethodClass, RetryPolicy, Version, method_class};
use bitcoin_node_api::*;
use common::{Reply, fake_on, fake_req, fake_with, ok, rpc_err};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const V31: Version = Version(310000);

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("{user}:{pass}")))
}

fn unauthorized() -> Option<Reply> {
    Some(Reply {
        status: 401,
        body: String::new(),
        delay: Duration::ZERO,
    })
}

fn slow(result: Value, delay: Duration) -> Option<Reply> {
    ok(result).map(|r| Reply { delay, ..r })
}

fn cookie_file(tag: &str, contents: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("btc-cookie-{tag}-{}", std::process::id()));
    std::fs::write(&path, contents).unwrap();
    path
}

/// Fast retries, so the tests don't wait.
fn quick_retry(attempts: u32) -> RetryPolicy {
    let mut p = RetryPolicy::default();
    p.max_attempts = attempts;
    p.initial_backoff = Duration::from_millis(20);
    p
}

// F1: cookie auth

#[test]
fn cookie_checks_the_file_and_keeps_the_path() {
    let path = cookie_file("parse", "__cookie__:s3cret\n");
    assert_eq!(Auth::cookie(&path).unwrap(), Auth::Cookie(path.clone()));
    std::fs::write(&path, "garbage").unwrap();
    assert!(Auth::cookie(&path).is_err());
    std::fs::remove_file(&path).unwrap();
    assert!(Auth::cookie(&path).is_err());
}

#[tokio::test]
async fn cookie_is_reread_after_a_401() {
    let path = cookie_file("reread", "__cookie__:old\n");
    let accepted = Arc::new(Mutex::new(basic("__cookie__", "old")));
    let a = accepted.clone();
    let f = fake_req(move |r| {
        if r.header("authorization") != Some(a.lock().unwrap().as_str()) {
            return unauthorized();
        }
        ok(json!(7))
    })
    .await;
    let c = Client::with_version(&f.url, Auth::cookie(&path).unwrap(), V31);
    // A clone shares the credentials it learns.
    let clone = c.clone();
    assert_eq!(c.get_block_count().await.unwrap(), 7);
    assert_eq!(f.requests(), 1);

    // The node restarts with a new cookie.
    *accepted.lock().unwrap() = basic("__cookie__", "new");
    std::fs::write(&path, "__cookie__:new\n").unwrap();
    assert_eq!(c.get_block_count().await.unwrap(), 7);
    assert_eq!(f.requests(), 3, "one rejected, one retried");
    assert_eq!(clone.get_block_count().await.unwrap(), 7);
    assert_eq!(f.requests(), 4, "the clone sends the new cookie first time");
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn unchanged_cookie_fails_after_one_request() {
    let path = cookie_file("unchanged", "__cookie__:old\n");
    let f = fake_req(|_| unauthorized()).await;
    let c = Client::with_version(&f.url, Auth::cookie(&path).unwrap(), V31);
    let err = c.get_block_count().await.unwrap_err();
    assert_eq!(err, TransportError::Auth.into());
    assert_eq!(f.requests(), 1);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn changed_cookie_still_rejected_retries_once() {
    let path = cookie_file("never", "__cookie__:old\n");
    let p = path.clone();
    let f = fake_req(move |_| {
        // Every request finds a newer cookie, and is rejected anyway: no loop.
        std::fs::write(
            &p,
            format!("__cookie__:{}\n", Instant::now().elapsed().as_nanos()),
        )
        .unwrap();
        unauthorized()
    })
    .await;
    let c = Client::with_version(&f.url, Auth::cookie(&path).unwrap(), V31);
    assert_eq!(
        c.get_block_count().await.unwrap_err(),
        TransportError::Auth.into()
    );
    assert_eq!(f.requests(), 2);
    std::fs::remove_file(path).unwrap();
}

// F2: the builder

#[tokio::test]
async fn timeouts_are_configurable() {
    let f = fake_with(|m, _| match m {
        "getblockcount" | "waitfornewblock" => slow(json!(1), Duration::from_millis(1500)),
        _ => None,
    })
    .await;
    let short = Client::builder(&f.url)
        .timeout(Duration::from_millis(500))
        .build(V31);
    assert_eq!(
        short.get_block_count().await.unwrap_err(),
        TransportError::Timeout.into()
    );
    // The long timeout covers `LONG_METHODS` only.
    let r = short.raw_call::<Value>("waitfornewblock", json!([])).await;
    assert_eq!(r.unwrap(), json!(1));
    let long_short = Client::builder(&f.url)
        .long_timeout(Duration::from_millis(500))
        .build(V31);
    let r = long_short
        .raw_call::<Value>("waitfornewblock", json!([]))
        .await;
    assert_eq!(r.unwrap_err(), TransportError::Timeout.into());
    // Defaults: 30 s.
    let default = Client::with_version(&f.url, Auth::None, V31);
    assert_eq!(default.get_block_count().await.unwrap(), 1);
}

#[tokio::test]
async fn custom_http_client_keeps_its_settings() {
    let f = fake_req(|r| ok(json!(r.header("x-test")))).await;
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-test", "hello".parse().unwrap());
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    let c = Client::builder(&f.url).http_client(http).build(V31);
    let seen: Value = c.raw_call("echo", json!([])).await.unwrap();
    assert_eq!(seen, json!("hello"));
}

#[tokio::test]
async fn builder_connects_and_hides_secrets() {
    let f = common::fake().await;
    let b = Client::builder(&f.url).auth(Auth::UserPass("alice".into(), "s3cret".into()));
    let shown = format!("{b:?}");
    assert!(
        shown.contains("alice") && !shown.contains("s3cret"),
        "{shown}"
    );
    let c = b.clone().connect().await.unwrap();
    assert_eq!(c.version().major(), 31);
    assert_eq!(c.auth(), &Auth::UserPass("alice".into(), "s3cret".into()));
}

// F3: retries

#[test]
fn method_classes() {
    assert_eq!(method_class("getblockcount"), Some(MethodClass::Read));
    assert_eq!(
        method_class("sendrawtransaction"),
        Some(MethodClass::Idempotent)
    );
    assert_eq!(method_class("sendtoaddress"), Some(MethodClass::Write));
    assert_eq!(method_class("nosuchmethod"), None);
}

#[tokio::test]
async fn connect_errors_are_retried() {
    // Nothing listens on the port for the first ~100 ms.
    let port = test_node::free_port();
    let c = Client::builder(&format!("http://127.0.0.1:{port}"))
        .retry(quick_retry(10))
        .build(V31);
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        fake_on(l, |_| ok(json!(5)))
    });
    // A write too: the request never left.
    let r: u64 = c.raw_call("sendtoaddress", json!([])).await.unwrap();
    assert_eq!(r, 5);
    let f = late.await.unwrap();
    assert_eq!(f.requests(), 1);
}

#[tokio::test]
async fn timeouts_retry_reads_only() {
    let f = fake_with(|_, _| slow(json!(1), Duration::from_millis(400))).await;
    let c = Client::builder(&f.url)
        .timeout(Duration::from_millis(100))
        .retry(quick_retry(3))
        .build(V31);
    let start = Instant::now();
    let err = c
        .raw_call::<Value>("sendtoaddress", json!([]))
        .await
        .unwrap_err();
    assert_eq!(err, TransportError::Timeout.into());
    assert_eq!(
        f.calls("sendtoaddress").len(),
        1,
        "may have paid: not resent"
    );
    assert!(start.elapsed() < Duration::from_millis(300));

    assert!(c.get_block_count().await.is_err());
    assert_eq!(f.calls("getblockcount").len(), 3);
    // Unknown methods are writes.
    assert!(
        c.raw_call::<Value>("nosuchmethod", json!([]))
            .await
            .is_err()
    );
    assert_eq!(f.calls("nosuchmethod").len(), 1);
}

#[tokio::test]
async fn warmup_is_retried_with_growing_gaps() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = seen.clone();
    let f = fake_with(move |m, _| {
        let mut s = s.lock().unwrap();
        s.push((m.to_owned(), Instant::now()));
        if s.iter().filter(|(x, _)| x == m).count() <= 2 {
            rpc_err(-28, "Loading block index…")
        } else {
            ok(json!(9))
        }
    })
    .await;
    let mut policy = RetryPolicy::default();
    policy.initial_backoff = Duration::from_millis(50);
    let c = Client::builder(&f.url).retry(policy).build(V31);
    assert_eq!(c.get_block_count().await.unwrap(), 9);
    let t: Vec<_> = seen.lock().unwrap().iter().map(|(_, t)| *t).collect();
    assert_eq!(t.len(), 3);
    let (g1, g2) = (t[1] - t[0], t[2] - t[1]);
    assert!(g1 >= Duration::from_millis(50) && g2 >= Duration::from_millis(100));
    assert!(g2 > g1, "{g1:?} then {g2:?}");
    // -28 means "not run": a write is retried too.
    let r: u64 = c.raw_call("sendtoaddress", json!([])).await.unwrap();
    assert_eq!(r, 9);
}

#[tokio::test]
async fn no_policy_no_retry() {
    let f = fake_with(|_, _| rpc_err(-28, "Loading")).await;
    let c = Client::with_version(&f.url, Auth::None, V31);
    assert!(c.get_block_count().await.is_err());
    assert_eq!(f.requests(), 1);
}

// F4: batching

fn hash(h: u64) -> String {
    format!("{h:064x}")
}

#[tokio::test]
async fn batches_are_chunked_and_matched_by_id() {
    let f = fake_with(|m, p| match (m, p[0].as_u64()) {
        ("getblockhash", Some(13)) => rpc_err(-8, "Block height out of range"),
        ("getblockhash", Some(h)) => ok(json!(hash(h))),
        _ => None,
    })
    .await;
    let c = Client::with_version(&f.url, Auth::None, V31);
    let params = (0..250u64).map(|h| json!([h])).collect();
    let out = c.batch::<BlockHash>("getblockhash", params).await.unwrap();
    assert_eq!(f.requests(), 3);
    assert_eq!(out.len(), 250);
    for (h, r) in out.iter().enumerate() {
        match (h, r) {
            (13, Err(NodeAPIError::Rpc { code, .. })) => {
                assert_eq!(*code, RpcErrorCode::InvalidParameter)
            }
            (h, Ok(b)) => assert_eq!(b.to_string(), hash(h as u64)),
            other => panic!("{other:?}"),
        }
    }

    // `get_block_hashes` fails as a whole on a bad height.
    assert!(c.get_block_hashes(10..=20).await.is_err());
    let hashes = c.get_block_hashes(20..=24).await.unwrap();
    let want: Vec<String> = (20..=24).map(hash).collect();
    assert_eq!(
        hashes.iter().map(|h| h.to_string()).collect::<Vec<_>>(),
        want
    );

    assert!(
        c.batch::<Value>("getblockhash", vec![])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn batch_chunk_is_configurable() {
    let f = fake_with(|_, p| ok(p[0].clone())).await;
    let c = Client::builder(&f.url).batch_chunk(10).build(V31);
    let out = c
        .batch::<u64>("echo", (0..25u64).map(|i| json!([i])).collect())
        .await
        .unwrap();
    assert_eq!(f.requests(), 3);
    assert_eq!(
        out.into_iter().map(Result::unwrap).collect::<Vec<_>>(),
        (0..25).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn get_blocks_reports_each_bad_hash() {
    let genesis = genesis_block(Network::Regtest);
    let known = genesis.block_hash();
    let raw = serialize_hex(&genesis);
    let f = fake_with(move |m, p| match m {
        "getblock" if p[0] == json!(known) => ok(json!(raw)),
        "getblock" => rpc_err(-5, "Block not found"),
        _ => None,
    })
    .await;
    let c = Client::with_version(&f.url, Auth::None, V31);
    let unknown: BlockHash = hash(1).parse().unwrap();
    let out = c.get_blocks(&[known, unknown, known]).await.unwrap();
    assert_eq!(f.requests(), 1);
    assert_eq!(out[0].as_ref().unwrap(), &genesis);
    assert!(matches!(
        out[1],
        Err(NodeAPIError::Rpc {
            code: RpcErrorCode::InvalidAddressOrKey,
            ..
        })
    ));
    assert_eq!(out[2].as_ref().unwrap(), &genesis);
}

#[tokio::test]
async fn batch_in_warmup_is_retried_whole() {
    let n = Arc::new(AtomicUsize::new(0));
    let k = n.clone();
    let f = fake_with(move |_, p| {
        // The first request's 3 items are all in warmup.
        if k.fetch_add(1, Ordering::SeqCst) < 3 {
            rpc_err(-28, "Loading")
        } else {
            ok(p[0].clone())
        }
    })
    .await;
    let c = Client::builder(&f.url).retry(quick_retry(3)).build(V31);
    let out = c
        .batch::<u64>("getblockhash", vec![json!([1]), json!([2]), json!([3])])
        .await
        .unwrap();
    assert_eq!(f.requests(), 2);
    assert_eq!(*out[2].as_ref().unwrap(), 3);
}
