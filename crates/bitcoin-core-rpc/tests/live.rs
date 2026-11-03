//! Runs `scenario` on a real `bitcoind` (from `BITCOIND_EXE`, else `PATH`) on
//! a fresh regtest datadir. `#[ignore]`: `cargo test -- --ignored`.
//! With `RECORD=1` the sessions are saved for `replay.rs`.
mod common;
mod scenario;

use bitcoin_core_rpc::{Auth, Client, RetryPolicy};
use std::future::Future;
use std::time::Duration;
use test_node::{RPC_PASSWORD, RPC_USER, TestNode, free_port};

const SESSION: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/core31/session.json"
);
const WALLET_SESSION: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/core31/wallet-session.json"
);

#[tokio::test]
#[ignore = "needs bitcoind"]
async fn every_method_on_core() {
    let zmq = format!("-zmqpubsequence=tcp://127.0.0.1:{}", free_port());
    run_live(&[&zmq], SESSION, |c| async move {
        scenario::run(&c, true).await
    })
    .await;
}

#[tokio::test]
#[ignore = "needs bitcoind"]
async fn every_wallet_method_on_core() {
    // Regtest has no fee estimates; sending fails without a fallback.
    run_live(&["-fallbackfee=0.0001"], WALLET_SESSION, |c| async move {
        scenario::wallet::run(&c).await
    })
    .await;
}

/// Starts a node with `args`, runs `scenario` (which stops it) through the
/// recorder, and saves the session to `session` when `RECORD=1`.
async fn run_live<F, Fut>(args: &[&str], session_path: &str, scenario: F)
where
    F: FnOnce(Client) -> Fut,
    Fut: Future<Output = scenario::Checks>,
{
    let mut node = TestNode::start(args);
    let (url, session) =
        common::recorder(node.rpc_url(), RPC_USER.into(), RPC_PASSWORD.into()).await;

    let mut client = None;
    for _ in 0..100 {
        match Client::connect(&url, Auth::None).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let client = client.expect("bitcoind came up");
    // Only the successful `connect` belongs in the session.
    {
        let mut s = session.lock().unwrap();
        let first_ok = s
            .iter()
            .rposition(|e| e["method"] == "getnetworkinfo")
            .unwrap();
        s.drain(..first_ok);
    }

    let major = client.version().major();
    eprintln!("Core {}", client.version().0);
    let checks = scenario(client).await;
    let log_tail = tokio::task::spawn_blocking(move || {
        node.wait();
        node.log_tail(40)
    })
    .await
    .unwrap();

    if std::env::var("RECORD").is_ok_and(|v| v == "1") {
        assert_eq!(major, 31, "the recorded session is Core 31's");
        let s = session.lock().unwrap();
        let path = std::path::Path::new(session_path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_string_pretty(&*s).unwrap() + "\n").unwrap();
    }
    assert!(
        checks.failures.is_empty(),
        "Core {major}: {} failures:\n{}\n\nend of debug.log:\n{log_tail}",
        checks.failures.len(),
        checks.failures.join("\n")
    );
}

/// F1: a `Client` on cookie auth keeps working across a node restart.
#[tokio::test]
#[ignore = "needs bitcoind"]
async fn cookie_survives_a_restart() {
    let mut node = TestNode::start_cookie(&[]);
    let cookie = node.cookie_path();
    for _ in 0..100 {
        if cookie.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut policy = RetryPolicy::default();
    policy.max_attempts = 100;
    policy.max_backoff = Duration::from_millis(200);
    let client = Client::builder(&node.rpc_url())
        .auth(Auth::cookie(&cookie).unwrap())
        .retry(policy)
        .connect()
        .await
        .unwrap();
    client.get_block_count().await.unwrap();

    let before = std::fs::read_to_string(&cookie).unwrap();
    node.restart();
    // Wait for the new cookie, then the same client must authenticate.
    for _ in 0..100 {
        if std::fs::read_to_string(&cookie).is_ok_and(|c| c != before) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_ne!(std::fs::read_to_string(&cookie).unwrap(), before);
    client.get_block_count().await.unwrap();
}

/// F4: 200 batched `getblockhash` equal 200 single calls; prints both times.
#[tokio::test]
#[ignore = "needs bitcoind"]
async fn batched_block_hashes_match_single_calls() {
    let node = TestNode::start(&[]);
    let mut policy = RetryPolicy::default();
    policy.max_attempts = 100;
    let client = Client::builder(&node.rpc_url())
        .auth(Auth::UserPass(RPC_USER.into(), RPC_PASSWORD.into()))
        .retry(policy)
        .connect()
        .await
        .unwrap();
    let to = bitcoin::Address::p2wsh(&bitcoin::ScriptBuf::new(), bitcoin::Network::Regtest);
    client.generate_to_address(199, &to).await.unwrap();

    let start = std::time::Instant::now();
    let mut singles = Vec::new();
    for h in 0..200 {
        singles.push(client.get_block_hash(h).await.unwrap());
    }
    let single_time = start.elapsed();
    let start = std::time::Instant::now();
    let batched = client.get_block_hashes(0..=199).await.unwrap();
    let batch_time = start.elapsed();
    assert_eq!(batched, singles);
    eprintln!("200 getblockhash: {single_time:?} one by one, {batch_time:?} batched");

    let blocks = client.get_blocks(&batched[..5]).await.unwrap();
    for (b, h) in blocks.iter().zip(&batched) {
        assert_eq!(b.as_ref().unwrap().block_hash(), *h);
    }
}
