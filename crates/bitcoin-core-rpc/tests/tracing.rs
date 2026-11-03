//! G2: with the `tracing` feature, one span per call, retries and cookie
//! re-reads inside it, and never a param or a result.
#![cfg(feature = "tracing")]
mod common;

use bitcoin_core_rpc::{Auth, Client, RetryPolicy, Version};
use common::{fake_with, ok, rpc_err};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn one_span_per_call_and_no_params() {
    let buf = Buf::default();
    let w = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_span_events(FmtSpan::CLOSE)
        .with_ansi(false)
        .with_writer(move || w.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let n = Arc::new(AtomicUsize::new(0));
    let k = n.clone();
    let f = fake_with(move |m, _| match m {
        // Warm up once: the retry is an event inside the call's span.
        "walletpassphrase" if k.fetch_add(1, Ordering::SeqCst) == 0 => {
            rpc_err(-28, "Loading wallet")
        }
        "walletpassphrase" => ok(Value::Null),
        "echo" => ok(json!("RESULT-SECRET")),
        _ => None,
    })
    .await;
    let mut policy = RetryPolicy::default();
    policy.initial_backoff = Duration::from_millis(1);
    let c = Client::builder(&f.url)
        .auth(Auth::None)
        .retry(policy)
        .build(Version(310000));

    let r: Value = c.raw_call("echo", json!(["PARAM-SECRET"])).await.unwrap();
    assert_eq!(r, json!("RESULT-SECRET"));
    c.wallet("hot")
        .wallet_passphrase("PARAM-SECRET", Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        c.raw_call::<Value>("nosuchmethod", json!([]))
            .await
            .is_err()
    );

    let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let closes = out.lines().filter(|l| l.contains("close")).count();
    assert_eq!(closes, 3, "one span per call:\n{out}");
    assert!(out.contains("method=\"walletpassphrase\""), "{out}");
    assert!(out.contains("wallet=\"hot\""), "{out}");
    assert!(out.contains("retrying"), "{out}");
    assert!(out.contains(r#"error="rpc -32601""#), "{out}");
    assert!(!out.contains("SECRET"), "params or results leaked:\n{out}");
}
