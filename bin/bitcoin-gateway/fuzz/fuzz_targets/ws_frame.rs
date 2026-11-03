//! Any WebSocket text message, from any key, on the node or a wallet route:
//! never panics, and every reply is JSON-RPC 2.0 (an object, or a non-empty
//! batch of them) whose error codes are JSON-RPC's or Core's.
#![no_main]
#[path = "common.rs"]
mod common;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

#[derive(Arbitrary, Debug)]
struct Input<'a> {
    key: u8,
    wallet: Option<&'a str>,
    text: &'a str,
}

fn check_reply(r: &Value) {
    let o = r.as_object().expect("a reply is an object");
    assert_eq!(o.get("jsonrpc"), Some(&Value::from("2.0")), "{r}");
    assert!(o.contains_key("id"), "{r}");
    assert!(o.contains_key("result") != o.contains_key("error"), "{r}");
    if let Some(e) = o.get("error") {
        assert!(e["code"].is_i64(), "{r}");
        assert!(e["message"].is_string(), "{r}");
    }
}

fuzz_target!(|input: Input| {
    let h = common::harness();
    let key = h.key(input.key);
    let wallet = input.wallet.map(str::to_owned);
    let reply = h.rt.block_on(bitcoin_gateway::ws::handle_text(
        h.gw.clone(),
        key,
        wallet,
        input.text,
    ));
    match reply {
        None => {}
        Some(Value::Array(items)) => {
            assert!(!items.is_empty());
            items.iter().for_each(check_reply);
        }
        Some(r) => check_reply(&r),
    }
});
