//! Replays the Core 31 sessions `live.rs` recorded (`RECORD=1`): every typed
//! method parses a real reply and sends exactly the params Core accepted.
mod common;
mod scenario;

use bitcoin_core_rpc::{Auth, Client};

fn session(file: &str) -> Vec<common::Exchange> {
    let path = format!(
        "{}/tests/fixtures/core31/{file}",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

async fn connect(session: Vec<common::Exchange>) -> (common::Fake, Client) {
    let f = common::replay(session).await;
    let c = Client::connect(&f.url, Auth::None).await.unwrap();
    assert_eq!(c.version().major(), 31);
    (f, c)
}

#[tokio::test]
async fn every_method_on_recorded_core_31() {
    let (_node, c) = connect(session("session.json")).await;
    let mut checks = scenario::run(&c, true).await;
    let (_wallets, c) = connect(session("wallet-session.json")).await;
    let wallet = scenario::wallet::run(&c).await;
    checks.failures.extend(wallet.failures);
    checks.ran.extend(wallet.ran);
    assert!(
        checks.failures.is_empty(),
        "{} failures:\n{}",
        checks.failures.len(),
        checks.failures.join("\n")
    );

    let mut missing = Vec::new();
    for path in common::source_files() {
        let text = std::fs::read_to_string(path).unwrap();
        for line in text.lines() {
            let Some(rest) = line.trim().strip_prefix("pub async fn ") else {
                continue;
            };
            let name = rest.split(['(', '<']).next().unwrap();
            // Batches: the recorder logs single calls only; `client.rs` and
            // the live test cover them.
            let untyped = [
                "connect",
                "raw_call",
                "batch",
                "get_block_hashes",
                "get_blocks",
            ];
            if !untyped.contains(&name) && !checks.ran.contains(name) {
                missing.push(name.to_owned());
            }
        }
    }
    assert!(missing.is_empty(), "not in the scenario: {missing:?}");
}

/// F3: every method the scenario calls has a retry/scope class, so a new
/// typed method can't ship without one.
#[test]
fn every_recorded_method_has_a_class() {
    let mut missing: Vec<String> = ["session.json", "wallet-session.json"]
        .into_iter()
        .flat_map(session)
        .map(|e| e["method"].as_str().unwrap().to_owned())
        .filter(|m| bitcoin_core_rpc::method_class(m).is_none())
        .collect();
    missing.sort();
    missing.dedup();
    assert!(missing.is_empty(), "no class: {missing:?}");
}
