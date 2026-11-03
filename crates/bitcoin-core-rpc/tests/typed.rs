//! Typed `Client` methods: per-version dispatch, the `core` structs, and
//! methods that build non-trivial params.
mod common;

use bitcoin::absolute::LockTime;
use bitcoin::blockdata::constants::genesis_block;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::{Address, Amount, FeeRate, MerkleBlock, Network, OutPoint, SignedAmount, Txid};
use bitcoin_core_rpc::{Auth, Client, SetBanCommand, Version};
use bitcoin_node_api::NodeAPIError;
use common::{fake_with, fixture, ok};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

fn client(url: &str, version: u32) -> Client {
    Client::with_version(url, Auth::None, Version(version))
}

/// Core 29–30 `getmempoolentry`: no `chunkweight`, no `fees.chunk`.
fn mempool_entry_v29() -> Value {
    let mut v = fixture("getmempoolentry").unwrap();
    let o = v.as_object_mut().unwrap();
    o.remove("chunkweight");
    o["fees"].as_object_mut().unwrap().remove("chunk");
    v
}

#[tokio::test]
async fn versioned_dispatch_parses_each_shape() {
    let f = fake_with(|m, p| match (m, p[0].as_str()) {
        // The txid picks the shape, so one fake serves both versions.
        ("getmempoolentry", Some(t)) if t.starts_with("00") => ok(mempool_entry_v29()),
        _ => None,
    })
    .await;
    let old = Txid::from_str(&"00".repeat(32)).unwrap();
    let new = Txid::from_str(&"11".repeat(32)).unwrap();

    let e29 = client(&f.url, 290500).get_mempool_entry(old).await.unwrap();
    let e31 = client(&f.url, 310000).get_mempool_entry(new).await.unwrap();
    assert_eq!(e29.0.vsize, e31.0.vsize);
    assert_eq!(e31.0.fees.base, Amount::from_sat(1410));

    // The v31 struct can't read a v29 reply: that's what the dispatch is for.
    let err = client(&f.url, 310000)
        .get_mempool_entry(old)
        .await
        .unwrap_err();
    assert!(matches!(err, NodeAPIError::Decode(_)), "{err}");
}

#[tokio::test]
async fn peer_info_parses_core_29_and_31() {
    let peer = |v31: bool| {
        let mut p = json!({
            "id": 0, "addr": "127.0.0.1:18444", "addrbind": "127.0.0.1:50000", "network": "not_publicly_routable",
            "services": "0000000000000c09", "servicesnames": ["NETWORK", "WITNESS", "NETWORK_LIMITED", "P2P_V2"],
            "relaytxes": true, "lastsend": 1730000000, "lastrecv": 1730000000, "last_transaction": 0, "last_block": 0,
            "bytessent": 1000, "bytesrecv": 1000, "conntime": 1730000000, "timeoffset": 0, "pingtime": 0.001,
            "minping": 0.001, "version": 70016, "subver": "/Satoshi:29.5.0/", "inbound": false,
            "bip152_hb_to": false, "bip152_hb_from": false, "presynced_headers": -1, "synced_headers": 0,
            "synced_blocks": 0, "inflight": [], "addr_relay_enabled": false, "addr_processed": 0,
            "addr_rate_limited": 0, "permissions": [], "minfeefilter": 0.00001000,
            "bytessent_per_msg": { "ping": 32 }, "bytesrecv_per_msg": { "pong": 32 },
            "connection_type": "manual", "transport_protocol_type": "v2", "session_id": "ab"
        });
        if v31 {
            p["inv_to_send"] = json!(0);
            p["last_inv_sequence"] = json!(7);
        }
        p
    };
    let f = fake_with(move |m, _| match m {
        "getpeerinfo" => ok(json!([peer(false), peer(true)])),
        _ => None,
    })
    .await;
    let peers = client(&f.url, 310000).get_peer_info().await.unwrap();
    assert_eq!(peers[0].last_inv_sequence, None);
    assert_eq!(peers[1].last_inv_sequence, Some(7));
}

#[tokio::test]
async fn logging_is_a_map() {
    let f = fake_with(|m, _| match m {
        "logging" => ok(json!({ "net": false, "mempool": true, "kernel": false })),
        _ => None,
    })
    .await;
    let c = client(&f.url, 300000);
    let l = c.logging(&["mempool"], &[]).await.unwrap();
    assert!(l["mempool"]);
    assert_eq!(f.calls("logging")[0], json!([["mempool"], []]));
}

#[tokio::test]
async fn params_are_core_shaped() {
    let f = fake_with(|m, _| match m {
        "createrawtransaction" => ok(json!(serialize_hex(
            &genesis_block(Network::Regtest).txdata[0]
        ))),
        "setban" | "disconnectnode" => ok(Value::Null),
        "prioritisetransaction" => ok(json!(true)),
        "sendrawtransaction" => ok(json!("00".repeat(32))),
        _ => None,
    })
    .await;
    let c = client(&f.url, 310000);
    let cb = genesis_block(Network::Regtest).txdata[0].clone();
    let txid = cb.compute_txid();
    let addr = Address::from_str("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080")
        .unwrap()
        .assume_checked();

    let out = OutPoint { txid, vout: 0 };
    c.create_raw_transaction(
        &[out],
        &[(addr.clone(), Amount::from_sat(12_345))],
        Some(b"hi"),
        Some(LockTime::from_height(100).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        f.calls("createrawtransaction")[0],
        json!([[{ "txid": txid, "vout": 0 }], [{ addr.to_string(): 0.00012345 }, { "data": "6869" }], 100])
    );

    c.set_ban(
        "10.0.0.0/8",
        SetBanCommand::Add,
        Some(Duration::from_secs(60)),
    )
    .await
    .unwrap();
    assert_eq!(f.calls("setban")[0], json!(["10.0.0.0/8", "add", 60]));

    c.disconnect_node_by_id(4).await.unwrap();
    assert_eq!(f.calls("disconnectnode")[0], json!(["", 4]));

    c.prioritise_transaction(txid, SignedAmount::from_sat(-500))
        .await
        .unwrap();
    assert_eq!(
        f.calls("prioritisetransaction")[0],
        json!([txid, null, -500])
    );

    // 2500 sat/kwu = 10 000 sat/kvB = 0.0001 BTC/kvB.
    c.send_raw_transaction(&cb, Some(FeeRate::from_sat_per_kwu(2500)))
        .await
        .unwrap();
    assert_eq!(f.calls("sendrawtransaction")[0][1], json!(0.0001));
}

#[tokio::test]
async fn tx_out_proof_round_trips() {
    let block = genesis_block(Network::Regtest);
    let txid = block.txdata[0].compute_txid();
    let proof = MerkleBlock::from_block_with_predicate(&block, |t| *t == txid);
    let hex = serialize_hex(&proof);
    let f = fake_with(move |m, _| match m {
        "gettxoutproof" => ok(json!(hex)),
        "verifytxoutproof" => ok(json!([txid])),
        "gettxout" => ok(Value::Null),
        _ => None,
    })
    .await;
    let c = client(&f.url, 310000);

    let got = c.get_tx_out_proof(&[txid], None).await.unwrap();
    // Decoding pads the flag bits to a byte, so compare encodings.
    assert_eq!(serialize_hex(&got), serialize_hex(&proof));
    assert_eq!(c.verify_tx_out_proof(&got).await.unwrap(), [txid]);
    let spent = c
        .get_tx_out(OutPoint { txid, vout: 0 }, true)
        .await
        .unwrap();
    assert_eq!(spent, None);
}

#[tokio::test]
async fn version_gates_skip_the_request() {
    let f = fake_with(|_, _| None).await;
    let c = client(&f.url, 300000);
    let txid = Txid::from_str(&"11".repeat(32)).unwrap();
    assert!(matches!(
        c.get_mempool_cluster(txid).await,
        Err(NodeAPIError::Unsupported(_))
    ));
    assert!(matches!(
        c.get_private_broadcast_info().await,
        Err(NodeAPIError::Unsupported(_))
    ));
    assert!(f.calls("getmempoolcluster").is_empty());
}

/// Every method in `src/` is named after its RPC: `getblockstats` -> `get_block_stats`.
#[test]
fn method_names_follow_rpc_names() {
    let mut seen = HashSet::new();
    for path in common::source_files() {
        let text = std::fs::read_to_string(path).unwrap();
        let mut name = None;
        for line in text.lines() {
            if let Some(rest) = line.trim().strip_prefix("pub async fn ") {
                // `connect` probes the version with `getnetworkinfo`.
                name = rest
                    .split(['(', '<'])
                    .next()
                    .filter(|n| *n != "connect")
                    .map(str::to_owned);
            }
            if let (Some(n), Some(i)) = (&name, line.find("(self, \"").or(line.find("call(\""))) {
                let rpc = line[i..].split('"').nth(1).unwrap();
                let flat = n.replace('_', "");
                assert!(
                    flat.starts_with(rpc) || rpc.starts_with(&flat),
                    "{n} calls {rpc}"
                );
                seen.insert(n.clone());
                name = None;
            }
        }
    }
    assert!(seen.len() > 60, "only checked {}", seen.len());
}
