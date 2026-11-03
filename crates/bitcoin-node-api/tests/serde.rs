//! The JSON shapes behind the `serde` feature, and that they round-trip.
#![cfg(feature = "serde")]
use bitcoin::blockdata::constants::genesis_block;
use bitcoin::hashes::Hash;
use bitcoin::{
    Amount, BlockHash, Network, ScriptBuf, Sequence, TxOut, Txid, Weight, absolute, transaction,
};
use bitcoin_node_api::{
    Coinbase, FeeMode, FeeTarget, HeaderInfo, MempoolAccept, MempoolEntry, NodeAPIError, NodeEvent,
    PackageOutcome, PackageResult, RpcErrorCode, Solution, Template, TemplateToken, TemplateTx,
    Tip, TransportError, Utxo, WaitOptions,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::fmt::Debug;
use std::str::FromStr;
use std::time::Duration;

const HASH: &str = "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206";

/// Serializes to `want` and reads back to `v`.
fn shape<T: Serialize + DeserializeOwned + PartialEq + Debug>(v: T, want: Value) {
    let got = serde_json::to_value(&v).unwrap();
    assert_eq!(got, want);
    assert_eq!(serde_json::from_value::<T>(got).unwrap(), v);
}

fn round_trip<T: Serialize + DeserializeOwned + PartialEq + Debug>(v: T) {
    let json = serde_json::to_string(&v).unwrap();
    assert_eq!(serde_json::from_str::<T>(&json).unwrap(), v, "{json}");
}

#[test]
fn events() {
    let hash = BlockHash::from_str(HASH).unwrap();
    let txid = Txid::from_str(HASH).unwrap();
    shape(
        NodeEvent::BlockConnected(hash),
        json!({ "type": "block_connected", "hash": HASH }),
    );
    shape(
        NodeEvent::BlockDisconnected(hash),
        json!({ "type": "block_disconnected", "hash": HASH }),
    );
    shape(
        NodeEvent::TxAdded {
            txid,
            mempool_sequence: Some(7),
        },
        json!({ "type": "tx_added", "txid": HASH, "mempool_sequence": 7 }),
    );
    shape(
        NodeEvent::TxRemoved {
            txid,
            mempool_sequence: None,
        },
        json!({ "type": "tx_removed", "txid": HASH, "mempool_sequence": null }),
    );
    shape(NodeEvent::Lagged, json!({ "type": "lagged" }));
}

#[test]
fn errors() {
    shape(
        NodeAPIError::Rpc {
            code: RpcErrorCode::VerifyError,
            message: "bad".into(),
        },
        json!({ "type": "rpc", "code": -25, "message": "bad" }),
    );
    // A code this crate doesn't name keeps its number.
    shape(
        NodeAPIError::Rpc {
            code: RpcErrorCode::Other(-999),
            message: "new".into(),
        },
        json!({ "type": "rpc", "code": -999, "message": "new" }),
    );
    shape(
        NodeAPIError::Transport(TransportError::Http(502)),
        json!({ "type": "transport", "error": { "type": "http", "status": 502 } }),
    );
    shape(
        NodeAPIError::Transport(TransportError::Timeout),
        json!({ "type": "transport", "error": { "type": "timeout" } }),
    );
    shape(
        NodeAPIError::Decode("x".into()),
        json!({ "type": "decode", "message": "x" }),
    );
    round_trip(NodeAPIError::Unsupported("y".into()));
    round_trip(NodeAPIError::Transport(TransportError::Connect(
        "refused".into(),
    )));
    round_trip(NodeAPIError::Transport(TransportError::Auth));
    round_trip(NodeAPIError::Transport(TransportError::Other("?".into())));
}

#[test]
fn chain_mempool_and_tx_types() {
    let hash = BlockHash::from_str(HASH).unwrap();
    shape(
        Tip { height: 5, hash },
        json!({ "height": 5, "hash": HASH }),
    );
    round_trip(Utxo {
        value: Amount::from_sat(1),
        script_pubkey: ScriptBuf::from_hex("51").unwrap(),
        confirmations: 3,
        coinbase: true,
    });
    shape(
        MempoolEntry {
            vsize: 110,
            weight: Weight::from_wu(437),
            fee: Amount::from_sat(1410),
            time: 1,
            height: 2,
            ancestors: 1,
            descendants: 1,
        },
        json!({ "vsize": 110, "weight": 437, "fee": 1410, "time": 1, "height": 2, "ancestors": 1, "descendants": 1 }),
    );
    round_trip(MempoolAccept {
        txid: Txid::all_zeros(),
        allowed: false,
        reject_reason: Some("dust".into()),
    });
    shape(
        PackageResult {
            txid: Txid::from_str(HASH).unwrap(),
            outcome: PackageOutcome::Accepted {
                fee: Amount::from_sat(500),
            },
        },
        json!({ "txid": HASH, "outcome": { "type": "accepted", "fee": 500 } }),
    );
    shape(
        PackageOutcome::Rejected("too-long-mempool-chain".into()),
        json!({ "type": "rejected", "reason": "too-long-mempool-chain" }),
    );
    round_trip(PackageOutcome::AlreadyInMempool);
    shape(
        FeeTarget {
            blocks: 6,
            mode: FeeMode::Economical,
        },
        json!({ "blocks": 6, "mode": "economical" }),
    );
}

#[test]
fn mining_types() {
    let block = genesis_block(Network::Regtest);
    let cb = block.txdata[0].clone();
    let coinbase = Coinbase {
        version: transaction::Version::TWO,
        sequence: Sequence::MAX,
        script_sig_prefix: ScriptBuf::from_hex("0166").unwrap(),
        witness: Some([0xab; 32]),
        reward_remaining: Amount::from_int_btc(50),
        required_outputs: vec![TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_hex("6a").unwrap(),
        }],
        lock_time: absolute::LockTime::from_consensus(101),
    };
    let json = serde_json::to_value(&coinbase).unwrap();
    assert_eq!(json["witness"], json!("ab".repeat(32)), "witness is hex");
    assert_eq!(
        json["reward_remaining"],
        json!(5_000_000_000u64),
        "satoshis"
    );
    round_trip(coinbase.clone());
    round_trip(Coinbase {
        witness: None,
        ..coinbase.clone()
    });

    let tx = TemplateTx {
        tx: cb.clone(),
        fee: Amount::from_sat(10),
        sigops: 4,
    };
    let json = serde_json::to_value(&tx).unwrap();
    assert_eq!(
        json["tx"],
        json!(bitcoin::consensus::encode::serialize_hex(&cb)),
        "tx is consensus hex"
    );
    round_trip(tx);

    round_trip(Solution {
        version: block.header.version,
        time: 1,
        nonce: 2,
        coinbase: cb,
    });

    shape(
        WaitOptions {
            timeout: Some(Duration::from_millis(1500)),
            fee_threshold: Some(Amount::from_sat(1000)),
        },
        json!({ "timeout": 1500, "fee_threshold": 1000 }),
    );
    round_trip(WaitOptions::default());

    // Serialize only: the token is a live backend handle.
    let tmpl = Template {
        prev_hash: block.block_hash(),
        height: 1,
        version: block.header.version,
        bits: block.header.bits,
        min_time: 0,
        cur_time: 0,
        coinbase,
        coinbase_merkle_path: vec![],
        token: TemplateToken::new(()),
    };
    let json = serde_json::to_value(&tmpl).unwrap();
    assert!(json.get("token").is_none());
    assert_eq!(json["height"], json!(1));
}

/// v1 (frozen, DESIGN.md §3.5): the exact JSON the gateway's REST and
/// WebSocket clients get for every type that was only round-tripped above.
#[test]
fn v1_frozen_shapes() {
    let g = genesis_block(Network::Regtest).header;
    let mut cw = [0; 32];
    cw[31] = 2;
    shape(
        HeaderInfo {
            header: g,
            height: 0,
            chain_work: bitcoin::Work::from_be_bytes(cw),
        },
        json!({
            "header": bitcoin::consensus::encode::serialize_hex(&g),
            "height": 0,
            "chain_work": format!("{:064x}", 2),
        }),
    );
    shape(
        Utxo {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_hex("51").unwrap(),
            confirmations: 3,
            coinbase: true,
        },
        json!({ "value": 1, "script_pubkey": "51", "confirmations": 3, "coinbase": true }),
    );
    shape(
        MempoolAccept {
            txid: Txid::from_str(HASH).unwrap(),
            allowed: false,
            reject_reason: Some("dust".into()),
        },
        json!({ "txid": HASH, "allowed": false, "reject_reason": "dust" }),
    );
    shape(
        MempoolAccept {
            txid: Txid::from_str(HASH).unwrap(),
            allowed: true,
            reject_reason: None,
        },
        json!({ "txid": HASH, "allowed": true, "reject_reason": null }),
    );
    shape(
        bitcoin_node_api::MempoolSnapshot {
            txids: vec![Txid::from_str(HASH).unwrap()],
            sequence: Some(9),
        },
        json!({ "txids": [HASH], "sequence": 9 }),
    );
    shape(
        PackageOutcome::AlreadyInMempool,
        json!({ "type": "already_in_mempool" }),
    );
    shape(
        FeeTarget {
            blocks: 2,
            mode: FeeMode::Conservative,
        },
        json!({ "blocks": 2, "mode": "conservative" }),
    );
    shape(
        NodeAPIError::Unsupported("y".into()),
        json!({ "type": "unsupported", "message": "y" }),
    );
    shape(
        NodeAPIError::Transport(TransportError::Connect("refused".into())),
        json!({ "type": "transport", "error": { "type": "connect", "message": "refused" } }),
    );
    shape(
        NodeAPIError::Transport(TransportError::Auth),
        json!({ "type": "transport", "error": { "type": "auth" } }),
    );
    shape(
        NodeAPIError::Transport(TransportError::Other("?".into())),
        json!({ "type": "transport", "error": { "type": "other", "message": "?" } }),
    );
}
