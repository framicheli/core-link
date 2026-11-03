//! `Wallet` against the fake: routing to `/wallet/<name>`, the params each
//! method builds, and replies from different Core versions.
mod common;

use bitcoin::absolute::LockTime;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::hashes::Hash;
use bitcoin::{
    Address, Amount, BlockHash, FeeRate, Network, OutPoint, Transaction, TxOut, Txid, Weight,
    transaction,
};
use bitcoin_core_rpc::{
    AddressType, Auth, BumpFeeOptions, Client, CreateWalletOptions, FundOptions, ImportDescriptor,
    ListUnspentQuery, LockAction, SendAllOptions, SendManyOptions, SendOptions,
    SendToAddressOptions, Timestamp, Version,
};
use bitcoin_node_api::{FeeMode, NodeAPIError};
use common::{fake_at, ok};
use serde_json::{Value, json};
use std::time::Duration;

fn client(url: &str, version: u32) -> Client {
    Client::with_version(url, Auth::None, Version(version))
}

/// Answers every call with `{name, warnings}` or `null`, wherever it goes.
async fn fake() -> common::Fake {
    fake_at(|_, m, p| match m {
        "createwallet" | "loadwallet" | "restorewallet" => {
            ok(json!({ "name": p[0], "warnings": ["careful"] }))
        }
        "unloadwallet" => ok(json!({})),
        _ => ok(Value::Null),
    })
    .await
}

#[tokio::test]
async fn wallet_calls_go_to_the_wallet_endpoint() {
    let f = fake().await;
    let c = client(&f.url, 310000);
    for name in ["w", "my wallet", "a/b", "ü", "100%"] {
        c.wallet(name).wallet_lock().await.unwrap();
    }
    // A trailing slash on the node URL doesn't double up.
    client(&format!("{}/", f.url), 310000)
        .wallet("w")
        .wallet_lock()
        .await
        .unwrap();
    let paths: Vec<_> = f
        .calls_at("walletlock")
        .into_iter()
        .map(|(p, _)| p)
        .collect();
    assert_eq!(
        paths,
        [
            "/wallet/w",
            "/wallet/my%20wallet",
            "/wallet/a%2Fb",
            "/wallet/%C3%BC",
            "/wallet/100%25",
            "/wallet/w",
        ]
    );

    // Node-level calls stay on the node.
    let loaded = c
        .create_wallet("new", CreateWalletOptions::default())
        .await
        .unwrap();
    assert_eq!(f.calls_at("createwallet")[0].0, "/");
    assert_eq!(loaded.warnings, ["careful"]);
    assert_eq!(loaded.wallet.name(), "new");
    loaded.wallet.wallet_lock().await.unwrap();
    assert_eq!(f.calls_at("walletlock").last().unwrap().0, "/wallet/new");
}

#[tokio::test]
async fn lifecycle_params() {
    let f = fake().await;
    let c = client(&f.url, 310000);
    c.create_wallet("a", CreateWalletOptions::default())
        .await
        .unwrap();
    let opts = CreateWalletOptions::default()
        .disable_private_keys(true)
        .blank(true)
        .passphrase("secret")
        .avoid_reuse(true)
        .load_on_startup(false)
        .external_signer(true);
    assert!(!format!("{opts:?}").contains("secret"), "{opts:?}");
    c.create_wallet("b", opts).await.unwrap();
    assert_eq!(
        f.calls("createwallet"),
        [
            json!(["a", false, false, null, false, true, null, false]),
            json!(["b", true, true, "secret", true, true, false, true]),
        ]
    );

    c.load_wallet("a", None).await.unwrap();
    c.load_wallet("a", Some(true)).await.unwrap();
    assert_eq!(f.calls("loadwallet"), [json!(["a"]), json!(["a", true])]);
    c.restore_wallet("r", "/tmp/a.bak", None).await.unwrap();
    assert_eq!(f.calls("restorewallet"), [json!(["r", "/tmp/a.bak"])]);

    let w = c.wallet("a");
    assert!(w.unload_wallet(None).await.unwrap().is_empty());
    w.unload_wallet(Some(false)).await.unwrap();
    // The name comes from the endpoint, not a param.
    assert_eq!(f.calls("unloadwallet"), [json!([]), json!([null, false])]);

    w.wallet_passphrase("pw", Duration::from_secs(90))
        .await
        .unwrap();
    assert_eq!(f.calls("walletpassphrase"), [json!(["pw", 90])]);
    w.keypool_refill(None).await.unwrap();
    w.keypool_refill(Some(5)).await.unwrap();
    assert_eq!(f.calls("keypoolrefill"), [json!([]), json!([5])]);
    c.migrate_wallet("old", None).await.unwrap_err();
    c.migrate_wallet("old", Some("pw")).await.unwrap_err();
    assert_eq!(
        f.calls("migratewallet"),
        [json!(["old"]), json!(["old", "pw"])]
    );
}

/// Shaped as Core 29.5 and 31.2 reply on regtest after mining 101 blocks.
fn wallet_info(major: u32) -> Value {
    let mut v = json!({
        "walletname": "w", "walletversion": 169900, "format": "sqlite", "txcount": 101,
        "keypoolsize": 4000, "keypoolsize_hd_internal": 4000, "private_keys_enabled": true,
        "avoid_reuse": false, "scanning": false, "descriptors": true, "external_signer": false,
        "blank": false, "birthtime": 1793383816,
        "lastprocessedblock": { "hash": "684be3040acef7d2e1f5414952bd0ab6a6535a02fab88dad3ba36429dfc63949", "height": 101 }
    });
    if major < 30 {
        v["balance"] = json!(50.0);
        v["unconfirmed_balance"] = json!(0.0);
        v["immature_balance"] = json!(5000.0);
        v["paytxfee"] = json!(0.0);
    } else {
        v["flags"] = json!(["last_hardened_xpub_cached", "descriptor_wallet"]);
    }
    v
}

#[tokio::test]
async fn wallet_info_and_dir_parse_core_29_and_31() {
    // The path names the version, so one fake serves both.
    let f = fake_at(|path, m, _| {
        let major = if path.ends_with("29") { 29 } else { 31 };
        match m {
            "getwalletinfo" => ok(wallet_info(major)),
            "listwalletdir" => {
                ok(json!({ "wallets": [{ "name": "w", "warnings": [] }, { "name": "x" }] }))
            }
            _ => None,
        }
    })
    .await;
    let old = client(&f.url, 290500)
        .wallet("29")
        .get_wallet_info()
        .await
        .unwrap();
    let new = client(&f.url, 310200)
        .wallet("31")
        .get_wallet_info()
        .await
        .unwrap();
    assert_eq!(old.balance, Some(Amount::from_int_btc(50)));
    assert_eq!(new.balance, None);
    assert_eq!(old.tx_count, new.tx_count);
    assert_eq!(new.flags.as_ref().map(Vec::len), Some(2));

    // Core 29 sends no `warnings`, 31 does.
    let dir = client(&f.url, 290500).list_wallet_dir().await.unwrap();
    assert_eq!(dir.wallets.len(), 2);
}

fn regtest_address() -> Address {
    "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080"
        .parse::<Address<_>>()
        .unwrap()
        .require_network(Network::Regtest)
        .unwrap()
}

#[tokio::test]
async fn coin_and_history_params() {
    let f = fake_at(|_, m, _| match m {
        "listunspent" | "listsinceblock" => ok(match m {
            "listunspent" => json!([]),
            _ => json!({ "transactions": [], "removed": [], "lastblock": "00".repeat(32) }),
        }),
        "lockunspent" => ok(json!(true)),
        "getbalance" => ok(json!(1.5)),
        "getnewaddress" => ok(json!("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080")),
        _ => None,
    })
    .await;
    let w = client(&f.url, 310000).wallet("w");
    let a = regtest_address();

    w.list_unspent(None, None, &[], None, ListUnspentQuery::default())
        .await
        .unwrap();
    let q = ListUnspentQuery::default()
        .minimum_amount(Amount::from_sat(12_345))
        .maximum_count(3);
    w.list_unspent(Some(0), None, std::slice::from_ref(&a), None, q)
        .await
        .unwrap();
    assert_eq!(
        f.calls("listunspent"),
        [
            json!([]),
            // Unset query fields are left out, not null.
            json!([0, null, [a.to_string()], null, { "minimumAmount": 0.00012345, "maximumCount": 3 }]),
        ]
    );

    let op = OutPoint {
        txid: Txid::all_zeros(),
        vout: 1,
    };
    w.lock_unspent(LockAction::Lock, &[op], Some(true))
        .await
        .unwrap();
    w.lock_unspent(LockAction::Unlock, &[], None).await.unwrap();
    assert_eq!(
        f.calls("lockunspent"),
        [
            json!([false, [{ "txid": op.txid, "vout": 1 }], true]),
            // Omitted, not `[]`: only that unlocks everything.
            json!([true]),
        ]
    );

    assert_eq!(
        w.get_balance(Some(2), None).await.unwrap(),
        Amount::from_sat(150_000_000)
    );
    assert_eq!(f.calls("getbalance"), [json!([null, 2])]);

    let hash = BlockHash::all_zeros();
    w.list_since_block(None, None, None, None, None)
        .await
        .unwrap();
    w.list_since_block(Some(hash), None, Some(false), None, Some("l"))
        .await
        .unwrap();
    assert_eq!(
        f.calls("listsinceblock"),
        [json!([]), json!([hash, null, null, false, null, "l"]),]
    );

    w.get_new_address(None, Some(AddressType::P2shSegwit))
        .await
        .unwrap();
    assert_eq!(f.calls("getnewaddress"), [json!([null, "p2sh-segwit"])]);
}

#[tokio::test]
async fn balances_parse_with_and_without_watch_only() {
    let mine = json!({ "trusted": 50.0, "untrusted_pending": 0.0, "immature": 5000.0 });
    let f = fake_at(move |path, m, _| match m {
        // A Core 29 legacy wallet still reports `watchonly`; Core 30 dropped it.
        "getbalances" if path.ends_with("29") => ok(json!({
            "mine": mine, "watchonly": { "trusted": 1.0, "untrusted_pending": 0.0, "immature": 0.0 }
        })),
        "getbalances" => ok(json!({ "mine": mine })),
        _ => None,
    })
    .await;
    let old = client(&f.url, 290500)
        .wallet("29")
        .get_balances()
        .await
        .unwrap();
    let new = client(&f.url, 310200)
        .wallet("31")
        .get_balances()
        .await
        .unwrap();
    assert_eq!(old.mine, new.mine);
    assert_eq!(old.watch_only.unwrap().trusted, Amount::from_int_btc(1));
    assert!(new.watch_only.is_none());
}

fn other_address() -> Address {
    "bcrt1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3qzf4jry"
        .parse::<Address<_>>()
        .unwrap()
        .require_network(Network::Regtest)
        .unwrap()
}

const TXID: &str = "0000000000000000000000000000000000000000000000000000000000000001";

/// Replies just well-formed enough for each spending RPC's model.
async fn spending_fake() -> common::Fake {
    fake_at(|_, m, _| match m {
        "send" | "sendall" => ok(json!({ "complete": true, "txid": TXID })),
        "sendtoaddress" | "sendmany" => ok(json!(TXID)),
        "bumpfee" => ok(json!({ "txid": TXID, "origfee": 0.0001, "fee": 0.0002, "errors": [] })),
        "importdescriptors" => ok(json!([{ "success": true }])),
        _ => None,
    })
    .await
}

#[tokio::test]
async fn send_params() {
    let f = spending_fake().await;
    let w = client(&f.url, 310000).wallet("w");
    let (a, b) = (regtest_address(), other_address());
    // `b` sorts before `a`: caller order must survive.
    assert!(b.to_string() < a.to_string());

    w.send(
        &[(a.clone(), Amount::ONE_BTC)],
        None,
        SendOptions::default(),
    )
    .await
    .unwrap();
    let op = OutPoint {
        txid: Txid::all_zeros(),
        vout: 3,
    };
    let opts = SendOptions::default()
        .fund(
            FundOptions::default()
                .fee_rate(FeeRate::from_sat_per_kwu(253))
                .change_type(AddressType::Bech32m)
                .subtract_fee_from_outputs(vec![1])
                .input_weights(vec![(op, Weight::from_wu(300))])
                .estimate_mode(FeeMode::Economical),
        )
        .inputs(vec![op])
        .add_to_wallet(false)
        .locktime(LockTime::ZERO)
        .version(transaction::Version(3));
    w.send(
        &[
            (a.clone(), Amount::from_sat(12_345)),
            (b.clone(), Amount::from_sat(2_099_999_999_999_999)),
        ],
        Some(b"hi"),
        opts,
    )
    .await
    .unwrap();
    assert_eq!(
        f.calls("send"),
        [
            json!([[{ a.to_string(): 1.0 }]]),
            json!([
                [{ a.to_string(): 0.00012345 }, { b.to_string(): 20999999.99999999 }, { "data": "6869" }],
                null, null, null,
                {
                    // 253 sat/kwu = 1.012 sat/vB
                    "fee_rate": 1.012,
                    "change_type": "bech32m",
                    "estimate_mode": "economical",
                    "subtract_fee_from_outputs": [1],
                    "add_to_wallet": false,
                    // The weight rides on the input, not in `input_weights`.
                    "inputs": [{ "txid": op.txid, "vout": 3, "weight": 300 }],
                    "locktime": 0,
                },
                3
            ]),
        ]
    );

    let opts = SendAllOptions::default()
        .send_max(true)
        .fee_rate(FeeRate::from_sat_per_vb_u32(2))
        .version(transaction::Version::TWO);
    w.send_all(
        &[(b.clone(), Some(Amount::ONE_BTC)), (a.clone(), None)],
        opts,
    )
    .await
    .unwrap();
    assert_eq!(
        f.calls("sendall"),
        [json!([
            [{ b.to_string(): 1.0 }, a.to_string()],
            null, null, null,
            { "send_max": true, "version": 2, "fee_rate": 2.0 }
        ])]
    );

    let opts = SendToAddressOptions::default()
        .comment("c")
        .conf_target(3)
        .estimate_mode(FeeMode::Conservative);
    w.send_to_address(&a, Amount::ONE_BTC, opts).await.unwrap();
    assert_eq!(
        f.calls("sendtoaddress"),
        [json!([
            a.to_string(),
            1.0,
            "c",
            null,
            null,
            null,
            3,
            "conservative"
        ])]
    );

    let opts = SendManyOptions::default().subtract_fee_from(vec![a.clone()]);
    w.send_many(&[(a.clone(), Amount::ONE_BTC)], opts)
        .await
        .unwrap();
    assert_eq!(
        f.calls("sendmany"),
        [json!(["", { a.to_string(): 1.0 }, null, null, [a.to_string()]])]
    );
}

#[tokio::test]
async fn tx_version_needs_core_30() {
    let f = spending_fake().await;
    let w = client(&f.url, 290500).wallet("w");
    let v = transaction::Version(3);
    let out = [(regtest_address(), Amount::ONE_BTC)];
    let err = w
        .send(&out, None, SendOptions::default().version(v))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeAPIError::Unsupported(_)), "{err}");
    let err = w
        .send_all(
            &[(out[0].0.clone(), None)],
            SendAllOptions::default().version(v),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, NodeAPIError::Unsupported(_)), "{err}");
    let err = w
        .wallet_create_funded_psbt(&[], &out, None, None, FundOptions::default(), None, Some(v))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeAPIError::Unsupported(_)), "{err}");
    // Refused without asking the node; unset, it's never sent.
    assert!(f.calls("send").is_empty() && f.calls("sendall").is_empty());
    w.send(&out, None, SendOptions::default()).await.unwrap();
    assert_eq!(f.calls("send")[0].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn funding_and_bumping_params() {
    let f = spending_fake().await;
    let w = client(&f.url, 310000).wallet("w");
    let a = regtest_address();
    let op = OutPoint {
        txid: Txid::all_zeros(),
        vout: 0,
    };
    let opts = FundOptions::default()
        .add_inputs(true)
        .min_conf(1)
        .change_address(a.clone())
        .change_position(0)
        .lock_unspents(true)
        .input_weights(vec![(op, Weight::from_wu(272))])
        .max_tx_weight(Weight::from_wu(4000));
    let unfunded = Transaction {
        version: transaction::Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![],
        output: vec![TxOut {
            value: Amount::ONE_BTC,
            script_pubkey: a.script_pubkey(),
        }],
    };
    // No reply to parse; only the params matter here.
    let _ = w.fund_raw_transaction(&unfunded, opts.clone()).await;
    let legacy = format!(
        "02000000000100e1f50500000000{}00000000",
        bitcoin::consensus::encode::serialize_hex(&a.script_pubkey())
    );
    assert_ne!(
        serialize_hex(&unfunded),
        legacy,
        "rust-bitcoin writes segwit form"
    );
    assert_eq!(
        f.calls("fundrawtransaction"),
        [json!([
            legacy,
            {
                "add_inputs": true, "minconf": 1, "change_address": a.to_string(),
                "change_position": 0, "lock_unspents": true,
                // No input list here: weights go in the options.
                "input_weights": [{ "txid": op.txid, "vout": 0, "weight": 272 }],
                "max_tx_weight": 4000,
            },
            false
        ])]
    );

    let _ = w
        .wallet_create_funded_psbt(
            &[op],
            &[(a.clone(), Amount::ONE_BTC)],
            None,
            None,
            opts,
            Some(false),
            None,
        )
        .await;
    let params = &f.calls("walletcreatefundedpsbt")[0];
    assert_eq!(
        params[0],
        json!([{ "txid": op.txid, "vout": 0, "weight": 272 }])
    );
    assert!(params[3].get("input_weights").is_none(), "{params}");
    assert_eq!(params[4], json!(false));
    assert_eq!(params.as_array().unwrap().len(), 5);

    let opts = BumpFeeOptions::default()
        .fee_rate(FeeRate::from_sat_per_vb_u32(25))
        .outputs(vec![(a.clone(), Amount::from_sat(1000))])
        .replaceable(false);
    w.bump_fee(op.txid, BumpFeeOptions::default())
        .await
        .unwrap();
    w.bump_fee(op.txid, opts).await.unwrap();
    assert_eq!(
        f.calls("bumpfee"),
        [
            json!([op.txid]),
            json!([op.txid, { "fee_rate": 25.0, "replaceable": false, "outputs": [{ a.to_string(): 0.00001 }] }]),
        ]
    );

    let reqs = [
        ImportDescriptor::new("addr(x)#y", Timestamp::Now).label("l"),
        ImportDescriptor::new("wpkh(z/*)#w", Timestamp::Time(0))
            .active(true)
            .range(0, 99)
            .next_index(5)
            .internal(true),
    ];
    w.import_descriptors(&reqs).await.unwrap();
    assert_eq!(
        f.calls("importdescriptors"),
        [json!([[
            { "desc": "addr(x)#y", "timestamp": "now", "label": "l" },
            { "desc": "wpkh(z/*)#w", "timestamp": 0, "active": true, "range": [0, 99], "next_index": 5, "internal": true },
        ]])]
    );
}

#[tokio::test]
async fn zero_input_tx_from_core_decodes() {
    // Core's `createrawtransaction` with no inputs: legacy form.
    let a = regtest_address();
    let hex = format!(
        "02000000000100e1f50500000000{}00000000",
        serialize_hex(&a.script_pubkey())
    );
    let f = fake_at(move |_, m, _| {
        (m == "createrawtransaction")
            .then(|| ok(json!(hex)))
            .flatten()
    })
    .await;
    let tx = client(&f.url, 310000)
        .create_raw_transaction(&[], &[(a.clone(), Amount::ONE_BTC)], None, None)
        .await
        .unwrap();
    assert!(tx.input.is_empty());
    assert_eq!(tx.output[0].script_pubkey, a.script_pubkey());
}
