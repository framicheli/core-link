//! The RPC backend against the fake bitcoind in `common`.
mod common;

use bitcoin::block::{Header, Version as BlockVersion};
use bitcoin::blockdata::constants::genesis_block;
use bitcoin::consensus::encode::{deserialize_hex, serialize_hex};
use bitcoin::hashes::Hash;
use bitcoin::{
    Amount, Block, BlockHash, CompactTarget, FeeRate, Network, OutPoint, Transaction, TxMerkleNode,
    Weight, Work, absolute,
};
use bitcoin_core_rpc::{Auth, Client, Feature, Version};
use bitcoin_node_api::*;
use common::{Reply, fake, fake_with, fixture, ok, rpc_err};
use futures_core::Stream;
use serde_json::{Map, Value, json};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn client(url: &str) -> Client {
    Client::with_version(url, Auth::None, Version(310000))
}

fn genesis() -> Block {
    genesis_block(Network::Regtest)
}

/// `n` distinct, decodable transactions.
fn txs(n: u32) -> Vec<Transaction> {
    let cb = genesis().txdata[0].clone();
    (0..n)
        .map(|i| Transaction {
            lock_time: absolute::LockTime::from_consensus(i + 1),
            ..cb.clone()
        })
        .collect()
}

/// The GBT fixture with `txs` patched in (fee 1000·(i+1), 4 sigops each).
fn gbt(longpollid: &str, height: u64, txs: &[Transaction]) -> Value {
    let mut v = fixture("getblocktemplate").unwrap();
    v["longpollid"] = json!(longpollid);
    v["height"] = json!(height);
    v["transactions"] = txs
        .iter()
        .enumerate()
        .map(|(i, t)| {
            json!({
                "data": serialize_hex(t), "txid": t.compute_txid(), "hash": t.compute_wtxid(),
                "depends": [], "fee": 1000 * (i + 1), "sigops": 4, "weight": t.weight().to_wu(),
            })
        })
        .collect();
    v
}

async fn next<S: Stream + Unpin>(s: &mut S) -> Option<S::Item> {
    std::future::poll_fn(|cx| Pin::new(&mut *s).poll_next(cx)).await
}

#[tokio::test]
async fn connect_detects_version() {
    let f = fake().await;
    let c = Client::connect(&f.url, Auth::None).await.unwrap();
    assert_eq!(c.version().major(), 31);
    assert!(c.supports(Feature::ClusterMempool));
    assert!(!Version(300400).supports(Feature::ClusterMempool));
}

#[tokio::test]
async fn connect_rejects_core_below_29() {
    // Core 28 reached end of life when 31.0 shipped.
    let f = fake_with(|m, _| match m {
        "getnetworkinfo" => ok(json!({ "version": 280400 })),
        _ => None,
    })
    .await;
    let err = Client::connect(&f.url, Auth::None).await.unwrap_err();
    match err {
        NodeAPIError::Unsupported(m) => assert!(m.contains("minimum 29"), "{m}"),
        e => panic!("{e}"),
    }
}

#[tokio::test]
async fn transport_errors_map() {
    let f = fake_with(|m, _| match m {
        "auth" => Some(Reply {
            status: 401,
            body: String::new(),
            delay: Duration::ZERO,
        }),
        "html" => Some(Reply {
            status: 500,
            body: "<html>oops</html>".into(),
            delay: Duration::ZERO,
        }),
        _ => None,
    })
    .await;
    let c = client(&f.url);
    let call = |m: &'static str| {
        let c = c.clone();
        async move { c.raw_call::<Value>(m, json!([])).await.unwrap_err() }
    };
    assert_eq!(call("auth").await, TransportError::Auth.into());
    assert_eq!(call("html").await, TransportError::Http(500).into());
    assert!(matches!(
        call("nope").await,
        NodeAPIError::Rpc {
            code: RpcErrorCode::MethodNotFound,
            ..
        }
    ));

    let dead = client("http://127.0.0.1:1");
    match dead.raw_call::<Value>("getblockcount", json!([])).await {
        Err(NodeAPIError::Transport(e)) => assert!(e.is_retryable(), "{e}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn chain_source_round_trips_genesis() {
    let g = genesis();
    let served = g.clone();
    let f = fake_with(move |m, _| match m {
        "getblockhash" => ok(json!(served.block_hash())),
        "getblockheader" => ok(json!(serialize_hex(&served.header))),
        "getblock" => ok(json!(serialize_hex(&served))),
        "getrawtransaction" => ok(json!(serialize_hex(&served.txdata[0]))),
        _ => None,
    })
    .await;
    let c = client(&f.url);

    let tip = c.tip().await.unwrap();
    assert_eq!(
        tip,
        Tip {
            height: 0,
            hash: g.block_hash()
        }
    );
    assert_eq!(c.block_hash(0).await.unwrap(), g.block_hash());
    assert_eq!(c.header(tip.hash).await.unwrap(), g.header);
    assert_eq!(c.block(tip.hash).await.unwrap(), g);

    let cb = &g.txdata[0];
    let got = c
        .transaction(cb.compute_txid(), Some(tip.hash))
        .await
        .unwrap();
    assert_eq!(&got, cb);
    assert_eq!(
        f.calls("getrawtransaction")[0],
        json!([cb.compute_txid(), 0, tip.hash])
    );
}

/// Core 31's verbose `getblockheader` for `h` at `height`.
fn verbose_header(h: &Header, height: u64, chainwork: &str) -> Value {
    let mut v = json!({
        "hash": h.block_hash(),
        "confirmations": 1,
        "height": height,
        "version": h.version.to_consensus(),
        "versionHex": format!("{:08x}", h.version.to_consensus()),
        "merkleroot": h.merkle_root,
        "time": h.time,
        "mediantime": h.time,
        "nonce": h.nonce,
        "bits": format!("{:08x}", h.bits.to_consensus()),
        "target": format!("{:064x}", h.target()),
        "difficulty": 4.656542373906925e-10,
        "chainwork": chainwork,
        "nTx": 1,
    });
    if h.prev_blockhash != BlockHash::all_zeros() {
        v["previousblockhash"] = json!(h.prev_blockhash);
    }
    v
}

#[tokio::test]
async fn header_info_from_one_verbose_call() {
    let g = genesis().header;
    let h1 = Header {
        version: BlockVersion::from_consensus(0x2000_0000),
        prev_blockhash: g.block_hash(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: 1296688603,
        bits: CompactTarget::from_consensus(0x207f_ffff),
        nonce: 7,
    };
    let mut lying = h1;
    lying.nonce = 8;
    let (hg, hh1, hl) = (g.block_hash(), h1.block_hash(), lying.block_hash());
    let f = fake_with(move |m, p| match (m, p[0].as_str()) {
        ("getblockheader", Some(h)) if h == hg.to_string() => {
            ok(verbose_header(&g, 0, &format!("{:064x}", 2)))
        }
        ("getblockheader", Some(h)) if h == hh1.to_string() => {
            ok(verbose_header(&h1, 1, &format!("{:064x}", 4)))
        }
        // Fields that don't hash to the asked-for block.
        ("getblockheader", Some(h)) if h == hl.to_string() => {
            ok(verbose_header(&h1, 1, &format!("{:064x}", 4)))
        }
        _ => None,
    })
    .await;
    let c = client(&f.url);

    let work = |n: u8| {
        let mut b = [0; 32];
        b[31] = n;
        Work::from_be_bytes(b)
    };
    let i = c.header_info(hg).await.unwrap();
    assert_eq!((i.header, i.height, i.chain_work), (g, 0, work(2)));
    let i = c.header_info(hh1).await.unwrap();
    assert_eq!((i.header, i.height, i.chain_work), (h1, 1, work(4)));
    assert_eq!(f.calls("getblockheader")[1], json!([hh1, true]));
    assert!(matches!(
        c.header_info(hl).await,
        Err(NodeAPIError::Decode(_))
    ));
}

#[tokio::test]
async fn block_filter_and_no_index() {
    let indexed = genesis().block_hash();
    let f = fake_with(move |m, p| match (m, p[0].as_str()) {
        ("getblockfilter", Some(h)) if h == indexed.to_string() => ok(json!({
            "filter": "017fa880",
            "header": "2b5adc66021d5c775f630efd91518cf6ce3e9f525bbf54d9f0d709451e305e48",
        })),
        ("getblockfilter", _) => rpc_err(-1, "Index is not enabled for filtertype basic"),
        _ => None,
    })
    .await;
    let c = client(&f.url);
    let filter = c.block_filter(indexed).await.unwrap();
    assert_eq!(filter.content, [0x01, 0x7f, 0xa8, 0x80]);
    assert_eq!(f.calls("getblockfilter")[0], json!([indexed, "basic"]));
    match c.block_filter(BlockHash::all_zeros()).await {
        Err(NodeAPIError::Unsupported(m)) => assert!(m.contains("-blockfilterindex"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn utxo_unspent_spent_unknown() {
    let f = fake_with(|m, p| match (m, p[1].as_u64()) {
        ("gettxout", Some(1)) => ok(Value::Null),
        ("gettxout", Some(2)) => rpc_err(-5, "No such mempool or blockchain transaction"),
        _ => None,
    })
    .await;
    let c = client(&f.url);
    let cb = &genesis().txdata[0];
    let op = |vout| OutPoint {
        txid: cb.compute_txid(),
        vout,
    };

    let u = c.utxo(op(0), true).await.unwrap().unwrap();
    assert_eq!(
        u,
        Utxo {
            value: Amount::from_int_btc(50),
            script_pubkey: cb.output[0].script_pubkey.clone(),
            confirmations: 1,
            coinbase: true,
        }
    );
    assert_eq!(f.calls("gettxout")[0], json!([cb.compute_txid(), 0, true]));
    assert_eq!(c.utxo(op(1), false).await.unwrap(), None, "spent");
    assert_eq!(c.utxo(op(2), false).await.unwrap(), None, "unknown");
}

#[tokio::test]
async fn mempool_source() {
    let unknown = genesis().txdata[0].compute_txid();
    let f = fake_with(move |m, p| match m {
        "getmempoolentry" if p[0] == json!(unknown) => rpc_err(-5, "Transaction not in mempool"),
        _ => None,
    })
    .await;
    let c = client(&f.url);

    assert_eq!(c.txids().await.unwrap().len(), 2);
    let some_txid = c.txids().await.unwrap()[1];
    assert_eq!(
        c.entry(some_txid).await.unwrap(),
        Some(MempoolEntry {
            vsize: 141,
            weight: Weight::from_wu(561),
            fee: Amount::from_sat(1410),
            time: 1730000000,
            height: 101,
            ancestors: 2,
            descendants: 1,
        })
    );
    assert_eq!(c.entry(unknown).await.unwrap(), None);
}

#[tokio::test]
async fn broadcast_and_test_accept() {
    let tx = txs(1).remove(0);
    let txid = tx.compute_txid();
    let f = fake_with(move |m, _| match m {
        "sendrawtransaction" => ok(json!(txid)),
        _ => None,
    })
    .await;
    let c = client(&f.url);

    assert_eq!(c.broadcast(&tx).await.unwrap(), txid);
    assert_eq!(
        f.calls("sendrawtransaction")[0],
        json!([serialize_hex(&tx)])
    );

    let r = c.test_accept(std::slice::from_ref(&tx)).await.unwrap();
    assert!(!r[0].allowed);
    assert_eq!(
        r[0].reject_reason.as_deref(),
        Some("min relay fee not met, 0 < 141")
    );
}

#[tokio::test]
async fn broadcast_package_outcome_per_tx() {
    let t = txs(5);
    let w: Vec<String> = t.iter().map(|t| t.compute_wtxid().to_string()).collect();
    let mut results = Map::new();
    results.insert(w[0].clone(), json!({ "txid": w[0], "vsize": 60,
        "fees": { "base": 0.00001000, "effective-feerate": 0.00016666, "effective-includes": [w[0]] } }));
    results.insert(
        w[1].clone(),
        json!({ "txid": w[1], "vsize": 60, "fees": { "base": 0.00000500 } }),
    );
    results.insert(
        w[2].clone(),
        json!({ "txid": w[2], "error": "bad-txns-inputs-missingorspent" }),
    );
    results.insert(w[3].clone(), json!({ "txid": w[3], "other-wtxid": w[0] }));
    // w[4] missing: Core stopped before it.
    let body = json!({ "package_msg": "transaction failed", "tx-results": results, "replaced-transactions": [] });
    let f = fake_with(move |m, _| match m {
        "submitpackage" => ok(body.clone()),
        _ => None,
    })
    .await;

    let r = client(&f.url).broadcast_package(&t).await.unwrap();
    let outcomes: Vec<_> = r.iter().map(|r| r.outcome.clone()).collect();
    assert_eq!(
        outcomes,
        [
            PackageOutcome::Accepted {
                fee: Amount::from_sat(1000)
            },
            PackageOutcome::AlreadyInMempool,
            PackageOutcome::Rejected("bad-txns-inputs-missingorspent".into()),
            PackageOutcome::AlreadyInMempool,
            PackageOutcome::Rejected("transaction failed".into()),
        ]
    );
    assert_eq!(r[2].txid, t[2].compute_txid());

    let old = Client::with_version(&f.url, Auth::None, Version(270000));
    let err = old.broadcast_package(&t).await.unwrap_err();
    assert!(matches!(err, NodeAPIError::Unsupported(_)), "{err}");
    assert_eq!(f.calls("submitpackage").len(), 1, "no request on Core 27");
}

#[tokio::test]
async fn estimate_fee() {
    let f = fake_with(|m, p| match m {
        "estimatesmartfee" if p[0] == 1 => ok(json!({
            "errors": ["Insufficient data or no feerate found"], "blocks": 2
        })),
        _ => None,
    })
    .await;
    let c = client(&f.url);

    let target = |blocks| FeeTarget {
        blocks,
        mode: FeeMode::Economical,
    };
    // 0.00012 BTC/kvB = 12000 sat/kvB = 3000 sat/kwu.
    assert_eq!(
        c.estimate_fee(target(6)).await.unwrap(),
        Some(FeeRate::from_sat_per_kwu(3000))
    );
    assert_eq!(f.calls("estimatesmartfee")[0], json!([6, "ECONOMICAL"]));
    assert_eq!(c.estimate_fee(target(1)).await.unwrap(), None);
}

#[tokio::test]
async fn template_is_lean_and_transactions_decode() {
    let t = txs(3);
    let body = gbt("lp0", 1, &t);
    let f = fake_with(move |m, _| match m {
        "getblocktemplate" => ok(body.clone()),
        _ => None,
    })
    .await;
    let c = client(&f.url);

    let tmpl = c.template().await.unwrap();
    assert_eq!(
        f.calls("getblocktemplate")[0],
        json!([{ "rules": ["segwit"] }])
    );
    assert_eq!(tmpl.prev_hash, genesis().block_hash());
    assert_eq!(tmpl.height, 1);
    assert_eq!(tmpl.version, BlockVersion::from_consensus(0x2000_0000));
    assert_eq!(tmpl.bits, CompactTarget::from_consensus(0x207f_ffff));
    assert_eq!((tmpl.min_time, tmpl.cur_time), (1296688603, 1730000000));

    let cb = &tmpl.coinbase;
    assert_eq!(
        cb.script_sig_prefix.as_bytes(),
        [0x51],
        "BIP34 height 1 = OP_1"
    );
    assert_eq!(cb.witness, Some([0; 32]));
    assert_eq!(cb.reward_remaining, Amount::from_sat(5_000_000_000));
    assert_eq!(cb.required_outputs.len(), 1);
    assert_eq!(cb.required_outputs[0].value, Amount::ZERO);
    assert!(
        cb.required_outputs[0]
            .script_pubkey
            .to_hex_string()
            .starts_with("6a24aa21a9ed")
    );
    assert_eq!(tmpl.coinbase_merkle_path.len(), 2, "4 leaves");

    let decoded = c.transactions(&tmpl).await.unwrap();
    let got: Vec<_> = decoded
        .iter()
        .map(|d| (&d.tx, d.fee.to_sat(), d.sigops))
        .collect();
    assert_eq!(got, [(&t[0], 1000, 4), (&t[1], 2000, 4), (&t[2], 3000, 4)]);
}

#[tokio::test]
async fn submit_solution_rebuilds_block() {
    let t = txs(2);
    let body = gbt("lp0", 1, &t);
    let n = Arc::new(AtomicUsize::new(0));
    let f = fake_with(move |m, _| match m {
        "getblocktemplate" => ok(body.clone()),
        "submitblock" => ok(match n.fetch_add(1, Ordering::SeqCst) {
            0 => Value::Null,
            1 => json!("duplicate"),
            _ => json!("high-hash"),
        }),
        _ => None,
    })
    .await;
    let c = client(&f.url);
    let tmpl = c.template().await.unwrap();
    let sol = Solution {
        version: BlockVersion::from_consensus(0x2000_0004),
        time: 1730000001,
        nonce: 7,
        coinbase: genesis().txdata[0].clone(),
    };

    c.submit_solution(&tmpl, &sol).await.unwrap();
    let hex = f.calls("submitblock")[0][0].as_str().unwrap().to_owned();
    let block: Block = deserialize_hex(&hex).unwrap();
    assert_eq!(block.header.prev_blockhash, tmpl.prev_hash);
    assert_eq!(block.header.bits, tmpl.bits);
    assert_eq!(
        (block.header.version, block.header.time, block.header.nonce),
        (sol.version, sol.time, sol.nonce)
    );
    assert_eq!(
        block.txdata,
        [sol.coinbase.clone(), t[0].clone(), t[1].clone()]
    );
    assert!(block.check_merkle_root());

    c.submit_solution(&tmpl, &sol).await.unwrap();
    let err = c.submit_solution(&tmpl, &sol).await.unwrap_err();
    assert_eq!(
        err,
        NodeAPIError::Rpc {
            code: RpcErrorCode::VerifyError,
            message: "high-hash".into()
        }
    );
}

#[tokio::test]
async fn wait_next_shares_one_longpoll() {
    let t = txs(1);
    let f = fake_with(move |m, p| match m {
        "getblocktemplate" if p[0].get("longpollid").is_some() => Some(Reply {
            delay: Duration::from_millis(300),
            ..ok(gbt("lp1", 2, &[])).unwrap()
        }),
        "getblocktemplate" => ok(gbt("lp0", 1, &t)),
        _ => None,
    })
    .await;
    let c = client(&f.url);
    let tmpl = c.template().await.unwrap();
    let short = WaitOptions {
        timeout: Some(Duration::from_millis(50)),
        ..Default::default()
    };

    // A gives up after 50ms then waits again; B waits with no deadline. One request in total.
    let a = async {
        let first = c.wait_next(&tmpl, short).await.unwrap();
        (
            first,
            c.wait_next(&tmpl, WaitOptions::default()).await.unwrap(),
        )
    };
    let ((a1, a2), b) = tokio::join!(a, c.wait_next(&tmpl, WaitOptions::default()));
    assert_eq!(a1, None);
    assert_eq!(a2.unwrap().height, 2);
    assert_eq!(b.unwrap().unwrap().height, 2);

    let longpolls = f.calls("getblocktemplate");
    let longpolls: Vec<_> = longpolls
        .iter()
        .filter(|p| p[0].get("longpollid").is_some())
        .collect();
    assert_eq!(longpolls.len(), 1);
    assert_eq!(longpolls[0][0]["longpollid"], "lp0");
}

#[tokio::test]
async fn events_follow_tip() {
    let h1 = Header {
        version: BlockVersion::from_consensus(0x2000_0000),
        prev_blockhash: genesis().block_hash(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: 1296688603,
        bits: CompactTarget::from_consensus(0x207f_ffff),
        nonce: 0,
    };
    let far = BlockHash::from_byte_array([3; 32]);
    let n = Arc::new(AtomicUsize::new(0));
    let f = fake_with(move |m, _| match m {
        "getblockheader" => ok(json!(serialize_hex(&h1))),
        "waitfornewblock" => Some(Reply {
            delay: Duration::from_millis(20),
            ..ok(match n.fetch_add(1, Ordering::SeqCst) {
                0 => json!({ "hash": h1.block_hash(), "height": 1 }),
                // Jumped two blocks: the consumer must resync.
                _ => json!({ "hash": far, "height": 3 }),
            })
            .unwrap()
        }),
        _ => None,
    })
    .await;
    let mut events = client(&f.url).subscribe().await.unwrap();

    assert_eq!(
        next(&mut events).await.unwrap().unwrap(),
        NodeEvent::BlockConnected(h1.block_hash())
    );
    assert_eq!(next(&mut events).await.unwrap().unwrap(), NodeEvent::Lagged);
}

/// The RPC error code of `r`, which must be an `Err(Rpc)`.
fn rpc_code<T: std::fmt::Debug>(r: ApiResult<T>) -> RpcErrorCode {
    match r {
        Err(NodeAPIError::Rpc { code, .. }) => code,
        other => panic!("want an rpc error, got {other:?}"),
    }
}

fn unsupported<T: std::fmt::Debug>(r: ApiResult<T>) {
    assert!(matches!(r, Err(NodeAPIError::Unsupported(_))), "{r:?}");
}

/// Every trait method passes a node error through. Lookups turn only -5 into
/// `None` (see `utxo_unspent_spent_unknown`, `mempool_source`); other codes
/// stay errors.
#[tokio::test]
async fn trait_methods_surface_errors() {
    let f = fake_with(|m, params| match m {
        "getblockchaininfo" => rpc_err(-28, "Loading block index…"),
        "getblockhash" => rpc_err(-8, "Block height out of range"),
        "getblockheader" => rpc_err(-5, "Block not found"),
        "getblock" => ok(json!("zz")),
        "getrawtransaction" => rpc_err(-5, "No such mempool or blockchain transaction"),
        "gettxout" => rpc_err(-8, "vout cannot be negative"),
        "getrawmempool" | "getmempoolentry" => rpc_err(-33, "Mempool disabled"),
        "sendrawtransaction" => rpc_err(-26, "min relay fee not met"),
        "submitpackage" => rpc_err(-25, "package topology disallowed"),
        "testmempoolaccept" => rpc_err(-22, "TX decode failed"),
        "estimatesmartfee" => rpc_err(-8, "Invalid conf_target"),
        "getblocktemplate" if params[0].get("longpollid").is_some() => {
            rpc_err(-10, "Bitcoin Core is in initial sync")
        }
        "getblocktemplate" => ok(gbt("lp", 1, &[])),
        "submitblock" => ok(json!("bad-txnmrklroot")),
        "waitfornewblock" => rpc_err(-1, "shutting down"),
        _ => None,
    })
    .await;
    let c = client(&f.url);
    let hash = genesis().block_hash();
    let tx = txs(1).remove(0);
    let txid = tx.compute_txid();

    // ChainSource
    assert_eq!(rpc_code(c.tip().await), RpcErrorCode::InWarmup);
    assert_eq!(
        rpc_code(c.block_hash(9).await),
        RpcErrorCode::InvalidParameter
    );
    assert_eq!(
        rpc_code(c.header(hash).await),
        RpcErrorCode::InvalidAddressOrKey
    );
    assert_eq!(
        rpc_code(c.header_info(hash).await),
        RpcErrorCode::InvalidAddressOrKey
    );
    assert!(matches!(c.block(hash).await, Err(NodeAPIError::Decode(_))));
    assert_eq!(
        rpc_code(c.transaction(txid, None).await),
        RpcErrorCode::InvalidAddressOrKey
    );
    let op = OutPoint { txid, vout: 0 };
    assert_eq!(
        rpc_code(c.utxo(op, true).await),
        RpcErrorCode::InvalidParameter
    );

    // MempoolSource
    assert_eq!(
        rpc_code(c.txids().await),
        RpcErrorCode::ClientMempoolDisabled
    );
    assert_eq!(
        rpc_code(c.snapshot().await),
        RpcErrorCode::ClientMempoolDisabled
    );
    assert_eq!(
        rpc_code(c.entry(txid).await),
        RpcErrorCode::ClientMempoolDisabled
    );

    // Broadcaster, FeeEstimator
    assert_eq!(
        rpc_code(c.broadcast(&tx).await),
        RpcErrorCode::VerifyRejected
    );
    let package = std::slice::from_ref(&tx);
    assert_eq!(
        rpc_code(c.broadcast_package(package).await),
        RpcErrorCode::VerifyError
    );
    assert_eq!(
        rpc_code(c.test_accept(package).await),
        RpcErrorCode::Deserialization
    );
    let target = FeeTarget {
        blocks: 0,
        mode: FeeMode::Economical,
    };
    assert_eq!(
        rpc_code(c.estimate_fee(target).await),
        RpcErrorCode::InvalidParameter
    );

    // TemplateSource: the longpoll's error reaches the waiter.
    let t = c.template().await.unwrap();
    let wait = WaitOptions::default();
    assert_eq!(
        rpc_code(c.wait_next(&t, wait).await),
        RpcErrorCode::ClientInInitialDownload
    );
    // A template from another backend is refused, not misread.
    let foreign = Template {
        token: TemplateToken::new(()),
        ..t.clone()
    };
    unsupported(c.transactions(&foreign).await);
    unsupported(c.wait_next(&foreign, wait).await);
    let sol = Solution {
        version: t.version,
        time: t.cur_time,
        nonce: 0,
        coinbase: t.coinbase_tx(&bitcoin::ScriptBuf::new(), &[]),
    };
    unsupported(c.submit_solution(&foreign, &sol).await);

    // EventSource: no stream without a tip; a failing poll is an error item.
    assert_eq!(rpc_code(c.subscribe().await), RpcErrorCode::InWarmup);
    let polls = fake_with(|m, _| match m {
        "getblockchaininfo" => ok(json!({ "blocks": 0, "bestblockhash": genesis().block_hash() })),
        "waitfornewblock" => rpc_err(-1, "shutting down"),
        _ => None,
    })
    .await;
    let mut events = client(&polls.url).subscribe().await.unwrap();
    assert_eq!(
        rpc_code(next(&mut events).await.unwrap()),
        RpcErrorCode::Misc
    );

    // Unreachable node: a retryable transport error from a trait method.
    match client("http://127.0.0.1:1").tip().await {
        Err(NodeAPIError::Transport(e)) => assert!(e.is_retryable(), "{e}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn debug_hides_the_password() {
    let auth = Auth::UserPass("alice".into(), "s3cret".into());
    let c = Client::with_version("http://127.0.0.1:1", auth, Version(310000));
    for shown in [format!("{c:?}"), format!("{:?}", c.wallet("w"))] {
        assert!(shown.contains("alice"), "{shown}");
        assert!(!shown.contains("s3cret"), "{shown}");
    }
}
