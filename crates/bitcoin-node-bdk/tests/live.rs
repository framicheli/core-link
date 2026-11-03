//! A `bdk_wallet` kept in sync with a real `bitcoind` over RPC through a
//! payment, a reorg and a fee bump. `#[ignore]`: `cargo test -- --ignored`,
//! with `bitcoind` on `PATH` or `BITCOIND_EXE`.
use bdk_wallet::{Balance, KeychainKind, Update, Wallet};
use bitcoin::{Amount, BlockHash, Network, Txid};
use bitcoin_core_rpc::{Auth, Client, CreateWalletOptions};
use bitcoin_node_api::ChainSource;
use bitcoin_node_bdk::bip158::FilterIter;
use bitcoin_node_bdk::{Emitter, NO_EXPECTED_MEMPOOL_TXS};
use serde_json::{Value, json};
use std::str::FromStr;
use std::time::Duration;
use test_node::{RPC_PASSWORD, RPC_USER, TestNode};

const EXTERNAL: &str = "tr([83737d5e/86'/1'/0']tpubDDR5GgtoxS8fJyjjvdahN4VzV5DV6jtbcyvVXhEKq2XtpxjxBXmxH3r8QrNbQqHg4bJM1EGkxi7Pjfkgnui9jQWqS7kxHvX6rhUeriLDKxz/0/*)";
const INTERNAL: &str = "tr([83737d5e/86'/1'/0']tpubDDR5GgtoxS8fJyjjvdahN4VzV5DV6jtbcyvVXhEKq2XtpxjxBXmxH3r8QrNbQqHg4bJM1EGkxi7Pjfkgnui9jQWqS7kxHvX6rhUeriLDKxz/1/*)";

async fn sync(wallet: &mut Wallet, emitter: &mut Emitter<Client>) -> (usize, Vec<Txid>) {
    let mut blocks = 0;
    while let Some(ev) = emitter.next_block().await.unwrap() {
        wallet
            .apply_block_connected_to(&ev.block, ev.block_height(), ev.connected_to())
            .unwrap();
        blocks += 1;
    }
    let mempool = emitter.mempool().await.unwrap();
    let evicted = mempool.evicted.iter().map(|(t, _)| *t).collect();
    wallet.apply_unconfirmed_txs(mempool.update);
    wallet.apply_evicted_txs(mempool.evicted);
    (blocks, evicted)
}

/// The wallet's chain is Core's.
async fn assert_at_tip(wallet: &Wallet, core: &Client) {
    let tip = core.tip().await.unwrap();
    let cp = wallet.latest_checkpoint();
    assert_eq!((u64::from(cp.height()), cp.hash()), (tip.height, tip.hash));
}

async fn connect(node: &TestNode) -> Client {
    let auth = Auth::UserPass(RPC_USER.into(), RPC_PASSWORD.into());
    for _ in 0..100 {
        if let Ok(c) = Client::connect(&node.rpc_url(), auth.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("node never answered:\n{}", node.log_tail(30))
}

#[tokio::test]
#[ignore = "needs bitcoind"]
async fn wallet_follows_core() {
    let node = TestNode::start(&["-fallbackfee=0.0001"]);
    let core = connect(&node).await;
    eprintln!("Core {}", core.version().0);
    let payer = core
        .create_wallet("payer", CreateWalletOptions::default())
        .await
        .unwrap()
        .wallet;
    let payer_addr: Value = payer.raw_call("getnewaddress", json!([])).await.unwrap();
    let payer_addr = payer_addr.as_str().unwrap().to_owned();
    let mine = |n: u32| {
        let (core, to) = (core.clone(), payer_addr.clone());
        async move {
            let _: Value = core
                .raw_call("generatetoaddress", json!([n, to]))
                .await
                .unwrap();
        }
    };
    mine(101).await;

    let mut wallet = Wallet::create(EXTERNAL, INTERNAL)
        .network(Network::Regtest)
        .create_wallet_no_persist()
        .unwrap();
    let addr = wallet.reveal_next_address(KeychainKind::External).address;
    let mut emitter = Emitter::new(
        core.clone(),
        wallet.latest_checkpoint(),
        0,
        NO_EXPECTED_MEMPOOL_TXS,
    );
    let (blocks, _) = sync(&mut wallet, &mut emitter).await;
    assert_eq!(blocks, 101);
    assert_at_tip(&wallet, &core).await;
    assert_eq!(wallet.balance(), Balance::default());

    // Paid: pending, then confirmed.
    let paid: Value = payer
        .raw_call("sendtoaddress", json!([addr.to_string(), 1.0]))
        .await
        .unwrap();
    let paid = Txid::from_str(paid.as_str().unwrap()).unwrap();
    sync(&mut wallet, &mut emitter).await;
    assert_eq!(wallet.balance().untrusted_pending, Amount::ONE_BTC);
    mine(1).await;
    sync(&mut wallet, &mut emitter).await;
    assert_eq!(wallet.balance().confirmed, Amount::ONE_BTC);
    assert_at_tip(&wallet, &core).await;

    // Its block replaced by two empty ones: the payment is back in the
    // mempool, and pending again.
    let tip = core.tip().await.unwrap();
    let _: Value = core
        .raw_call("invalidateblock", json!([tip.hash]))
        .await
        .unwrap();
    for _ in 0..2 {
        let _: Value = core
            .raw_call("generateblock", json!([payer_addr, []]))
            .await
            .unwrap();
    }
    let (blocks, _) = sync(&mut wallet, &mut emitter).await;
    assert_eq!(blocks, 2, "the fork's two blocks");
    assert_at_tip(&wallet, &core).await;
    let b = wallet.balance();
    assert_eq!(
        (b.confirmed, b.untrusted_pending),
        (Amount::ZERO, Amount::ONE_BTC)
    );

    // A fee bump replaces it: the original is evicted, the replacement
    // (paying the same) is pending.
    let bumped: Value = payer
        .raw_call("bumpfee", json!([paid.to_string()]))
        .await
        .unwrap();
    let bumped = Txid::from_str(bumped["txid"].as_str().unwrap()).unwrap();
    let (_, evicted) = sync(&mut wallet, &mut emitter).await;
    assert_eq!(evicted, [paid]);
    assert_eq!(wallet.balance().untrusted_pending, Amount::ONE_BTC);
    assert!(wallet.get_tx(bumped).is_some());

    mine(1).await;
    sync(&mut wallet, &mut emitter).await;
    assert_eq!(wallet.balance().confirmed, Amount::ONE_BTC);
    assert_at_tip(&wallet, &core).await;
}

/// Core's filters are what the mock builds (the block plus the scripts it
/// spends); filter sync fetches only the paying block and follows a reorg;
/// without the index, `Unsupported`.
#[tokio::test]
#[ignore = "needs bitcoind"]
async fn filter_sync_on_core() {
    let node = TestNode::start(&["-fallbackfee=0.0001"]);
    let core = connect(&node).await;
    let payer = core
        .create_wallet("payer", CreateWalletOptions::default())
        .await
        .unwrap()
        .wallet;
    let payer_addr: Value = payer.raw_call("getnewaddress", json!([])).await.unwrap();
    let payer_addr = payer_addr.as_str().unwrap().to_owned();
    let mine = |n: u32| {
        let (core, to) = (core.clone(), payer_addr.clone());
        async move {
            let v: Vec<String> = core
                .raw_call("generatetoaddress", json!([n, to]))
                .await
                .unwrap();
            v
        }
    };
    mine(110).await;

    let mut wallet = Wallet::create(EXTERNAL, INTERNAL)
        .network(Network::Regtest)
        .create_wallet_no_persist()
        .unwrap();
    let spk = wallet
        .reveal_next_address(KeychainKind::External)
        .script_pubkey();
    // Payments spending Core's coins, each in its own block.
    let mut paying = Vec::new();
    for _ in 0..3 {
        let addr = bitcoin::Address::from_script(&spk, Network::Regtest).unwrap();
        let _: Value = payer
            .raw_call("sendtoaddress", json!([addr.to_string(), 0.5]))
            .await
            .unwrap();
        paying.push(mine(1).await[0].clone());
        mine(2).await;
    }

    // Every block's filter, rebuilt from the block and its spent outputs
    // (-txindex), is Core's byte for byte.
    let tip = core.tip().await.unwrap();
    for h in [1, 100, 111, 112, tip.height] {
        let hash = core.block_hash(h).await.unwrap();
        let block = core.block(hash).await.unwrap();
        let mut spent = std::collections::HashMap::new();
        for tx in block.txdata.iter().skip(1) {
            for i in &tx.input {
                let prev = core
                    .transaction(i.previous_output.txid, None)
                    .await
                    .unwrap();
                spent.insert(
                    i.previous_output,
                    prev.output[i.previous_output.vout as usize]
                        .script_pubkey
                        .clone(),
                );
            }
        }
        let rebuilt = bitcoin::bip158::BlockFilter::new_script_filter(&block, |op| {
            Ok::<_, bitcoin::bip158::Error>(spent[op].clone())
        })
        .unwrap();
        assert_eq!(
            rebuilt,
            core.block_filter(hash).await.unwrap(),
            "height {h}"
        );
    }

    // Filter sync: three blocks fetched, the balance right.
    let mut iter = FilterIter::new(core.clone(), wallet.latest_checkpoint(), [spk.clone()]);
    let mut fetched = Vec::new();
    while let Some(ev) = iter.next().await.unwrap() {
        match &ev.block {
            Some(b) => {
                fetched.push(b.block_hash().to_string());
                wallet
                    .apply_block_connected_to(b, ev.height(), ev.connected_to())
                    .unwrap();
            }
            None => wallet
                .apply_update(Update {
                    chain: Some(ev.cp.clone()),
                    ..Default::default()
                })
                .unwrap(),
        }
    }
    assert_eq!(fetched, paying, "exactly the paying blocks");
    assert_eq!(wallet.balance().confirmed, Amount::from_sat(150_000_000));
    assert_at_tip(&wallet, &core).await;

    // The last payment's block (two below the tip) reorged out for empty
    // blocks: the scan rewinds and the wallet drops it.
    let last = BlockHash::from_str(&paying[2]).unwrap();
    let _: Value = core
        .raw_call("invalidateblock", json!([last]))
        .await
        .unwrap();
    for _ in 0..4 {
        let _: Value = core
            .raw_call("generateblock", json!([payer_addr, []]))
            .await
            .unwrap();
    }
    while let Some(ev) = iter.next().await.unwrap() {
        assert!(!ev.is_match(), "the payment is in the mempool, not a block");
        wallet
            .apply_update(Update {
                chain: Some(ev.cp.clone()),
                ..Default::default()
            })
            .unwrap();
    }
    assert_eq!(wallet.balance().confirmed, Amount::from_sat(100_000_000));
    assert_at_tip(&wallet, &core).await;

    // Without the index.
    let bare = TestNode::start(&["-blockfilterindex=0"]);
    let bare = connect(&bare).await;
    let genesis = bare.block_hash(0).await.unwrap();
    match bare.block_filter(genesis).await {
        Err(bitcoin_node_api::NodeAPIError::Unsupported(m)) => {
            assert!(m.contains("-blockfilterindex"), "{m}")
        }
        other => panic!("{other:?}"),
    }
}
