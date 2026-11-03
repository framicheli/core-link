//! Calls every `Wallet` method, and the wallet methods on `Client`, against a
//! fresh regtest node started with `-fallbackfee`. Same rules as the node
//! scenario: params must only depend on replies, so the replay can serve them.
use super::Checks;
use bitcoin::absolute::LockTime;
use bitcoin::address::NetworkUnchecked;
use bitcoin::{Address, Amount, FeeRate, OutPoint, TapSighashType, Weight, transaction};
use bitcoin_core_rpc::model::{AddressPurpose, TransactionCategory};
use bitcoin_core_rpc::{
    AddressType, BumpFeeOptions, Client, CreateWalletOptions, FundOptions, ImportDescriptor,
    ListUnspentQuery, LockAction, SendAllOptions, SendManyOptions, SendOptions,
    SendToAddressOptions, Timestamp, Wallet,
};
use bitcoin_node_api::ApiResult;
use bitcoin_node_api::FeeMode;
use std::time::Duration;

/// Wallet error codes from Core's `rpc/protocol.h`.
const MISC_ERROR: i64 = -1;
const WALLET_ERROR: i64 = -4;
const INSUFFICIENT_FUNDS: i64 = -6;
const UNLOCK_NEEDED: i64 = -13;
const WRONG_ENC_STATE: i64 = -15;
const PASSPHRASE_INCORRECT: i64 = -14;
const NOT_FOUND: i64 = -18;
const ALREADY_LOADED: i64 = -35;
const ALREADY_EXISTS: i64 = -36;

/// Runs the scenario, then `stop`s the node.
pub async fn run(c: &Client) -> Checks {
    let mut ck = Checks::default();
    if let Err(e) = scenario(c, &mut ck).await {
        ck.failures
            .push(format!("setup failed, scenario aborted: {e}"));
    }
    ck.ok("stop", c.stop().await);
    ck
}

async fn create(
    c: &Client,
    ck: &mut Checks,
    name: &str,
    opts: CreateWalletOptions,
) -> ApiResult<Wallet> {
    let loaded = c.create_wallet(name, opts).await?;
    ck.ran.insert("create_wallet");
    ck.check(
        "create_wallet",
        loaded.wallet.name() == name,
        "named as asked",
    );
    Ok(loaded.wallet)
}

async fn scenario(c: &Client, ck: &mut Checks) -> ApiResult<()> {
    let major = c.version().major();

    // == Lifecycle
    let hot = create(c, ck, "hot", CreateWalletOptions::default()).await?;
    let watch = create(
        c,
        ck,
        "watch",
        CreateWalletOptions::default().disable_private_keys(true),
    )
    .await?;
    let blank = create(c, ck, "blank", CreateWalletOptions::default().blank(true)).await?;
    // Percent-encoded as one path segment; Core makes the directory.
    let odd = create(c, ck, "odd/my wallet ü", CreateWalletOptions::default()).await?;
    if let Some(i) = ck.ok("get_wallet_info", odd.get_wallet_info().await) {
        ck.check(
            "get_wallet_info",
            i.wallet_name == odd.name(),
            "routed to the odd name",
        );
    }
    // "Database already exists" is a plain wallet error; -36 is for restores.
    ck.rpc_err(
        "create_wallet",
        c.create_wallet("hot", CreateWalletOptions::default()).await,
        WALLET_ERROR,
    );
    if let Some(names) = ck.ok("list_wallets", c.list_wallets().await) {
        ck.check("list_wallets", names.len() == 4, "4 wallets loaded");
    }
    if let Some(dir) = ck.ok("list_wallet_dir", c.list_wallet_dir().await) {
        ck.check(
            "list_wallet_dir",
            dir.wallets.len() == 4,
            "4 wallets on disk",
        );
    }
    if let Some(w) = ck.ok("unload_wallet", blank.unload_wallet(None).await) {
        ck.check("unload_wallet", w.is_empty(), "no warnings");
    }
    ck.rpc_err("get_wallet_info", blank.get_wallet_info().await, NOT_FOUND);
    if let Some(l) = ck.ok("load_wallet", c.load_wallet("blank", None).await) {
        ck.check("load_wallet", l.wallet.name() == "blank", "loaded by name");
    }
    ck.rpc_err(
        "load_wallet",
        c.load_wallet("blank", None).await,
        ALREADY_LOADED,
    );

    if let Some(i) = ck.ok("get_wallet_info", hot.get_wallet_info().await) {
        ck.check("get_wallet_info", i.wallet_name == "hot", "wallet name");
        ck.check(
            "get_wallet_info",
            i.private_keys_enabled && i.descriptors == Some(true),
            "descriptor wallet with keys",
        );
        ck.check(
            "get_wallet_info",
            i.balance.is_some() == (major < 30),
            "balance fields only before Core 30",
        );
    }
    if let Some(i) = ck.ok("get_wallet_info", watch.get_wallet_info().await) {
        ck.check("get_wallet_info", !i.private_keys_enabled, "watch-only");
    }
    ck.ok("keypool_refill", hot.keypool_refill(Some(10)).await);
    ck.ok("keypool_refill", hot.keypool_refill(None).await);

    // A directory on the node: where it saves the mempool. From a reply, so
    // the replay sends the same path.
    let saved = c.save_mempool().await?.filename;
    let dir = std::path::Path::new(&saved).parent().unwrap();
    let backup = dir.join("hot.bak").to_str().unwrap().to_owned();
    ck.ok("backup_wallet", hot.backup_wallet(&backup).await);
    if let Some(r) = ck.ok(
        "restore_wallet",
        c.restore_wallet("restored", &backup, None).await,
    ) {
        ck.check(
            "restore_wallet",
            r.wallet.name() == "restored",
            "restored by name",
        );
        if let Some(f) = ck.ok(
            "set_wallet_flag",
            r.wallet.set_wallet_flag("avoid_reuse", true).await,
        ) {
            ck.check("set_wallet_flag", f.flag_state, "avoid_reuse on");
        }
        ck.ok("unload_wallet", r.wallet.unload_wallet(Some(false)).await);
    }
    ck.rpc_err(
        "restore_wallet",
        c.restore_wallet("hot", &backup, None).await,
        ALREADY_EXISTS,
    );

    let mine = addresses_coins_history(c, ck, &hot).await?;
    spending(c, ck, &hot, &watch, &odd, &mine).await?;
    descriptors_and_more(c, ck, &hot, &blank, &odd, &mine).await?;

    // == Encryption
    let vault = create(c, ck, "vault", CreateWalletOptions::default()).await?;
    ck.ok("encrypt_wallet", vault.encrypt_wallet("pw1").await);
    ck.rpc_err(
        "encrypt_wallet",
        vault.encrypt_wallet("pw1").await,
        WRONG_ENC_STATE,
    );
    let minute = Duration::from_secs(60);
    ck.rpc_err(
        "wallet_passphrase",
        vault.wallet_passphrase("bad", minute).await,
        PASSPHRASE_INCORRECT,
    );
    ck.ok(
        "wallet_passphrase",
        vault.wallet_passphrase("pw1", minute).await,
    );
    if let Some(i) = ck.ok("get_wallet_info", vault.get_wallet_info().await) {
        ck.check(
            "get_wallet_info",
            i.unlocked_until.is_some_and(|t| t > 0),
            "unlocked",
        );
    }
    ck.ok(
        "wallet_passphrase_change",
        vault.wallet_passphrase_change("pw1", "pw2").await,
    );
    ck.ok("wallet_lock", vault.wallet_lock().await);
    let legacy = checked(
        vault
            .get_new_address(None, Some(AddressType::Legacy))
            .await?,
    );
    ck.rpc_err(
        "sign_message",
        vault.sign_message(&legacy, "locked").await,
        UNLOCK_NEEDED,
    );
    let sealed = create(
        c,
        ck,
        "sealed",
        CreateWalletOptions::default().passphrase("pw"),
    )
    .await?;
    ck.rpc_err(
        "encrypt_wallet",
        sealed.encrypt_wallet("pw").await,
        WRONG_ENC_STATE,
    );

    // == Migration: needs a legacy wallet, which Core 30+ can't create.
    // Refused for a descriptor wallet, which proves the call reached Core.
    ck.rpc_err(
        "migrate_wallet",
        c.migrate_wallet("hot", None).await,
        WALLET_ERROR,
    );

    for w in [&hot, &watch, &blank, &odd, &vault, &sealed] {
        ck.ok("unload_wallet", w.unload_wallet(None).await);
    }
    Ok(())
}

/// Regtest addresses come back unchecked; the node is ours.
fn checked(a: Address<NetworkUnchecked>) -> Address {
    a.assume_checked()
}

/// D2: addresses, labels, then 101 blocks mined to `hot` for balances,
/// coins, locks and history.
async fn addresses_coins_history(c: &Client, ck: &mut Checks, hot: &Wallet) -> ApiResult<Address> {
    // == Addresses and labels
    let mine = checked(hot.get_new_address(None, None).await?);
    ck.ran.insert("get_new_address");
    for t in [
        AddressType::Legacy,
        AddressType::P2shSegwit,
        AddressType::Bech32,
        AddressType::Bech32m,
    ] {
        if let Some(a) = ck.ok(
            "get_new_address",
            hot.get_new_address(Some("pay"), Some(t)).await,
        ) {
            let a = checked(a);
            let want = match t {
                AddressType::Legacy => a.script_pubkey().is_p2pkh(),
                AddressType::P2shSegwit => a.script_pubkey().is_p2sh(),
                AddressType::Bech32 => a.script_pubkey().is_p2wpkh(),
                _ => a.script_pubkey().is_p2tr(),
            };
            ck.check("get_new_address", want, &format!("{t:?} address"));
        }
    }
    ck.ok(
        "get_raw_change_address",
        hot.get_raw_change_address(None).await,
    );
    if let Some(a) = ck.ok(
        "get_raw_change_address",
        hot.get_raw_change_address(Some(AddressType::Bech32m)).await,
    ) {
        ck.check(
            "get_raw_change_address",
            checked(a).script_pubkey().is_p2tr(),
            "taproot change",
        );
    }
    if let Some(i) = ck.ok("get_address_info", hot.get_address_info(&mine).await) {
        ck.check(
            "get_address_info",
            i.is_mine && i.solvable == Some(true),
            "ours",
        );
    }
    ck.ok("set_label", hot.set_label(&mine, "renamed").await);
    if let Some(l) = ck.ok("list_labels", hot.list_labels(None).await) {
        ck.check(
            "list_labels",
            l.iter().any(|l| l == "renamed"),
            "lists the new label",
        );
    }
    ck.ok(
        "list_labels",
        hot.list_labels(Some(AddressPurpose::Receive)).await,
    );
    if let Some(a) = ck.ok(
        "get_addresses_by_label",
        hot.get_addresses_by_label("pay").await,
    ) {
        ck.check(
            "get_addresses_by_label",
            a.0.len() == 4,
            "4 labelled addresses",
        );
    }

    // == Funds: one mature coinbase.
    let mined = c.generate_to_address(101, &mine).await?.0;
    if let Some(b) = ck.ok("get_balance", hot.get_balance(None, None).await) {
        ck.check(
            "get_balance",
            b == Amount::from_int_btc(50),
            "one mature coinbase",
        );
    }
    ck.ok("get_balance", hot.get_balance(Some(1), Some(false)).await);
    if let Some(b) = ck.ok("get_balances", hot.get_balances().await) {
        ck.check(
            "get_balances",
            b.mine.trusted == Amount::from_int_btc(50)
                && b.mine.immature == Amount::from_int_btc(5000),
            "trusted and immature",
        );
    }
    let coins = hot
        .list_unspent(None, None, &[], None, ListUnspentQuery::default())
        .await?;
    ck.ran.insert("list_unspent");
    ck.check("list_unspent", coins.0.len() == 1, "1 spendable coin");
    let query = ListUnspentQuery::default()
        .minimum_amount(Amount::from_int_btc(1))
        .maximum_count(5)
        .include_immature_coinbase(true);
    if let Some(u) = ck.ok(
        "list_unspent",
        hot.list_unspent(
            Some(0),
            Some(200),
            std::slice::from_ref(&mine),
            Some(false),
            query,
        )
        .await,
    ) {
        ck.check(
            "list_unspent",
            u.0.len() == 5,
            "capped at 5, immature included",
        );
    }
    ck.ok("list_address_groupings", hot.list_address_groupings().await);

    // == Locks
    let coin = OutPoint {
        txid: coins.0[0].txid,
        vout: coins.0[0].vout,
    };
    if let Some(ok) = ck.ok(
        "lock_unspent",
        hot.lock_unspent(LockAction::Lock, &[coin], None).await,
    ) {
        ck.check("lock_unspent", ok, "locked");
    }
    if let Some(l) = ck.ok("list_lock_unspent", hot.list_lock_unspent().await) {
        ck.check("list_lock_unspent", l == [coin], "the locked coin");
    }
    ck.ok(
        "lock_unspent",
        hot.lock_unspent(LockAction::Unlock, &[coin], None).await,
    );
    ck.ok(
        "lock_unspent",
        hot.lock_unspent(LockAction::Lock, &[coin], Some(true))
            .await,
    );
    ck.ok(
        "lock_unspent",
        hot.lock_unspent(LockAction::Unlock, &[], None).await,
    );
    if let Some(l) = ck.ok("list_lock_unspent", hot.list_lock_unspent().await) {
        ck.check("list_lock_unspent", l.is_empty(), "all unlocked");
    }

    // == History: so far, coinbases.
    if let Some(t) = ck.ok("get_transaction", hot.get_transaction(coin.txid).await) {
        ck.check(
            "get_transaction",
            t.generated == Some(true) && t.decoded.is_none(),
            "coinbase",
        );
    }
    if let Some(t) = ck.ok(
        "get_transaction_verbose",
        hot.get_transaction_verbose(coin.txid).await,
    ) {
        ck.check(
            "get_transaction_verbose",
            t.decoded.as_ref() == Some(&t.tx),
            "decoded is the tx",
        );
    }
    if let Some(l) = ck.ok(
        "list_transactions",
        hot.list_transactions(None, None, None).await,
    ) {
        ck.check("list_transactions", l.0.len() == 10, "10 by default");
    }
    if let Some(l) = ck.ok(
        "list_transactions",
        hot.list_transactions(Some("*"), Some(5), Some(1)).await,
    ) {
        ck.check("list_transactions", l.0.len() == 5, "5 asked");
    }
    if let Some(l) = ck.ok(
        "list_since_block",
        hot.list_since_block(None, None, None, None, None).await,
    ) {
        ck.check(
            "list_since_block",
            l.transactions.len() == 101,
            "all 101 coinbases",
        );
    }
    let since = mined[89];
    if let Some(l) = ck.ok(
        "list_since_block",
        hot.list_since_block(
            Some(since),
            Some(6),
            Some(true),
            Some(true),
            Some("renamed"),
        )
        .await,
    ) {
        ck.check(
            "list_since_block",
            l.transactions.len() == 11 && l.last_block == mined[95],
            "11 newer, last_block 6 deep",
        );
        ck.check(
            "list_since_block",
            l.transactions
                .iter()
                .all(|t| t.category != TransactionCategory::Send),
            "no sends yet",
        );
    }
    if let Some(r) = ck.ok(
        "list_received_by_address",
        hot.list_received_by_address(None, None, None, None).await,
    ) {
        ck.check(
            "list_received_by_address",
            r.0.len() == 1 && r.0[0].amount == Amount::from_int_btc(50),
            "the mature coinbase",
        );
    }
    if let Some(r) = ck.ok(
        "list_received_by_address",
        hot.list_received_by_address(Some(0), Some(true), Some(&mine), Some(true))
            .await,
    ) {
        ck.check(
            "list_received_by_address",
            r.0.len() == 1 && r.0[0].amount == Amount::from_int_btc(5050),
            "every coinbase to `mine`",
        );
    }
    ck.ok(
        "list_received_by_label",
        hot.list_received_by_label(None, None, None).await,
    );
    if let Some(r) = ck.ok(
        "list_received_by_label",
        hot.list_received_by_label(Some(0), Some(true), Some(true))
            .await,
    ) {
        ck.check("list_received_by_label", r.0.len() == 2, "renamed and pay");
    }
    if let Some(a) = ck.ok(
        "get_received_by_address",
        hot.get_received_by_address(&mine, None, None).await,
    ) {
        ck.check(
            "get_received_by_address",
            a == Amount::from_int_btc(50),
            "the mature coinbase",
        );
    }
    if let Some(a) = ck.ok(
        "get_received_by_address",
        hot.get_received_by_address(&mine, Some(100), Some(true))
            .await,
    ) {
        ck.check(
            "get_received_by_address",
            a == Amount::from_int_btc(100),
            "2 coinbases 100 deep",
        );
    }
    ck.ok(
        "get_received_by_label",
        hot.get_received_by_label("pay", None, None).await,
    );
    if let Some(a) = ck.ok(
        "get_received_by_label",
        hot.get_received_by_label("renamed", Some(1), Some(true))
            .await,
    ) {
        ck.check(
            "get_received_by_label",
            a == Amount::from_int_btc(5050),
            "every coinbase",
        );
    }
    Ok(mine)
}

const BTC: Amount = Amount::ONE_BTC;

fn sat_vb(n: u32) -> FeeRate {
    FeeRate::from_sat_per_vb_u32(n)
}

/// D3, part 1: every way to send, the watch-only PSBT flow, fee bumping.
async fn spending(
    c: &Client,
    ck: &mut Checks,
    hot: &Wallet,
    watch: &Wallet,
    odd: &Wallet,
    mine: &Address,
) -> ApiResult<()> {
    let major = c.version().major();
    // 50 more mature coinbases.
    c.generate_to_address(50, mine).await?;
    let to = |w: &Wallet| {
        let w = w.clone();
        async move { w.get_new_address(None, None).await.map(checked) }
    };

    // == Sending
    let (r1, r2, r3) = (to(odd).await?, to(odd).await?, to(odd).await?);
    let opts = SendToAddressOptions::default()
        .comment("rent")
        .comment_to("landlord")
        .replaceable(true)
        .fee_rate(sat_vb(2));
    if let Some(txid) = ck.ok("send_to_address", hot.send_to_address(&r1, BTC, opts).await)
        && let Some(t) = ck.ok("get_transaction", hot.get_transaction(txid).await)
    {
        ck.check(
            "send_to_address",
            t.comment.as_deref() == Some("rent") && t.to.as_deref() == Some("landlord"),
            "comments stored",
        );
        ck.check(
            "send_to_address",
            t.amount.to_unsigned().is_err(),
            "an outgoing amount",
        );
    }
    let opts = SendToAddressOptions::default()
        .subtract_fee_from_amount(true)
        .conf_target(6)
        .estimate_mode(FeeMode::Economical)
        .avoid_reuse(false);
    ck.ok("send_to_address", hot.send_to_address(&r1, BTC, opts).await);
    ck.rpc_err(
        "send_to_address",
        hot.send_to_address(
            &r1,
            Amount::from_int_btc(1_000_000),
            SendToAddressOptions::default(),
        )
        .await,
        INSUFFICIENT_FUNDS,
    );
    ck.ok(
        "send_many",
        hot.send_many(&[(r2.clone(), BTC)], SendManyOptions::default())
            .await,
    );
    let opts = SendManyOptions::default()
        .comment("split")
        .subtract_fee_from(vec![r2.clone()])
        .replaceable(false)
        .fee_rate(sat_vb(3));
    if let Some(txid) = ck.ok(
        "send_many",
        hot.send_many(&[(r2.clone(), BTC), (r3.clone(), BTC * 2)], opts)
            .await,
    ) && let Some(t) = ck.ok("get_transaction", hot.get_transaction(txid).await)
    {
        ck.check("send_many", t.tx.output.len() == 3, "2 payments and change");
    }
    if let Some(s) = ck.ok(
        "send",
        hot.send(&[(r1.clone(), BTC)], None, SendOptions::default())
            .await,
    ) {
        ck.check("send", s.complete && s.txid.is_some(), "sent");
    }
    // Spends a chosen coin, with its weight, and keeps the tx.
    let coin = hot
        .list_unspent(Some(1), None, &[], None, ListUnspentQuery::default())
        .await?
        .0[0]
        .clone();
    let coin = OutPoint {
        txid: coin.txid,
        vout: coin.vout,
    };
    let opts = SendOptions::default()
        .fund(
            FundOptions::default()
                .fee_rate(sat_vb(3))
                .add_inputs(false)
                .input_weights(vec![(coin, Weight::from_wu(272))])
                .change_type(AddressType::Bech32m),
        )
        .inputs(vec![coin])
        .add_to_wallet(false)
        .locktime(LockTime::ZERO);
    if let Some(s) = ck.ok(
        "send",
        hot.send(&[(r1.clone(), BTC)], Some(b"hi"), opts).await,
    ) {
        let tx = s.hex.as_ref();
        ck.check(
            "send",
            s.complete
                && tx.is_some_and(|t| {
                    t.input.len() == 1
                        && t.input[0].previous_output == coin
                        && t.output.len() == 3
                        && t.output.iter().any(|o| o.script_pubkey.is_op_return())
                }),
            "the chosen coin; payment, OP_RETURN and change",
        );
    }
    if let Some(s) = ck.ok(
        "send",
        hot.send(
            &[(r1.clone(), BTC)],
            None,
            SendOptions::default().psbt(true),
        )
        .await,
    ) {
        ck.check("send", s.psbt.is_some() && s.complete, "a signed PSBT too");
    }
    let v3 = SendOptions::default()
        .add_to_wallet(false)
        .version(transaction::Version(3));
    if major >= 30 {
        if let Some(s) = ck.ok("send", hot.send(&[(r1.clone(), BTC)], None, v3).await) {
            ck.check(
                "send",
                s.hex.is_some_and(|t| t.version == transaction::Version(3)),
                "version 3",
            );
        }
    } else {
        ck.unsupported("send", hot.send(&[(r1.clone(), BTC)], None, v3).await);
    }

    // == sendall, from a wallet holding two coins.
    let spent = create(c, ck, "spent", CreateWalletOptions::default()).await?;
    let (s1, s2) = (to(&spent).await?, to(&spent).await?);
    hot.send_many(&[(s1, BTC), (s2, BTC)], SendManyOptions::default())
        .await?;
    c.generate_to_address(1, mine).await?;
    let mut opts = SendAllOptions::default()
        .fee_rate(sat_vb(2))
        .add_to_wallet(false)
        .send_max(true)
        .lock_unspents(false)
        .locktime(LockTime::ZERO)
        .replaceable(true);
    if major >= 30 {
        opts = opts.version(transaction::Version::TWO);
    }
    if let Some(s) = ck.ok(
        "send_all",
        spent
            .send_all(
                &[
                    (r2.clone(), Some(Amount::from_sat(50_000_000))),
                    (r3.clone(), None),
                ],
                opts,
            )
            .await,
    ) {
        ck.check(
            "send_all",
            s.complete
                && s.hex
                    .is_some_and(|t| t.input.len() == 2 && t.output.len() == 2),
            "both coins, both recipients, not sent",
        );
    }
    if major < 30 {
        let opts = SendAllOptions::default().version(transaction::Version::TWO);
        ck.unsupported(
            "send_all",
            spent.send_all(&[(r3.clone(), None)], opts).await,
        );
    }
    let opts = SendAllOptions::default()
        .min_conf(1)
        .max_conf(1000)
        .conf_target(3)
        .estimate_mode(FeeMode::Conservative);
    if let Some(s) = ck.ok(
        "send_all",
        spent.send_all(&[(r3.clone(), None)], opts).await,
    ) {
        ck.check("send_all", s.complete && s.txid.is_some(), "sent");
    }
    if let Some(b) = ck.ok("get_balance", spent.get_balance(Some(0), None).await) {
        ck.check("send_all", b == Amount::ZERO, "spent everything");
    }

    // == Watch-only PSBT flow: `watch` gets `hot`'s public descriptors,
    // builds the PSBT, `hot` signs it.
    let descs = hot.list_descriptors(false).await?;
    ck.ran.insert("list_descriptors");
    ck.check(
        "list_descriptors",
        descs.descriptors.len() == 8,
        "4 types x external/internal",
    );
    let requests: Vec<_> = descs
        .descriptors
        .iter()
        .map(|d| {
            let mut r = ImportDescriptor::new(&d.descriptor, Timestamp::Time(0))
                .active(d.active)
                .internal(d.internal.unwrap_or(false));
            if let Some([a, b]) = d.range {
                r = r.range(a as u32, b as u32);
            }
            if let Some(n) = d.next_index.or(d.next) {
                r = r.next_index(n as u32);
            }
            r
        })
        .collect();
    if let Some(r) = ck.ok(
        "import_descriptors",
        watch.import_descriptors(&requests).await,
    ) {
        ck.check(
            "import_descriptors",
            r.0.iter().all(|r| r.success),
            "all imported",
        );
    }
    let (hb, wb) = (hot.get_balances().await?, watch.get_balances().await?);
    ck.check(
        "import_descriptors",
        hb.mine.trusted == wb.mine.trusted,
        "watch sees hot's coins",
    );
    let opts = FundOptions::default()
        .fee_rate(sat_vb(2))
        .change_type(AddressType::Bech32);
    let funded = watch
        .wallet_create_funded_psbt(
            &[],
            &[(r1.clone(), BTC)],
            None,
            None,
            opts,
            Some(true),
            None,
        )
        .await?;
    ck.ran.insert("wallet_create_funded_psbt");
    if let Some(p) = ck.ok(
        "wallet_process_psbt",
        watch
            .wallet_process_psbt(&funded.psbt, None, None, None, None)
            .await,
    ) {
        ck.check("wallet_process_psbt", !p.complete, "watch-only can't sign");
    }
    if let Some(p) = ck.ok(
        "wallet_process_psbt",
        hot.wallet_process_psbt(&funded.psbt, None, None, None, None)
            .await,
    ) {
        ck.check(
            "wallet_process_psbt",
            p.complete && p.hex.is_some(),
            "hot signs it",
        );
        if let Some(tx) = p.hex {
            ck.ok(
                "send_raw_transaction",
                c.send_raw_transaction(&tx, None).await,
            );
        }
    }
    // A chosen input, an OP_RETURN, a locktime and a tx version.
    let height = c.get_block_count().await? as u32;
    let coin2 = hot
        .list_unspent(Some(1), None, &[], None, ListUnspentQuery::default())
        .await?
        .0
        .iter()
        .map(|u| OutPoint {
            txid: u.txid,
            vout: u.vout,
        })
        .find(|o| *o != coin)
        .unwrap();
    let opts = FundOptions::default()
        .add_inputs(true)
        .conf_target(2)
        .lock_unspents(false)
        .subtract_fee_from_outputs(vec![0])
        .replaceable(true)
        .max_tx_weight(Weight::from_wu(40_000));
    let lock = LockTime::from_height(height).unwrap();
    let version = (major >= 30).then_some(transaction::Version::TWO);
    if let Some(p) = ck.ok(
        "wallet_create_funded_psbt",
        hot.wallet_create_funded_psbt(
            &[coin2],
            &[(r1.clone(), BTC)],
            Some(b"psbt"),
            Some(lock),
            opts.clone(),
            Some(false),
            version,
        )
        .await,
    ) {
        let tx = &p.psbt.unsigned_tx;
        ck.check(
            "wallet_create_funded_psbt",
            tx.input[0].previous_output == coin2 && tx.lock_time == lock,
            "chosen input and locktime",
        );
        if let Some(q) = ck.ok(
            "wallet_process_psbt",
            hot.wallet_process_psbt(
                &p.psbt,
                Some(false),
                Some(TapSighashType::All),
                Some(false),
                Some(false),
            )
            .await,
        ) {
            ck.check(
                "wallet_process_psbt",
                !q.complete,
                "not signed when asked not to",
            );
        }
        // The node signs with `hot`'s private descriptors, no wallet involved.
        let private = hot.list_descriptors(true).await?;
        let private: Vec<_> = private
            .descriptors
            .iter()
            .map(|d| d.descriptor.as_str())
            .collect();
        if let Some(d) = ck.ok(
            "descriptor_process_psbt",
            c.descriptor_process_psbt(&p.psbt, &private, None, None, None)
                .await,
        ) {
            ck.check(
                "descriptor_process_psbt",
                d.complete && d.tx.is_some(),
                "signed and final",
            );
        }
        ck.ok(
            "descriptor_process_psbt",
            c.descriptor_process_psbt(
                &p.psbt,
                &private,
                Some(TapSighashType::All),
                Some(true),
                Some(false),
            )
            .await,
        );
    }
    if major < 30 {
        ck.unsupported(
            "wallet_create_funded_psbt",
            hot.wallet_create_funded_psbt(
                &[],
                &[(r1.clone(), BTC)],
                None,
                None,
                FundOptions::default(),
                None,
                Some(transaction::Version::TWO),
            )
            .await,
        );
    }

    // == fundrawtransaction + signrawtransactionwithwallet
    let raw = c
        .create_raw_transaction(&[], &[(r2.clone(), BTC)], None, None)
        .await?;
    ck.check("create_raw_transaction", raw.input.is_empty(), "no inputs");
    let opts = FundOptions::default()
        .fee_rate(sat_vb(4))
        .change_position(1)
        .change_address(to(hot).await?)
        .include_unsafe(false)
        .min_conf(1)
        .lock_unspents(true);
    if let Some(f) = ck.ok(
        "fund_raw_transaction",
        hot.fund_raw_transaction(&raw, opts).await,
    ) {
        ck.check(
            "fund_raw_transaction",
            f.change_position == 1,
            "change where asked",
        );
        let input = f.tx.input[0].previous_output;
        let prev = hot.get_transaction(input.txid).await?.tx.output[input.vout as usize].clone();
        ck.ok(
            "sign_raw_transaction_with_wallet",
            hot.sign_raw_transaction_with_wallet(
                &f.tx,
                &[(input, prev)],
                Some(TapSighashType::All),
            )
            .await,
        );
        if let Some(s) = ck.ok(
            "sign_raw_transaction_with_wallet",
            hot.sign_raw_transaction_with_wallet(&f.tx, &[], None).await,
        ) {
            ck.check("sign_raw_transaction_with_wallet", s.complete, "signed");
            if let Some(sim) = ck.ok(
                "simulate_raw_transaction",
                hot.simulate_raw_transaction(std::slice::from_ref(&s.tx))
                    .await,
            ) {
                ck.check(
                    "simulate_raw_transaction",
                    sim.balance_change == -(f.fee.to_signed().unwrap() + BTC.to_signed().unwrap()),
                    "pays 1 BTC plus the fee",
                );
            }
            ck.ok(
                "send_raw_transaction",
                c.send_raw_transaction(&s.tx, None).await,
            );
        }
    }
    let solving = bitcoin_core_rpc::SolvingData::default().descriptors(vec![]);
    ck.ok(
        "fund_raw_transaction",
        hot.fund_raw_transaction(&raw, FundOptions::default().solving_data(solving))
            .await,
    );

    // == Fee bumping and abandoning
    let rbf = SendToAddressOptions::default().replaceable(true);
    let original = hot.send_to_address(&r3, BTC, rbf.clone()).await?;
    let opts = BumpFeeOptions::default()
        .fee_rate(sat_vb(25))
        .replaceable(true);
    if let Some(b) = ck.ok("bump_fee", hot.bump_fee(original, opts).await) {
        ck.check(
            "bump_fee",
            b.fee > b.original_fee && b.txid != original,
            "a dearer replacement",
        );
    }
    // Replaced, so out of the mempool: it can be abandoned.
    ck.ok(
        "abandon_transaction",
        hot.abandon_transaction(original).await,
    );
    if let Some(t) = ck.ok("get_transaction", hot.get_transaction(original).await) {
        ck.check(
            "abandon_transaction",
            t.details.iter().all(|d| d.abandoned == Some(true)),
            "abandoned",
        );
    }
    let other = hot.send_to_address(&r3, BTC, rbf).await?;
    let opts = BumpFeeOptions::default()
        .conf_target(2)
        .estimate_mode(FeeMode::Economical);
    if let Some(b) = ck.ok("psbt_bump_fee", hot.psbt_bump_fee(other, opts).await) {
        ck.check("psbt_bump_fee", b.fee > b.original_fee, "a dearer PSBT");
    }
    let opts = BumpFeeOptions::default().outputs(vec![(r2.clone(), Amount::from_sat(10_000_000))]);
    ck.ok("psbt_bump_fee", hot.psbt_bump_fee(other, opts).await);
    c.generate_to_address(1, mine).await?;
    Ok(())
}

/// D3, part 2: descriptors and HD keys, rescans, pruned funds, messages,
/// the external signer.
async fn descriptors_and_more(
    c: &Client,
    ck: &mut Checks,
    hot: &Wallet,
    blank: &Wallet,
    odd: &Wallet,
    mine: &Address,
) -> ApiResult<()> {
    // == HD keys and descriptors
    if let Some(k) = ck.ok("get_hd_keys", hot.get_hd_keys(false, false).await) {
        ck.check(
            "get_hd_keys",
            k.0.len() == 1 && k.0[0].xpriv.is_none(),
            "one key, public",
        );
    }
    let keys = hot.get_hd_keys(true, true).await?;
    ck.ran.insert("get_hd_keys");
    let key = &keys.0[0];
    ck.check(
        "get_hd_keys",
        key.xpriv.is_some() && key.has_private,
        "private when asked",
    );
    // `blank` learns the key from a descriptor, then derives taproot ones from it.
    let desc = format!("wpkh({}/84h/1h/0h/0/*)", key.xpriv.unwrap());
    let desc = format!("{desc}#{}", c.get_descriptor_info(&desc).await?.checksum);
    let import = [ImportDescriptor::new(&desc, Timestamp::Now)
        .active(true)
        .range(0, 10)];
    if let Some(r) = ck.ok(
        "import_descriptors",
        blank.import_descriptors(&import).await,
    ) {
        ck.check("import_descriptors", r.0[0].success, "imported");
    }
    if let Some(d) = ck.ok(
        "create_wallet_descriptor",
        blank
            .create_wallet_descriptor(AddressType::Bech32m, None, Some(&key.xpub))
            .await,
    ) {
        ck.check(
            "create_wallet_descriptor",
            d.descriptors.len() == 2,
            "external and internal",
        );
    }
    if let Some(d) = ck.ok(
        "create_wallet_descriptor",
        blank
            .create_wallet_descriptor(AddressType::P2shSegwit, Some(true), None)
            .await,
    ) {
        ck.check(
            "create_wallet_descriptor",
            d.descriptors.len() == 1,
            "internal only",
        );
    }
    if let Some(r) = ck.ok("rescan_blockchain", hot.rescan_blockchain(None, None).await) {
        ck.check("rescan_blockchain", r.start_height == 0, "from genesis");
    }
    if let Some(r) = ck.ok(
        "rescan_blockchain",
        hot.rescan_blockchain(Some(100), Some(110)).await,
    ) {
        ck.check("rescan_blockchain", r.stop_height == 110, "to 110");
    }
    if let Some(a) = ck.ok("abort_rescan", hot.abort_rescan().await) {
        ck.check("abort_rescan", !a, "nothing to abort");
    }

    // == Pruned funds: a confirmed payment to `odd`, with its proof.
    let pay = checked(odd.get_new_address(None, None).await?);
    let txid = hot
        .send_to_address(&pay, BTC, SendToAddressOptions::default())
        .await?;
    c.generate_to_address(1, mine).await?;
    let tx = hot.get_transaction(txid).await?.tx;
    let proof = c.get_tx_out_proof(&[txid], None).await?;
    ck.ok(
        "import_pruned_funds",
        odd.import_pruned_funds(&tx, &proof).await,
    );
    ck.ok("remove_pruned_funds", odd.remove_pruned_funds(txid).await);

    // == Messages
    let legacy = checked(hot.get_new_address(None, Some(AddressType::Legacy)).await?);
    if let Some(sig) = ck.ok("sign_message", hot.sign_message(&legacy, "hello").await)
        && let Some(v) = ck.ok(
            "verify_message",
            c.verify_message(&legacy, &sig, "hello").await,
        )
    {
        ck.check("sign_message", v, "verifies");
    }

    // == External signer: none configured.
    ck.rpc_err(
        "wallet_display_address",
        hot.wallet_display_address(mine).await,
        MISC_ERROR,
    );
    Ok(())
}
