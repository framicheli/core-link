//! Calls every typed `Client` method against a fresh regtest chain.
//! `live.rs` runs it on a real node and records the session; `replay.rs`
//! runs it again on that recording, so params must only depend on replies.
#![allow(dead_code)]
pub mod wallet;
use bitcoin::absolute::LockTime;
use bitcoin::bip32::{Xpriv, Xpub};
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::{
    Address, Amount, Block, CompressedPublicKey, Network, OutPoint, PrivateKey, PublicKey,
    Sequence, SignedAmount, Transaction, TxIn, TxMerkleNode, TxOut, Witness, block, script,
    transaction,
};
use bitcoin_core_rpc::model::GetBlockTemplate;
use bitcoin_core_rpc::{AddNodeCommand, Client, SetBanCommand};
use bitcoin_node_api::{ApiResult, FeeMode, NodeAPIError};
use serde_json::json;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::time::Duration;

#[derive(Default)]
pub struct Checks {
    pub failures: Vec<String>,
    /// Methods that ran, by name.
    pub ran: BTreeSet<&'static str>,
}

impl Checks {
    /// Expects success.
    fn ok<T>(&mut self, name: &'static str, r: ApiResult<T>) -> Option<T> {
        self.ran.insert(name);
        r.map_err(|e| self.failures.push(format!("{name}: {e}")))
            .ok()
    }

    /// Expects Core to refuse with `code`: proves the params parsed and the
    /// call got as far as the node's own checks.
    fn rpc_err<T: Debug>(&mut self, name: &'static str, r: ApiResult<T>, code: i64) {
        self.ran.insert(name);
        match r {
            Err(NodeAPIError::Rpc { code: c, .. }) if c.code() == code => {}
            other => self
                .failures
                .push(format!("{name}: expected RPC error {code}, got {other:?}")),
        }
    }

    /// Expects the client to refuse without asking the node.
    fn unsupported<T: Debug>(&mut self, name: &'static str, r: ApiResult<T>) {
        self.ran.insert(name);
        if !matches!(r, Err(NodeAPIError::Unsupported(_))) {
            self.failures
                .push(format!("{name}: expected Unsupported, got {r:?}"));
        }
    }

    fn check(&mut self, name: &'static str, cond: bool, what: &str) {
        if !cond {
            self.failures.push(format!("{name}: {what}"));
        }
    }
}

struct Keys {
    key: PrivateKey,
    pubkey: PublicKey,
    /// P2WPKH: coinbases pay here.
    addr: Address,
    /// P2PKH, for message signing.
    legacy: Address,
    xpub: Xpub,
}

fn keys() -> Keys {
    let secp = Secp256k1::new();
    let sk = SecretKey::from_slice(&[0x11; 32]).unwrap();
    let key = PrivateKey::new(sk, Network::Regtest);
    let pubkey = key.public_key(&secp);
    let cpk = CompressedPublicKey::from_private_key(&secp, &key).unwrap();
    let master = Xpriv::new_master(Network::Regtest, &[7; 32]).unwrap();
    Keys {
        key,
        pubkey,
        addr: Address::p2wpkh(&cpk, Network::Regtest),
        legacy: Address::p2pkh(pubkey, Network::Regtest),
        xpub: Xpub::from_priv(&secp, &master),
    }
}

const FEE: Amount = Amount::from_sat(10_000);
/// JSON-RPC "method not found": an RPC newer than the node.
const NOT_FOUND: i64 = -32601;

/// Spends output `vout` of `prev` (worth `value`) back to our address.
/// Passes the prevout to the signer when `unknown`: the node can't see it yet.
async fn spend(
    c: &Client,
    k: &Keys,
    prev: OutPoint,
    value: Amount,
    unknown: bool,
) -> ApiResult<(Transaction, Amount)> {
    let out = value - FEE;
    let tx = c
        .create_raw_transaction(&[prev], &[(k.addr.clone(), out)], None, None)
        .await?;
    let prevout = TxOut {
        value,
        script_pubkey: k.addr.script_pubkey(),
    };
    let prevouts: &[_] = if unknown { &[(prev, prevout)] } else { &[] };
    let signed = c
        .sign_raw_transaction_with_key(&tx, &[k.key], prevouts)
        .await?;
    if !signed.complete {
        return Err(NodeAPIError::Decode(format!(
            "unsigned: {:?}",
            signed.errors
        )));
    }
    Ok((signed.tx, out))
}

async fn coinbase(c: &Client, height: u64) -> ApiResult<(OutPoint, Amount)> {
    let block = c.get_block(c.get_block_hash(height).await?).await?;
    let cb = &block.txdata[0];
    Ok((
        OutPoint {
            txid: cb.compute_txid(),
            vout: 0,
        },
        cb.output[0].value,
    ))
}

/// The template's next block with only a coinbase. Nonce 0: proposals skip
/// the proof-of-work check.
fn empty_block(t: &GetBlockTemplate, k: &Keys) -> Block {
    let coinbase = Transaction {
        version: transaction::Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            // BIP 34 height, then one byte: the script must be 2-100 bytes.
            script_sig: script::Builder::new()
                .push_int(i64::from(t.height))
                .push_int(0)
                .into_script(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: t.coinbase_value.to_unsigned().unwrap(),
            script_pubkey: k.addr.script_pubkey(),
        }],
    };
    let mut block = Block {
        header: block::Header {
            version: t.version,
            prev_blockhash: t.previous_block_hash,
            merkle_root: TxMerkleNode::all_zeros(),
            time: t.current_time as u32,
            bits: t.bits,
            nonce: 0,
        },
        txdata: vec![coinbase],
    };
    block.header.merkle_root = block.compute_merkle_root().unwrap();
    block
}

/// `stop` too when `stop_node`.
pub async fn run(c: &Client, stop_node: bool) -> Checks {
    let mut ck = Checks::default();
    if let Err(e) = scenario(c, &mut ck).await {
        ck.failures
            .push(format!("setup failed, scenario aborted: {e}"));
    }
    if stop_node {
        ck.ok("stop", c.stop().await);
    }
    ck
}

async fn scenario(c: &Client, ck: &mut Checks) -> ApiResult<()> {
    let major = c.version().major();
    let k = keys();
    let desc = format!("addr({})", k.addr);
    let desc = c.get_descriptor_info(&desc).await?.descriptor;
    ck.ran.insert("get_descriptor_info");

    // == Regtest: 110 blocks, so coinbases 1..=10 are spendable.
    let mined = c.generate_to_address(110, &k.addr).await?;
    ck.ran.insert("generate_to_address");
    ck.check("generate_to_address", mined.0.len() == 110, "110 hashes");
    ck.ok(
        "generate_to_descriptor",
        c.generate_to_descriptor(1, &desc).await,
    );

    // == Mempool: parent -> child, plus a package.
    let (cb1, v1) = coinbase(c, 1).await?;
    let (parent, pv) = spend(c, &k, cb1, v1, false).await?;
    ck.ran.insert("create_raw_transaction");
    ck.ran.insert("sign_raw_transaction_with_key");
    let child_in = OutPoint {
        txid: parent.compute_txid(),
        vout: 0,
    };
    if let Some(r) = ck.ok(
        "test_mempool_accept",
        c.test_mempool_accept(std::slice::from_ref(&parent)).await,
    ) {
        ck.check(
            "test_mempool_accept",
            r.results[0].allowed,
            "parent allowed",
        );
    }
    let ptxid = c.send_raw_transaction(&parent, None).await?;
    ck.ran.insert("send_raw_transaction");
    let (child, _) = spend(c, &k, child_in, pv, false).await?;
    let ctxid = child.compute_txid();
    ck.ok(
        "send_raw_transaction",
        c.send_raw_transaction(&child, Some(bitcoin::FeeRate::from_sat_per_vb_u32(1000)))
            .await,
    );

    let (cb2, v2) = coinbase(c, 2).await?;
    let (p2, p2v) = spend(c, &k, cb2, v2, false).await?;
    let p2_out = OutPoint {
        txid: p2.compute_txid(),
        vout: 0,
    };
    let (c2, _) = spend(c, &k, p2_out, p2v, true).await?;
    ck.ok("submit_package", c.submit_package(&[p2, c2]).await);

    ck.ok("get_mempool_info", c.get_mempool_info().await);
    if let Some(m) = ck.ok("get_raw_mempool", c.get_raw_mempool().await) {
        ck.check("get_raw_mempool", m.0.len() == 4, "4 txs in mempool");
    }
    ck.ok("get_raw_mempool_verbose", c.get_raw_mempool_verbose().await);
    if let Some(m) = ck.ok(
        "get_raw_mempool_sequence",
        c.get_raw_mempool_sequence().await,
    ) {
        ck.check("get_raw_mempool_sequence", m.txids.len() == 4, "4 txids");
    }
    ck.ok("get_mempool_entry", c.get_mempool_entry(ctxid).await);
    if let Some(a) = ck.ok(
        "get_mempool_ancestors",
        c.get_mempool_ancestors(ctxid).await,
    ) {
        ck.check(
            "get_mempool_ancestors",
            a.0 == [ptxid],
            "parent is the ancestor",
        );
    }
    ck.ok(
        "get_mempool_ancestors_verbose",
        c.get_mempool_ancestors_verbose(ctxid).await,
    );
    ck.ok(
        "get_mempool_descendants",
        c.get_mempool_descendants(ptxid).await,
    );
    ck.ok(
        "get_mempool_descendants_verbose",
        c.get_mempool_descendants_verbose(ptxid).await,
    );
    if major >= 31 {
        ck.ok("get_mempool_cluster", c.get_mempool_cluster(ctxid).await);
    } else {
        ck.unsupported("get_mempool_cluster", c.get_mempool_cluster(ctxid).await);
    }
    ck.ok(
        "get_tx_spending_prevout",
        c.get_tx_spending_prevout(&[cb1, child_in]).await,
    );
    ck.ok(
        "prioritise_transaction",
        c.prioritise_transaction(ctxid, SignedAmount::from_sat(1000))
            .await,
    );
    ck.ok(
        "get_prioritised_transactions",
        c.get_prioritised_transactions().await,
    );
    ck.ok("get_tx_out", c.get_tx_out(child_in, true).await);
    if let Some(spent) = ck.ok("get_tx_out", c.get_tx_out(cb1, true).await) {
        ck.check("get_tx_out", spent.is_none(), "spent coinbase is None");
    }
    if major >= 29 {
        ck.ok("get_orphan_txs", c.get_orphan_txs().await);
    } else {
        ck.rpc_err("get_orphan_txs", c.get_orphan_txs().await, NOT_FOUND);
    }
    // Needs its inputs unspent in the chain or mempool.
    if let Some(tx) = ck.ok(
        "combine_raw_transaction",
        c.combine_raw_transaction(&[child.clone(), child.clone()])
            .await,
    ) {
        ck.check("combine_raw_transaction", tx == child, "same tx");
    }
    if let Some(saved) = ck.ok("save_mempool", c.save_mempool().await) {
        ck.ok("import_mempool", c.import_mempool(&saved.filename).await);
    }
    if major >= 31 {
        ck.ok(
            "get_private_broadcast_info",
            c.get_private_broadcast_info().await,
        );
        ck.rpc_err(
            "abort_private_broadcast",
            c.abort_private_broadcast(ctxid).await,
            -5,
        );
    } else {
        ck.unsupported(
            "get_private_broadcast_info",
            c.get_private_broadcast_info().await,
        );
        ck.unsupported(
            "abort_private_broadcast",
            c.abort_private_broadcast(ctxid).await,
        );
    }

    // == Mining
    if let Some(t) = ck.ok("get_block_template", c.get_block_template().await) {
        ck.check(
            "get_block_template",
            t.transactions.len() == 4,
            "4 template txs",
        );
    }
    ck.ok("get_mining_info", c.get_mining_info().await);
    ck.ok(
        "get_network_hash_ps",
        c.get_network_hash_ps(None, None).await,
    );
    ck.ok(
        "get_network_hash_ps",
        c.get_network_hash_ps(Some(10), Some(50)).await,
    );
    let (cb3, v3) = coinbase(c, 3).await?;
    let (t3, _) = spend(c, &k, cb3, v3, false).await?;
    let t3id = t3.compute_txid();
    ck.ok(
        "generate_block",
        c.generate_block(&k.addr.to_string(), &[t3]).await,
    );
    // Confirms the mempool.
    let tip = c.generate_to_address(1, &k.addr).await?.0[0];
    let height = c.get_block_count().await?;
    ck.ran.insert("get_block_count");
    ck.check("get_block_count", height == 113, "height 113");
    let block = c.get_block(tip).await?;
    ck.ran.insert("get_block");
    ck.ran.insert("get_block_hash");
    ck.check(
        "get_block",
        block.txdata.len() == 5,
        "tip has the 4 mempool txs",
    );
    if let Some(r) = ck.ok("submit_block", c.submit_block(&block).await) {
        ck.check(
            "submit_block",
            r.as_deref() == Some("duplicate"),
            "duplicate",
        );
    }
    ck.ok("submit_header", c.submit_header(&block.header).await);

    // == Blockchain
    if let Some(h) = ck.ok("get_best_block_hash", c.get_best_block_hash().await) {
        ck.check("get_best_block_hash", h == tip, "tip");
    }
    ck.ok("get_block_verbose_one", c.get_block_verbose_one(tip).await);
    ck.ok("get_block_verbose_two", c.get_block_verbose_two(tip).await);
    ck.ok(
        "get_block_verbose_three",
        c.get_block_verbose_three(tip).await,
    );
    ck.ok("get_blockchain_info", c.get_blockchain_info().await);
    ck.ok("get_block_filter", c.get_block_filter(tip, None).await);
    ck.ok("get_block_header", c.get_block_header(tip).await);
    ck.ok(
        "get_block_header_verbose",
        c.get_block_header_verbose(tip).await,
    );
    ck.ok("get_block_stats", c.get_block_stats(tip).await);
    if let Some(s) = ck.ok(
        "get_block_stats_filtered",
        c.get_block_stats_filtered(tip, &["avgfeerate", "feerate_percentiles"])
            .await,
    ) {
        ck.check(
            "get_block_stats_filtered",
            s.average_fee_rate.is_some() && s.fee_rate_percentiles.is_some() && s.txs.is_none(),
            "only the requested stats",
        );
    }
    ck.ok(
        "get_block_stats_by_height",
        c.get_block_stats_by_height(height).await,
    );
    ck.ok("get_chain_states", c.get_chain_states().await);
    ck.ok("get_chain_tips", c.get_chain_tips().await);
    ck.ok("get_chain_tx_stats", c.get_chain_tx_stats(None, None).await);
    ck.ok(
        "get_chain_tx_stats",
        c.get_chain_tx_stats(Some(10), Some(tip)).await,
    );
    ck.ok("get_deployment_info", c.get_deployment_info(None).await);
    ck.ok(
        "get_deployment_info",
        c.get_deployment_info(Some(tip)).await,
    );
    let activity = c.get_descriptor_activity(&[tip], &[&desc], true).await;
    if major >= 29 {
        if let Some(a) = ck.ok("get_descriptor_activity", activity) {
            ck.check(
                "get_descriptor_activity",
                !a.activity.is_empty(),
                "tip spends and pays our descriptor",
            );
        }
    } else {
        ck.rpc_err("get_descriptor_activity", activity, NOT_FOUND);
    }
    ck.ok("get_difficulty", c.get_difficulty().await);
    if let Some(p) = ck.ok("get_tx_out_proof", c.get_tx_out_proof(&[ctxid], None).await)
        && let Some(ids) = ck.ok("verify_tx_out_proof", c.verify_tx_out_proof(&p).await)
    {
        ck.check("verify_tx_out_proof", ids == [ctxid], "proves the child");
    }
    ck.ok(
        "get_tx_out_proof",
        c.get_tx_out_proof(&[t3id], Some(c.get_block_hash(112).await?))
            .await,
    );
    ck.ok("get_tx_out_set_info", c.get_tx_out_set_info().await);
    for hash_type in ["none", "hash_serialized_3"] {
        if let Some(i) = ck.ok(
            "get_tx_out_set_info_hash_type",
            c.get_tx_out_set_info_hash_type(hash_type).await,
        ) {
            ck.check(
                "get_tx_out_set_info_hash_type",
                i.hash_serialized_3.is_some() == (hash_type != "none"),
                "hash only when asked",
            );
        }
    }
    ck.ok("dump_tx_out_set", c.dump_tx_out_set("utxo.dat").await);
    // Regtest only knows the snapshot heights in its chainparams.
    ck.rpc_err(
        "load_tx_out_set",
        c.load_tx_out_set("utxo.dat").await,
        -32603,
    );
    ck.ok("precious_block", c.precious_block(tip).await);
    ck.rpc_err("prune_blockchain", c.prune_blockchain(10).await, -1);
    ck.ok(
        "scan_blocks_start",
        c.scan_blocks_start(&[&desc], None, None).await,
    );
    ck.ok(
        "scan_blocks_start",
        c.scan_blocks_start(&[&desc], Some(100), Some(height)).await,
    );
    ck.ok("scan_blocks_status", c.scan_blocks_status().await);
    ck.ok("scan_blocks_abort", c.scan_blocks_abort().await);
    if let Some(s) = ck.ok(
        "scan_tx_out_set_start",
        c.scan_tx_out_set_start(&[&desc]).await,
    ) {
        ck.check("scan_tx_out_set_start", s.success, "scan succeeded");
    }
    ck.ok("scan_tx_out_set_status", c.scan_tx_out_set_status().await);
    ck.ok("scan_tx_out_set_abort", c.scan_tx_out_set_abort().await);
    ck.ok("verify_chain", c.verify_chain(None, None).await);
    ck.ok("verify_chain", c.verify_chain(Some(4), Some(10)).await);
    let short = Some(Duration::from_millis(100));
    ck.ok("wait_for_block", c.wait_for_block(tip, short).await);
    ck.ok(
        "wait_for_block_height",
        c.wait_for_block_height(height, short).await,
    );
    ck.ok("wait_for_new_block", c.wait_for_new_block(short).await);
    ck.ok("invalidate_block", c.invalidate_block(tip).await);
    ck.ok("reconsider_block", c.reconsider_block(tip).await);
    let next = empty_block(&c.get_block_template().await?, &k);
    if let Some(r) = ck.ok(
        "get_block_template_proposal",
        c.get_block_template_proposal(&next).await,
    ) {
        ck.check("get_block_template_proposal", r.is_none(), "valid proposal");
    }
    let mut bad = next;
    bad.header.merkle_root = TxMerkleNode::all_zeros();
    if let Some(r) = ck.ok(
        "get_block_template_proposal",
        c.get_block_template_proposal(&bad).await,
    ) {
        ck.check(
            "get_block_template_proposal",
            r.as_deref() == Some("bad-txnmrklroot"),
            "bad merkle root rejected",
        );
    }
    ck.rpc_err(
        "get_block_from_peer",
        c.get_block_from_peer(tip, 0).await,
        -1,
    );
    let time = u64::from(block.header.time);
    ck.ok("set_mock_time", c.set_mock_time(time + 60).await);
    ck.ok("set_mock_time", c.set_mock_time(0).await);

    // == Rawtransactions
    if let Some(tx) = ck.ok(
        "get_raw_transaction",
        c.get_raw_transaction(ctxid, None).await,
    ) {
        ck.check("get_raw_transaction", tx == child, "same tx");
    }
    ck.ok(
        "get_raw_transaction",
        c.get_raw_transaction(ctxid, Some(tip)).await,
    );
    ck.ok(
        "get_raw_transaction_verbose",
        c.get_raw_transaction_verbose(ctxid, None).await,
    );
    ck.ok(
        "get_raw_transaction_verbose",
        c.get_raw_transaction_verbose(ctxid, Some(tip)).await,
    );
    if let Some(t) = ck.ok(
        "get_raw_transaction_verbose_two",
        c.get_raw_transaction_verbose_two(ctxid, None).await,
    ) {
        ck.check(
            "get_raw_transaction_verbose_two",
            t.fee == Some(FEE) && t.prevouts.len() == 1 && t.prevouts[0].is_some(),
            "fee and prevout from undo data",
        );
    }
    ck.ok(
        "get_raw_transaction_verbose_two",
        c.get_raw_transaction_verbose_two(ctxid, Some(tip)).await,
    );
    ck.ok(
        "decode_raw_transaction",
        c.decode_raw_transaction(&child).await,
    );
    ck.ok(
        "decode_script",
        c.decode_script(&k.addr.script_pubkey()).await,
    );
    let (cb4, v4) = coinbase(c, 4).await?;
    let (cb5, v5) = coinbase(c, 5).await?;
    let out = |v: Amount| [(k.addr.clone(), v - FEE)];
    if let Some(psbt) = ck.ok(
        "create_psbt",
        c.create_psbt(&[cb4], &out(v4), None, None).await,
    ) {
        if let Some(p) = ck.ok("utxo_update_psbt", c.utxo_update_psbt(&psbt).await) {
            ck.check(
                "utxo_update_psbt",
                p.inputs[0].witness_utxo.is_some(),
                "witness_utxo filled",
            );
            ck.ok("analyze_psbt", c.analyze_psbt(&p).await);
            ck.ok("decode_psbt", c.decode_psbt(&p).await);
            ck.ok(
                "combine_psbt",
                c.combine_psbt(&[p.clone(), psbt.clone()]).await,
            );
            if let Some(f) = ck.ok("finalize_psbt", c.finalize_psbt(&p).await) {
                ck.check("finalize_psbt", !f.complete, "unsigned is incomplete");
            }
        }
        if let Some(other) = ck.ok(
            "create_psbt",
            c.create_psbt(&[cb5], &out(v5), Some(b"psbt"), None).await,
        ) && let Some(j) = ck.ok("join_psbts", c.join_psbts(&[psbt, other]).await)
        {
            ck.check("join_psbts", j.inputs.len() == 2, "2 inputs");
        }
    }
    let lock = LockTime::from_height(height as u32).unwrap();
    if let Some(unsigned) = ck.ok(
        "create_raw_transaction",
        c.create_raw_transaction(&[cb4], &out(v4), Some(b"mined"), Some(lock))
            .await,
    ) {
        ck.check(
            "create_raw_transaction",
            unsigned.lock_time == lock
                && unsigned.output.len() == 2
                && unsigned.output[1].script_pubkey.is_op_return()
                && unsigned.output[0].script_pubkey == k.addr.script_pubkey(),
            "payment then OP_RETURN, with locktime",
        );
        ck.ok("convert_to_psbt", c.convert_to_psbt(&unsigned).await);
    }

    // == Util
    ck.ok(
        "create_multisig",
        c.create_multisig(1, &[k.pubkey, k.pubkey]).await,
    );
    if let Some(a) = ck.ok("derive_addresses", c.derive_addresses(&desc, None).await) {
        ck.check("derive_addresses", a.addresses.len() == 1, "1 address");
    }
    let ranged = c
        .get_descriptor_info(&format!("wpkh({}/0/*)", k.xpub))
        .await?
        .descriptor;
    if let Some(a) = ck.ok(
        "derive_addresses",
        c.derive_addresses(&ranged, Some((0, 2))).await,
    ) {
        ck.check("derive_addresses", a.addresses.len() == 3, "3 addresses");
    }
    // `descriptor` is the first expansion; the checksum is the multipath one.
    let multi = format!("wpkh({}/<0;1>/*)", k.xpub);
    let multi = format!("{multi}#{}", c.get_descriptor_info(&multi).await?.checksum);
    if let Some(a) = ck.ok(
        "derive_addresses_multipath",
        c.derive_addresses_multipath(&multi, Some((0, 1))).await,
    ) {
        ck.check(
            "derive_addresses_multipath",
            a.addresses.len() == 2 && a.addresses.iter().all(|p| p.addresses.len() == 2),
            "2 paths x 2 addresses",
        );
    }
    ck.ok("estimate_smart_fee", c.estimate_smart_fee(6, None).await);
    ck.ok(
        "estimate_smart_fee",
        c.estimate_smart_fee(2, Some(FeeMode::Economical)).await,
    );
    ck.ok("get_index_info", c.get_index_info().await);
    if let Some(sig) = ck.ok(
        "sign_message_with_priv_key",
        c.sign_message_with_priv_key(&k.key, "hello").await,
    ) && let Some(v) = ck.ok(
        "verify_message",
        c.verify_message(&k.legacy, &sig.0, "hello").await,
    ) {
        ck.check("verify_message", v, "signature verifies");
    }
    ck.ok(
        "validate_address",
        c.validate_address(&k.addr.to_string()).await,
    );

    // == Network
    let peer = "127.0.0.1:1";
    ck.ok(
        "add_node",
        c.add_node(peer, AddNodeCommand::Add, Some(true)).await,
    );
    ck.ok("get_added_node_info", c.get_added_node_info(None).await);
    ck.ok(
        "get_added_node_info",
        c.get_added_node_info(Some(peer)).await,
    );
    ck.ok(
        "add_node",
        c.add_node(peer, AddNodeCommand::Remove, None).await,
    );
    ck.rpc_err("disconnect_node", c.disconnect_node(peer).await, -29);
    ck.rpc_err(
        "disconnect_node_by_id",
        c.disconnect_node_by_id(99).await,
        -29,
    );
    ck.ok(
        "set_ban",
        c.set_ban(
            "10.0.0.0/8",
            SetBanCommand::Add,
            Some(Duration::from_secs(60)),
        )
        .await,
    );
    ck.ok(
        "set_ban",
        c.set_ban("192.168.1.1", SetBanCommand::Add, None).await,
    );
    if let Some(b) = ck.ok("list_banned", c.list_banned().await) {
        ck.check("list_banned", b.0.len() == 2, "2 bans");
    }
    ck.ok(
        "set_ban",
        c.set_ban("10.0.0.0/8", SetBanCommand::Remove, None).await,
    );
    ck.ok("clear_banned", c.clear_banned().await);
    ck.ok("get_addr_man_info", c.get_addr_man_info().await);
    ck.ok("get_connection_count", c.get_connection_count().await);
    ck.ok("get_net_totals", c.get_net_totals().await);
    ck.ok("get_network_info", c.get_network_info().await);
    ck.ok("get_node_addresses", c.get_node_addresses(None, None).await);
    ck.ok(
        "get_node_addresses",
        c.get_node_addresses(Some(0), Some("ipv4")).await,
    );
    ck.ok("get_peer_info", c.get_peer_info().await);
    ck.ok("ping", c.ping().await);
    if let Some(a) = ck.ok("set_network_active", c.set_network_active(false).await) {
        ck.check("set_network_active", !a, "now inactive");
    }
    ck.ok("set_network_active", c.set_network_active(true).await);

    // == Control, Zmq, Signer, hidden
    ck.ok("get_memory_info", c.get_memory_info().await);
    ck.ok("get_rpc_info", c.get_rpc_info().await);
    ck.ok("help", c.help(None).await);
    ck.ok("help", c.help(Some("getblock")).await);
    ck.ok("logging", c.logging(&[], &[]).await);
    ck.ok("logging", c.logging(&["mempool"], &["net"]).await);
    ck.ok("uptime", c.uptime().await);
    if let Some(z) = ck.ok("get_zmq_notifications", c.get_zmq_notifications().await) {
        ck.check(
            "get_zmq_notifications",
            z.len() == 1 && z[0].type_ == "pubsequence",
            "lists -zmqpubsequence",
        );
    }
    ck.rpc_err("enumerate_signers", c.enumerate_signers().await, -1);
    if let Some(v) = ck.ok("echo", c.echo(&[json!(1), json!("a")]).await) {
        ck.check("echo", v == json!([1, "a"]), "echoes args");
    }
    if let Some(v) = ck.ok("echo_json", c.echo_json(&[json!({ "a": [1] })]).await) {
        ck.check("echo_json", v == json!([{ "a": [1] }]), "echoes args");
    }
    Ok(())
}
