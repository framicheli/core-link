//! Sending, PSBTs, fee bumping, abandoning, simulating, funding and signing.
use super::options::{estimate_mode, inputs_json, sat_per_vb};
use super::{
    BumpFeeOptions, FundOptions, SendAllOptions, SendManyOptions, SendOptions,
    SendToAddressOptions, Wallet,
};
use crate::Feature;
use crate::raw_transactions::{outputs_json, prevtxs_json, sighash_str, unfunded_hex};
use bitcoin::absolute::LockTime;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::{
    Address, Amount, OutPoint, Psbt, TapSighashType, Transaction, TxOut, Txid, transaction,
};
use bitcoin_node_api::ApiResult;
use corepc_types::model;
use serde_json::{Map, Value, json};

impl Wallet {
    /// Refuses a tx `version` before Core 30 rather than send it.
    fn need_version_arg(&self, version: Option<transaction::Version>, rpc: &str) -> ApiResult<()> {
        match version {
            Some(_) => self.client.need(
                Feature::TxVersionArg,
                &format!("{rpc} takes a tx version on Core 30+"),
            ),
            None => Ok(()),
        }
    }

    /// Funds, signs and (unless `add_to_wallet(false)` or `psbt(true)`)
    /// broadcasts. `data` adds an `OP_RETURN` output after `outputs`.
    pub async fn send(
        &self,
        outputs: &[(Address, Amount)],
        data: Option<&[u8]>,
        opts: SendOptions,
    ) -> ApiResult<model::Send> {
        self.need_version_arg(opts.version, "send")?;
        // Fee arguments go in the options object, which also takes them.
        let params = json!([
            outputs_json(outputs, data),
            null,
            null,
            null,
            opts.to_json(),
            opts.version.map(|v| v.0)
        ]);
        versioned!(self, "send", params, Send)
    }

    /// `sendtoaddress`: pays `amount` to `address`.
    pub async fn send_to_address(
        &self,
        address: &Address,
        amount: Amount,
        opts: SendToAddressOptions,
    ) -> ApiResult<Txid> {
        let params = json!([
            address,
            amount.to_btc(),
            opts.comment,
            opts.comment_to,
            opts.subtract_fee_from_amount,
            opts.replaceable,
            opts.conf_target,
            opts.estimate_mode.map(estimate_mode),
            opts.avoid_reuse,
            opts.fee_rate.map(sat_per_vb)
        ]);
        let r: model::SendToAddress = versioned!(self, "sendtoaddress", params, SendToAddress)?;
        Ok(r.txid)
    }

    /// Each address at most once.
    pub async fn send_many(
        &self,
        outputs: &[(Address, Amount)],
        opts: SendManyOptions,
    ) -> ApiResult<Txid> {
        let amounts: Map<String, Value> = outputs
            .iter()
            .map(|(a, v)| (a.to_string(), json!(v.to_btc())))
            .collect();
        let subtract: Vec<_> = opts
            .subtract_fee_from
            .iter()
            .map(Address::to_string)
            .collect();
        let params = json!([
            "", // dummy
            amounts,
            null, // minconf: ignored
            opts.comment,
            (!subtract.is_empty()).then_some(subtract),
            opts.replaceable,
            opts.conf_target,
            opts.estimate_mode.map(estimate_mode),
            opts.fee_rate.map(sat_per_vb)
        ]);
        let r: model::SendMany = versioned!(self, "sendmany", params, SendMany)?;
        Ok(r.0)
    }

    /// Spends every coin (or `opts.inputs`). Recipients with an amount get
    /// it; those without share the rest. At least one must have none.
    pub async fn send_all(
        &self,
        recipients: &[(Address, Option<Amount>)],
        opts: SendAllOptions,
    ) -> ApiResult<model::SendAll> {
        self.need_version_arg(opts.version, "sendall")?;
        let recipients: Vec<_> = recipients
            .iter()
            .map(|(a, v)| match v {
                Some(v) => json!({ a.to_string(): v.to_btc() }),
                None => json!(a),
            })
            .collect();
        let params = json!([recipients, null, null, null, opts.to_json()]);
        versioned!(self, "sendall", params, SendAll)
    }

    /// A funded, unsigned PSBT. With no `inputs` the wallet picks them.
    /// `bip32derivs` defaults to true; `version` needs Core 30+.
    #[allow(clippy::too_many_arguments)]
    pub async fn wallet_create_funded_psbt(
        &self,
        inputs: &[OutPoint],
        outputs: &[(Address, Amount)],
        data: Option<&[u8]>,
        lock_time: Option<LockTime>,
        opts: FundOptions,
        bip32derivs: Option<bool>,
        version: Option<transaction::Version>,
    ) -> ApiResult<model::WalletCreateFundedPsbt> {
        self.need_version_arg(version, "walletcreatefundedpsbt")?;
        let params = json!([
            inputs_json(inputs, &opts.input_weights),
            outputs_json(outputs, data),
            lock_time.map(LockTime::to_consensus_u32),
            crate::wallet::options::object_or_null(opts.to_map(false)),
            bip32derivs,
            version.map(|v| v.0)
        ]);
        versioned!(
            self,
            "walletcreatefundedpsbt",
            params,
            WalletCreateFundedPsbt
        )
    }

    /// Adds what the wallet knows, signs (`sign`, default true) and
    /// finalizes (`finalize`, default true).
    pub async fn wallet_process_psbt(
        &self,
        psbt: &Psbt,
        sign: Option<bool>,
        sighash: Option<TapSighashType>,
        bip32derivs: Option<bool>,
        finalize: Option<bool>,
    ) -> ApiResult<model::WalletProcessPsbt> {
        let params = json!([
            psbt.to_string(),
            sign,
            sighash.map(sighash_str),
            bip32derivs,
            finalize
        ]);
        versioned!(self, "walletprocesspsbt", params, WalletProcessPsbt)
    }

    /// `prevouts` are the outputs spent that the node can't look up.
    pub async fn sign_raw_transaction_with_wallet(
        &self,
        tx: &Transaction,
        prevouts: &[(OutPoint, TxOut)],
        sighash: Option<TapSighashType>,
    ) -> ApiResult<model::SignRawTransaction> {
        let prevtxs = (!prevouts.is_empty()).then(|| prevtxs_json(prevouts));
        let params = json!([serialize_hex(tx), prevtxs, sighash.map(sighash_str)]);
        versioned!(
            self,
            "signrawtransactionwithwallet",
            params,
            SignRawTransactionWithWallet
        )
    }

    /// Replaces a wallet tx with a higher-fee one, signed and broadcast.
    pub async fn bump_fee(&self, txid: Txid, opts: BumpFeeOptions) -> ApiResult<model::BumpFee> {
        versioned!(self, "bumpfee", json!([txid, opts.to_json()]), BumpFee)
    }

    /// `bump_fee`, returning the replacement as an unsigned PSBT.
    pub async fn psbt_bump_fee(
        &self,
        txid: Txid,
        opts: BumpFeeOptions,
    ) -> ApiResult<model::PsbtBumpFee> {
        versioned!(
            self,
            "psbtbumpfee",
            json!([txid, opts.to_json()]),
            PsbtBumpFee
        )
    }

    /// Frees the inputs of a tx that is neither confirmed nor in the mempool.
    pub async fn abandon_transaction(&self, txid: Txid) -> ApiResult<()> {
        self.call("abandontransaction", json!([txid])).await
    }

    /// The wallet's balance change if `txs` (in order) confirmed.
    pub async fn simulate_raw_transaction(
        &self,
        txs: &[Transaction],
    ) -> ApiResult<model::SimulateRawTransaction> {
        let txs: Vec<_> = txs.iter().map(serialize_hex).collect();
        versioned!(
            self,
            "simulaterawtransaction",
            json!([txs]),
            SimulateRawTransaction
        )
    }

    /// Adds inputs (and change) until `tx`'s outputs are paid for. Unsigned.
    pub async fn fund_raw_transaction(
        &self,
        tx: &Transaction,
        opts: FundOptions,
    ) -> ApiResult<model::FundRawTransaction> {
        let opts = crate::wallet::options::object_or_null(opts.to_map(true));
        // A 0-input tx is ambiguous; say it has no witness.
        let iswitness = tx.input.is_empty().then_some(false);
        let params = json!([unfunded_hex(tx), opts, iswitness]);
        versioned!(self, "fundrawtransaction", params, FundRawTransaction)
    }
}
