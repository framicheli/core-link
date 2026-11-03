//! `== Rawtransactions ==` (minus `fundrawtransaction`, which needs a
//! wallet: it's on `Wallet`).
use crate::client::IntoApi;
use crate::{Client, Feature, hex};
use bitcoin::absolute::LockTime;
use bitcoin::consensus::encode::{deserialize, serialize, serialize_hex};
use bitcoin::hex::{DisplayHex, FromHex};
use bitcoin::{
    Address, Amount, BlockHash, FeeRate, OutPoint, PrivateKey, Psbt, Script, TapSighashType,
    Transaction, TxOut, Txid,
};
use bitcoin_node_api::{ApiResult, NodeAPIError};
use corepc_types::model;
use serde::Deserialize;
use serde_json::{Value, json};

/// A tx with no inputs, in the legacy form Core expects. rust-bitcoin writes
/// it in segwit form (marker and flag after the version) so it can read it
/// back, and Core refuses that ("superfluous witness record").
pub(crate) fn unfunded_hex(tx: &Transaction) -> String {
    let mut b = serialize(tx);
    if tx.input.is_empty() {
        b.drain(4..6);
    }
    b.to_lower_hex_string()
}

/// The reverse: Core's 0-input tx, which rust-bitcoin reads as a segwit
/// marker unless the marker and flag are there.
fn unfunded_tx(hex_: &str) -> ApiResult<Transaction> {
    let mut b = Vec::<u8>::from_hex(hex_).map_err(|e| NodeAPIError::Decode(e.to_string()))?;
    if b.get(4) == Some(&0) {
        b.splice(4..4, [0, 1]);
    }
    deserialize(&b).map_err(|e| NodeAPIError::Decode(e.to_string()))
}

fn psbts(psbts: &[Psbt]) -> Vec<String> {
    psbts.iter().map(Psbt::to_string).collect()
}

fn hexes(txs: &[Transaction]) -> Vec<String> {
    txs.iter().map(serialize_hex).collect()
}

/// `[{address: btc}, ..., {data: hex}]`. An array, not an object: an object
/// would be reordered by address.
pub(crate) fn outputs_json(outputs: &[(Address, Amount)], data: Option<&[u8]>) -> Vec<Value> {
    let mut outs: Vec<_> = outputs
        .iter()
        .map(|(a, v)| json!({ a.to_string(): v.to_btc() }))
        .collect();
    if let Some(d) = data {
        outs.push(json!({ "data": d.to_lower_hex_string() }));
    }
    outs
}

/// `prevtxs` for the sign RPCs: the outputs spent that the node can't see.
pub(crate) fn prevtxs_json(prevouts: &[(OutPoint, TxOut)]) -> Vec<Value> {
    prevouts
        .iter()
        .map(|(o, out)| {
            json!({
                "txid": o.txid,
                "vout": o.vout,
                "scriptPubKey": out.script_pubkey.to_hex_string(),
                "amount": out.value.to_btc(),
            })
        })
        .collect()
}

/// Core's sighash names. `TapSighashType` has exactly Core's seven, and Core
/// takes them for every input kind.
pub(crate) fn sighash_str(t: TapSighashType) -> &'static str {
    match t {
        TapSighashType::Default => "DEFAULT",
        TapSighashType::All => "ALL",
        TapSighashType::None => "NONE",
        TapSighashType::Single => "SINGLE",
        TapSighashType::AllPlusAnyoneCanPay => "ALL|ANYONECANPAY",
        TapSighashType::NonePlusAnyoneCanPay => "NONE|ANYONECANPAY",
        TapSighashType::SinglePlusAnyoneCanPay => "SINGLE|ANYONECANPAY",
    }
}

/// Core's `[{txid, vout}]`, outputs, `locktime`.
fn raw_tx_params(
    inputs: &[OutPoint],
    outputs: &[(Address, Amount)],
    data: Option<&[u8]>,
    lock_time: Option<LockTime>,
) -> Value {
    let ins: Vec<_> = inputs
        .iter()
        .map(|o| json!({ "txid": o.txid, "vout": o.vout }))
        .collect();
    let outs = outputs_json(outputs, data);
    match lock_time {
        Some(l) => json!([ins, outs, l.to_consensus_u32()]),
        None => json!([ins, outs]),
    }
}

/// `getrawtransaction` verbosity 2.
#[derive(Clone, Debug, PartialEq)]
pub struct RawTransactionWithPrevouts {
    /// The verbosity 1 fields.
    pub tx: model::GetRawTransactionVerbose,
    /// One per input; `None` for a coinbase or without undo data (mempool txs).
    pub prevouts: Vec<Option<model::GetBlockVerboseThreePrevout>>,
    /// `None` without undo data.
    pub fee: Option<Amount>,
}

/// corepc's struct has no `fee`.
#[derive(Deserialize)]
struct VerboseTwo {
    #[serde(flatten)]
    tx: corepc_types::v29::GetRawTransactionVerboseWithPrevout,
    fee: Option<f64>,
}

impl Client {
    pub(crate) fn need(&self, f: Feature, what: &str) -> ApiResult<()> {
        if self.supports(f) {
            Ok(())
        } else {
            Err(NodeAPIError::Unsupported(what.into()))
        }
    }

    /// Core 31+. `id` is a txid or wtxid.
    pub async fn abort_private_broadcast(
        &self,
        id: Txid,
    ) -> ApiResult<model::AbortPrivateBroadcast> {
        self.need(
            Feature::PrivateBroadcast,
            "abortprivatebroadcast needs Core 31+",
        )?;
        self.call::<corepc_types::v31::AbortPrivateBroadcast>("abortprivatebroadcast", json!([id]))
            .await?
            .into_model()
            .into_api("abortprivatebroadcast")
    }

    /// `analyzepsbt`: each input's status and the next role needed, plus fee and size
    /// estimates when known.
    pub async fn analyze_psbt(&self, psbt: &Psbt) -> ApiResult<model::AnalyzePsbt> {
        versioned!(self, "analyzepsbt", json!([psbt.to_string()]), AnalyzePsbt)
    }

    /// `combinepsbt`: merges the data of several PSBTs for the same tx.
    pub async fn combine_psbt(&self, psbts_: &[Psbt]) -> ApiResult<Psbt> {
        let r: model::CombinePsbt =
            versioned!(self, "combinepsbt", json!([psbts(psbts_)]), CombinePsbt)?;
        Ok(r.0)
    }

    /// `combinerawtransaction`: merges the signatures of partially signed copies of one tx.
    pub async fn combine_raw_transaction(&self, txs: &[Transaction]) -> ApiResult<Transaction> {
        hex(&self
            .call::<String>("combinerawtransaction", json!([hexes(txs)]))
            .await?)
    }

    /// `converttopsbt`: an unsigned PSBT for `tx`. Errors if `tx` has signatures.
    pub async fn convert_to_psbt(&self, tx: &Transaction) -> ApiResult<Psbt> {
        let r: model::ConvertToPsbt = versioned!(
            self,
            "converttopsbt",
            json!([serialize_hex(tx)]),
            ConvertToPsbt
        )?;
        Ok(r.0)
    }

    /// `data` adds an `OP_RETURN` output after `outputs`.
    pub async fn create_psbt(
        &self,
        inputs: &[OutPoint],
        outputs: &[(Address, Amount)],
        data: Option<&[u8]>,
        lock_time: Option<LockTime>,
    ) -> ApiResult<Psbt> {
        let params = raw_tx_params(inputs, outputs, data, lock_time);
        let r: model::CreatePsbt = versioned!(self, "createpsbt", params, CreatePsbt)?;
        Ok(r.0)
    }

    /// `data` adds an `OP_RETURN` output after `outputs`. With no `inputs`
    /// it's for `Wallet::fund_raw_transaction`.
    pub async fn create_raw_transaction(
        &self,
        inputs: &[OutPoint],
        outputs: &[(Address, Amount)],
        data: Option<&[u8]>,
        lock_time: Option<LockTime>,
    ) -> ApiResult<Transaction> {
        let params = raw_tx_params(inputs, outputs, data, lock_time);
        let tx = self.call::<String>("createrawtransaction", params).await?;
        if inputs.is_empty() {
            unfunded_tx(&tx)
        } else {
            hex(&tx)
        }
    }

    /// Updates, signs (with keys in `descriptors`) and finalizes. No wallet
    /// involved. `sighash` defaults to `Default` for Taproot, `All` otherwise;
    /// `bip32derivs` and `finalize` default to true.
    pub async fn descriptor_process_psbt(
        &self,
        psbt: &Psbt,
        descriptors: &[&str],
        sighash: Option<TapSighashType>,
        bip32derivs: Option<bool>,
        finalize: Option<bool>,
    ) -> ApiResult<model::DescriptorProcessPsbt> {
        let params = json!([
            psbt.to_string(),
            descriptors,
            sighash.map(sighash_str),
            bip32derivs,
            finalize
        ]);
        versioned!(self, "descriptorprocesspsbt", params, DescriptorProcessPsbt)
    }

    /// `decodepsbt`: the PSBT's tx, inputs, outputs and fee (if known), as JSON.
    pub async fn decode_psbt(&self, psbt: &Psbt) -> ApiResult<model::DecodePsbt> {
        versioned!(self, "decodepsbt", json!([psbt.to_string()]), DecodePsbt)
    }

    /// `decoderawtransaction`: `tx` as Core's JSON.
    pub async fn decode_raw_transaction(
        &self,
        tx: &Transaction,
    ) -> ApiResult<model::DecodeRawTransaction> {
        let params = json!([serialize_hex(tx)]);
        versioned!(self, "decoderawtransaction", params, DecodeRawTransaction)
    }

    /// `decodescript`: type, asm and address of `script`, and its P2SH/segwit wrappings.
    pub async fn decode_script(&self, script: &Script) -> ApiResult<model::DecodeScript> {
        versioned!(
            self,
            "decodescript",
            json!([script.to_hex_string()]),
            DecodeScript
        )
    }

    /// `finalizepsbt`: builds the final scriptSigs and witnesses; the network tx if complete.
    pub async fn finalize_psbt(&self, psbt: &Psbt) -> ApiResult<model::FinalizePsbt> {
        versioned!(
            self,
            "finalizepsbt",
            json!([psbt.to_string()]),
            FinalizePsbt
        )
    }

    /// Core 31+.
    pub async fn get_private_broadcast_info(&self) -> ApiResult<model::GetPrivateBroadcastInfo> {
        self.need(
            Feature::PrivateBroadcast,
            "getprivatebroadcastinfo needs Core 31+",
        )?;
        self.call::<corepc_types::v31::GetPrivateBroadcastInfo>(
            "getprivatebroadcastinfo",
            json!([]),
        )
        .await?
        .into_model()
        .into_api("getprivatebroadcastinfo")
    }

    /// `hash` is needed for confirmed txs without `-txindex`.
    pub async fn get_raw_transaction(
        &self,
        txid: Txid,
        hash: Option<BlockHash>,
    ) -> ApiResult<Transaction> {
        let params = match hash {
            Some(h) => json!([txid, 0, h]),
            None => json!([txid, 0]),
        };
        hex(&self.call::<String>("getrawtransaction", params).await?)
    }

    /// `getrawtransaction` verbosity 1: the decoded tx, plus block hash and confirmations
    /// when confirmed. `hash` is needed for confirmed txs without `-txindex`.
    pub async fn get_raw_transaction_verbose(
        &self,
        txid: Txid,
        hash: Option<BlockHash>,
    ) -> ApiResult<model::GetRawTransactionVerbose> {
        let params = match hash {
            Some(h) => json!([txid, 1, h]),
            None => json!([txid, 1]),
        };
        versioned!(self, "getrawtransaction", params, GetRawTransactionVerbose)
    }

    /// Adds each input's prevout and the fee, from undo data: confirmed txs only.
    pub async fn get_raw_transaction_verbose_two(
        &self,
        txid: Txid,
        hash: Option<BlockHash>,
    ) -> ApiResult<RawTransactionWithPrevouts> {
        let params = match hash {
            Some(h) => json!([txid, 2, h]),
            None => json!([txid, 2]),
        };
        // Core 29–31 share the v29 shape.
        let r: VerboseTwo = self.call("getrawtransaction", params).await?;
        let (tx, prevouts) =
            r.tx.into_model_with_prevouts()
                .into_api("getrawtransaction")?;
        let fee = r
            .fee
            .map(Amount::from_btc)
            .transpose()
            .into_api("getrawtransaction")?;
        Ok(RawTransactionWithPrevouts { tx, prevouts, fee })
    }

    /// `joinpsbts`: one PSBT with the inputs and outputs of several PSBTs for different txs.
    pub async fn join_psbts(&self, psbts_: &[Psbt]) -> ApiResult<Psbt> {
        let r: model::JoinPsbts = versioned!(self, "joinpsbts", json!([psbts(psbts_)]), JoinPsbts)?;
        Ok(r.0)
    }

    /// `max_fee_rate` defaults to Core's 0.10 BTC/kvB; `Some(FeeRate::ZERO)` disables the check.
    pub async fn send_raw_transaction(
        &self,
        tx: &Transaction,
        max_fee_rate: Option<FeeRate>,
    ) -> ApiResult<Txid> {
        let params = match max_fee_rate {
            // sat/kwu -> BTC/kvB
            Some(r) => json!([serialize_hex(tx), r.to_sat_per_kwu() as f64 * 4.0 / 1e8]),
            None => json!([serialize_hex(tx)]),
        };
        self.call("sendrawtransaction", params).await
    }

    /// Never send keys to a node you don't run. `prevouts` are the outputs
    /// spent that the node can't look up, e.g. a parent not yet in the mempool.
    pub async fn sign_raw_transaction_with_key(
        &self,
        tx: &Transaction,
        keys: &[PrivateKey],
        prevouts: &[(OutPoint, TxOut)],
    ) -> ApiResult<model::SignRawTransaction> {
        let keys: Vec<_> = keys.iter().map(|k| k.to_wif()).collect();
        let params = json!([serialize_hex(tx), keys, prevtxs_json(prevouts)]);
        versioned!(
            self,
            "signrawtransactionwithkey",
            params,
            SignRawTransactionWithKey
        )
    }

    /// `submitpackage`: submits `txs` (parents first) to the mempool as a package and
    /// relays them. Core 28+.
    pub async fn submit_package(&self, txs: &[Transaction]) -> ApiResult<model::SubmitPackage> {
        self.need(Feature::SubmitPackage, "submitpackage needs Core 28+")?;
        versioned!(self, "submitpackage", json!([hexes(txs)]), SubmitPackage)
    }

    /// `testmempoolaccept`: whether each tx would be accepted to the mempool, without
    /// submitting it.
    pub async fn test_mempool_accept(
        &self,
        txs: &[Transaction],
    ) -> ApiResult<model::TestMempoolAccept> {
        versioned!(
            self,
            "testmempoolaccept",
            json!([hexes(txs)]),
            TestMempoolAccept
        )
    }

    /// `utxoupdatepsbt`: fills in inputs' UTXO data from the UTXO set or mempool.
    pub async fn utxo_update_psbt(&self, psbt: &Psbt) -> ApiResult<Psbt> {
        let r: model::UtxoUpdatePsbt = versioned!(
            self,
            "utxoupdatepsbt",
            json!([psbt.to_string()]),
            UtxoUpdatePsbt
        )?;
        Ok(r.0)
    }
}
