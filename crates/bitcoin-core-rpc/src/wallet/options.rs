//! Options for wallet RPCs with many optional arguments. `#[non_exhaustive]`
//! with a setter per field, so Core can add options without breaking callers:
//! `CreateWalletOptions::default().blank(true)`.
use bitcoin::absolute::LockTime;
use bitcoin::{Address, Amount, FeeRate, OutPoint, PublicKey, ScriptBuf, Weight, transaction};
use bitcoin_node_api::FeeMode;
use serde_json::{Map, Value, json};

/// `createwallet`. Every wallet is a descriptor wallet.
#[derive(Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CreateWalletOptions {
    /// Watch-only: no private keys, ever.
    pub disable_private_keys: bool,
    /// No keys or descriptors until imported.
    pub blank: bool,
    /// Encrypt with this passphrase. Hidden from `Debug`.
    pub passphrase: Option<String>,
    /// Track address reuse and keep reused ("dirty") coins apart from clean ones.
    pub avoid_reuse: bool,
    /// `None` leaves the startup list unchanged.
    pub load_on_startup: Option<bool>,
    /// Needs `-signer` and `disable_private_keys`.
    pub external_signer: bool,
}

impl CreateWalletOptions {
    /// Watch-only wallet with no private keys. Default false.
    pub fn disable_private_keys(mut self, v: bool) -> Self {
        self.disable_private_keys = v;
        self
    }

    /// Create an empty wallet with no keys or descriptors. Default false.
    pub fn blank(mut self, v: bool) -> Self {
        self.blank = v;
        self
    }

    /// Encrypt the wallet with this passphrase.
    pub fn passphrase(mut self, v: &str) -> Self {
        self.passphrase = Some(v.to_owned());
        self
    }

    /// Set the `avoid_reuse` wallet flag. Default false.
    pub fn avoid_reuse(mut self, v: bool) -> Self {
        self.avoid_reuse = v;
        self
    }

    /// `true` adds the wallet to the startup list, `false` removes it. Omitted: list unchanged.
    pub fn load_on_startup(mut self, v: bool) -> Self {
        self.load_on_startup = Some(v);
        self
    }

    /// Use an external signer (hardware wallet). Needs `-signer` and `disable_private_keys`.
    pub fn external_signer(mut self, v: bool) -> Self {
        self.external_signer = v;
        self
    }

    pub(crate) fn params(&self, name: &str) -> Value {
        json!([
            name,
            self.disable_private_keys,
            self.blank,
            self.passphrase,
            self.avoid_reuse,
            true, // descriptors
            self.load_on_startup,
            self.external_signer,
        ])
    }
}

impl std::fmt::Debug for CreateWalletOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateWalletOptions")
            .field("disable_private_keys", &self.disable_private_keys)
            .field("blank", &self.blank)
            .field("passphrase", &self.passphrase.as_ref().map(|_| "<hidden>"))
            .field("avoid_reuse", &self.avoid_reuse)
            .field("load_on_startup", &self.load_on_startup)
            .field("external_signer", &self.external_signer)
            .finish()
    }
}

/// Core's `-addresstype` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AddressType {
    /// P2PKH.
    Legacy,
    /// P2SH-wrapped P2WPKH.
    P2shSegwit,
    /// Native segwit v0, P2WPKH.
    Bech32,
    /// Taproot, P2TR.
    Bech32m,
}

impl AddressType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            AddressType::Legacy => "legacy",
            AddressType::P2shSegwit => "p2sh-segwit",
            AddressType::Bech32 => "bech32",
            AddressType::Bech32m => "bech32m",
        }
    }
}

/// `lockunspent`'s `unlock` flag, named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockAction {
    /// Lock the outputs: coin selection skips them.
    Lock,
    /// Unlock the outputs.
    Unlock,
}

/// `listunspent`'s `query_options`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ListUnspentQuery {
    /// Smallest output value listed.
    pub minimum_amount: Option<Amount>,
    /// Largest output value listed.
    pub maximum_amount: Option<Amount>,
    /// Maximum number of outputs listed.
    pub maximum_count: Option<u32>,
    /// Stop once the outputs listed add up to this.
    pub minimum_sum_amount: Option<Amount>,
    /// Also list immature coinbase outputs.
    pub include_immature_coinbase: bool,
}

impl ListUnspentQuery {
    /// Only outputs worth at least this (`minimumAmount`). Default 0.
    pub fn minimum_amount(mut self, v: Amount) -> Self {
        self.minimum_amount = Some(v);
        self
    }

    /// Only outputs worth at most this (`maximumAmount`). Default unlimited.
    pub fn maximum_amount(mut self, v: Amount) -> Self {
        self.maximum_amount = Some(v);
        self
    }

    /// List at most this many outputs (`maximumCount`). Default unlimited.
    pub fn maximum_count(mut self, v: u32) -> Self {
        self.maximum_count = Some(v);
        self
    }

    /// Stop once the outputs listed add up to this (`minimumSumAmount`). Default unlimited.
    pub fn minimum_sum_amount(mut self, v: Amount) -> Self {
        self.minimum_sum_amount = Some(v);
        self
    }

    /// Include immature coinbase outputs. Default false.
    pub fn include_immature_coinbase(mut self, v: bool) -> Self {
        self.include_immature_coinbase = v;
        self
    }

    /// `null` when nothing is set.
    pub(crate) fn to_json(&self) -> Value {
        let mut o = Map::new();
        put(
            &mut o,
            "minimumAmount",
            self.minimum_amount.map(Amount::to_btc),
        );
        put(
            &mut o,
            "maximumAmount",
            self.maximum_amount.map(Amount::to_btc),
        );
        put(&mut o, "maximumCount", self.maximum_count);
        put(
            &mut o,
            "minimumSumAmount",
            self.minimum_sum_amount.map(Amount::to_btc),
        );
        put(
            &mut o,
            "include_immature_coinbase",
            self.include_immature_coinbase.then_some(true),
        );
        object_or_null(o)
    }
}

/// Adds `key` when `v` is set: unset options are left out, not `null`.
pub(crate) fn put(o: &mut Map<String, Value>, key: &str, v: Option<impl Into<Value>>) {
    if let Some(v) = v {
        o.insert(key.to_owned(), v.into());
    }
}

pub(crate) fn object_or_null(o: Map<String, Value>) -> Value {
    if o.is_empty() {
        Value::Null
    } else {
        Value::Object(o)
    }
}

/// Core takes fee rates in sat/vB, up to 3 decimals; a `FeeRate` is sat/kwu.
pub(crate) fn sat_per_vb(r: FeeRate) -> f64 {
    r.to_sat_per_kwu() as f64 / 250.0
}

pub(crate) fn estimate_mode(m: FeeMode) -> &'static str {
    match m {
        FeeMode::Conservative => "conservative",
        FeeMode::Economical => "economical",
    }
}

/// `[{txid, vout}, ...]`, with `weight` for inputs that have one.
pub(crate) fn inputs_json(inputs: &[OutPoint], weights: &[(OutPoint, Weight)]) -> Vec<Value> {
    inputs
        .iter()
        .map(|o| {
            let mut v = json!({ "txid": o.txid, "vout": o.vout });
            if let Some((_, w)) = weights.iter().find(|(p, _)| p == o) {
                v["weight"] = json!(w.to_wu());
            }
            v
        })
        .collect()
}

/// Keys and scripts for inputs the wallet can't solve itself, so coin
/// selection can size their signatures.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SolvingData {
    /// Public keys involved.
    pub pubkeys: Vec<PublicKey>,
    /// Redeem and witness scripts involved.
    pub scripts: Vec<ScriptBuf>,
    /// Descriptors that provide solving data.
    pub descriptors: Vec<String>,
}

impl SolvingData {
    /// Public keys the wallet doesn't know.
    pub fn pubkeys(mut self, v: Vec<PublicKey>) -> Self {
        self.pubkeys = v;
        self
    }

    /// Redeem and witness scripts, as `scripts` hex.
    pub fn scripts(mut self, v: Vec<ScriptBuf>) -> Self {
        self.scripts = v;
        self
    }

    /// Descriptors that provide solving data.
    pub fn descriptors(mut self, v: Vec<String>) -> Self {
        self.descriptors = v;
        self
    }

    fn to_json(&self) -> Value {
        let pubkeys: Vec<_> = self.pubkeys.iter().map(PublicKey::to_string).collect();
        let scripts: Vec<_> = self.scripts.iter().map(|s| s.to_hex_string()).collect();
        json!({ "pubkeys": pubkeys, "scripts": scripts, "descriptors": self.descriptors })
    }
}

/// Coin selection and fees, shared by `fund_raw_transaction`,
/// `wallet_create_funded_psbt` and `send`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FundOptions {
    /// Default: true, or false when inputs are given.
    pub add_inputs: Option<bool>,
    /// Allow unsafe inputs: unconfirmed from outside keys, or unconfirmed replacements.
    pub include_unsafe: Option<bool>,
    /// Minimum confirmations for inputs the wallet adds.
    pub min_conf: Option<u32>,
    /// Maximum confirmations for inputs the wallet adds.
    pub max_conf: Option<u32>,
    /// Where change goes. Default: a new wallet address.
    pub change_address: Option<Address>,
    /// Index of the change output. Default: random.
    pub change_position: Option<u32>,
    /// Ignored with `change_address`.
    pub change_type: Option<AddressType>,
    /// Fee rate, sent to Core as sat/vB.
    pub fee_rate: Option<FeeRate>,
    /// With `estimate_mode`, instead of `fee_rate`.
    pub conf_target: Option<u16>,
    /// Fee estimation mode, with `conf_target`.
    pub estimate_mode: Option<FeeMode>,
    /// Lock the selected coins.
    pub lock_unspents: Option<bool>,
    /// Signal BIP 125 replaceability.
    pub replaceable: Option<bool>,
    /// Output indexes, before change is added.
    pub subtract_fee_from_outputs: Vec<u32>,
    /// Maximum weight of inputs the wallet can't solve; only for inputs given
    /// explicitly (or already in the transaction, for `fund_raw_transaction`).
    pub input_weights: Vec<(OutPoint, Weight)>,
    /// Upper bound on the transaction's weight.
    pub max_tx_weight: Option<Weight>,
    /// Keys, scripts and descriptors for inputs the wallet can't solve.
    pub solving_data: Option<SolvingData>,
}

impl FundOptions {
    /// Let the wallet add inputs beyond the ones given.
    /// Default: true, or false when inputs are given.
    pub fn add_inputs(mut self, v: bool) -> Self {
        self.add_inputs = Some(v);
        self
    }

    /// Allow unconfirmed inputs from outside keys and unconfirmed replacements;
    /// the transaction is invalidated if they go away. Default false.
    pub fn include_unsafe(mut self, v: bool) -> Self {
        self.include_unsafe = Some(v);
        self
    }

    /// Only add inputs with at least this many confirmations (`minconf`).
    pub fn min_conf(mut self, v: u32) -> Self {
        self.min_conf = Some(v);
        self
    }

    /// Only add inputs with at most this many confirmations (`maxconf`).
    pub fn max_conf(mut self, v: u32) -> Self {
        self.max_conf = Some(v);
        self
    }

    /// Send change here. Default: a new wallet address.
    pub fn change_address(mut self, v: Address) -> Self {
        self.change_address = Some(v);
        self
    }

    /// Index of the change output. Default: random.
    pub fn change_position(mut self, v: u32) -> Self {
        self.change_position = Some(v);
        self
    }

    /// Address type for change. Ignored with `change_address`. Default: `-changetype`.
    pub fn change_type(mut self, v: AddressType) -> Self {
        self.change_type = Some(v);
        self
    }

    /// Explicit fee rate, sent as sat/vB (Core's `fee_rate` unit). Instead of `conf_target`.
    pub fn fee_rate(mut self, v: FeeRate) -> Self {
        self.fee_rate = Some(v);
        self
    }

    /// Confirmation target in blocks, for fee estimation. Default: `-txconfirmtarget`.
    pub fn conf_target(mut self, v: u16) -> Self {
        self.conf_target = Some(v);
        self
    }

    /// Fee estimation mode used with `conf_target`.
    pub fn estimate_mode(mut self, v: FeeMode) -> Self {
        self.estimate_mode = Some(v);
        self
    }

    /// Lock the selected coins so later coin selection skips them. Default false.
    pub fn lock_unspents(mut self, v: bool) -> Self {
        self.lock_unspents = Some(v);
        self
    }

    /// Signal BIP 125 replaceability. Default: `-walletrbf`.
    pub fn replaceable(mut self, v: bool) -> Self {
        self.replaceable = Some(v);
        self
    }

    /// Deduct the fee equally from these outputs, by index, before change is added.
    pub fn subtract_fee_from_outputs(mut self, v: Vec<u32>) -> Self {
        self.subtract_fee_from_outputs = v;
        self
    }

    /// Maximum weight of inputs the wallet can't solve. See the field.
    pub fn input_weights(mut self, v: Vec<(OutPoint, Weight)>) -> Self {
        self.input_weights = v;
        self
    }

    /// Fail if the funded transaction would weigh more than this. Default 400,000 WU.
    pub fn max_tx_weight(mut self, v: Weight) -> Self {
        self.max_tx_weight = Some(v);
        self
    }

    /// Keys, scripts and descriptors for inputs the wallet can't solve.
    pub fn solving_data(mut self, v: SolvingData) -> Self {
        self.solving_data = Some(v);
        self
    }

    /// The options object. `input_weights` goes in only when the inputs
    /// aren't a list of their own (`fundrawtransaction`): Core refuses it
    /// next to `inputs`, where each input carries its `weight`.
    pub(crate) fn to_map(&self, input_weights: bool) -> Map<String, Value> {
        let mut o = Map::new();
        put(&mut o, "add_inputs", self.add_inputs);
        put(&mut o, "include_unsafe", self.include_unsafe);
        put(&mut o, "minconf", self.min_conf);
        put(&mut o, "maxconf", self.max_conf);
        put(
            &mut o,
            "change_address",
            self.change_address.as_ref().map(|a| a.to_string()),
        );
        put(&mut o, "change_position", self.change_position);
        put(
            &mut o,
            "change_type",
            self.change_type.map(AddressType::as_str),
        );
        put(&mut o, "fee_rate", self.fee_rate.map(sat_per_vb));
        put(&mut o, "conf_target", self.conf_target);
        put(
            &mut o,
            "estimate_mode",
            self.estimate_mode.map(estimate_mode),
        );
        put(&mut o, "lock_unspents", self.lock_unspents);
        put(&mut o, "replaceable", self.replaceable);
        if !self.subtract_fee_from_outputs.is_empty() {
            o.insert(
                "subtract_fee_from_outputs".into(),
                json!(self.subtract_fee_from_outputs),
            );
        }
        if input_weights && !self.input_weights.is_empty() {
            let w: Vec<_> = self
                .input_weights
                .iter()
                .map(|(p, w)| json!({ "txid": p.txid, "vout": p.vout, "weight": w.to_wu() }))
                .collect();
            o.insert("input_weights".into(), json!(w));
        }
        put(
            &mut o,
            "max_tx_weight",
            self.max_tx_weight.map(Weight::to_wu),
        );
        put(
            &mut o,
            "solving_data",
            self.solving_data.as_ref().map(SolvingData::to_json),
        );
        o
    }
}

/// `send`: `FundOptions` plus what `send` alone takes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SendOptions {
    /// Coin selection and fees.
    pub fund: FundOptions,
    /// `false`: return the signed tx without broadcasting or storing it.
    pub add_to_wallet: Option<bool>,
    /// Always return a PSBT; implies `add_to_wallet(false)`.
    pub psbt: Option<bool>,
    /// Spend these; `fund.add_inputs` decides whether to add more.
    pub inputs: Vec<OutPoint>,
    /// Default: anti-fee-sniping (close to the tip).
    pub locktime: Option<LockTime>,
    /// Core 30+.
    pub version: Option<transaction::Version>,
}

impl SendOptions {
    /// Coin selection and fees.
    pub fn fund(mut self, v: FundOptions) -> Self {
        self.fund = v;
        self
    }

    /// `false`: return the signed tx without broadcasting or storing it. Default true.
    pub fn add_to_wallet(mut self, v: bool) -> Self {
        self.add_to_wallet = Some(v);
        self
    }

    /// Always return a PSBT; implies `add_to_wallet(false)`.
    pub fn psbt(mut self, v: bool) -> Self {
        self.psbt = Some(v);
        self
    }

    /// Spend these; `fund.add_inputs` decides whether to add more.
    pub fn inputs(mut self, v: Vec<OutPoint>) -> Self {
        self.inputs = v;
        self
    }

    /// Raw locktime. Default: anti-fee-sniping (close to the tip).
    pub fn locktime(mut self, v: LockTime) -> Self {
        self.locktime = Some(v);
        self
    }

    /// Transaction version. Core 30+.
    pub fn version(mut self, v: transaction::Version) -> Self {
        self.version = Some(v);
        self
    }

    pub(crate) fn to_json(&self) -> Value {
        let mut o = self.fund.to_map(false);
        put(&mut o, "add_to_wallet", self.add_to_wallet);
        put(&mut o, "psbt", self.psbt);
        if !self.inputs.is_empty() {
            let inputs = inputs_json(&self.inputs, &self.fund.input_weights);
            o.insert("inputs".into(), json!(inputs));
        }
        put(
            &mut o,
            "locktime",
            self.locktime.map(LockTime::to_consensus_u32),
        );
        object_or_null(o)
    }
}

/// `sendall`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SendAllOptions {
    /// Spend exactly these. Not with `send_max`, `min_conf` or `max_conf`.
    pub inputs: Vec<OutPoint>,
    /// Leave out coins that cost more in fees than they're worth.
    pub send_max: Option<bool>,
    /// Minimum confirmations for inputs.
    pub min_conf: Option<u32>,
    /// Maximum confirmations for inputs.
    pub max_conf: Option<u32>,
    /// Raw locktime.
    pub locktime: Option<LockTime>,
    /// Lock the spent coins.
    pub lock_unspents: Option<bool>,
    /// `false`: return the signed tx without broadcasting or storing it.
    pub add_to_wallet: Option<bool>,
    /// Always return a PSBT.
    pub psbt: Option<bool>,
    /// Core 30+.
    pub version: Option<transaction::Version>,
    /// Fee rate, sent to Core as sat/vB.
    pub fee_rate: Option<FeeRate>,
    /// Confirmation target in blocks.
    pub conf_target: Option<u16>,
    /// Fee estimation mode, with `conf_target`.
    pub estimate_mode: Option<FeeMode>,
    /// Signal BIP 125 replaceability.
    pub replaceable: Option<bool>,
}

impl SendAllOptions {
    /// Spend exactly these. Not with `send_max`, `min_conf` or `max_conf`.
    pub fn inputs(mut self, v: Vec<OutPoint>) -> Self {
        self.inputs = v;
        self
    }

    /// Leave out coins that cost more in fees than they're worth. Default false.
    pub fn send_max(mut self, v: bool) -> Self {
        self.send_max = Some(v);
        self
    }

    /// Only spend coins with at least this many confirmations (`minconf`).
    pub fn min_conf(mut self, v: u32) -> Self {
        self.min_conf = Some(v);
        self
    }

    /// Only spend coins with at most this many confirmations (`maxconf`).
    pub fn max_conf(mut self, v: u32) -> Self {
        self.max_conf = Some(v);
        self
    }

    /// Raw locktime. Default: anti-fee-sniping (close to the tip).
    pub fn locktime(mut self, v: LockTime) -> Self {
        self.locktime = Some(v);
        self
    }

    /// Lock the spent coins so later coin selection skips them. Default false.
    pub fn lock_unspents(mut self, v: bool) -> Self {
        self.lock_unspents = Some(v);
        self
    }

    /// `false`: return the signed tx without broadcasting or storing it. Default true.
    pub fn add_to_wallet(mut self, v: bool) -> Self {
        self.add_to_wallet = Some(v);
        self
    }

    /// Always return a PSBT; implies `add_to_wallet(false)`.
    pub fn psbt(mut self, v: bool) -> Self {
        self.psbt = Some(v);
        self
    }

    /// Transaction version. Core 30+.
    pub fn version(mut self, v: transaction::Version) -> Self {
        self.version = Some(v);
        self
    }

    /// Explicit fee rate, sent as sat/vB (Core's `fee_rate` unit). Instead of `conf_target`.
    pub fn fee_rate(mut self, v: FeeRate) -> Self {
        self.fee_rate = Some(v);
        self
    }

    /// Confirmation target in blocks, for fee estimation. Default: `-txconfirmtarget`.
    pub fn conf_target(mut self, v: u16) -> Self {
        self.conf_target = Some(v);
        self
    }

    /// Fee estimation mode used with `conf_target`.
    pub fn estimate_mode(mut self, v: FeeMode) -> Self {
        self.estimate_mode = Some(v);
        self
    }

    /// Signal BIP 125 replaceability. Default: `-walletrbf`.
    pub fn replaceable(mut self, v: bool) -> Self {
        self.replaceable = Some(v);
        self
    }

    pub(crate) fn to_json(&self) -> Value {
        let mut o = Map::new();
        if !self.inputs.is_empty() {
            o.insert("inputs".into(), json!(inputs_json(&self.inputs, &[])));
        }
        put(&mut o, "send_max", self.send_max);
        put(&mut o, "minconf", self.min_conf);
        put(&mut o, "maxconf", self.max_conf);
        put(
            &mut o,
            "locktime",
            self.locktime.map(LockTime::to_consensus_u32),
        );
        put(&mut o, "lock_unspents", self.lock_unspents);
        put(&mut o, "add_to_wallet", self.add_to_wallet);
        put(&mut o, "psbt", self.psbt);
        put(&mut o, "version", self.version.map(|v| v.0));
        put(&mut o, "fee_rate", self.fee_rate.map(sat_per_vb));
        put(&mut o, "conf_target", self.conf_target);
        put(
            &mut o,
            "estimate_mode",
            self.estimate_mode.map(estimate_mode),
        );
        put(&mut o, "replaceable", self.replaceable);
        object_or_null(o)
    }
}

/// `sendtoaddress`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SendToAddressOptions {
    /// Stored in the wallet, not in the transaction.
    pub comment: Option<String>,
    /// Who it's for; stored in the wallet only.
    pub comment_to: Option<String>,
    /// The recipient pays the fee.
    pub subtract_fee_from_amount: Option<bool>,
    /// Signal BIP 125 replaceability.
    pub replaceable: Option<bool>,
    /// Confirmation target in blocks.
    pub conf_target: Option<u16>,
    /// Fee estimation mode, with `conf_target`.
    pub estimate_mode: Option<FeeMode>,
    /// Only for wallets with the `avoid_reuse` flag.
    pub avoid_reuse: Option<bool>,
    /// Fee rate, sent to Core as sat/vB.
    pub fee_rate: Option<FeeRate>,
}

impl SendToAddressOptions {
    /// Stored in the wallet, not in the transaction.
    pub fn comment(mut self, v: &str) -> Self {
        self.comment = Some(v.to_owned());
        self
    }

    /// Who it's for (`comment_to`); stored in the wallet only.
    pub fn comment_to(mut self, v: &str) -> Self {
        self.comment_to = Some(v.to_owned());
        self
    }

    /// The recipient pays the fee: it comes out of the amount
    /// (`subtractfeefromamount`). Default false.
    pub fn subtract_fee_from_amount(mut self, v: bool) -> Self {
        self.subtract_fee_from_amount = Some(v);
        self
    }

    /// Signal BIP 125 replaceability. Default: `-walletrbf`.
    pub fn replaceable(mut self, v: bool) -> Self {
        self.replaceable = Some(v);
        self
    }

    /// Confirmation target in blocks, for fee estimation. Default: `-txconfirmtarget`.
    pub fn conf_target(mut self, v: u16) -> Self {
        self.conf_target = Some(v);
        self
    }

    /// Fee estimation mode used with `conf_target`.
    pub fn estimate_mode(mut self, v: FeeMode) -> Self {
        self.estimate_mode = Some(v);
        self
    }

    /// Don't spend from previously used addresses. Only for wallets with the
    /// `avoid_reuse` flag. Default true.
    pub fn avoid_reuse(mut self, v: bool) -> Self {
        self.avoid_reuse = Some(v);
        self
    }

    /// Explicit fee rate, sent as sat/vB (Core's `fee_rate` unit). Instead of `conf_target`.
    pub fn fee_rate(mut self, v: FeeRate) -> Self {
        self.fee_rate = Some(v);
        self
    }
}

/// `sendmany`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SendManyOptions {
    /// Stored in the wallet, not in the transaction.
    pub comment: Option<String>,
    /// These recipients share the fee.
    pub subtract_fee_from: Vec<Address>,
    /// Signal BIP 125 replaceability.
    pub replaceable: Option<bool>,
    /// Confirmation target in blocks.
    pub conf_target: Option<u16>,
    /// Fee estimation mode, with `conf_target`.
    pub estimate_mode: Option<FeeMode>,
    /// Fee rate, sent to Core as sat/vB.
    pub fee_rate: Option<FeeRate>,
}

impl SendManyOptions {
    /// Stored in the wallet, not in the transaction.
    pub fn comment(mut self, v: &str) -> Self {
        self.comment = Some(v.to_owned());
        self
    }

    /// These recipients share the fee equally, out of their amounts (`subtractfeefrom`).
    pub fn subtract_fee_from(mut self, v: Vec<Address>) -> Self {
        self.subtract_fee_from = v;
        self
    }

    /// Signal BIP 125 replaceability. Default: `-walletrbf`.
    pub fn replaceable(mut self, v: bool) -> Self {
        self.replaceable = Some(v);
        self
    }

    /// Confirmation target in blocks, for fee estimation. Default: `-txconfirmtarget`.
    pub fn conf_target(mut self, v: u16) -> Self {
        self.conf_target = Some(v);
        self
    }

    /// Fee estimation mode used with `conf_target`.
    pub fn estimate_mode(mut self, v: FeeMode) -> Self {
        self.estimate_mode = Some(v);
        self
    }

    /// Explicit fee rate, sent as sat/vB (Core's `fee_rate` unit). Instead of `conf_target`.
    pub fn fee_rate(mut self, v: FeeRate) -> Self {
        self.fee_rate = Some(v);
        self
    }
}

/// `bumpfee` and `psbtbumpfee`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BumpFeeOptions {
    /// Confirmation target in blocks.
    pub conf_target: Option<u16>,
    /// At least 0.1 sat/vB above the original.
    pub fee_rate: Option<FeeRate>,
    /// Default true.
    pub replaceable: Option<bool>,
    /// Fee estimation mode, with `conf_target`.
    pub estimate_mode: Option<FeeMode>,
    /// Replace the outputs. Not with `original_change_index`.
    pub outputs: Vec<(Address, Amount)>,
    /// The original's change output, to recycle.
    pub original_change_index: Option<u32>,
}

impl BumpFeeOptions {
    /// Confirmation target in blocks, for fee estimation. Default: `-txconfirmtarget`.
    pub fn conf_target(mut self, v: u16) -> Self {
        self.conf_target = Some(v);
        self
    }

    /// New fee rate, sent as sat/vB (Core's `fee_rate` unit); at least 0.1 sat/vB
    /// above the original. Instead of `conf_target`.
    pub fn fee_rate(mut self, v: FeeRate) -> Self {
        self.fee_rate = Some(v);
        self
    }

    /// Signal BIP 125 replaceability on the replacement. Default true.
    pub fn replaceable(mut self, v: bool) -> Self {
        self.replaceable = Some(v);
        self
    }

    /// Fee estimation mode used with `conf_target`.
    pub fn estimate_mode(mut self, v: FeeMode) -> Self {
        self.estimate_mode = Some(v);
        self
    }

    /// Replace the original's outputs with these. Not with `original_change_index`.
    pub fn outputs(mut self, v: Vec<(Address, Amount)>) -> Self {
        self.outputs = v;
        self
    }

    /// Index of the original's change output, to recycle as change.
    pub fn original_change_index(mut self, v: u32) -> Self {
        self.original_change_index = Some(v);
        self
    }

    pub(crate) fn to_json(&self) -> Value {
        let mut o = Map::new();
        put(&mut o, "conf_target", self.conf_target);
        put(&mut o, "fee_rate", self.fee_rate.map(sat_per_vb));
        put(&mut o, "replaceable", self.replaceable);
        put(
            &mut o,
            "estimate_mode",
            self.estimate_mode.map(estimate_mode),
        );
        if !self.outputs.is_empty() {
            let outs = crate::raw_transactions::outputs_json(&self.outputs, None);
            o.insert("outputs".into(), json!(outs));
        }
        put(&mut o, "original_change_index", self.original_change_index);
        object_or_null(o)
    }
}

/// When an imported descriptor was first used: rescanning starts there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timestamp {
    /// Never used: no rescan.
    Now,
    /// UNIX time; 0 scans the whole chain.
    Time(u64),
}

/// One `importdescriptors` request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ImportDescriptor {
    /// The descriptor, with checksum.
    pub desc: String,
    /// Where rescanning starts.
    pub timestamp: Timestamp,
    /// The wallet's active descriptor for its type and internal/external.
    pub active: Option<bool>,
    /// Inclusive, for ranged descriptors.
    pub range: Option<(u32, u32)>,
    /// Next index to derive from a ranged descriptor.
    pub next_index: Option<u32>,
    /// Change, not incoming payments.
    pub internal: Option<bool>,
    /// Not for ranged descriptors.
    pub label: Option<String>,
}

impl ImportDescriptor {
    /// Import `desc`, rescanning from `timestamp`.
    pub fn new(desc: &str, timestamp: Timestamp) -> Self {
        ImportDescriptor {
            desc: desc.to_owned(),
            timestamp,
            active: None,
            range: None,
            next_index: None,
            internal: None,
            label: None,
        }
    }

    /// Make it the wallet's active descriptor for its type and internal/external. Default false.
    pub fn active(mut self, v: bool) -> Self {
        self.active = Some(v);
        self
    }

    /// Range to import from a ranged descriptor, both ends inclusive.
    pub fn range(mut self, start: u32, end: u32) -> Self {
        self.range = Some((start, end));
        self
    }

    /// Next index to derive from a ranged descriptor.
    pub fn next_index(mut self, v: u32) -> Self {
        self.next_index = Some(v);
        self
    }

    /// For change, not incoming payments. Default false.
    pub fn internal(mut self, v: bool) -> Self {
        self.internal = Some(v);
        self
    }

    /// Label for the address. Not for ranged descriptors.
    pub fn label(mut self, v: &str) -> Self {
        self.label = Some(v.to_owned());
        self
    }

    pub(crate) fn to_json(&self) -> Value {
        let mut o = Map::new();
        o.insert("desc".into(), json!(self.desc));
        let timestamp = match self.timestamp {
            Timestamp::Now => json!("now"),
            Timestamp::Time(t) => json!(t),
        };
        o.insert("timestamp".into(), timestamp);
        put(&mut o, "active", self.active);
        put(&mut o, "range", self.range.map(|(a, b)| json!([a, b])));
        put(&mut o, "next_index", self.next_index);
        put(&mut o, "internal", self.internal);
        put(&mut o, "label", self.label.clone());
        Value::Object(o)
    }
}
