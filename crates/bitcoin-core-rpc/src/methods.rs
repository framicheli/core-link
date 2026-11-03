//! Every RPC this crate types, classed by what repeating it does. Used by
//! [`RetryPolicy`] and meant for anything that authorizes calls by method
//! (a gateway's scopes): a method missing from the table is `None`, and
//! callers should treat it as the most dangerous class.
use bitcoin_node_api::{NodeAPIError, RpcErrorCode, TransportError};
use std::time::Duration;

/// What sending a method twice does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MethodClass {
    /// Changes nothing: safe to repeat.
    Read,
    /// Changes node state, but a repeat is answered as "already done"
    /// (`sendrawtransaction`, `submitblock`, ...): safe to repeat.
    Idempotent,
    /// Changes node or wallet state, or uses a wallet's keys; a repeat may
    /// do it twice (`sendtoaddress` pays twice).
    Write,
}

macro_rules! classes {
    ($($class:ident: [$($m:literal),* $(,)?])*) => {
        /// The class of `method`, or `None` for a method this crate doesn't
        /// type (e.g. a `raw_call` to a newer RPC).
        pub fn method_class(method: &str) -> Option<MethodClass> {
            match method {
                $($($m)|* => Some(MethodClass::$class),)*
                _ => None,
            }
        }

        /// Every method in the table, for tests and audits.
        pub fn classified_methods() -> impl Iterator<Item = (&'static str, MethodClass)> {
            [$($(($m, MethodClass::$class),)*)*].into_iter()
        }
    };
}

classes! {
    Read: [
        // blockchain
        "getbestblockhash", "getblock", "getblockchaininfo", "getblockcount", "getblockfilter",
        "getblockhash", "getblockheader", "getblockstats", "getchainstates", "getchaintips",
        "getchaintxstats", "getdeploymentinfo", "getdescriptoractivity", "getdifficulty",
        "getmempoolancestors", "getmempoolcluster", "getmempooldescendants", "getmempoolentry",
        "getmempoolinfo", "getrawmempool", "gettxout", "gettxoutproof", "gettxoutsetinfo",
        "gettxspendingprevout", "verifychain", "verifytxoutproof", "waitforblock",
        "waitforblockheight", "waitfornewblock", "getorphantxs", "getprioritisedtransactions",
        "getprivatebroadcastinfo",
        // control
        "getmemoryinfo", "getrpcinfo", "help", "uptime",
        // mining
        "getblocktemplate", "getmininginfo", "getnetworkhashps",
        // network
        "getaddednodeinfo", "getaddrmaninfo", "getconnectioncount", "getnettotals",
        "getnetworkinfo", "getnodeaddresses", "getpeerinfo", "listbanned", "ping",
        // raw transactions: pure functions of their arguments
        "analyzepsbt", "combinepsbt", "combinerawtransaction", "converttopsbt", "createpsbt",
        "createrawtransaction", "decodepsbt", "decoderawtransaction", "decodescript",
        "descriptorprocesspsbt", "finalizepsbt", "getrawtransaction", "joinpsbts",
        "signrawtransactionwithkey", "testmempoolaccept", "utxoupdatepsbt",
        // util
        "createmultisig", "deriveaddresses", "estimatesmartfee", "getdescriptorinfo",
        "getindexinfo", "signmessagewithprivkey", "validateaddress", "verifymessage",
        "echo", "echojson", "getzmqnotifications", "enumeratesigners",
        // wallet: reads only
        "getaddressesbylabel", "getaddressinfo", "getbalance", "getbalances", "gethdkeys",
        "getreceivedbyaddress", "getreceivedbylabel", "gettransaction", "getwalletinfo",
        "listaddressgroupings", "listdescriptors", "listlabels", "listlockunspent",
        "listreceivedbyaddress", "listreceivedbylabel", "listsinceblock", "listtransactions",
        "listunspent", "listwalletdir", "listwallets", "simulaterawtransaction",
    ]
    Idempotent: [
        "sendrawtransaction", "submitblock", "submitheader", "submitpackage",
    ]
    Write: [
        // node state
        "abortprivatebroadcast", "addnode", "clearbanned", "getblockfrompeer", "disconnectnode", "setban",
        "setnetworkactive", "prioritisetransaction", "invalidateblock", "reconsiderblock",
        "preciousblock", "pruneblockchain", "savemempool", "importmempool", "dumptxoutset",
        "loadtxoutset", "logging", "stop", "setmocktime", "generateblock", "generatetoaddress",
        "generatetodescriptor",
        // scans hold one slot and take start/abort actions
        "scanblocks", "scantxoutset",
        // wallet: changes it, or signs with its keys
        "abandontransaction", "abortrescan", "backupwallet", "bumpfee", "createwallet",
        "createwalletdescriptor", "encryptwallet", "fundrawtransaction", "getnewaddress",
        "getrawchangeaddress", "importdescriptors", "importprunedfunds", "keypoolrefill",
        "loadwallet", "lockunspent", "migratewallet", "psbtbumpfee", "removeprunedfunds",
        "rescanblockchain", "restorewallet", "send", "sendall", "sendmany", "sendtoaddress",
        "setlabel", "setwalletflag", "signmessage", "signrawtransactionwithwallet",
        "unloadwallet", "walletcreatefundedpsbt", "walletdisplayaddress", "walletlock",
        "walletpassphrase", "walletpassphrasechange", "walletprocesspsbt",
    ]
}

/// Opt-in retries (`Client::builder(..).retry(..)`), safe by method class:
///
/// | Error                               | Read, Idempotent | Write |
/// | ----------------------------------- | :--------------: | :---: |
/// | `Connect` (request never left)      |        ✓         |   ✓   |
/// | `Rpc { InWarmup }` (not run)        |        ✓         |   ✓   |
/// | `Timeout`, `Other` (may have run)   |        ✓         |   ✗   |
/// | anything else                       |        ✗         |   ✗   |
///
/// A method missing from [`method_class`] is a `Write`. Template longpolls
/// (`TemplateSource::wait_next`) are never retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Requests in all, the first included. 1 means no retry.
    pub max_attempts: u32,
    /// Wait before the first retry; doubles each time.
    pub initial_backoff: Duration,
    /// Cap on one wait.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    /// 3 attempts, waiting 100 ms then 200 ms (doubling, capped at 2 s).
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}

impl RetryPolicy {
    /// Whether `err` from a `class` method is worth sending again.
    pub fn should_retry(class: Option<MethodClass>, err: &NodeAPIError) -> bool {
        let repeatable = matches!(class, Some(MethodClass::Read | MethodClass::Idempotent));
        match err {
            NodeAPIError::Transport(TransportError::Connect(_)) => true,
            NodeAPIError::Rpc {
                code: RpcErrorCode::InWarmup,
                ..
            } => true,
            NodeAPIError::Transport(TransportError::Timeout | TransportError::Other(_)) => {
                repeatable
            }
            _ => false,
        }
    }

    /// The wait before retry number `retry` (0-based).
    pub(crate) fn backoff(&self, retry: u32) -> Duration {
        self.initial_backoff
            .saturating_mul(2u32.saturating_pow(retry))
            .min(self.max_backoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_method_is_in_two_classes() {
        let mut seen = std::collections::HashSet::new();
        for (m, _) in classified_methods() {
            assert!(seen.insert(m), "{m} twice");
        }
    }

    #[test]
    fn backoff_doubles_to_the_cap() {
        let p = RetryPolicy::default();
        let ms: Vec<_> = (0..7).map(|i| p.backoff(i).as_millis()).collect();
        assert_eq!(ms, [100, 200, 400, 800, 1600, 2000, 2000]);
    }
}
