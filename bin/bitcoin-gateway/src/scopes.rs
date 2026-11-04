//! Scopes, and the scope every Core method needs.
//!
//! The class of a method (read, idempotent, write) is `bitcoin-core-rpc`'s
//! table ([`method_class`]); this module adds the area. A method in neither
//! table is denied.
use bitcoin_core_rpc::{MethodClass, method_class};
use std::fmt;

/// One permission a key can hold.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    /// Blockchain reads.
    ChainRead,
    /// `invalidateblock` and friends.
    ChainWrite,
    /// Mempool reads.
    MempoolRead,
    /// `sendrawtransaction`, `submitpackage`, `testmempoolaccept`.
    Broadcast,
    /// `estimatesmartfee`.
    FeesRead,
    /// Event streams.
    Events,
    /// `getblocktemplate`, `getmininginfo`.
    MiningRead,
    /// `submitblock`, `prioritisetransaction`.
    MiningWrite,
    /// Peer and network reads.
    NetworkRead,
    /// `addnode`, `setban`, `disconnectnode`.
    NetworkWrite,
    /// Pure functions: decoding, address validation, descriptors.
    UtilRead,
    /// Wallet reads on one wallet.
    WalletRead(String),
    /// Spending, signing, address and label changes on one wallet.
    WalletWrite(String),
    /// Everything, including the admin-only methods.
    Admin,
}

impl Scope {
    /// Parses `chain:read`, `wallet:hot:write`, `admin`, ...
    pub fn parse(s: &str) -> Option<Scope> {
        Some(match s {
            "chain:read" => Scope::ChainRead,
            "chain:write" => Scope::ChainWrite,
            "mempool:read" => Scope::MempoolRead,
            "broadcast" => Scope::Broadcast,
            "fees:read" => Scope::FeesRead,
            "events" => Scope::Events,
            "mining:read" => Scope::MiningRead,
            "mining:write" => Scope::MiningWrite,
            "network:read" => Scope::NetworkRead,
            "network:write" => Scope::NetworkWrite,
            "util:read" => Scope::UtilRead,
            "admin" => Scope::Admin,
            _ => {
                let rest = s.strip_prefix("wallet:")?;
                let (name, class) = rest.rsplit_once(':')?;
                // No route reaches an empty name (`/ws/wallet/` has none).
                if name.is_empty() {
                    return None;
                }
                match class {
                    "read" => Scope::WalletRead(name.to_owned()),
                    "write" => Scope::WalletWrite(name.to_owned()),
                    _ => return None,
                }
            }
        })
    }

    /// The wallet a wallet scope names.
    pub fn wallet(&self) -> Option<&str> {
        match self {
            Scope::WalletRead(w) | Scope::WalletWrite(w) => Some(w),
            _ => None,
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scope::ChainRead => f.write_str("chain:read"),
            Scope::ChainWrite => f.write_str("chain:write"),
            Scope::MempoolRead => f.write_str("mempool:read"),
            Scope::Broadcast => f.write_str("broadcast"),
            Scope::FeesRead => f.write_str("fees:read"),
            Scope::Events => f.write_str("events"),
            Scope::MiningRead => f.write_str("mining:read"),
            Scope::MiningWrite => f.write_str("mining:write"),
            Scope::NetworkRead => f.write_str("network:read"),
            Scope::NetworkWrite => f.write_str("network:write"),
            Scope::UtilRead => f.write_str("util:read"),
            Scope::WalletRead(w) => write!(f, "wallet:{w}:read"),
            Scope::WalletWrite(w) => write!(f, "wallet:{w}:write"),
            Scope::Admin => f.write_str("admin"),
        }
    }
}

/// What part of the node a method touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Area {
    /// Blocks, headers, the UTXO set.
    Chain,
    /// The mempool.
    Mempool,
    /// Submitting transactions.
    Broadcast,
    /// Fee estimates.
    Fees,
    /// Templates and block submission.
    Mining,
    /// Peers.
    Network,
    /// Pure functions of their arguments, and harmless node info.
    Util,
    /// One wallet's state or keys.
    Wallet,
    /// The node host or the process: admin only.
    Admin,
}

macro_rules! areas {
    ($($area:ident: [$($m:literal),* $(,)?])*) => {
        /// The area of `method`, or `None` for a method the gateway doesn't know.
        pub fn method_area(method: &str) -> Option<Area> {
            match method {
                $($($m)|* => Some(Area::$area),)*
                _ => None,
            }
        }

        /// Every method with an area, for tests.
        pub fn areas() -> impl Iterator<Item = (&'static str, Area)> {
            [$($(($m, Area::$area),)*)*].into_iter()
        }
    };
}

areas! {
    Chain: [
        "getbestblockhash", "getblock", "getblockchaininfo", "getblockcount", "getblockfilter",
        "getblockhash", "getblockheader", "getblockstats", "getchainstates", "getchaintips",
        "getchaintxstats", "getdeploymentinfo", "getdescriptoractivity", "getdifficulty",
        "gettxout", "gettxoutproof", "gettxoutsetinfo", "verifychain", "verifytxoutproof",
        "waitforblock", "waitforblockheight", "waitfornewblock", "getrawtransaction",
        // writes
        "invalidateblock", "reconsiderblock", "preciousblock", "pruneblockchain",
        "scanblocks", "scantxoutset",
    ]
    Mempool: [
        "getmempoolancestors", "getmempoolcluster", "getmempooldescendants", "getmempoolentry",
        "getmempoolinfo", "getrawmempool", "gettxspendingprevout", "getorphantxs",
        "getprioritisedtransactions", "getprivatebroadcastinfo",
    ]
    Broadcast: [
        "sendrawtransaction", "submitpackage", "testmempoolaccept", "abortprivatebroadcast",
    ]
    Fees: ["estimatesmartfee"]
    Mining: [
        "getblocktemplate", "getmininginfo", "getnetworkhashps",
        // writes
        "submitblock", "submitheader", "prioritisetransaction", "generateblock",
        "generatetoaddress", "generatetodescriptor",
    ]
    Network: [
        "getaddednodeinfo", "getaddrmaninfo", "getconnectioncount", "getnettotals",
        "getnetworkinfo", "getnodeaddresses", "getpeerinfo", "listbanned", "ping",
        // writes
        "addnode", "clearbanned", "getblockfrompeer", "disconnectnode", "setban",
    ]
    Util: [
        "getmemoryinfo", "getrpcinfo", "help", "uptime",
        "analyzepsbt", "combinepsbt", "combinerawtransaction", "converttopsbt", "createpsbt",
        "createrawtransaction", "decodepsbt", "decoderawtransaction", "decodescript",
        "descriptorprocesspsbt", "finalizepsbt", "joinpsbts", "signrawtransactionwithkey",
        "utxoupdatepsbt", "createmultisig", "deriveaddresses", "getdescriptorinfo",
        "getindexinfo", "signmessagewithprivkey", "validateaddress", "verifymessage", "echo",
        "echojson", "getzmqnotifications",
    ]
    Wallet: [
        "getaddressesbylabel", "getaddressinfo", "getbalance", "getbalances",
        "getreceivedbyaddress", "getreceivedbylabel", "gettransaction", "getwalletinfo",
        "listaddressgroupings", "listlabels", "listlockunspent", "listreceivedbyaddress",
        "listreceivedbylabel", "listsinceblock", "listtransactions", "listunspent",
        "simulaterawtransaction",
        // writes; `listdescriptors` and `gethdkeys` can export private keys,
        // so they need the write scope although Core counts them as reads
        "listdescriptors", "gethdkeys",
        "abandontransaction", "abortrescan", "bumpfee", "createwalletdescriptor",
        "fundrawtransaction", "getnewaddress", "getrawchangeaddress", "importdescriptors",
        "importprunedfunds", "keypoolrefill", "lockunspent", "psbtbumpfee", "removeprunedfunds",
        "rescanblockchain", "send", "sendall", "sendmany", "sendtoaddress", "setlabel",
        "setwalletflag", "signmessage", "signrawtransactionwithwallet", "walletcreatefundedpsbt",
        "walletlock", "walletpassphrase", "walletpassphrasechange", "walletprocesspsbt",
    ]
    Admin: [
        // a path on the node's filesystem
        "backupwallet", "dumptxoutset", "loadtxoutset", "importmempool", "restorewallet",
        "loadwallet", "createwallet", "migratewallet",
        // the process
        "stop", "logging", "setnetworkactive", "savemempool", "unloadwallet", "encryptwallet",
        "setmocktime",
        // run an external signer on the node host
        "enumeratesigners", "walletdisplayaddress",
        // every wallet on the node, not one a key is scoped to
        "listwallets", "listwalletdir",
    ]
}

/// Methods Core counts as reads that the gateway treats as writes.
const WRITE_ANYWAY: &[&str] = &["listdescriptors", "gethdkeys"];

/// What a call needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Need {
    /// This scope (or `admin`).
    Scope(Scope),
    /// `admin`.
    Admin,
    /// A method in no table: `admin` with `allow_unknown`.
    Unknown,
}

/// The scope `method` needs; `wallet` is the wallet the call is routed to
/// (`/ws/wallet/<name>`), `None` on the node route.
pub fn required(method: &str, wallet: Option<&str>) -> Need {
    let (Some(area), Some(class)) = (method_area(method), method_class(method)) else {
        return Need::Unknown;
    };
    let read = class == MethodClass::Read && !WRITE_ANYWAY.contains(&method);
    let pick = |r: Scope, w: Scope| Need::Scope(if read { r } else { w });
    match area {
        Area::Admin => Need::Admin,
        // On the node route Core picks the wallet (the only one loaded), so
        // the key's wallet scopes can't be checked.
        Area::Wallet => match wallet {
            None => Need::Admin,
            Some(w) => pick(Scope::WalletRead(w.into()), Scope::WalletWrite(w.into())),
        },
        Area::Chain => pick(Scope::ChainRead, Scope::ChainWrite),
        Area::Mining => pick(Scope::MiningRead, Scope::MiningWrite),
        Area::Network => pick(Scope::NetworkRead, Scope::NetworkWrite),
        Area::Broadcast => Need::Scope(Scope::Broadcast),
        Area::Mempool if read => Need::Scope(Scope::MempoolRead),
        Area::Fees if read => Need::Scope(Scope::FeesRead),
        Area::Util if read => Need::Scope(Scope::UtilRead),
        Area::Mempool | Area::Fees | Area::Util => Need::Admin,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin_core_rpc::classified_methods;
    use std::collections::HashSet;

    #[test]
    fn every_typed_rpc_has_an_area() {
        let missing: Vec<_> = classified_methods()
            .filter(|(m, _)| method_area(m).is_none())
            .map(|(m, _)| m)
            .collect();
        assert!(missing.is_empty(), "no area for {missing:?}");
    }

    #[test]
    fn every_area_entry_is_a_typed_rpc_once() {
        let mut seen = HashSet::new();
        for (m, _) in areas() {
            assert!(method_class(m).is_some(), "{m} is not in method_class");
            assert!(seen.insert(m), "{m} twice");
        }
    }

    #[test]
    fn read_only_areas_hold_only_reads() {
        for (m, area) in areas() {
            if matches!(area, Area::Mempool | Area::Fees | Area::Util) {
                assert_eq!(method_class(m), Some(MethodClass::Read), "{m}");
            }
        }
    }

    #[test]
    fn scopes_round_trip() {
        for s in [
            "chain:read",
            "chain:write",
            "mempool:read",
            "broadcast",
            "fees:read",
            "events",
            "mining:read",
            "mining:write",
            "network:read",
            "network:write",
            "util:read",
            "wallet:hot:read",
            "wallet:a:b:write",
            "admin",
        ] {
            assert_eq!(Scope::parse(s).unwrap().to_string(), s);
        }
        for s in [
            "chain",
            "chain:admin",
            "wallet:hot",
            "wallet:hot:spend",
            // Core's unnamed wallet: no `/ws/wallet/<name>` route reaches it.
            "wallet::read",
            "Admin",
        ] {
            assert_eq!(Scope::parse(s), None, "{s}");
        }
    }

    #[test]
    fn needs() {
        assert_eq!(required("getblock", None), Need::Scope(Scope::ChainRead));
        assert_eq!(
            required("invalidateblock", None),
            Need::Scope(Scope::ChainWrite)
        );
        assert_eq!(
            required("testmempoolaccept", None),
            Need::Scope(Scope::Broadcast)
        );
        assert_eq!(
            required("submitblock", None),
            Need::Scope(Scope::MiningWrite)
        );
        assert_eq!(
            required("getbalance", Some("hot")),
            Need::Scope(Scope::WalletRead("hot".into()))
        );
        assert_eq!(
            required("sendtoaddress", Some("hot")),
            Need::Scope(Scope::WalletWrite("hot".into()))
        );
        assert_eq!(
            required("listdescriptors", Some("hot")),
            Need::Scope(Scope::WalletWrite("hot".into()))
        );
        assert_eq!(required("getbalance", None), Need::Admin);
        assert_eq!(required("stop", None), Need::Admin);
        assert_eq!(required("backupwallet", Some("hot")), Need::Admin);
        assert_eq!(required("dumpprivkey", Some("hot")), Need::Unknown);
        assert_eq!(required("getfuturething", None), Need::Unknown);
    }
}
