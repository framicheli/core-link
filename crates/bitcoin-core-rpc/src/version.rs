/// `getnetworkinfo.version`, e.g. 310100.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(pub u32);

impl Version {
    /// Major release: 310100 -> 31.
    pub fn major(self) -> u32 {
        self.0 / 10_000
    }

    /// Whether this version has `f`.
    pub fn supports(self, f: Feature) -> bool {
        match f {
            Feature::SubmitPackage => self.major() >= 28,
            Feature::ClusterMempool => self.major() >= 31,
            Feature::PrivateBroadcast => self.major() >= 31,
            Feature::TxVersionArg => self.major() >= 30,
        }
    }
}

/// RPCs and behavior that depend on the Core version; see `Version::supports`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Feature {
    /// `submitpackage`. Core 28+.
    SubmitPackage,
    /// Cluster mempool RPCs, e.g. `getmempoolcluster`. Core 31+.
    ClusterMempool,
    /// `getprivatebroadcastinfo` and `abortprivatebroadcast`. Core 31+.
    PrivateBroadcast,
    /// `send`, `sendall` and `walletcreatefundedpsbt` take a tx `version`.
    TxVersionArg,
}
