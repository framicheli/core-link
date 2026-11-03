//! `== Signer ==`
use crate::{Client, core};
use bitcoin_node_api::ApiResult;
use serde_json::json;

impl Client {
    /// External signers (`-signer`), e.g. HWI.
    pub async fn enumerate_signers(&self) -> ApiResult<core::EnumerateSigners> {
        self.call("enumeratesigners", json!([])).await
    }
}
