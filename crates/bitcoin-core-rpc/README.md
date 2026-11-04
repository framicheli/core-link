# bitcoin-core-rpc

A complete, typed, async JSON-RPC client for Bitcoin Core 29–31 (the
versions Core maintains), and the
JSON-RPC backend for the [`bitcoin-node-api`](https://crates.io/crates/bitcoin-node-api)
traits.

- One method per public RPC (`getblockstats` → `get_block_stats`), with
  `bitcoin::*` types in and out, and a `Wallet` handle for wallet RPCs.
- Core version detected on connect; replies parsed for that version.
- Cookie auth that survives a node restart, configurable timeouts and HTTP
  client, opt-in retries that never resend a payment, and JSON-RPC batching.
- `raw_call` for anything newer than this crate.

```rust
use bitcoin_core_rpc::{Auth, Client, RetryPolicy};

let client = Client::builder("http://127.0.0.1:8332")
    .auth(Auth::cookie("/home/me/.bitcoin/.cookie")?)
    .retry(RetryPolicy::default())
    .connect()
    .await?;
let info = client.get_blockchain_info().await?;
let hashes = client.get_block_hashes(0..=99).await?; // one batched request
```

Retries follow `method_class(name)`: reads and idempotent submits
(`sendrawtransaction`, `submitblock`) are retried after a timeout; writes
like `sendtoaddress` only when the request never left.

Feature `tracing`: a span per request (method, id, wallet, duration, error
kind), never params or results.

More: the [repository README](https://github.com/framicheli/core-link).

License: MIT.
