# bitcoin-core-ipc

Bitcoin Core 30+'s mining interface over IPC (Cap'n Proto) as a
[`bitcoin-node-api`](https://crates.io/crates/bitcoin-node-api)
`TemplateSource`, for miners and pools that want new templates the moment
the tip or fees change, without polling `getblocktemplate`.

```rust
use bitcoin_core_ipc::IpcClient;
use bitcoin_node_api::TemplateSource;

// bitcoin-node -ipcbind=unix   (listens on <datadir>/<chain>/node.sock)
let ipc = IpcClient::connect("/home/me/.bitcoin/node.sock").await?;
let template = ipc.template().await?;
```

Speaks the Core 30 and Core 31 protocols (picked on connect). The
generated Cap'n Proto code is committed: no `capnp` binary needed to build.
Feature `tracing`: connect, protocol picked, Core threads created.

More: the [repository README](https://github.com/framicheli/bitcoin_rpc).

License: MIT.
