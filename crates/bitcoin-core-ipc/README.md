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

Core 30 and 31's multiprocess mining interface
over a Unix socket, implementing `TemplateSource` and `ChainSource::tip`.

Start Core's multiprocess node with a socket:

```sh
bitcoin -m node -ipcbind=unix            # or libexec/bitcoin-node -ipcbind=unix
# listens on <datadir>/<chain>/node.sock
```

## Two protocols

Core 31 renumbered methods (`makeMining` @2→@3, `submitSolution` @9→@7),
added a context argument to `createNewBlock` and `checkBlock`, and turned
`getCoinbaseTx` from a placeholder transaction into a `CoinbaseTx` struct.
So each version has its own generated code, and `version.rs` shares the
common parts with a macro. `IpcClient::connect` asks for Core 31's
`makeMining` first and falls back to Core 30's on `Unimplemented`;
`core_major()` says which one is in use.

## Schemas and generated code

`schema/v30/` and `schema/v31/` hold verbatim copies of Core's
`src/ipc/capnp/*.capnp` and libmultiprocess' `mp/proxy.capnp` at the
matching tag. The generated Rust in `src/generated/` is **committed**, so
building the crate needs no `capnp` compiler. It is regenerated with
`cargo run -p ipc-codegen`, and CI checks the committed code matches byte
for byte (see [§13](https://github.com/framicheli/core-link#13-continuous-integration)). `bitcoin-capnp-types` was
not used because it runs `capnp` in every build.

## The worker thread

`capnp-rpc` types are not `Send`. So one dedicated OS thread with a tokio
`LocalSet` owns the connection; `IpcClient` is a cheap `Send + Sync` handle
that sends boxed jobs to it over an unbounded channel, each run with
`spawn_local`, so calls run concurrently on the worker. Templates stay on
the worker, keyed by a `u64` id. The `TemplateToken` holds that id and sends
`Release(id)` when its last clone drops, so Core can free the template. A
token from a different `IpcClient` is refused.

## Core threads

Every IPC call names a Core-side thread in its context. Core 30 refuses a
call on a thread that is already running one ("thread busy"), so each call
leases an idle thread from a pool and creates a new one when all are busy.
This is also what keeps a long `waitNext` from blocking a `submitSolution`.

## Mapping to `TemplateSource`

- `template()`: `createNewBlock`, with `getBlockHeader`, `getCoinbaseTx`
  (plus `getCoinbaseCommitment` on Core 30) and `getCoinbaseMerklePath`
  **pipelined** on its promised result: one round trip. Height comes from
  the coinbase's BIP 34 push; `min_time` is the header's time (IPC sends no
  minimum). Core 30's placeholder coinbase is reshaped into `Coinbase`.
- `transactions()`: `getBlock` + `getTxFees` + `getTxSigops`.
- `wait_next()`: `waitNext` with both options always set (Core 30's schema
  defaults them to 0); a null result is `None`.
- `submit_solution()`: `submitSolution` with the complete coinbase; `false`
  is a rejection, "already known" is `Ok`.
- `ChainSource::tip` uses `getTip`; the other chain methods return
  `Unsupported`.
- `EventSource`: `waitTipChanged` in a loop (10 s per call), tip only. One
  block higher is `BlockConnected`; any other tip change is `Lagged` (IPC
  gives no parent hash, so resync from `tip()`). A transport error ends the
  stream.

Measured on empty regtest: warm `template()` is ~1.0–1.2 ms over IPC vs
~0.9 ms over RPC. IPC's advantage is in `wait_next` reacting immediately to
new tips and fee changes, and in not paying for transactions you don't fetch.

`bitcoin_core_sv2` was read but not wrapped: its API is Stratum v2 messages
and it pulls in `stratum-core`, tokio "full" and `tracing-subscriber`.

License: Apache-2.0.
