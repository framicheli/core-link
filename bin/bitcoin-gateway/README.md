# bitcoin-gateway

gRPC, REST and WebSocket in front of Bitcoin Core, behind per-key
permissions, built on the `bitcoin-node-api` traits and the
`bitcoin-core-rpc` / `bitcoin-core-zmq` backends.

```sh
bitcoin-gateway hash-token                   # a token and its hash for a [[key]]
bitcoin-gateway -c gateway.toml check-config
bitcoin-gateway -c gateway.toml
```

Every setting is in [`gateway.example.toml`](gateway.example.toml). The
design is in [`PHASE_2.md`](../../PHASE_2.md); usage, the scopes, every
API and the reasons behind each choice are in the workspace README's
gateway section.
