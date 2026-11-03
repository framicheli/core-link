# Fuzzing the gateway

libFuzzer targets for everything the gateway parses from clients and
operators. They need nightly Rust and `cargo-fuzz`:

```sh
rustup toolchain install nightly --profile minimal
cargo install cargo-fuzz --locked
cd bin/bitcoin-gateway
cargo +nightly fuzz run rest -- -max_total_time=300
```

| Target     | Input                                                    | Checks                                                       |
| ---------- | -------------------------------------------------------- | ------------------------------------------------------------ |
| `config`   | any text as `gateway.toml`                               | no panic; a config that parses has keys that check           |
| `scopes`   | scope strings, method names, wallet names                | no panic; scopes round-trip through `Display`                |
| `ws_frame` | any WebSocket text message, any key, node or wallet route | no panic; every reply is JSON-RPC 2.0, batches non-empty     |
| `rest`     | method, path, query, token, headers and body for REST    | no panic; never a 500 (the mock never fails, so it'd be ours) |

`ws_frame` and `rest` run the real handlers on `bitcoin-node-mock`, with a
stub Core for the pass-through. Corpora and crashes land in `corpus/` and
`artifacts/` (not committed). To reproduce a crash:

```sh
cargo +nightly fuzz run <target> artifacts/<target>/crash-<hash>
```

CI runs each target for 30 seconds on every pull request (the `fuzz` job).
