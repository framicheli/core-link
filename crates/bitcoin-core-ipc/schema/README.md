Verbatim copies of Bitcoin Core's IPC schemas (MIT), one directory per
supported major:

- `vNN/*.capnp` from `src/ipc/capnp/` at tag `vNN.x`
- `vNN/mp/proxy.capnp` from `src/ipc/libmultiprocess/include/mp/` at the same tag

The Rust in `../src/generated/` is generated from them by `cargo run -p ipc-codegen` (needs `capnp`).
