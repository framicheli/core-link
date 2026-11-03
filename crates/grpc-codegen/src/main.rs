//! Regenerates `bin/bitcoin-gateway/src/grpc/generated/` from
//! `bin/bitcoin-gateway/proto/`. Run from anywhere in the workspace:
//!
//! ```sh
//! cargo run -p grpc-codegen
//! ```
//!
//! The output is committed, so building the gateway needs no `protoc`; CI
//! runs this and fails on any diff. `protoc` comes from
//! `protoc-bin-vendored`, so the output depends only on `Cargo.lock`.
use std::path::Path;

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../bin/bitcoin-gateway")
        .canonicalize()
        .expect("bin/bitcoin-gateway");
    let proto_dir = root.join("proto");
    let out = root.join("src/grpc/generated");
    std::fs::create_dir_all(&out).expect("create the output directory");
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    // SAFETY: single-threaded; nothing else reads the environment yet.
    unsafe { std::env::set_var("PROTOC", protoc) };
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(&out)
        .file_descriptor_set_path(out.join("descriptor.bin"))
        .compile_protos(
            &[proto_dir.join("bitcoin/node/v1/node.proto")],
            &[proto_dir],
        )
        .expect("compile the proto");
    println!("wrote {}", out.display());
}
