//! `cargo run -p ipc-codegen`: regenerates `bitcoin-core-ipc/src/generated/vNN/`
//! from `bitcoin-core-ipc/schema/vNN/`. Needs the `capnp` compiler on `PATH`,
//! any version: it's recorded in each file's header, and CI regenerates with
//! that same version to check the committed code byte for byte.
use std::path::Path;

const VERSIONS: &[&str] = &["v30", "v31"];
const FILES: &[&str] = &[
    "common.capnp",
    "echo.capnp",
    "init.capnp",
    "mining.capnp",
    "mp/proxy.capnp",
];

fn main() {
    let ipc = Path::new(env!("CARGO_MANIFEST_DIR")).join("../bitcoin-core-ipc");
    for v in VERSIONS {
        let schema = ipc.join("schema").join(v);
        let out = ipc.join("src/generated").join(v);
        let _ = std::fs::remove_dir_all(&out);
        std::fs::create_dir_all(&out).unwrap();
        let mut cmd = capnpc::CompilerCommand::new();
        cmd.src_prefix(&schema)
            // Core imports `/mp/proxy.capnp`; `/capnp/c++.capnp` comes with `capnp`.
            .import_path(&schema)
            .default_parent_module(vec!["generated".into(), (*v).into()])
            .output_path(&out);
        for f in FILES {
            cmd.file(schema.join(f));
        }
        cmd.run().unwrap_or_else(|e| panic!("{v}: {e}"));
        println!("generated {}", out.display());
    }
}
