//! Follows the chain and mempool and prints every change.
//!
//! ```text
//! chain-follower --rpc http://127.0.0.1:18443 --user u --pass p
//!     [--zmq tcp://127.0.0.1:28333] [--start HEIGHT]
//! ```
//!
//! Events over ZMQ (`-zmqpubsequence`) if `--zmq` is given, else RPC polling
//! (blocks only: the mempool is then only re-read on `Lagged`).
use bitcoin_core_rpc::{Auth, Client};
use bitcoin_core_zmq::{Topic, ZmqEvents};
use bitcoin_node_api::{ChainSource, EventSource};
use consumers::follower::Follower;
use consumers::next_event;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: HashMap<_, _> = args
        .chunks(2)
        .map(|kv| (kv[0].clone(), kv.get(1).cloned().unwrap_or_default()))
        .collect();
    let rpc = Client::connect(
        &args["--rpc"],
        Auth::UserPass(args["--user"].clone(), args["--pass"].clone()),
    )
    .await
    .expect("RPC");
    let start = match args.get("--start") {
        Some(h) => h.parse().expect("--start HEIGHT"),
        None => rpc.tip().await.expect("tip").height.saturating_sub(10),
    };
    match args.get("--zmq") {
        Some(ep) => {
            let zmq = ZmqEvents::connect(ep, Topic::Sequence).await.expect("ZMQ");
            let mut events = zmq.subscribe().await.expect("subscribe");
            let mut f = Follower::start(rpc, start).await.expect("sync");
            println!("synced to {:?}", f.tip());
            while let Some(ev) = next_event(&mut events).await {
                println!("{:?}", f.apply(ev.expect("event")).await.expect("apply"));
            }
        }
        None => {
            let mut events = rpc.subscribe().await.expect("subscribe");
            let mut f = Follower::start(rpc, start).await.expect("sync");
            println!("synced to {:?}", f.tip());
            while let Some(ev) = next_event(&mut events).await {
                println!("{:?}", f.apply(ev.expect("event")).await.expect("apply"));
            }
        }
    }
}
