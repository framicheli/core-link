//! Prints new work as it arrives, and with `--mine N` mines N blocks (regtest).
//!
//! ```text
//! template-feed --rpc http://127.0.0.1:18443 --user u --pass p
//!     [--ipc <datadir>/regtest/node.sock] [--zmq tcp://127.0.0.1:28333]
//!     [--mine N --payout <address>]
//! ```
//!
//! Templates come over IPC if `--ipc` is given, else RPC; block events over
//! ZMQ (`-zmqpubsequence`) if `--zmq` is given, else RPC polling.
use bitcoin::{Address, Amount};
use bitcoin_core_ipc::IpcClient;
use bitcoin_core_rpc::{Auth, Client};
use bitcoin_core_zmq::{Topic, ZmqEvents};
use bitcoin_node_api::{ChainSource, EventSource, TemplateSource};
use consumers::feed::{self, NewWork};
use futures_core::Stream;
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let args = flags();
    let rpc = Client::connect(
        &args["--rpc"],
        Auth::UserPass(args["--user"].clone(), args["--pass"].clone()),
    )
    .await
    .expect("RPC");
    let payout = args.get("--payout").map(|a| {
        a.parse::<Address<_>>()
            .expect("--payout address")
            .assume_checked()
            .script_pubkey()
    });
    let blocks: u32 = args
        .get("--mine")
        .map_or(0, |n| n.parse().expect("--mine N"));
    match args.get("--ipc") {
        Some(sock) => {
            let ipc = IpcClient::connect(sock).await.expect("IPC");
            events(&rpc, &args, |e| run(&ipc, e, payout.as_deref(), blocks)).await
        }
        None => events(&rpc, &args, |e| run(&rpc, e, payout.as_deref(), blocks)).await,
    }
}

/// Hands `run` the event stream the flags ask for.
async fn events<F, Fut>(rpc: &Client, args: &HashMap<String, String>, run: F)
where
    F: FnOnce(
        Box<
            dyn Stream<Item = bitcoin_node_api::ApiResult<bitcoin_node_api::NodeEvent>>
                + Send
                + Unpin,
        >,
    ) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    match args.get("--zmq") {
        Some(ep) => {
            let zmq = ZmqEvents::connect(ep, Topic::Sequence).await.expect("ZMQ");
            run(Box::new(zmq.subscribe().await.expect("subscribe"))).await
        }
        None => run(Box::new(rpc.subscribe().await.expect("subscribe"))).await,
    }
}

async fn run<S: TemplateSource + ChainSource>(
    src: &S,
    mut events: impl Stream<Item = bitcoin_node_api::ApiResult<bitcoin_node_api::NodeEvent>> + Unpin,
    payout: Option<&bitcoin::Script>,
    mut blocks: u32,
) {
    let mut t = src.template().await.expect("template");
    loop {
        println!(
            "template height {} on {}: {} to claim",
            t.height, t.prev_hash, t.coinbase.reward_remaining
        );
        if let (Some(payout), true) = (payout, blocks > 0) {
            let hash = feed::mine(src, &t, payout).await.expect("submit");
            println!("mined {hash}");
            blocks -= 1;
            if blocks == 0 {
                return;
            }
            t = src.template().await.expect("template");
            continue;
        }
        let work = feed::next_work(
            src,
            &t,
            &mut events,
            Some(Amount::from_sat(1)),
            Duration::from_secs(60),
        )
        .await
        .expect("new work");
        t = match work {
            NewWork::Template(next) => next,
            NewWork::Block(h) => {
                println!("block {h}");
                src.template().await.expect("template")
            }
            NewWork::Lagged | NewWork::Timeout => src.template().await.expect("template"),
        };
    }
}

fn flags() -> HashMap<String, String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    args.chunks(2)
        .map(|kv| (kv[0].clone(), kv.get(1).cloned().unwrap_or_default()))
        .collect()
}
