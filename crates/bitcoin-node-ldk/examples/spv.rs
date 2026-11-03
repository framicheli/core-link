//! LDK's `SpvClient` following a chain through `ChainBlockSource`.
//!
//! On the mock (mines, reorgs, exits):
//!     cargo run -p bitcoin-node-ldk --example spv
//! On Core over RPC (polls every second until Ctrl-C):
//!     cargo run -p bitcoin-node-ldk --example spv -- http://127.0.0.1:18443 user pass
use bitcoin::Network;
use bitcoin::block::Header;
use bitcoin_core_rpc::{Auth, Client};
use bitcoin_node_api::ChainSource;
use bitcoin_node_ldk::ChainBlockSource;
use bitcoin_node_mock::MockNode;
use lightning::chain::transaction::TransactionData;
use lightning::chain::{BestBlock, Listen};
use lightning_block_sync::poll::{ChainPoller, ChainTip};
use lightning_block_sync::{SpvClient, UnboundedCache, init};
use std::time::Duration;

/// Prints what LDK tells a `Listen`er (a `ChannelManager`, a `ChainMonitor`).
struct Printer;

impl Listen for Printer {
    fn filtered_block_connected(&self, header: &Header, txdata: &TransactionData, height: u32) {
        println!(
            "connected    {height} {} ({} txs)",
            header.block_hash(),
            txdata.len()
        );
    }

    fn blocks_disconnected(&self, fork_point: BestBlock) {
        println!(
            "disconnected down to {} {}",
            fork_point.height, fork_point.block_hash
        );
    }
}

/// Follows `chain` from its current tip; `step` runs between polls.
async fn follow<C, F>(chain: C, network: Network, mut step: F)
where
    C: ChainSource + Send + Sync,
    F: AsyncFnMut() -> bool,
{
    let source = ChainBlockSource::new(chain);
    let tip = init::validate_best_block_header(&source)
        .await
        .expect("tip");
    println!("start at     {} chainwork {:x}", tip.height, tip.chainwork);
    let mut cache = UnboundedCache::new();
    let poller = ChainPoller::new(&source, network);
    let mut spv = SpvClient::new(tip, poller, &mut cache, &Printer);
    while step().await {
        match spv.poll_best_tip().await {
            Ok((ChainTip::Worse(t), _)) => println!("ignored      {} (less work)", t.height),
            Ok(_) => {}
            Err(e) => eprintln!("poll failed ({:?}): {}", e.kind(), e.into_inner()),
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let [url, user, pass] = &args[..] {
        let rpc = Client::connect(url, Auth::UserPass(user.clone(), pass.clone()))
            .await
            .expect("connect");
        let network = rpc.get_blockchain_info().await.expect("chain").chain;
        follow(rpc, network, async || {
            tokio::time::sleep(Duration::from_secs(1)).await;
            true
        })
        .await;
        return;
    }

    let node = MockNode::regtest();
    node.mine_empty(5);
    let driver = node.clone();
    let mut script = vec![
        ("mine 3", (0, 3)),
        ("reorg: 2 off, 3 on", (2, 3)),
        ("reorg to less work: 2 off, 1 on", (2, 1)),
        ("mine 2", (0, 2)),
    ]
    .into_iter();
    follow(node, Network::Regtest, async || match script.next() {
        Some((what, (off, on))) => {
            println!("-- {what}");
            driver.reorg(off, on);
            true
        }
        None => false,
    })
    .await;
}
