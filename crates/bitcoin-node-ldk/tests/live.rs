//! lightning-block-sync's sync flow on a real node, over RPC. `#[ignore]`:
//! `cargo test -- --ignored`, with `bitcoind` on `PATH` or `BITCOIND_EXE`.
use bitcoin::block::Header;
use bitcoin::{Address, BlockHash, Network, ScriptBuf, Work};
use bitcoin_core_rpc::{Auth, Client};
use bitcoin_node_api::ChainSource;
use bitcoin_node_ldk::ChainBlockSource;
use lightning::chain::transaction::TransactionData;
use lightning::chain::{BestBlock, Listen};
use lightning_block_sync::poll::ChainPoller;
use lightning_block_sync::{BlockSource, SpvClient, UnboundedCache, init};
use std::sync::Mutex;
use std::time::Duration;
use test_node::{RPC_PASSWORD, RPC_USER, TestNode};

/// Keeps the chain it was told about; checks each call against it.
struct Recorder {
    chain: Mutex<Vec<BlockHash>>,
    disconnects: Mutex<Vec<BestBlock>>,
}

impl Recorder {
    async fn at(rpc: &Client, height: u64) -> Recorder {
        let mut chain = Vec::new();
        for h in 0..=height {
            chain.push(rpc.block_hash(h).await.unwrap());
        }
        Recorder {
            chain: Mutex::new(chain),
            disconnects: Mutex::new(Vec::new()),
        }
    }

    fn tip(&self) -> BlockHash {
        *self.chain.lock().unwrap().last().unwrap()
    }

    async fn assert_synced(&self, rpc: &Client) {
        let chain = self.chain.lock().unwrap().clone();
        assert_eq!(chain.len() as u64, rpc.tip().await.unwrap().height + 1);
        for (h, hash) in chain.iter().enumerate() {
            assert_eq!(*hash, rpc.block_hash(h as u64).await.unwrap(), "height {h}");
        }
    }
}

impl Listen for Recorder {
    fn filtered_block_connected(&self, header: &Header, _: &TransactionData, height: u32) {
        let mut chain = self.chain.lock().unwrap();
        assert_eq!(chain.len() as u32, height, "connects in order");
        assert_eq!(
            Some(&header.prev_blockhash),
            chain.last(),
            "connects on the tip"
        );
        chain.push(header.block_hash());
    }

    fn blocks_disconnected(&self, fork_point: BestBlock) {
        let mut chain = self.chain.lock().unwrap();
        assert_eq!(
            chain.get(fork_point.height as usize),
            Some(&fork_point.block_hash)
        );
        chain.truncate(fork_point.height as usize + 1);
        self.disconnects.lock().unwrap().push(fork_point);
    }
}

fn address(tag: u8) -> Address {
    Address::p2wsh(&ScriptBuf::from(vec![tag]), Network::Regtest)
}

/// Invalidates the top `depth` blocks and mines `depth + 1` others.
async fn reorg(rpc: &Client, depth: u64, tag: u8) {
    let tip = rpc.tip().await.unwrap();
    let first = rpc.block_hash(tip.height - depth + 1).await.unwrap();
    rpc.invalidate_block(first).await.unwrap();
    rpc.generate_to_address(depth as u32 + 1, &address(tag))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "needs bitcoind"]
async fn sync_on_core() {
    let _node = TestNode::start(&[]);
    let auth = Auth::UserPass(RPC_USER.into(), RPC_PASSWORD.into());
    let mut rpc = None;
    for _ in 0..100 {
        match Client::connect(&_node.rpc_url(), auth.clone()).await {
            Ok(c) => {
                rpc = Some(c);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let rpc = rpc.expect("node came up");
    eprintln!("Core {}", rpc.version().0);
    // Past a retarget period boundary.
    rpc.generate_to_address(2020, &address(0)).await.unwrap();

    // Height and chainwork come from Core; check them against the sum of
    // every header's own work, around the retarget boundary and elsewhere.
    let source = ChainBlockSource::new(rpc.clone());
    let mut total = Work::from_be_bytes([0; 32]);
    for h in 0..=2020u64 {
        let hash = rpc.block_hash(h).await.unwrap();
        total = total + rpc.header(hash).await.unwrap().work();
        if [0, 1, 16, 17, 1000, 2014, 2015, 2016, 2017, 2020].contains(&h) {
            for hint in [Some(h as u32), None] {
                let d = source.get_header(&hash, hint).await.unwrap();
                assert_eq!(d.height as u64, h, "height {h}");
                assert_eq!(d.chainwork, total, "chainwork at {h}");
            }
        }
    }

    // `synchronize_listeners`: one behind, one current, one on a block a
    // reorg makes stale.
    let behind = Recorder::at(&rpc, 2000).await;
    let stale = Recorder::at(&rpc, 2020).await;
    reorg(&rpc, 2, 1).await;
    let current = Recorder::at(&rpc, 2021).await;
    rpc.generate_to_address(3, &address(0)).await.unwrap();
    let mut cache = UnboundedCache::new();
    let listeners: Vec<(BlockHash, &dyn Listen)> = vec![
        (behind.tip(), &behind),
        (stale.tip(), &stale),
        (current.tip(), &current),
    ];
    let tip = init::synchronize_listeners(&source, Network::Regtest, &mut cache, listeners)
        .await
        .unwrap();
    assert_eq!(tip.height, 2024);
    let core_tip = rpc
        .get_block_header_verbose(rpc.tip().await.unwrap().hash)
        .await
        .unwrap();
    assert_eq!(tip.chainwork, core_tip.chain_work);
    for l in [&behind, &stale, &current] {
        l.assert_synced(&rpc).await;
    }
    assert_eq!(
        stale.disconnects.lock().unwrap()[0].height,
        2018,
        "fork point"
    );

    // `SpvClient` + `ChainPoller`: new blocks, then a reorg.
    let poller = ChainPoller::new(&source, Network::Regtest);
    let mut spv = SpvClient::new(tip, poller, &mut cache, &current);
    rpc.generate_to_address(2, &address(0)).await.unwrap();
    let (_, changed) = spv.poll_best_tip().await.unwrap();
    assert!(changed);
    current.assert_synced(&rpc).await;

    reorg(&rpc, 3, 2).await;
    let (_, changed) = spv.poll_best_tip().await.unwrap();
    assert!(changed);
    current.assert_synced(&rpc).await;
    assert_eq!(
        current.disconnects.lock().unwrap().last().unwrap().height,
        2023
    );
}
