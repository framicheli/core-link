//! lightning-block-sync's own sync flow on the mock: `synchronize_listeners`,
//! then `SpvClient` + `ChainPoller`, through reorgs.
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::pow::Work;
use bitcoin::{BlockHash, Network};
use bitcoin_node_api::{ChainSource, NodeAPIError, RpcErrorCode, TransportError};
use bitcoin_node_ldk::ChainBlockSource;
use bitcoin_node_mock::MockNode;
use lightning::chain::transaction::TransactionData;
use lightning::chain::{BestBlock, Listen};
use lightning_block_sync::poll::ChainPoller;
use lightning_block_sync::{BlockSource, BlockSourceErrorKind, SpvClient, UnboundedCache, init};
use std::sync::Mutex;

/// A `Listen` that keeps the chain it was told about and checks each call
/// against it: connects extend the tip, disconnects land on a block it has.
struct Recorder {
    chain: Mutex<Vec<BlockHash>>,
    disconnects: Mutex<Vec<BestBlock>>,
}

impl Recorder {
    /// A listener whose chain is the node's, up to `height`.
    fn at(node: &MockNode, height: u64) -> Recorder {
        Recorder {
            chain: Mutex::new(
                (0..=height)
                    .map(|h| node.block_at(h).unwrap().block_hash())
                    .collect(),
            ),
            disconnects: Mutex::new(Vec::new()),
        }
    }

    fn tip(&self) -> BlockHash {
        *self.chain.lock().unwrap().last().unwrap()
    }

    /// Its chain is the node's active chain, block for block.
    async fn assert_synced(&self, node: &MockNode) {
        let chain = self.chain.lock().unwrap().clone();
        assert_eq!(chain.len() as u64, node.tip().await.unwrap().height + 1);
        for (h, hash) in chain.iter().enumerate() {
            assert_eq!(
                *hash,
                node.block_hash(h as u64).await.unwrap(),
                "height {h}"
            );
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

fn regtest_work(blocks: u32) -> Work {
    // Regtest bits 0x207fffff: 2 per block.
    let mut b = [0; 32];
    b[28..].copy_from_slice(&(2 * blocks).to_be_bytes());
    Work::from_be_bytes(b)
}

#[tokio::test]
async fn header_height_and_chainwork() {
    let node = MockNode::regtest();
    node.mine_empty(2100); // crosses a retarget period boundary
    let source = ChainBlockSource::new(node.clone());
    let tip = init::validate_best_block_header(&source).await.unwrap();
    assert_eq!(tip.height, 2100);
    assert_eq!(tip.chainwork, regtest_work(2101));

    // The node's height and chainwork, with a hint, without one, or with a
    // wrong one: LDK's hint isn't needed.
    for h in [0u64, 1, 16, 17, 2015, 2016, 2099] {
        let hash = node.block_hash(h).await.unwrap();
        for hint in [Some(h as u32), None, Some(h as u32 + 1)] {
            let d = source.get_header(&hash, hint).await.unwrap();
            assert_eq!(d.height as u64, h);
            assert_eq!(d.chainwork, regtest_work(h as u32 + 1));
        }
    }
}

#[tokio::test]
async fn synchronize_listeners_then_spv_client() {
    let node = MockNode::regtest();
    node.mine_empty(10);

    // Three listeners: one at genesis, one current, one on a block that a
    // reorg later makes stale.
    let behind = Recorder::at(&node, 0);
    let stale = Recorder::at(&node, 10);
    node.reorg(2, 3);
    let current = Recorder::at(&node, 11);
    node.mine_empty(4);

    let source = ChainBlockSource::new(node.clone());
    let mut cache = UnboundedCache::new();
    let listeners: Vec<(BlockHash, &dyn Listen)> = vec![
        (behind.tip(), &behind),
        (stale.tip(), &stale),
        (current.tip(), &current),
    ];
    let tip = init::synchronize_listeners(&source, Network::Regtest, &mut cache, listeners)
        .await
        .unwrap();
    assert_eq!(tip.height, 15);
    for l in [&behind, &stale, &current] {
        l.assert_synced(&node).await;
    }
    assert_eq!(stale.disconnects.lock().unwrap()[0].height, 8, "fork point");
    assert!(behind.disconnects.lock().unwrap().is_empty());

    // Then the SPV client keeps one listener in sync.
    let poller = ChainPoller::new(&source, Network::Regtest);
    let mut spv = SpvClient::new(tip, poller, &mut cache, &current);
    let (_, changed) = spv.poll_best_tip().await.unwrap();
    assert!(!changed, "nothing new");

    node.mine_empty(3);
    let (_, changed) = spv.poll_best_tip().await.unwrap();
    assert!(changed);
    current.assert_synced(&node).await;

    // A reorg: disconnects down to the fork, then the new branch.
    node.reorg(3, 5);
    spv.poll_best_tip().await.unwrap();
    current.assert_synced(&node).await;
    let fork = *current.disconnects.lock().unwrap().last().unwrap();
    assert_eq!(fork.height, 15);

    // A reorg to a branch with less work isn't followed.
    let before = current.tip();
    node.reorg(2, 1);
    let (_, changed) = spv.poll_best_tip().await.unwrap();
    assert!(!changed);
    assert_eq!(current.tip(), before);
    // Until it overtakes.
    node.mine_empty(2);
    spv.poll_best_tip().await.unwrap();
    current.assert_synced(&node).await;
}

#[tokio::test]
async fn errors_map_to_ldk_kinds() {
    let node = MockNode::regtest();
    node.mine_empty(3);
    let source = ChainBlockSource::new(node.clone());
    let tip = node.tip().await.unwrap().hash;

    node.fail_next("tip", TransportError::Timeout.into());
    let e = source.get_best_block().await.unwrap_err();
    assert_eq!(e.kind(), BlockSourceErrorKind::Transient);
    let inner = e.into_inner().downcast::<NodeAPIError>().unwrap();
    assert_eq!(*inner, NodeAPIError::Transport(TransportError::Timeout));

    let e = source
        .get_header(&BlockHash::all_zeros(), Some(1))
        .await
        .unwrap_err();
    assert_eq!(e.kind(), BlockSourceErrorKind::Persistent, "unknown block");

    node.fail_next("block", NodeAPIError::Decode("bad".into()));
    let Err(e) = source.get_block(&tip).await else {
        panic!("decode error expected")
    };
    assert_eq!(e.kind(), BlockSourceErrorKind::Persistent);

    node.fail_next(
        "header_info",
        NodeAPIError::Rpc {
            code: RpcErrorCode::InWarmup,
            message: "Loading block index...".into(),
        },
    );
    let e = source.get_header(&tip, Some(3)).await.unwrap_err();
    assert_eq!(e.kind(), BlockSourceErrorKind::Transient);

    // And it recovers.
    assert_eq!(source.get_header(&tip, Some(3)).await.unwrap().height, 3);
}
