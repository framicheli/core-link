//! `Template`'s block-building helpers.
use bitcoin::blockdata::constants::genesis_block;
use bitcoin::hashes::{Hash, HashEngine};
use bitcoin::{
    Amount, Block, Network, ScriptBuf, Sequence, Transaction, TxMerkleNode, TxOut, Txid, absolute,
    transaction,
};
use bitcoin_node_api::{Coinbase, Solution, Template, TemplateToken};

/// Siblings of the coinbase, leaf to root, for a block of `coinbase` + `txids`.
fn path(txids: &[Txid]) -> Vec<TxMerkleNode> {
    let mut level: Vec<TxMerkleNode> = txids
        .iter()
        .map(|t| TxMerkleNode::from_raw_hash(t.to_raw_hash()))
        .collect();
    let mut path = Vec::new();
    while let Some(&sibling) = level.first() {
        path.push(sibling);
        level = level[1..]
            .chunks(2)
            .map(|c| {
                let mut e = TxMerkleNode::engine();
                e.input(c[0].as_byte_array());
                e.input(c[c.len() - 1].as_byte_array());
                TxMerkleNode::from_engine(e)
            })
            .collect();
    }
    path
}

fn template(others: &[Transaction]) -> Template {
    let genesis = genesis_block(Network::Regtest);
    let txids: Vec<_> = others.iter().map(Transaction::compute_txid).collect();
    Template {
        prev_hash: genesis.block_hash(),
        height: 1,
        version: genesis.header.version,
        bits: genesis.header.bits,
        min_time: 0,
        cur_time: 0,
        coinbase: Coinbase {
            version: transaction::Version::TWO,
            sequence: Sequence::MAX,
            script_sig_prefix: ScriptBuf::from_hex("51").unwrap(),
            witness: Some([0; 32]),
            reward_remaining: Amount::from_int_btc(50),
            required_outputs: vec![TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::from_hex("6a").unwrap(),
            }],
            lock_time: absolute::LockTime::ZERO,
        },
        coinbase_merkle_path: path(&txids),
        token: TemplateToken::new(()),
    }
}

/// Distinct dummy transactions.
fn txs(n: u32) -> Vec<Transaction> {
    (0..n)
        .map(|i| Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::from_consensus(i),
            input: vec![],
            output: vec![],
        })
        .collect()
}

#[test]
fn coinbase_layout() {
    let t = template(&[]);
    let cb = t.coinbase_tx(&ScriptBuf::from_hex("52").unwrap(), &[0xaa, 0xbb]);
    assert!(cb.is_coinbase());
    assert_eq!(
        cb.input[0].script_sig.as_bytes(),
        [0x51, 0x02, 0xaa, 0xbb],
        "prefix, then a push"
    );
    assert_eq!(cb.input[0].witness.len(), 1);
    assert_eq!(cb.output.len(), 2, "payout, then required");
    assert_eq!(cb.output[0].value, Amount::from_int_btc(50));
    assert_eq!(cb.output[1], t.coinbase.required_outputs[0]);
}

#[test]
fn merkle_root_matches_the_full_block() {
    for n in [0, 1, 2, 3, 4, 5, 8, 13] {
        let others = txs(n);
        let t = template(&others);
        let sol = Solution {
            version: t.version,
            time: 1,
            nonce: 7,
            coinbase: t.coinbase_tx(&ScriptBuf::new(), &[1]),
        };
        let header = t.header(&sol);
        let block = Block {
            header,
            txdata: [sol.coinbase.clone()].into_iter().chain(others).collect(),
        };
        assert_eq!(
            Some(header.merkle_root),
            block.compute_merkle_root(),
            "{n} other txs"
        );
        assert_eq!(header.prev_blockhash, t.prev_hash);
        assert_eq!((header.time, header.nonce), (1, 7));
    }
}
