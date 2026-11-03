//! J2/J3: every gRPC method against the mock through a tonic client.
mod common;

use bitcoin::consensus::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{Amount, FeeRate};
use bitcoin_gateway::grpc::pb;
use bitcoin_node_api::{FeeMode, FeeTarget, NodeAPIError, RpcErrorCode, TransportError};
use common::{TestGateway, coinbase_out, spend};
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Code, Request};

fn req<T>(token: &str, msg: T) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

const R: &str = "reader-token";
const S: &str = "sender-token";

async fn channel(gw: &TestGateway) -> Channel {
    Channel::from_shared(gw.grpc_url())
        .unwrap()
        .connect()
        .await
        .unwrap()
}

#[tokio::test]
async fn chain() {
    let gw = TestGateway::start().await;
    let hashes = gw.node.mine_empty(3);
    let mut c = pb::chain_client::ChainClient::new(channel(&gw).await);

    let tip = c.tip(req(R, pb::TipRequest {})).await.unwrap().into_inner();
    assert_eq!(tip.height, 3);
    assert_eq!(tip.hash, hashes[2].to_byte_array());

    let h = c
        .block_hash(req(R, pb::BlockHashRequest { height: 1 }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(h.hash, hashes[0].to_byte_array());

    let hash = hashes[1].to_byte_array().to_vec();
    let header = c
        .header(req(R, pb::HashRequest { hash: hash.clone() }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(header.header.len(), 80);
    // Height 2, regtest work 2 per block.
    assert_eq!(header.height, 2);
    let mut work = [0u8; 32];
    work[31] = 6;
    assert_eq!(header.chain_work, work);
    let header: bitcoin::block::Header = deserialize(&header.header).unwrap();
    assert_eq!(header.block_hash(), hashes[1]);

    let filter = c
        .block_filter(req(R, pb::HashRequest { hash: hash.clone() }))
        .await
        .unwrap()
        .into_inner();
    let want = bitcoin_node_api::ChainSource::block_filter(&gw.node, hashes[1])
        .await
        .unwrap();
    assert_eq!(filter.filter, want.content);

    let block = c
        .block(req(R, pb::HashRequest { hash }))
        .await
        .unwrap()
        .into_inner();
    let block: bitcoin::Block = deserialize(&block.block).unwrap();
    assert_eq!(block, gw.node.block_at(2).unwrap());

    let cb = block.txdata[0].compute_txid();
    let tx = c
        .transaction(req(
            R,
            pb::TransactionRequest {
                txid: cb.to_byte_array().to_vec(),
                block: Some(hashes[1].to_byte_array().to_vec()),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        deserialize::<bitcoin::Transaction>(&tx.transaction).unwrap(),
        block.txdata[0]
    );

    let utxo = |vout| pb::UtxoRequest {
        txid: cb.to_byte_array().to_vec(),
        vout,
        include_mempool: true,
    };
    let u = c.utxo(req(R, utxo(0))).await.unwrap().into_inner();
    let u = u.utxo.expect("unspent");
    assert_eq!(u.value, Amount::from_int_btc(50).to_sat());
    assert!(u.coinbase);
    assert_eq!(u.confirmations, 2);
    // None is an unset field, not an error
    let none = c.utxo(req(R, utxo(7))).await.unwrap().into_inner();
    assert_eq!(none.utxo, None);
}

#[tokio::test]
async fn mempool_broadcast_fees() {
    let gw = TestGateway::start().await;
    gw.node.mine_empty(101);
    let ch = channel(&gw).await;
    let mut b = pb::broadcast_client::BroadcastClient::new(ch.clone());
    let mut m = pb::mempool_client::MempoolClient::new(ch.clone());
    let mut f = pb::fees_client::FeesClient::new(ch);

    let tx1 = spend(coinbase_out(&gw.node, 1), 1000);
    let tx2 = spend(coinbase_out(&gw.node, 2), 1000);
    let tx3 = spend(coinbase_out(&gw.node, 3), 1000);

    let t = b
        .test_accept(req(
            S,
            pb::TxsRequest {
                transactions: vec![serialize(&tx1)],
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(t.results[0].allowed);
    assert_eq!(t.results[0].reject_reason, None);

    let id = b
        .broadcast(req(
            S,
            pb::TxRequest {
                transaction: serialize(&tx1),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(id.txid, tx1.compute_txid().to_byte_array());

    let p = b
        .broadcast_package(req(
            S,
            pb::TxsRequest {
                transactions: vec![serialize(&tx1), serialize(&tx2)],
            },
        ))
        .await
        .unwrap()
        .into_inner();
    use pb::package_result::Outcome;
    assert!(matches!(
        p.results[0].outcome,
        Some(Outcome::AlreadyInMempool(_))
    ));
    assert!(matches!(p.results[1].outcome, Some(Outcome::Accepted(_))));

    let ids = m
        .txids(req(R, pb::TxidsRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids.txids.len(), 2);
    let snap = m
        .snapshot(req(R, pb::SnapshotRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(snap.txids.len(), 2);
    assert!(snap.sequence.is_some());
    let e = m
        .entry(req(
            R,
            pb::TxidRequest {
                txid: tx1.compute_txid().to_byte_array().to_vec(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(e.entry.unwrap().ancestors, 1);
    let none = m
        .entry(req(
            R,
            pb::TxidRequest {
                txid: tx3.compute_txid().to_byte_array().to_vec(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(none.entry, None);

    let target = |mode| pb::FeeTarget { blocks: 6, mode };
    let fee = f
        .estimate_fee(req(R, target(pb::FeeMode::Economical as i32)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fee.sat_per_kwu, None);
    gw.node.set_fee(
        FeeTarget {
            blocks: 6,
            mode: FeeMode::Economical,
        },
        Some(FeeRate::from_sat_per_kwu(250)),
    );
    let fee = f
        .estimate_fee(req(R, target(pb::FeeMode::Unspecified as i32)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fee.sat_per_kwu, Some(250));
}

#[tokio::test]
async fn auth_and_errors() {
    let gw = TestGateway::start().await;
    let mut c = pb::chain_client::ChainClient::new(channel(&gw).await);
    let code = |r: Result<tonic::Response<pb::TipReply>, tonic::Status>| r.unwrap_err().code();

    assert_eq!(code(c.tip(pb::TipRequest {}).await), Code::Unauthenticated);
    assert_eq!(
        code(c.tip(req("nope", pb::TipRequest {})).await),
        Code::Unauthenticated
    );
    // the sender key has no chain:read
    assert_eq!(
        code(c.tip(req(S, pb::TipRequest {})).await),
        Code::PermissionDenied
    );

    // the error table, with Core's code alongside
    let cases = [
        (
            NodeAPIError::Rpc {
                code: RpcErrorCode::InvalidAddressOrKey,
                message: "m".into(),
            },
            Code::NotFound,
            Some("-5"),
        ),
        (
            NodeAPIError::Rpc {
                code: RpcErrorCode::VerifyRejected,
                message: "m".into(),
            },
            Code::FailedPrecondition,
            Some("-26"),
        ),
        (
            NodeAPIError::Rpc {
                code: RpcErrorCode::InvalidParameter,
                message: "m".into(),
            },
            Code::InvalidArgument,
            Some("-8"),
        ),
        (
            NodeAPIError::Rpc {
                code: RpcErrorCode::InWarmup,
                message: "m".into(),
            },
            Code::Unavailable,
            Some("-28"),
        ),
        (
            NodeAPIError::Transport(TransportError::Connect("x".into())),
            Code::Unavailable,
            None,
        ),
        (
            NodeAPIError::Transport(TransportError::Timeout),
            Code::DeadlineExceeded,
            None,
        ),
        (
            NodeAPIError::Unsupported("x".into()),
            Code::Unimplemented,
            None,
        ),
        (NodeAPIError::Decode("x".into()), Code::Internal, None),
        (
            NodeAPIError::Transport(TransportError::Auth),
            Code::Internal,
            None,
        ),
    ];
    for (err, want, core) in cases {
        gw.node.fail_next("tip", err.clone());
        let s = c.tip(req(R, pb::TipRequest {})).await.unwrap_err();
        assert_eq!(s.code(), want, "{err:?}");
        assert_eq!(
            s.metadata()
                .get("x-core-code")
                .map(|v| v.to_str().unwrap().to_owned()),
            core.map(str::to_owned),
            "{err:?}"
        );
    }

    // bad input is INVALID_ARGUMENT before reaching the node
    let s = c
        .header(req(
            R,
            pb::HashRequest {
                hash: vec![1, 2, 3],
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(s.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn rate_limit_is_resource_exhausted() {
    let mut k = common::key("slow", "slow-token", &["chain:read"]);
    k.rate = Some(bitcoin_gateway::config::Rate {
        per_second: 0.001,
        burst: 2,
    });
    let core = common::FakeCore::start(|_, _, _| Ok(serde_json::Value::Null)).await;
    let gw = TestGateway::with(vec![k], Default::default(), core).await;
    let mut c = pb::chain_client::ChainClient::new(channel(&gw).await);
    for _ in 0..2 {
        c.tip(req("slow-token", pb::TipRequest {})).await.unwrap();
    }
    let s = c
        .tip(req("slow-token", pb::TipRequest {}))
        .await
        .unwrap_err();
    assert_eq!(s.code(), Code::ResourceExhausted);
}

async fn next(s: &mut tonic::Streaming<pb::NodeEvent>) -> pb::node_event::Event {
    tokio::time::timeout(Duration::from_secs(5), s.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .event
        .unwrap()
}

#[tokio::test]
async fn events_stream() {
    let gw = TestGateway::start().await;
    let mut e = pb::events_client::EventsClient::new(channel(&gw).await);
    assert_eq!(
        e.subscribe(req(S, pb::SubscribeRequest {}))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut s = e
        .subscribe(req(R, pb::SubscribeRequest {}))
        .await
        .unwrap()
        .into_inner();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let h = gw.node.mine(&[]);
    gw.node.lag();
    use pb::node_event::Event;
    assert_eq!(
        next(&mut s).await,
        Event::BlockConnected(pb::BlockConnected {
            hash: h.to_byte_array().to_vec()
        })
    );
    assert_eq!(next(&mut s).await, Event::Lagged(pb::Lagged {}));
    // shutdown ends the stream cleanly
    gw.shutdown.cancel();
    let end = tokio::time::timeout(Duration::from_secs(5), s.next())
        .await
        .unwrap();
    assert!(end.is_none() || end.unwrap().is_ok());
}

#[tokio::test]
async fn health_and_reflection() {
    let gw = TestGateway::start().await;
    let ch = channel(&gw).await;
    let mut h = tonic_health::pb::health_client::HealthClient::new(ch.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;
    for service in ["", "bitcoin.node.v1.Chain"] {
        let r = h
            .check(tonic_health::pb::HealthCheckRequest {
                service: service.into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            r.status,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        );
    }
    gw.gw.health.send_replace(false);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let r = h
        .check(tonic_health::pb::HealthCheckRequest { service: "".into() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        r.status,
        tonic_health::pb::health_check_response::ServingStatus::NotServing as i32
    );

    use tonic_reflection::pb::v1::ServerReflectionRequest;
    use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
    use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
    use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;
    let mut r = ServerReflectionClient::new(ch);
    let ask = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut resp = r
        .server_reflection_info(tokio_stream::iter([ask]))
        .await
        .unwrap()
        .into_inner();
    let Some(MessageResponse::ListServicesResponse(list)) =
        resp.next().await.unwrap().unwrap().message_response
    else {
        panic!("not a service list")
    };
    let names: Vec<_> = list.service.into_iter().map(|s| s.name).collect();
    for s in bitcoin_gateway::grpc::SERVICES {
        assert!(names.iter().any(|n| n == s), "{s} not in {names:?}");
    }
}

#[tokio::test]
async fn grpc_web_when_enabled() {
    use prost::Message;
    let mut settings = bitcoin_gateway::Settings::default();
    settings.grpc.web = true;
    let core = common::FakeCore::start(|_, _, _| Ok(serde_json::Value::Null)).await;
    let gw = TestGateway::with(common::keys(), settings, core).await;
    gw.node.mine_empty(2);
    let msg = pb::TipRequest {}.encode_to_vec();
    let mut body = vec![0u8];
    body.extend((msg.len() as u32).to_be_bytes());
    body.extend(msg);
    let r = reqwest::Client::new()
        .post(format!("{}/bitcoin.node.v1.Chain/Tip", gw.grpc_url()))
        .header("content-type", "application/grpc-web+proto")
        .header("x-grpc-web", "1")
        .header("authorization", format!("Bearer {R}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let bytes = r.bytes().await.unwrap();
    // first frame: the message
    assert_eq!(bytes[0], 0);
    let len = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
    let tip = pb::TipReply::decode(&bytes[5..5 + len]).unwrap();
    assert_eq!(tip.height, 2);
}
