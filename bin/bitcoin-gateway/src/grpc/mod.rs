//! gRPC: one service per trait over the [`api`](crate::api) handlers, plus
//! health and reflection. Hashes are 32 bytes in internal byte order;
//! transactions, blocks and headers are consensus bytes (see the proto).
use crate::Gateway;
use crate::auth::Key;
use crate::error::{ApiError, Kind};
use crate::node::Backend;
use axum::Router;
use bitcoin::consensus::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, OutPoint, Transaction, Txid};
use bitcoin_node_api::{FeeMode, FeeTarget, NodeEvent, PackageOutcome};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

/// The generated code: messages, servers and clients.
#[allow(missing_docs, clippy::all)]
pub mod pb {
    include!("generated/bitcoin.node.v1.rs");
}

/// The encoded descriptor set, for reflection.
pub const DESCRIPTOR: &[u8] = include_bytes!("generated/descriptor.bin");

/// Every gRPC service's full name.
pub const SERVICES: &[&str] = &[
    "bitcoin.node.v1.Chain",
    "bitcoin.node.v1.Mempool",
    "bitcoin.node.v1.Broadcast",
    "bitcoin.node.v1.Fees",
    "bitcoin.node.v1.Events",
];

impl From<ApiError> for Status {
    fn from(e: ApiError) -> Status {
        let code = match e.kind {
            Kind::NotFound => tonic::Code::NotFound,
            Kind::FailedPrecondition => tonic::Code::FailedPrecondition,
            Kind::InvalidArgument => tonic::Code::InvalidArgument,
            Kind::Unavailable => tonic::Code::Unavailable,
            Kind::DeadlineExceeded => tonic::Code::DeadlineExceeded,
            Kind::Unimplemented => tonic::Code::Unimplemented,
            Kind::Internal => tonic::Code::Internal,
            Kind::Unauthenticated => tonic::Code::Unauthenticated,
            Kind::PermissionDenied => tonic::Code::PermissionDenied,
            Kind::ResourceExhausted => tonic::Code::ResourceExhausted,
        };
        let mut s = Status::new(code, e.message);
        if let Some(c) = e.core_code {
            s.metadata_mut()
                .insert("x-core-code", MetadataValue::from(c));
        }
        s.metadata_mut().insert(
            "x-error-code",
            e.code
                .parse()
                .unwrap_or(MetadataValue::from_static("other")),
        );
        s
    }
}

/// The services, sharing one gateway.
pub struct Svc<N>(pub Arc<Gateway<N>>);

impl<N> Clone for Svc<N> {
    fn clone(&self) -> Self {
        Svc(self.0.clone())
    }
}

impl<N: Backend> Svc<N> {
    fn key<T>(&self, req: &Request<T>) -> Result<Arc<Key>, Status> {
        let auth = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        Ok(self.0.keys.authenticate(auth)?)
    }
}

fn hash32(b: &[u8], what: &str) -> Result<[u8; 32], Status> {
    <[u8; 32]>::try_from(b).map_err(|_| {
        ApiError::bad_request(format!("{what} must be 32 bytes, internal byte order")).into()
    })
}

fn block_hash(b: &[u8]) -> Result<BlockHash, Status> {
    hash32(b, "hash").map(BlockHash::from_byte_array)
}

fn txid(b: &[u8]) -> Result<Txid, Status> {
    hash32(b, "txid").map(Txid::from_byte_array)
}

fn tx(b: &[u8]) -> Result<Transaction, Status> {
    deserialize(b).map_err(|e| ApiError::bad_request(format!("transaction: {e}")).into())
}

fn txs(bs: &[Vec<u8>]) -> Result<Vec<Transaction>, Status> {
    bs.iter().map(|b| tx(b)).collect()
}

type Reply<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl<N: Backend> pb::chain_server::Chain for Svc<N> {
    async fn tip(&self, req: Request<pb::TipRequest>) -> Reply<pb::TipReply> {
        let t = self.0.tip(self.key(&req)?.as_ref()).await?;
        Ok(Response::new(pb::TipReply {
            height: t.height,
            hash: t.hash.to_byte_array().to_vec(),
        }))
    }

    async fn block_hash(&self, req: Request<pb::BlockHashRequest>) -> Reply<pb::BlockHashReply> {
        let h = self
            .0
            .block_hash(self.key(&req)?.as_ref(), req.get_ref().height)
            .await?;
        Ok(Response::new(pb::BlockHashReply {
            hash: h.to_byte_array().to_vec(),
        }))
    }

    async fn header(&self, req: Request<pb::HashRequest>) -> Reply<pb::HeaderReply> {
        let hash = block_hash(&req.get_ref().hash)?;
        let i = self.0.header(self.key(&req)?.as_ref(), hash).await?;
        Ok(Response::new(pb::HeaderReply {
            header: serialize(&i.header),
            height: i.height,
            chain_work: i.chain_work.to_be_bytes().to_vec(),
        }))
    }

    async fn block_filter(&self, req: Request<pb::HashRequest>) -> Reply<pb::BlockFilterReply> {
        let hash = block_hash(&req.get_ref().hash)?;
        let f = self.0.block_filter(self.key(&req)?.as_ref(), hash).await?;
        Ok(Response::new(pb::BlockFilterReply { filter: f.content }))
    }

    async fn block(&self, req: Request<pb::HashRequest>) -> Reply<pb::BlockReply> {
        let hash = block_hash(&req.get_ref().hash)?;
        let b = self.0.block(self.key(&req)?.as_ref(), hash).await?;
        Ok(Response::new(pb::BlockReply {
            block: serialize(&b),
        }))
    }

    async fn transaction(
        &self,
        req: Request<pb::TransactionRequest>,
    ) -> Reply<pb::TransactionReply> {
        let r = req.get_ref();
        let id = txid(&r.txid)?;
        let block = r.block.as_deref().map(block_hash).transpose()?;
        let t = self
            .0
            .transaction(self.key(&req)?.as_ref(), id, block)
            .await?;
        Ok(Response::new(pb::TransactionReply {
            transaction: serialize(&t),
        }))
    }

    async fn utxo(&self, req: Request<pb::UtxoRequest>) -> Reply<pb::UtxoReply> {
        let r = req.get_ref();
        let op = OutPoint::new(txid(&r.txid)?, r.vout);
        let u = self
            .0
            .utxo(self.key(&req)?.as_ref(), op, r.include_mempool)
            .await?;
        Ok(Response::new(pb::UtxoReply {
            utxo: u.map(|u| pb::Utxo {
                value: u.value.to_sat(),
                script_pubkey: u.script_pubkey.to_bytes(),
                confirmations: u.confirmations,
                coinbase: u.coinbase,
            }),
        }))
    }
}

#[tonic::async_trait]
impl<N: Backend> pb::mempool_server::Mempool for Svc<N> {
    async fn txids(&self, req: Request<pb::TxidsRequest>) -> Reply<pb::TxidsReply> {
        let ids = self.0.txids(self.key(&req)?.as_ref()).await?;
        Ok(Response::new(pb::TxidsReply {
            txids: ids.iter().map(|t| t.to_byte_array().to_vec()).collect(),
        }))
    }

    async fn snapshot(&self, req: Request<pb::SnapshotRequest>) -> Reply<pb::MempoolSnapshot> {
        let s = self.0.snapshot(self.key(&req)?.as_ref()).await?;
        Ok(Response::new(pb::MempoolSnapshot {
            txids: s.txids.iter().map(|t| t.to_byte_array().to_vec()).collect(),
            sequence: s.sequence,
        }))
    }

    async fn entry(&self, req: Request<pb::TxidRequest>) -> Reply<pb::EntryReply> {
        let id = txid(&req.get_ref().txid)?;
        let e = self.0.entry(self.key(&req)?.as_ref(), id).await?;
        Ok(Response::new(pb::EntryReply {
            entry: e.map(|e| pb::MempoolEntry {
                vsize: e.vsize,
                weight: e.weight.to_wu(),
                fee: e.fee.to_sat(),
                time: e.time,
                height: e.height,
                ancestors: e.ancestors,
                descendants: e.descendants,
            }),
        }))
    }
}

#[tonic::async_trait]
impl<N: Backend> pb::broadcast_server::Broadcast for Svc<N> {
    async fn broadcast(&self, req: Request<pb::TxRequest>) -> Reply<pb::TxidReply> {
        let t = tx(&req.get_ref().transaction)?;
        let id = self.0.broadcast(self.key(&req)?.as_ref(), &t).await?;
        Ok(Response::new(pb::TxidReply {
            txid: id.to_byte_array().to_vec(),
        }))
    }

    async fn broadcast_package(&self, req: Request<pb::TxsRequest>) -> Reply<pb::PackageReply> {
        let t = txs(&req.get_ref().transactions)?;
        let rs = self
            .0
            .broadcast_package(self.key(&req)?.as_ref(), &t)
            .await?;
        let results = rs
            .into_iter()
            .map(|r| {
                use pb::package_result::Outcome;
                pb::PackageResult {
                    txid: r.txid.to_byte_array().to_vec(),
                    outcome: Some(match r.outcome {
                        PackageOutcome::Accepted { fee } => {
                            Outcome::Accepted(pb::Accepted { fee: fee.to_sat() })
                        }
                        PackageOutcome::AlreadyInMempool => {
                            Outcome::AlreadyInMempool(pb::AlreadyInMempool {})
                        }
                        PackageOutcome::Rejected(reason) => Outcome::Rejected(reason),
                    }),
                }
            })
            .collect();
        Ok(Response::new(pb::PackageReply { results }))
    }

    async fn test_accept(&self, req: Request<pb::TxsRequest>) -> Reply<pb::TestAcceptReply> {
        let t = txs(&req.get_ref().transactions)?;
        let rs = self.0.test_accept(self.key(&req)?.as_ref(), &t).await?;
        Ok(Response::new(pb::TestAcceptReply {
            results: rs
                .into_iter()
                .map(|r| pb::MempoolAccept {
                    txid: r.txid.to_byte_array().to_vec(),
                    allowed: r.allowed,
                    reject_reason: r.reject_reason,
                })
                .collect(),
        }))
    }
}

#[tonic::async_trait]
impl<N: Backend> pb::fees_server::Fees for Svc<N> {
    async fn estimate_fee(&self, req: Request<pb::FeeTarget>) -> Reply<pb::FeeReply> {
        let r = req.get_ref();
        let blocks = u16::try_from(r.blocks)
            .map_err(|_| ApiError::bad_request("blocks must be at most 65535"))?;
        let mode = match pb::FeeMode::try_from(r.mode) {
            Ok(pb::FeeMode::Conservative) => FeeMode::Conservative,
            Ok(pb::FeeMode::Economical | pb::FeeMode::Unspecified) => FeeMode::Economical,
            Err(_) => return Err(ApiError::bad_request("unknown fee mode").into()),
        };
        let rate = self
            .0
            .estimate_fee(self.key(&req)?.as_ref(), FeeTarget { blocks, mode })
            .await?;
        Ok(Response::new(pb::FeeReply {
            sat_per_kwu: rate.map(|r| r.to_sat_per_kwu()),
        }))
    }
}

/// `NodeEvent` as the proto's.
pub fn event(e: NodeEvent) -> Option<pb::NodeEvent> {
    use pb::node_event::Event;
    let ev = match e {
        NodeEvent::BlockConnected(h) => Event::BlockConnected(pb::BlockConnected {
            hash: h.to_byte_array().to_vec(),
        }),
        NodeEvent::BlockDisconnected(h) => Event::BlockDisconnected(pb::BlockDisconnected {
            hash: h.to_byte_array().to_vec(),
        }),
        NodeEvent::TxAdded {
            txid,
            mempool_sequence,
        } => Event::TxAdded(pb::TxAdded {
            txid: txid.to_byte_array().to_vec(),
            mempool_sequence,
        }),
        NodeEvent::TxRemoved {
            txid,
            mempool_sequence,
        } => Event::TxRemoved(pb::TxRemoved {
            txid: txid.to_byte_array().to_vec(),
            mempool_sequence,
        }),
        NodeEvent::Lagged => Event::Lagged(pb::Lagged {}),
        // A variant newer than this proto: tell the client it missed something.
        _ => Event::Lagged(pb::Lagged {}),
    };
    Some(pb::NodeEvent { event: Some(ev) })
}

type EventStream = Pin<Box<dyn Stream<Item = Result<pb::NodeEvent, Status>> + Send>>;

#[tonic::async_trait]
impl<N: Backend> pb::events_server::Events for Svc<N> {
    type SubscribeStream = EventStream;

    async fn subscribe(&self, req: Request<pb::SubscribeRequest>) -> Reply<EventStream> {
        let sub = self.0.subscribe(self.key(&req)?.as_ref()).await?;
        Ok(Response::new(Box::pin(
            sub.filter_map(|e| event(e).map(Ok)),
        )))
    }
}

impl<N: Backend> Gateway<N> {
    /// The gRPC listener's routes: the five services, health, reflection,
    /// and gRPC-Web when `[grpc] web` is set.
    pub fn grpc_router(self: &Arc<Self>) -> Router {
        let svc = Svc(self.clone());
        let (reporter, health) = tonic_health::server::health_reporter();
        let status = self.health.clone();
        // The health service follows `/readyz`.
        tokio::spawn(async move {
            let mut rx = status.subscribe();
            loop {
                let serving = *rx.borrow_and_update();
                let s = if serving {
                    tonic_health::ServingStatus::Serving
                } else {
                    tonic_health::ServingStatus::NotServing
                };
                for name in std::iter::once("").chain(SERVICES.iter().copied()) {
                    reporter.set_service_status(name, s).await;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            }
        });
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(DESCRIPTOR)
            .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
            .build_v1()
            .expect("the committed descriptor set is valid");
        let routes = tonic::service::Routes::new(pb::chain_server::ChainServer::new(svc.clone()))
            .add_service(pb::mempool_server::MempoolServer::new(svc.clone()))
            .add_service(pb::broadcast_server::BroadcastServer::new(svc.clone()))
            .add_service(pb::fees_server::FeesServer::new(svc.clone()))
            .add_service(pb::events_server::EventsServer::new(svc))
            .add_service(health)
            .add_service(reflection);
        let router = routes.into_axum_router();
        if self.settings.grpc.web {
            router
                .layer(tonic_web::GrpcWebLayer::new())
                .layer(crate::cors(&self.settings.http.cors_origins))
        } else {
            router
        }
    }
}
