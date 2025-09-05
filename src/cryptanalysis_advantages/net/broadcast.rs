use anyhow::{Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig,
    tpu_client::TpuClient,
};
use solana_connection_cache::connection_cache::ConnectionCache;
use solana_sdk::{
    commitment_config::CommitmentLevel,
    signature::Signature,
    transaction::VersionedTransaction,
};
use std::{net::SocketAddr, sync::Arc, time::Instant};
use tokio::{task, time::{timeout, Duration}};

use jito_sdk_rust::JitoJsonRpcSDK;

use crate::execution::types::{RouteKind, SendOutcome};

#[async_trait]
pub trait Route: Send + Sync {
    async fn send(&self, tx: &VersionedTransaction) -> Result<SendOutcome>;
    fn kind(&self) -> RouteKind;
}

pub struct PublicRpcRoute {
    rpc: Arc<RpcClient>,
    min_context_slot: DashMap<&'static str, u64>,
    send_timeout: Duration,
}

impl PublicRpcRoute {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        Self { rpc, min_context_slot: DashMap::new(), send_timeout: Duration::from_millis(800) }
    }
    pub fn set_min_context_slot(&self, key: &'static str, slot: u64) {
        self.min_context_slot.insert(key, slot);
    }
}

#[async_trait]
impl Route for PublicRpcRoute {
    async fn send(&self, tx: &VersionedTransaction) -> Result<SendOutcome> {
        let cfg = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: None,
            max_retries: Some(0),
            min_context_slot: self.min_context_slot.get("global").map(|v| *v.value()),
        };
        let sent_at = Instant::now();
        let sig = timeout(self.send_timeout, self.rpc.send_transaction_with_config(tx, cfg))
            .await
            .context("rpc send timeout")?
            .context("rpc send failed")?;
        Ok(SendOutcome { signature: sig, route: RouteKind::PublicRpc, sent_at })
    }
    fn kind(&self) -> RouteKind { RouteKind::PublicRpc }
}

pub struct DirectTpuRoute {
    tpu: Arc<TpuClient>,
}

impl DirectTpuRoute {
    pub fn new(rpc: Arc<RpcClient>) -> Result<Self> {
        // TpuClient is sync; wrap with blocking for send
        let connection_cache = Arc::new(ConnectionCache::new(1024));
        let tpu = TpuClient::new_with_rpc_client(rpc, connection_cache, None);
        Ok(Self { tpu: Arc::new(tpu) })
    }
}

#[async_trait]
impl Route for DirectTpuRoute {
    async fn send(&self, tx: &VersionedTransaction) -> Result<SendOutcome> {
        let wire = bincode::serialize(tx)?;
        let sent_at = Instant::now();
        let tpu = self.tpu.clone();
        // Use blocking to access sync TpuClient; offload to thread pool
        task::spawn_blocking(move || {
            tpu.send_wire_transaction(&wire)
                .map_err(|e| anyhow::anyhow!("TPU send error: {e}"))
        }).await??;
        // For signature derivation we need the tx signature
        let sig = tx.signatures.get(0).cloned()
            .ok_or_else(|| anyhow::anyhow!("missing signature"))?;
        Ok(SendOutcome { signature: sig, route: RouteKind::DirectTpu, sent_at })
    }
    fn kind(&self) -> RouteKind { RouteKind::DirectTpu }
}

pub struct JitoBundleRoute {
    sdk: JitoJsonRpcSDK,
    // Tip bidding is encoded as a transfer to a tip account in the bundle.
}

impl JitoBundleRoute {
    pub fn new(block_engine_url: &str, uuid: Option<String>) -> Self {
        Self { sdk: JitoJsonRpcSDK::new(block_engine_url, uuid) }
    }
}

#[async_trait]
impl Route for JitoBundleRoute {
    async fn send(&self, tx: &VersionedTransaction) -> Result<SendOutcome> {
        // Encode tx as base64 for JSON-RPC submission
        let raw = bincode::serialize(tx)?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        let tip_acc = self.sdk.get_random_tip_account().await
            .context("get tip account")?;
        // Single-transaction bundle for low latency
        let sent_at = Instant::now();
        let res = self.sdk.send_bundle(vec![b64], Some(tip_acc), None).await
            .context("jito send bundle")?;
        // Jito returns a bundle id; we still use the tx signature for confirmation
        let sig = tx.signatures[0];
        // Optional: poll bundle status out-of-band
        let _ = res;
        Ok(SendOutcome { signature: sig, route: RouteKind::JitoBundle, sent_at })
    }
    fn kind(&self) -> RouteKind { RouteKind::JitoBundle }
}

pub struct Broadcaster {
    routes: Vec<Arc<dyn Route>>,
}

impl Broadcaster {
    pub fn new(routes: Vec<Arc<dyn Route>>) -> Self { Self { routes } }

    pub async fn race_send(&self, tx: VersionedTransaction) -> Result<SendOutcome> {
        let mut handles = futures::future::select_all(
            self.routes
                .iter()
                .map(|r| {
                    let t = tx.clone();
                    let r = r.clone();
                    async move { r.send(&t).await }
                })
                .collect::<Vec<_>>(),
        );
        // First to succeed wins
        while !handles.2.is_empty() {
            match handles.0.await {
                Ok(out) => return Ok(out),
                Err(_) => {
                    let (next, _, rest) = futures::future::select_all(handles.2).await;
                    handles = (next, 0, rest);
                }
            }
        }
        anyhow::bail!("all routes failed")
    }
}
