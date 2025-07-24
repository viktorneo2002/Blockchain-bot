use anyhow::{anyhow, Result};
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use quinn::{ClientConfig, Endpoint, EndpointConfig, TransportConfig, VarInt};
use rustls::{Certificate, PrivateKey, RootCertStore};
use solana_sdk::{
    packet::PACKET_DATA_SIZE,
    pubkey::Pubkey,
    signature::Keypair,
    transaction::VersionedTransaction,
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, RwLock, Semaphore},
    time::{interval, sleep, timeout},
};
use tracing::{debug, error, info, warn};

const MAX_CONCURRENT_CONNECTIONS: usize = 256;
const MAX_CONCURRENT_STREAMS: usize = 2048;
const CONNECTION_TIMEOUT_MS: u64 = 1500;
const STREAM_TIMEOUT_MS: u64 = 500;
const MAX_RETRIES: u32 = 3;
const RETRY_BACKOFF_MS: u64 = 50;
const CONNECTION_POOL_SIZE: usize = 32;
const KEEPALIVE_INTERVAL_MS: u64 = 1000;
const MAX_BUNDLE_SIZE: usize = 1232;
const QUIC_PORT: u16 = 8000;
const MAX_DATAGRAM_SIZE: usize = 1280;
const INITIAL_RTT_MS: u32 = 50;
const MAX_IDLE_TIMEOUT_MS: u64 = 5000;
const STREAM_RECEIVE_WINDOW: u32 = 1024 * 1024;
const CONNECTION_RECEIVE_WINDOW: u32 = 10 * 1024 * 1024;
const MAX_CONCURRENT_BIDI_STREAMS: u32 = 128;
const MAX_CONCURRENT_UNI_STREAMS: u32 = 512;

#[derive(Clone)]
pub struct QuicBundleTransport {
    endpoint: Arc<Endpoint>,
    connections: Arc<DashMap<SocketAddr, Arc<ConnectionPool>>>,
    metrics: Arc<TransportMetrics>,
    semaphore: Arc<Semaphore>,
    config: Arc<TransportConfig>,
    keypair: Arc<Keypair>,
}

struct ConnectionPool {
    connections: Arc<RwLock<Vec<Arc<quinn::Connection>>>>,
    addr: SocketAddr,
    last_used: Arc<AtomicU64>,
    failures: Arc<AtomicUsize>,
}

#[derive(Default)]
struct TransportMetrics {
    bundles_sent: AtomicU64,
    bundles_failed: AtomicU64,
    connections_created: AtomicU64,
    connections_failed: AtomicU64,
    avg_latency_us: AtomicU64,
    bytes_sent: AtomicU64,
}

pub struct BundleSubmission {
    pub transactions: Vec<VersionedTransaction>,
    pub slot: u64,
    pub priority: u64,
    pub tip_account: Option<Pubkey>,
}

pub struct SubmissionResult {
    pub success: bool,
    pub latency_us: u64,
    pub error: Option<String>,
    pub retry_count: u32,
}

impl QuicBundleTransport {
    pub async fn new(keypair: Keypair, bind_addr: SocketAddr) -> Result<Self> {
        let client_config = Self::create_client_config()?;
        let endpoint_config = Self::create_endpoint_config();
        
        let endpoint = Endpoint::client(bind_addr)?;
        endpoint.set_default_client_config(client_config);
        
        let transport_config = Arc::new(Self::create_transport_config());
        
        Ok(Self {
            endpoint: Arc::new(endpoint),
            connections: Arc::new(DashMap::new()),
            metrics: Arc::new(TransportMetrics::default()),
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS)),
            config: transport_config,
            keypair: Arc::new(keypair),
        })
    }

    fn create_client_config() -> Result<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
            rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            )
        }));

        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(roots)
            .with_no_client_auth();

        crypto.dangerous().set_certificate_verifier(Arc::new(NoVerifier));
        crypto.alpn_protocols = vec![b"solana-tpu".to_vec()];

        let mut config = ClientConfig::new(Arc::new(crypto));
        let mut transport = TransportConfig::default();
        
        transport.max_concurrent_bidi_streams(VarInt::from_u32(MAX_CONCURRENT_BIDI_STREAMS));
        transport.max_concurrent_uni_streams(VarInt::from_u32(MAX_CONCURRENT_UNI_STREAMS));
        transport.max_idle_timeout(Some(Duration::from_millis(MAX_IDLE_TIMEOUT_MS).try_into()?));
        transport.initial_rtt(Duration::from_millis(INITIAL_RTT_MS as u64));
        transport.stream_receive_window(VarInt::from_u32(STREAM_RECEIVE_WINDOW));
        transport.receive_window(VarInt::from_u32(CONNECTION_RECEIVE_WINDOW));
        transport.datagram_receive_buffer_size(Some(MAX_DATAGRAM_SIZE));
        transport.datagram_send_buffer_size(MAX_DATAGRAM_SIZE);
        
        config.transport_config(Arc::new(transport));
        Ok(config)
    }

    fn create_endpoint_config() -> EndpointConfig {
        EndpointConfig::default()
    }

    fn create_transport_config() -> TransportConfig {
        let mut config = TransportConfig::default();
        config.max_concurrent_bidi_streams(VarInt::from_u32(MAX_CONCURRENT_BIDI_STREAMS));
        config.max_concurrent_uni_streams(VarInt::from_u32(MAX_CONCURRENT_UNI_STREAMS));
        config.initial_rtt(Duration::from_millis(INITIAL_RTT_MS as u64));
        config.stream_receive_window(VarInt::from_u32(STREAM_RECEIVE_WINDOW));
        config.receive_window(VarInt::from_u32(CONNECTION_RECEIVE_WINDOW));
        config
    }

    pub async fn submit_bundle(
        &self,
        submission: BundleSubmission,
        endpoints: Vec<SocketAddr>,
    ) -> Result<SubmissionResult> {
        let start = Instant::now();
        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count < MAX_RETRIES {
            match self.try_submit_bundle(&submission, &endpoints, retry_count).await {
                Ok(result) => {
                    self.metrics.bundles_sent.fetch_add(1, Ordering::Relaxed);
                    let latency_us = start.elapsed().as_micros() as u64;
                    self.update_latency_metric(latency_us);
                    
                    return Ok(SubmissionResult {
                        success: true,
                        latency_us,
                        error: None,
                        retry_count,
                    });
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    retry_count += 1;
                    
                    if retry_count < MAX_RETRIES {
                        let backoff = RETRY_BACKOFF_MS * (1 << retry_count.min(4));
                        sleep(Duration::from_millis(backoff)).await;
                    }
                }
            }
        }

        self.metrics.bundles_failed.fetch_add(1, Ordering::Relaxed);
        Ok(SubmissionResult {
            success: false,
            latency_us: start.elapsed().as_micros() as u64,
            error: last_error,
            retry_count,
        })
    }

    async fn try_submit_bundle(
        &self,
        submission: &BundleSubmission,
        endpoints: &[SocketAddr],
        retry_count: u32,
    ) -> Result<()> {
        let bundle_data = self.serialize_bundle(submission)?;
        
        if bundle_data.len() > MAX_BUNDLE_SIZE {
            return Err(anyhow!("Bundle exceeds maximum size"));
        }

        let mut futures = FuturesUnordered::new();
        
        for endpoint in endpoints.iter() {
            let bundle_clone = bundle_data.clone();
            let transport = self.clone();
            let addr = *endpoint;
            
            futures.push(async move {
                transport.send_to_endpoint(addr, bundle_clone).await
            });
        }

        let timeout_duration = Duration::from_millis(
            CONNECTION_TIMEOUT_MS + (retry_count as u64 * 100)
        );

        match timeout(timeout_duration, async {
            while let Some(result) = futures.next().await {
                if result.is_ok() {
                    return Ok(());
                }
            }
            Err(anyhow!("All endpoints failed"))
        }).await {
            Ok(result) => result,
            Err(_) => Err(anyhow!("Bundle submission timed out")),
        }
    }

    async fn send_to_endpoint(&self, addr: SocketAddr, data: Vec<u8>) -> Result<()> {
        let _permit = self.semaphore.acquire().await?;
        let connection = self.get_or_create_connection(addr).await?;
        
        match timeout(
            Duration::from_millis(STREAM_TIMEOUT_MS),
            self.send_on_connection(&connection, &data)
        ).await {
            Ok(Ok(_)) => {
                self.metrics.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);
                Ok(())
            }
            Ok(Err(e)) => {
                self.handle_connection_error(addr).await;
                Err(e)
            }
            Err(_) => {
                self.handle_connection_error(addr).await;
                Err(anyhow!("Stream timeout"))
            }
        }
    }

    async fn get_or_create_connection(&self, addr: SocketAddr) -> Result<Arc<quinn::Connection>> {
        if let Some(pool) = self.connections.get(&addr) {
            if let Some(conn) = self.get_healthy_connection(&pool).await {
                pool.last_used.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    Ordering::Relaxed
                );
                return Ok(conn);
            }
        }

        self.create_new_connection(addr).await
    }

    async fn get_healthy_connection(&self, pool: &ConnectionPool) -> Option<Arc<quinn::Connection>> {
        let connections = pool.connections.read().await;
        
        for conn in connections.iter() {
            if !conn.close_reason().is_some() {
                return Some(Arc::clone(conn));
            }
        }
        
        None
    }

    async fn create_new_connection(&self, addr: SocketAddr) -> Result<Arc<quinn::Connection>> {
        let connecting = match timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MS),
            self.endpoint.connect(addr, "localhost")
        ).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                self.metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow!("Failed to initiate connection: {}", e));
            }
            Err(_) => {
                self.metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow!("Connection timeout"));
            }
        };

        let connection = match timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MS),
            connecting
        ).await {
            Ok(Ok(conn)) => Arc::new(conn),
            Ok(Err(e)) => {
                self.metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow!("Connection failed: {}", e));
            }
            Err(_) => {
                self.metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow!("Connection establishment timeout"));
            }
        };

        self.metrics.connections_created.fetch_add(1, Ordering::Relaxed);
        
        let pool = self.connections.entry(addr).or_insert_with(|| {
            Arc::new(ConnectionPool {
                connections: Arc::new(RwLock::new(Vec::new())),
                addr,
                last_used: Arc::new(AtomicU64::new(0)),
                failures: Arc::new(AtomicUsize::new(0)),
            })
        });

        let mut connections = pool.connections.write().await;
        connections.retain(|c| !c.close_reason().is_some());
        
        if connections.len() < CONNECTION_POOL_SIZE {
            connections.push(Arc::clone(&connection));
        }

        Ok(connection)
    }

    async fn send_on_connection(&self, connection: &quinn::Connection, data: &[u8]) -> Result<()> {
        let mut stream = connection.open_uni().await?;
        stream.write_all(data).await?;
        stream.finish().await?;
        Ok(())
    }

        async fn handle_connection_error(&self, addr: SocketAddr) {
        if let Some(pool) = self.connections.get(&addr) {
            pool.failures.fetch_add(1, Ordering::Relaxed);
            
            let failures = pool.failures.load(Ordering::Relaxed);
            if failures > 10 {
                let mut connections = pool.connections.write().await;
                connections.clear();
                pool.failures.store(0, Ordering::Relaxed);
            }
        }
    }

    fn serialize_bundle(&self, submission: &BundleSubmission) -> Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(MAX_BUNDLE_SIZE);
        
        buffer.extend_from_slice(&submission.slot.to_le_bytes());
        buffer.extend_from_slice(&submission.priority.to_le_bytes());
        buffer.push(submission.transactions.len() as u8);
        
        if let Some(tip_account) = &submission.tip_account {
            buffer.push(1);
            buffer.extend_from_slice(tip_account.as_ref());
        } else {
            buffer.push(0);
        }
        
        for tx in &submission.transactions {
            let tx_bytes = bincode::serialize(tx)
                .map_err(|e| anyhow!("Failed to serialize transaction: {}", e))?;
            
            if buffer.len() + tx_bytes.len() + 2 > MAX_BUNDLE_SIZE {
                return Err(anyhow!("Bundle size exceeds limit"));
            }
            
            buffer.extend_from_slice(&(tx_bytes.len() as u16).to_le_bytes());
            buffer.extend_from_slice(&tx_bytes);
        }
        
        Ok(buffer)
    }

    fn update_latency_metric(&self, latency_us: u64) {
        let current_avg = self.metrics.avg_latency_us.load(Ordering::Relaxed);
        let total_sent = self.metrics.bundles_sent.load(Ordering::Relaxed);
        
        if total_sent > 0 {
            let new_avg = (current_avg * (total_sent - 1) + latency_us) / total_sent;
            self.metrics.avg_latency_us.store(new_avg, Ordering::Relaxed);
        } else {
            self.metrics.avg_latency_us.store(latency_us, Ordering::Relaxed);
        }
    }

    pub async fn start_maintenance_tasks(&self) {
        let transport = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                transport.cleanup_stale_connections().await;
            }
        });

        let transport = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(KEEPALIVE_INTERVAL_MS));
            loop {
                interval.tick().await;
                transport.send_keepalives().await;
            }
        });
    }

    async fn cleanup_stale_connections(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut to_remove = Vec::new();
        
        for entry in self.connections.iter() {
            let last_used = entry.value().last_used.load(Ordering::Relaxed);
            if now - last_used > 300 {
                to_remove.push(entry.key().clone());
            }
        }

        for addr in to_remove {
            if let Some((_, pool)) = self.connections.remove(&addr) {
                let mut connections = pool.connections.write().await;
                for conn in connections.iter() {
                    conn.close(0u32.into(), b"stale");
                }
                connections.clear();
            }
        }
    }

    async fn send_keepalives(&self) {
        let mut futures = FuturesUnordered::new();
        
        for entry in self.connections.iter() {
            let pool = entry.value().clone();
            futures.push(async move {
                let connections = pool.connections.read().await;
                for conn in connections.iter() {
                    if !conn.close_reason().is_some() {
                        let _ = conn.send_datagram(b"ping");
                    }
                }
            });
        }

        while futures.next().await.is_some() {}
    }

    pub fn get_metrics(&self) -> TransportMetricsSnapshot {
        TransportMetricsSnapshot {
            bundles_sent: self.metrics.bundles_sent.load(Ordering::Relaxed),
            bundles_failed: self.metrics.bundles_failed.load(Ordering::Relaxed),
            connections_created: self.metrics.connections_created.load(Ordering::Relaxed),
            connections_failed: self.metrics.connections_failed.load(Ordering::Relaxed),
            avg_latency_us: self.metrics.avg_latency_us.load(Ordering::Relaxed),
            bytes_sent: self.metrics.bytes_sent.load(Ordering::Relaxed),
            active_connections: self.connections.len(),
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        for entry in self.connections.iter() {
            let pool = entry.value();
            let connections = pool.connections.read().await;
            for conn in connections.iter() {
                conn.close(0u32.into(), b"shutdown");
            }
        }
        
        self.connections.clear();
        self.endpoint.wait_idle().await;
        Ok(())
    }

    pub async fn connect_to_leaders(&self, leaders: Vec<(Pubkey, SocketAddr)>) -> Result<()> {
        let mut futures = FuturesUnordered::new();
        
        for (_, addr) in leaders.iter().take(16) {
            let transport = self.clone();
            let addr = *addr;
            
            futures.push(async move {
                let _ = transport.create_new_connection(addr).await;
            });
        }

        while futures.next().await.is_some() {}
        Ok(())
    }

    pub async fn submit_bundle_with_priority(
        &self,
        submission: BundleSubmission,
        endpoints: Vec<SocketAddr>,
        priority_multiplier: f64,
    ) -> Result<SubmissionResult> {
        let adjusted_submission = BundleSubmission {
            priority: (submission.priority as f64 * priority_multiplier) as u64,
            ..submission
        };

        self.submit_bundle(adjusted_submission, endpoints).await
    }

    pub async fn parallel_submit(
        &self,
        submissions: Vec<BundleSubmission>,
        endpoints: Vec<SocketAddr>,
    ) -> Vec<Result<SubmissionResult>> {
        let mut futures = FuturesUnordered::new();
        
        for submission in submissions {
            let transport = self.clone();
            let endpoints_clone = endpoints.clone();
            
            futures.push(async move {
                transport.submit_bundle(submission, endpoints_clone).await
            });
        }

        let mut results = Vec::new();
        while let Some(result) = futures.next().await {
            results.push(result);
        }
        
        results
    }

    pub fn validate_bundle(&self, submission: &BundleSubmission) -> Result<()> {
        if submission.transactions.is_empty() {
            return Err(anyhow!("Bundle contains no transactions"));
        }

        if submission.transactions.len() > 5 {
            return Err(anyhow!("Bundle exceeds maximum transaction count"));
        }

        let total_size: usize = submission.transactions.iter()
            .map(|tx| bincode::serialize(tx).map(|b| b.len()).unwrap_or(0))
            .sum();

        if total_size > MAX_BUNDLE_SIZE - 100 {
            return Err(anyhow!("Bundle size too large"));
        }

        Ok(())
    }

    pub async fn get_connection_stats(&self) -> ConnectionStats {
        let mut total_connections = 0;
        let mut healthy_connections = 0;
        let mut total_failures = 0;

        for entry in self.connections.iter() {
            let pool = entry.value();
            let connections = pool.connections.read().await;
            
            total_connections += connections.len();
            healthy_connections += connections.iter()
                .filter(|c| !c.close_reason().is_some())
                .count();
            
            total_failures += pool.failures.load(Ordering::Relaxed);
        }

        ConnectionStats {
            total_connections,
            healthy_connections,
            total_failures,
            pools: self.connections.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransportMetricsSnapshot {
    pub bundles_sent: u64,
    pub bundles_failed: u64,
    pub connections_created: u64,
    pub connections_failed: u64,
    pub avg_latency_us: u64,
    pub bytes_sent: u64,
    pub active_connections: usize,
}

#[derive(Debug)]
pub struct ConnectionStats {
    pub total_connections: usize,
    pub healthy_connections: usize,
    pub total_failures: usize,
    pub pools: usize,
}

struct NoVerifier;

impl rustls::client::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

pub struct QuicTransportBuilder {
    keypair: Option<Keypair>,
    bind_addr: Option<SocketAddr>,
}

impl QuicTransportBuilder {
    pub fn new() -> Self {
        Self {
            keypair: None,
            bind_addr: None,
        }
    }

    pub fn with_keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = Some(keypair);
        self
    }

    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    pub async fn build(self) -> Result<QuicBundleTransport> {
        let keypair = self.keypair
            .ok_or_else(|| anyhow!("Keypair required"))?;
        
        let bind_addr = self.bind_addr
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));

        QuicBundleTransport::new(keypair, bind_addr).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transport_creation() {
        let keypair = Keypair::new();
        let addr = "0.0.0.0:0".parse().unwrap();
        let transport = QuicBundleTransport::new(keypair, addr).await;
        assert!(transport.is_ok());
    }

    #[tokio::test]
    async fn test_bundle_validation() {
        let keypair = Keypair::new();
        let addr = "0.0.0.0:0".parse().unwrap();
        let transport = QuicBundleTransport::new(keypair, addr).await.unwrap();
        
        let submission = BundleSubmission {
            transactions: vec![],
            slot: 100,
            priority: 1000,
            tip_account: None,
        };
        
        let result = transport.validate_bundle(&submission);
        assert!(result.is_err());
    }
}

