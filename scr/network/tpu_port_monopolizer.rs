use std::sync::{Arc, RwLock, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::collections::{HashMap, VecDeque, BTreeMap};
use std::net::{SocketAddr, UdpSocket, IpAddr, Ipv4Addr};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Semaphore, RwLock as TokioRwLock};
use tokio::time::{interval, timeout, sleep};
use solana_client::{
    rpc_client::RpcClient,
    nonblocking::rpc_client::RpcClient as NonblockingRpcClient,
    tpu_client::{TpuClient, TpuClientConfig},
    connection_cache::{ConnectionCache, DEFAULT_TPU_CONNECTION_POOL_SIZE},
};
use solana_sdk::{
    pubkey::Pubkey,
    transaction::Transaction,
    signature::{Signature, Keypair},
    commitment_config::{CommitmentConfig, CommitmentLevel},
    clock::Slot,
    epoch_info::EpochInfo,
};
use solana_gossip::cluster_info::ClusterInfo;
use solana_streamer::socket::SocketAddrSpace;
use solana_net_utils::MINIMUM_VALIDATOR_PORT_RANGE_WIDTH;
use quinn::{ClientConfig, Endpoint, Connection as QuicConnection, SendStream};
use rustls::ClientConfig as RustlsClientConfig;
use bincode::serialize;
use dashmap::DashMap;
use rand::{thread_rng, Rng, seq::SliceRandom};
use futures::future::join_all;
use crossbeam_channel::{bounded, Sender, Receiver};

const MAX_CONNECTIONS_PER_LEADER: usize = 8;
const CONNECTION_POOL_SIZE: usize = 512;
const MAX_PARALLEL_SENDS: usize = 16;
const PORT_RANGE_START: u16 = 8000;
const PORT_RANGE_END: u16 = 10000;
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 2;
const CONNECTION_TIMEOUT_MS: u64 = 500;
const HEALTH_CHECK_INTERVAL_MS: u64 = 250;
const MAX_PACKET_SIZE: usize = 1232;
const QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS: usize = 128;
const LEADER_SCHEDULE_SLOT_OFFSET: u64 = 4;
const CONNECTION_CACHE_SIZE: usize = 4096;
const LEADER_SCHEDULE_CACHE_TTL_MS: u64 = 400;
const MAX_QUIC_CONNECTIONS_PER_PEER: usize = 8;

#[derive(Clone)]
pub struct TpuPortMonopolizer {
    rpc_client: Arc<RpcClient>,
    async_rpc_client: Arc<NonblockingRpcClient>,
    connection_cache: Arc<ConnectionCache>,
    connection_pool: Arc<DashMap<SocketAddr, ConnectionState>>,
    active_connections: Arc<RwLock<HashMap<Pubkey, Vec<SocketAddr>>>>,
    leader_schedule: Arc<TokioRwLock<LeaderScheduleCache>>,
    port_allocator: Arc<PortAllocator>,
    health_monitor: Arc<HealthMonitor>,
    metrics: Arc<Metrics>,
    identity: Arc<Keypair>,
    recent_slots: Arc<TokioRwLock<BTreeMap<Slot, Instant>>>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Clone)]
struct ConnectionState {
    endpoint: Arc<Mutex<Option<Endpoint>>>,
    quic_conn: Arc<Mutex<Option<Arc<QuicConnection>>>>,
    udp_socket: Arc<Mutex<Option<Arc<UdpSocket>>>>,
    last_used: Arc<RwLock<Instant>>,
    success_count: AtomicU64,
    failure_count: AtomicU64,
    latency_ns: AtomicU64,
    is_healthy: AtomicBool,
    addr: SocketAddr,
}

#[derive(Clone)]
struct LeaderScheduleCache {
    schedule: BTreeMap<Slot, Pubkey>,
    tpu_addresses: HashMap<Pubkey, (Vec<SocketAddr>, Vec<SocketAddr>)>,
    current_epoch: u64,
    slots_per_epoch: u64,
    last_update: Instant,
    last_slot: Slot,
}

struct PortAllocator {
    available_ports: Arc<Mutex<VecDeque<u16>>>,
    port_usage: Arc<DashMap<u16, (Instant, SocketAddr)>>,
}

struct HealthMonitor {
    connection_health: Arc<DashMap<SocketAddr, ConnectionHealth>>,
}

#[derive(Clone)]
struct ConnectionHealth {
    last_check: Instant,
    consecutive_failures: u32,
    avg_latency_ns: u64,
    success_rate: f64,
}

struct Metrics {
    total_sent: AtomicU64,
    total_confirmed: AtomicU64,
    total_failed: AtomicU64,
    avg_latency_ns: AtomicU64,
}

impl TpuPortMonopolizer {
    pub async fn new(
        rpc_url: &str,
        ws_url: &str,
        identity: Arc<Keypair>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        
        let async_rpc_client = Arc::new(NonblockingRpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let connection_cache = Arc::new(ConnectionCache::new_with_client_options(
            CONNECTION_CACHE_SIZE,
            Some(CONNECTION_POOL_SIZE),
            Some((&identity, IpAddr::V4(Ipv4Addr::UNSPECIFIED))),
            Some(MAX_QUIC_CONNECTIONS_PER_PEER),
        ));

        let epoch_info = rpc_client.get_epoch_info()?;
        let leader_schedule_cache = LeaderScheduleCache {
            schedule: BTreeMap::new(),
            tpu_addresses: HashMap::new(),
            current_epoch: epoch_info.epoch,
            slots_per_epoch: epoch_info.slots_in_epoch,
            last_update: Instant::now(),
            last_slot: epoch_info.absolute_slot,
        };

        let port_allocator = Arc::new(PortAllocator::new());
        let health_monitor = Arc::new(HealthMonitor::new());
        
        let monopolizer = Self {
            rpc_client,
            async_rpc_client,
            connection_cache,
            connection_pool: Arc::new(DashMap::new()),
            active_connections: Arc::new(RwLock::new(HashMap::new())),
            leader_schedule: Arc::new(TokioRwLock::new(leader_schedule_cache)),
            port_allocator,
            health_monitor,
            metrics: Arc::new(Metrics::new()),
            identity,
            recent_slots: Arc::new(TokioRwLock::new(BTreeMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        monopolizer.start_background_tasks().await;
        monopolizer.refresh_leader_schedule().await?;
        
        Ok(monopolizer)
    }

    pub async fn send_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let start = Instant::now();
        let signature = transaction.signatures[0];
        let wire_transaction = serialize(transaction)?;
        
        if wire_transaction.len() > MAX_PACKET_SIZE {
            return Err("Transaction too large".into());
        }

        let slot = self.get_current_slot().await?;
        let leader = self.get_slot_leader(slot + LEADER_SCHEDULE_SLOT_OFFSET).await?;
        
        let connections = self.get_leader_connections(&leader).await?;
        if connections.is_empty() {
            return Err("No TPU connections available".into());
        }

        let (tx, mut rx) = mpsc::channel(connections.len());
        let semaphore = Arc::new(Semaphore::new(MAX_PARALLEL_SENDS));

        for addr in connections {
            let semaphore = semaphore.clone();
            let tx = tx.clone();
            let wire_tx = wire_transaction.clone();
            let metrics = self.metrics.clone();
            let connection_pool = self.connection_pool.clone();
            
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await.ok()?;
                let result = timeout(
                    Duration::from_millis(CONNECTION_TIMEOUT_MS),
                    Self::send_to_address(&connection_pool, &addr, &wire_tx, &metrics)
                ).await;
                
                let send_result = match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err("Timeout".into()),
                };
                
                tx.send((addr, send_result)).await.ok();
                Some(())
            });
        }

        drop(tx);
        
        let mut retry_addrs = Vec::new();
        let mut attempts = 0;
        
        while let Some((addr, result)) = rx.recv().await {
            attempts += 1;
            match result {
                Ok(()) => {
                    self.metrics.total_confirmed.fetch_add(1, Ordering::Relaxed);
                    let latency = start.elapsed().as_nanos() as u64;
                    self.update_latency_metrics(latency);
                    
                    self.record_recent_slot(slot).await;
                    
                    return Ok(signature);
                }
                Err(_) => {
                    retry_addrs.push(addr);
                }
            }
            
            if attempts >= connections.len() {
                break;
            }
        }

        for (retry_num, addr) in retry_addrs.iter().take(MAX_RETRIES as usize).enumerate() {
            sleep(Duration::from_millis(RETRY_DELAY_MS * (retry_num as u64 + 1))).await;
            
            if let Ok(()) = timeout(
                Duration::from_millis(CONNECTION_TIMEOUT_MS),
                Self::send_to_address(&self.connection_pool, addr, &wire_transaction, &self.metrics)
            ).await.unwrap_or(Err("Retry timeout".into())) {
                self.metrics.total_confirmed.fetch_add(1, Ordering::Relaxed);
                return Ok(signature);
            }
        }

        self.metrics.total_failed.fetch_add(1, Ordering::Relaxed);
        Err("Failed to send transaction after all retries".into())
    }

    async fn send_to_address(
        connection_pool: &DashMap<SocketAddr, ConnectionState>,
        addr: &SocketAddr,
        wire_transaction: &[u8],
        metrics: &Arc<Metrics>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        metrics.total_sent.fetch_add(1, Ordering::Relaxed);
        
        let conn_state = connection_pool.get(addr)
            .ok_or("Connection not found")?;

        if !conn_state.is_healthy.load(Ordering::Relaxed) {
            return Err("Unhealthy connection".into());
        }

        let start = Instant::now();
        
        let result = if addr.port() % 2 == 0 {
            Self::send_quic(&conn_state, wire_transaction).await
        } else {
            Self::send_udp(&conn_state, wire_transaction).await
        };

        match &result {
            Ok(()) => {
                let latency = start.elapsed().as_nanos() as u64;
                conn_state.latency_ns.store(latency, Ordering::Relaxed);
                conn_state.success_count.fetch_add(1, Ordering::Relaxed);
                *conn_state.last_used.write().unwrap() = Instant::now();
            }
            Err(_) => {
                conn_state.failure_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        result
    }

    async fn send_quic(
        conn_state: &ConnectionState,
        wire_transaction: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let quic_conn = conn_state.quic_conn.lock().await;
        if let Some(conn) = quic_conn.as_ref() {
            if conn.close_reason().is_some() {
                return Err("QUIC connection closed".into());
            }
            
            let mut stream = conn.open_uni().await?;
            stream.write_all(wire_transaction).await?;
            stream.finish().await?;
            Ok(())
        } else {
            Err("No QUIC connection".into())
        }
    }

    async fn send_udp(
        conn_state: &ConnectionState,
        wire_transaction: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let udp_socket = conn_state.udp_socket.lock().await;
        if let Some(socket) = udp_socket.as_ref() {
            match socket.send(wire_transaction) {
                Ok(sent) if sent == wire_transaction.len() => Ok(()),
                Ok(_) => Err("Partial send".into()),
                Err(e) => Err(e.into()),
            }
        } else {
            Err("No UDP socket".into())
        }
    }

    async fn get_current_slot(&self) -> Result<Slot, Box<dyn std::error::Error>> {
        Ok(self.async_rpc_client.get_slot().await?)
    }

    async fn get_slot_leader(&self, slot: Slot) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let schedule = self.leader_schedule.read().await;
        
        if let Some(leader) = schedule.schedule.get(&slot) {
            return Ok(*leader);
        }
        
        drop(schedule);
        self.refresh_leader_schedule().await?;
        
                let schedule = self.leader_schedule.read().await;
        schedule.schedule.get(&slot)
            .copied()
            .ok_or_else(|| format!("No leader found for slot {}", slot).into())
    }

    async fn get_leader_connections(
        &self,
        leader_pubkey: &Pubkey,
    ) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
        let mut connections = {
            let active = self.active_connections.read().unwrap();
            active.get(leader_pubkey).cloned().unwrap_or_default()
        };

        if connections.is_empty() || self.should_refresh_connections() {
            connections = self.establish_leader_connections(leader_pubkey).await?;
            self.active_connections.write().unwrap()
                .insert(*leader_pubkey, connections.clone());
        }

        self.prioritize_connections(&mut connections).await;
        Ok(connections)
    }

    async fn establish_leader_connections(
        &self,
        leader_pubkey: &Pubkey,
    ) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
        let (tpu_addrs, tpu_fwd_addrs) = self.get_leader_tpu_addresses(leader_pubkey).await?;
        let mut established = Vec::new();
        
        let all_addrs: Vec<_> = tpu_addrs.into_iter()
            .chain(tpu_fwd_addrs.into_iter())
            .collect();

        for base_addr in all_addrs {
            let ports = self.port_allocator.allocate_ports(MAX_CONNECTIONS_PER_LEADER / 2).await;
            
            for port in ports {
                let addr = SocketAddr::new(base_addr.ip(), port);
                
                if self.connection_pool.contains_key(&addr) {
                    if let Some(conn) = self.connection_pool.get(&addr) {
                        if conn.is_healthy.load(Ordering::Relaxed) {
                            established.push(addr);
                            continue;
                        }
                    }
                }

                match timeout(
                    Duration::from_millis(CONNECTION_TIMEOUT_MS),
                    self.create_connection(addr)
                ).await {
                    Ok(Ok(conn_state)) => {
                        self.connection_pool.insert(addr, conn_state);
                        established.push(addr);
                    }
                    _ => {
                        self.port_allocator.release_port(port).await;
                    }
                }

                if established.len() >= MAX_CONNECTIONS_PER_LEADER {
                    break;
                }
            }
        }

        Ok(established)
    }

    async fn create_connection(
        &self,
        addr: SocketAddr,
    ) -> Result<ConnectionState, Box<dyn std::error::Error>> {
        let conn_state = ConnectionState {
            endpoint: Arc::new(Mutex::new(None)),
            quic_conn: Arc::new(Mutex::new(None)),
            udp_socket: Arc::new(Mutex::new(None)),
            last_used: Arc::new(RwLock::new(Instant::now())),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            latency_ns: AtomicU64::new(0),
            is_healthy: AtomicBool::new(true),
            addr,
        };

        if addr.port() % 2 == 0 {
            self.create_quic_connection(&conn_state, addr).await?;
        } else {
            self.create_udp_connection(&conn_state, addr).await?;
        }

        Ok(conn_state)
    }

    async fn create_quic_connection(
        &self,
        conn_state: &ConnectionState,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client_config = self.create_quic_client_config();
        let bind_addr = if addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        
        let mut endpoint = Endpoint::client(bind_addr.parse()?)?;
        endpoint.set_default_client_config(client_config);

        let connection = endpoint.connect(addr, "localhost")?.await?;
        
        connection.set_receive_window(32 * 1024 * 1024_u32.into());
        
        *conn_state.endpoint.lock().await = Some(endpoint);
        *conn_state.quic_conn.lock().await = Some(Arc::new(connection));
        
        Ok(())
    }

    async fn create_udp_connection(
        &self,
        conn_state: &ConnectionState,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bind_addr = if addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        
        let socket = UdpSocket::bind(bind_addr)?;
        socket.set_nonblocking(true)?;
        socket.connect(addr)?;
        
        socket.set_send_buffer_size(8 * 1024 * 1024).ok();
        socket.set_recv_buffer_size(8 * 1024 * 1024).ok();
        
        *conn_state.udp_socket.lock().await = Some(Arc::new(socket));
        Ok(())
    }

    fn create_quic_client_config(&self) -> ClientConfig {
        let crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth();

        let mut config = ClientConfig::new(Arc::new(crypto));
        let transport_config = Arc::get_mut(&mut config.transport).unwrap();
        
        transport_config.max_concurrent_bidi_streams(0_u8.into());
        transport_config.max_concurrent_uni_streams(QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS.try_into().unwrap());
        transport_config.keep_alive_interval(Some(Duration::from_secs(1)));
        transport_config.stream_receive_window(8 * 1024 * 1024_u32.into());
        transport_config.receive_window(32 * 1024 * 1024_u32.into());
        transport_config.send_window(32 * 1024 * 1024);
        transport_config.initial_mtu(MAX_PACKET_SIZE as u16);
        transport_config.datagram_receive_buffer_size(Some(64 * 1024 * 1024));
        transport_config.datagram_send_buffer_size(64 * 1024 * 1024);
        transport_config.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
        
        config
    }

    async fn get_leader_tpu_addresses(
        &self,
        leader_pubkey: &Pubkey,
    ) -> Result<(Vec<SocketAddr>, Vec<SocketAddr>), Box<dyn std::error::Error>> {
        let should_refresh = {
            let cache = self.leader_schedule.read().await;
            cache.last_update.elapsed() > Duration::from_millis(LEADER_SCHEDULE_CACHE_TTL_MS)
        };

        if should_refresh {
            self.refresh_leader_schedule().await?;
        }

        let cache = self.leader_schedule.read().await;
        cache.tpu_addresses.get(leader_pubkey)
            .cloned()
            .ok_or_else(|| format!("TPU addresses not found for leader {}", leader_pubkey).into())
    }

    async fn refresh_leader_schedule(&self) -> Result<(), Box<dyn std::error::Error>> {
        let slot = self.async_rpc_client.get_slot().await?;
        let epoch_info = self.async_rpc_client.get_epoch_info().await?;
        let epoch = epoch_info.epoch;
        let slots_in_epoch = epoch_info.slots_in_epoch;
        
        let leader_schedule = self.async_rpc_client
            .get_leader_schedule_with_commitment(
                Some(slot),
                CommitmentConfig::finalized()
            ).await?
            .ok_or("Failed to get leader schedule")?;

        let mut schedule_map = BTreeMap::new();
        let start_slot = epoch * slots_in_epoch;
        
        for (pubkey_str, slots) in leader_schedule.iter() {
            let pubkey = pubkey_str.parse::<Pubkey>()?;
            for &leader_slot in slots {
                schedule_map.insert(start_slot + leader_slot as u64, pubkey);
            }
        }

        let cluster_nodes = self.async_rpc_client.get_cluster_nodes().await?;
        let mut tpu_addresses = HashMap::new();
        
        for node in cluster_nodes {
            if let (Some(tpu), Some(tpu_fwd)) = (node.tpu, node.tpu_forwards) {
                tpu_addresses.insert(
                    node.pubkey.parse::<Pubkey>()?, 
                    (vec![tpu], vec![tpu_fwd])
                );
            }
        }

        let mut cache = self.leader_schedule.write().await;
        cache.schedule = schedule_map;
        cache.tpu_addresses = tpu_addresses;
        cache.current_epoch = epoch;
        cache.slots_per_epoch = slots_in_epoch;
        cache.last_update = Instant::now();
        cache.last_slot = slot;

        Ok(())
    }

    async fn prioritize_connections(&self, connections: &mut Vec<SocketAddr>) {
        let mut scored_connections: Vec<(SocketAddr, f64)> = connections
            .iter()
            .filter_map(|addr| {
                self.connection_pool.get(addr).map(|conn| {
                    let success = conn.success_count.load(Ordering::Relaxed) as f64;
                    let failure = conn.failure_count.load(Ordering::Relaxed) as f64;
                    let total = success + failure;
                    
                    if total == 0.0 {
                        (*addr, 0.5)
                    } else {
                        let success_rate = success / total;
                        let latency = conn.latency_ns.load(Ordering::Relaxed) as f64;
                        let latency_score = 1.0 / (1.0 + latency / 1_000_000.0);
                        let recency_score = {
                            let last_used = conn.last_used.read().unwrap();
                            let age_ms = last_used.elapsed().as_millis() as f64;
                            (1000.0 / (1000.0 + age_ms)).max(0.1)
                        };
                        
                        let health_bonus = if conn.is_healthy.load(Ordering::Relaxed) { 0.1 } else { 0.0 };
                        let score = success_rate * 0.4 + latency_score * 0.4 + recency_score * 0.1 + health_bonus;
                        (*addr, score)
                    }
                })
            })
            .collect();

        scored_connections.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        let mut rng = thread_rng();
        let top_count = connections.len().min(MAX_CONNECTIONS_PER_LEADER);
        let elite_count = top_count * 3 / 4;
        let random_count = top_count - elite_count;
        
        let mut selected = Vec::new();
        
        for (addr, _) in scored_connections.iter().take(elite_count) {
            selected.push(*addr);
        }
        
        if random_count > 0 && scored_connections.len() > elite_count {
            let remaining: Vec<_> = scored_connections[elite_count..]
                .iter()
                .map(|(addr, _)| *addr)
                .collect();
            
            let random_selection: Vec<_> = remaining
                .choose_multiple(&mut rng, random_count)
                .cloned()
                .collect();
            
            selected.extend(random_selection);
        }
        
        *connections = selected;
    }

    fn should_refresh_connections(&self) -> bool {
        thread_rng().gen_bool(0.02)
    }

    async fn record_recent_slot(&self, slot: Slot) {
        let mut recent_slots = self.recent_slots.write().await;
        recent_slots.insert(slot, Instant::now());
        
        let cutoff = Instant::now() - Duration::from_secs(60);
        recent_slots.retain(|_, instant| *instant > cutoff);
    }

    async fn start_background_tasks(&self) {
        let health_monitor = self.health_monitor.clone();
        let connection_pool = self.connection_pool.clone();
        let metrics = self.metrics.clone();
        let shutdown = self.shutdown.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS));
            while !shutdown.load(Ordering::Relaxed) {
                interval.tick().await;
                Self::health_check_task(&health_monitor, &connection_pool, &metrics).await;
            }
        });

        let connection_pool = self.connection_pool.clone();
        let shutdown = self.shutdown.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            while !shutdown.load(Ordering::Relaxed) {
                interval.tick().await;
                Self::cleanup_stale_connections(&connection_pool).await;
            }
        });

        let port_allocator = self.port_allocator.clone();
        let shutdown = self.shutdown.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));
            while !shutdown.load(Ordering::Relaxed) {
                interval.tick().await;
                port_allocator.cleanup_expired_ports().await;
            }
        });

        let leader_schedule = self.leader_schedule.clone();
        let async_rpc = self.async_rpc_client.clone();
        let shutdown = self.shutdown.clone();
        
                tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(LEADER_SCHEDULE_CACHE_TTL_MS));
            while !shutdown.load(Ordering::Relaxed) {
                interval.tick().await;
                if let Ok(slot) = async_rpc.get_slot().await {
                    let mut cache = leader_schedule.write().await;
                    cache.last_slot = slot;
                }
            }
        });
    }

    async fn health_check_task(
        health_monitor: &Arc<HealthMonitor>,
        connection_pool: &DashMap<SocketAddr, ConnectionState>,
        metrics: &Arc<Metrics>,
    ) {
        let mut total_latency = 0u64;
        let mut count = 0u64;
        let mut to_mark_unhealthy = Vec::new();

        for entry in connection_pool.iter() {
            let addr = *entry.key();
            let conn_state = entry.value();
            
            let success = conn_state.success_count.load(Ordering::Relaxed) as f64;
            let failure = conn_state.failure_count.load(Ordering::Relaxed) as f64;
            let total = success + failure;
            
            let health = if total < 10.0 {
                ConnectionHealth {
                    last_check: Instant::now(),
                    consecutive_failures: 0,
                    avg_latency_ns: conn_state.latency_ns.load(Ordering::Relaxed),
                    success_rate: if total > 0.0 { success / total } else { 1.0 },
                }
            } else {
                let success_rate = success / total;
                let consecutive_failures = if success_rate < 0.5 {
                    health_monitor.connection_health
                        .get(&addr)
                        .map(|h| h.consecutive_failures + 1)
                        .unwrap_or(1)
                } else {
                    0
                };
                
                let is_healthy = success_rate > 0.3 && 
                                consecutive_failures < 5 && 
                                conn_state.latency_ns.load(Ordering::Relaxed) < 100_000_000;
                
                if !is_healthy && conn_state.is_healthy.load(Ordering::Relaxed) {
                    to_mark_unhealthy.push(addr);
                }
                
                conn_state.is_healthy.store(is_healthy, Ordering::Relaxed);
                
                ConnectionHealth {
                    last_check: Instant::now(),
                    consecutive_failures,
                    avg_latency_ns: conn_state.latency_ns.load(Ordering::Relaxed),
                    success_rate,
                }
            };

            if health.success_rate > 0.0 && health.avg_latency_ns < 1_000_000_000 {
                total_latency += health.avg_latency_ns;
                count += 1;
            }

            health_monitor.connection_health.insert(addr, health);
        }

        if count > 0 {
            let avg = total_latency / count;
            let current_avg = metrics.avg_latency_ns.load(Ordering::Relaxed);
            let new_avg = (current_avg * 7 + avg * 3) / 10;
            metrics.avg_latency_ns.store(new_avg, Ordering::Relaxed);
        }

        for addr in to_mark_unhealthy {
            if let Some(mut entry) = connection_pool.get_mut(&addr) {
                entry.value_mut().is_healthy.store(false, Ordering::Relaxed);
            }
        }
    }

    async fn cleanup_stale_connections(connection_pool: &DashMap<SocketAddr, ConnectionState>) {
        let stale_threshold = Duration::from_secs(120);
        let unhealthy_threshold = Duration::from_secs(30);
        let mut to_remove = Vec::new();

        for entry in connection_pool.iter() {
            let last_used = entry.value().last_used.read().unwrap();
            let is_healthy = entry.value().is_healthy.load(Ordering::Relaxed);
            
            let should_remove = if is_healthy {
                last_used.elapsed() > stale_threshold
            } else {
                last_used.elapsed() > unhealthy_threshold
            };
            
            if should_remove {
                to_remove.push(*entry.key());
            }
        }

        for addr in to_remove {
            if let Some((_, conn_state)) = connection_pool.remove(&addr) {
                if let Some(endpoint) = conn_state.endpoint.lock().await.take() {
                    endpoint.close(0u32.into(), b"stale");
                }
            }
        }
    }

    fn update_latency_metrics(&self, latency_ns: u64) {
        let current = self.metrics.avg_latency_ns.load(Ordering::Relaxed);
        let new_avg = if current == 0 {
            latency_ns
        } else {
            (current * 95 + latency_ns * 5) / 100
        };
        self.metrics.avg_latency_ns.store(new_avg, Ordering::Relaxed);
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        
        sleep(Duration::from_millis(100)).await;
        
        let connections: Vec<_> = self.connection_pool.iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        
        for (addr, conn_state) in connections {
            if let Some(endpoint) = conn_state.endpoint.lock().await.take() {
                endpoint.close(0u32.into(), b"shutdown");
            }
            self.connection_pool.remove(&addr);
        }
        
        self.active_connections.write().unwrap().clear();
    }

    pub fn get_metrics(&self) -> (u64, u64, u64, u64) {
        (
            self.metrics.total_sent.load(Ordering::Relaxed),
            self.metrics.total_confirmed.load(Ordering::Relaxed),
            self.metrics.total_failed.load(Ordering::Relaxed),
            self.metrics.avg_latency_ns.load(Ordering::Relaxed),
        )
    }

    pub fn get_connection_count(&self) -> usize {
        self.connection_pool.len()
    }

    pub fn get_healthy_connection_count(&self) -> usize {
        self.connection_pool.iter()
            .filter(|entry| entry.value().is_healthy.load(Ordering::Relaxed))
            .count()
    }
}

impl PortAllocator {
    fn new() -> Self {
        let mut available_ports = VecDeque::new();
        for port in (PORT_RANGE_START..=PORT_RANGE_END).step_by(2) {
            available_ports.push_back(port);
        }
        
        let mut rng = thread_rng();
        let ports_vec: Vec<_> = available_ports.into_iter().collect();
        let mut shuffled_ports = ports_vec;
        shuffled_ports.shuffle(&mut rng);
        
        Self {
            available_ports: Arc::new(Mutex::new(shuffled_ports.into_iter().collect())),
            port_usage: Arc::new(DashMap::new()),
        }
    }

    async fn allocate_ports(&self, count: usize) -> Vec<u16> {
        let mut ports = Vec::new();
        let mut available = self.available_ports.lock().await;
        let mut rng = thread_rng();
        
        let take_count = count.min(available.len() / 4);
        
        for _ in 0..take_count {
            if let Some(port) = available.pop_front() {
                self.port_usage.insert(port, (Instant::now(), SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)));
                ports.push(port);
                available.push_back(port);
            }
        }
        
        while ports.len() < count {
            let port = rng.gen_range(PORT_RANGE_START..=PORT_RANGE_END);
            if !ports.contains(&port) {
                ports.push(port);
            }
        }
        
        ports
    }

    async fn release_port(&self, port: u16) {
        self.port_usage.remove(&port);
    }

    async fn cleanup_expired_ports(&self) {
        let expiry = Duration::from_secs(300);
        let mut expired = Vec::new();
        
        for entry in self.port_usage.iter() {
            if entry.value().0.elapsed() > expiry {
                expired.push(*entry.key());
            }
        }
        
        for port in expired {
            self.port_usage.remove(&port);
        }
    }
}

impl HealthMonitor {
    fn new() -> Self {
        Self {
            connection_health: Arc::new(DashMap::new()),
        }
    }

    pub fn get_connection_health(&self, addr: &SocketAddr) -> Option<ConnectionHealth> {
        self.connection_health.get(addr).map(|h| h.clone())
    }

    pub fn get_unhealthy_connections(&self) -> Vec<SocketAddr> {
        self.connection_health.iter()
            .filter(|entry| entry.value().success_rate < 0.5 || entry.value().consecutive_failures > 3)
            .map(|entry| *entry.key())
            .collect()
    }
}

impl Metrics {
    fn new() -> Self {
        Self {
            total_sent: AtomicU64::new(0),
            total_confirmed: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            avg_latency_ns: AtomicU64::new(0),
        }
    }

    pub fn success_rate(&self) -> f64 {
        let sent = self.total_sent.load(Ordering::Relaxed) as f64;
        let confirmed = self.total_confirmed.load(Ordering::Relaxed) as f64;
        if sent > 0.0 {
            confirmed / sent
        } else {
            0.0
        }
    }
}

struct SkipServerVerification;

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

impl Drop for TpuPortMonopolizer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

