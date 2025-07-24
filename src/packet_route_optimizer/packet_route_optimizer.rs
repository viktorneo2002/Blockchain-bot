use std::collections::{HashMap, HashSet, BinaryHeap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::cmp::Ordering;
use std::net::SocketAddr;
use tokio::sync::{mpsc, RwLock as AsyncRwLock};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::clock::Slot;
use solana_sdk::signature::Signature;
use solana_gossip::cluster_info::ClusterInfo;
use solana_streamer::socket::SocketAddrSpace;
use solana_client::tpu_connection::TpuConnection;
use solana_client::connection_cache::{ConnectionCache, Protocol};
use solana_metrics::datapoint_info;
use dashmap::DashMap;
use arc_swap::ArcSwap;

const MAX_ROUTES_PER_LEADER: usize = 8;
const ROUTE_SCORE_DECAY_FACTOR: f64 = 0.95;
const MIN_ROUTE_SCORE: f64 = 0.1;
const ROUTE_PROBE_INTERVAL_MS: u64 = 100;
const LEADER_SCHEDULE_REFRESH_SLOTS: u64 = 10;
const MAX_PACKET_BATCH_SIZE: usize = 64;
const ROUTE_LATENCY_WEIGHT: f64 = 0.4;
const ROUTE_SUCCESS_WEIGHT: f64 = 0.35;
const ROUTE_STAKE_WEIGHT: f64 = 0.25;
const JITO_BLOCK_ENGINE_URL: &str = "https://mainnet.block-engine.jito.wtf";
const MAX_CONCURRENT_ROUTES: usize = 16;
const ROUTE_TIMEOUT_MS: u64 = 150;
const CRITICAL_LATENCY_THRESHOLD_MS: u64 = 50;

#[derive(Clone, Debug)]
pub struct RouteMetrics {
    pub latency_ms: f64,
    pub success_rate: f64,
    pub packets_sent: u64,
    pub packets_confirmed: u64,
    pub last_update: Instant,
    pub consecutive_failures: u32,
}

#[derive(Clone, Debug)]
pub struct RouteNode {
    pub address: SocketAddr,
    pub pubkey: Pubkey,
    pub stake: u64,
    pub is_leader: bool,
    pub is_jito: bool,
    pub tpu_forwards: Vec<SocketAddr>,
    pub metrics: Arc<RwLock<RouteMetrics>>,
}

#[derive(Clone, Debug)]
pub struct Route {
    pub id: u64,
    pub nodes: Vec<RouteNode>,
    pub score: f64,
    pub priority: RoutePriority,
    pub last_used: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutePriority {
    Critical,
    High,
    Normal,
    Low,
}

impl Ord for Route {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.partial_cmp(&other.score).unwrap_or(Ordering::Equal)
            .then_with(|| match (&self.priority, &other.priority) {
                (RoutePriority::Critical, RoutePriority::Critical) => Ordering::Equal,
                (RoutePriority::Critical, _) => Ordering::Greater,
                (_, RoutePriority::Critical) => Ordering::Less,
                (RoutePriority::High, RoutePriority::High) => Ordering::Equal,
                (RoutePriority::High, _) => Ordering::Greater,
                (_, RoutePriority::High) => Ordering::Less,
                (RoutePriority::Normal, RoutePriority::Normal) => Ordering::Equal,
                (RoutePriority::Normal, RoutePriority::Low) => Ordering::Greater,
                (RoutePriority::Low, RoutePriority::Normal) => Ordering::Less,
                (RoutePriority::Low, RoutePriority::Low) => Ordering::Equal,
            })
    }
}

impl PartialOrd for Route {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Route {}

pub struct PacketRouteOptimizer {
    routes: Arc<DashMap<Pubkey, Vec<Route>>>,
    leader_schedule: Arc<ArcSwap<HashMap<Slot, Pubkey>>>,
    connection_cache: Arc<ConnectionCache>,
    route_scores: Arc<DashMap<u64, f64>>,
    active_probes: Arc<AsyncRwLock<HashSet<u64>>>,
    metrics_aggregator: Arc<RwLock<HashMap<u64, RouteMetrics>>>,
    jito_client: Option<Arc<jito_protos::searcher::searcher_client::SearcherClient<tonic::transport::Channel>>>,
    route_id_counter: Arc<RwLock<u64>>,
    cluster_nodes: Arc<DashMap<Pubkey, RouteNode>>,
}

impl PacketRouteOptimizer {
    pub async fn new(
        cluster_info: Arc<ClusterInfo>,
        connection_cache: Arc<ConnectionCache>,
    ) -> anyhow::Result<Self> {
        let jito_client = Self::init_jito_client().await.ok();
        
        let optimizer = Self {
            routes: Arc::new(DashMap::new()),
            leader_schedule: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            connection_cache,
            route_scores: Arc::new(DashMap::new()),
            active_probes: Arc::new(AsyncRwLock::new(HashSet::new())),
            metrics_aggregator: Arc::new(RwLock::new(HashMap::new())),
            jito_client,
            route_id_counter: Arc::new(RwLock::new(0)),
            cluster_nodes: Arc::new(DashMap::new()),
        };

        optimizer.initialize_cluster_topology(cluster_info).await?;
        optimizer.start_background_tasks();
        
        Ok(optimizer)
    }

    async fn init_jito_client() -> anyhow::Result<Arc<jito_protos::searcher::searcher_client::SearcherClient<tonic::transport::Channel>>> {
        let channel = tonic::transport::Channel::from_static(JITO_BLOCK_ENGINE_URL)
            .timeout(Duration::from_millis(ROUTE_TIMEOUT_MS))
            .connect()
            .await?;
        
        Ok(Arc::new(jito_protos::searcher::searcher_client::SearcherClient::new(channel)))
    }

    async fn initialize_cluster_topology(&self, cluster_info: Arc<ClusterInfo>) -> anyhow::Result<()> {
        let nodes = cluster_info.all_peers();
        
        for (pubkey, peer_info) in nodes {
            let stake = self.get_node_stake(&pubkey).await.unwrap_or(0);
            let tpu_forwards = self.get_tpu_forwards(&pubkey, &peer_info).await;
            
            let node = RouteNode {
                address: peer_info.tpu,
                pubkey,
                stake,
                is_leader: false,
                is_jito: self.is_jito_validator(&pubkey),
                tpu_forwards,
                metrics: Arc::new(RwLock::new(RouteMetrics {
                    latency_ms: 100.0,
                    success_rate: 0.5,
                    packets_sent: 0,
                    packets_confirmed: 0,
                    last_update: Instant::now(),
                    consecutive_failures: 0,
                })),
            };
            
            self.cluster_nodes.insert(pubkey, node);
        }
        
        Ok(())
    }

    pub async fn optimize_routes(&self, leader: &Pubkey, packet_priority: RoutePriority) -> Vec<Route> {
        let mut optimal_routes = Vec::new();
        
        if let Some(existing_routes) = self.routes.get(leader) {
            let mut scored_routes: BinaryHeap<Route> = existing_routes
                .iter()
                .filter(|r| self.is_route_healthy(r))
                .cloned()
                .collect();
            
            while optimal_routes.len() < MAX_ROUTES_PER_LEADER && !scored_routes.is_empty() {
                if let Some(route) = scored_routes.pop() {
                    optimal_routes.push(route);
                }
            }
        }
        
        if optimal_routes.len() < MAX_ROUTES_PER_LEADER {
            let new_routes = self.generate_new_routes(leader, packet_priority).await;
            optimal_routes.extend(new_routes.into_iter().take(MAX_ROUTES_PER_LEADER - optimal_routes.len()));
        }
        
        self.update_route_cache(leader, &optimal_routes);
        optimal_routes
    }

    async fn generate_new_routes(&self, leader: &Pubkey, priority: RoutePriority) -> Vec<Route> {
        let mut routes = Vec::new();
        
        if let Some(leader_node) = self.cluster_nodes.get(leader) {
            let direct_route = self.create_direct_route(&leader_node, priority.clone());
            routes.push(direct_route);
            
            if self.jito_client.is_some() && self.is_jito_validator(leader) {
                if let Ok(jito_route) = self.create_jito_route(leader, priority.clone()).await {
                    routes.push(jito_route);
                }
            }
            
            let relay_routes = self.create_relay_routes(&leader_node, priority.clone());
            routes.extend(relay_routes);
            
            let multi_path_routes = self.create_multi_path_routes(&leader_node, priority);
            routes.extend(multi_path_routes);
        }
        
        routes.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        routes.truncate(MAX_ROUTES_PER_LEADER);
        routes
    }

    fn create_direct_route(&self, leader_node: &RouteNode, priority: RoutePriority) -> Route {
        let route_id = self.generate_route_id();
        let score = self.calculate_initial_score(leader_node, true);
        
        Route {
            id: route_id,
            nodes: vec![leader_node.clone()],
            score,
            priority,
            last_used: Instant::now(),
        }
    }

    async fn create_jito_route(&self, leader: &Pubkey, priority: RoutePriority) -> anyhow::Result<Route> {
        let route_id = self.generate_route_id();
        let jito_node = RouteNode {
            address: JITO_BLOCK_ENGINE_URL.parse::<SocketAddr>()?,
            pubkey: *leader,
            stake: u64::MAX,
            is_leader: true,
            is_jito: true,
            tpu_forwards: vec![],
            metrics: Arc::new(RwLock::new(RouteMetrics {
                latency_ms: 30.0,
                success_rate: 0.95,
                packets_sent: 0,
                packets_confirmed: 0,
                last_update: Instant::now(),
                consecutive_failures: 0,
            })),
        };
        
        Ok(Route {
            id: route_id,
            nodes: vec![jito_node],
            score: 1.0,
            priority,
            last_used: Instant::now(),
        })
    }

    fn create_relay_routes(&self, leader_node: &RouteNode, priority: RoutePriority) -> Vec<Route> {
        let mut routes = Vec::new();
        let high_stake_nodes: Vec<_> = self.cluster_nodes.iter()
            .filter(|n| n.key() != &leader_node.pubkey && n.value().stake > 1_000_000)
            .collect();
        
        for relay in high_stake_nodes.iter().take(3) {
            let route_id = self.generate_route_id();
            let nodes = vec![relay.value().clone(), leader_node.clone()];
            let score = self.calculate_multi_hop_score(&nodes);
            
            routes.push(Route {
                id: route_id,
                nodes,
                score,
                priority: priority.clone(),
                last_used: Instant::now(),
            });
        }
        
        routes
    }

    fn create_multi_path_routes(&self, leader_node: &RouteNode, priority: RoutePriority) -> Vec<Route> {
        let mut routes = Vec::new();
        
        for forward_addr in &leader_node.tpu_forwards {
            let route_id = self.generate_route_id();
            let forward_node = RouteNode {
                address: *forward_addr,
                pubkey: leader_node.pubkey,
                stake: leader_node.stake,
                is_leader: true,
                is_jito: false,
                tpu_forwards: vec![],
                metrics: leader_node.metrics.clone(),
            };
            
            routes.push(Route {
                id: route_id,
                nodes: vec![forward_node],
                score: self.calculate_initial_score(leader_node, false) * 0.9,
                priority: priority.clone(),
                last_used: Instant::now(),
            });
        }
        
        routes
    }

    pub async fn send_packet_batch(
        &self,
        packets: Vec<Vec<u8>>,
        routes: &[Route],
        priority: RoutePriority,
    ) -> anyhow::Result<Vec<(Signature, bool)>> {
        let mut results = Vec::new();
        let batch_size = (packets.len() / routes.len()).max(1);
        
        let mut handles = Vec::new();
        
        for (i, route) in routes.iter().enumerate() {
            let start_idx = i * batch_size;
            let end_idx = ((i + 1) * batch_size).min(packets.len());
            
            if start_idx >= packets.len() {
                break;
            }
            
            let batch = packets[start_idx..end_idx].to_vec();
            let route_clone = route.clone();
            let connection_cache = self.connection_cache.clone();
            
                        let handle = tokio::spawn(async move {
                let mut batch_results = Vec::new();
                let start_time = Instant::now();
                
                for packet in batch {
                    match Self::send_via_route(&route_clone, &packet, &connection_cache).await {
                        Ok(sig) => {
                            batch_results.push((sig, true));
                        }
                        Err(e) => {
                            let sig = Signature::default();
                            batch_results.push((sig, false));
                        }
                    }
                }
                
                let latency = start_time.elapsed().as_millis() as f64;
                (route_clone.id, batch_results, latency)
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            if let Ok((route_id, batch_results, latency)) = handle.await? {
                self.update_route_metrics(route_id, batch_results.len(), 
                    batch_results.iter().filter(|(_, success)| *success).count(), latency).await;
                results.extend(batch_results);
            }
        }
        
        Ok(results)
    }

    async fn send_via_route(
        route: &Route,
        packet: &[u8],
        connection_cache: &Arc<ConnectionCache>,
    ) -> anyhow::Result<Signature> {
        let timeout = Duration::from_millis(ROUTE_TIMEOUT_MS);
        
        for (i, node) in route.nodes.iter().enumerate() {
            if node.is_jito {
                continue;
            }
            
            let connection = connection_cache.get_connection(&node.address);
            let send_result = tokio::time::timeout(
                timeout,
                connection.send_wire_transaction(packet)
            ).await;
            
            match send_result {
                Ok(Ok(_)) => {
                    if i == route.nodes.len() - 1 {
                        let sig = Self::extract_signature_from_packet(packet)?;
                        return Ok(sig);
                    }
                }
                Ok(Err(e)) => {
                    if i == route.nodes.len() - 1 {
                        return Err(anyhow::anyhow!("Failed to send packet: {}", e));
                    }
                }
                Err(_) => {
                    if i == route.nodes.len() - 1 {
                        return Err(anyhow::anyhow!("Packet send timeout"));
                    }
                }
            }
        }
        
        Err(anyhow::anyhow!("Route exhausted without success"))
    }

    fn extract_signature_from_packet(packet: &[u8]) -> anyhow::Result<Signature> {
        if packet.len() < 64 {
            return Err(anyhow::anyhow!("Invalid packet size"));
        }
        
        let sig_bytes: [u8; 64] = packet[1..65].try_into()
            .map_err(|_| anyhow::anyhow!("Failed to extract signature"))?;
        
        Ok(Signature::from(sig_bytes))
    }

    async fn update_route_metrics(&self, route_id: u64, sent: usize, confirmed: usize, latency: f64) {
        if let Some(mut score_entry) = self.route_scores.get_mut(&route_id) {
            let success_rate = if sent > 0 { confirmed as f64 / sent as f64 } else { 0.0 };
            
            let latency_score = (1.0 - (latency / 1000.0).min(1.0)).max(0.0);
            let success_score = success_rate;
            
            let new_score = (latency_score * ROUTE_LATENCY_WEIGHT) +
                           (success_score * ROUTE_SUCCESS_WEIGHT) +
                           (*score_entry * ROUTE_STAKE_WEIGHT);
            
            *score_entry = new_score.max(MIN_ROUTE_SCORE);
        }
        
        if let Ok(mut metrics) = self.metrics_aggregator.write() {
            metrics.entry(route_id).and_modify(|m| {
                m.packets_sent += sent as u64;
                m.packets_confirmed += confirmed as u64;
                m.latency_ms = (m.latency_ms * 0.9) + (latency * 0.1);
                m.success_rate = m.packets_confirmed as f64 / m.packets_sent.max(1) as f64;
                m.last_update = Instant::now();
                
                if confirmed == 0 && sent > 0 {
                    m.consecutive_failures += 1;
                } else {
                    m.consecutive_failures = 0;
                }
            });
        }
        
        datapoint_info!(
            "route_metrics",
            ("route_id", route_id, i64),
            ("sent", sent, i64),
            ("confirmed", confirmed, i64),
            ("latency_ms", latency, f64)
        );
    }

    fn is_route_healthy(&self, route: &Route) -> bool {
        if route.score < MIN_ROUTE_SCORE {
            return false;
        }
        
        for node in &route.nodes {
            if let Ok(metrics) = node.metrics.read() {
                if metrics.consecutive_failures > 5 {
                    return false;
                }
                
                if metrics.last_update.elapsed() > Duration::from_secs(30) {
                    return false;
                }
                
                if metrics.latency_ms > CRITICAL_LATENCY_THRESHOLD_MS as f64 * 2.0 {
                    return false;
                }
            }
        }
        
        true
    }

    async fn get_node_stake(&self, pubkey: &Pubkey) -> anyhow::Result<u64> {
        let stakes = vec![
            (Pubkey::from_str("7Np41oeYqPefeNQEHSv1UDhYrehxin3NStELsSKCT4K2").unwrap(), 15_000_000),
            (Pubkey::from_str("GdnSyH3YtwcxFvQrVVJMm1JhTS4QVX7MFsX56uJLUfiZ").unwrap(), 12_000_000),
            (Pubkey::from_str("CiDwVBFgWV9E5MvXWoLgnEgn2hK7rJikbvfWavzAQz3").unwrap(), 10_000_000),
        ];
        
        Ok(stakes.iter()
            .find(|(k, _)| k == pubkey)
            .map(|(_, stake)| *stake)
            .unwrap_or(1_000_000))
    }

    async fn get_tpu_forwards(&self, pubkey: &Pubkey, peer_info: &ContactInfo) -> Vec<SocketAddr> {
        let mut forwards = Vec::new();
        
        if let Some(tpu_forward) = peer_info.tpu_forwards {
            forwards.push(tpu_forward);
        }
        
        if forwards.is_empty() && peer_info.tpu != peer_info.tpu_forwards.unwrap_or(peer_info.tpu) {
            let alt_port = peer_info.tpu.port() + 2;
            forwards.push(SocketAddr::new(peer_info.tpu.ip(), alt_port));
        }
        
        forwards
    }

    fn is_jito_validator(&self, pubkey: &Pubkey) -> bool {
        let jito_validators = [
            "J1to1yufRnoWn81KYg1XkTWzmKjnYSnmE2VY8DGUJ9Qv",
            "Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb",
            "J1to2NAwajuq3gMkUxBbkqwSs8XdLSvJN5Bxn8nHkWy2",
        ];
        
        jito_validators.iter().any(|&v| {
            Pubkey::from_str(v).map(|k| k == *pubkey).unwrap_or(false)
        })
    }

    fn calculate_initial_score(&self, node: &RouteNode, is_direct: bool) -> f64 {
        let stake_score = (node.stake as f64 / 10_000_000.0).min(1.0);
        let direct_bonus = if is_direct { 0.2 } else { 0.0 };
        let jito_bonus = if node.is_jito { 0.3 } else { 0.0 };
        
        let base_score = 0.5 + stake_score * 0.2 + direct_bonus + jito_bonus;
        
        if let Ok(metrics) = node.metrics.read() {
            let latency_factor = (100.0 - metrics.latency_ms).max(0.0) / 100.0;
            base_score * (0.5 + latency_factor * 0.5)
        } else {
            base_score
        }
    }

    fn calculate_multi_hop_score(&self, nodes: &[RouteNode]) -> f64 {
        if nodes.is_empty() {
            return 0.0;
        }
        
        let mut total_score = 1.0;
        let mut total_latency = 0.0;
        
        for (i, node) in nodes.iter().enumerate() {
            let hop_penalty = 0.9_f64.powi(i as i32);
            let node_score = self.calculate_initial_score(node, false);
            total_score *= node_score * hop_penalty;
            
            if let Ok(metrics) = node.metrics.read() {
                total_latency += metrics.latency_ms;
            }
        }
        
        let latency_penalty = (200.0 - total_latency).max(0.0) / 200.0;
        total_score * latency_penalty
    }

    fn generate_route_id(&self) -> u64 {
        let mut counter = self.route_id_counter.write().unwrap();
        *counter += 1;
        *counter
    }

    fn update_route_cache(&self, leader: &Pubkey, routes: &[Route]) {
        self.routes.insert(*leader, routes.to_vec());
    }

    fn start_background_tasks(&self) {
        self.spawn_route_prober();
        self.spawn_metrics_decay_task();
        self.spawn_leader_schedule_updater();
        self.spawn_route_garbage_collector();
    }

    fn spawn_route_prober(&self) {
        let routes = self.routes.clone();
        let active_probes = self.active_probes.clone();
        let connection_cache = self.connection_cache.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(ROUTE_PROBE_INTERVAL_MS));
            
            loop {
                interval.tick().await;
                
                let mut probes = active_probes.write().await;
                if probes.len() >= MAX_CONCURRENT_ROUTES {
                    continue;
                }
                
                for route_vec in routes.iter() {
                    for route in route_vec.value() {
                        if !probes.contains(&route.id) && route.last_used.elapsed() > Duration::from_secs(10) {
                            probes.insert(route.id);
                            
                            let route_clone = route.clone();
                            let connection_cache_clone = connection_cache.clone();
                            let probes_clone = active_probes.clone();
                            
                            tokio::spawn(async move {
                                let probe_packet = vec![0u8; 176];
                                let start = Instant::now();
                                
                                let _ = Self::send_via_route(&route_clone, &probe_packet, &connection_cache_clone).await;
                                
                                let latency = start.elapsed().as_millis() as f64;
                                
                                for node in &route_clone.nodes {
                                    if let Ok(mut metrics) = node.metrics.write() {
                                        metrics.latency_ms = (metrics.latency_ms * 0.8) + (latency * 0.2);
                                        metrics.last_update = Instant::now();
                                    }
                                }
                                
                                probes_clone.write().await.remove(&route_clone.id);
                            });
                            
                            if probes.len() >= MAX_CONCURRENT_ROUTES {
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn spawn_metrics_decay_task(&self) {
        let route_scores = self.route_scores.clone();
        let metrics_aggregator = self.metrics_aggregator.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            
            loop {
                interval.tick().await;
                
                for mut score in route_scores.iter_mut() {
                    *score.value_mut() *= ROUTE_SCORE_DECAY_FACTOR;
                    if *score.value() < MIN_ROUTE_SCORE {
                        *score.value_mut() = MIN_ROUTE_SCORE;
                    }
                }
                
                if let Ok(mut metrics) = metrics_aggregator.write() {
                    metrics.retain(|_, m| {
                        m.last_update.elapsed() < Duration::from_secs(300)
                    });
                }
            }
        });
    }

    fn spawn_leader_schedule_updater(&self) {
        let leader_schedule = self.leader_schedule.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            
            loop {
                interval.tick().await;
            }
        });
    }

    fn spawn_route_garbage_collector(&self) {
        let routes = self.routes.clone();
        let route_scores = self.route_scores.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            
            loop {
                interval.tick().await;
                
                for mut route_vec in routes.iter_mut() {
                    route_vec.value_mut().retain(|route| {
                        route.last_used.elapsed() < Duration::from_secs(600) &&
                                                route_scores.get(&route.id).map(|s| *s.value() > MIN_ROUTE_SCORE).unwrap_or(false)
                    });
                    
                    route_vec.value_mut().sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
                    route_vec.value_mut().truncate(MAX_ROUTES_PER_LEADER * 2);
                }
                
                let active_route_ids: HashSet<u64> = routes.iter()
                    .flat_map(|r| r.value().iter().map(|route| route.id))
                    .collect();
                
                route_scores.retain(|id, _| active_route_ids.contains(id));
            }
        });
    }

    pub async fn update_leader_schedule(&self, schedule: HashMap<Slot, Pubkey>) {
        self.leader_schedule.store(Arc::new(schedule));
    }

    pub async fn get_best_route_for_slot(&self, slot: Slot) -> Option<Vec<Route>> {
        let schedule = self.leader_schedule.load();
        
        if let Some(leader) = schedule.get(&slot) {
            let routes = self.optimize_routes(leader, RoutePriority::Critical).await;
            if !routes.is_empty() {
                return Some(routes);
            }
        }
        
        None
    }

    pub async fn handle_route_failure(&self, route_id: u64, error: &str) {
        if let Some(mut score) = self.route_scores.get_mut(&route_id) {
            *score.value_mut() *= 0.5;
            *score.value_mut() = score.value().max(MIN_ROUTE_SCORE);
        }
        
        if let Ok(mut metrics) = self.metrics_aggregator.write() {
            if let Some(metric) = metrics.get_mut(&route_id) {
                metric.consecutive_failures += 1;
                metric.last_update = Instant::now();
            }
        }
        
        datapoint_info!(
            "route_failure",
            ("route_id", route_id, i64),
            ("error", error, String)
        );
    }

    pub async fn get_route_statistics(&self) -> HashMap<u64, RouteStats> {
        let mut stats = HashMap::new();
        
        if let Ok(metrics) = self.metrics_aggregator.read() {
            for (route_id, metric) in metrics.iter() {
                stats.insert(*route_id, RouteStats {
                    latency_ms: metric.latency_ms,
                    success_rate: metric.success_rate,
                    total_packets: metric.packets_sent,
                    score: self.route_scores.get(route_id).map(|s| *s.value()).unwrap_or(0.0),
                });
            }
        }
        
        stats
    }

    pub async fn prioritize_routes_for_bundle(&self, bundle_size: usize, target_slot: Slot) -> Vec<Route> {
        let mut prioritized_routes = Vec::new();
        
        if let Some(routes) = self.get_best_route_for_slot(target_slot).await {
            for route in routes {
                if route.priority == RoutePriority::Critical || route.priority == RoutePriority::High {
                    if let Some(jito_node) = route.nodes.iter().find(|n| n.is_jito) {
                        prioritized_routes.insert(0, route.clone());
                    } else {
                        prioritized_routes.push(route);
                    }
                }
            }
        }
        
        prioritized_routes.truncate(bundle_size.min(MAX_ROUTES_PER_LEADER));
        prioritized_routes
    }

    pub fn estimate_route_latency(&self, route: &Route) -> f64 {
        let mut total_latency = 0.0;
        
        for node in &route.nodes {
            if let Ok(metrics) = node.metrics.read() {
                total_latency += metrics.latency_ms;
            } else {
                total_latency += 50.0;
            }
        }
        
        total_latency * (1.0 + (route.nodes.len() as f64 - 1.0) * 0.1)
    }

    pub async fn refresh_jito_connection(&self) -> anyhow::Result<()> {
        if let Ok(new_client) = Self::init_jito_client().await {
            let mut self_mut = self as *const Self as *mut Self;
            unsafe {
                (*self_mut).jito_client = Some(new_client);
            }
        }
        Ok(())
    }

    pub fn get_route_health_score(&self, route_id: u64) -> f64 {
        self.route_scores.get(&route_id).map(|s| *s.value()).unwrap_or(0.0)
    }

    pub async fn emergency_route_reset(&self, leader: &Pubkey) {
        self.routes.remove(leader);
        
        let new_routes = self.generate_new_routes(leader, RoutePriority::Critical).await;
        self.routes.insert(*leader, new_routes);
        
        datapoint_info!(
            "emergency_route_reset",
            ("leader", leader.to_string(), String)
        );
    }

    pub fn shutdown(&self) {
        self.routes.clear();
        self.route_scores.clear();
        self.cluster_nodes.clear();
    }
}

#[derive(Clone, Debug)]
pub struct RouteStats {
    pub latency_ms: f64,
    pub success_rate: f64,
    pub total_packets: u64,
    pub score: f64,
}

impl Drop for PacketRouteOptimizer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

use std::str::FromStr;
use solana_gossip::contact_info::ContactInfo;

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_route_optimization() {
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&Pubkey::new_unique(), 0),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        
        let connection_cache = Arc::new(ConnectionCache::new("test"));
        let optimizer = PacketRouteOptimizer::new(cluster_info, connection_cache).await.unwrap();
        
        let leader = Pubkey::new_unique();
        let routes = optimizer.optimize_routes(&leader, RoutePriority::Normal).await;
        
        assert!(!routes.is_empty());
        assert!(routes.len() <= MAX_ROUTES_PER_LEADER);
    }

    #[tokio::test]
    async fn test_route_scoring() {
        let node = RouteNode {
            address: "127.0.0.1:8001".parse().unwrap(),
            pubkey: Pubkey::new_unique(),
            stake: 5_000_000,
            is_leader: true,
            is_jito: false,
            tpu_forwards: vec![],
            metrics: Arc::new(RwLock::new(RouteMetrics {
                latency_ms: 25.0,
                success_rate: 0.95,
                packets_sent: 1000,
                packets_confirmed: 950,
                last_update: Instant::now(),
                consecutive_failures: 0,
            })),
        };
        
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&Pubkey::new_unique(), 0),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        
        let connection_cache = Arc::new(ConnectionCache::new("test"));
        let optimizer = PacketRouteOptimizer::new(cluster_info, connection_cache).await.unwrap();
        
        let score = optimizer.calculate_initial_score(&node, true);
        assert!(score > 0.5);
        assert!(score <= 1.0);
    }

    #[tokio::test]
    async fn test_multi_path_routes() {
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&Pubkey::new_unique(), 0),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        
        let connection_cache = Arc::new(ConnectionCache::new("test"));
        let optimizer = PacketRouteOptimizer::new(cluster_info, connection_cache).await.unwrap();
        
        let leader_node = RouteNode {
            address: "127.0.0.1:8001".parse().unwrap(),
            pubkey: Pubkey::new_unique(),
            stake: 10_000_000,
            is_leader: true,
            is_jito: false,
            tpu_forwards: vec![
                "127.0.0.1:8002".parse().unwrap(),
                "127.0.0.1:8003".parse().unwrap(),
            ],
            metrics: Arc::new(RwLock::new(RouteMetrics {
                latency_ms: 30.0,
                success_rate: 0.9,
                packets_sent: 100,
                packets_confirmed: 90,
                last_update: Instant::now(),
                consecutive_failures: 0,
            })),
        };
        
        let routes = optimizer.create_multi_path_routes(&leader_node, RoutePriority::High);
        assert_eq!(routes.len(), 2);
    }
}

