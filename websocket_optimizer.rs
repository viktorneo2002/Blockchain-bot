use std::time::{Duration, Instant};
use tokio::time::{interval, timeout};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message, WebSocketStream, MaybeTlsStream};
use tokio::net::TcpStream;
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;
use std::io::{Write, Read};
use blake3::Hasher;
use crossbeam_channel::{bounded, Sender, Receiver};
use mio::Poll;
use socket2::{Socket, Domain, Type, Protocol};
use nix::sys::socket::{setsockopt, sockopt};

const MAX_CONNECTIONS: usize = 32;
const RECONNECT_BASE_DELAY: u64 = 100;
const MAX_RECONNECT_DELAY: u64 = 10000;
const MESSAGE_BUFFER_SIZE: usize = 65536;
const PING_INTERVAL: u64 = 5000;
const CONNECTION_TIMEOUT: u64 = 3000;
const MAX_MESSAGE_SIZE: usize = 1048576;
const PRIORITY_LEVELS: usize = 4;
const LATENCY_WINDOW: usize = 1000;
const COMPRESSION_THRESHOLD: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WSMessage {
    pub id: u64,
    pub priority: u8,
    pub timestamp: u64,
    pub payload: Bytes,
    pub compressed: bool,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct ConnectionMetrics {
    pub latency_ms: f64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub reconnects: u32,
    pub errors: u32,
    pub last_activity: Instant,
}

#[derive(Debug)]
struct ConnectionState {
    id: usize,
    url: String,
    stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    priority_queues: Vec<VecDeque<WSMessage>>,
    send_semaphore: Arc<Semaphore>,
    reconnect_count: u32,
    last_ping: Instant,
    latency_samples: VecDeque<f64>,
}

pub struct WebSocketOptimizer {
    connections: Arc<DashMap<usize, Arc<Mutex<ConnectionState>>>>,
    message_cache: Arc<DashMap<[u8; 32], (WSMessage, Instant)>>,
    tx_channels: Arc<DashMap<usize, mpsc::Sender<WSMessage>>>,
    rx_channel: mpsc::Receiver<WSMessage>,
    rx_sender: mpsc::Sender<WSMessage>,
    compressor: Arc<Mutex<GzEncoder<Vec<u8>>>>,
    endpoints: Vec<String>,
}

impl WebSocketOptimizer {
    pub fn new(endpoints: Vec<String>) -> Self {
        let (rx_sender, rx_channel) = mpsc::channel(MESSAGE_BUFFER_SIZE);

        Self {
            connections: Arc::new(DashMap::new()),
            message_cache: Arc::new(DashMap::new()),
            tx_channels: Arc::new(DashMap::new()),
            rx_channel,
            rx_sender,
            compressor: Arc::new(Mutex::new(GzEncoder::new(Vec::new(), Compression::best()))),
            endpoints,
        }
    }
}

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let num_connections = std::cmp::min(self.endpoints.len(), MAX_CONNECTIONS);
        
        for i in 0..num_connections {
            let endpoint = self.endpoints[i % self.endpoints.len()].clone();
            self.spawn_connection(i, endpoint).await?;
        }

        self.start_maintenance_tasks();
        Ok(())
    }

    async fn spawn_connection(&self, id: usize, url: String) -> Result<(), Box<dyn std::error::Error>> {
        let (tx, mut rx) = mpsc::channel(MESSAGE_BUFFER_SIZE);
        self.tx_channels.insert(id, tx);

        let state = Arc::new(Mutex::new(ConnectionState {
            id,
            url: url.clone(),
            stream: None,
                latency_ms: 0.0,
                messages_sent: 0,
                messages_received: 0,
                bytes_sent: 0,
                bytes_received: 0,
                reconnects: 0,
                errors: 0,
                last_activity: Instant::now(),
            })),
            priority_queues: (0..PRIORITY_LEVELS).map(|_| VecDeque::with_capacity(1024)).collect(),
            send_semaphore: Arc::new(Semaphore::new(256)),
            reconnect_count: 0,
            last_ping: Instant::now(),
            latency_samples: VecDeque::with_capacity(LATENCY_WINDOW),

        self.connections.insert(id, state.clone());
        self.connection_pool.write().push(id);

        let connections = self.connections.clone();
        let rx_sender = self.rx_sender.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            loop {
                if *shutdown.read() {
                    break;
                }

                match Self::connect_with_optimization(&url).await {
                    Ok(ws_stream) => {
                        let mut state_guard = state.lock().await;
                        state_guard.stream = Some(ws_stream);
                        state_guard.reconnect_count = 0;
                        drop(state_guard);

                        if let Err(e) = Self::handle_connection(
                            state.clone(),
                            &mut rx,
                            rx_sender.clone(),
                            shutdown.clone()
                        ).await {
                            let mut state_guard = state.lock().await;
                            state_guard.metrics.write().errors += 1;
                            eprintln!("Connection {} error: {:?}", id, e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to {}: {:?}", url, e);
                    }
                }

                let mut state_guard = state.lock().await;
                state_guard.reconnect_count += 1;
                state_guard.metrics.write().reconnects += 1;
                let delay = Self::calculate_backoff(state_guard.reconnect_count);
                drop(state_guard);

                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        });

        Ok(())
    

    async fn connect_with_optimization(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<dyn std::error::Error>> {
        let parsed_url = url::Url::parse(url)?;
        let host = parsed_url.host_str().ok_or("Invalid URL")?;
        let port = parsed_url.port_or_known_default().ok_or("Invalid port")?;
        
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_nodelay(true)?;
        socket.set_nonblocking(true)?;
        
        #[cfg(target_os = "linux")]
        {
            socket.set_reuse_address(true)?;
            socket.set_reuse_port(true)?;
            let _ = socket.set_tcp_quickack(true);
        }
        
        let addr = format!("{}:{}", host, port).parse::<std::net::SocketAddr>()?;
        match socket.connect(&addr.into()) {
            Ok(_) => {},
            Err(e) if e.raw_os_error() == Some(115) => {},
            Err(e) => return Err(e.into()),
        }

        let tcp_stream: TcpStream = socket.into();
        tcp_stream.set_nodelay(true)?;

        let (ws_stream, _) = timeout(
            Duration::from_millis(CONNECTION_TIMEOUT),
            connect_async(url)
        ).await??;

        Ok(ws_stream)
    }

    async fn handle_connection(
        state: Arc<Mutex<ConnectionState>>,
        rx: &mut mpsc::Receiver<WSMessage>,
        tx_global: mpsc::Sender<WSMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ping_interval = interval(Duration::from_millis(PING_INTERVAL));
        tokio::pin!(ping_interval);

        loop {
            if *shutdown.read() {
                break;
            }

            let mut state_guard = state.lock().await;
            if state_guard.stream.is_none() {
                drop(state_guard);
                return Err("Stream disconnected".into());
            }

            let ws_stream = state_guard.stream.as_mut().unwrap();
            
            tokio::select! {
                msg = rx.recv() => {
                    if let Some(msg) = msg {
                        let priority = msg.priority as usize;
                        if priority < PRIORITY_LEVELS {
                            state_guard.priority_queues[priority].push_back(msg);
                        }
                    }
                }
                
                ws_msg = ws_stream.next() => {
                    match ws_msg {
                        Some(Ok(Message::Binary(data))) => {
                            let received_msg = Self::process_incoming_message(data)?;
                            state_guard.metrics.write().messages_received += 1;
                            state_guard.metrics.write().bytes_received += received_msg.payload.len() as u64;
                            drop(state_guard);
                            tx_global.send(received_msg).await?;
                        }
                        Some(Ok(Message::Pong(data))) => {
                            let latency = state_guard.last_ping.elapsed().as_micros() as f64 / 1000.0;
                            state_guard.latency_samples.push_back(latency);
                            if state_guard.latency_samples.len() > LATENCY_WINDOW {
                                state_guard.latency_samples.pop_front();
                            }
                            let avg_latency = state_guard.latency_samples.iter().sum::<f64>() 
                                / state_guard.latency_samples.len() as f64;
                            state_guard.metrics.write().latency_ms = avg_latency;
                        }
                        Some(Ok(Message::Close(_))) => {
                            drop(state_guard);
                            return Err("Connection closed".into());
                        }
                        Some(Err(e)) => {
                            drop(state_guard);
                            return Err(e.into());
                        }
                        None => {
                            drop(state_guard);
                            return Err("Stream ended".into());
                        }
                        _ => {}
                    }
                }
                
                _ = ping_interval.tick() => {
                    state_guard.last_ping = Instant::now();
                    let ping = Message::Ping(vec![]);
                    ws_stream.send(ping).await?;
                }
            }

            for priority in 0..PRIORITY_LEVELS {
                if let Some(msg) = state_guard.priority_queues[priority].pop_front() {
                    if let Ok(_permit) = state_guard.send_semaphore.try_acquire() {
                        let data = Self::prepare_outgoing_message(&msg)?;
                        ws_stream.send(Message::Binary(data)).await?;
                        state_guard.metrics.write().messages_sent += 1;
                        state_guard.metrics.write().bytes_sent += msg.payload.len() as u64;
                    } else {
                        state_guard.priority_queues[priority].push_front(msg);
                        break;
                    }
                }
            }

            state_guard.metrics.write().last_activity = Instant::now();
            drop(state_guard);
        }

        Ok(())
    }

    pub async fn send_message(&self, payload: Vec<u8>, priority: u8) -> Result<u64, Box<dyn std::error::Error>> {
        let id = {
            let mut next_id = self.next_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        let compressed = payload.len() > COMPRESSION_THRESHOLD;
        let processed_payload = if compressed {
            Self::compress_data(&payload)?
        } else {
            payload
        };

        let hash = Self::calculate_hash(&processed_payload);
        
        let message = WSMessage {
            id,
            priority: priority.min((PRIORITY_LEVELS - 1) as u8),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,
            payload: Bytes::from(processed_payload),
            compressed,
            hash,
        };

        self.message_cache.insert(hash, (message.clone(), Instant::now()));

        let pool = self.connection_pool.read();
        if pool.is_empty() {
            return Err("No active connections".into());
        }

        let best_connection = self.select_best_connection(&pool).await?;
        drop(pool);

        if let Some(tx) = self.tx_channels.get(&best_connection) {
            tx.send(message).await?;
            Ok(id)
        } else {
            Err("Connection channel not found".into())
        }
    }

    async fn select_best_connection(&self, pool: &[usize]) -> Result<usize, Box<dyn std::error::Error>> {
        let mut best_id = pool[0];
        let mut best_score = f64::MAX;

        for &conn_id in pool {
            if let Some(conn) = self.connections.get(&conn_id) {
                let state = conn.lock().await;
                let metrics = state.metrics.read();
                
                let queue_size: usize = state.priority_queues.iter().map(|q| q.len()).sum();
                let latency = metrics.latency_ms;
                let error_rate = metrics.errors as f64 / metrics.messages_sent.max(1) as f64;
                let activity_age = state.last_ping.elapsed().as_millis() as f64;
                
                let score = latency * 0.4 
                    + (queue_size as f64) * 0.3 
                    + error_rate * 1000.0 * 0.2
                    + activity_age * 0.1;

                if score < best_score && state.stream.is_some() {
                    best_score = score;
                    best_id = conn_id;
                }
            }
        }

        Ok(best_id)
    }

    pub async fn receive_message(&mut self) -> Result<WSMessage, Box<dyn std::error::Error>> {
        self.rx_channel.recv().await.ok_or("Receive channel closed".into())
    }

    fn start_maintenance_tasks(&self) {
        let cache = self.message_cache.clone();
        let connections = self.connections.clone();
        let pool = self.connection_pool.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(30));
            let mut health_check_interval = interval(Duration::from_secs(5));

            loop {
                if *shutdown.read() {
                    break;
                }

                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        let now = Instant::now();
                        cache.retain(|_, (_, timestamp)| {
                            now.duration_since(*timestamp).as_secs() < 300
                        });
                    }

                    _ = health_check_interval.tick() => {
                        let mut healthy_connections = Vec::new();
                        
                        for entry in connections.iter() {
                            let conn_id = *entry.key();
                            let conn_state = entry.value();
                            let state = conn_state.lock().await;
                            
                            let healthy = state.stream.is_some() && 
                                         state.last_ping.elapsed().as_secs() < 30 &&
                                         state.metrics.read().latency_ms < 1000.0;
                            
                            if healthy {
                                healthy_connections.push(conn_id);
                            }
                        }

                        *pool.write() = healthy_connections;
                    }
                }
            }
        });
    }

    fn calculate_backoff(attempt: u32) -> u64 {
        let delay = RECONNECT_BASE_DELAY * (2u64.pow(attempt.min(10)));
        delay.min(MAX_RECONNECT_DELAY)
    }

    fn compress_data(data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut encoder = GzEncoder::new(Vec::with_capacity(data.len()), Compression::best());
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }

    fn decompress_data(data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    fn calculate_hash(data: &[u8]) -> [u8; 32] {
        let mut hasher = Hasher::new();
        hasher.update(data);
        *hasher.finalize().as_bytes()
    }

    fn prepare_outgoing_message(msg: &WSMessage) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let header = vec![
            msg.priority,
            if msg.compressed { 1 } else { 0 },
            (msg.id >> 56) as u8,
            (msg.id >> 48) as u8,
            (msg.id >> 40) as u8,
            (msg.id >> 32) as u8,
            (msg.id >> 24) as u8,
            (msg.id >> 16) as u8,
            (msg.id >> 8) as u8,
            msg.id as u8,
            (msg.timestamp >> 56) as u8,
            (msg.timestamp >> 48) as u8,
            (msg.timestamp >> 40) as u8,
            (msg.timestamp >> 32) as u8,
            (msg.timestamp >> 24) as u8,
            (msg.timestamp >> 16) as u8,
            (msg.timestamp >> 8) as u8,
            msg.timestamp as u8,
        ];

        let mut result = header;
        result.extend_from_slice(&msg.hash);
        result.extend_from_slice(&msg.payload);
        
        Ok(result)
    }

    fn process_incoming_message(data: Vec<u8>) -> Result<WSMessage, Box<dyn std::error::Error>> {
        if data.len() < 50 {
            return Err("Invalid message format".into());
        }

        let priority = data[0];
        let compressed = data[1] == 1;
        
        let id = u64::from_be_bytes([
            data[2], data[3], data[4], data[5],
            data[6], data[7], data[8], data[9]
        ]);
        
        let timestamp = u64::from_be_bytes([
            data[10], data[11], data[12], data[13],
            data[14], data[15], data[16], data[17]
        ]);

        let mut hash = [0u8; 32];
        hash.copy_from_slice(&data[18..50]);

        let payload_data = &data[50..];
        let payload = if compressed {
            Self::decompress_data(payload_data)?
        } else {
            payload_data.to_vec()
        };

        Ok(WSMessage {
            id,
            priority,
            timestamp,
            payload: Bytes::from(payload),
            compressed,
            hash,
        })
    }

    pub async fn get_metrics(&self) -> HashMap<usize, ConnectionMetrics> {
        let mut metrics = HashMap::new();
        
        for entry in self.connections.iter() {
            let id = *entry.key();
            let state = entry.value().lock().await;
            metrics.insert(id, state.metrics.read().clone());
        }

        metrics
    }

    pub async fn get_best_latency(&self) -> Option<f64> {
        let pool = self.connection_pool.read();
        let mut best_latency = None;

        for &conn_id in pool.iter() {
            if let Some(conn) = self.connections.get(&conn_id) {
                let state = conn.lock().await;
                let latency = state.metrics.read().latency_ms;
                
                match best_latency {
                    None => best_latency = Some(latency),
                    Some(current_best) if latency < current_best => best_latency = Some(latency),
                    _ => {}
                }
            }
        }

        best_latency
    }

    pub async fn force_reconnect(&self, connection_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(conn) = self.connections.get(&connection_id) {
            let mut state = conn.lock().await;
            if let Some(stream) = state.stream.take() {
                let _ = stream.close(None).await;
            }
            state.reconnect_count = 0;
        }
        Ok(())
    }

    pub fn is_duplicate(&self, hash: &[u8; 32]) -> bool {
        self.message_cache.contains_key(hash)
    }

    pub async fn shutdown(&mut self) {
        *self.shutdown.write() = true;
        
        for entry in self.connections.iter() {
            let mut state = entry.value().lock().await;
            if let Some(mut stream) = state.stream.take() {
                let _ = stream.close(None).await;
            }
        }

        self.connections.clear();
        self.tx_channels.clear();
        self.message_cache.clear();
        self.connection_pool.write().clear();
    }

    pub fn get_active_connections(&self) -> usize {
        self.connection_pool.read().len()
    }

    pub async fn add_endpoint(&mut self, endpoint: String) -> Result<(), Box<dyn std::error::Error>> {
        if !self.endpoints.contains(&endpoint) {
            self.endpoints.push(endpoint.clone());
            
            let id = self.connections.len();
            if id < MAX_CONNECTIONS {
                self.spawn_connection(id, endpoint).await?;
            }
        }
        Ok(())
    }

    pub async fn remove_endpoint(&mut self, endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.endpoints.retain(|e| e != endpoint);
        
        let mut to_remove = Vec::new();
        for entry in self.connections.iter() {
            let state = entry.value().lock().await;
            if state.url == endpoint {
                to_remove.push(*entry.key());
            }
        }

        for id in to_remove {
            if let Some((_, conn)) = self.connections.remove(&id) {
                let mut state = conn.lock().await;
                if let Some(mut stream) = state.stream.take() {
                    let _ = stream.close(None).await;
                }
            }
            self.tx_channels.remove(&id);
            self.connection_pool.write().retain(|&x| x != id);
        }

        Ok(())
    }

    pub async fn optimize_connection_pool(&self) {
        let metrics = self.get_metrics().await;
        let mut scored_connections: Vec<(usize, f64)> = Vec::new();

        for (id, metric) in metrics {
            let score = metric.latency_ms * 0.5 
                + (metric.errors as f64 / metric.messages_sent.max(1) as f64) * 500.0
                + (metric.reconnects as f64) * 10.0;
            scored_connections.push((id, score));
        }

        scored_connections.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let optimal_size = (scored_connections.len() / 2).max(4).min(MAX_CONNECTIONS);
        let mut new_pool = Vec::new();

        for (i, (id, _)) in scored_connections.iter().enumerate() {
            if i < optimal_size {
                new_pool.push(*id);
            }
        }

        *self.connection_pool.write() = new_pool;
    }



#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_websocket_optimizer_initialization() {
        let endpoints = vec!["wss://api.mainnet-beta.solana.com".to_string()];
        let optimizer = WebSocketOptimizer::new(endpoints);
        assert_eq!(optimizer.get_active_connections(), 0);
    }

    #[tokio::test]
    async fn test_message_compression() {
        let data = vec![1u8; 2048];
        let compressed = WebSocketOptimizer::compress_data(&data).unwrap();
        assert!(compressed.len() < data.len());
        
        let decompressed = WebSocketOptimizer::decompress_data(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[tokio::test]
    async fn test_hash_calculation() {
        let data = b"test message";
        let hash1 = WebSocketOptimizer::calculate_hash(data);
        let hash2 = WebSocketOptimizer::calculate_hash(data);
        assert_eq!(hash1, hash2);
        
        let different_data = b"different message";
        let hash3 = WebSocketOptimizer::calculate_hash(different_data);
        assert_ne!(hash1, hash3);
    }
    } // <- CLOSES spawn_connection
} // <- CLOSES mod tests

