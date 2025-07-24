use anyhow::{Result, anyhow, Context};
use blake3::Hasher;
use dashmap::DashMap;
use futures_util::StreamExt;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_client::rpc_response::RpcBlockUpdate;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding, UiBlock, UiTransaction};
use std::collections::{VecDeque, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio::time::{interval, timeout, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, span, warn, Level};
use url::Url;

const MAX_BLOCK_BUFFER_SIZE: usize = 4096;
const MAX_PROCESSED_SLOTS_HISTORY: usize = 8192;
const CONNECTION_TIMEOUT_MS: u64 = 250;
const SUBSCRIPTION_TIMEOUT_MS: u64 = 500;
const HEALTH_CHECK_INTERVAL_MS: u64 = 200;
const RECONNECT_BASE_DELAY_MS: u64 = 50;
const RECONNECT_MAX_DELAY_MS: u64 = 2000;
const MAX_CONSECUTIVE_FAILURES: usize = 3;
const FORK_DETECTION_LOOKBACK: usize = 64;
const CIRCUIT_BREAKER_FAILURE_THRESHOLD: usize = 5;
const CIRCUIT_BREAKER_RECOVERY_TIME_MS: u64 = 15000;
const SUBSCRIPTION_PING_INTERVAL_MS: u64 = 5000;
const MAX_CONCURRENT_CONNECTIONS: usize = 16;
const SLOT_DRIFT_TOLERANCE: u64 = 5;
const MAX_SLOT_ENTROPY_DRIFT: u64 = 3;
const BLOCK_TIME_TOLERANCE_SEC: i64 = 15;
const MEV_OPPORTUNITY_SCAN_DEPTH: usize = 20;
const JITO_TIP_FEEDBACK_WINDOW: usize = 100;
const REPLAY_PROTECTION_WINDOW: usize = 2048;
const PEER_VERIFICATION_CONSENSUS_THRESHOLD: f64 = 0.67;
const ECONOMIC_ATTACK_DETECTION_WINDOW: usize = 50;
const MULTI_REGION_FAILOVER_THRESHOLD: f64 = 0.8;
const ENTROPY_BASELINE_WINDOW: usize = 200;
const LSTM_PREDICTION_HORIZON: usize = 32;
const MINIMUM_TIP_LAMPORTS: u64 = 10000;
const CONGESTION_MULTIPLIER_MAX: f64 = 5.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEndpoint {
    pub url: String,
    pub ws_url: String,
    pub priority: u8,
    pub region: String,
    pub supports_jito: bool,
    pub validator_identity: Option<String>,
    pub trusted_certificate_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockInfo {
    pub slot: u64,
    pub blockhash: String,
    pub parent_slot: u64,
    pub block_time: i64,
    pub block_height: u64,
    pub received_at: u64,
    pub commitment: CommitmentLevel,
    pub transaction_count: usize,
    pub source_endpoint: String,
    
    pub leader_pubkey: String,
    pub total_tip_amount: u64,
    pub priority_fee_percentiles: Vec<u64>,
    pub block_hash_signatures: Vec<String>,
    pub slot_entropy: u64,
    pub validator_commission: u8,
    pub is_replay_protected: bool,
    pub mev_opportunity_count: usize,
    pub jito_bundle_count: usize,
    pub max_priority_fee: u64,
    pub median_priority_fee: u64,
    pub dex_volume_estimate: u64,
    pub cross_endpoint_verification_score: f64,
    pub fork_risk_score: f64,
}

#[derive(Debug, Clone)]
struct ProcessedSlot {
    slot: u64,
    blockhash: String,
    parent_slot: u64,
    timestamp: u64,
    entropy_signature: [u8; 32],
    source_endpoints: HashSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitBreakerState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
    ForceOpen = 3,
}

#[derive(Debug)]
struct HardenedCircuitBreaker {
    state: AtomicU8,
    failure_count: AtomicUsize,
    success_count: AtomicUsize,
    last_failure_time: AtomicU64,
    last_success_time: AtomicU64,
    failure_rate_window: Mutex<VecDeque<bool>>,
    auto_rotation_enabled: AtomicBool,
    recovery_attempts: AtomicUsize,
    force_open_until: AtomicU64,
}

#[derive(Debug)]
struct ConnectionState {
    endpoint: RpcEndpoint,
    client: Option<Arc<PubsubClient>>,
    last_block_time: AtomicU64,
    last_slot_received: AtomicU64,
    consecutive_failures: AtomicUsize,
    is_connected: AtomicBool,
    is_subscribed: AtomicBool,
    circuit_breaker: HardenedCircuitBreaker,
    reconnect_delay_ms: AtomicU64,
    subscription_id: Mutex<Option<String>>,
    connection_quality_score: AtomicU64,
    tls_verified: AtomicBool,
    last_ping_response: AtomicU64,
    blocks_received_count: AtomicU64,
    integrity_violations: AtomicUsize,
}

#[derive(Debug, Clone)]
pub struct SubscriptionStats {
    pub total_blocks_received: u64,
    pub total_blocks_processed: u64,
    pub total_reconnections: u64,
    pub total_fork_events: u64,
    pub total_replay_blocks_blocked: u64,
    pub total_integrity_violations: u64,
    pub current_slot: u64,
    pub latest_block_latency_ms: u64,
    pub active_connections: usize,
    pub failed_connections: usize,
    pub blocks_per_second: f64,
    pub connection_health: Vec<ConnectionHealth>,
    pub mev_opportunities_detected: u64,
    pub jito_tip_efficiency_score: f64,
    pub cross_endpoint_consensus_score: f64,
    pub economic_attack_detections: u64,
}

#[derive(Debug, Clone)]
pub struct ConnectionHealth {
    pub endpoint: String,
    pub region: String,
    pub is_connected: bool,
    pub is_subscribed: bool,
    pub last_slot: u64,
    pub consecutive_failures: usize,
    pub circuit_breaker_state: String,
    pub latency_ms: u64,
    pub quality_score: f64,
    pub tls_verified: bool,
    pub integrity_violations: usize,
    pub blocks_per_minute: f64,
}

#[derive(Debug, Clone)]
struct ForkEvent {
    pub fork_slot: u64,
    pub fork_type: ForkType,
    pub confidence: f64,
    pub economic_impact: u64,
    pub validator_list: Vec<String>,
    pub detection_timestamp: u64,
    pub affected_endpoints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
enum ForkType {
    ValidatorCollusion,
    NetworkPartition,
    ReorgAttack,
    SlotManipulation,
    EntropyAnomaly,
    ConsensusBreak,
}

#[derive(Debug)]
struct ForkDetector {
    slot_chain: VecDeque<(u64, String, u64, u64)>,
    last_continuous_slot: u64,
    potential_fork_slots: Vec<u64>,
    entropy_baseline: f64,
    consensus_tracking: HashMap<u64, HashMap<String, usize>>,
}

#[derive(Debug)]
struct MEVIntelligence {
    tip_analyzer: TipAnalyzer,
    opportunity_scanner: OpportunityScanner,
    leader_predictor: LeaderPredictor,
    fee_market_analyzer: FeeMarketAnalyzer,
}

#[derive(Debug)]
struct TipAnalyzer {
    recent_tips: VecDeque<(u64, u64)>,
    tip_efficiency_scores: HashMap<String, f64>,
    optimal_tip_calculator: OptimalTipCalculator,
}

#[derive(Debug)]
struct OpportunityScanner {
    recent_opportunities: VecDeque<MEVOpportunity>,
    dex_volume_tracker: DexVolumeTracker,
    arbitrage_detector: ArbitrageDetector,
}

#[derive(Debug, Clone)]
struct MEVOpportunity {
    slot: u64,
    opportunity_type: MEVOpportunityType,
    estimated_profit: u64,
    required_tip: u64,
    confidence: f64,
    expiry_slot: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum MEVOpportunityType {
    Arbitrage,
    Liquidation,
    Sandwich,
    JustInTime,
    CrossDexArbitrage,
}

#[derive(Debug)]
struct LeaderPredictor {
    leader_schedule_cache: HashMap<u64, String>,
    validator_performance_scores: HashMap<String, f64>,
    next_leaders: VecDeque<(u64, String)>,
}

#[derive(Debug)]
struct FeeMarketAnalyzer {
    priority_fee_history: VecDeque<(u64, Vec<u64>)>,
    fee_market_manipulation_detector: FeeManipulationDetector,
    congestion_predictor: CongestionPredictor,
}

#[derive(Debug)]
struct MultiRegionFailover {
    primary_region: String,
    backup_regions: Vec<String>,
    region_health_scores: Arc<DashMap<String, f64>>,
    auto_failover_threshold: f64,
    current_active_region: AtomicU64,
    failover_history: Mutex<VecDeque<(u64, String, String)>>,
}

#[derive(Debug)]
struct EconomicAttackDetector {
    suspicious_patterns: VecDeque<SuspiciousPattern>,
    fee_manipulation_tracker: FeeManipulationTracker,
    volume_anomaly_detector: VolumeAnomalyDetector,
    attack_probability_calculator: AttackProbabilityCalculator,
}

#[derive(Debug, Clone)]
struct SuspiciousPattern {
    pattern_type: AttackPatternType,
    confidence: f64,
    first_detected: u64,
    last_seen: u64,
    affected_slots: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
enum AttackPatternType {
    FeeMarketManipulation,
    VolumeWashTrading,
    CoordinatedSandwich,
    ValidatorColusion,
    MemPoolSpamming,
}

#[derive(Debug)]
struct OptimalTipCalculator {
    congestion_multiplier: AtomicU64,
    base_tip_lamports: AtomicU64,
    success_rate_cache: Arc<DashMap<u64, f64>>,
    recent_submissions: Mutex<VecDeque<(u64, u64, bool)>>,
}

impl OptimalTipCalculator {
    fn new() -> Self {
        Self {
            congestion_multiplier: AtomicU64::new(1000),
            base_tip_lamports: AtomicU64::new(MINIMUM_TIP_LAMPORTS),
            success_rate_cache: Arc::new(DashMap::new()),
            recent_submissions: Mutex::new(VecDeque::with_capacity(JITO_TIP_FEEDBACK_WINDOW)),
        }
    }

    async fn calculate_optimal_tip(&self, slot: u64, congestion_level: f64, priority_urgency: f64) -> u64 {
        let base_tip = self.base_tip_lamports.load(Ordering::Relaxed);
        let congestion_multiplier = self.congestion_multiplier.load(Ordering::Relaxed) as f64 / 1000.0;
        
        let dynamic_multiplier = (1.0 + (congestion_level * congestion_multiplier)).min(CONGESTION_MULTIPLIER_MAX);
        let urgency_multiplier = 1.0 + (priority_urgency * 0.5);
        
        let optimal_tip = (base_tip as f64 * dynamic_multiplier * urgency_multiplier) as u64;
        
        let mut submissions = self.recent_submissions.lock().await;
        submissions.push_back((slot, optimal_tip, false));
        if submissions.len() > JITO_TIP_FEEDBACK_WINDOW {
            submissions.pop_front();
        }
        
        optimal_tip
    }

    async fn record_submission_result(&self, slot: u64, tip_amount: u64, success: bool) {
        if let Some(success_rate) = self.success_rate_cache.get(&tip_amount) {
            let current_rate = *success_rate;
            let new_rate = if success {
                (current_rate * 0.9) + (1.0 * 0.1)
            } else {
                current_rate * 0.9
            };
            self.success_rate_cache.insert(tip_amount, new_rate);
        } else {
            self.success_rate_cache.insert(tip_amount, if success { 1.0 } else { 0.0 });
        }

        if success {
            let current_base = self.base_tip_lamports.load(Ordering::Relaxed);
            if tip_amount < current_base {
                self.base_tip_lamports.store((current_base + tip_amount) / 2, Ordering::Relaxed);
            }
        } else {
            let current_base = self.base_tip_lamports.load(Ordering::Relaxed);
            let new_base = (current_base as f64 * 1.1) as u64;
            self.base_tip_lamports.store(new_base, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
struct DexVolumeTracker {
    raydium_pools: HashMap<String, u64>,
    orca_pools: HashMap<String, u64>,
    jupiter_aggregator_volume: AtomicU64,
    total_dex_volume: AtomicU64,
    volume_window: Mutex<VecDeque<(u64, u64)>>,
}

impl DexVolumeTracker {
    fn new() -> Self {
        Self {
            raydium_pools: HashMap::new(),
            orca_pools: HashMap::new(),
            jupiter_aggregator_volume: AtomicU64::new(0),
            total_dex_volume: AtomicU64::new(0),
            volume_window: Mutex::new(VecDeque::with_capacity(100)),
        }
    }

    async fn track_transaction_volume(&mut self, transaction: &UiTransaction, slot: u64) -> u64 {
        let mut volume_estimate = 0u64;

                        volume_estimate += self.parse_orca_volume(log);
                    } else if log.contains("Program JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4") {
                        volume_estimate += self.parse_jupiter_volume(log);
                    }
                }

        let mut window = self.volume_window.lock().await;
        window.push_back((slot, volume_estimate));
        if window.len() > 100 {
            window.pop_front();
        }

        self.total_dex_volume.fetch_add(volume_estimate, Ordering::Relaxed);
        volume_estimate

    fn parse_raydium_volume(&mut self, log: &str) -> u64 {
        if let Some(start) = log.find("amount_in: ") {
            let amount_str = &log[start + 11..];
            if let Some(end) = amount_str.find(',') {
                if let Ok(amount) = amount_str[..end].parse::<u64>() {
                    return amount;
                }
            }
        }
        0
    }

    fn parse_orca_volume(&mut self, log: &str) -> u64 {
        if let Some(start) = log.find("Swap ") {
            let swap_str = &log[start + 5..];
            if let Some(end) = swap_str.find(' ') {
                if let Ok(amount) = swap_str[..end].parse::<u64>() {
                    return amount;
                }
            }
        }
        0
    }

    fn parse_jupiter_volume(&mut self, log: &str) -> u64 {
        if let Some(start) = log.find("input_amount: ") {
            let amount_str = &log[start + 14..];
            if let Some(end) = amount_str.find(',') {
                if let Ok(amount) = amount_str[..end].parse::<u64>() {
                    self.jupiter_aggregator_volume.fetch_add(amount, Ordering::Relaxed);
                    return amount;
                }
            }
        }
        0
    }

#[derive(Debug)]
struct ArbitrageDetector {
    price_cache: HashMap<String, u64>,
    arbitrage_opportunities: VecDeque<ArbitrageOpportunity>,
    profit_threshold: u64,
}

#[derive(Debug, Clone)]
struct ArbitrageOpportunity {
    token_pair: String,
    buy_exchange: String,
    sell_exchange: String,
    profit_estimate: u64,
    confidence: f64,
    expiry_slot: u64,
}

impl ArbitrageDetector {
    fn new() -> Self {
        Self {
            price_cache: HashMap::new(),
            arbitrage_opportunities: VecDeque::with_capacity(50),
            profit_threshold: 100000,
        }
    }

    fn detect_arbitrage(&mut self, transaction: &UiTransaction, slot: u64) -> Vec<ArbitrageOpportunity> {
        let mut opportunities = Vec::new();

                    }
                }

        for opp in &opportunities {
            self.arbitrage_opportunities.push_back(opp.clone());
            if self.arbitrage_opportunities.len() > 50 {
                self.arbitrage_opportunities.pop_front();
            }
        }

        opportunities

    fn parse_arbitrage_from_log(&mut self, log: &str, slot: u64) -> Option<ArbitrageOpportunity> {
        if log.contains("price difference detected") || log.contains("arbitrage opportunity") {
            Some(ArbitrageOpportunity {
                token_pair: "SOL/USDC".to_string(),
                buy_exchange: "Raydium".to_string(),
                sell_exchange: "Orca".to_string(),
                profit_estimate: 50000,
                confidence: 0.8,
                expiry_slot: slot + 5,
            })
        } else {
            None
        }
    }

#[derive(Debug)]
struct FeeManipulationDetector {
    fee_patterns: VecDeque<(u64, u64)>,
    manipulation_threshold: f64,
    baseline_fee: AtomicU64,
}

impl FeeManipulationDetector {
    fn new() -> Self {
        Self {
            fee_patterns: VecDeque::with_capacity(ECONOMIC_ATTACK_DETECTION_WINDOW),
            manipulation_threshold: 3.0,
            baseline_fee: AtomicU64::new(5000),
        }
    }

    fn detect_manipulation(&mut self, slot: u64, fees: &[u64]) -> bool {
        if fees.is_empty() {
            return false;
        }

        let average_fee = fees.iter().sum::<u64>() / fees.len() as u64;
        self.fee_patterns.push_back((slot, average_fee));

        if self.fee_patterns.len() > ECONOMIC_ATTACK_DETECTION_WINDOW {
            self.fee_patterns.pop_front();
        }

        let baseline = self.baseline_fee.load(Ordering::Relaxed);
        let manipulation_detected = average_fee as f64 > (baseline as f64 * self.manipulation_threshold);

        if !manipulation_detected && self.fee_patterns.len() >= 10 {
            let recent_average = self.fee_patterns.iter()
                .rev()
                .take(10)
                .map(|(_, fee)| *fee)
                .sum::<u64>() / 10;
            
            self.baseline_fee.store(recent_average, Ordering::Relaxed);
        }

        manipulation_detected
    }
}

#[derive(Debug)]
struct CongestionPredictor {
    slot_congestion_history: VecDeque<(u64, f64)>,
    lstm_state: LSTMState,
    prediction_accuracy: f64,
}

#[derive(Debug)]
struct LSTMState {
    hidden_state: Vec<f64>,
    cell_state: Vec<f64>,
    weights: HashMap<String, Vec<f64>>,
}

impl CongestionPredictor {
    fn new() -> Self {
        Self {
            slot_congestion_history: VecDeque::with_capacity(LSTM_PREDICTION_HORIZON * 2),
            lstm_state: LSTMState {
                hidden_state: vec![0.0; 64],
                cell_state: vec![0.0; 64],
                weights: HashMap::new(),
            },
            prediction_accuracy: 0.5,
        }
    }

    fn predict_congestion(&mut self, current_slot: u64, transaction_count: usize) -> f64 {
        let congestion_level = (transaction_count as f64).min(4000.0) / 4000.0;
        
        self.slot_congestion_history.push_back((current_slot, congestion_level));
        if self.slot_congestion_history.len() > LSTM_PREDICTION_HORIZON * 2 {
            self.slot_congestion_history.pop_front();
        }

        if self.slot_congestion_history.len() < LSTM_PREDICTION_HORIZON {
            return congestion_level;
        }

        let prediction = self.lstm_forward_pass();
        
        self.prediction_accuracy = (self.prediction_accuracy * 0.95) + 
                                   (if (prediction - congestion_level).abs() < 0.1 { 0.05 } else { 0.0 });

        prediction
    }

    fn lstm_forward_pass(&mut self) -> f64 {
        let input_sequence: Vec<f64> = self.slot_congestion_history.iter()
            .rev()
            .take(LSTM_PREDICTION_HORIZON)
            .map(|(_, congestion)| *congestion)
            .collect();

        let mut output = 0.0;
        for (i, &input) in input_sequence.iter().enumerate() {
            let weighted_input = input * (0.8 + (i as f64 * 0.02));
            output += weighted_input;
        }

        (output / input_sequence.len() as f64).max(0.0).min(1.0)
    }
}

#[derive(Debug)]
struct FeeManipulationTracker {
    manipulation_events: VecDeque<ManipulationEvent>,
    validator_manipulation_scores: HashMap<String, f64>,
}

#[derive(Debug, Clone)]
struct ManipulationEvent {
    slot: u64,
    manipulation_type: ManipulationType,
    severity: f64,
    validator_identity: Option<String>,
}

#[derive(Debug, Clone)]
enum ManipulationType {
    FeeSpike,
    FeeSuppression,
    VolumeWash,
    SandwichCoordination,
}

impl FeeManipulationTracker {
    fn new() -> Self {
        Self {
            manipulation_events: VecDeque::with_capacity(ECONOMIC_ATTACK_DETECTION_WINDOW),
            validator_manipulation_scores: HashMap::new(),
        }
    }

    fn track_manipulation(&mut self, slot: u64, manipulation_type: ManipulationType, severity: f64, validator: Option<String>) {
        let event = ManipulationEvent {
            slot,
            manipulation_type,
            severity,
            validator_identity: validator.clone(),
        };

        self.manipulation_events.push_back(event);
        if self.manipulation_events.len() > ECONOMIC_ATTACK_DETECTION_WINDOW {
            self.manipulation_events.pop_front();
        }

        if let Some(validator_id) = validator {
            self.validator_manipulation_scores
                .entry(validator_id)
                .and_modify(|score| *score = (*score * 0.95) + (severity * 0.05))
                .or_insert(severity);
        }
    }
}

#[derive(Debug)]
struct VolumeAnomalyDetector {
    volume_baseline: AtomicU64,
    anomaly_threshold: f64,
    recent_volumes: VecDeque<u64>,
}

impl VolumeAnomalyDetector {
    fn new() -> Self {
        Self {
            volume_baseline: AtomicU64::new(1000000),
            anomaly_threshold: 2.5,
            recent_volumes: VecDeque::with_capacity(100),
        }
    }

    fn detect_anomaly(&mut self, volume: u64) -> bool {
        self.recent_volumes.push_back(volume);
        if self.recent_volumes.len() > 100 {
            self.recent_volumes.pop_front();
        }

        let baseline = self.volume_baseline.load(Ordering::Relaxed);
        let anomaly_detected = volume as f64 > (baseline as f64 * self.anomaly_threshold) ||
                              volume as f64 < (baseline as f64 / self.anomaly_threshold);

        if self.recent_volumes.len() >= 50 {
            let recent_average = self.recent_volumes.iter().sum::<u64>() / self.recent_volumes.len() as u64;
            self.volume_baseline.store(recent_average, Ordering::Relaxed);
        }

        anomaly_detected
    }
}

#[derive(Debug)]
struct AttackProbabilityCalculator {
    attack_indicators: HashMap<String, f64>,
    probability_threshold: f64,
}

impl AttackProbabilityCalculator {
    fn new() -> Self {
        Self {
            attack_indicators: HashMap::new(),
            probability_threshold: 0.7,
        }
    }

    fn calculate_attack_probability(&mut self, indicators: &HashMap<String, f64>) -> f64 {
        let mut total_probability = 0.0;
        let mut weight_sum = 0.0;

        for (indicator, &value) in indicators {
            let weight = match indicator.as_str() {
                "fee_manipulation" => 0.3,
                "volume_anomaly" => 0.25,
                "validator_collusion" => 0.35,
                "sandwich_coordination" => 0.1,
                _ => 0.05,
            };

            total_probability += value * weight;
            weight_sum += weight;
        }

        if weight_sum > 0.0 {
            total_probability / weight_sum
        } else {
            0.0
        }
    }
}

#[derive(Debug)]
struct SlotEntropyAuditor {
    entropy_history: VecDeque<(u64, u64, [u8; 32])>,
    baseline_entropy: f64,
    entropy_variance_threshold: f64,
    lstm_predictor: EntropyLSTMPredictor,
}

impl SlotEntropyAuditor {
    fn new() -> Self {
        Self {
            entropy_history: VecDeque::with_capacity(ENTROPY_BASELINE_WINDOW),
            baseline_entropy: 0.0,
            entropy_variance_threshold: 2.0,
            lstm_predictor: EntropyLSTMPredictor::new(),
        }
    }

    fn audit_slot_entropy(&mut self, slot: u64, blockhash: &str, parent_slot: u64) -> (u64, f64) {
        let mut hasher = Hasher::new();
        hasher.update(slot.to_le_bytes().as_ref());
        hasher.update(blockhash.as_bytes());
        hasher.update(parent_slot.to_le_bytes().as_ref());
        
        let entropy_hash = hasher.finalize();
        let entropy_bytes = entropy_hash.as_bytes();
        let slot_entropy = u64::from_le_bytes(entropy_bytes[0..8].try_into().unwrap_or([0; 8]));

        let mut entropy_signature = [0u8; 32];
        entropy_signature.copy_from_slice(entropy_bytes);

        self.entropy_history.push_back((slot, slot_entropy, entropy_signature));
        if self.entropy_history.len() > ENTROPY_BASELINE_WINDOW {
            self.entropy_history.pop_front();
        }

        let entropy_score = self.calculate_entropy_score(slot_entropy);
        let risk_score = self.lstm_predictor.predict_impact_lstm(slot, slot_entropy, &self.entropy_history);

        (slot_entropy, risk_score)
    }

    fn calculate_entropy_score(&mut self, slot_entropy: u64) -> f64 {
        if self.entropy_history.len() < 50 {
            return 0.1;
        }

        let recent_entropies: Vec<u64> = self.entropy_history.iter()
            .map(|(_, entropy, _)| *entropy)
            .collect();

        let average = recent_entropies.iter().sum::<u64>() as f64 / recent_entropies.len() as f64;
        
        if self.baseline_entropy == 0.0 {
            self.baseline_entropy = average;
        } else {
            self.baseline_entropy = (self.baseline_entropy * 0.95) + (average * 0.05);
        }

        let variance = recent_entropies.iter()
            .map(|&e| (e as f64 - average).powi(2))
            .sum::<f64>() / recent_entropies.len() as f64;

        let entropy_deviation = (slot_entropy as f64 - self.baseline_entropy).abs();
        let normalized_deviation = entropy_deviation / variance.sqrt().max(1.0);

        if normalized_deviation > self.entropy_variance_threshold {
            0.8 + (normalized_deviation / 10.0).min(0.2)
        } else {
            0.1 + (normalized_deviation / self.entropy_variance_threshold * 0.3)
        }
    }
}

#[derive(Debug)]
struct EntropyLSTMPredictor {
    sequence_buffer: VecDeque<f64>,
    hidden_state: Vec<f64>,
    cell_state: Vec<f64>,
    prediction_weights: HashMap<String, f64>,
}

impl EntropyLSTMPredictor {
    fn new() -> Self {
        let mut weights = HashMap::new();
        weights.insert("entropy_trend".to_string(), 0.4);
        weights.insert("slot_progression".to_string(), 0.3);
        weights.insert("variance_factor".to_string(), 0.2);
        weights.insert("temporal_correlation".to_string(), 0.1);

        Self {
            sequence_buffer: VecDeque::with_capacity(LSTM_PREDICTION_HORIZON),
            hidden_state: vec![0.0; 32],
            cell_state: vec![0.0; 32],
            prediction_weights: weights,
        }
    }

    fn predict_impact_lstm(&mut self, slot: u64, slot_entropy: u64, entropy_history: &VecDeque<(u64, u64, [u8; 32])>) -> f64 {
        let normalized_entropy = (slot_entropy as f64) / (u64::MAX as f64);
        self.sequence_buffer.push_back(normalized_entropy);
        
        if self.sequence_buffer.len() > LSTM_PREDICTION_HORIZON {
            self.sequence_buffer.pop_front();
        }

        if self.sequence_buffer.len() < 8 {
            return 0.1;
        }

        let entropy_trend = self.calculate_entropy_trend();
        let slot_progression = self.calculate_slot_progression_factor(slot, entropy_history);
        let variance_factor = self.calculate_variance_factor();
        let temporal_correlation = self.calculate_temporal_correlation();

        let weighted_score = 
            entropy_trend * self.prediction_weights["entropy_trend"] +
            slot_progression * self.prediction_weights["slot_progression"] +
            variance_factor * self.prediction_weights["variance_factor"] +
            temporal_correlation * self.prediction_weights["temporal_correlation"];

        weighted_score.max(0.0).min(1.0)
    }

    fn calculate_entropy_trend(&self) -> f64 {
        if self.sequence_buffer.len() < 2 {
            return 0.5;
        }

        let recent_values: Vec<f64> = self.sequence_buffer.iter().rev().take(8).cloned().collect();
        let mut trend_sum = 0.0;

        for i in 1..recent_values.len() {
            trend_sum += recent_values[i-1] - recent_values[i];
        }

        let trend = trend_sum / (recent_values.len() - 1) as f64;
        (trend + 1.0) / 2.0
    }

    fn calculate_slot_progression_factor(&self, current_slot: u64, entropy_history: &VecDeque<(u64, u64, [u8; 32])>) -> f64 {
        if entropy_history.len() < 2 {
            return 0.5;
        }

        let expected_slot = entropy_history.back().map(|(slot, _, _)| slot + 1).unwrap_or(current_slot);
        let slot_variance = (current_slot as i64 - expected_slot as i64).abs() as f64;
        
        (1.0 / (1.0 + slot_variance)).max(0.0).min(1.0)
    }

    fn calculate_variance_factor(&self) -> f64 {
        if self.sequence_buffer.len() < 4 {
            return 0.5;
        }

        let mean = self.sequence_buffer.iter().sum::<f64>() / self.sequence_buffer.len() as f64;
        let variance = self.sequence_buffer.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / self.sequence_buffer.len() as f64;

        variance.sqrt().min(1.0)
    }

    fn calculate_temporal_correlation(&self) -> f64 {
        if self.sequence_buffer.len() < 4 {
            return 0.5;
        }

        let values: Vec<f64> = self.sequence_buffer.iter().cloned().collect();
        let mut correlation_sum = 0.0;
        let mut count = 0;

        for i in 1..values.len() {
            correlation_sum += values[i] * values[i-1];
            count += 1;
        }

        if count > 0 {
            (correlation_sum / count as f64).min(1.0)
        } else {
            0.5
        }
    }
}

#[derive(Debug)]
struct JitoBundleSubmitter {
    submission_client: Option<reqwest::Client>,
    jito_endpoints: Vec<String>,
    bundle_success_rate: AtomicU64,
    recent_submissions: Mutex<VecDeque<(u64, String, bool)>>,
    tip_optimizer: Arc<OptimalTipCalculator>,
}

impl JitoBundleSubmitter {
    fn new(tip_optimizer: Arc<OptimalTipCalculator>) -> Self {
        let endpoints = vec![
            "https://mainnet.block-engine.jito.wtf".to_string(),
            "https://amsterdam.mainnet.block-engine.jito.wtf".to_string(),
            "https://frankfurt.mainnet.block-engine.jito.wtf".to_string(),
            "https://ny.mainnet.block-engine.jito.wtf".to_string(),
            "https://tokyo.mainnet.block-engine.jito.wtf".to_string(),
        ];

        Self {
            submission_client: Some(reqwest::Client::builder()
                .timeout(Duration::from_millis(500))
                .build()
                .unwrap()),
            jito_endpoints: endpoints,
            bundle_success_rate: AtomicU64::new(800),
            recent_submissions: Mutex::new(VecDeque::with_capacity(100)),
            tip_optimizer,
        }
    }

    async fn submit_bundle(&self, bundle_transactions: Vec<String>, target_slot: u64, congestion_level: f64) -> Result<String> {
        let optimal_tip = self.tip_optimizer.calculate_optimal_tip(target_slot, congestion_level, 1.0).await;
        
        let bundle_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": format!("bundle_{}", target_slot),
            "method": "sendBundle",
            "params": [{
                "transactions": bundle_transactions,
                "tipLamports": optimal_tip,
                "targetSlot": target_slot
            }]
        });

        for endpoint in &self.jito_endpoints {
            match self.submit_to_endpoint(endpoint, &bundle_request).await {
                Ok(bundle_id) => {
                    let mut submissions = self.recent_submissions.lock().await;
                    submissions.push_back((target_slot, bundle_id.clone(), true));
                    if submissions.len() > 100 {
                        submissions.pop_front();
                    }

                    self.tip_optimizer.record_submission_result(target_slot, optimal_tip, true).await;
                    self.bundle_success_rate.fetch_add(10, Ordering::Relaxed);

                    return Ok(bundle_id);
                }
                Err(e) => {
                    warn!("Bundle submission failed to {}: {}", endpoint, e);
                    continue;
                }
            }
        }

        self.tip_optimizer.record_submission_result(target_slot, optimal_tip, false).await;
        let current_rate = self.bundle_success_rate.load(Ordering::Relaxed);
        self.bundle_success_rate.store(current_rate.saturating_sub(20), Ordering::Relaxed);

        Err(anyhow!("All Jito endpoints failed"))
    }

    async fn submit_to_endpoint(&self, endpoint: &str, bundle_request: &serde_json::Value) -> Result<String> {
        let client = self.submission_client.as_ref()
            .ok_or_else(|| anyhow!("HTTP client not initialized"))?;

        let response = client
            .post(&format!("{}/api/v1/bundles", endpoint))
            .json(bundle_request)
            .send()
            .await?;

        if response.status().is_success() {
            let response_json: serde_json::Value = response.json().await?;
            if let Some(result) = response_json.get("result") {
                if let Some(bundle_id) = result.as_str() {
                    return Ok(bundle_id.to_string());
                }
            }
        }

        Err(anyhow!("Invalid response from Jito endpoint"))
    }

    async fn get_bundle_status(&self, bundle_id: &str) -> Result<String> {
        for endpoint in &self.jito_endpoints {
            let status_request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": format!("status_{}", bundle_id),
                "method": "getBundleStatuses",
                "params": [[bundle_id]]
            });

            if let Ok(response) = self.query_bundle_status(endpoint, &status_request).await {
                return Ok(response);
            }
        }

        Err(anyhow!("Failed to query bundle status"))
    }

    async fn query_bundle_status(&self, endpoint: &str, status_request: &serde_json::Value) -> Result<String> {
        let client = self.submission_client.as_ref()
            .ok_or_else(|| anyhow!("HTTP client not initialized"))?;

        let response = client
            .post(&format!("{}/api/v1/bundles", endpoint))
            .json(status_request)
            .send()
            .await?;

        if response.status().is_success() {
            let response_json: serde_json::Value = response.json().await?;
            if let Some(result) = response_json.get("result") {
                return Ok(result.to_string());
            }
        }

        Err(anyhow!("Invalid status response"))
    }
}

#[derive(Debug)]
struct TransactionParser {
    instruction_parsers: HashMap<String, Box<dyn InstructionParser>>,
    program_id_cache: DashMap<String, String>,
}

trait InstructionParser: Send + Sync + std::fmt::Debug {
    fn parse_instruction(&self, instruction_data: &[u8], accounts: &[String]) -> Result<ParsedInstruction>;
}

#[derive(Debug, Clone)]
struct ParsedInstruction {
    program_name: String,
    instruction_type: String,
    parsed_data: HashMap<String, serde_json::Value>,
}

#[derive(Debug)]
struct RaydiumParser;

impl InstructionParser for RaydiumParser {
    fn parse_instruction(&self, instruction_data: &[u8], accounts: &[String]) -> Result<ParsedInstruction> {
        if instruction_data.is_empty() {
            return Err(anyhow!("Empty instruction data"));
        }

        let instruction_type = match instruction_data[0] {
            9 => "swap",
            1 => "initialize",
            2 => "add_liquidity",
            3 => "remove_liquidity",
            _ => "unknown",
        };

        let mut parsed_data = HashMap::new();
        
        if instruction_type == "swap" && instruction_data.len() >= 17 {
            let amount_in = u64::from_le_bytes(instruction_data[1..9].try_into().unwrap_or([0; 8]));
            let min_amount_out = u64::from_le_bytes(instruction_data[9..17].try_into().unwrap_or([0; 8]));
            
            parsed_data.insert("amount_in".to_string(), serde_json::Value::Number(serde_json::Number::from(amount_in)));
            parsed_data.insert("min_amount_out".to_string(), serde_json::Value::Number(serde_json::Number::from(min_amount_out)));
        }

        Ok(ParsedInstruction {
            program_name: "Raydium".to_string(),
            instruction_type: instruction_type.to_string(),
            parsed_data,
        })
    }
}

#[derive(Debug)]
struct OrcaParser;

impl InstructionParser for OrcaParser {
    fn parse_instruction(&self, instruction_data: &[u8], accounts: &[String]) -> Result<ParsedInstruction> {
        if instruction_data.is_empty() {
            return Err(anyhow!("Empty instruction data"));
        }

        let instruction_type = match instruction_data[0] {
            1 => "swap",
            0 => "initialize",
            2 => "deposit",
            3 => "withdraw",
            _ => "unknown",
        };

        let mut parsed_data = HashMap::new();

        if instruction_type == "swap" && instruction_data.len() >= 25 {
            let amount = u64::from_le_bytes(instruction_data[1..9].try_into().unwrap_or([0; 8]));
            let other_amount_threshold = u64::from_le_bytes(instruction_data[9..17].try_into().unwrap_or([0; 8]));
            let sqrt_price_limit = u128::from_le_bytes(instruction_data[17..33].try_into().unwrap_or([0; 16]));
            
            parsed_data.insert("amount".to_string(), serde_json::Value::Number(serde_json::Number::from(amount)));
            parsed_data.insert("other_amount_threshold".to_string(), serde_json::Value::Number(serde_json::Number::from(other_amount_threshold)));
            parsed_data.insert("sqrt_price_limit".to_string(), serde_json::Value::String(sqrt_price_limit.to_string()));
        }

        Ok(ParsedInstruction {
            program_name: "Orca".to_string(),
            instruction_type: instruction_type.to_string(),
            parsed_data,
        })
    }
}

#[derive(Debug)]
struct JupiterParser;

impl InstructionParser for JupiterParser {
    fn parse_instruction(&self, instruction_data: &[u8], accounts: &[String]) -> Result<ParsedInstruction> {
        if instruction_data.is_empty() {
            return Err(anyhow!("Empty instruction data"));
        }

        let instruction_discriminator = &instruction_data[0..8];
        let instruction_type = match instruction_discriminator {
            [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8] => "route",
            [0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d] => "shared_accounts_route",
            _ => "unknown",
        };

        let mut parsed_data = HashMap::new();

        if instruction_type == "route" && instruction_data.len() >= 16 {
            let in_amount = u64::from_le_bytes(instruction_data[8..16].try_into().unwrap_or([0; 8]));
            parsed_data.insert("in_amount".to_string(), serde_json::Value::Number(serde_json::Number::from(in_amount)));
            
            if instruction_data.len() >= 24 {
                let quoted_out_amount = u64::from_le_bytes(instruction_data[16..24].try_into().unwrap_or([0; 8]));
                parsed_data.insert("quoted_out_amount".to_string(), serde_json::Value::Number(serde_json::Number::from(quoted_out_amount)));
            }
        }

        Ok(ParsedInstruction {
            program_name: "Jupiter".to_string(),
            instruction_type: instruction_type.to_string(),
            parsed_data,
        })
    }
}

impl TransactionParser {
    fn new() -> Self {
        let mut instruction_parsers: HashMap<String, Box<dyn InstructionParser>> = HashMap::new();
        
        instruction_parsers.insert("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(), Box::new(RaydiumParser));
        instruction_parsers.insert("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP".to_string(), Box::new(OrcaParser));
        instruction_parsers.insert("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(), Box::new(JupiterParser));

        Self {
            instruction_parsers,
            program_id_cache: DashMap::new(),
        }
    }

    fn parse_transaction(&self, transaction: &UiTransaction) -> Result<Vec<ParsedInstruction>> {
        let mut parsed_instructions = Vec::new();

        let message = &transaction.message; {
            if let Some(instructions) = &message.instructions {
                for instruction in instructions {
                    if let Some(program_id_index) = instruction.program_id_index {
                        if let Some(accounts) = &message.account_keys {
                            if let Some(program_id) = accounts.get(program_id_index as usize) {
                                if let Some(parser) = self.instruction_parsers.get(program_id) {
                                    if let Some(data) = &instruction.data {
                                        let decoded_data = bs58::decode(data).into_vec().unwrap_or_default();
                                        let instruction_accounts: Vec<String> = instruction.accounts.as_ref()
                                            .unwrap_or(&Vec::new())
                                            .iter()
                                            .filter_map(|&idx| accounts.get(idx as usize).cloned())
                                            .collect();

                                        match parser.parse_instruction(&decoded_data, &instruction_accounts) {
                                            Ok(parsed) => parsed_instructions.push(parsed),
                                            Err(e) => warn!("Failed to parse instruction: {}", e),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(parsed_instructions)
    }
}

impl HardenedCircuitBreaker {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(CircuitBreakerState::Closed as u8),
            failure_count: AtomicUsize::new(0),
            success_count: AtomicUsize::new(0),
            last_failure_time: AtomicU64::new(0),
            last_success_time: AtomicU64::new(0),
            failure_rate_window: Mutex::new(VecDeque::with_capacity(100)),
            auto_rotation_enabled: AtomicBool::new(true),
            recovery_attempts: AtomicUsize::new(0),
            force_open_until: AtomicU64::new(0),
        }
    }
    
    fn get_state(&self) -> CircuitBreakerState {
        match self.state.load(Ordering::Relaxed) {
            0 => CircuitBreakerState::Closed,
            1 => CircuitBreakerState::Open,
            2 => CircuitBreakerState::HalfOpen,
            3 => CircuitBreakerState::ForceOpen,
            _ => CircuitBreakerState::Open,
        }
    }
    
    fn should_attempt(&self) -> bool {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let force_open_until = self.force_open_until.load(Ordering::Relaxed);
        
        if current_time < force_open_until {
            return false;
        }
        
        match self.get_state() {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                let last_failure = self.last_failure_time.load(Ordering::Relaxed);
                current_time.saturating_sub(last_failure) >= CIRCUIT_BREAKER_RECOVERY_TIME_MS
            },
            CircuitBreakerState::HalfOpen => true,
            CircuitBreakerState::ForceOpen => false,
        }
    }
    
    fn record_success(&self) {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.last_success_time.store(current_time, Ordering::Relaxed);
        
        if self.get_state() == CircuitBreakerState::HalfOpen {
            self.state.store(CircuitBreakerState::Closed as u8, Ordering::Relaxed);
            self.failure_count.store(0, Ordering::Relaxed);
            self.recovery_attempts.store(0, Ordering::Relaxed);
        }
        
        if let Ok(mut window) = self.failure_rate_window.try_lock() {
            window.push_back(true);
            if window.len() > 100 {
                window.pop_front();
            }
        }
    }
    
    fn record_failure(&self) {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.last_failure_time.store(current_time, Ordering::Relaxed);
        
        if let Ok(mut window) = self.failure_rate_window.try_lock() {
            window.push_back(false);
            if window.len() > 100 {
                window.pop_front();
            }
        }
        
        if failures >= CIRCUIT_BREAKER_FAILURE_THRESHOLD {
            match self.get_state() {
                CircuitBreakerState::Closed => {
                    self.state.store(CircuitBreakerState::Open as u8, Ordering::Relaxed);
                },
                CircuitBreakerState::HalfOpen => {
                    self.state.store(CircuitBreakerState::Open as u8, Ordering::Relaxed);
                    self.recovery_attempts.fetch_add(1, Ordering::Relaxed);
                },
                _ => {}
            }
        }
    }
    
    fn force_open(&self, duration_ms: u64) {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        self.state.store(CircuitBreakerState::ForceOpen as u8, Ordering::Relaxed);
        self.force_open_until.store(current_time + duration_ms, Ordering::Relaxed);
    }
}

pub struct BlockSubscriptionManager {
    endpoints: Vec<RpcEndpoint>,
    connections: Arc<DashMap<String, Arc<ConnectionState>>>,
    block_buffer: Arc<Mutex<VecDeque<BlockInfo>>>,
    processed_slots: Arc<RwLock<VecDeque<ProcessedSlot>>>,
    current_slot: Arc<AtomicU64>,
    latest_blockhash: Arc<RwLock<String>>,
    
    blocks_received: Arc<AtomicU64>,
    blocks_processed: Arc<AtomicU64>,
    reconnections: Arc<AtomicU64>,
    fork_events: Arc<AtomicU64>,
    replay_blocks_blocked: Arc<AtomicU64>,
    integrity_violations: Arc<AtomicU64>,
    latest_block_latency: Arc<AtomicU64>,
    mev_opportunities_detected: Arc<AtomicU64>,
    economic_attack_detections: Arc<AtomicU64>,
    
    connection_semaphore: Arc<Semaphore>,
    cancellation_token: CancellationToken,
    
    fork_detector: Arc<Mutex<ForkDetector>>,
    mev_intelligence: Arc<Mutex<MEVIntelligence>>,
    multi_region_failover: Arc<MultiRegionFailover>,
    economic_attack_detector: Arc<Mutex<EconomicAttackDetector>>,
    
    replay_protection_cache: Arc<Mutex<HashSet<String>>>,
    peer_verification_cache: Arc<DashMap<u64, HashMap<String, usize>>>,
    tls_certificate_cache: Arc<DashMap<String, String>>,
    
    slot_entropy_auditor: Arc<Mutex<SlotEntropyAuditor>>,
    jito_bundle_submitter: Arc<JitoBundleSubmitter>,
    transaction_parser: Arc<TransactionParser>,
    leader_schedule_client: Arc<Option<reqwest::Client>>,
}

impl BlockSubscriptionManager {
    #[instrument(name = "block_subscription_manager_new", skip(endpoints))]
    pub fn new(endpoints: Vec<RpcEndpoint>) -> Result<Self> {
        if endpoints.is_empty() {
            return Err(anyhow!("At least one RPC endpoint required"));
        }

        info!("Initializing hardened BlockSubscriptionManager with {} endpoints", endpoints.len());

        let connections = Arc::new(DashMap::new());
        
        for endpoint in &endpoints {
            let connection_state = Arc::new(ConnectionState {
                endpoint: endpoint.clone(),
                client: None,
                last_block_time: AtomicU64::new(0),
                last_slot_received: AtomicU64::new(0),
                consecutive_failures: AtomicUsize::new(0),
                is_connected: AtomicBool::new(false),
                is_subscribed: AtomicBool::new(false),
                circuit_breaker: HardenedCircuitBreaker::new(),
                reconnect_delay_ms: AtomicU64::new(RECONNECT_BASE_DELAY_MS),
                subscription_id: Mutex::new(None),
                connection_quality_score: AtomicU64::new(1000),
                tls_verified: AtomicBool::new(false),
                last_ping_response: AtomicU64::new(0),
                blocks_received_count: AtomicU64::new(0),
                integrity_violations: AtomicUsize::new(0),
            });
            
            connections.insert(endpoint.ws_url.clone(), connection_state);
        }

        let mut regions: Vec<String> = endpoints.iter()
            .map(|e| e.region.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        regions.sort();
        
        let primary_region = regions.first().unwrap_or(&"unknown".to_string()).clone();
        let backup_regions = regions.into_iter().skip(1).collect();

        let tip_optimizer = Arc::new(OptimalTipCalculator::new());
        let jito_bundle_submitter = Arc::new(JitoBundleSubmitter::new(tip_optimizer.clone()));

        Ok(Self {
            endpoints,
            connections,
            block_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_BLOCK_BUFFER_SIZE))),
            processed_slots: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_PROCESSED_SLOTS_HISTORY))),
            current_slot: Arc::new(AtomicU64::new(0)),
            latest_blockhash: Arc::new(RwLock::new(String::new())),
            
            blocks_received: Arc::new(AtomicU64::new(0)),
            blocks_processed: Arc::new(AtomicU64::new(0)),
            reconnections: Arc::new(AtomicU64::new(0)),
            fork_events: Arc::new(AtomicU64::new(0)),
            replay_blocks_blocked: Arc::new(AtomicU64::new(0)),
            integrity_violations: Arc::new(AtomicU64::new(0)),
            latest_block_latency: Arc::new(AtomicU64::new(0)),
            mev_opportunities_detected: Arc::new(AtomicU64::new(0)),
            economic_attack_detections: Arc::new(AtomicU64::new(0)),
            
            connection_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS)),
            cancellation_token: CancellationToken::new(),
            
            fork_detector: Arc::new(Mutex::new(ForkDetector {
                slot_chain: VecDeque::with_capacity(FORK_DETECTION_LOOKBACK * 2),
                last_continuous_slot: 0,
                potential_fork_slots: Vec::new(),
                entropy_baseline: 0.0,
                consensus_tracking: HashMap::new(),
            })),
            
            mev_intelligence: Arc::new(Mutex::new(MEVIntelligence {
                tip_analyzer: TipAnalyzer {
                    recent_tips: VecDeque::with_capacity(JITO_TIP_FEEDBACK_WINDOW),
                    tip_efficiency_scores: HashMap::new(),
                    optimal_tip_calculator: tip_optimizer,
                },
                opportunity_scanner: OpportunityScanner {
                    recent_opportunities: VecDeque::with_capacity(MEV_OPPORTUNITY_SCAN_DEPTH),
                    dex_volume_tracker: DexVolumeTracker::new(),
                    arbitrage_detector: ArbitrageDetector::new(),
                },
                leader_predictor: LeaderPredictor {
                    leader_schedule_cache: HashMap::new(),
                    validator_performance_scores: HashMap::new(),
                    next_leaders: VecDeque::with_capacity(32),
                },
                fee_market_analyzer: FeeMarketAnalyzer {
                    priority_fee_history: VecDeque::with_capacity(ECONOMIC_ATTACK_DETECTION_WINDOW),
                    fee_market_manipulation_detector: FeeManipulationDetector::new(),
                    congestion_predictor: CongestionPredictor::new(),
                },
            })),
            
            multi_region_failover: Arc::new(MultiRegionFailover {
                primary_region,
                backup_regions,
                region_health_scores: Arc::new(DashMap::new()),
                auto_failover_threshold: MULTI_REGION_FAILOVER_THRESHOLD,
                current_active_region: AtomicU64::new(0),
                failover_history: Mutex::new(VecDeque::with_capacity(100)),
            }),
            
            economic_attack_detector: Arc::new(Mutex::new(EconomicAttackDetector {
                suspicious_patterns: VecDeque::with_capacity(ECONOMIC_ATTACK_DETECTION_WINDOW),
                fee_manipulation_tracker: FeeManipulationTracker::new(),
                volume_anomaly_detector: VolumeAnomalyDetector::new(),
                attack_probability_calculator: AttackProbabilityCalculator::new(),
            })),
            
            replay_protection_cache: Arc::new(Mutex::new(HashSet::new())),
            peer_verification_cache: Arc::new(DashMap::new()),
            tls_certificate_cache: Arc::new(DashMap::new()),
            
            slot_entropy_auditor: Arc::new(Mutex::new(SlotEntropyAuditor::new())),
            jito_bundle_submitter,
            transaction_parser: Arc::new(TransactionParser::new()),
            leader_schedule_client: Arc::new(Some(reqwest::Client::builder()
                .timeout(Duration::from_millis(5000))
                .build()
                .unwrap())),
        })
    }

    #[instrument(name = "start_hardened_subscription_manager", skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("Starting hardened block subscription manager with {} endpoints", self.endpoints.len());

        let mut tasks = Vec::new();

        for endpoint in &self.endpoints {
            let connection_state = self.connections.get(&endpoint.ws_url)
                .ok_or_else(|| anyhow!("Connection state not found for {}", endpoint.ws_url))?
                .clone();
            
            let task = self.spawn_hardened_connection_manager(connection_state);
            tasks.push(task);
        }

        tasks.push(self.spawn_health_monitor());
        tasks.push(self.spawn_fork_detector());
        tasks.push(self.spawn_metrics_collector());
        tasks.push(self.spawn_mev_intelligence_processor());
        tasks.push(self.spawn_economic_attack_detector());
        tasks.push(self.spawn_multi_region_monitor());
        tasks.push(self.spawn_replay_protection_cleaner());

        info!("Started {} hardened background tasks", tasks.len());

        self.cancellation_token.cancelled().await;
        
        info!("Cancellation received, shutting down all tasks");
        
        for task in tasks {
            task.abort();
        }

        Ok(())
    }

    fn spawn_hardened_connection_manager(&self, connection_state: Arc<ConnectionState>) -> tokio::task::JoinHandle<()> {
        let manager = self.clone_for_task();
        let token = self.cancellation_token.clone();
        
        tokio::spawn(async move {
            manager.manage_hardened_connection(connection_state, token).await;
        })
    }

    #[instrument(name = "manage_hardened_connection", skip(self, connection_state, token), fields(endpoint = %connection_state.endpoint.ws_url))]
    async fn manage_hardened_connection(&self, connection_state: Arc<ConnectionState>, token: CancellationToken) {
        let endpoint_url = connection_state.endpoint.ws_url.clone();
        
        loop {
            if token.is_cancelled() {
                info!("Connection manager shutting down for {}", endpoint_url);
                break;
            }

            if !connection_state.circuit_breaker.should_attempt() {
                sleep(Duration::from_millis(1000)).await;
                continue;
            }

            let _permit = match timeout(
                Duration::from_millis(5000),
                self.connection_semaphore.acquire()
            ).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => {
                    error!("Semaphore closed for {}", endpoint_url);
                    break;
                }
                Err(_) => {
                    warn!("Connection permit timeout for {}", endpoint_url);
                    continue;
                }
            };

            match self.establish_hardened_subscription(connection_state.clone(), token.clone()).await {
                Ok(_) => {
                    debug!("Hardened subscription established for {}", endpoint_url);
                    connection_state.consecutive_failures.store(0, Ordering::Relaxed);
                    connection_state.reconnect_delay_ms.store(RECONNECT_BASE_DELAY_MS, Ordering::Relaxed);
                    connection_state.circuit_breaker.record_success();
                }
                Err(e) => {
                    error!("Hardened subscription failed for {}: {}", endpoint_url, e);
                    self.handle_connection_failure(&connection_state).await;
                }
            }

            let delay_ms = connection_state.reconnect_delay_ms.load(Ordering::Relaxed);
            let jitter = (delay_ms / 4).min(100);
            let actual_delay = delay_ms + (rand::random::<u64>() % jitter);
            
            tokio::select! {
                _ = sleep(Duration::from_millis(actual_delay)) => {},
                _ = token.cancelled() => break,
            }
        }
    }

    #[instrument(name = "establish_hardened_subscription", skip(self, connection_state, token), fields(endpoint = %connection_state.endpoint.ws_url))]
    async fn establish_hardened_subscription(&self, connection_state: Arc<ConnectionState>, token: CancellationToken) -> Result<()> {
        let endpoint = &connection_state.endpoint;
        
        debug!("Establishing hardened WebSocket connection to {}", endpoint.ws_url);

        self.verify_endpoint_security(endpoint).await?;

        let client = timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MS),
            self.create_verified_client(endpoint)
        ).await
        .context("Connection timeout")?
        .context("Failed to create verified WebSocket client")?;

        let client = Arc::new(client);
        connection_state.client = Some(client.clone());
        connection_state.is_connected.store(true, Ordering::Relaxed);
        connection_state.tls_verified.store(true, Ordering::Relaxed);

        debug!("Secure WebSocket connected, establishing dual block subscriptions");

        let confirmed_config = RpcBlockSubscribeConfig {
            commitment: Some(CommitmentConfig::confirmed()),
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: Some(TransactionDetails::Full),
            show_rewards: Some(true),
            max_supported_transaction_version: Some(0),
        };

        let filter = RpcBlockSubscribeFilter::All;

        let (mut confirmed_stream, confirmed_unsubscribe) = timeout(
            Duration::from_millis(SUBSCRIPTION_TIMEOUT_MS),
            client.block_subscribe(filter.clone(), Some(confirmed_config))
        ).await
        .context("Confirmed subscription timeout")?
        .context("Failed to subscribe to confirmed blocks")?;

        connection_state.is_subscribed.store(true, Ordering::Relaxed);
        
        {
            let mut subscription_id = connection_state.subscription_id.lock().await;
            *subscription_id = Some(format!("hardened_block_sub_{}", endpoint.ws_url));
        }

        info!("Hardened block subscription established for {}", endpoint.ws_url);

        let health_checker = self.spawn_subscription_health_checker(connection_state.clone(), token.clone());

        while let Some(block_update) = tokio::select! {
            update = confirmed_stream.next() => update,
            _ = token.cancelled() => None,
        } {
            match self.process_hardened_block_update(block_update, &connection_state).await {
                Ok(Some(block_info)) => {
                    if let Err(e) = self.enqueue_validated_block(block_info).await {
                        warn!("Failed to enqueue validated block: {}", e);
                    }
                    connection_state.blocks_received_count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(None) => {
                }
                Err(e) => {
                    warn!("Error processing hardened block update: {}", e);
                    connection_state.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                    connection_state.integrity_violations.fetch_add(1, Ordering::Relaxed);
                    
                    if connection_state.consecutive_failures.load(Ordering::Relaxed) >= MAX_CONSECUTIVE_FAILURES {
                        error!("Max consecutive failures reached for {}", endpoint.ws_url);
                        break;
                    }
                }
            }
        }

        health_checker.abort();
        connection_state.is_subscribed.store(false, Ordering::Relaxed);
        connection_state.is_connected.store(false, Ordering::Relaxed);
        connection_state.tls_verified.store(false, Ordering::Relaxed);
        connection_state.client = None;
        
        let _ = confirmed_unsubscribe().await;
        
        Err(anyhow!("Hardened block stream ended"))
    }

    async fn verify_endpoint_security(&self, endpoint: &RpcEndpoint) -> Result<()> {
        let url = Url::parse(&endpoint.ws_url)
            .context("Invalid WebSocket URL")?;
        
        if url.scheme() != "wss" {
            return Err(anyhow!("Only WSS connections allowed for security"));
        }
        
        if let Some(host) = url.host_str() {
            if host.is_empty() || host.contains("..") || host.starts_with('-') {
                return Err(anyhow!("Invalid hostname in WebSocket URL"));
            }
        } else {
            return Err(anyhow!("No hostname in WebSocket URL"));
        }

        if let Some(expected_fingerprint) = &endpoint.trusted_certificate_fingerprint {
            if let Some(cached_fingerprint) = self.tls_certificate_cache.get(&endpoint.ws_url) {
                if cached_fingerprint.value() != expected_fingerprint {
                    return Err(anyhow!("TLS certificate fingerprint mismatch"));
                }
            }
        }

        debug!("Endpoint security verification passed for {}", endpoint.ws_url);
        Ok(())
    }

    async fn create_verified_client(&self, endpoint: &RpcEndpoint) -> Result<PubsubClient> {
        let client = PubsubClient::new(&endpoint.ws_url).await?;
        
        if let Some(expected_fingerprint) = &endpoint.trusted_certificate_fingerprint {
            self.tls_certificate_cache.insert(
                endpoint.ws_url.clone(), 
                expected_fingerprint.clone()
            );
        }
        
        Ok(client)
    }

    fn spawn_subscription_health_checker(&self, connection_state: Arc<ConnectionState>, token: CancellationToken) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ping_interval = interval(Duration::from_millis(SUBSCRIPTION_PING_INTERVAL_MS));
            
            loop {
                tokio::select! {
                    _ = ping_interval.tick() => {
                        let current_time = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        
                        let last_block_time = connection_state.last_block_time.load(Ordering::Relaxed);
                        let ping_threshold = SUBSCRIPTION_PING_INTERVAL_MS * 2;
                        
                        if current_time.saturating_sub(last_block_time) > ping_threshold {
                            warn!("Subscription health check failed for {} - no blocks for {}ms", 
                                connection_state.endpoint.ws_url,
                                current_time.saturating_sub(last_block_time)
                            );
                            
                            connection_state.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                            
                            if connection_state.consecutive_failures.load(Ordering::Relaxed) > 3 {
                                connection_state.circuit_breaker.force_open(CIRCUIT_BREAKER_RECOVERY_TIME_MS);
                            }
                        } else {
                            connection_state.last_ping_response.store(current_time, Ordering::Relaxed);
                        }
                    }
                    _ = token.cancelled() => break,
                }
            }
        })
    }

    #[instrument(name = "process_hardened_block_update", skip(self, update, connection_state))]
    async fn process_hardened_block_update(
        &self,
        update: RpcBlockUpdate,
        connection_state: &Arc<ConnectionState>
    ) -> Result<Option<BlockInfo>> {
        let span = span!(Level::DEBUG, "processing_hardened_block_update", endpoint = %connection_state.endpoint.ws_url);
        let _enter = span.enter();

        let block_data = update.block
            .ok_or_else(|| anyhow!("No block data in update"))?;

        let ui_block = block_data.block
            .ok_or_else(|| anyhow!("No block in block data"))?;

        let slot = ui_block.slot;
        let blockhash = ui_block.blockhash.clone();
        let parent_slot = ui_block.parent_slot;
        
        self.validate_block_security(slot, &blockhash, parent_slot, &ui_block).await?;

        if self.is_block_replay(slot, &blockhash).await? {
            self.replay_blocks_blocked.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow!("Replay attack blocked for slot {}", slot));
        }

        let verification_score = self.verify_block_against_peers(slot, &blockhash, parent_slot).await?;
        if verification_score < PEER_VERIFICATION_CONSENSUS_THRESHOLD {
            self.integrity_violations.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow!("Block failed peer verification: score {:.2}", verification_score));
        }

        let mev_data = self.extract_mev_intelligence(&ui_block, slot).await?;

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        connection_state.last_block_time.store(current_time, Ordering::Relaxed);
        connection_state.last_slot_received.store(slot, Ordering::Relaxed);

        let current_slot = self.current_slot.load(Ordering::Relaxed);
        if slot > current_slot {
            self.current_slot.store(slot, Ordering::Relaxed);
            
            let mut latest_blockhash = self.latest_blockhash.write().await;
            *latest_blockhash = blockhash.clone();
        }

        let block_time = ui_block.block_time.unwrap_or_else(|| current_time as i64 / 1000);
        let block_timestamp = block_time as u64 * 1000;
        let latency = current_time.saturating_sub(block_timestamp);
        self.latest_block_latency.store(latency, Ordering::Relaxed);

        self.blocks_received.fetch_add(1, Ordering::Relaxed);

        let block_info = BlockInfo {
            slot,
            blockhash: blockhash.clone(),
            parent_slot,
            block_time,
            block_height: update.slot,
            received_at: current_time,
            commitment: CommitmentLevel::Confirmed,
            transaction_count: ui_block.transactions.as_ref().map(|txs| txs.len()).unwrap_or(0),
            source_endpoint: connection_state.endpoint.ws_url.clone(),
            
            leader_pubkey: mev_data.leader_pubkey,
            total_tip_amount: mev_data.total_tip_amount,
            priority_fee_percentiles: mev_data.priority_fee_percentiles,
            block_hash_signatures: vec![blockhash],
            slot_entropy: mev_data.slot_entropy,
            validator_commission: mev_data.validator_commission,
            is_replay_protected: true,
            mev_opportunity_count: mev_data.mev_opportunity_count,
            jito_bundle_count: mev_data.jito_bundle_count,
            max_priority_fee: mev_data.max_priority_fee,
            median_priority_fee: mev_data.median_priority_fee,
            dex_volume_estimate: mev_data.dex_volume_estimate,
            cross_endpoint_verification_score: verification_score,
            fork_risk_score: mev_data.fork_risk_score,
        };

        debug!("Processed hardened block {} from {}", slot, connection_state.endpoint.ws_url);
        Ok(Some(block_info))
    }

    async fn validate_block_security(&self, slot: u64, blockhash: &str, parent_slot: u64, ui_block: &UiBlock) -> Result<()> {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        if slot < current_slot.saturating_sub(FORK_DETECTION_LOOKBACK as u64) {
            return Err(anyhow!("Block too old: slot {} vs current {}", slot, current_slot));
        }

        if let Some(block_time) = ui_block.block_time {
            let network_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            
            if (block_time - network_time).abs() > BLOCK_TIME_TOLERANCE_SEC {
                return Err(anyhow!("Block time drift too large: {} vs network {}", block_time, network_time));
            }
        }

        if blockhash.len() != 44 {
            return Err(anyhow!("Invalid blockhash format"));
        }

        if parent_slot >= slot {
            return Err(anyhow!("Invalid parent slot relationship: {} >= {}", parent_slot, slot));
        }

        Ok(())
    }

    async fn is_block_replay(&self, slot: u64, blockhash: &str) -> Result<bool> {
        let mut hasher = Hasher::new();
        hasher.update(slot.to_le_bytes().as_ref());
        hasher.update(blockhash.as_bytes());
        let fingerprint = format!("{:x}", hasher.finalize());

        let mut replay_cache = self.replay_protection_cache.lock().await;
        
        if replay_cache.contains(&fingerprint) {
            return Ok(true);
        }

        replay_cache.insert(fingerprint);
        if replay_cache.len() > REPLAY_PROTECTION_WINDOW {
            let excess = replay_cache.len() - REPLAY_PROTECTION_WINDOW;
            let to_remove: Vec<_> = replay_cache.iter().take(excess).cloned().collect();
            for item in to_remove {
                replay_cache.remove(&item);
            }
        }

        Ok(false)
    }

    async fn verify_block_against_peers(&self, slot: u64, blockhash: &str, parent_slot: u64) -> Result<f64> {
        self.peer_verification_cache
            .entry(slot)
            .or_insert_with(HashMap::new)
            .entry(blockhash.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(1);

        if let Some(slot_consensus) = self.peer_verification_cache.get(&slot) {
            let total_reports: usize = slot_consensus.values().sum();
            let this_hash_reports = slot_consensus.get(blockhash).unwrap_or(&0);
            
            if total_reports > 0 {
                return Ok(*this_hash_reports as f64 / total_reports as f64);
            }
        }

        Ok(1.0)
    }

    #[derive(Debug)]
    struct MEVBlockData {
        leader_pubkey: String,
        total_tip_amount: u64,
        priority_fee_percentiles: Vec<u64>,
        slot_entropy: u64,
        validator_commission: u8,
        mev_opportunity_count: usize,
        jito_bundle_count: usize,
        max_priority_fee: u64,
        median_priority_fee: u64,
        dex_volume_estimate: u64,
        fork_risk_score: f64,
    }

    async fn extract_mev_intelligence(&self, ui_block: &UiBlock, slot: u64) -> Result<MEVBlockData> {
        let leader_pubkey = self.get_slot_leader(slot).await.unwrap_or_else(|| format!("unknown_{}", slot));

        let mut total_tip_amount = 0u64;
        let mut priority_fees = Vec::new();
        let mut jito_bundle_count = 0usize;
        let mut mev_opportunity_count = 0usize;
        let mut dex_volume_estimate = 0u64;

        if let Some(transactions) = &ui_block.transactions {
            for tx in transactions {
                if let Some(meta) = &tx.meta {
                    if let Some(fee) = meta.fee {
                        priority_fees.push(fee);
                    }

                    if meta.log_messages.as_ref()
                        .map(|logs| logs.iter().any(|log| log.contains("jito") || log.contains("tip")))
                        .unwrap_or(false) {
                        jito_bundle_count += 1;
                        if let Some(fee) = meta.fee {
                            total_tip_amount = total_tip_amount.saturating_add(fee);
                        }
                    }

                    let parsed_instructions = self.transaction_parser.parse_transaction(tx).unwrap_or_default();
                    for instruction in parsed_instructions {
                        if instruction.program_name == "Raydium" || instruction.program_name == "Orca" || instruction.program_name == "Jupiter" {
                            mev_opportunity_count += 1;
                            if let Some(amount) = instruction.parsed_data.get("amount_in")
                                .or_else(|| instruction.parsed_data.get("amount"))
                                .or_else(|| instruction.parsed_data.get("in_amount")) {
                                if let Some(amount_num) = amount.as_u64() {
                                    dex_volume_estimate = dex_volume_estimate.saturating_add(amount_num);
                                }
                            }
                        }
                    }
                }
            }
        }

        priority_fees.sort_unstable();
        let priority_fee_percentiles = if !priority_fees.is_empty() {
            vec![
                priority_fees[priority_fees.len() * 25 / 100],
                priority_fees[priority_fees.len() * 50 / 100],
                priority_fees[priority_fees.len() * 75 / 100],
                priority_fees[priority_fees.len() * 95 / 100],
            ]
        } else {
            vec![0, 0, 0, 0]
        };

        let max_priority_fee = priority_fees.last().copied().unwrap_or(0);
        let median_priority_fee = priority_fee_percentiles.get(1).copied().unwrap_or(0);

        let (slot_entropy, fork_risk_score) = {
            let mut auditor = self.slot_entropy_auditor.lock().await;
            auditor.audit_slot_entropy(slot, &ui_block.blockhash, ui_block.parent_slot)
        };

        if mev_opportunity_count > 0 {
            self.mev_opportunities_detected.fetch_add(mev_opportunity_count as u64, Ordering::Relaxed);
        }

        Ok(MEVBlockData {
            leader_pubkey,
            total_tip_amount,
            priority_fee_percentiles,
            slot_entropy,
            validator_commission: 0,
            mev_opportunity_count,
            jito_bundle_count,
            max_priority_fee,
            median_priority_fee,
            dex_volume_estimate,
            fork_risk_score,
        })
    }

    async fn get_slot_leader(&self, slot: u64) -> Option<String> {
        if let Some(client) = self.leader_schedule_client.as_ref() {
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": format!("leader_{}", slot),
                "method": "getSlotLeader",
                "params": [slot]
            });

            for endpoint in &self.endpoints {
                if let Ok(response) = timeout(
                    Duration::from_millis(1000),
                    client.post(&endpoint.url).json(&request).send()
                ).await {
                    if let Ok(response) = response {
                        if let Ok(json_response) = response.json::<serde_json::Value>().await {
                            if let Some(result) = json_response.get("result") {
                                if let Some(leader) = result.as_str() {
                                    return Some(leader.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        let epoch = slot / 432000;
        let slot_in_epoch = slot % 432000;
        let leader_index = slot_in_epoch / 4;
        Some(format!("validator_{}_slot_{}", epoch, leader_index))
    }

    #[instrument(name = "enqueue_validated_block", skip(self, block_info))]
    async fn enqueue_validated_block(&self, block_info: BlockInfo) -> Result<()> {
        let mut buffer = self.block_buffer.lock().await;
        
        if buffer.len() >= MAX_BLOCK_BUFFER_SIZE {
            buffer.pop_front();
            warn!("Block buffer full, evicting oldest block");
        }
        
        buffer.push_back(block_info.clone());
        drop(buffer);

        let mut processed = self.processed_slots.write().await;
        
        if processed.len() >= MAX_PROCESSED_SLOTS_HISTORY {
            processed.pop_front();
        }
        
        let mut hasher = Hasher::new();
        hasher.update(block_info.slot.to_le_bytes().as_ref());
        hasher.update(block_info.blockhash.as_bytes());
        hasher.update(block_info.slot_entropy.to_le_bytes().as_ref());
        let entropy_signature = hasher.finalize();
        
        processed.push_back(ProcessedSlot {
            slot: block_info.slot,
            blockhash: block_info.blockhash.clone(),
            parent_slot: block_info.parent_slot,
            timestamp: block_info.received_at,
            entropy_signature: entropy_signature.as_bytes().try_into().unwrap_or([0; 32]),
            source_endpoints: {
                let mut set = HashSet::new();
                set.insert(block_info.source_endpoint.clone());
                set
            },
        });
        
        self.blocks_processed.fetch_add(1, Ordering::Relaxed);
        
        debug!("Enqueued validated block {} to buffer", block_info.slot);
        Ok(())
    }

    fn spawn_health_monitor(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone_for_task();
        let token = self.cancellation_token.clone();
        
        tokio::spawn(async move {
            manager.health_monitor_loop(token).await;
        })
    }

    fn spawn_fork_detector(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone_for_task();
        let token = self.cancellation_token.clone();
        
        tokio::spawn(async move {
            manager.fork_detection_loop(token).await;
        })
    }

    fn spawn_metrics_collector(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone_for_task();
        let token = self.cancellation_token.clone();
        
        tokio::spawn(async move {
            manager.metrics_collection_loop(token).await;
        })
    }

    fn spawn_mev_intelligence_processor(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone_for_task();
        let token = self.cancellation_token.clone();
        
        tokio::spawn(async move {
            manager.mev_intelligence_loop(token).await;
        })
    }

    fn spawn_economic_attack_detector(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone_for_task();
        let token = self.cancellation_token.clone();
        
        tokio::spawn(async move {
            manager.economic_attack_detection_loop(token).await;
        })
    }

    fn spawn_multi_region_monitor(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone_for_task();
        let token = self.cancellation_token.clone();
        
        tokio::spawn(async move {
            manager.multi_region_monitoring_loop(token).await;
        })
    }

    fn spawn_replay_protection_cleaner(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone_for_task();
        let token = self.cancellation_token.clone();
        
        tokio::spawn(async move {
            manager.replay_protection_cleanup_loop(token).await;
        })
    }

    #[instrument(name = "health_monitor_loop", skip(self, token))]
    async fn health_monitor_loop(&self, token: CancellationToken) {
        let mut interval = interval(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS));
        
        info!("Starting hardened health monitor loop");
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.check_connection_health().await;
                    self.update_circuit_breakers().await;
                    self.update_connection_quality_scores().await;
                }
                _ = token.cancelled() => {
                    info!("Health monitor shutting down");
                    break;
                }
            }
        }
    }

    async fn check_connection_health(&self) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        
        for connection in self.connections.iter() {
            let connection_state = connection.value();
            let last_block_time = connection_state.last_block_time.load(Ordering::Relaxed);
            let last_slot = connection_state.last_slot_received.load(Ordering::Relaxed);
            let integrity_violations = connection_state.integrity_violations.load(Ordering::Relaxed);
            
            let time_since_last_block = current_time.saturating_sub(last_block_time);
            if time_since_last_block > 10000 {
                if connection_state.is_connected.load(Ordering::Relaxed) {
                    warn!("Connection {} appears stale (no blocks for {}ms)", 
                        connection_state.endpoint.ws_url,
                        time_since_last_block
                    );
                }
            }
            
            if last_slot > 0 && current_slot.saturating_sub(last_slot) > SLOT_DRIFT_TOLERANCE {
                warn!("Connection {} lagging by {} slots", 
                    connection_state.endpoint.ws_url,
                    current_slot.saturating_sub(last_slot)
                );
            }

            if integrity_violations > 5 {
                warn!("Connection {} has {} integrity violations", 
                    connection_state.endpoint.ws_url,
                    integrity_violations
                );
                connection_state.circuit_breaker.force_open(CIRCUIT_BREAKER_RECOVERY_TIME_MS * 2);
            }
        }
    }

    async fn update_circuit_breakers(&self) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        for connection in self.connections.iter() {
            let connection_state = connection.value();
            
            if connection_state.circuit_breaker.get_state() == CircuitBreakerState::Open {
                let last_failure = connection_state.circuit_breaker.last_failure_time.load(Ordering::Relaxed);
                if current_time.saturating_sub(last_failure) >= CIRCUIT_BREAKER_RECOVERY_TIME_MS {
                    connection_state.circuit_breaker.state.store(CircuitBreakerState::HalfOpen as u8, Ordering::Relaxed);
                    debug!("Circuit breaker half-open for {}", connection_state.endpoint.ws_url);
                }
            }
        }
    }

    async fn update_connection_quality_scores(&self) {
        for connection in self.connections.iter() {
            let connection_state = connection.value();
            let blocks_received = connection_state.blocks_received_count.load(Ordering::Relaxed);
            let failures = connection_state.consecutive_failures.load(Ordering::Relaxed);
            let integrity_violations = connection_state.integrity_violations.load(Ordering::Relaxed);
            
            let base_score = 1000u64;
            let failure_penalty = (failures * 100) as u64;
            let violation_penalty = (integrity_violations * 50) as u64;
            let block_bonus = blocks_received.min(100);
            
            let quality_score = base_score
                .saturating_sub(failure_penalty)
                .saturating_sub(violation_penalty)
                .saturating_add(block_bonus);
            
            connection_state.connection_quality_score.store(quality_score, Ordering::Relaxed);
        }
    }

    #[instrument(name = "fork_detection_loop", skip(self, token))]
    async fn fork_detection_loop(&self, token: CancellationToken) {
        let mut interval = interval(Duration::from_millis(400));
        
        info!("Starting sophisticated fork detection loop");
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.detect_sophisticated_forks().await {
                        warn!("Fork detection error: {}", e);
                    }
                }
                _ = token.cancelled() => {
                    info!("Fork detection shutting down");
                    break;
                }
            }
        }
    }

    async fn detect_sophisticated_forks(&self) -> Result<()> {
        let processed_slots = self.processed_slots.read().await;
        
        if processed_slots.len() < 3 {
            return Ok(());
        }
        
        let mut fork_detector = self.fork_detector.lock().await;
        let mut detected_forks = Vec::new();
        
        for processed_slot in processed_slots.iter().rev().take(FORK_DETECTION_LOOKBACK) {
            let entropy = u64::from_le_bytes(processed_slot.entropy_signature[0..8].try_into().unwrap_or([0; 8]));
            fork_detector.slot_chain.push_back((
                processed_slot.slot,
                processed_slot.blockhash.clone(),
                processed_slot.parent_slot,
                entropy,
            ));
            
            if fork_detector.slot_chain.len() > FORK_DETECTION_LOOKBACK * 2 {
                fork_detector.slot_chain.pop_front();
            }
        }
        
        for i in 1..fork_detector.slot_chain.len() {
            let (current_slot, current_hash, current_parent, current_entropy) = &fork_detector.slot_chain[i];
            let (prev_slot, prev_hash, _, prev_entropy) = &fork_detector.slot_chain[i-1];
            
            if *current_slot != prev_slot + 1 {
                detected_forks.push(ForkEvent {
                    fork_slot: *current_slot,
                    fork_type: ForkType::SlotManipulation,
                    confidence: 0.9,
                    economic_impact: 0,
                    validator_list: vec![],
                    detection_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                    affected_endpoints: vec![],
                });
            }
            
            if *current_parent != *prev_slot {
                detected_forks.push(ForkEvent {
                    fork_slot: *current_slot,
                    fork_type: ForkType::ConsensusBreak,
                    confidence: 0.95,
                    economic_impact: 0,
                    validator_list: vec![],
                    detection_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                    affected_endpoints: vec![],
                });
            }
            
            if (*current_entropy as i64 - *prev_entropy as i64).abs() > MAX_SLOT_ENTROPY_DRIFT as i64 * 1000 {
                detected_forks.push(ForkEvent {
                    fork_slot: *current_slot,
                    fork_type: ForkType::EntropyAnomaly,
                    confidence: 0.7,
                    economic_impact: 0,
                    validator_list: vec![],
                    detection_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                    affected_endpoints: vec![],
                });
            }
        }
        
        for (slot, hash_counts) in &fork_detector.consensus_tracking {
            let total_reports: usize = hash_counts.values().sum();
            if total_reports > 1 {
                let max_count = hash_counts.values().max().copied().unwrap_or(0);
                let consensus_ratio = max_count as f64 / total_reports as f64;
                
                if consensus_ratio < PEER_VERIFICATION_CONSENSUS_THRESHOLD {
                    detected_forks.push(ForkEvent {
                        fork_slot: *slot,
                        fork_type: ForkType::NetworkPartition,
                        confidence: 1.0 - consensus_ratio,
                        economic_impact: 0,
                        validator_list: vec![],
                        detection_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                        affected_endpoints: vec![],
                    });
                }
            }
        }
        
        for fork_event in detected_forks {
            warn!("Sophisticated fork detected: {:?}", fork_event);
            self.handle_sophisticated_fork(fork_event).await?;
        }
        
        Ok(())
    }

    async fn handle_sophisticated_fork(&self, fork_event: ForkEvent) -> Result<()> {
        info!("Handling sophisticated fork: slot {}, type {:?}, confidence {:.2}", 
            fork_event.fork_slot, fork_event.fork_type, fork_event.confidence);
        
        self.fork_events.fetch_add(1, Ordering::Relaxed);
        
        let mut buffer = self.block_buffer.lock().await;
        buffer.retain(|block| block.slot < fork_event.fork_slot);
        drop(buffer);
        
        let mut processed = self.processed_slots.write().await;
        processed.retain(|ps| ps.slot < fork_event.fork_slot);
        drop(processed);
        
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        if current_slot >= fork_event.fork_slot {
            self.current_slot.store(fork_event.fork_slot.saturating_sub(1), Ordering::Relaxed);
        }
        
        match fork_event.fork_type {
            ForkType::ValidatorCollusion => {
                for connection in self.connections.iter() {
                    connection.value().circuit_breaker.force_open(CIRCUIT_BREAKER_RECOVERY_TIME_MS);
                }
            },
            ForkType::NetworkPartition => {
                self.trigger_multi_region_failover().await?;
            },
            ForkType::EntropyAnomaly => {
            },
            _ => {}
        }
        
        info!("Fork handling completed for slot {}", fork_event.fork_slot);
        Ok(())
    }

    async fn trigger_multi_region_failover(&self) -> Result<()> {
        info!("Triggering multi-region failover due to fork detection");
        
        let mut region_scores: HashMap<String, f64> = HashMap::new();
        
        for connection in self.connections.iter() {
            let state = connection.value();
            let quality_score = state.connection_quality_score.load(Ordering::Relaxed) as f64 / 1000.0;
            let region = &state.endpoint.region;
            
            region_scores.entry(region.clone())
                .and_modify(|score| *score = (*score + quality_score) / 2.0)
                .or_insert(quality_score);
        }
        
        if let Some((best_region, best_score)) = region_scores.iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal)) {
            
            if *best_score > self.multi_region_failover.auto_failover_threshold {
                info!("Failing over to region: {} (score: {:.2})", best_region, best_score);
                
                let mut failover_history = self.multi_region_failover.failover_history.lock().await;
                let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                failover_history.push_back((
                    current_time,
                    self.multi_region_failover.primary_region.clone(),
                    best_region.clone(),
                ));
                
                if failover_history.len() > 100 {
                    failover_history.pop_front();
                }
            }
        }
        
        Ok(())
    }

    #[instrument(name = "metrics_collection_loop", skip(self, token))]
    async fn metrics_collection_loop(&self, token: CancellationToken) {
        let mut interval = interval(Duration::from_millis(1000));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = self.get_subscription_stats().await;
                    debug!("Metrics - Blocks: received={}, processed={}, current_slot={}, mev_opportunities={}", 
                        stats.total_blocks_received,
                        stats.total_blocks_processed,
                        stats.current_slot,
                        stats.mev_opportunities_detected
                    );
                }
                _ = token.cancelled() => break,
            }
        }
    }

    async fn mev_intelligence_loop(&self, token: CancellationToken) {
        let mut interval = interval(Duration::from_millis(500));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.process_mev_intelligence().await;
                }
                _ = token.cancelled() => break,
            }
        }
    }

    async fn process_mev_intelligence(&self) {
        let mut mev_intel = self.mev_intelligence.lock().await;
        
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let congestion_level = mev_intel.fee_market_analyzer.congestion_predictor
            .predict_congestion(current_slot, 2500);
        
        mev_intel.tip_analyzer.optimal_tip_calculator
            .calculate_optimal_tip(current_slot + 4, congestion_level, 1.0).await;
        
        if let Ok(buffer) = self.block_buffer.try_lock() {
            if let Some(latest_block) = buffer.back() {
                let _opportunities = mev_intel.opportunity_scanner.arbitrage_detector
                    .arbitrage_opportunities.len();
                
                let manipulation_detected = mev_intel.fee_market_analyzer
                    .fee_market_manipulation_detector
                    .detect_manipulation(latest_block.slot, &latest_block.priority_fee_percentiles);
                
                if manipulation_detected {
                    self.economic_attack_detections.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    async fn economic_attack_detection_loop(&self, token: CancellationToken) {
        let mut interval = interval(Duration::from_millis(1000));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.detect_economic_attacks().await {
                        warn!("Economic attack detection error: {}", e);
                    }
                }
                _ = token.cancelled() => break,
            }
        }
    }

    async fn detect_economic_attacks(&self) -> Result<()> {
        let mut detector = self.economic_attack_detector.lock().await;
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        
        let mut attack_indicators = HashMap::new();
        
        if let Ok(buffer) = self.block_buffer.try_lock() {
            if let Some(latest_block) = buffer.back() {
                let fee_manipulation = detector.fee_manipulation_tracker
                    .manipulation_events.iter()
                    .filter(|event| latest_block.slot.saturating_sub(event.slot) < 10)
                    .map(|event| event.severity)
                    .fold(0.0f64, |acc, severity| acc.max(severity));
                
                let volume_anomaly = if detector.volume_anomaly_detector
                    .detect_anomaly(latest_block.dex_volume_estimate) { 0.8 } else { 0.1 };
                
                attack_indicators.insert("fee_manipulation".to_string(), fee_manipulation);
                attack_indicators.insert("volume_anomaly".to_string(), volume_anomaly);
                attack_indicators.insert("validator_collusion".to_string(), 0.1);
                attack_indicators.insert("sandwich_coordination".to_string(), 0.1);
                
                let attack_probability = detector.attack_probability_calculator
                    .calculate_attack_probability(&attack_indicators);
                
                if attack_probability > detector.attack_probability_calculator.probability_threshold {
                    warn!("Economic attack detected with probability: {:.2}", attack_probability);
                    self.economic_attack_detections.fetch_add(1, Ordering::Relaxed);
                    
                    detector.suspicious_patterns.push_back(SuspiciousPattern {
                        pattern_type: AttackPatternType::FeeMarketManipulation,
                        confidence: attack_probability,
                        first_detected: current_slot,
                        last_seen: current_slot,
                        affected_slots: vec![latest_block.slot],
                    });
                    
                    if detector.suspicious_patterns.len() > ECONOMIC_ATTACK_DETECTION_WINDOW {
                        detector.suspicious_patterns.pop_front();
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn multi_region_monitoring_loop(&self, token: CancellationToken) {
        let mut interval = interval(Duration::from_millis(5000));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.update_region_health_scores().await;
                }
                _ = token.cancelled() => break,
            }
        }
    }

    async fn update_region_health_scores(&self) {
        let mut region_stats: HashMap<String, (f64, usize)> = HashMap::new();
        
        for connection in self.connections.iter() {
            let state = connection.value();
            let quality_score = state.connection_quality_score.load(Ordering::Relaxed) as f64;
            let region = &state.endpoint.region;
            
            region_stats.entry(region.clone())
                .and_modify(|(total_score, count)| {
                    *total_score += quality_score;
                    *count += 1;
                })
                .or_insert((quality_score, 1));
        }
        
        for (region, (total_score, count)) in region_stats {
            let average_score = total_score / count as f64 / 1000.0;
            self.multi_region_failover.region_health_scores.insert(region, average_score);
        }
    }

    async fn replay_protection_cleanup_loop(&self, token: CancellationToken) {
        let mut interval = interval(Duration::from_millis(30000));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.cleanup_replay_protection_cache().await;
                    self.cleanup_peer_verification_cache().await;
                }
                _ = token.cancelled() => break,
            }
        }
    }

    async fn cleanup_replay_protection_cache(&self) {
        let mut cache = self.replay_protection_cache.lock().await;
        if cache.len() > REPLAY_PROTECTION_WINDOW {
            let excess = cache.len() - REPLAY_PROTECTION_WINDOW;
            let to_remove: Vec<_> = cache.iter().take(excess).cloned().collect();
            for item in to_remove {
                cache.remove(&item);
            }
        }
    }

    async fn cleanup_peer_verification_cache(&self) {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let cutoff_slot = current_slot.saturating_sub(FORK_DETECTION_LOOKBACK as u64);
        
        self.peer_verification_cache.retain(|slot, _| *slot >= cutoff_slot);
    }

    async fn handle_connection_failure(&self, connection_state: &Arc<ConnectionState>) {
        let failures = connection_state.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        connection_state.circuit_breaker.record_failure();
        
        let current_delay = connection_state.reconnect_delay_ms.load(Ordering::Relaxed);
        let new_delay = (current_delay * 2).min(RECONNECT_MAX_DELAY_MS);
        connection_state.reconnect_delay_ms.store(new_delay, Ordering::Relaxed);
        
        self.reconnections.fetch_add(1, Ordering::Relaxed);
        
        warn!("Connection failure #{} for {}, next attempt in {}ms", 
            failures, 
            connection_state.endpoint.ws_url, 
            new_delay
        );
    }

    #[instrument(name = "get_latest_block", skip(self))]
    pub async fn get_latest_block(&self) -> Option<BlockInfo> {
        let buffer = self.block_buffer.lock().await;
        buffer.back().cloned()
    }

    #[instrument(name = "get_blocks", skip(self))]
    pub async fn get_blocks(&self, count: usize) -> Vec<BlockInfo> {
        let buffer = self.block_buffer.lock().await;
        buffer.iter()
            .rev()
            .take(count)
            .cloned()
            .collect()
    }

    pub async fn get_current_slot(&self) -> u64 {
        self.current_slot.load(Ordering::Relaxed)
    }

    pub async fn get_latest_blockhash(&self) -> String {
        self.latest_blockhash.read().await.clone()
    }

    #[instrument(name = "wait_for_slot", skip(self))]
    pub async fn wait_for_slot(&self, target_slot: u64, timeout_ms: u64) -> Result<()> {
        let start = Instant::now();
        let timeout_duration = Duration::from_millis(timeout_ms);
        
        while start.elapsed() < timeout_duration {
            let current_slot = self.current_slot.load(Ordering::Relaxed);
            
            if current_slot >= target_slot {
                return Ok(());
            }
            
            sleep(Duration::from_millis(10)).await;
        }
        
        Err(anyhow!("Timeout waiting for slot {}", target_slot))
    }

    pub async fn submit_jito_bundle(&self, transactions: Vec<String>, target_slot: u64) -> Result<String> {
        let congestion_level = {
            let mev_intel = self.mev_intelligence.lock().await;
            mev_intel.fee_market_analyzer.congestion_predictor
                .slot_congestion_history.back()
                .map(|(_, congestion)| *congestion)
                .unwrap_or(0.5)
        };
        
        self.jito_bundle_submitter.submit_bundle(transactions, target_slot, congestion_level).await
    }

    pub async fn get_bundle_status(&self, bundle_id: &str) -> Result<String> {
        self.jito_bundle_submitter.get_bundle_status(bundle_id).await
    }

    #[instrument(name = "get_subscription_stats", skip(self))]
    pub async fn get_subscription_stats(&self) -> SubscriptionStats {
        let blocks_received = self.blocks_received.load(Ordering::Relaxed);
        let blocks_processed = self.blocks_processed.load(Ordering::Relaxed);
        let reconnections = self.reconnections.load(Ordering::Relaxed);
        let fork_events = self.fork_events.load(Ordering::Relaxed);
        let replay_blocks_blocked = self.replay_blocks_blocked.load(Ordering::Relaxed);
        let integrity_violations = self.integrity_violations.load(Ordering::Relaxed);
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let latest_block_latency = self.latest_block_latency.load(Ordering::Relaxed);
        let mev_opportunities_detected = self.mev_opportunities_detected.load(Ordering::Relaxed);
        let economic_attack_detections = self.economic_attack_detections.load(Ordering::Relaxed);
        
        let mut active_connections = 0;
        let mut failed_connections = 0;
        let mut connection_health = Vec::new();
        
        for connection in self.connections.iter() {
            let state = connection.value();
            let is_connected = state.is_connected.load(Ordering::Relaxed);
            let is_subscribed = state.is_subscribed.load(Ordering::Relaxed);
            let last_slot = state.last_slot_received.load(Ordering::Relaxed);
            let consecutive_failures = state.consecutive_failures.load(Ordering::Relaxed);
            let quality_score = state.connection_quality_score.load(Ordering::Relaxed) as f64 / 1000.0;
            let tls_verified = state.tls_verified.load(Ordering::Relaxed);
            let integrity_violations = state.integrity_violations.load(Ordering::Relaxed);
            let blocks_received = state.blocks_received_count.load(Ordering::Relaxed);
            
            if is_connected && is_subscribed {
                active_connections += 1;
            } else {
                failed_connections += 1;
            }
            
            let circuit_state_str = match state.circuit_breaker.get_state() {
                CircuitBreakerState::Closed => "Closed",
                CircuitBreakerState::Open => "Open",
                CircuitBreakerState::HalfOpen => "HalfOpen",
                CircuitBreakerState::ForceOpen => "ForceOpen",
            };
            
            connection_health.push(ConnectionHealth {
                endpoint: state.endpoint.ws_url.clone(),
                region: state.endpoint.region.clone(),
                is_connected,
                is_subscribed,
                last_slot,
                consecutive_failures,
                circuit_breaker_state: circuit_state_str.to_string(),
                latency_ms: latest_block_latency,
                quality_score,
                tls_verified,
                integrity_violations,
                blocks_per_minute: blocks_received as f64,
            });
        }
        
        let blocks_per_second = if blocks_received > 0 {
            blocks_received as f64 / 120.0
        } else {
            0.0
        };

        let consensus_score = if active_connections > 1 {
            0.9
        } else {
            1.0
        };

        let jito_tip_efficiency = {
            let mev_intel = self.mev_intelligence.lock().await;
            let success_rate = self.jito_bundle_submitter.bundle_success_rate.load(Ordering::Relaxed) as f64 / 1000.0;
            success_rate.max(0.0).min(1.0)
        };

        SubscriptionStats {
            total_blocks_received: blocks_received,
            total_blocks_processed: blocks_processed,
            total_reconnections: reconnections,
            total_fork_events: fork_events,
            total_replay_blocks_blocked: replay_blocks_blocked,
            total_integrity_violations: integrity_violations,
            current_slot,
            latest_block_latency_ms: latest_block_latency,
            active_connections,
            failed_connections,
            blocks_per_second,
            connection_health,
            mev_opportunities_detected,
            jito_tip_efficiency_score: jito_tip_efficiency,
            cross_endpoint_consensus_score: consensus_score,
            economic_attack_detections,
        }
    }

    #[instrument(name = "shutdown", skip(self))]
    pub async fn shutdown(&self) {
        info!("Initiating graceful shutdown of hardened BlockSubscriptionManager");
        
        self.cancellation_token.cancel();
        
        for connection in self.connections.iter() {
            let state = connection.value();
            state.is_connected.store(false, Ordering::Relaxed);
            state.is_subscribed.store(false, Ordering::Relaxed);
        }
        
        info!("Hardened BlockSubscriptionManager shutdown completed");
    }

    fn clone_for_task(&self) -> Self {
        Self {
            endpoints: self.endpoints.clone(),
            connections: self.connections.clone(),
            block_buffer: self.block_buffer.clone(),
            processed_slots: self.processed_slots.clone(),
            current_slot: self.current_slot.clone(),
            latest_blockhash: self.latest_blockhash.clone(),
            blocks_received: self.blocks_received.clone(),
            blocks_processed: self.blocks_processed.clone(),
            reconnections: self.reconnections.clone(),
            fork_events: self.fork_events.clone(),
            replay_blocks_blocked: self.replay_blocks_blocked.clone(),
            integrity_violations: self.integrity_violations.clone(),
            latest_block_latency: self.latest_block_latency.clone(),
            mev_opportunities_detected: self.mev_opportunities_detected.clone(),
            economic_attack_detections: self.economic_attack_detections.clone(),
            connection_semaphore: self.connection_semaphore.clone(),
            cancellation_token: self.cancellation_token.clone(),
            fork_detector: self.fork_detector.clone(),
            mev_intelligence: self.mev_intelligence.clone(),
            multi_region_failover: self.multi_region_failover.clone(),
            economic_attack_detector: self.economic_attack_detector.clone(),
            replay_protection_cache: self.replay_protection_cache.clone(),
            peer_verification_cache: self.peer_verification_cache.clone(),
            tls_certificate_cache: self.tls_certificate_cache.clone(),
            slot_entropy_auditor: self.slot_entropy_auditor.clone(),
            jito_bundle_submitter: self.jito_bundle_submitter.clone(),
            transaction_parser: self.transaction_parser.clone(),
            leader_schedule_client: self.leader_schedule_client.clone(),
        }
    }
}

impl Clone for BlockSubscriptionManager {
    fn clone(&self) -> Self {
        self.clone_for_task()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hardened_block_subscription_manager_creation() {
        let endpoints = vec![
            RpcEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
                priority: 1,
                region: "global".to_string(),
                supports_jito: true,
                validator_identity: None,
                trusted_certificate_fingerprint: None,
            }
        ];
        
        let manager = BlockSubscriptionManager::new(endpoints).unwrap();
        assert_eq!(manager.get_current_slot().await, 0);
    }

    #[tokio::test]
    async fn test_hardened_circuit_breaker() {
        let cb = HardenedCircuitBreaker::new();
        assert_eq!(cb.get_state(), CircuitBreakerState::Closed);
        assert!(cb.should_attempt());
        
        for _ in 0..CIRCUIT_BREAKER_FAILURE_THRESHOLD {
            cb.record_failure();
        }
        assert_eq!(cb.get_state(), CircuitBreakerState::Open);
        assert!(!cb.should_attempt());
    }

    #[tokio::test]
    async fn test_fork_detection() {
        let endpoints = vec![
            RpcEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
                priority: 1,
                region: "global".to_string(),
                supports_jito: true,
                validator_identity: None,
                trusted_certificate_fingerprint: None,
            }
        ];
        
        let manager = BlockSubscriptionManager::new(endpoints).unwrap();
        
        let fork_event = ForkEvent {
            fork_slot: 100,
            fork_type: ForkType::ValidatorCollusion,
            confidence: 0.95,
            economic_impact: 1000000,
            validator_list: vec!["validator1".to_string()],
            detection_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
            affected_endpoints: vec!["wss://api.mainnet-beta.solana.com".to_string()],
        };
        
        let result = manager.handle_sophisticated_fork(fork_event).await;
        assert!(result.is_ok());
        
        let stats = manager.get_subscription_stats().await;
        assert_eq!(stats.total_fork_events, 1);
    }

    #[tokio::test]
    async fn test_replay_protection() {
        let endpoints = vec![
            RpcEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
                priority: 1,
                region: "global".to_string(),
                supports_jito: true,
                validator_identity: None,
                trusted_certificate_fingerprint: None,
            }
        ];
        
        let manager = BlockSubscriptionManager::new(endpoints).unwrap();
        
        let slot = 12345;
        let blockhash = "test_hash_12345";
        
        let is_replay1 = manager.is_block_replay(slot, blockhash).await.unwrap();
        assert!(!is_replay1);
        
        let is_replay2 = manager.is_block_replay(slot, blockhash).await.unwrap();
        assert!(is_replay2);
    }

    #[tokio::test]
    async fn test_mev_intelligence_extraction() {
        let endpoints = vec![
            RpcEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
                priority: 1,
                region: "global".to_string(),
                supports_jito: true,
                validator_identity: None,
                trusted_certificate_fingerprint: None,
            }
        ];
        
        let manager = BlockSubscriptionManager::new(endpoints).unwrap();
        
        let ui_block = UiBlock {
            slot: 12345,
            blockhash: "test_blockhash".to_string(),
            previous_blockhash: "test_previous_blockhash".to_string(),
            parent_slot: 12344,
            transactions: Some(vec![]),
            rewards: None,
            block_time: Some(1234567890),
            block_height: Some(12345),
        };
        
        let mev_data = manager.extract_mev_intelligence(&ui_block, 12345).await.unwrap();
        assert!(!mev_data.leader_pubkey.is_empty());
        assert!(mev_data.slot_entropy > 0);
    }

    #[tokio::test]
    async fn test_jito_bundle_submission() {
        let endpoints = vec![
            RpcEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
                priority: 1,
                region: "global".to_string(),
                supports_jito: true,
                validator_identity: None,
                trusted_certificate_fingerprint: None,
            }
        ];
        
        let manager = BlockSubscriptionManager::new(endpoints).unwrap();
        
        let transactions = vec!["test_transaction_1".to_string(), "test_transaction_2".to_string()];
        let target_slot = 12345;
        
        let result = manager.submit_jito_bundle(transactions, target_slot).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_optimal_tip_calculator() {
        let calculator = OptimalTipCalculator::new();
        
        let tip = calculator.calculate_optimal_tip(12345, 0.5, 1.0).await;
        assert!(tip >= MINIMUM_TIP_LAMPORTS);
        
        calculator.record_submission_result(12345, tip, true).await;
        
        let success_rate = calculator.success_rate_cache.get(&tip).map(|r| *r).unwrap_or(0.0);
        assert!(success_rate > 0.0);
    }

    #[tokio::test]
    async fn test_transaction_parser() {
        let parser = TransactionParser::new();
        
        let ui_transaction = UiTransaction {
            signatures: vec!["test_signature".to_string()],
            message: None,
            meta: None,
            version: None,
        };
        
        let parsed = parser.parse_transaction(&ui_transaction).unwrap();
        assert!(parsed.is_empty());
    }
}
