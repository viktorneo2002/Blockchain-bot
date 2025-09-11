use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::VecDeque;

use parking_lot::RwLock;
use hashbrown::HashMap;
use smallvec::SmallVec;

use solana_sdk::{
    instruction::Instruction,
    compute_budget::{ComputeBudgetInstruction, id as compute_budget_program_id},
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
    message::VersionedMessage,
};
use solana_client::{rpc_client::RpcClient, rpc_config::RpcSimulateTransactionConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use dashmap::DashMap;
use hdrhistogram::Histogram;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const MIN_COMPUTE_UNITS: u32 = 200_000;
const BASE_PRIORITY_FEE: u64 = 1_000;
const MAX_PRIORITY_FEE: u64 = 50_000_000;
const SLOT_HISTORY_SIZE: usize = 150;
const PRICE_DECAY_FACTOR: f64 = 0.95;
const CONGESTION_THRESHOLD: f64 = 0.85;
const FAILURE_PENALTY_MULTIPLIER: f64 = 1.15;
const SUCCESS_REWARD_MULTIPLIER: f64 = 0.98;
const PERCENTILE_TARGET: f64 = 0.95;
const MIN_SAMPLE_SIZE: usize = 10;
const ADAPTIVE_WINDOW: usize = 50;
const EXPONENTIAL_BACKOFF_BASE: f64 = 1.5;
const MAX_RETRIES: u8 = 3;

const HIST_WINDOWS: usize = 4;
const HIST_WINDOW_SLOTS: u64 = 128;

type FeeVec = SmallVec<[u64; 32]>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeUnitMetrics {
    pub slot: u64,
    pub compute_units_used: u32,
    pub priority_fee: u64,
    pub success: bool,
    pub latency_ms: u64,
    pub network_congestion: f64,
    pub competitor_fees: Vec<u64>,
    pub timestamp_ns: u128,
}

#[derive(Debug, Clone)]
pub struct AuctionStrategy {
    pub base_fee: u64,
    pub aggression_factor: f64,
    pub max_budget: u64,
    pub dynamic_adjustment: bool,
    pub congestion_multiplier: f64,
    pub competition_threshold: f64,
}

#[derive(Debug, Clone)]
pub struct NetworkState {
    pub current_slot: u64,
    pub recent_block_production_rate: f64,
    pub average_priority_fees: VecDeque<u64>,
    pub congestion_score: f64,
    pub competitor_activity: HashMap<Pubkey, FeeVec>,
    pub last_update: Instant,
}

pub struct ComputeUnitAuctionEngine {
    rpc_client: Arc<RpcClient>,
    metrics_history: Arc<RwLock<VecDeque<ComputeUnitMetrics>>>,
    network_state: Arc<RwLock<NetworkState>>,
    strategy: Arc<RwLock<AuctionStrategy>>,
    performance_cache: Arc<DashMap<u64, f64>>,
    fee_percentiles: Arc<RwLock<Vec<u64>>>,
    adaptive_params: Arc<Mutex<AdaptiveParameters>>,
    fee_ring: Arc<RwLock<RingHist>>,
    latency_ring: Arc<RwLock<RingHist>>,
}

#[derive(Debug, Clone)]
struct AdaptiveParameters {
    learning_rate: f64,
    exploration_rate: f64,
    momentum: f64,
    last_adjustment: Instant,
    performance_window: VecDeque<f64>,
}

#[derive(Debug)]
struct RingHist {
    windows: Vec<Histogram<u64>>,
    idx: usize,
    start_slot: u64,
    window_slots: u64,
    low: u64,
    high: u64,
    sigfig: u8,
}

impl RingHist {
    fn new(low: u64, high: u64, sigfig: u8, window_slots: u64, windows: usize) -> Self {
        let mut v = Vec::with_capacity(windows);
        for _ in 0..windows {
            v.push(Histogram::new_with_bounds(low, high, sigfig).expect("hist"));
        }
        Self { windows: v, idx: 0, start_slot: 0, window_slots, low, high, sigfig }
    }
    #[inline(always)]
    fn rotate_to(&mut self, slot: u64) {
        if self.start_slot == 0 {
            self.start_slot = (slot / self.window_slots) * self.window_slots;
            return;
        }
        let mut steps = slot.saturating_sub(self.start_slot) / self.window_slots;
        while steps > 0 {
            self.idx = (self.idx + 1) % self.windows.len();
            self.windows[self.idx].reset();
            self.start_slot = self.start_slot.saturating_add(self.window_slots);
            steps -= 1;
        }
    }
    #[inline(always)]
    fn record(&mut self, slot: u64, value: u64) {
        self.rotate_to(slot);
        let _ = self.windows[self.idx].record(value);
    }
    fn merge_recent(&self, k: usize) -> Option<Histogram<u64>> {
        let mut out = Histogram::new_with_bounds(self.low, self.high, self.sigfig).ok()?;
        let mut count = 0u64;
        for i in 0..k.min(self.windows.len()) {
            let j = (self.idx + self.windows.len() - i) % self.windows.len();
            let _ = out.add(&self.windows[j]);
            count += self.windows[j].len();
        }
        if count > 0 { Some(out) } else { None }
    }
    #[inline(always)]
    fn q_recent(&self, q: f64, k: usize) -> Option<u64> {
        self.merge_recent(k).map(|h| h.value_at_quantile(q))
    }
}

impl ComputeUnitAuctionEngine {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        let network_state = NetworkState {
            current_slot: 0,
            recent_block_production_rate: 1.0,
            average_priority_fees: VecDeque::with_capacity(SLOT_HISTORY_SIZE),
            congestion_score: 0.0,
            competitor_activity: HashMap::new(),
            last_update: Instant::now(),
        };

        let strategy = AuctionStrategy {
            base_fee: BASE_PRIORITY_FEE,
            aggression_factor: 1.0,
            max_budget: MAX_PRIORITY_FEE,
            dynamic_adjustment: true,
            congestion_multiplier: 1.2,
            competition_threshold: 0.8,
        };

        let adaptive_params = AdaptiveParameters {
            learning_rate: 0.1,
            exploration_rate: 0.05,
            momentum: 0.9,
            last_adjustment: Instant::now(),
            performance_window: VecDeque::with_capacity(ADAPTIVE_WINDOW),
        };

        let fee_ring = RingHist::new(1, MAX_PRIORITY_FEE, 3, HIST_WINDOW_SLOTS, HIST_WINDOWS);
        let latency_ring = RingHist::new(1, 5_000, 3, HIST_WINDOW_SLOTS, HIST_WINDOWS);

        Self {
            rpc_client,
            metrics_history: Arc::new(RwLock::new(VecDeque::with_capacity(SLOT_HISTORY_SIZE))),
            network_state: Arc::new(RwLock::new(network_state)),
            strategy: Arc::new(RwLock::new(strategy)),
            performance_cache: Arc::new(DashMap::new()),
            fee_percentiles: Arc::new(RwLock::new(Vec::new())),
            adaptive_params: Arc::new(Mutex::new(adaptive_params)),
            fee_ring: Arc::new(RwLock::new(fee_ring)),
            latency_ring: Arc::new(RwLock::new(latency_ring)),
        }
    }

    pub async fn calculate_optimal_compute_units(
        &self,
        transaction: &Transaction,
        urgency_factor: f64,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        self.update_network_state().await?;
        
        let simulated_units = self.simulate_compute_units(transaction).await?;
        let compute_units = self.adjust_compute_units(simulated_units);
        
        let priority_fee = self.calculate_priority_fee(urgency_factor).await?;
        
        Ok((compute_units, priority_fee))
    }

    async fn simulate_compute_units(
        &self,
        transaction: &Transaction,
    ) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()),
            ..Default::default()
        };

        let result = self.rpc_client.simulate_transaction_with_config(transaction, config)?;
        
        if let Some(err) = result.value.err {
            return Err(format!("Simulation failed: {:?}", err).into());
        }

        let units_consumed = result.value.units_consumed.unwrap_or(200_000) as u32;
        Ok(units_consumed)
    }

    fn adjust_compute_units(&self, simulated: u32) -> u32 {
        let p90_ms = self.latency_ring.read().q_recent(0.90, 2).unwrap_or(600);
        let safety = (1.05 + ((p90_ms as f64) / 1000.0).min(0.20));
        let adjusted = (simulated as f64 * safety).ceil() as u32;
        adjusted.clamp(MIN_COMPUTE_UNITS, MAX_COMPUTE_UNITS)
    }

    async fn calculate_priority_fee(
        &self,
        urgency_factor: f64,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let network_state = self.network_state.read();
        let strategy = self.strategy.read();
        
        let mut base_fee = self.get_dynamic_base_fee(&network_state, &strategy);
        if let Some(p75) = self.competitor_fee_quantile(0.75) {
            let floor = ((p75 as f64) * 0.99) as u64;
            if floor > base_fee { base_fee = floor; }
        }
        let competition_multiplier = self.calculate_competition_multiplier(&network_state);
        let congestion_adjustment = self.get_congestion_adjustment(&network_state);
        
        let adaptive_factor = self.get_adaptive_factor().await;
        // deterministic anti-herding jitter per-slot
        let mut rng = ChaCha8Rng::seed_from_u64(network_state.current_slot ^ base_fee);
        let jitter: f64 = rng.gen_range(0.9925..=1.0075);

        let fee = (base_fee as f64 
            * urgency_factor 
            * competition_multiplier 
            * congestion_adjustment 
            * adaptive_factor
            * strategy.aggression_factor
            * jitter) as u64;
        
        let final_fee = fee.min(strategy.max_budget).max(BASE_PRIORITY_FEE);
        
        Ok(final_fee)
    }

    fn get_dynamic_base_fee(&self, network_state: &NetworkState, strategy: &AuctionStrategy) -> u64 {
        if let Some(q) = self.fee_ring.read().q_recent(PERCENTILE_TARGET, 3) {
            return q;
        }
        if network_state.average_priority_fees.is_empty() {
            return strategy.base_fee;
        }
        let mut sorted_fees: Vec<u64> = network_state.average_priority_fees.iter().cloned().collect();
        sorted_fees.sort_unstable();
        let percentile_idx = ((sorted_fees.len() as f64 * PERCENTILE_TARGET) as usize)
            .min(sorted_fees.len() - 1);
        sorted_fees[percentile_idx]
    }

    fn calculate_competition_multiplier(&self, network_state: &NetworkState) -> f64 {
        if network_state.competitor_activity.is_empty() {
            return 1.0;
        }

        let total_competitors = network_state.competitor_activity.len() as f64;
        let avg_competitor_fee = network_state.competitor_activity
            .values()
            .flat_map(|fees| fees.iter())
            .sum::<u64>() as f64 
            / network_state.competitor_activity.values().map(|v| v.len()).sum::<usize>().max(1) as f64;

        let competition_intensity = (total_competitors / 10.0).min(2.0);
        let fee_pressure = (avg_competitor_fee / BASE_PRIORITY_FEE as f64).log2().max(0.0) / 10.0 + 1.0;
        
        competition_intensity * fee_pressure
    }

    fn get_congestion_adjustment(&self, network_state: &NetworkState) -> f64 {
        if network_state.congestion_score < CONGESTION_THRESHOLD {
            1.0
        } else {
            let excess_congestion = network_state.congestion_score - CONGESTION_THRESHOLD;
            1.0 + (excess_congestion * 2.0).min(3.0)
        }
    }

    async fn get_adaptive_factor(&self) -> f64 {
        let adaptive_params = self.adaptive_params.lock().await;
        let performance_score = self.calculate_recent_performance(&adaptive_params.performance_window);
        
        let exploration_bonus = if adaptive_params.exploration_rate > rand::random::<f64>() {
            1.0 + (rand::random::<f64>() * 0.2 - 0.1)
        } else {
            1.0
        };
        
        performance_score * exploration_bonus
    }

    fn calculate_recent_performance(&self, window: &VecDeque<f64>) -> f64 {
        if window.is_empty() {
            return 1.0;
        }

        let weighted_sum: f64 = window.iter()
            .enumerate()
            .map(|(i, &perf)| perf * (0.9_f64.powi(i as i32)))
            .sum();
        
        let weight_sum: f64 = (0..window.len())
            .map(|i| 0.9_f64.powi(i as i32))
            .sum();
        
        (weighted_sum / weight_sum).max(0.5).min(2.0)
    }

    pub async fn update_network_state(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let slot = self.rpc_client.get_slot()?;
        let block_production = self.rpc_client.get_block_production()?;

        let mut produced: u64 = 0;
        let mut leader: u64 = 0;
        for (_k, (ls, bp)) in block_production.value.by_identity.iter() {
            leader = leader.saturating_add(*ls as u64);
            produced = produced.saturating_add(*bp as u64);
        }
        let rate = if leader == 0 { 1.0 } else { (produced as f64 / leader as f64).clamp(0.0, 1.0) };

        let mut network_state = self.network_state.write();
        network_state.current_slot = slot;
        network_state.recent_block_production_rate = rate;
        network_state.congestion_score = self.calculate_congestion_score(&network_state);
        network_state.last_update = Instant::now();

        Ok(())
    }

    fn calculate_congestion_score(&self, network_state: &NetworkState) -> f64 {
        let production_penalty = (1.0 - network_state.recent_block_production_rate) * 2.0;
        let fee_pressure = if !network_state.average_priority_fees.is_empty() {
            let avg_fee = network_state.average_priority_fees.iter().sum::<u64>() 
                / network_state.average_priority_fees.len() as u64;
            (avg_fee as f64 / BASE_PRIORITY_FEE as f64).log10() / 5.0
        } else {
            0.0
        };
        
        (production_penalty + fee_pressure).min(1.0).max(0.0)
    }

    pub async fn record_transaction_result(
        &self,
        _signature: &Signature,
        compute_units: u32,
        priority_fee: u64,
        success: bool,
        latency_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let slot = self.rpc_client.get_slot()?;
        
        let metric = ComputeUnitMetrics {
            slot,
            compute_units_used: compute_units,
            priority_fee,
            success,
            latency_ms,
            network_congestion: self.network_state.read().congestion_score,
            competitor_fees: self.get_recent_competitor_fees(),
            timestamp_ns: now_ns(),
        };
        
        self.update_metrics_history(metric);

        if success {
            self.fee_ring.write().record(slot, priority_fee);
            self.latency_ring.write().record(slot, latency_ms.min(5_000));
        }
        self.update_adaptive_parameters(success, priority_fee).await;
        self.update_fee_percentiles();
        
        Ok(())
    }

    fn get_recent_competitor_fees(&self) -> Vec<u64> {
        let network_state = self.network_state.read();
        network_state.competitor_activity
            .values()
            .flat_map(|fees| fees.iter())
            .take(20)
            .cloned()
            .collect()
    }

    // ... (rest of the code remains the same)

    async fn update_adaptive_parameters(&self, success: bool, priority_fee: u64) {
        let mut params = self.adaptive_params.lock().await;
        
        let performance = if success {
            let fee_efficiency = BASE_PRIORITY_FEE as f64 / priority_fee.max(1) as f64;
            (fee_efficiency * 2.0).min(1.5)
        } else {
            0.5
        };
        
        params.performance_window.push_back(performance);
        if params.performance_window.len() > ADAPTIVE_WINDOW {
            params.performance_window.pop_front();
        }
        
        if params.performance_window.len() >= MIN_SAMPLE_SIZE {
            let avg_performance = params.performance_window.iter().sum::<f64>() 
                / params.performance_window.len() as f64;
            
            if avg_performance < 0.7 {
                params.exploration_rate = (params.exploration_rate * 1.1).min(0.15);
                params.learning_rate = (params.learning_rate * 1.05).min(0.2);
            } else if avg_performance > 0.9 {
                params.exploration_rate = (params.exploration_rate * 0.95).max(0.01);
                params.learning_rate = (params.learning_rate * 0.98).max(0.05);
            }
        }
        
        params.last_adjustment = Instant::now();
    }

    fn update_fee_percentiles(&self) {
        if let Some(h) = self.fee_ring.read().merge_recent(3) {
            let percentiles = vec![
                h.value_at_quantile(0.50),
                h.value_at_quantile(0.75),
                h.value_at_quantile(0.90),
                h.value_at_quantile(0.95),
                h.value_at_quantile(0.99),
            ];
            *self.fee_percentiles.write() = percentiles;
        }
    }

    pub async fn get_aggressive_bid(
        &self,
        min_position: f64,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let network_state = self.network_state.read();
        let strategy = self.strategy.read();
        
        let percentiles = self.fee_percentiles.read();
        if percentiles.is_empty() {
            return Ok((MAX_COMPUTE_UNITS, strategy.base_fee * 5));
        }
        
        let target_percentile = (min_position * 100.0) as usize;
        let index = (target_percentile / 25).min(percentiles.len() - 1);
        let aggressive_fee = percentiles[index];
        
        let competition_boost = if network_state.congestion_score > CONGESTION_THRESHOLD {
            1.5
        } else {
            1.2
        };
        
        let final_fee = ((aggressive_fee as f64 * competition_boost) as u64)
            .min(strategy.max_budget);
        
        Ok((MAX_COMPUTE_UNITS, final_fee))
    }

    pub async fn get_conservative_bid(&self) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let network_state = self.network_state.read();
        let strategy = self.strategy.read();
        
        let conservative_units = MIN_COMPUTE_UNITS + 50_000;
        
        let base_fee = if !network_state.average_priority_fees.is_empty() {
            let sum: u64 = network_state.average_priority_fees.iter().sum();
            sum / network_state.average_priority_fees.len() as u64
        } else {
            strategy.base_fee
        };
        
        let conservative_fee = (base_fee as f64 * 0.8) as u64;
        
        Ok((conservative_units, conservative_fee.max(BASE_PRIORITY_FEE)))
    }

    pub async fn get_adaptive_bid(
        &self,
        transaction_value: u64,
        max_slippage_bps: u64,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let max_fee_budget = (transaction_value * max_slippage_bps) / 10_000;
        
        let network_state = self.network_state.read();
        let strategy = self.strategy.read();
        
        let recent_success_rate = self.calculate_recent_success_rate();
        let optimal_percentile = if recent_success_rate < 0.5 {
            0.95
        } else if recent_success_rate < 0.7 {
            0.85
        } else if recent_success_rate < 0.9 {
            0.75
        } else {
            0.65
        };
        
        let base_fee = self.get_percentile_fee(optimal_percentile);
        let adaptive_multiplier = self.get_adaptive_multiplier(recent_success_rate).await;
        
        let compute_units = if network_state.congestion_score > 0.7 {
            MAX_COMPUTE_UNITS
        } else {
            (MAX_COMPUTE_UNITS as f64 * 0.8) as u32
        };
        
        let final_fee = ((base_fee as f64 * adaptive_multiplier) as u64)
            .min(max_fee_budget)
            .min(strategy.max_budget);
        
        Ok((compute_units, final_fee))
    }

    fn calculate_recent_success_rate(&self) -> f64 {
        let history = self.metrics_history.read();
        if history.is_empty() {
            return 0.5;
        }
        
        let recent_count = 20.min(history.len());
        let successes = history.iter()
            .rev()
            .take(recent_count)
            .filter(|m| m.success)
            .count();
        
        successes as f64 / recent_count as f64
    }

    fn get_percentile_fee(&self, percentile: f64) -> u64 {
        let history = self.metrics_history.read();
        if history.is_empty() {
            return BASE_PRIORITY_FEE;
        }
        
        let mut fees: Vec<u64> = history.iter()
            .map(|m| m.priority_fee)
            .collect();
        
        if fees.is_empty() {
            return BASE_PRIORITY_FEE;
        }
        
        fees.sort_unstable();
        let index = ((fees.len() as f64 * percentile) as usize)
            .min(fees.len() - 1);
        
        fees[index]
    }

    async fn get_adaptive_multiplier(&self, success_rate: f64) -> f64 {
        let params = self.adaptive_params.lock().await;
        let momentum_factor = params.momentum;
        let learning_adjustment = params.learning_rate * (0.8 - success_rate);
        
        let base_multiplier = if success_rate < 0.6 {
            1.0 + learning_adjustment.abs()
        } else if success_rate > 0.85 {
            1.0 - (learning_adjustment.abs() * 0.5)
        } else {
            1.0
        };
        
        let final_multiplier = base_multiplier * momentum_factor + (1.0 - momentum_factor);
        final_multiplier.max(0.7).min(1.5)
    }

    pub async fn optimize_for_latency(
        &self,
        target_latency_ms: u64,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(fh) = self.fee_ring.read().merge_recent(3) {
            let p50_fee = fh.value_at_quantile(0.50);
            let p90_fee = fh.value_at_quantile(0.90);
            let tight = (50.0 / target_latency_ms as f64).clamp(0.0, 1.0);
            let fee = (p50_fee as f64 * (1.0 - tight) + p90_fee as f64 * tight) as u64;
            return Ok((MAX_COMPUTE_UNITS, fee));
        }
        // Fallback to aggressive bid if histograms are not warmed
        self.get_aggressive_bid(0.9).await
    }

    pub fn build_compute_budget_instructions(
        &self,
        compute_units: u32,
        priority_fee: u64,
    ) -> Vec<Instruction> {
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(compute_units),
            ComputeBudgetInstruction::set_compute_unit_price(priority_fee),
        ]
    }

    #[inline(always)]
    fn parse_cbix(data: &[u8]) -> Option<(Option<u64>, Option<u32>)> {
        match data.first().copied() {
            Some(2) if data.len() >= 5 => {
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&data[1..5]);
                Some((None, Some(u32::from_le_bytes(buf))))
            }
            Some(3) if data.len() >= 9 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&data[1..9]);
                Some((Some(u64::from_le_bytes(buf)), None))
            }
            _ => None,
        }
    }

    fn extract_competitor_cbix(&self, msg: &VersionedMessage) -> (Option<u64>, Option<u32>) {
        let prog = compute_budget_program_id();
        match msg {
            VersionedMessage::Legacy(m) => {
                let pids = m.program_ids();
                let mut price = None;
                let mut limit = None;
                for ix in &m.instructions {
                    let pid = pids[ix.program_id_index as usize];
                    if pid == prog {
                        if let Some((p, l)) = Self::parse_cbix(&ix.data) {
                            if price.is_none() { price = p; }
                            if limit.is_none() { limit = l; }
                        }
                    }
                }
                (price, limit)
            }
            VersionedMessage::V0(m) => {
                let pids = m.program_ids();
                let mut price = None;
                let mut limit = None;
                for ix in &m.instructions {
                    let pid = pids[ix.program_id_index as usize];
                    if pid == prog {
                        if let Some((p, l)) = Self::parse_cbix(&ix.data) {
                            if price.is_none() { price = p; }
                            if limit.is_none() { limit = l; }
                        }
                    }
                }
                (price, limit)
            }
        }
    }

    pub async fn analyze_competitor_transaction(
        &self,
        tx_signature: &Signature,
        competitor_pubkey: &Pubkey,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tx = self.rpc_client.get_transaction(
            tx_signature,
            solana_transaction_status::UiTransactionEncoding::Base64,
        )?;
        if let Some(versioned) = tx.transaction.transaction.decode() {
            let (price_opt, _limit_opt) = self.extract_competitor_cbix(&versioned.message);
            if let Some(priority_fee) = price_opt {
                let mut ns = self.network_state.write();
                ns.competitor_activity
                    .entry(*competitor_pubkey)
                    .or_insert_with(|| SmallVec::new())
                    .push(priority_fee);
                if let Some(fees) = ns.competitor_activity.get_mut(competitor_pubkey) {
                    if fees.len() > 32 { fees.drain(..fees.len() - 32); }
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn competitor_fee_quantile(&self, q: f64) -> Option<u64> {
        let ns = self.network_state.read();
        let mut v: Vec<u64> = Vec::with_capacity(ns.competitor_activity.len() * 16);
        for fees in ns.competitor_activity.values() {
            v.extend(fees.iter().copied());
        }
        if v.is_empty() { return None; }
        v.sort_unstable();
        let idx = ((v.len() as f64 * q).floor() as usize).min(v.len() - 1);
        Some(v[idx])
    }

    pub async fn get_dynamic_bid_with_retry(
        &self,
        transaction: &Transaction,
        urgency_factor: f64,
        retry_count: u8,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let base_result = self.calculate_optimal_compute_units(transaction, urgency_factor).await?;
        
        if retry_count == 0 {
            return Ok(base_result);
        }
        
        let retry_multiplier = EXPONENTIAL_BACKOFF_BASE.powi(retry_count.min(MAX_RETRIES) as i32);
        let adjusted_fee = (base_result.1 as f64 * retry_multiplier) as u64;
        
        let strategy = self.strategy.read();
        let final_fee = adjusted_fee.min(strategy.max_budget);
        
        Ok((base_result.0, final_fee))
    }

    pub fn get_current_network_metrics(&self) -> NetworkMetrics {
        let network_state = self.network_state.read();
        let history = self.metrics_history.read();
        
        let recent_fees = history.iter()
            .rev()
            .take(10)
            .map(|m| m.priority_fee)
            .collect::<Vec<_>>();
        
        let avg_fee = if !recent_fees.is_empty() {
            recent_fees.iter().sum::<u64>() / recent_fees.len() as u64
        } else {
            BASE_PRIORITY_FEE
        };
        
        NetworkMetrics {
            current_slot: network_state.current_slot,
            congestion_score: network_state.congestion_score,
            average_priority_fee: avg_fee,
            success_rate: self.calculate_recent_success_rate(),
            competitor_count: network_state.competitor_activity.len(),
        }
    }

    pub async fn emergency_bid(&self) -> (u32, u64) {
        let strategy = self.strategy.read();
        (MAX_COMPUTE_UNITS, strategy.max_budget)
    }

    pub async fn update_strategy_parameters(
        &self,
        aggression_factor: Option<f64>,
        max_budget: Option<u64>,
        congestion_multiplier: Option<f64>,
    ) {
        let mut strategy = self.strategy.write();
        
        if let Some(factor) = aggression_factor {
            strategy.aggression_factor = factor.max(0.5).min(3.0);
        }
        
        if let Some(budget) = max_budget {
            strategy.max_budget = budget.min(MAX_PRIORITY_FEE);
        }
        
        if let Some(multiplier) = congestion_multiplier {
            strategy.congestion_multiplier = multiplier.max(1.0).min(5.0);
        }
    }

    pub async fn get_slot_synchronized_bid(
        &self,
        slot_offset: u64,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let current_slot = self.rpc_client.get_slot()?;
        let target_slot = current_slot + slot_offset;
        
        let slot_urgency = 1.0 + (slot_offset as f64 / 10.0);
        let network_state = self.network_state.read();
        
        let base_fee = if !network_state.average_priority_fees.is_empty() {
            let recent_avg = network_state.average_priority_fees.iter()
                .rev()
                .take(5)
                .sum::<u64>() / 5.min(network_state.average_priority_fees.len()) as u64;
            recent_avg
        } else {
            BASE_PRIORITY_FEE * 2
        };
        
        let synchronized_fee = (base_fee as f64 * slot_urgency) as u64;
        
        Ok((MAX_COMPUTE_UNITS, synchronized_fee))
    }

    pub fn reset_adaptive_parameters(&self) {
        if let Ok(mut params) = self.adaptive_params.try_lock() {
            params.learning_rate = 0.1;
            params.exploration_rate = 0.05;
            params.momentum = 0.9;
            params.performance_window.clear();
            params.last_adjustment = Instant::now();
        }
    }

    pub async fn get_statistical_bid(
        &self,
        confidence_level: f64,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let history = self.metrics_history.read();
        if history.len() < MIN_SAMPLE_SIZE {
            return self.get_conservative_bid().await;
        }
        
        let successful_fees: Vec<u64> = history.iter()
            .filter(|m| m.success)
            .map(|m| m.priority_fee)
            .collect();
        
        if successful_fees.is_empty() {
            return self.get_aggressive_bid(0.9).await;
        }
        
        let mean = successful_fees.iter().sum::<u64>() as f64 / successful_fees.len() as f64;
        let variance = successful_fees.iter()
            .map(|&fee| {
                let diff = fee as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / successful_fees.len() as f64;
        
        let std_dev = variance.sqrt();
        let z_score = match (confidence_level * 100.0) as u32 {
            90 => 1.645,
            95 => 1.96,
            99 => 2.576,
            _ => 1.96,
        };
        
        let statistical_fee = (mean + z_score * std_dev) as u64;
        let strategy = self.strategy.read();
        
        Ok((MAX_COMPUTE_UNITS, statistical_fee.min(strategy.max_budget)))
    }

    pub fn prune_old_metrics(&self) {
        let mut history = self.metrics_history.write();
        let cutoff_ns = now_ns().saturating_sub(300_000_000_000);
        
        history.retain(|metric| metric.timestamp_ns > cutoff_ns);
        
        let current_slot = if let Ok(slot) = self.rpc_client.get_slot() {
            slot
        } else {
            return;
        };
        
        let slot_cutoff = current_slot.saturating_sub(500);
        self.performance_cache.retain(|&slot, _| slot > slot_cutoff);
    }

    // ... (rest of the code remains the same)
        &self,
        spread_bps: u64,
        volume: u64,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let volume_tier = match volume {
            0..=1_000_000_000 => 1.0,
            1_000_000_001..=10_000_000_000 => 1.2,
            10_000_000_001..=100_000_000_000 => 1.5,
            _ => 2.0,
        };
        
        let spread_multiplier = (spread_bps as f64 / 100.0).sqrt();
        let network_state = self.network_state.read();
        
        let base_fee = self.get_dynamic_base_fee(&network_state, &self.strategy.read());
        let market_maker_fee = (base_fee as f64 * volume_tier * spread_multiplier) as u64;
        
        let compute_units = if spread_bps < 50 {
            MAX_COMPUTE_UNITS
        } else {
            (MAX_COMPUTE_UNITS as f64 * 0.9) as u32
        };
        
        Ok((compute_units, market_maker_fee))
    }

    pub fn get_health_status(&self) -> EngineHealth {
        let history = self.metrics_history.read();
        let network_state = self.network_state.read();
        
        let recent_success_rate = self.calculate_recent_success_rate();
        let data_freshness = network_state.last_update.elapsed() < Duration::from_secs(10);
        let sufficient_data = history.len() >= MIN_SAMPLE_SIZE;
        
        let health_score = (recent_success_rate * 0.5) + 
                          (if data_freshness { 0.3 } else { 0.0 }) +
                          (if sufficient_data { 0.2 } else { 0.0 });
        
        EngineHealth {
            is_healthy: health_score > 0.6,
            success_rate: recent_success_rate,
            data_points: history.len(),
            last_update: network_state.last_update,
            health_score,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineHealth {
    pub is_healthy: bool,
    pub success_rate: f64,
    pub data_points: usize,
    pub last_update: Instant,
    pub health_score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::commitment_config::CommitmentConfig;

    #[tokio::test]
    async fn test_compute_unit_bounds() {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            "https://api.mainnet-beta.solana.com".to_string(),
            CommitmentConfig::confirmed(),
        ));
        let engine = ComputeUnitAuctionEngine::new(rpc);
        
        assert!(engine.validate_compute_budget(MIN_COMPUTE_UNITS, BASE_PRIORITY_FEE));
        assert!(engine.validate_compute_budget(MAX_COMPUTE_UNITS, MAX_PRIORITY_FEE));
        assert!(!engine.validate_compute_budget(MAX_COMPUTE_UNITS + 1, BASE_PRIORITY_FEE));
        assert!(!engine.validate_compute_budget(MIN_COMPUTE_UNITS, MAX_PRIORITY_FEE + 1));
    }
}

