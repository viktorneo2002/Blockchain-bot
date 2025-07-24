use anchor_lang::prelude::*;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
};
use spl_token_2022::{
    extension::StateWithExtensions,
    state::{Account as TokenAccount, Mint},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock as AsyncRwLock;

const DETECTION_WINDOW_MS: u64 = 1000;
const MAX_HISTORICAL_TXNS: usize = 10000;
const FAILURE_RATE_THRESHOLD: f64 = 0.15;
const SLIPPAGE_SPIKE_THRESHOLD: f64 = 0.02;
const GAS_SPIKE_MULTIPLIER: f64 = 1.5;
const POOL_MANIPULATION_THRESHOLD: f64 = 0.05;
const MIN_PROFIT_THRESHOLD_LAMPORTS: u64 = 100_000;
const MAX_POSITION_SIZE_LAMPORTS: u64 = 10_000_000_000;
const TOXIC_FLOW_WINDOW_SLOTS: u64 = 3;
const MEMPOOL_ANALYSIS_DEPTH: usize = 100;

#[derive(Clone, Debug)]
pub struct TransactionMetrics {
    pub timestamp: u64,
    pub slot: u64,
    pub signature: Signature,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub success: bool,
    pub profit_lamports: i64,
    pub slippage: f64,
    pub latency_ms: u64,
}

#[derive(Clone, Debug)]
pub struct PoolState {
    pub reserve_a: u64,
    pub reserve_b: u64,
    pub last_update_slot: u64,
    pub volume_24h: u64,
    pub manipulation_score: f64,
}

#[derive(Clone, Debug)]
pub struct AdverseSelectionMetrics {
    pub honeypot_probability: f64,
    pub frontrun_risk: f64,
    pub manipulation_score: f64,
    pub toxic_flow_indicator: f64,
    pub gas_anomaly_score: f64,
    pub overall_risk_score: f64,
}

pub struct AdverseSelectionDetector {
    rpc_client: Arc<RpcClient>,
    transaction_history: Arc<RwLock<VecDeque<TransactionMetrics>>>,
    pool_states: Arc<AsyncRwLock<HashMap<Pubkey, PoolState>>>,
    known_bots: Arc<RwLock<HashMap<Pubkey, BotProfile>>>,
    gas_history: Arc<RwLock<VecDeque<(u64, u64)>>>,
    failure_patterns: Arc<RwLock<HashMap<String, FailurePattern>>>,
}

#[derive(Clone, Debug)]
struct BotProfile {
    pub address: Pubkey,
    pub success_rate: f64,
    pub avg_priority_fee: u64,
    pub last_seen_slot: u64,
    pub frontrun_attempts: u32,
}

#[derive(Clone, Debug)]
struct FailurePattern {
    pub pattern_hash: String,
    pub occurrences: u32,
    pub last_seen: u64,
    pub avg_loss: i64,
}

impl AdverseSelectionDetector {
    pub fn new(rpc_endpoint: &str) -> Self {
        Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_endpoint.to_string(),
                CommitmentConfig::confirmed(),
            )),
            transaction_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORICAL_TXNS))),
            pool_states: Arc::new(AsyncRwLock::new(HashMap::new())),
            known_bots: Arc::new(RwLock::new(HashMap::new())),
            gas_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            failure_patterns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn analyze_opportunity(
        &self,
        pool_address: &Pubkey,
        input_amount: u64,
        expected_output: u64,
        priority_fee: u64,
        recent_transactions: &[Transaction],
    ) -> Result<AdverseSelectionMetrics, Box<dyn std::error::Error>> {
        let current_slot = self.rpc_client.get_slot()?;
        
        let honeypot_prob = self.detect_honeypot(pool_address, current_slot).await?;
        let frontrun_risk = self.calculate_frontrun_risk(priority_fee, recent_transactions, current_slot)?;
        let manipulation_score = self.detect_pool_manipulation(pool_address, current_slot).await?;
        let toxic_flow = self.analyze_toxic_flow(pool_address, current_slot).await?;
        let gas_anomaly = self.detect_gas_anomalies(priority_fee, current_slot)?;
        
        let pool_state = self.get_pool_state(pool_address).await?;
        let slippage_risk = self.calculate_slippage_risk(
            input_amount,
            expected_output,
            &pool_state,
        );
        
        let historical_failure_rate = self.calculate_historical_failure_rate(pool_address);
        
        let overall_risk = self.calculate_overall_risk(
            honeypot_prob,
            frontrun_risk,
            manipulation_score,
            toxic_flow,
            gas_anomaly,
            slippage_risk,
            historical_failure_rate,
        );
        
        Ok(AdverseSelectionMetrics {
            honeypot_probability: honeypot_prob,
            frontrun_risk,
            manipulation_score,
            toxic_flow_indicator: toxic_flow,
            gas_anomaly_score: gas_anomaly,
            overall_risk_score: overall_risk,
        })
    }

    async fn detect_honeypot(
        &self,
        pool_address: &Pubkey,
        current_slot: u64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let pool_account = self.rpc_client.get_account(pool_address)?;
        let mut honeypot_score = 0.0;
        
        if pool_account.data.len() < 324 {
            honeypot_score += 0.3;
        }
        
        let pool_states = self.pool_states.read().await;
        if let Some(state) = pool_states.get(pool_address) {
            if state.volume_24h < 1_000_000_000 {
                honeypot_score += 0.2;
            }
            
            let age_slots = current_slot.saturating_sub(state.last_update_slot);
            if age_slots < 100 {
                honeypot_score += 0.15;
            }
            
            if state.reserve_a < 100_000_000 || state.reserve_b < 100_000_000 {
                honeypot_score += 0.25;
            }
        } else {
            honeypot_score += 0.4;
        }
        
        let recent_failures = self.count_recent_failures(pool_address);
        if recent_failures > 5 {
            honeypot_score += 0.3;
        }
        
        Ok(honeypot_score.min(1.0))
    }

    fn calculate_frontrun_risk(
        &self,
        priority_fee: u64,
        recent_transactions: &[Transaction],
        current_slot: u64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mut risk_score = 0.0;
        
        let gas_history = self.gas_history.read().unwrap();
        let avg_gas = if gas_history.len() > 10 {
            gas_history.iter().take(50).map(|(_, fee)| fee).sum::<u64>() / 50.min(gas_history.len()) as u64
        } else {
            priority_fee
        };
        
        if priority_fee < avg_gas {
            risk_score += 0.3;
        }
        
        let high_fee_txns = recent_transactions.iter()
            .filter(|tx| {
                tx.message.recent_blockhash != solana_sdk::hash::Hash::default()
            })
            .count();
        
        if high_fee_txns > 5 {
            risk_score += 0.25;
        }
        
        let known_bots = self.known_bots.read().unwrap();
        let active_bots = known_bots.values()
            .filter(|bot| current_slot.saturating_sub(bot.last_seen_slot) < 10)
            .count();
        
        if active_bots > 3 {
            risk_score += 0.2 + (0.05 * active_bots.min(10) as f64);
        }
        
        Ok(risk_score.min(1.0))
    }

    async fn detect_pool_manipulation(
        &self,
        pool_address: &Pubkey,
        current_slot: u64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mut manipulation_score = 0.0;
        
        let pool_states = self.pool_states.read().await;
        if let Some(state) = pool_states.get(pool_address) {
            if state.manipulation_score > 0.5 {
                manipulation_score += state.manipulation_score * 0.7;
            }
            
            let slot_diff = current_slot.saturating_sub(state.last_update_slot);
            if slot_diff < TOXIC_FLOW_WINDOW_SLOTS {
                let reserve_ratio = state.reserve_a as f64 / state.reserve_b.max(1) as f64;
                if reserve_ratio > 1.1 || reserve_ratio < 0.9 {
                    manipulation_score += 0.3;
                }
            }
        }
        
        let recent_swaps = self.get_recent_pool_swaps(pool_address, current_slot)?;
        let large_swaps = recent_swaps.iter()
            .filter(|swap| swap.amount > MAX_POSITION_SIZE_LAMPORTS / 10)
            .count();
        
        if large_swaps > 2 {
            manipulation_score += 0.25 + (0.1 * large_swaps.min(5) as f64);
        }
        
        Ok(manipulation_score.min(1.0))
    }

    async fn analyze_toxic_flow(
        &self,
        pool_address: &Pubkey,
        current_slot: u64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mut toxic_score = 0.0;
        
        let transaction_history = self.transaction_history.read().unwrap();
        let recent_txns: Vec<_> = transaction_history.iter()
            .filter(|tx| current_slot.saturating_sub(tx.slot) < TOXIC_FLOW_WINDOW_SLOTS)
            .collect();
        
        let failed_txns = recent_txns.iter()
            .filter(|tx| !tx.success)
            .count();
        
        if recent_txns.len() > 0 {
            let failure_rate = failed_txns as f64 / recent_txns.len() as f64;
            if failure_rate > FAILURE_RATE_THRESHOLD {
                toxic_score += failure_rate * 0.8;
            }
        }
        
        let avg_slippage = recent_txns.iter()
            .map(|tx| tx.slippage)
            .sum::<f64>() / recent_txns.len().max(1) as f64;
        
        if avg_slippage > SLIPPAGE_SPIKE_THRESHOLD {
            toxic_score += (avg_slippage / SLIPPAGE_SPIKE_THRESHOLD) * 0.3;
        }
        
        let rapid_price_changes = self.detect_rapid_price_changes(pool_address, current_slot).await?;
        toxic_score += rapid_price_changes * 0.4;
        
        Ok(toxic_score.min(1.0))
    }

    fn detect_gas_anomalies(
        &self,
        priority_fee: u64,
        current_slot: u64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mut anomaly_score = 0.0;
        
        let gas_history = self.gas_history.read().unwrap();
        if gas_history.len() < 10 {
            return Ok(0.0);
        }
        
        let recent_fees: Vec<u64> = gas_history.iter()
            .take(20)
            .map(|(_, fee)| *fee)
            .collect();
        
        let avg_fee = recent_fees.iter().sum::<u64>() / recent_fees.len() as u64;
        let std_dev = self.calculate_std_dev(&recent_fees, avg_fee);
        
        if priority_fee > avg_fee + (2.0 * std_dev) as u64 {
            anomaly_score += 0.4;
        }
        
        if priority_fee > avg_fee * GAS_SPIKE_MULTIPLIER as u64 {
            anomaly_score += 0.3;
        }
        
        let sudden_spikes = self.detect_gas_spikes(&recent_fees);
        anomaly_score += sudden_spikes * 0.3;
        
        Ok(anomaly_score.min(1.0))
    }

    async fn get_pool_state(
        &self,
        pool_address: &Pubkey,
    ) -> Result<PoolState, Box<dyn std::error::Error>> {
        let pool_states = self.pool_states.read().await;
        
                if let Some(state) = pool_states.get(pool_address) {
            Ok(state.clone())
        } else {
            let pool_account = self.rpc_client.get_account(pool_address)?;
            let current_slot = self.rpc_client.get_slot()?;
            
            let (reserve_a, reserve_b) = self.parse_pool_reserves(&pool_account.data)?;
            
            Ok(PoolState {
                reserve_a,
                reserve_b,
                last_update_slot: current_slot,
                volume_24h: 0,
                manipulation_score: 0.0,
            })
        }
    }

    fn parse_pool_reserves(&self, data: &[u8]) -> Result<(u64, u64), Box<dyn std::error::Error>> {
        if data.len() < 324 {
            return Err("Invalid pool data length".into());
        }
        
        let reserve_a = u64::from_le_bytes(data[64..72].try_into()?);
        let reserve_b = u64::from_le_bytes(data[72..80].try_into()?);
        
        Ok((reserve_a, reserve_b))
    }

    fn calculate_slippage_risk(
        &self,
        input_amount: u64,
        expected_output: u64,
        pool_state: &PoolState,
    ) -> f64 {
        let constant_product = pool_state.reserve_a as u128 * pool_state.reserve_b as u128;
        let new_reserve_a = pool_state.reserve_a as u128 + input_amount as u128;
        let new_reserve_b = constant_product / new_reserve_a;
        let actual_output = pool_state.reserve_b as u128 - new_reserve_b;
        
        if actual_output > 0 && expected_output > 0 {
            let slippage = ((expected_output as f64 - actual_output as f64) / expected_output as f64).abs();
            (slippage / SLIPPAGE_SPIKE_THRESHOLD).min(1.0)
        } else {
            1.0
        }
    }

    fn calculate_historical_failure_rate(&self, pool_address: &Pubkey) -> f64 {
        let history = self.transaction_history.read().unwrap();
        let pool_txns: Vec<_> = history.iter()
            .filter(|tx| {
                let addr_bytes = pool_address.to_bytes();
                let sig_bytes = tx.signature.to_bytes();
                sig_bytes[0..8] == addr_bytes[0..8]
            })
            .collect();
        
        if pool_txns.is_empty() {
            return 0.5;
        }
        
        let failures = pool_txns.iter().filter(|tx| !tx.success).count();
        failures as f64 / pool_txns.len() as f64
    }

    fn count_recent_failures(&self, pool_address: &Pubkey) -> usize {
        let history = self.transaction_history.read().unwrap();
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        
        history.iter()
            .filter(|tx| {
                let addr_bytes = pool_address.to_bytes();
                let sig_bytes = tx.signature.to_bytes();
                sig_bytes[0..8] == addr_bytes[0..8] &&
                !tx.success &&
                current_time.saturating_sub(tx.timestamp) < DETECTION_WINDOW_MS
            })
            .count()
    }

    fn get_recent_pool_swaps(
        &self,
        pool_address: &Pubkey,
        current_slot: u64,
    ) -> Result<Vec<SwapInfo>, Box<dyn std::error::Error>> {
        let history = self.transaction_history.read().unwrap();
        
        let swaps: Vec<SwapInfo> = history.iter()
            .filter(|tx| {
                let addr_bytes = pool_address.to_bytes();
                let sig_bytes = tx.signature.to_bytes();
                sig_bytes[0..8] == addr_bytes[0..8] &&
                current_slot.saturating_sub(tx.slot) < TOXIC_FLOW_WINDOW_SLOTS
            })
            .map(|tx| SwapInfo {
                amount: tx.profit_lamports.abs() as u64,
                slot: tx.slot,
                success: tx.success,
            })
            .collect();
        
        Ok(swaps)
    }

    async fn detect_rapid_price_changes(
        &self,
        pool_address: &Pubkey,
        current_slot: u64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let pool_states = self.pool_states.read().await;
        
        if let Some(state) = pool_states.get(pool_address) {
            let slot_diff = current_slot.saturating_sub(state.last_update_slot);
            if slot_diff > 0 && slot_diff < 10 {
                let price_before = state.reserve_a as f64 / state.reserve_b.max(1) as f64;
                
                drop(pool_states);
                let current_state = self.get_pool_state(pool_address).await?;
                let price_after = current_state.reserve_a as f64 / current_state.reserve_b.max(1) as f64;
                
                let price_change = ((price_after - price_before) / price_before).abs();
                return Ok((price_change / POOL_MANIPULATION_THRESHOLD).min(1.0));
            }
        }
        
        Ok(0.0)
    }

    fn calculate_std_dev(&self, values: &[u64], mean: u64) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let variance = values.iter()
            .map(|&value| {
                let diff = if value > mean {
                    value - mean
                } else {
                    mean - value
                };
                (diff as f64).powi(2)
            })
            .sum::<f64>() / values.len() as f64;
        
        variance.sqrt()
    }

    fn detect_gas_spikes(&self, recent_fees: &[u64]) -> f64 {
        if recent_fees.len() < 3 {
            return 0.0;
        }
        
        let mut spike_count = 0;
        for i in 1..recent_fees.len() {
            if recent_fees[i] > recent_fees[i-1] * 2 {
                spike_count += 1;
            }
        }
        
        (spike_count as f64 / recent_fees.len() as f64).min(1.0)
    }

    fn calculate_overall_risk(
        &self,
        honeypot_prob: f64,
        frontrun_risk: f64,
        manipulation_score: f64,
        toxic_flow: f64,
        gas_anomaly: f64,
        slippage_risk: f64,
        historical_failure: f64,
    ) -> f64 {
        let weights = [
            (honeypot_prob, 0.25),
            (frontrun_risk, 0.20),
            (manipulation_score, 0.20),
            (toxic_flow, 0.15),
            (gas_anomaly, 0.10),
            (slippage_risk, 0.05),
            (historical_failure, 0.05),
        ];
        
        let weighted_sum: f64 = weights.iter()
            .map(|(score, weight)| score * weight)
            .sum();
        
        weighted_sum.min(1.0)
    }

    pub async fn update_transaction_metrics(
        &self,
        metrics: TransactionMetrics,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut history = self.transaction_history.write().unwrap();
        
        if history.len() >= MAX_HISTORICAL_TXNS {
            history.pop_front();
        }
        
        history.push_back(metrics.clone());
        
        let mut gas_history = self.gas_history.write().unwrap();
        gas_history.push_back((metrics.slot, metrics.priority_fee));
        if gas_history.len() > 1000 {
            gas_history.pop_front();
        }
        
        if !metrics.success {
            self.update_failure_patterns(&metrics)?;
        }
        
        Ok(())
    }

    fn update_failure_patterns(
        &self,
        metrics: &TransactionMetrics,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut patterns = self.failure_patterns.write().unwrap();
        
        let pattern_hash = format!(
            "{}_{}_{}", 
            metrics.slippage as i32,
            metrics.priority_fee / 1_000_000,
            metrics.compute_units / 10_000
        );
        
        if let Some(pattern) = patterns.get_mut(&pattern_hash) {
            pattern.occurrences += 1;
            pattern.last_seen = metrics.timestamp;
            pattern.avg_loss = (pattern.avg_loss * (pattern.occurrences - 1) as i64 + metrics.profit_lamports) 
                / pattern.occurrences as i64;
        } else {
            patterns.insert(pattern_hash.clone(), FailurePattern {
                pattern_hash,
                occurrences: 1,
                last_seen: metrics.timestamp,
                avg_loss: metrics.profit_lamports,
            });
        }
        
        if patterns.len() > 1000 {
            let oldest_key = patterns.iter()
                .min_by_key(|(_, p)| p.last_seen)
                .map(|(k, _)| k.clone())
                .unwrap();
            patterns.remove(&oldest_key);
        }
        
        Ok(())
    }

    pub async fn update_pool_state(
        &self,
        pool_address: &Pubkey,
        new_state: PoolState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut pool_states = self.pool_states.write().await;
        
        if let Some(existing_state) = pool_states.get_mut(pool_address) {
            let price_before = existing_state.reserve_a as f64 / existing_state.reserve_b.max(1) as f64;
            let price_after = new_state.reserve_a as f64 / new_state.reserve_b.max(1) as f64;
            let price_change = ((price_after - price_before) / price_before).abs();
            
            existing_state.manipulation_score = existing_state.manipulation_score * 0.9 
                + (price_change / POOL_MANIPULATION_THRESHOLD).min(1.0) * 0.1;
            
            existing_state.reserve_a = new_state.reserve_a;
            existing_state.reserve_b = new_state.reserve_b;
            existing_state.last_update_slot = new_state.last_update_slot;
            existing_state.volume_24h = new_state.volume_24h;
        } else {
            pool_states.insert(*pool_address, new_state);
        }
        
        if pool_states.len() > 10000 {
            let oldest_pool = pool_states.iter()
                .min_by_key(|(_, state)| state.last_update_slot)
                .map(|(k, _)| *k);
            
            if let Some(key) = oldest_pool {
                pool_states.remove(&key);
            }
        }
        
        Ok(())
    }

    pub fn update_bot_profile(
        &self,
        bot_address: &Pubkey,
        transaction: &Transaction,
        success: bool,
        slot: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut known_bots = self.known_bots.write().unwrap();
        
        if let Some(bot) = known_bots.get_mut(bot_address) {
            let total_txns = (1.0 / (1.0 - bot.success_rate)) as u32;
            let successful_txns = (bot.success_rate * total_txns as f64) as u32;
            
            bot.success_rate = if success {
                (successful_txns + 1) as f64 / (total_txns + 1) as f64
            } else {
                successful_txns as f64 / (total_txns + 1) as f64
            };
            
            bot.last_seen_slot = slot;
            
            if let Some(instruction) = transaction.message.instructions.first() {
                let priority_fee = instruction.data.get(0..8)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                
                bot.avg_priority_fee = (bot.avg_priority_fee * 9 + priority_fee) / 10;
            }
        } else {
            known_bots.insert(*bot_address, BotProfile {
                address: *bot_address,
                success_rate: if success { 1.0 } else { 0.0 },
                avg_priority_fee: 0,
                last_seen_slot: slot,
                frontrun_attempts: 0,
            });
        }
        
        if known_bots.len() > 5000 {
            let oldest_bot = known_bots.iter()
                .min_by_key(|(_, bot)| bot.last_seen_slot)
                .map(|(k, _)| *k);
            
            if let Some(key) = oldest_bot {
                known_bots.remove(&key);
            }
        }
        
        Ok(())
    }

    pub fn should_execute_trade(
        &self,
        metrics: &AdverseSelectionMetrics,
        expected_profit: u64,
    ) -> bool {
        if metrics.overall_risk_score > 0.7 {
            return false;
        }
        
        if metrics.honeypot_probability > 0.6 {
            return false;
        }
        
        if metrics.frontrun_risk > 0.8 && expected_profit < MIN_PROFIT_THRESHOLD_LAMPORTS * 2 {
            return false;
        }
        
        if metrics.manipulation_score > 0.7 {
            return false;
        }
        
        if metrics.toxic_flow_indicator > 0.5 && metrics.gas_anomaly_score > 0.5 {
            return false;
        }
        
        expected_profit > MIN_PROFIT_THRESHOLD_LAMPORTS
    }
}

#[derive(Clone, Debug)]
struct SwapInfo {
    amount: u64,
    slot: u64,
    success: bool,
}

impl AdverseSelectionDetector {
    pub async fn monitor_mempool_for_risks(
        &self,
        target_pool: &Pubkey,
        pending_transactions: &[Transaction],
    ) -> Result<Vec<RiskAlert>, Box<dyn std::error::Error>> {
        let mut risk_alerts = Vec::new();
        let current_slot = self.rpc_client.get_slot()?;
        
        for (idx, tx) in pending_transactions.iter().enumerate() {
            if idx >= MEMPOOL_ANALYSIS_DEPTH {
                break;
            }
            
            let risk_level = self.analyze_transaction_risk(tx, target_pool, current_slot)?;
            
            if risk_level > 0.5 {
                risk_alerts.push(RiskAlert {
                    transaction_index: idx,
                    risk_type: self.classify_risk_type(&risk_level),
                    severity: risk_level,
                    recommended_action: self.get_recommended_action(risk_level),
                });
            }
        }
        
        risk_alerts.sort_by(|a, b| b.severity.partial_cmp(&a.severity).unwrap());
        Ok(risk_alerts)
    }

    fn analyze_transaction_risk(
        &self,
        transaction: &Transaction,
        target_pool: &Pubkey,
        current_slot: u64,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mut risk_score = 0.0;
        
        if transaction.message.instructions.is_empty() {
            return Ok(0.0);
        }
        
        let primary_instruction = &transaction.message.instructions[0];
        let instruction_data = &primary_instruction.data;
        
        if instruction_data.len() >= 9 {
            let discriminator = &instruction_data[0..8];
            
            if discriminator == [0x09, 0xe0, 0x5e, 0x2d, 0x67, 0x8a, 0x71, 0x88] {
                risk_score += 0.2;
            }
            
            if instruction_data.len() >= 16 {
                let amount = u64::from_le_bytes(instruction_data[8..16].try_into().unwrap_or([0; 8]));
                if amount > MAX_POSITION_SIZE_LAMPORTS {
                    risk_score += 0.3;
                }
            }
        }
        
        let accounts_match = transaction.message.account_keys.iter()
            .any(|key| key == target_pool);
        
        if accounts_match && transaction.message.instructions.len() > 3 {
            risk_score += 0.25;
        }
        
        let gas_price = self.extract_priority_fee(transaction);
        let gas_history = self.gas_history.read().unwrap();
        if !gas_history.is_empty() {
            let recent_avg = gas_history.iter()
                .take(10)
                .map(|(_, fee)| *fee)
                .sum::<u64>() / 10.min(gas_history.len()) as u64;
            
            if gas_price > recent_avg * 3 {
                risk_score += 0.3;
            }
        }
        
        Ok(risk_score.min(1.0))
    }

    fn extract_priority_fee(&self, transaction: &Transaction) -> u64 {
        transaction.message.instructions.iter()
            .find(|inst| inst.program_id == solana_sdk::compute_budget::id())
            .and_then(|inst| {
                if inst.data.len() >= 9 && inst.data[0] == 3 {
                    Some(u64::from_le_bytes(inst.data[1..9].try_into().ok()?))
                } else {
                    None
                }
            })
            .unwrap_or(5000)
    }

    fn classify_risk_type(&self, risk_level: &f64) -> RiskType {
        if *risk_level > 0.8 {
            RiskType::Critical
        } else if *risk_level > 0.6 {
            RiskType::High
        } else if *risk_level > 0.4 {
            RiskType::Medium
        } else {
            RiskType::Low
        }
    }

    fn get_recommended_action(&self, risk_level: f64) -> RecommendedAction {
        if risk_level > 0.8 {
            RecommendedAction::Abort
        } else if risk_level > 0.6 {
            RecommendedAction::ReduceSize
        } else if risk_level > 0.4 {
            RecommendedAction::IncreasePriorityFee
        } else {
            RecommendedAction::Proceed
        }
    }

    pub async fn get_dynamic_risk_threshold(
        &self,
        pool_address: &Pubkey,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let base_threshold = 0.7;
        let mut adjustment = 0.0;
        
        let pool_states = self.pool_states.read().await;
        if let Some(state) = pool_states.get(pool_address) {
            if state.volume_24h > 100_000_000_000 {
                adjustment -= 0.1;
            }
            
            if state.manipulation_score < 0.2 {
                adjustment -= 0.05;
            }
        }
        
        let history = self.transaction_history.read().unwrap();
        let recent_success_rate = history.iter()
            .rev()
            .take(100)
            .filter(|tx| {
                let addr_bytes = pool_address.to_bytes();
                let sig_bytes = tx.signature.to_bytes();
                sig_bytes[0..8] == addr_bytes[0..8]
            })
            .fold((0, 0), |(success, total), tx| {
                if tx.success {
                    (success + 1, total + 1)
                } else {
                    (success, total + 1)
                }
            });
        
        if recent_success_rate.1 > 10 {
            let success_ratio = recent_success_rate.0 as f64 / recent_success_rate.1 as f64;
            if success_ratio > 0.9 {
                adjustment -= 0.1;
            } else if success_ratio < 0.5 {
                adjustment += 0.15;
            }
        }
        
        Ok((base_threshold + adjustment).max(0.3).min(0.95))
    }

    pub fn calculate_adaptive_position_size(
        &self,
        base_amount: u64,
        risk_metrics: &AdverseSelectionMetrics,
    ) -> u64 {
        let risk_multiplier = 1.0 - risk_metrics.overall_risk_score;
        let adjusted_amount = (base_amount as f64 * risk_multiplier) as u64;
        
        if risk_metrics.frontrun_risk > 0.7 {
            adjusted_amount.min(base_amount / 2)
        } else if risk_metrics.manipulation_score > 0.6 {
            adjusted_amount.min(base_amount * 2 / 3)
        } else {
            adjusted_amount.min(MAX_POSITION_SIZE_LAMPORTS)
        }
    }

    pub fn get_optimal_priority_fee(
        &self,
        base_fee: u64,
        risk_metrics: &AdverseSelectionMetrics,
    ) -> u64 {
        let mut fee_multiplier = 1.0;
        
        if risk_metrics.frontrun_risk > 0.5 {
            fee_multiplier += risk_metrics.frontrun_risk * 0.5;
        }
        
        if risk_metrics.gas_anomaly_score > 0.3 {
            fee_multiplier += 0.2;
        }
        
        let gas_history = self.gas_history.read().unwrap();
        if gas_history.len() > 20 {
            let recent_max = gas_history.iter()
                .take(20)
                .map(|(_, fee)| *fee)
                .max()
                .unwrap_or(base_fee);
            
            if recent_max > base_fee * 2 {
                fee_multiplier += 0.3;
            }
        }
        
        (base_fee as f64 * fee_multiplier.min(3.0)) as u64
    }

    pub async fn validate_pool_liquidity(
        &self,
        pool_address: &Pubkey,
        required_liquidity: u64,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let pool_state = self.get_pool_state(pool_address).await?;
        
        let available_liquidity = pool_state.reserve_a.min(pool_state.reserve_b);
        
        if available_liquidity < required_liquidity {
            return Ok(false);
        }
        
        let liquidity_ratio = required_liquidity as f64 / available_liquidity as f64;
        if liquidity_ratio > 0.1 {
            return Ok(false);
        }
        
        Ok(true)
    }

    pub fn get_risk_adjusted_profit_threshold(
        &self,
        risk_metrics: &AdverseSelectionMetrics,
    ) -> u64 {
        let base_threshold = MIN_PROFIT_THRESHOLD_LAMPORTS;
        let risk_multiplier = 1.0 + (risk_metrics.overall_risk_score * 2.0);
        
        (base_threshold as f64 * risk_multiplier) as u64
    }

    pub async fn cleanup_stale_data(&self, current_slot: u64) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut history = self.transaction_history.write().unwrap();
            let cutoff_slot = current_slot.saturating_sub(10000);
            history.retain(|tx| tx.slot > cutoff_slot);
        }
        
        {
            let mut known_bots = self.known_bots.write().unwrap();
            let cutoff_slot = current_slot.saturating_sub(1000);
            known_bots.retain(|_, bot| bot.last_seen_slot > cutoff_slot);
        }
        
        {
            let mut pool_states = self.pool_states.write().await;
            let cutoff_slot = current_slot.saturating_sub(5000);
            pool_states.retain(|_, state| state.last_update_slot > cutoff_slot);
        }
        
        {
            let mut patterns = self.failure_patterns.write().unwrap();
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            let cutoff_time = current_time.saturating_sub(86400000);
            patterns.retain(|_, pattern| pattern.last_seen > cutoff_time);
        }
        
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RiskAlert {
    pub transaction_index: usize,
    pub risk_type: RiskType,
    pub severity: f64,
    pub recommended_action: RecommendedAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RiskType {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RecommendedAction {
    Abort,
    ReduceSize,
    IncreasePriorityFee,
    Proceed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_adverse_selection_detection() {
        let detector = AdverseSelectionDetector::new("https://api.mainnet-beta.solana.com");
        let pool_address = Pubkey::new_unique();
        
        let metrics = detector.analyze_opportunity(
            &pool_address,
            1_000_000_000,
            950_000_000,
            100_000,
            &[],
        ).await;
        
        assert!(metrics.is_ok());
    }
}

