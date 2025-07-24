use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use serde::{Serialize, Deserialize};
use dashmap::DashMap;

const DECAY_WINDOW_SLOTS: u64 = 150;
const MIN_SAMPLE_SIZE: usize = 10;
const DECAY_RATE_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
const ALPHA_THRESHOLD_BASIS_POINTS: u64 = 5;
const MAX_HISTORICAL_ENTRIES: usize = 10000;
const DECAY_RATE_SMOOTHING_FACTOR: f64 = 0.15;
const COMPETITION_WEIGHT: f64 = 0.3;
const TIME_DECAY_WEIGHT: f64 = 0.7;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaOpportunity {
    pub id: [u8; 32],
    pub opportunity_type: OpportunityType,
    pub initial_profit: u64,
    pub current_profit: u64,
    pub discovered_slot: u64,
    pub discovered_time: Instant,
    pub competition_count: u32,
    pub execution_attempts: u32,
    pub successful_executions: u32,
    pub market_pair: (Pubkey, Pubkey),
    pub pool_address: Pubkey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpportunityType {
    Arbitrage,
    Liquidation,
    JitLiquidity,
    Sandwich,
    BackRun,
}

#[derive(Debug, Clone)]
pub struct DecayMetrics {
    pub average_decay_rate: f64,
    pub median_lifetime_ms: u64,
    pub competition_pressure: f64,
    pub success_rate: f64,
    pub total_opportunities: u64,
    pub profitable_opportunities: u64,
    pub decay_acceleration: f64,
}

#[derive(Debug)]
pub struct AlphaDecayRateMonitor {
    opportunities: Arc<DashMap<[u8; 32], AlphaOpportunity>>,
    decay_rates: Arc<RwLock<HashMap<OpportunityType, VecDeque<f64>>>>,
    historical_data: Arc<TokioRwLock<BTreeMap<u64, DecayMetrics>>>,
    current_slot: Arc<AtomicU64>,
    is_running: Arc<AtomicBool>,
    rpc_client: Arc<RpcClient>,
    metrics_tx: mpsc::UnboundedSender<DecayMetrics>,
    decay_coefficients: Arc<RwLock<HashMap<OpportunityType, DecayCoefficients>>>,
}

#[derive(Debug, Clone)]
struct DecayCoefficients {
    base_decay: f64,
    competition_multiplier: f64,
    time_factor: f64,
    volatility_adjustment: f64,
}

impl Default for DecayCoefficients {
    fn default() -> Self {
        Self {
            base_decay: 0.015,
            competition_multiplier: 1.5,
            time_factor: 0.98,
            volatility_adjustment: 1.0,
        }
    }
}

impl AlphaDecayRateMonitor {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        metrics_tx: mpsc::UnboundedSender<DecayMetrics>,
    ) -> Self {
        let mut decay_coefficients = HashMap::new();
        decay_coefficients.insert(OpportunityType::Arbitrage, DecayCoefficients {
            base_decay: 0.025,
            competition_multiplier: 2.0,
            time_factor: 0.95,
            volatility_adjustment: 1.2,
        });
        decay_coefficients.insert(OpportunityType::Liquidation, DecayCoefficients {
            base_decay: 0.012,
            competition_multiplier: 1.3,
            time_factor: 0.97,
            volatility_adjustment: 0.9,
        });
        decay_coefficients.insert(OpportunityType::JitLiquidity, DecayCoefficients {
            base_decay: 0.018,
            competition_multiplier: 1.8,
            time_factor: 0.96,
            volatility_adjustment: 1.1,
        });
        decay_coefficients.insert(OpportunityType::Sandwich, DecayCoefficients {
            base_decay: 0.022,
            competition_multiplier: 2.2,
            time_factor: 0.94,
            volatility_adjustment: 1.3,
        });
        decay_coefficients.insert(OpportunityType::BackRun, DecayCoefficients {
            base_decay: 0.020,
            competition_multiplier: 1.7,
            time_factor: 0.95,
            volatility_adjustment: 1.15,
        });

        Self {
            opportunities: Arc::new(DashMap::new()),
            decay_rates: Arc::new(RwLock::new(HashMap::new())),
            historical_data: Arc::new(TokioRwLock::new(BTreeMap::new())),
            current_slot: Arc::new(AtomicU64::new(0)),
            is_running: Arc::new(AtomicBool::new(false)),
            rpc_client,
            metrics_tx,
            decay_coefficients: Arc::new(RwLock::new(decay_coefficients)),
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.is_running.store(true, Ordering::SeqCst);
        
        let monitor_handle = self.spawn_monitor_task();
        let cleanup_handle = self.spawn_cleanup_task();
        let slot_update_handle = self.spawn_slot_update_task();
        
        tokio::try_join!(monitor_handle, cleanup_handle, slot_update_handle)?;
        Ok(())
    }

    pub fn track_opportunity(&self, opportunity: AlphaOpportunity) {
        self.opportunities.insert(opportunity.id, opportunity);
    }

    pub fn update_opportunity_profit(&self, id: [u8; 32], new_profit: u64, competition_count: u32) {
        if let Some(mut opportunity) = self.opportunities.get_mut(&id) {
            opportunity.current_profit = new_profit;
            opportunity.competition_count = competition_count;
        }
    }

    pub fn record_execution_attempt(&self, id: [u8; 32], success: bool) {
        if let Some(mut opportunity) = self.opportunities.get_mut(&id) {
            opportunity.execution_attempts += 1;
            if success {
                opportunity.successful_executions += 1;
            }
        }
    }

    fn spawn_monitor_task(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        let opportunities = Arc::clone(&self.opportunities);
        let decay_rates = Arc::clone(&self.decay_rates);
        let historical_data = Arc::clone(&self.historical_data);
        let current_slot = Arc::clone(&self.current_slot);
        let is_running = Arc::clone(&self.is_running);
        let metrics_tx = self.metrics_tx.clone();
        let decay_coefficients = Arc::clone(&self.decay_coefficients);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(DECAY_RATE_UPDATE_INTERVAL);
            
            while is_running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                let current_slot_value = current_slot.load(Ordering::SeqCst);
                let mut type_metrics: HashMap<OpportunityType, Vec<f64>> = HashMap::new();
                let mut total_opportunities = 0u64;
                let mut profitable_opportunities = 0u64;
                let mut competition_scores = Vec::new();
                let mut lifetimes = Vec::new();
                
                for entry in opportunities.iter() {
                    let opportunity = entry.value();
                    total_opportunities += 1;
                    
                    if opportunity.current_profit > 0 {
                        profitable_opportunities += 1;
                    }
                    
                    let lifetime_ms = opportunity.discovered_time.elapsed().as_millis() as u64;
                    lifetimes.push(lifetime_ms);
                    
                    let decay_rate = calculate_decay_rate(
                        opportunity,
                        current_slot_value,
                        &decay_coefficients,
                    );
                    
                    type_metrics.entry(opportunity.opportunity_type)
                        .or_insert_with(Vec::new)
                        .push(decay_rate);
                    
                    let competition_score = calculate_competition_pressure(
                        opportunity.competition_count,
                        opportunity.execution_attempts,
                        opportunity.successful_executions,
                    );
                    competition_scores.push(competition_score);
                }
                
                let average_decay_rate = type_metrics.values()
                    .flat_map(|v| v.iter())
                    .copied()
                    .sum::<f64>() / total_opportunities.max(1) as f64;
                
                let median_lifetime_ms = calculate_median(&mut lifetimes);
                let competition_pressure = calculate_average(&competition_scores);
                let success_rate = calculate_global_success_rate(&opportunities);
                
                update_decay_rates(&decay_rates, &type_metrics);
                
                let decay_acceleration = calculate_decay_acceleration(&decay_rates);
                
                let metrics = DecayMetrics {
                    average_decay_rate,
                    median_lifetime_ms,
                    competition_pressure,
                    success_rate,
                    total_opportunities,
                    profitable_opportunities,
                    decay_acceleration,
                };
                
                let _ = metrics_tx.send(metrics.clone());
                
                historical_data.write().await.insert(
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                    metrics,
                );
                
                adjust_decay_coefficients(&decay_coefficients, &type_metrics, competition_pressure);
            }
            
            Ok(())
        })
    }

    fn spawn_cleanup_task(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        let opportunities = Arc::clone(&self.opportunities);
        let historical_data = Arc::clone(&self.historical_data);
        let current_slot = Arc::clone(&self.current_slot);
        let is_running = Arc::clone(&self.is_running);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            
            while is_running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                let current_slot_value = current_slot.load(Ordering::SeqCst);
                let cutoff_slot = current_slot_value.saturating_sub(DECAY_WINDOW_SLOTS);
                
                opportunities.retain(|_, opportunity| {
                    opportunity.discovered_slot > cutoff_slot ||
                    opportunity.current_profit > ALPHA_THRESHOLD_BASIS_POINTS
                });
                
                let mut historical = historical_data.write().await;
                if historical.len() > MAX_HISTORICAL_ENTRIES {
                    let to_remove = historical.len() - MAX_HISTORICAL_ENTRIES;
                    let keys_to_remove: Vec<_> = historical.keys()
                        .take(to_remove)
                        .copied()
                        .collect();
                    for key in keys_to_remove {
                        historical.remove(&key);
                    }
                }
            }
            
            Ok(())
        })
    }

    fn spawn_slot_update_task(&self) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        let current_slot = Arc::clone(&self.current_slot);
        let rpc_client = Arc::clone(&self.rpc_client);
        let is_running = Arc::clone(&self.is_running);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(400));
            
            while is_running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                match rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await {
                    Ok(slot) => {
                        current_slot.store(slot, Ordering::SeqCst);
                    }
                    Err(e) => {
                        eprintln!("Failed to update slot: {}", e);
                    }
                }
            }
            
            Ok(())
        })
    }

    pub fn get_decay_rate(&self, opportunity_type: OpportunityType) -> f64 {
        self.decay_rates.read().unwrap()
            .get(&opportunity_type)
            .and_then(|rates| {
                if rates.len() >= MIN_SAMPLE_SIZE {
                    Some(rates.iter().sum::<f64>() / rates.len() as f64)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                self.decay_coefficients.read().unwrap()
                    .get(&opportunity_type)
                    .map(|c| c.base_decay)
                    .unwrap_or(0.015)
            })
    }

    pub async fn get_recent_metrics(&self) -> Option<DecayMetrics> {
        self.historical_data.read().await
            .iter()
            .last()
            .map(|(_, metrics)| metrics.clone())
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }
}

fn calculate_decay_rate(
    opportunity: &AlphaOpportunity,
    current_slot: u64,
    decay_coefficients: &Arc<RwLock<HashMap<OpportunityType, DecayCoefficients>>>,
) -> f64 {
    let coefficients = decay_coefficients.read().unwrap();
    let coeff = coefficients.get(&opportunity.opportunity_type)
        .cloned()
        .unwrap_or_default();
    
    let slot_delta = current_slot.saturating_sub(opportunity.discovered_slot) as f64;
    let time_elapsed = opportunity.discovered_time.elapsed().as_secs_f64();
    
    let profit_ratio = if opportunity.initial_profit > 0 {
        opportunity.current_profit as f64 / opportunity.initial_profit as f64
    } else {
        0.0
    };
    
    let competition_factor = 1.0 + (opportunity.competition_count as f64 * 0.1);
    let execution_penalty = if opportunity.execution_attempts > 0 {
        1.0 - (opportunity.successful_executions as f64 / opportunity.execution_attempts as f64) * 0.5
    } else {
        1.0
    };
    
    let base_decay = coeff.base_decay * (1.0 + slot_delta * 0.001);
    let time_decay = (1.0 - coeff.time_factor.powf(time_elapsed)) * TIME_DECAY_WEIGHT;
    let competition_decay = (competition_factor - 1.0) * coeff.competition_multiplier * COMPETITION_WEIGHT;
    
    let total_decay = (base_decay + time_decay + competition_decay) * execution_penalty * coeff.volatility_adjustment;
    
    (1.0 - profit_ratio).max(0.0).min(1.0) * total_decay
}

fn calculate_competition_pressure(
    competition_count: u32,
    execution_attempts: u32,
    successful_executions: u32,
) -> f64 {
    let base_pressure = (competition_count as f64).ln_1p() / 10.0;
    let failure_rate = if execution_attempts > 0 {
        1.0 - (successful_executions as f64 / execution_attempts as f64)
    } else {
        0.0
    };
    
    (base_pressure + failure_rate * 0.5).min(1.0)
}

fn calculate_median(values: &mut Vec<u64>) -> u64 {
    if values.is_empty() {
        return 0;
    }
    
    values.sort_unstable();
    let mid = values.len() / 2;
    
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2
    } else {
        values[mid]
    }
}

fn calculate_average(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    
    values.iter().sum::<f64>() / values.len() as f64
}

fn calculate_global_success_rate(opportunities: &DashMap<[u8; 32], AlphaOpportunity>) -> f64 {
    let mut total_attempts = 0u64;
    let mut total_successes = 0u64;
    
    for entry in opportunities.iter() {
        total_attempts += entry.value().execution_attempts as u64;
        total_successes += entry.value().successful_executions as u64;
    }
    
    if total_attempts > 0 {
        total_successes as f64 / total_attempts as f64
    } else {
        0.0
    }
}

fn update_decay_rates(
    decay_rates: &Arc<RwLock<HashMap<OpportunityType, VecDeque<f64>>>>,
    type_metrics: &HashMap<OpportunityType, Vec<f64>>,
) {
    let mut rates = decay_rates.write().unwrap();
    
    for (opportunity_type, metrics) in type_metrics {
        let avg_metric = metrics.iter().sum::<f64>() / metrics.len().max(1) as f64;
        
        let queue = rates.entry(*opportunity_type).or_insert_with(|| VecDeque::with_capacity(100));
        
        if queue.len() >= 100 {
            queue.pop_front();
        }
        
        let smoothed_value = if let Some(&last_value) = queue.back() {
            last_value * (1.0 - DECAY_RATE_SMOOTHING_FACTOR) + avg_metric * DECAY_RATE_SMOOTHING_FACTOR
        } else {
            avg_metric
        };
        
        queue.push_back(smoothed_value);
    }
}

fn calculate_decay_acceleration(
    decay_rates: &Arc<RwLock<HashMap<OpportunityType, VecDeque<f64>>>>,
) -> f64 {
    let rates = decay_rates.read().unwrap();
    let mut accelerations = Vec::new();
    
    for queue in rates.values() {
        if queue.len() >= 3 {
            let recent: Vec<f64> = queue.iter().rev().take(10).copied().collect();
            if recent.len() >= 3 {
                let mut derivatives = Vec::new();
                for i in 1..recent.len() {
                    derivatives.push(recent[i - 1] - recent[i]);
                }
                
                let mut second_derivatives = Vec::new();
                for i in 1..derivatives.len() {
                    second_derivatives.push(derivatives[i - 1] - derivatives[i]);
                }
                
                if !second_derivatives.is_empty() {
                    let avg_acceleration = second_derivatives.iter().sum::<f64>() / second_derivatives.len() as f64;
                    accelerations.push(avg_acceleration);
                }
            }
        }
    }
    
    if !accelerations.is_empty() {
        accelerations.iter().sum::<f64>() / accelerations.len() as f64
    } else {
        0.0
    }
}

fn adjust_decay_coefficients(
    decay_coefficients: &Arc<RwLock<HashMap<OpportunityType, DecayCoefficients>>>,
    type_metrics: &HashMap<OpportunityType, Vec<f64>>,
    competition_pressure: f64,
) {
    let mut coefficients = decay_coefficients.write().unwrap();
    
    for (opportunity_type, metrics) in type_metrics {
        if metrics.len() < MIN_SAMPLE_SIZE {
            continue;
        }
        
        let avg_decay = metrics.iter().sum::<f64>() / metrics.len() as f64;
        let variance = metrics.iter()
            .map(|&x| (x - avg_decay).powi(2))
            .sum::<f64>() / metrics.len() as f64;
        let std_dev = variance.sqrt();
        
        if let Some(coeff) = coefficients.get_mut(opportunity_type) {
            // Adaptive adjustment based on market conditions
            let target_decay = match opportunity_type {
                OpportunityType::Arbitrage => 0.025,
                OpportunityType::Liquidation => 0.012,
                OpportunityType::JitLiquidity => 0.018,
                OpportunityType::Sandwich => 0.022,
                OpportunityType::BackRun => 0.020,
            };
            
            let adjustment_factor = (target_decay - avg_decay).tanh() * 0.1;
            coeff.base_decay = (coeff.base_decay * (1.0 + adjustment_factor)).max(0.001).min(0.1);
            
            // Adjust competition multiplier based on pressure
            if competition_pressure > 0.7 {
                coeff.competition_multiplier = (coeff.competition_multiplier * 1.05).min(3.0);
            } else if competition_pressure < 0.3 {
                coeff.competition_multiplier = (coeff.competition_multiplier * 0.95).max(1.0);
            }
            
            // Volatility adjustment based on standard deviation
            coeff.volatility_adjustment = (1.0 + std_dev).min(2.0);
            
            // Time factor adjustment based on decay patterns
            if avg_decay > target_decay * 1.5 {
                coeff.time_factor = (coeff.time_factor * 0.98).max(0.9);
            } else if avg_decay < target_decay * 0.5 {
                coeff.time_factor = (coeff.time_factor * 1.02).min(0.99);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AlphaDecayAnalysis {
    pub opportunity_type: OpportunityType,
    pub current_decay_rate: f64,
    pub predicted_lifetime_ms: u64,
    pub optimal_execution_window: Duration,
    pub risk_score: f64,
}

impl AlphaDecayRateMonitor {
    pub async fn analyze_opportunity(&self, opportunity: &AlphaOpportunity) -> AlphaDecayAnalysis {
        let current_slot = self.current_slot.load(Ordering::SeqCst);
        let decay_rate = calculate_decay_rate(
            opportunity,
            current_slot,
            &self.decay_coefficients,
        );
        
        let historical_decay = self.get_decay_rate(opportunity.opportunity_type);
        let combined_decay = decay_rate * 0.7 + historical_decay * 0.3;
        
        let remaining_profit_ratio = opportunity.current_profit as f64 / opportunity.initial_profit as f64;
        let predicted_lifetime_ms = if combined_decay > 0.0 {
            ((remaining_profit_ratio.ln() / -combined_decay) * 1000.0) as u64
        } else {
            u64::MAX
        };
        
        let optimal_execution_window = Duration::from_millis(
            (predicted_lifetime_ms as f64 * 0.3).min(5000.0) as u64
        );
        
        let metrics = self.get_recent_metrics().await;
        let risk_score = calculate_risk_score(
            opportunity,
            combined_decay,
            metrics.as_ref().map(|m| m.competition_pressure).unwrap_or(0.5),
        );
        
        AlphaDecayAnalysis {
            opportunity_type: opportunity.opportunity_type,
            current_decay_rate: combined_decay,
            predicted_lifetime_ms,
            optimal_execution_window,
            risk_score,
        }
    }
    
    pub fn get_opportunity_health(&self, id: [u8; 32]) -> Option<f64> {
        self.opportunities.get(&id).map(|opportunity| {
            let profit_health = opportunity.current_profit as f64 / opportunity.initial_profit.max(1) as f64;
            let execution_health = if opportunity.execution_attempts > 0 {
                opportunity.successful_executions as f64 / opportunity.execution_attempts as f64
            } else {
                1.0
            };
            let competition_health = 1.0 / (1.0 + opportunity.competition_count as f64 * 0.1);
            let time_health = (-opportunity.discovered_time.elapsed().as_secs_f64() / 10.0).exp();
            
            (profit_health * 0.4 + execution_health * 0.3 + competition_health * 0.2 + time_health * 0.1)
                .max(0.0)
                .min(1.0)
        })
    }
    
    pub async fn get_market_decay_trends(&self) -> HashMap<OpportunityType, f64> {
        let rates = self.decay_rates.read().unwrap();
        let mut trends = HashMap::new();
        
        for (opportunity_type, queue) in rates.iter() {
            if queue.len() >= MIN_SAMPLE_SIZE {
                let recent: Vec<f64> = queue.iter().rev().take(20).copied().collect();
                let older: Vec<f64> = queue.iter().rev().skip(20).take(20).copied().collect();
                
                if !older.is_empty() {
                    let recent_avg = recent.iter().sum::<f64>() / recent.len() as f64;
                    let older_avg = older.iter().sum::<f64>() / older.len() as f64;
                    let trend = (recent_avg - older_avg) / older_avg.max(0.001);
                    trends.insert(*opportunity_type, trend);
                }
            }
        }
        
        trends
    }
}

fn calculate_risk_score(
    opportunity: &AlphaOpportunity,
    decay_rate: f64,
    competition_pressure: f64,
) -> f64 {
    let profit_risk = 1.0 - (opportunity.current_profit as f64 / opportunity.initial_profit.max(1) as f64);
    let decay_risk = decay_rate.min(1.0);
    let execution_risk = if opportunity.execution_attempts > 0 {
        1.0 - (opportunity.successful_executions as f64 / opportunity.execution_attempts as f64)
    } else {
        0.5
    };
    
    let weighted_risk = profit_risk * 0.3 + decay_risk * 0.4 + execution_risk * 0.2 + competition_pressure * 0.1;
    weighted_risk.max(0.0).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_alpha_decay_monitor() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let rpc = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let monitor = AlphaDecayRateMonitor::new(rpc, tx);
        
        let opportunity = AlphaOpportunity {
            id: [1u8; 32],
            opportunity_type: OpportunityType::Arbitrage,
            initial_profit: 1000000,
            current_profit: 950000,
            discovered_slot: 150000000,
            discovered_time: Instant::now(),
            competition_count: 3,
            execution_attempts: 5,
            successful_executions: 3,
            market_pair: (Pubkey::new_unique(), Pubkey::new_unique()),
            pool_address: Pubkey::new_unique(),
        };
        
        monitor.track_opportunity(opportunity.clone());
        assert_eq!(monitor.opportunities.len(), 1);
        
        let health = monitor.get_opportunity_health([1u8; 32]).unwrap();
        assert!(health > 0.0 && health <= 1.0);
    }
}

