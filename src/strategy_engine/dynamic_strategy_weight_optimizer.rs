use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use rand::distributions::{Distribution, Beta};
use rand::thread_rng;

const MAX_STRATEGIES: usize = 32;
const MIN_WEIGHT: f64 = 0.001;
const MAX_WEIGHT: f64 = 0.95;
const WEIGHT_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
const PERFORMANCE_WINDOW: usize = 10000;
const DECAY_FACTOR: f64 = 0.995;
const EXPLORATION_RATE: f64 = 0.1;
const CONFIDENCE_THRESHOLD: f64 = 0.95;
const MIN_SAMPLES: usize = 100;
const VOLATILITY_WINDOW: usize = 1000;
const RISK_ADJUSTMENT_FACTOR: f64 = 0.7;
const LATENCY_PENALTY_FACTOR: f64 = 0.0001;
const FAILURE_PENALTY: f64 = 0.8;
const SUCCESS_REWARD: f64 = 1.2;
const PROFIT_SCALE_FACTOR: f64 = 0.000001;
const NETWORK_CONGESTION_THRESHOLD: f64 = 0.7;
const COMPETITIVE_EDGE_BONUS: f64 = 1.5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyMetrics {
    pub total_attempts: AtomicU64,
    pub successful_executions: AtomicU64,
    pub total_profit: AtomicU64,
    pub total_gas_used: AtomicU64,
    pub cumulative_latency: AtomicU64,
    pub last_update: AtomicU64,
    pub consecutive_failures: AtomicU64,
    pub volatility_score: f64,
    pub competition_level: f64,
    pub network_conditions: f64,
}

#[derive(Debug, Clone)]
pub struct PerformanceSnapshot {
    pub timestamp: u64,
    pub success_rate: f64,
    pub avg_profit: f64,
    pub avg_latency: f64,
    pub volatility: f64,
    pub risk_adjusted_return: f64,
}

#[derive(Debug, Clone)]
pub struct StrategyWeight {
    pub strategy_id: String,
    pub current_weight: f64,
    pub target_weight: f64,
    pub confidence_interval: (f64, f64),
    pub last_adjustment: Instant,
    pub adjustment_velocity: f64,
    pub momentum: f64,
}

pub struct DynamicStrategyWeightOptimizer {
    weights: Arc<RwLock<HashMap<String, StrategyWeight>>>,
    metrics: Arc<RwLock<HashMap<String, StrategyMetrics>>>,
    performance_history: Arc<Mutex<HashMap<String, Vec<PerformanceSnapshot>>>>,
    global_metrics: Arc<GlobalMetrics>,
    thompson_sampler: Arc<Mutex<ThompsonSampler>>,
    last_optimization: Arc<Mutex<Instant>>,
    optimization_counter: AtomicU64,
}

struct GlobalMetrics {
    total_volume: AtomicU64,
    network_congestion: AtomicU64,
    avg_block_time: AtomicU64,
    competition_intensity: AtomicU64,
}

struct ThompsonSampler {
    alpha_params: HashMap<String, f64>,
    beta_params: HashMap<String, f64>,
    exploration_bonus: HashMap<String, f64>,
}

impl StrategyMetrics {
    fn new() -> Self {
        Self {
            total_attempts: AtomicU64::new(0),
            successful_executions: AtomicU64::new(0),
            total_profit: AtomicU64::new(0),
            total_gas_used: AtomicU64::new(0),
            cumulative_latency: AtomicU64::new(0),
            last_update: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
            volatility_score: 0.0,
            competition_level: 0.5,
            network_conditions: 1.0,
        }
    }

    fn success_rate(&self) -> f64 {
        let attempts = self.total_attempts.load(Ordering::Relaxed) as f64;
        if attempts > 0.0 {
            self.successful_executions.load(Ordering::Relaxed) as f64 / attempts
        } else {
            0.5
        }
    }

    fn avg_profit(&self) -> f64 {
        let executions = self.successful_executions.load(Ordering::Relaxed) as f64;
        if executions > 0.0 {
            self.total_profit.load(Ordering::Relaxed) as f64 / executions
        } else {
            0.0
        }
    }

    fn avg_latency(&self) -> f64 {
        let attempts = self.total_attempts.load(Ordering::Relaxed) as f64;
        if attempts > 0.0 {
            self.cumulative_latency.load(Ordering::Relaxed) as f64 / attempts
        } else {
            f64::MAX
        }
    }
}

impl DynamicStrategyWeightOptimizer {
    pub fn new() -> Self {
        Self {
            weights: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(HashMap::new())),
            performance_history: Arc::new(Mutex::new(HashMap::new())),
            global_metrics: Arc::new(GlobalMetrics {
                total_volume: AtomicU64::new(0),
                network_congestion: AtomicU64::new(500),
                avg_block_time: AtomicU64::new(400),
                competition_intensity: AtomicU64::new(500),
            }),
            thompson_sampler: Arc::new(Mutex::new(ThompsonSampler {
                alpha_params: HashMap::new(),
                beta_params: HashMap::new(),
                exploration_bonus: HashMap::new(),
            })),
            last_optimization: Arc::new(Mutex::new(Instant::now())),
            optimization_counter: AtomicU64::new(0),
        }
    }

    pub async fn register_strategy(&self, strategy_id: String) -> Result<(), Box<dyn std::error::Error>> {
        let mut weights = self.weights.write().unwrap();
        let mut metrics = self.metrics.write().unwrap();
        
        if weights.len() >= MAX_STRATEGIES {
            return Err("Maximum strategies reached".into());
        }

        let initial_weight = 1.0 / (weights.len() + 1) as f64;
        
        weights.insert(strategy_id.clone(), StrategyWeight {
            strategy_id: strategy_id.clone(),
            current_weight: initial_weight,
            target_weight: initial_weight,
            confidence_interval: (initial_weight * 0.8, initial_weight * 1.2),
            last_adjustment: Instant::now(),
            adjustment_velocity: 0.0,
            momentum: 0.0,
        });

        metrics.insert(strategy_id.clone(), StrategyMetrics::new());

        let mut sampler = self.thompson_sampler.lock().await;
        sampler.alpha_params.insert(strategy_id.clone(), 1.0);
        sampler.beta_params.insert(strategy_id.clone(), 1.0);
        sampler.exploration_bonus.insert(strategy_id, 0.1);

        self.normalize_weights().await;
        Ok(())
    }

    pub async fn record_execution(
        &self,
        strategy_id: &str,
        success: bool,
        profit: i64,
        gas_used: u64,
        latency_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut metrics = self.metrics.write().unwrap();
        let metric = metrics.get_mut(strategy_id)
            .ok_or("Strategy not found")?;

        metric.total_attempts.fetch_add(1, Ordering::Relaxed);
        
        if success {
            metric.successful_executions.fetch_add(1, Ordering::Relaxed);
            metric.consecutive_failures.store(0, Ordering::Relaxed);
            if profit > 0 {
                metric.total_profit.fetch_add(profit as u64, Ordering::Relaxed);
            }
        } else {
            metric.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        }

        metric.total_gas_used.fetch_add(gas_used, Ordering::Relaxed);
        metric.cumulative_latency.fetch_add(latency_ms, Ordering::Relaxed);
        metric.last_update.store(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            Ordering::Relaxed
        );

        drop(metrics);

        self.update_thompson_sampler(strategy_id, success).await;
        self.update_performance_history(strategy_id).await;

        if self.should_optimize().await {
            self.optimize_weights().await?;
        }

        Ok(())
    }

    async fn update_thompson_sampler(&self, strategy_id: &str, success: bool) {
        let mut sampler = self.thompson_sampler.lock().await;
        
        if let Some(alpha) = sampler.alpha_params.get_mut(strategy_id) {
            if success {
                *alpha += 1.0;
            }
        }
        
        if let Some(beta) = sampler.beta_params.get_mut(strategy_id) {
            if !success {
                *beta += 1.0;
            }
        }
    }

    async fn update_performance_history(&self, strategy_id: &str) {
        let metrics = self.metrics.read().unwrap();
        if let Some(metric) = metrics.get(strategy_id) {
            let snapshot = PerformanceSnapshot {
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                success_rate: metric.success_rate(),
                avg_profit: metric.avg_profit(),
                avg_latency: metric.avg_latency(),
                volatility: metric.volatility_score,
                risk_adjusted_return: self.calculate_risk_adjusted_return(metric),
            };

            let mut history = self.performance_history.lock().await;
            let snapshots = history.entry(strategy_id.to_string()).or_insert_with(Vec::new);
            snapshots.push(snapshot);

            if snapshots.len() > PERFORMANCE_WINDOW {
                snapshots.remove(0);
            }
        }
    }

    fn calculate_risk_adjusted_return(&self, metrics: &StrategyMetrics) -> f64 {
        let success_rate = metrics.success_rate();
        let avg_profit = metrics.avg_profit();
        let volatility = metrics.volatility_score.max(0.1);
        let consecutive_failures = metrics.consecutive_failures.load(Ordering::Relaxed) as f64;
        
        let base_return = success_rate * avg_profit * PROFIT_SCALE_FACTOR;
        let risk_penalty = volatility * RISK_ADJUSTMENT_FACTOR;
        let failure_penalty = (consecutive_failures * 0.1).min(0.5);
        let network_adjustment = metrics.network_conditions;
        let competition_adjustment = 1.0 + (1.0 - metrics.competition_level) * 0.5;
        
        (base_return * network_adjustment * competition_adjustment - risk_penalty - failure_penalty).max(0.0)
    }

    async fn should_optimize(&self) -> bool {
        let last_opt = self.last_optimization.lock().await;
        let elapsed = last_opt.elapsed();
        
        if elapsed < WEIGHT_UPDATE_INTERVAL {
            return false;
        }

        let counter = self.optimization_counter.fetch_add(1, Ordering::Relaxed);
        counter % 10 == 0 || elapsed > Duration::from_secs(1)
    }

    async fn optimize_weights(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut new_weights = HashMap::new();
        let metrics = self.metrics.read().unwrap().clone();
        let mut sampler = self.thompson_sampler.lock().await;

        for (strategy_id, metric) in &metrics {
            let score = self.calculate_strategy_score(strategy_id, metric, &sampler).await;
            new_weights.insert(strategy_id.clone(), score);
        }

        let total_score: f64 = new_weights.values().sum();
        if total_score <= 0.0 {
            return Ok(());
        }

        let mut weights = self.weights.write().unwrap();
        
        for (strategy_id, raw_score) in new_weights {
            let normalized_weight = (raw_score / total_score).max(MIN_WEIGHT).min(MAX_WEIGHT);
            
            if let Some(weight) = weights.get_mut(&strategy_id) {
                let current = weight.current_weight;
                let diff = normalized_weight - current;
                
                weight.momentum = weight.momentum * 0.9 + diff * 0.1;
                weight.adjustment_velocity = (weight.adjustment_velocity * 0.8) + (diff * 0.2);
                
                let adjustment = diff * 0.3 + weight.momentum * 0.2;
                let new_weight = (current + adjustment).max(MIN_WEIGHT).min(MAX_WEIGHT);
                
                weight.target_weight = normalized_weight;
                weight.current_weight = new_weight;
                weight.confidence_interval = self.calculate_confidence_interval(&metrics[&strategy_id], new_weight);
                weight.last_adjustment = Instant::now();
            }
        }

                self.ensure_weight_sum_unity(&mut weights);
        
        *self.last_optimization.lock().await = Instant::now();
        Ok(())
    }

    fn ensure_weight_sum_unity(&self, weights: &mut HashMap<String, StrategyWeight>) {
        let total: f64 = weights.values().map(|w| w.current_weight).sum();
        if total > 0.0 && (total - 1.0).abs() > 0.001 {
            for weight in weights.values_mut() {
                weight.current_weight /= total;
            }
        }
    }

    async fn calculate_strategy_score(
        &self,
        strategy_id: &str,
        metrics: &StrategyMetrics,
        sampler: &ThompsonSampler,
    ) -> f64 {
        let attempts = metrics.total_attempts.load(Ordering::Relaxed);
        if attempts < MIN_SAMPLES as u64 {
            return self.thompson_sample(strategy_id, sampler);
        }

        let success_rate = metrics.success_rate();
        let avg_profit = metrics.avg_profit();
        let avg_latency = metrics.avg_latency();
        let consecutive_failures = metrics.consecutive_failures.load(Ordering::Relaxed) as f64;
        
        let network_congestion = self.global_metrics.network_congestion.load(Ordering::Relaxed) as f64 / 1000.0;
        let competition = self.global_metrics.competition_intensity.load(Ordering::Relaxed) as f64 / 1000.0;
        
        let profit_score = avg_profit * PROFIT_SCALE_FACTOR * success_rate;
        let latency_penalty = avg_latency * LATENCY_PENALTY_FACTOR;
        let failure_penalty = consecutive_failures.powf(1.5) * 0.01;
        let volatility_penalty = metrics.volatility_score * 0.1;
        
        let network_bonus = if network_congestion > NETWORK_CONGESTION_THRESHOLD {
            0.8
        } else {
            1.0 + (1.0 - network_congestion) * 0.2
        };
        
        let competition_factor = if competition > 0.8 {
            COMPETITIVE_EDGE_BONUS
        } else {
            1.0 + (1.0 - competition) * 0.3
        };
        
        let exploration_score = self.thompson_sample(strategy_id, sampler) * EXPLORATION_RATE;
        let exploitation_score = (profit_score * network_bonus * competition_factor - latency_penalty - failure_penalty - volatility_penalty).max(0.0);
        
        exploration_score + exploitation_score * (1.0 - EXPLORATION_RATE)
    }

    fn thompson_sample(&self, strategy_id: &str, sampler: &ThompsonSampler) -> f64 {
        let alpha = sampler.alpha_params.get(strategy_id).unwrap_or(&1.0);
        let beta = sampler.beta_params.get(strategy_id).unwrap_or(&1.0);
        let exploration_bonus = sampler.exploration_bonus.get(strategy_id).unwrap_or(&0.0);
        
        let beta_dist = Beta::new(*alpha, *beta).unwrap();
        let sample = beta_dist.sample(&mut thread_rng());
        
        sample + exploration_bonus
    }

    fn calculate_confidence_interval(&self, metrics: &StrategyMetrics, weight: f64) -> (f64, f64) {
        let attempts = metrics.total_attempts.load(Ordering::Relaxed) as f64;
        let success_rate = metrics.success_rate();
        
        if attempts < MIN_SAMPLES as f64 {
            return (weight * 0.5, weight * 1.5);
        }
        
        let std_error = ((success_rate * (1.0 - success_rate)) / attempts).sqrt();
        let margin = 1.96 * std_error;
        
        let lower = (weight * (1.0 - margin)).max(MIN_WEIGHT);
        let upper = (weight * (1.0 + margin)).min(MAX_WEIGHT);
        
        (lower, upper)
    }

    async fn normalize_weights(&self) {
        let mut weights = self.weights.write().unwrap();
        self.ensure_weight_sum_unity(&mut weights);
    }

    pub async fn get_strategy_weights(&self) -> HashMap<String, f64> {
        let weights = self.weights.read().unwrap();
        weights.iter()
            .map(|(id, w)| (id.clone(), w.current_weight))
            .collect()
    }

    pub async fn get_strategy_selection_probabilities(&self) -> Vec<(String, f64)> {
        let weights = self.weights.read().unwrap();
        let metrics = self.metrics.read().unwrap();
        
        let mut probabilities = Vec::new();
        let network_congestion = self.global_metrics.network_congestion.load(Ordering::Relaxed) as f64 / 1000.0;
        
        for (id, weight) in weights.iter() {
            let mut prob = weight.current_weight;
            
            if let Some(metric) = metrics.get(id) {
                let recent_failures = metric.consecutive_failures.load(Ordering::Relaxed);
                if recent_failures > 5 {
                    prob *= FAILURE_PENALTY.powf(recent_failures as f64 / 5.0);
                }
                
                let last_update = metric.last_update.load(Ordering::Relaxed);
                let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let staleness = (current_time - last_update) as f64;
                
                if staleness > 60.0 {
                    prob *= 0.9_f64.powf(staleness / 60.0);
                }
                
                if network_congestion > 0.8 && metric.avg_latency() > 100.0 {
                    prob *= 0.7;
                }
            }
            
            probabilities.push((id.clone(), prob));
        }
        
        let total: f64 = probabilities.iter().map(|(_, p)| p).sum();
        if total > 0.0 {
            for (_, prob) in &mut probabilities {
                *prob /= total;
            }
        }
        
        probabilities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        probabilities
    }

    pub async fn update_network_conditions(
        &self,
        avg_block_time_ms: u64,
        network_congestion_ratio: f64,
        competition_intensity: f64,
    ) {
        self.global_metrics.avg_block_time.store(avg_block_time_ms, Ordering::Relaxed);
        self.global_metrics.network_congestion.store((network_congestion_ratio * 1000.0) as u64, Ordering::Relaxed);
        self.global_metrics.competition_intensity.store((competition_intensity * 1000.0) as u64, Ordering::Relaxed);
        
        let mut metrics = self.metrics.write().unwrap();
        for metric in metrics.values_mut() {
            metric.network_conditions = 1.0 - network_congestion_ratio * 0.5;
            metric.competition_level = competition_intensity;
        }
    }

    pub async fn decay_old_metrics(&self) {
        let mut metrics = self.metrics.write().unwrap();
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        for metric in metrics.values_mut() {
            let last_update = metric.last_update.load(Ordering::Relaxed);
            let age_seconds = (current_time - last_update) as f64;
            
            if age_seconds > 300.0 {
                let decay = DECAY_FACTOR.powf(age_seconds / 300.0);
                let attempts = metric.total_attempts.load(Ordering::Relaxed);
                let successes = metric.successful_executions.load(Ordering::Relaxed);
                
                metric.total_attempts.store((attempts as f64 * decay) as u64, Ordering::Relaxed);
                metric.successful_executions.store((successes as f64 * decay) as u64, Ordering::Relaxed);
            }
        }
        
        let mut sampler = self.thompson_sampler.lock().await;
        for (_, alpha) in sampler.alpha_params.iter_mut() {
            *alpha = (*alpha - 1.0) * DECAY_FACTOR + 1.0;
        }
        for (_, beta) in sampler.beta_params.iter_mut() {
            *beta = (*beta - 1.0) * DECAY_FACTOR + 1.0;
        }
    }

    pub async fn calculate_volatility(&self, strategy_id: &str) -> f64 {
        let history = self.performance_history.lock().await;
        
        if let Some(snapshots) = history.get(strategy_id) {
            if snapshots.len() < 10 {
                return 0.5;
            }
            
            let recent = &snapshots[snapshots.len().saturating_sub(VOLATILITY_WINDOW)..];
            let returns: Vec<f64> = recent.iter().map(|s| s.risk_adjusted_return).collect();
            
            if returns.is_empty() {
                return 0.5;
            }
            
            let mean = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
            
            variance.sqrt()
        } else {
            0.5
        }
    }

    pub async fn update_strategy_volatility(&self) {
        let strategies: Vec<String> = {
            let weights = self.weights.read().unwrap();
            weights.keys().cloned().collect()
        };
        
        for strategy_id in strategies {
            let volatility = self.calculate_volatility(&strategy_id).await;
            
            let mut metrics = self.metrics.write().unwrap();
            if let Some(metric) = metrics.get_mut(&strategy_id) {
                metric.volatility_score = volatility;
            }
        }
    }

    pub async fn get_performance_report(&self) -> HashMap<String, serde_json::Value> {
        let weights = self.weights.read().unwrap();
        let metrics = self.metrics.read().unwrap();
        let mut report = HashMap::new();
        
        for (strategy_id, weight) in weights.iter() {
            if let Some(metric) = metrics.get(strategy_id) {
                let mut strategy_report = serde_json::json!({
                    "current_weight": weight.current_weight,
                    "target_weight": weight.target_weight,
                    "confidence_interval": weight.confidence_interval,
                    "momentum": weight.momentum,
                    "success_rate": metric.success_rate(),
                    "avg_profit": metric.avg_profit(),
                    "avg_latency": metric.avg_latency(),
                    "volatility": metric.volatility_score,
                    "total_attempts": metric.total_attempts.load(Ordering::Relaxed),
                    "consecutive_failures": metric.consecutive_failures.load(Ordering::Relaxed),
                    "risk_adjusted_return": self.calculate_risk_adjusted_return(metric),
                });
                
                report.insert(strategy_id.clone(), strategy_report);
            }
        }
        
        report
    }

    pub async fn emergency_rebalance(&self) {
        let mut weights = self.weights.write().unwrap();
        let metrics = self.metrics.read().unwrap();
        
        let equal_weight = 1.0 / weights.len() as f64;
        
        for (strategy_id, weight) in weights.iter_mut() {
            if let Some(metric) = metrics.get(strategy_id) {
                let failures = metric.consecutive_failures.load(Ordering::Relaxed);
                if failures > 10 {
                    weight.current_weight = MIN_WEIGHT;
                } else {
                    weight.current_weight = equal_weight;
                }
                weight.momentum = 0.0;
                weight.adjustment_velocity = 0.0;
            }
        }
        
        self.ensure_weight_sum_unity(&mut weights);
    }

    pub async fn persist_state(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let weights = self.weights.read().unwrap();
        let metrics = self.metrics.read().unwrap();
        
        let state = serde_json::json!({
            "weights": weights.iter().map(|(k, v)| (k.clone(), v.current_weight)).collect::<HashMap<_, _>>(),
            "metrics": metrics.iter().map(|(k, v)| {
                (k.clone(), serde_json::json!({
                    "total_attempts": v.total_attempts.load(Ordering::Relaxed),
                    "successful_executions": v.successful_executions.load(Ordering::Relaxed),
                    "total_profit": v.total_profit.load(Ordering::Relaxed),
                    "volatility_score": v.volatility_score,
                }))
            }).collect::<HashMap<_, _>>(),
        });
        
        Ok(serde_json::to_vec(&state)?)
    }

    pub async fn restore_state(&self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let state: serde_json::Value = serde_json::from_slice(data)?;
        
        if let Some(weights_data) = state.get("weights").and_then(|v| v.as_object()) {
            let mut weights = self.weights.write().unwrap();
            for (strategy_id, weight_value) in weights_data {
                if let Some(weight) = weight_value.as_f64() {
                    if let Some(strategy_weight) = weights.get_mut(strategy_id) {
                        strategy_weight.current_weight = weight;
                        strategy_weight.target_weight = weight;
                    }
                }
            }
        }
        
        Ok(())
    }
}

impl Default for DynamicStrategyWeightOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

