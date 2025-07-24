use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::clock::Slot;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use anyhow::{Result, Context};

const FACTOR_WINDOW_SIZE: usize = 1000;
const DECAY_FACTOR: f64 = 0.95;
const MIN_SAMPLE_SIZE: usize = 50;
const ATTRIBUTION_UPDATE_INTERVAL_MS: u64 = 100;
const MAX_FACTOR_AGE_MS: u64 = 30000;
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const CORRELATION_THRESHOLD: f64 = 0.3;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FactorMetrics {
    pub timestamp: Instant,
    pub slot: Slot,
    pub gas_price: f64,
    pub network_congestion: f64,
    pub liquidity_depth: f64,
    pub price_volatility: f64,
    pub competition_level: f64,
    pub slippage_estimate: f64,
    pub block_space_utilization: f64,
    pub priority_fee_percentile: f64,
    pub success_probability: f64,
    pub latency_ms: f64,
}

#[derive(Clone, Debug)]
pub struct FactorAttribution {
    pub factor_name: String,
    pub weight: f64,
    pub contribution: f64,
    pub confidence: f64,
    pub correlation: f64,
    pub samples: usize,
    pub last_updated: Instant,
}

#[derive(Clone, Debug)]
pub struct AttributionResult {
    pub total_attribution: f64,
    pub factors: HashMap<String, FactorAttribution>,
    pub confidence_score: f64,
    pub timestamp: Instant,
    pub recommendations: Vec<String>,
}

pub struct FactorAttributionEngine {
    factor_history: Arc<RwLock<VecDeque<FactorMetrics>>>,
    attribution_cache: Arc<RwLock<HashMap<String, AttributionResult>>>,
    factor_weights: Arc<RwLock<HashMap<String, f64>>>,
    correlation_matrix: Arc<RwLock<HashMap<(String, String), f64>>>,
    performance_history: Arc<RwLock<VecDeque<(Instant, f64)>>>,
    update_sender: mpsc::UnboundedSender<FactorMetrics>,
    shutdown_signal: Arc<RwLock<bool>>,
}

impl FactorAttributionEngine {
    pub fn new() -> Result<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let factor_history = Arc::new(RwLock::new(VecDeque::with_capacity(FACTOR_WINDOW_SIZE)));
        let attribution_cache = Arc::new(RwLock::new(HashMap::new()));
        let factor_weights = Arc::new(RwLock::new(Self::initialize_weights()));
        let correlation_matrix = Arc::new(RwLock::new(HashMap::new()));
        let performance_history = Arc::new(RwLock::new(VecDeque::with_capacity(FACTOR_WINDOW_SIZE)));
        let shutdown_signal = Arc::new(RwLock::new(false));

        let engine = Self {
            factor_history: factor_history.clone(),
            attribution_cache: attribution_cache.clone(),
            factor_weights: factor_weights.clone(),
            correlation_matrix: correlation_matrix.clone(),
            performance_history: performance_history.clone(),
            update_sender: tx,
            shutdown_signal: shutdown_signal.clone(),
        };

        let history_clone = factor_history.clone();
        let cache_clone = attribution_cache.clone();
        let weights_clone = factor_weights.clone();
        let correlation_clone = correlation_matrix.clone();
        let perf_clone = performance_history.clone();
        let shutdown_clone = shutdown_signal.clone();

        tokio::spawn(async move {
            let mut update_interval = tokio::time::interval(Duration::from_millis(ATTRIBUTION_UPDATE_INTERVAL_MS));
            
            loop {
                tokio::select! {
                    Some(metrics) = rx.recv() => {
                        Self::process_metrics(
                            metrics,
                            &history_clone,
                            &cache_clone,
                            &weights_clone,
                            &correlation_clone,
                            &perf_clone
                        ).await;
                    }
                    _ = update_interval.tick() => {
                        if *shutdown_clone.read().unwrap() {
                            break;
                        }
                        Self::update_attributions(
                            &history_clone,
                            &cache_clone,
                            &weights_clone,
                            &correlation_clone
                        ).await;
                    }
                }
            }
        });

        Ok(engine)
    }

    fn initialize_weights() -> HashMap<String, f64> {
        let mut weights = HashMap::new();
        weights.insert("gas_price".to_string(), 0.15);
        weights.insert("network_congestion".to_string(), 0.12);
        weights.insert("liquidity_depth".to_string(), 0.20);
        weights.insert("price_volatility".to_string(), 0.18);
        weights.insert("competition_level".to_string(), 0.10);
        weights.insert("slippage_estimate".to_string(), 0.08);
        weights.insert("block_space_utilization".to_string(), 0.07);
        weights.insert("priority_fee_percentile".to_string(), 0.05);
        weights.insert("success_probability".to_string(), 0.03);
        weights.insert("latency_ms".to_string(), 0.02);
        weights
    }

    pub async fn submit_metrics(&self, metrics: FactorMetrics) -> Result<()> {
        self.update_sender.send(metrics)
            .context("Failed to submit metrics")?;
        Ok(())
    }

    async fn process_metrics(
        metrics: FactorMetrics,
        history: &Arc<RwLock<VecDeque<FactorMetrics>>>,
        cache: &Arc<RwLock<HashMap<String, AttributionResult>>>,
        weights: &Arc<RwLock<HashMap<String, f64>>>,
        correlations: &Arc<RwLock<HashMap<(String, String), f64>>>,
        performance: &Arc<RwLock<VecDeque<(Instant, f64)>>>,
    ) {
        let mut hist = history.write().unwrap();
        
        if hist.len() >= FACTOR_WINDOW_SIZE {
            hist.pop_front();
        }
        hist.push_back(metrics.clone());
        
        drop(hist);

        if let Ok(attribution) = Self::calculate_attribution(&metrics, history, weights, correlations).await {
            let mut cache_guard = cache.write().unwrap();
            cache_guard.insert(metrics.slot.to_string(), attribution);
            
            if cache_guard.len() > FACTOR_WINDOW_SIZE * 2 {
                let oldest_key = cache_guard.keys().next().cloned();
                if let Some(key) = oldest_key {
                    cache_guard.remove(&key);
                }
            }
        }
    }

    async fn calculate_attribution(
        metrics: &FactorMetrics,
        history: &Arc<RwLock<VecDeque<FactorMetrics>>>,
        weights: &Arc<RwLock<HashMap<String, f64>>>,
        correlations: &Arc<RwLock<HashMap<(String, String), f64>>>,
    ) -> Result<AttributionResult> {
        let hist = history.read().unwrap();
        if hist.len() < MIN_SAMPLE_SIZE {
            return Err(anyhow::anyhow!("Insufficient data for attribution"));
        }

        let weights_guard = weights.read().unwrap();
        let mut factors = HashMap::new();
        let mut total_contribution = 0.0;
        let mut total_weight = 0.0;

        let factor_values = vec![
            ("gas_price", metrics.gas_price),
            ("network_congestion", metrics.network_congestion),
            ("liquidity_depth", metrics.liquidity_depth),
            ("price_volatility", metrics.price_volatility),
            ("competition_level", metrics.competition_level),
            ("slippage_estimate", metrics.slippage_estimate),
            ("block_space_utilization", metrics.block_space_utilization),
            ("priority_fee_percentile", metrics.priority_fee_percentile),
            ("success_probability", metrics.success_probability),
            ("latency_ms", metrics.latency_ms),
        ];

        for (factor_name, value) in factor_values {
            let weight = weights_guard.get(factor_name).copied().unwrap_or(0.1);
            let normalized_value = Self::normalize_factor_value(factor_name, value, &hist);
            let contribution = weight * normalized_value;
            
            let correlation = Self::calculate_factor_correlation(factor_name, &hist);
            let confidence = Self::calculate_confidence(factor_name, &hist, correlation);
            
            factors.insert(
                factor_name.to_string(),
                FactorAttribution {
                    factor_name: factor_name.to_string(),
                    weight,
                    contribution,
                    confidence,
                    correlation,
                    samples: hist.len(),
                    last_updated: Instant::now(),
                },
            );

            total_contribution += contribution;
            total_weight += weight;
        }

        let confidence_score = Self::calculate_overall_confidence(&factors);
        let recommendations = Self::generate_recommendations(&factors, metrics);

        Ok(AttributionResult {
            total_attribution: total_contribution / total_weight.max(0.001),
            factors,
            confidence_score,
            timestamp: Instant::now(),
            recommendations,
        })
    }

    fn normalize_factor_value(factor_name: &str, value: f64, history: &VecDeque<FactorMetrics>) -> f64 {
        let values: Vec<f64> = history.iter().map(|m| {
            match factor_name {
                "gas_price" => m.gas_price,
                "network_congestion" => m.network_congestion,
                "liquidity_depth" => m.liquidity_depth,
                "price_volatility" => m.price_volatility,
                "competition_level" => m.competition_level,
                "slippage_estimate" => m.slippage_estimate,
                "block_space_utilization" => m.block_space_utilization,
                "priority_fee_percentile" => m.priority_fee_percentile,
                "success_probability" => m.success_probability,
                "latency_ms" => m.latency_ms,
                _ => 0.0,
            }
        }).collect();

        if values.is_empty() {
            return 0.5;
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev < 0.0001 {
            return 0.5;
        }

        let z_score = (value - mean) / std_dev;
        let normalized = 1.0 / (1.0 + (-z_score).exp());

        match factor_name {
            "liquidity_depth" | "success_probability" => normalized,
            "gas_price" | "network_congestion" | "competition_level" | 
            "slippage_estimate" | "latency_ms" => 1.0 - normalized,
            _ => normalized,
        }
    }

    fn calculate_factor_correlation(factor_name: &str, history: &VecDeque<FactorMetrics>) -> f64 {
        if history.len() < MIN_SAMPLE_SIZE {
            return 0.0;
        }

        let factor_values: Vec<f64> = history.iter().map(|m| {
            match factor_name {
                "gas_price" => m.gas_price,
                "network_congestion" => m.network_congestion,
                "liquidity_depth" => m.liquidity_depth,
                "price_volatility" => m.price_volatility,
                "competition_level" => m.competition_level,
                "slippage_estimate" => m.slippage_estimate,
                "block_space_utilization" => m.block_space_utilization,
                "priority_fee_percentile" => m.priority_fee_percentile,
                "success_probability" => m.success_probability,
                "latency_ms" => m.latency_ms,
                _ => 0.0,
            }
        }).collect();

        let success_values: Vec<f64> = history.iter().map(|m| m.success_probability).collect();

        Self::pearson_correlation(&factor_values, &success_values)
    }

    fn pearson_correlation(x: &[f64], y: &[f64]) -> f64 {
        let n = x.len().min(y.len()) as f64;
        if n < 2.0 {
            return 0.0;
        }

        let sum_x: f64 = x.iter().take(n as usize).sum();
        let sum_y: f64 = y.iter().take(n as usize).sum();
        let sum_xy: f64 = x.iter().zip(y.iter()).take(n as usize).map(|(a, b)| a * b).sum();
        let sum_x2: f64 = x.iter().take(n as usize).map(|a| a * a).sum();
        let sum_y2: f64 = y.iter().take(n as usize).map(|b| b * b).sum();

        let numerator = n * sum_xy - sum_x * sum_y;
        let denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)).sqrt();

        if denominator < 0.0001 {
            return 0.0;
        }

        (numerator / denominator).max(-1.0).min(1.0)
    }

    fn calculate_confidence(factor_name: &str, history: &VecDeque<FactorMetrics>, correlation: f64) -> f64 {
            let sample_confidence = (history.len() as f64 / FACTOR_WINDOW_SIZE as f64).min(1.0);
        let correlation_confidence = correlation.abs();
        let recency_factor = Self::calculate_recency_factor(history);
        
        (sample_confidence * 0.4 + correlation_confidence * 0.4 + recency_factor * 0.2)
            .max(0.0)
            .min(1.0)
    }

    fn calculate_recency_factor(history: &VecDeque<FactorMetrics>) -> f64 {
        if history.is_empty() {
            return 0.0;
        }

        let now = Instant::now();
        let weights: Vec<f64> = history.iter().enumerate().map(|(i, m)| {
            let age_ms = now.duration_since(m.timestamp).as_millis() as f64;
            let age_factor = (-age_ms / MAX_FACTOR_AGE_MS as f64).exp();
            let position_factor = (i as f64 / history.len() as f64).powf(0.5);
            age_factor * position_factor
        }).collect();

        let sum_weights: f64 = weights.iter().sum();
        if sum_weights > 0.0 {
            weights.iter().sum::<f64>() / history.len() as f64
        } else {
            0.0
        }
    }

    fn calculate_overall_confidence(factors: &HashMap<String, FactorAttribution>) -> f64 {
        if factors.is_empty() {
            return 0.0;
        }

        let weighted_confidence: f64 = factors.values()
            .map(|f| f.confidence * f.weight)
            .sum();
        
        let total_weight: f64 = factors.values()
            .map(|f| f.weight)
            .sum();

        if total_weight > 0.0 {
            (weighted_confidence / total_weight).max(0.0).min(1.0)
        } else {
            0.0
        }
    }

    fn generate_recommendations(factors: &HashMap<String, FactorAttribution>, metrics: &FactorMetrics) -> Vec<String> {
        let mut recommendations = Vec::new();

        if let Some(gas_attr) = factors.get("gas_price") {
            if gas_attr.contribution > 0.7 && metrics.gas_price > 100.0 {
                recommendations.push("High gas prices detected - consider delaying non-urgent transactions".to_string());
            }
        }

        if let Some(liquidity_attr) = factors.get("liquidity_depth") {
            if liquidity_attr.contribution < 0.3 && metrics.liquidity_depth < 1000000.0 {
                recommendations.push("Low liquidity depth - increase slippage tolerance or reduce position size".to_string());
            }
        }

        if let Some(volatility_attr) = factors.get("price_volatility") {
            if volatility_attr.contribution > 0.6 && metrics.price_volatility > 0.05 {
                recommendations.push("High volatility - consider tighter stop losses and smaller positions".to_string());
            }
        }

        if let Some(competition_attr) = factors.get("competition_level") {
            if competition_attr.contribution > 0.5 && metrics.competition_level > 0.7 {
                recommendations.push("High competition detected - increase priority fees for critical transactions".to_string());
            }
        }

        if metrics.success_probability < 0.5 {
            recommendations.push("Low success probability - review strategy parameters and market conditions".to_string());
        }

        if metrics.latency_ms > 50.0 {
            recommendations.push("High latency detected - optimize network connection or use closer RPC nodes".to_string());
        }

        recommendations
    }

    async fn update_attributions(
        history: &Arc<RwLock<VecDeque<FactorMetrics>>>,
        cache: &Arc<RwLock<HashMap<String, AttributionResult>>>,
        weights: &Arc<RwLock<HashMap<String, f64>>>,
        correlations: &Arc<RwLock<HashMap<(String, String), f64>>>,
    ) {
        let hist = history.read().unwrap();
        if hist.len() < MIN_SAMPLE_SIZE {
            return;
        }

        let factor_names = vec![
            "gas_price",
            "network_congestion",
            "liquidity_depth",
            "price_volatility",
            "competition_level",
            "slippage_estimate",
            "block_space_utilization",
            "priority_fee_percentile",
            "success_probability",
            "latency_ms",
        ];

        let mut new_correlations = HashMap::new();
        for i in 0..factor_names.len() {
            for j in i+1..factor_names.len() {
                let factor_i = factor_names[i];
                let factor_j = factor_names[j];
                
                let values_i: Vec<f64> = hist.iter().map(|m| {
                    Self::get_factor_value(m, factor_i)
                }).collect();
                
                let values_j: Vec<f64> = hist.iter().map(|m| {
                    Self::get_factor_value(m, factor_j)
                }).collect();
                
                let correlation = Self::pearson_correlation(&values_i, &values_j);
                new_correlations.insert((factor_i.to_string(), factor_j.to_string()), correlation);
                new_correlations.insert((factor_j.to_string(), factor_i.to_string()), correlation);
            }
        }

        *correlations.write().unwrap() = new_correlations;

        Self::update_weights_adaptive(history, weights).await;
    }

    fn get_factor_value(metrics: &FactorMetrics, factor_name: &str) -> f64 {
        match factor_name {
            "gas_price" => metrics.gas_price,
            "network_congestion" => metrics.network_congestion,
            "liquidity_depth" => metrics.liquidity_depth,
            "price_volatility" => metrics.price_volatility,
            "competition_level" => metrics.competition_level,
            "slippage_estimate" => metrics.slippage_estimate,
            "block_space_utilization" => metrics.block_space_utilization,
            "priority_fee_percentile" => metrics.priority_fee_percentile,
            "success_probability" => metrics.success_probability,
            "latency_ms" => metrics.latency_ms,
            _ => 0.0,
        }
    }

    async fn update_weights_adaptive(
        history: &Arc<RwLock<VecDeque<FactorMetrics>>>,
        weights: &Arc<RwLock<HashMap<String, f64>>>,
    ) {
        let hist = history.read().unwrap();
        if hist.len() < MIN_SAMPLE_SIZE * 2 {
            return;
        }

        let mut new_weights = HashMap::new();
        let factor_names = vec![
            "gas_price",
            "network_congestion",
            "liquidity_depth",
            "price_volatility",
            "competition_level",
            "slippage_estimate",
            "block_space_utilization",
            "priority_fee_percentile",
            "success_probability",
            "latency_ms",
        ];

        let mut importance_scores = HashMap::new();
        
        for factor_name in &factor_names {
            let correlation = Self::calculate_factor_correlation(factor_name, &hist);
            let variance = Self::calculate_factor_variance(factor_name, &hist);
            let impact = Self::calculate_factor_impact(factor_name, &hist);
            
            let importance = (correlation.abs() * 0.4 + variance * 0.3 + impact * 0.3)
                .max(0.01)
                .min(1.0);
            
            importance_scores.insert(factor_name.to_string(), importance);
        }

        let total_importance: f64 = importance_scores.values().sum();
        
        for (factor_name, importance) in importance_scores {
            let normalized_weight = importance / total_importance;
            let current_weight = weights.read().unwrap().get(&factor_name).copied().unwrap_or(0.1);
            let new_weight = current_weight * (1.0 - DECAY_FACTOR) + normalized_weight * DECAY_FACTOR;
            new_weights.insert(factor_name, new_weight.max(0.01).min(0.5));
        }

        *weights.write().unwrap() = new_weights;
    }

    fn calculate_factor_variance(factor_name: &str, history: &VecDeque<FactorMetrics>) -> f64 {
        let values: Vec<f64> = history.iter().map(|m| {
            Self::get_factor_value(m, factor_name)
        }).collect();

        if values.len() < 2 {
            return 0.0;
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let normalized_variance = (variance / (mean.powi(2) + 1.0)).min(1.0);
        
        normalized_variance
    }

    fn calculate_factor_impact(factor_name: &str, history: &VecDeque<FactorMetrics>) -> f64 {
        if history.len() < MIN_SAMPLE_SIZE {
            return 0.5;
        }

        let mut high_success_values = Vec::new();
        let mut low_success_values = Vec::new();

        for metrics in history.iter() {
            let value = Self::get_factor_value(metrics, factor_name);
            if metrics.success_probability > 0.7 {
                high_success_values.push(value);
            } else if metrics.success_probability < 0.3 {
                low_success_values.push(value);
            }
        }

        if high_success_values.is_empty() || low_success_values.is_empty() {
            return 0.5;
        }

        let high_mean = high_success_values.iter().sum::<f64>() / high_success_values.len() as f64;
        let low_mean = low_success_values.iter().sum::<f64>() / low_success_values.len() as f64;
        
        let difference = (high_mean - low_mean).abs();
        let scale = (high_mean.abs() + low_mean.abs()) / 2.0 + 1.0;
        
        (difference / scale).min(1.0)
    }

    pub async fn get_current_attribution(&self, pool_address: &Pubkey) -> Result<AttributionResult> {
        let cache = self.attribution_cache.read().unwrap();
        
        if let Some(result) = cache.values().max_by_key(|r| r.timestamp) {
            if result.timestamp.elapsed() < Duration::from_millis(MAX_FACTOR_AGE_MS) {
                return Ok(result.clone());
            }
        }

        let history = self.factor_history.read().unwrap();
        if let Some(latest_metrics) = history.back() {
            Self::calculate_attribution(
                latest_metrics,
                &self.factor_history,
                &self.factor_weights,
                &self.correlation_matrix,
            ).await
        } else {
            Err(anyhow::anyhow!("No metrics available for attribution"))
        }
    }

    pub async fn get_factor_recommendation(&self, factor_name: &str) -> Result<String> {
        let history = self.factor_history.read().unwrap();
        if history.is_empty() {
            return Err(anyhow::anyhow!("No historical data available"));
        }

        let current_value = Self::get_factor_value(history.back().unwrap(), factor_name);
        let mean = history.iter()
            .map(|m| Self::get_factor_value(m, factor_name))
            .sum::<f64>() / history.len() as f64;

        let recommendation = match factor_name {
            "gas_price" => {
                if current_value > mean * 1.5 {
                    "Gas prices are significantly elevated. Consider postponing non-critical transactions."
                } else if current_value < mean * 0.7 {
                    "Gas prices are favorable. Good opportunity for executing pending transactions."
                } else {
                    "Gas prices are within normal range."
                }
            },
            "liquidity_depth" => {
                if current_value < mean * 0.5 {
                    "Liquidity is critically low. Reduce position sizes to avoid high slippage."
                } else if current_value > mean * 1.5 {
                    "Excellent liquidity available. Larger positions can be executed efficiently."
                } else {
                    "Liquidity levels are adequate for standard operations."
                }
            },
            "competition_level" => {
                if current_value > 0.8 {
                    "Extreme competition detected. Maximize priority fees and optimize latency."
                } else if current_value < 0.3 {
                    "Low competition environment. Standard priority fees should suffice."
                } else {
                    "Moderate competition. Monitor closely and adjust fees as needed."
                }
            },
            _ => "Factor within expected parameters.",
        };

        Ok(recommendation.to_string())
    }

    pub async fn shutdown(&self) -> Result<()> {
        *self.shutdown_signal.write().unwrap() = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_factor_attribution_engine() {
        let engine = FactorAttributionEngine::new().unwrap();
        
        let metrics = FactorMetrics {
            timestamp: Instant::now(),
            slot: 100,
            gas_price: 50.0,
            network_congestion: 0.5,
            liquidity_depth: 1000000.0,
            price_volatility: 0.02,
            competition_level: 0.4,
            slippage_estimate: 0.001,
            block_space_utilization: 0.6,
            priority_fee_percentile: 75.0,
            success_probability: 0.8,
            latency_ms: 25.0,
        };

        engine.submit_metrics(metrics).await.unwrap();
    }
}

