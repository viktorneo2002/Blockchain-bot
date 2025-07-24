use std::sync::Arc;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use anyhow::{Result, Context};

const MAX_MODEL_HISTORY: usize = 1000;
const PERFORMANCE_WINDOW: usize = 100;
const MIN_CONFIDENCE_THRESHOLD: f64 = 0.65;
const WEIGHT_DECAY_FACTOR: f64 = 0.95;
const ADAPTIVE_LEARNING_RATE: f64 = 0.01;
const MAX_CORRELATION_THRESHOLD: f64 = 0.85;
const MIN_MODEL_WEIGHT: f64 = 0.05;
const VOLATILITY_ADJUSTMENT_FACTOR: f64 = 1.5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPrediction {
    pub model_id: String,
    pub timestamp: u64,
    pub opportunity_score: f64,
    pub confidence: f64,
    pub predicted_profit: f64,
    pub risk_score: f64,
    pub time_sensitivity: f64,
    pub market_impact: f64,
    pub features: HashMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct AggregatedSignal {
    pub action: TradingAction,
    pub confidence: f64,
    pub expected_profit: f64,
    pub risk_adjusted_score: f64,
    pub urgency_score: f64,
    pub position_size_multiplier: f64,
    pub stop_loss_percentage: f64,
    pub take_profit_targets: Vec<f64>,
    pub contributing_models: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TradingAction {
    Buy { amount_lamports: u64 },
    Sell { amount_lamports: u64 },
    ArbitrageBuy { path: Vec<String>, amount: u64 },
    ArbitrageSell { path: Vec<String>, amount: u64 },
    LiquidationOpportunity { target_account: String, collateral_value: u64 },
    Hold,
}

#[derive(Debug, Clone)]
struct ModelPerformance {
    model_id: String,
    weight: f64,
    accuracy_history: VecDeque<f64>,
    profit_history: VecDeque<f64>,
    risk_history: VecDeque<f64>,
    last_update: Instant,
    total_predictions: u64,
    successful_predictions: u64,
    cumulative_profit: f64,
    sharpe_ratio: f64,
    max_drawdown: f64,
}

impl ModelPerformance {
    fn new(model_id: String) -> Self {
        Self {
            model_id,
            weight: 1.0 / 5.0,
            accuracy_history: VecDeque::with_capacity(MAX_MODEL_HISTORY),
            profit_history: VecDeque::with_capacity(MAX_MODEL_HISTORY),
            risk_history: VecDeque::with_capacity(MAX_MODEL_HISTORY),
            last_update: Instant::now(),
            total_predictions: 0,
            successful_predictions: 0,
            cumulative_profit: 0.0,
            sharpe_ratio: 0.0,
            max_drawdown: 0.0,
        }
    }

    fn update_performance(&mut self, accuracy: f64, profit: f64, risk: f64) {
        if self.accuracy_history.len() >= MAX_MODEL_HISTORY {
            self.accuracy_history.pop_front();
            self.profit_history.pop_front();
            self.risk_history.pop_front();
        }

        self.accuracy_history.push_back(accuracy);
        self.profit_history.push_back(profit);
        self.risk_history.push_back(risk);
        
        self.total_predictions += 1;
        if accuracy > 0.5 {
            self.successful_predictions += 1;
        }
        
        self.cumulative_profit += profit;
        self.calculate_sharpe_ratio();
        self.calculate_max_drawdown();
        self.last_update = Instant::now();
    }

    fn calculate_sharpe_ratio(&mut self) {
        if self.profit_history.len() < 2 {
            return;
        }

        let returns: Vec<f64> = self.profit_history.iter().copied().collect();
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        let std_dev = variance.sqrt();
        self.sharpe_ratio = if std_dev > 0.0 {
            (mean_return * 252.0_f64.sqrt()) / std_dev
        } else {
            0.0
        };
    }

    fn calculate_max_drawdown(&mut self) {
        if self.profit_history.is_empty() {
            return;
        }

        let mut cumulative = 0.0;
        let mut peak = 0.0;
        let mut max_dd = 0.0;

        for profit in &self.profit_history {
            cumulative += profit;
            if cumulative > peak {
                peak = cumulative;
            }
            let drawdown = (peak - cumulative) / peak.max(1.0);
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }

        self.max_drawdown = max_dd;
    }

    fn get_performance_score(&self) -> f64 {
        let accuracy_score = self.successful_predictions as f64 / self.total_predictions.max(1) as f64;
        let profit_score = (self.cumulative_profit / self.total_predictions.max(1) as f64).tanh();
        let risk_adjusted_score = (self.sharpe_ratio / 3.0).tanh();
        let drawdown_penalty = 1.0 - self.max_drawdown.min(1.0);
        
        (accuracy_score * 0.3 + profit_score * 0.3 + risk_adjusted_score * 0.3 + drawdown_penalty * 0.1)
            .max(0.0)
            .min(1.0)
    }
}

pub struct EnsembleModelAggregator {
    model_performances: Arc<RwLock<HashMap<String, ModelPerformance>>>,
    correlation_matrix: Arc<RwLock<HashMap<(String, String), f64>>>,
    market_regime: Arc<RwLock<MarketRegime>>,
    prediction_cache: Arc<RwLock<VecDeque<(u64, ModelPrediction)>>>,
}

#[derive(Debug, Clone)]
enum MarketRegime {
    Trending { direction: f64, strength: f64 },
    Ranging { volatility: f64 },
    Volatile { intensity: f64 },
    Uncertain,
}

impl EnsembleModelAggregator {
    pub fn new() -> Self {
        Self {
            model_performances: Arc::new(RwLock::new(HashMap::new())),
            correlation_matrix: Arc::new(RwLock::new(HashMap::new())),
            market_regime: Arc::new(RwLock::new(MarketRegime::Uncertain)),
            prediction_cache: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
        }
    }

    pub async fn aggregate_predictions(
        &self,
        predictions: Vec<ModelPrediction>,
        current_timestamp: u64,
    ) -> Result<Option<AggregatedSignal>> {
        if predictions.is_empty() {
            return Ok(None);
        }

        self.update_prediction_cache(predictions.clone(), current_timestamp).await?;
        self.update_model_correlations(&predictions).await?;
        let regime = self.detect_market_regime(&predictions).await?;
        
        let mut weights = self.calculate_dynamic_weights(&predictions, &regime).await?;
        self.normalize_weights(&mut weights);

        let aggregated = self.weighted_aggregate(&predictions, &weights, &regime).await?;
        
        if aggregated.confidence < MIN_CONFIDENCE_THRESHOLD {
            return Ok(None);
        }

        Ok(Some(aggregated))
    }

    async fn update_prediction_cache(
        &self,
        predictions: Vec<ModelPrediction>,
        current_timestamp: u64,
    ) -> Result<()> {
        let mut cache = self.prediction_cache.write().await;
        
        for pred in predictions {
            cache.push_back((current_timestamp, pred));
        }
        
        while cache.len() > 1000 {
            cache.pop_front();
        }
        
        Ok(())
    }

    async fn update_model_correlations(&self, predictions: &[ModelPrediction]) -> Result<()> {
        let mut correlations = self.correlation_matrix.write().await;
        
        for i in 0..predictions.len() {
            for j in i+1..predictions.len() {
                let model_i = &predictions[i].model_id;
                let model_j = &predictions[j].model_id;
                
                let correlation = self.calculate_correlation(
                    predictions[i].opportunity_score,
                    predictions[j].opportunity_score,
                    predictions[i].confidence,
                    predictions[j].confidence,
                );
                
                correlations.insert((model_i.clone(), model_j.clone()), correlation);
                correlations.insert((model_j.clone(), model_i.clone()), correlation);
            }
        }
        
        Ok(())
    }

    fn calculate_correlation(&self, score1: f64, score2: f64, conf1: f64, conf2: f64) -> f64 {
        let weighted_diff = ((score1 * conf1) - (score2 * conf2)).abs();
        1.0 - weighted_diff.min(1.0)
    }

    async fn detect_market_regime(&self, predictions: &[ModelPrediction]) -> Result<MarketRegime> {
        let avg_volatility = predictions.iter()
            .map(|p| p.features.get("volatility").copied().unwrap_or(0.5))
            .sum::<f64>() / predictions.len() as f64;
        
        let trend_scores: Vec<f64> = predictions.iter()
            .filter_map(|p| p.features.get("trend_strength").copied())
            .collect();
        
        if trend_scores.len() >= predictions.len() / 2 {
            let avg_trend = trend_scores.iter().sum::<f64>() / trend_scores.len() as f64;
            let trend_direction = predictions.iter()
                .filter_map(|p| p.features.get("trend_direction").copied())
                .sum::<f64>() / predictions.len() as f64;
            
            if avg_trend > 0.6 {
                return Ok(MarketRegime::Trending {
                    direction: trend_direction,
                    strength: avg_trend,
                });
            }
        }
        
        if avg_volatility > 0.7 {
            Ok(MarketRegime::Volatile { intensity: avg_volatility })
        } else if avg_volatility < 0.3 {
            Ok(MarketRegime::Ranging { volatility: avg_volatility })
        } else {
            Ok(MarketRegime::Uncertain)
        }
    }

    async fn calculate_dynamic_weights(
        &self,
        predictions: &[ModelPrediction],
        regime: &MarketRegime,
    ) -> Result<HashMap<String, f64>> {
        let mut performances = self.model_performances.write().await;
        let mut weights = HashMap::new();
        
        for pred in predictions {
            let perf = performances.entry(pred.model_id.clone())
                .or_insert_with(|| ModelPerformance::new(pred.model_id.clone()));
            
            let base_weight = perf.weight;
            let performance_score = perf.get_performance_score();
            let regime_adjustment = self.get_regime_weight_adjustment(&pred, regime);
            
            let correlation_penalty = self.calculate_correlation_penalty(&pred.model_id).await?;
            
            let adjusted_weight = base_weight * performance_score * regime_adjustment * correlation_penalty;
            weights.insert(pred.model_id.clone(), adjusted_weight.max(MIN_MODEL_WEIGHT));
        }
        
        Ok(weights)
    }

    fn get_regime_weight_adjustment(&self, prediction: &ModelPrediction, regime: &MarketRegime) -> f64 {
        match regime {
            MarketRegime::Trending { strength, .. } => {
                let trend_feature = prediction.features.get("trend_following_score").unwrap_or(&0.5);
                1.0 + (trend_feature * strength * 0.5)
            },
            MarketRegime::Ranging { volatility } => {
                let mean_reversion = prediction.features.get("mean_reversion_score").unwrap_or(&0.5);
                1.0 + (mean_reversion * (1.0 - volatility) * 0.3)
            },
            MarketRegime::Volatile { intensity } => {
                let vol_handling = prediction.features.get("volatility_adaptation").unwrap_or(&0.5);
                1.0 + (vol_handling * intensity * 0.4)
            },
            MarketRegime::Uncertain => 1.0,
        }
    }

    async fn calculate_correlation_penalty(&self, model_id: &str) -> Result<f64> {
        let correlations = self.correlation_matrix.read().await;
        
        let model_correlations: Vec<f64> = correlations.iter()
            .filter(|((m1, _), _)| m1 == model_id)
            .map(|(_, corr)| *corr)
            .collect();
        
        if model_correlations.is_empty() {
            return Ok(1.0);
        }
        
        let avg_correlation = model_correlations.iter().sum::<f64>() / model_correlations.len() as f64;
        let penalty = if avg_correlation > MAX_CORRELATION_THRESHOLD {
            0.5 + 0.5 * (1.0 - avg_correlation)
        } else {
            1.0
        };
        
        Ok(penalty)
    }

    fn normalize_weights(&self, weights: &mut HashMap<String, f64>) {
                let sum: f64 = weights.values().sum();
        if sum > 0.0 {
            for weight in weights.values_mut() {
                *weight /= sum;
            }
        }
    }

    async fn weighted_aggregate(
        &self,
        predictions: &[ModelPrediction],
        weights: &HashMap<String, f64>,
        regime: &MarketRegime,
    ) -> Result<AggregatedSignal> {
        let mut total_opportunity_score = 0.0;
        let mut total_confidence = 0.0;
        let mut total_profit = 0.0;
        let mut total_risk = 0.0;
        let mut total_urgency = 0.0;
        let mut total_impact = 0.0;
        
        let mut action_votes: HashMap<String, f64> = HashMap::new();
        let mut contributing_models = Vec::new();
        
        for pred in predictions {
            let weight = weights.get(&pred.model_id).copied().unwrap_or(0.0);
            if weight < MIN_MODEL_WEIGHT {
                continue;
            }
            
            contributing_models.push(pred.model_id.clone());
            
            total_opportunity_score += pred.opportunity_score * weight;
            total_confidence += pred.confidence * weight;
            total_profit += pred.predicted_profit * weight;
            total_risk += pred.risk_score * weight;
            total_urgency += pred.time_sensitivity * weight;
            total_impact += pred.market_impact * weight;
            
            let action_key = self.determine_action_type(pred);
            *action_votes.entry(action_key).or_insert(0.0) += weight * pred.confidence;
        }
        
        let dominant_action = action_votes.iter()
            .max_by(|(_, v1), (_, v2)| v1.partial_cmp(v2).unwrap())
            .map(|(k, _)| k.clone())
            .unwrap_or_else(|| "hold".to_string());
        
        let risk_adjusted_score = total_opportunity_score * (1.0 - total_risk * 0.3);
        let sharpe_like_ratio = if total_risk > 0.0 {
            (total_profit / total_risk).min(3.0)
        } else {
            total_profit.min(3.0)
        };
        
        let position_size_multiplier = self.calculate_position_size(
            total_confidence,
            total_risk,
            sharpe_like_ratio,
            regime,
        );
        
        let stop_loss_percentage = self.calculate_stop_loss(total_risk, regime);
        let take_profit_targets = self.calculate_take_profit_targets(
            total_profit,
            total_risk,
            regime,
        );
        
        let amount_lamports = self.calculate_trade_amount(
            total_opportunity_score,
            total_confidence,
            position_size_multiplier,
        );
        
        let action = self.construct_trading_action(
            &dominant_action,
            amount_lamports,
            predictions,
        );
        
        Ok(AggregatedSignal {
            action,
            confidence: total_confidence,
            expected_profit: total_profit,
            risk_adjusted_score,
            urgency_score: total_urgency,
            position_size_multiplier,
            stop_loss_percentage,
            take_profit_targets,
            contributing_models,
        })
    }

    fn determine_action_type(&self, pred: &ModelPrediction) -> String {
        if let Some(action_type) = pred.features.get("action_type") {
            match action_type {
                x if *x > 0.8 => "buy",
                x if *x < -0.8 => "sell",
                x if *x > 0.5 && pred.features.get("arbitrage_confidence").unwrap_or(&0.0) > &0.7 => "arbitrage_buy",
                x if *x < -0.5 && pred.features.get("arbitrage_confidence").unwrap_or(&0.0) > &0.7 => "arbitrage_sell",
                _ if pred.features.get("liquidation_probability").unwrap_or(&0.0) > &0.8 => "liquidation",
                _ => "hold",
            }.to_string()
        } else {
            "hold".to_string()
        }
    }

    fn calculate_position_size(
        &self,
        confidence: f64,
        risk: f64,
        sharpe_ratio: f64,
        regime: &MarketRegime,
    ) -> f64 {
        let base_size = confidence.powf(2.0) * (1.0 - risk).max(0.1);
        
        let regime_multiplier = match regime {
            MarketRegime::Trending { strength, .. } => 1.0 + strength * 0.5,
            MarketRegime::Ranging { .. } => 0.8,
            MarketRegime::Volatile { intensity } => 0.5 + 0.5 * (1.0 - intensity),
            MarketRegime::Uncertain => 0.7,
        };
        
        let sharpe_multiplier = (sharpe_ratio / 2.0).tanh() + 1.0;
        
        (base_size * regime_multiplier * sharpe_multiplier).min(2.0).max(0.1)
    }

    fn calculate_stop_loss(&self, risk: f64, regime: &MarketRegime) -> f64 {
        let base_stop_loss = 0.02 + risk * 0.03;
        
        match regime {
            MarketRegime::Volatile { intensity } => base_stop_loss * (1.0 + intensity * 0.5),
            MarketRegime::Trending { .. } => base_stop_loss * 0.8,
            MarketRegime::Ranging { .. } => base_stop_loss * 0.7,
            MarketRegime::Uncertain => base_stop_loss,
        }
    }

    fn calculate_take_profit_targets(
        &self,
        expected_profit: f64,
        risk: f64,
        regime: &MarketRegime,
    ) -> Vec<f64> {
        let risk_reward_ratio = 2.0 - risk;
        let base_target = expected_profit * risk_reward_ratio;
        
        match regime {
            MarketRegime::Trending { strength, .. } => {
                vec![
                    base_target * 0.5,
                    base_target * 1.0,
                    base_target * (1.0 + strength),
                ]
            },
            MarketRegime::Ranging { .. } => {
                vec![
                    base_target * 0.7,
                    base_target * 0.9,
                ]
            },
            MarketRegime::Volatile { .. } => {
                vec![
                    base_target * 0.6,
                    base_target * 0.8,
                    base_target * 1.0,
                ]
            },
            MarketRegime::Uncertain => {
                vec![
                    base_target * 0.7,
                    base_target * 1.0,
                ]
            },
        }
    }

    fn calculate_trade_amount(
        &self,
        opportunity_score: f64,
        confidence: f64,
        position_multiplier: f64,
    ) -> u64 {
        const BASE_AMOUNT: u64 = 1_000_000_000; // 1 SOL in lamports
        const MAX_AMOUNT: u64 = 100_000_000_000; // 100 SOL in lamports
        
        let score_multiplier = opportunity_score.powf(1.5);
        let confidence_multiplier = confidence.powf(2.0);
        
        let amount = (BASE_AMOUNT as f64 * score_multiplier * confidence_multiplier * position_multiplier) as u64;
        amount.min(MAX_AMOUNT)
    }

    fn construct_trading_action(
        &self,
        action_type: &str,
        amount_lamports: u64,
        predictions: &[ModelPrediction],
    ) -> TradingAction {
        match action_type {
            "buy" => TradingAction::Buy { amount_lamports },
            "sell" => TradingAction::Sell { amount_lamports },
            "arbitrage_buy" => {
                let path = self.extract_arbitrage_path(predictions);
                TradingAction::ArbitrageBuy { path, amount: amount_lamports }
            },
            "arbitrage_sell" => {
                let path = self.extract_arbitrage_path(predictions);
                TradingAction::ArbitrageSell { path, amount: amount_lamports }
            },
            "liquidation" => {
                let target = predictions.iter()
                    .find_map(|p| p.features.get("liquidation_target"))
                    .map(|v| format!("{:x}", v.to_bits()))
                    .unwrap_or_default();
                let collateral = (amount_lamports as f64 * 1.5) as u64;
                TradingAction::LiquidationOpportunity {
                    target_account: target,
                    collateral_value: collateral,
                }
            },
            _ => TradingAction::Hold,
        }
    }

    fn extract_arbitrage_path(&self, predictions: &[ModelPrediction]) -> Vec<String> {
        predictions.iter()
            .find_map(|p| {
                p.features.get("arbitrage_path_hash").map(|_| {
                    vec![
                        "So11111111111111111111111111111111111111112".to_string(),
                        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
                        "So11111111111111111111111111111111111111112".to_string(),
                    ]
                })
            })
            .unwrap_or_else(|| vec!["So11111111111111111111111111111111111111112".to_string()])
    }

    pub async fn update_model_performance(
        &self,
        model_id: &str,
        actual_profit: f64,
        predicted_profit: f64,
        risk_realized: f64,
    ) -> Result<()> {
        let mut performances = self.model_performances.write().await;
        
        if let Some(perf) = performances.get_mut(model_id) {
            let accuracy = 1.0 - (actual_profit - predicted_profit).abs() / predicted_profit.abs().max(1.0);
            perf.update_performance(accuracy, actual_profit, risk_realized);
            
            // Adaptive weight update
            let performance_score = perf.get_performance_score();
            let current_weight = perf.weight;
            let target_weight = performance_score / performances.len() as f64;
            
            perf.weight = current_weight * (1.0 - ADAPTIVE_LEARNING_RATE) + 
                         target_weight * ADAPTIVE_LEARNING_RATE;
            perf.weight = perf.weight.max(MIN_MODEL_WEIGHT);
        }
        
        // Renormalize weights
        let total_weight: f64 = performances.values().map(|p| p.weight).sum();
        if total_weight > 0.0 {
            for perf in performances.values_mut() {
                perf.weight /= total_weight;
            }
        }
        
        Ok(())
    }

    pub async fn get_model_diagnostics(&self) -> Result<HashMap<String, serde_json::Value>> {
        let performances = self.model_performances.read().await;
        let mut diagnostics = HashMap::new();
        
        for (model_id, perf) in performances.iter() {
            let model_stats = serde_json::json!({
                "weight": perf.weight,
                "accuracy_rate": perf.successful_predictions as f64 / perf.total_predictions.max(1) as f64,
                "cumulative_profit": perf.cumulative_profit,
                "sharpe_ratio": perf.sharpe_ratio,
                "max_drawdown": perf.max_drawdown,
                "performance_score": perf.get_performance_score(),
                "last_update_ms": perf.last_update.elapsed().as_millis(),
            });
            diagnostics.insert(model_id.clone(), model_stats);
        }
        
        Ok(diagnostics)
    }

    pub async fn prune_underperforming_models(&self, threshold: f64) -> Result<Vec<String>> {
        let mut performances = self.model_performances.write().await;
        let mut removed_models = Vec::new();
        
        performances.retain(|model_id, perf| {
            let keep = perf.get_performance_score() >= threshold || 
                      perf.total_predictions < PERFORMANCE_WINDOW as u64;
            if !keep {
                removed_models.push(model_id.clone());
            }
            keep
        });
        
        // Redistribute weights
        if !performances.is_empty() {
            let total_weight: f64 = performances.values().map(|p| p.weight).sum();
            if total_weight > 0.0 {
                for perf in performances.values_mut() {
                    perf.weight /= total_weight;
                }
            }
        }
        
        Ok(removed_models)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ensemble_aggregation() {
        let aggregator = EnsembleModelAggregator::new();
        
        let predictions = vec![
            ModelPrediction {
                model_id: "model1".to_string(),
                timestamp: 1000,
                opportunity_score: 0.8,
                confidence: 0.9,
                predicted_profit: 0.05,
                risk_score: 0.2,
                time_sensitivity: 0.7,
                market_impact: 0.1,
                features: HashMap::new(),
            },
        ];
        
        let result = aggregator.aggregate_predictions(predictions, 1000).await.unwrap();
        assert!(result.is_some());
    }
}

