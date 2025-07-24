use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use borsh::{BorshDeserialize, BorshSerialize};
use rayon::prelude::*;
use statrs::statistics::Statistics;

const PRICE_HISTORY_SIZE: usize = 1000;
const ORDERBOOK_DEPTH: usize = 50;
const PREDICTION_WINDOW_MS: u64 = 100;
const MIN_PRICE_CHANGE_BPS: f64 = 0.5;
const VOLUME_DECAY_FACTOR: f64 = 0.95;
const MOMENTUM_PERIODS: [usize; 5] = [5, 10, 20, 50, 100];
const CONFIDENCE_THRESHOLD: f64 = 0.75;
const MAX_POSITION_SIZE_BPS: f64 = 100.0;

#[derive(Debug, Clone, Copy)]
pub struct PricePoint {
    pub price: f64,
    pub volume: f64,
    pub timestamp_ms: u64,
    pub bid: f64,
    pub ask: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub trades_count: u32,
}

#[derive(Debug, Clone)]
pub struct OrderFlowMetrics {
    pub buy_pressure: f64,
    pub sell_pressure: f64,
    pub order_imbalance: f64,
    pub volume_ratio: f64,
    pub large_order_ratio: f64,
    pub trade_intensity: f64,
}

#[derive(Debug, Clone)]
pub struct MicrostructureFeatures {
    pub spread: f64,
    pub spread_volatility: f64,
    pub depth_imbalance: f64,
    pub price_impact: f64,
    pub realized_volatility: f64,
    pub tick_direction: i8,
    pub volume_clock: f64,
}

#[derive(Debug, Clone)]
pub struct TechnicalIndicators {
    pub ema_5: f64,
    pub ema_20: f64,
    pub rsi: f64,
    pub macd: f64,
    pub macd_signal: f64,
    pub bollinger_upper: f64,
    pub bollinger_lower: f64,
    pub vwap: f64,
    pub momentum_scores: Vec<f64>,
}

#[derive(Debug, Clone, Copy)]
pub enum PredictionSignal {
    StrongBuy(f64),
    Buy(f64),
    Neutral,
    Sell(f64),
    StrongSell(f64),
}

pub struct MicroPricePredictor {
    price_history: Arc<RwLock<VecDeque<PricePoint>>>,
    orderflow_cache: Arc<RwLock<VecDeque<OrderFlowMetrics>>>,
    prediction_cache: Arc<RwLock<Vec<PredictionResult>>>,
    feature_weights: Arc<RwLock<FeatureWeights>>,
    performance_tracker: Arc<RwLock<PerformanceMetrics>>,
}

#[derive(Debug, Clone)]
struct PredictionResult {
    pub signal: PredictionSignal,
    pub confidence: f64,
    pub expected_move_bps: f64,
    pub risk_score: f64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone)]
struct FeatureWeights {
    pub momentum_weight: f64,
    pub orderflow_weight: f64,
    pub microstructure_weight: f64,
    pub technical_weight: f64,
    pub adaptive_factor: f64,
}

#[derive(Debug, Clone)]
struct PerformanceMetrics {
    pub predictions_count: u64,
    pub correct_predictions: u64,
    pub total_pnl_bps: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
}

impl MicroPricePredictor {
    pub fn new() -> Self {
        Self {
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(PRICE_HISTORY_SIZE))),
            orderflow_cache: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
            prediction_cache: Arc::new(RwLock::new(Vec::with_capacity(1000))),
            feature_weights: Arc::new(RwLock::new(FeatureWeights {
                momentum_weight: 0.25,
                orderflow_weight: 0.35,
                microstructure_weight: 0.25,
                technical_weight: 0.15,
                adaptive_factor: 1.0,
            })),
            performance_tracker: Arc::new(RwLock::new(PerformanceMetrics {
                predictions_count: 0,
                correct_predictions: 0,
                total_pnl_bps: 0.0,
                sharpe_ratio: 0.0,
                max_drawdown: 0.0,
                win_rate: 0.0,
            })),
        }
    }

    pub fn update_price_data(&self, price_point: PricePoint) -> Result<(), Box<dyn std::error::Error>> {
        let mut history = self.price_history.write().unwrap();
        
        if history.len() >= PRICE_HISTORY_SIZE {
            history.pop_front();
        }
        
        history.push_back(price_point);
        
        if history.len() > 20 {
            let orderflow = self.calculate_orderflow_metrics(&history);
            let mut orderflow_cache = self.orderflow_cache.write().unwrap();
            if orderflow_cache.len() >= 100 {
                orderflow_cache.pop_front();
            }
            orderflow_cache.push_back(orderflow);
        }
        
        Ok(())
    }

    pub fn predict_movement(&self) -> Option<PredictionResult> {
        let history = self.price_history.read().unwrap();
        
        if history.len() < 100 {
            return None;
        }

        let current_price = history.back()?.price;
        let current_timestamp = history.back()?.timestamp_ms;

        let microstructure = self.calculate_microstructure_features(&history);
        let technical = self.calculate_technical_indicators(&history);
        let orderflow = self.orderflow_cache.read().unwrap().back()?.clone();
        let momentum = self.calculate_momentum_features(&history);

        let weights = self.feature_weights.read().unwrap().clone();
        
        let composite_score = self.calculate_composite_score(
            &microstructure,
            &technical,
            &orderflow,
            &momentum,
            &weights,
        );

        let (signal, confidence) = self.generate_signal(composite_score);
        let expected_move = self.calculate_expected_move(&history, composite_score);
        let risk_score = self.calculate_risk_score(&history, &microstructure);

        let result = PredictionResult {
            signal,
            confidence,
            expected_move_bps: expected_move,
            risk_score,
            timestamp_ms: current_timestamp,
        };

        let mut cache = self.prediction_cache.write().unwrap();
        cache.push(result.clone());
        
        self.adaptive_weight_update(&result);

        Some(result)
    }

    fn calculate_orderflow_metrics(&self, history: &VecDeque<PricePoint>) -> OrderFlowMetrics {
        let recent_points: Vec<_> = history.iter().rev().take(20).collect();
        
        let mut buy_volume = 0.0;
        let mut sell_volume = 0.0;
        let mut total_trades = 0u32;
        
        for (i, point) in recent_points.iter().enumerate() {
            let weight = (20.0 - i as f64) / 20.0;
            
            if i > 0 {
                let prev = recent_points[i - 1];
                if point.price > prev.price {
                    buy_volume += point.volume * weight;
                } else if point.price < prev.price {
                    sell_volume += point.volume * weight;
                }
            }
            
            total_trades += point.trades_count;
        }

        let total_volume = buy_volume + sell_volume;
        let buy_pressure = if total_volume > 0.0 { buy_volume / total_volume } else { 0.5 };
        let sell_pressure = 1.0 - buy_pressure;
        
        let bid_volume: f64 = recent_points.iter().map(|p| p.bid_volume).sum();
        let ask_volume: f64 = recent_points.iter().map(|p| p.ask_volume).sum();
        
        let order_imbalance = if bid_volume + ask_volume > 0.0 {
            (bid_volume - ask_volume) / (bid_volume + ask_volume)
        } else {
            0.0
        };

        let avg_volume = total_volume / recent_points.len() as f64;
        let large_trades = recent_points.iter()
            .filter(|p| p.volume > avg_volume * 2.0)
            .count() as f64;
        
        OrderFlowMetrics {
            buy_pressure,
            sell_pressure,
            order_imbalance,
            volume_ratio: buy_volume / (sell_volume + 1e-10),
            large_order_ratio: large_trades / recent_points.len() as f64,
            trade_intensity: total_trades as f64 / recent_points.len() as f64,
        }
    }

    fn calculate_microstructure_features(&self, history: &VecDeque<PricePoint>) -> MicrostructureFeatures {
        let recent: Vec<_> = history.iter().rev().take(50).collect();
        
        let spreads: Vec<f64> = recent.iter().map(|p| p.ask - p.bid).collect();
        let spread = spreads.last().copied().unwrap_or(0.0);
        let spread_volatility = spreads.std_dev();
        
        let latest = recent[0];
        let depth_imbalance = (latest.bid_volume - latest.ask_volume) / 
                             (latest.bid_volume + latest.ask_volume + 1e-10);
        
        let returns: Vec<f64> = recent.windows(2)
            .map(|w| (w[0].price / w[1].price).ln())
            .collect();
        
        let realized_volatility = returns.std_dev() * (1000.0_f64).sqrt();
        
        let price_changes: Vec<f64> = recent.windows(2)
            .map(|w| w[0].price - w[1].price)
            .collect();
        
        let price_impact = price_changes.iter()
            .zip(recent.windows(2).map(|w| w[0].volume))
            .map(|(change, vol)| change.abs() / (vol + 1e-10))
            .sum::<f64>() / price_changes.len() as f64;
        
        let tick_direction = if price_changes.last().copied().unwrap_or(0.0) > 0.0 { 1 }
                            else if price_changes.last().copied().unwrap_or(0.0) < 0.0 { -1 }
                            else { 0 };
        
        let volume_clock = recent.iter().map(|p| p.volume).sum::<f64>() / 
                          recent.len() as f64;
        
        MicrostructureFeatures {
            spread,
            spread_volatility,
            depth_imbalance,
            price_impact,
            realized_volatility,
            tick_direction,
            volume_clock,
        }
    }

    fn calculate_technical_indicators(&self, history: &VecDeque<PricePoint>) -> TechnicalIndicators {
        let prices: Vec<f64> = history.iter().map(|p| p.price).collect();
        let volumes: Vec<f64> = history.iter().map(|p| p.volume).collect();
        
        let ema_5 = self.calculate_ema(&prices, 5);
        let ema_20 = self.calculate_ema(&prices, 20);
        let ema_12 = self.calculate_ema(&prices, 12);
        let ema_26 = self.calculate_ema(&prices, 26);
        
        let macd = ema_12 - ema_26;
        let macd_signal = self.calculate_ema(&vec![macd; 9], 9);
        
        let rsi = self.calculate_rsi(&prices, 14);
        
        let typical_prices: Vec<f64> = history.iter()
            .map(|p| (p.price + p.bid + p.ask) / 3.0)
            .collect();
        
        let sma_20 = typical_prices.iter().rev().take(20).sum::<f64>() / 20.0;
        let std_20 = typical_prices.iter().rev().take(20)
            .map(|p| (p - sma_20).powi(2))
            .sum::<f64>() / 20.0;
        let std_20 = std_20.sqrt();
        
        let bollinger_upper = sma_20 + 2.0 * std_20;
        let bollinger_lower = sma_20 - 2.0 * std_20;
        
        let vwap = prices.iter().zip(volumes.iter())
            .map(|(p, v)| p * v)
            .sum::<f64>() / volumes.iter().sum::<f64>();
        
        let momentum_scores = MOMENTUM_PERIODS.iter()
            .map(|&period| {
                if prices.len() > period {
                    let current = prices.last().unwrap();
                    let past = prices[prices.len() - period - 1];
                    (current - past) / past * 100.0
                } else {
                    0.0
                }
            })
            .collect();
        
                TechnicalIndicators {
            ema_5,
            ema_20,
            rsi,
            macd,
            macd_signal,
            bollinger_upper,
            bollinger_lower,
            vwap,
            momentum_scores,
        }
    }

    fn calculate_momentum_features(&self, history: &VecDeque<PricePoint>) -> Vec<f64> {
        let recent: Vec<_> = history.iter().rev().take(100).collect();
        let mut features = Vec::with_capacity(10);
        
        // Price acceleration
        if recent.len() >= 3 {
            let p0 = recent[0].price;
            let p1 = recent[1].price;
            let p2 = recent[2].price;
            let acceleration = (p0 - p1) - (p1 - p2);
            features.push(acceleration / p0 * 10000.0);
        }
        
        // Volume-weighted momentum
        let vw_momentum = recent.windows(10)
            .map(|w| {
                let price_change = (w[0].price - w[9].price) / w[9].price;
                let avg_volume = w.iter().map(|p| p.volume).sum::<f64>() / 10.0;
                price_change * avg_volume.ln()
            })
            .next()
            .unwrap_or(0.0);
        features.push(vw_momentum);
        
        // Momentum divergence
        let price_momentum = if recent.len() > 20 {
            (recent[0].price - recent[20].price) / recent[20].price
        } else { 0.0 };
        
        let volume_momentum = if recent.len() > 20 {
            let recent_vol = recent.iter().take(10).map(|p| p.volume).sum::<f64>();
            let past_vol = recent.iter().skip(10).take(10).map(|p| p.volume).sum::<f64>();
            (recent_vol - past_vol) / (past_vol + 1e-10)
        } else { 0.0 };
        
        features.push(price_momentum - volume_momentum);
        
        // Tick rule momentum
        let upticks = recent.windows(2)
            .filter(|w| w[0].price > w[1].price)
            .count() as f64;
        let downticks = recent.windows(2)
            .filter(|w| w[0].price < w[1].price)
            .count() as f64;
        
        features.push((upticks - downticks) / (upticks + downticks + 1e-10));
        
        // Order flow momentum
        let order_momentum = recent.windows(5)
            .map(|w| {
                let bid_change = (w[0].bid_volume - w[4].bid_volume) / (w[4].bid_volume + 1e-10);
                let ask_change = (w[0].ask_volume - w[4].ask_volume) / (w[4].ask_volume + 1e-10);
                bid_change - ask_change
            })
            .next()
            .unwrap_or(0.0);
        features.push(order_momentum);
        
        features
    }

    fn calculate_composite_score(
        &self,
        microstructure: &MicrostructureFeatures,
        technical: &TechnicalIndicators,
        orderflow: &OrderFlowMetrics,
        momentum: &Vec<f64>,
        weights: &FeatureWeights,
    ) -> f64 {
        let micro_score = self.score_microstructure(microstructure);
        let tech_score = self.score_technical(technical);
        let flow_score = self.score_orderflow(orderflow);
        let mom_score = self.score_momentum(momentum);
        
        let weighted_score = micro_score * weights.microstructure_weight +
                            tech_score * weights.technical_weight +
                            flow_score * weights.orderflow_weight +
                            mom_score * weights.momentum_weight;
        
        weighted_score * weights.adaptive_factor
    }

    fn score_microstructure(&self, features: &MicrostructureFeatures) -> f64 {
        let mut score = 0.0;
        
        // Tighter spreads are bullish
        score += (0.001 - features.spread.min(0.001)) * 1000.0;
        
        // Lower spread volatility is better
        score -= features.spread_volatility * 50.0;
        
        // Positive depth imbalance is bullish
        score += features.depth_imbalance * 0.3;
        
        // Lower price impact is better
        score -= features.price_impact * 100.0;
        
        // Consider tick direction
        score += features.tick_direction as f64 * 0.1;
        
        // Normalize realized volatility impact
        let vol_factor = 1.0 / (1.0 + features.realized_volatility * 0.1);
        score *= vol_factor;
        
        score.max(-1.0).min(1.0)
    }

    fn score_technical(&self, indicators: &TechnicalIndicators) -> f64 {
        let mut score = 0.0;
        
        // EMA crossover
        if indicators.ema_5 > indicators.ema_20 {
            score += 0.2;
        } else {
            score -= 0.2;
        }
        
        // RSI signals
        if indicators.rsi < 30.0 {
            score += 0.3;
        } else if indicators.rsi > 70.0 {
            score -= 0.3;
        }
        
        // MACD signal
        if indicators.macd > indicators.macd_signal {
            score += 0.25;
        } else {
            score -= 0.25;
        }
        
        // Bollinger band position
        let current_price = indicators.vwap;
        let bb_position = (current_price - indicators.bollinger_lower) / 
                         (indicators.bollinger_upper - indicators.bollinger_lower + 1e-10);
        
        if bb_position < 0.2 {
            score += 0.2;
        } else if bb_position > 0.8 {
            score -= 0.2;
        }
        
        // Momentum alignment
        let positive_momentum = indicators.momentum_scores.iter()
            .filter(|&&m| m > 0.0)
            .count() as f64;
        let momentum_ratio = positive_momentum / indicators.momentum_scores.len() as f64;
        score += (momentum_ratio - 0.5) * 0.4;
        
        score.max(-1.0).min(1.0)
    }

    fn score_orderflow(&self, metrics: &OrderFlowMetrics) -> f64 {
        let mut score = 0.0;
        
        // Strong buy pressure
        score += (metrics.buy_pressure - 0.5) * 2.0;
        
        // Order imbalance
        score += metrics.order_imbalance * 0.5;
        
        // Volume ratio
        let log_ratio = metrics.volume_ratio.ln();
        score += log_ratio.max(-0.5).min(0.5);
        
        // Large order presence
        score += metrics.large_order_ratio * 0.3;
        
        // Trade intensity
        let normalized_intensity = (metrics.trade_intensity - 10.0) / 10.0;
        score += normalized_intensity.max(-0.2).min(0.2);
        
        score.max(-1.0).min(1.0)
    }

    fn score_momentum(&self, features: &Vec<f64>) -> f64 {
        if features.is_empty() {
            return 0.0;
        }
        
        let weights = vec![0.3, 0.25, 0.2, 0.15, 0.1];
        let mut score = 0.0;
        
        for (i, &feature) in features.iter().enumerate() {
            if i < weights.len() {
                score += feature * weights[i];
            }
        }
        
        score.max(-1.0).min(1.0)
    }

    fn generate_signal(&self, composite_score: f64) -> (PredictionSignal, f64) {
        let confidence = composite_score.abs();
        
        let signal = match composite_score {
            s if s > 0.5 => PredictionSignal::StrongBuy(s),
            s if s > 0.2 => PredictionSignal::Buy(s),
            s if s < -0.5 => PredictionSignal::StrongSell(s.abs()),
            s if s < -0.2 => PredictionSignal::Sell(s.abs()),
            _ => PredictionSignal::Neutral,
        };
        
        let adjusted_confidence = confidence * (1.0 / (1.0 + (-confidence * 5.0).exp()));
        
        (signal, adjusted_confidence.min(0.95))
    }

    fn calculate_expected_move(&self, history: &VecDeque<PricePoint>, score: f64) -> f64 {
        let recent_moves: Vec<f64> = history.iter()
            .rev()
            .take(50)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| ((w[0].price / w[1].price) - 1.0) * 10000.0)
            .collect();
        
        if recent_moves.is_empty() {
            return 0.0;
        }
        
        let avg_move = recent_moves.iter().map(|m| m.abs()).sum::<f64>() / recent_moves.len() as f64;
        let std_move = recent_moves.std_dev();
        
        let expected = avg_move * score.abs() + std_move * 0.5 * score.signum();
        
        expected.max(-MAX_POSITION_SIZE_BPS).min(MAX_POSITION_SIZE_BPS)
    }

    fn calculate_risk_score(&self, history: &VecDeque<PricePoint>, micro: &MicrostructureFeatures) -> f64 {
        let mut risk = 0.0;
        
        // Volatility risk
        risk += micro.realized_volatility.min(1.0) * 0.3;
        
        // Spread risk
        risk += (micro.spread / 0.001).min(1.0) * 0.2;
        
        // Volume risk
        let recent_volumes: Vec<f64> = history.iter()
            .rev()
            .take(20)
            .map(|p| p.volume)
            .collect();
        
        if !recent_volumes.is_empty() {
            let vol_std = recent_volumes.std_dev();
            let vol_mean = recent_volumes.mean();
            let vol_cv = vol_std / (vol_mean + 1e-10);
            risk += vol_cv.min(1.0) * 0.25;
        }
        
        // Price impact risk
        risk += (micro.price_impact * 1000.0).min(1.0) * 0.25;
        
        risk.min(1.0)
    }

    fn adaptive_weight_update(&self, result: &PredictionResult) {
        let mut tracker = self.performance_tracker.write().unwrap();
        tracker.predictions_count += 1;
        
        if tracker.predictions_count % 100 == 0 {
            let win_rate = tracker.correct_predictions as f64 / tracker.predictions_count as f64;
            let mut weights = self.feature_weights.write().unwrap();
            
            if win_rate < 0.45 {
                weights.adaptive_factor *= 0.95;
            } else if win_rate > 0.55 {
                weights.adaptive_factor *= 1.02;
            }
            
            weights.adaptive_factor = weights.adaptive_factor.max(0.5).min(1.5);
            
            // Rebalance weights based on recent performance
            if tracker.sharpe_ratio < 1.0 {
                weights.orderflow_weight = (weights.orderflow_weight * 1.05).min(0.5);
                weights.momentum_weight = (weights.momentum_weight * 0.95).max(0.1);
            }
        }
    }

    fn calculate_ema(&self, values: &[f64], period: usize) -> f64 {
        if values.len() < period {
            return values.last().copied().unwrap_or(0.0);
        }
        
        let alpha = 2.0 / (period as f64 + 1.0);
        let mut ema = values[values.len() - period];
        
        for i in (values.len() - period + 1)..values.len() {
            ema = values[i] * alpha + ema * (1.0 - alpha);
        }
        
        ema
    }

    fn calculate_rsi(&self, prices: &[f64], period: usize) -> f64 {
        if prices.len() < period + 1 {
            return 50.0;
        }
        
        let mut gains = 0.0;
        let mut losses = 0.0;
        
        let start = prices.len().saturating_sub(period + 1);
        for i in start..prices.len() - 1 {
            let change = prices[i + 1] - prices[i];
            if change > 0.0 {
                gains += change;
            } else {
                losses -= change;
            }
        }
        
        let avg_gain = gains / period as f64;
        let avg_loss = losses / period as f64;
        
        if avg_loss == 0.0 {
            return 100.0;
        }
        
        let rs = avg_gain / avg_loss;
        100.0 - (100.0 / (1.0 + rs))
    }

    pub fn get_position_size(&self, signal: &PredictionSignal, confidence: f64, risk_score: f64) -> f64 {
        let base_size = match signal {
            PredictionSignal::StrongBuy(s) | PredictionSignal::StrongSell(s) => s * 0.8,
            PredictionSignal::Buy(s) | PredictionSignal::Sell(s) => s * 0.5,
            PredictionSignal::Neutral => 0.0,
        };
        
        let risk_adjusted = base_size * (1.0 - risk_score * 0.5);
        let confidence_adjusted = risk_adjusted * confidence;
        
        confidence_adjusted.max(-1.0).min(1.0)
    }

        pub fn should_execute(&self, prediction: &PredictionResult) -> bool {
        if prediction.confidence < CONFIDENCE_THRESHOLD {
            return false;
        }
        
        if prediction.risk_score > 0.7 {
            return false;
        }
        
        if prediction.expected_move_bps.abs() < MIN_PRICE_CHANGE_BPS {
            return false;
        }
        
        // Check recent performance
        let tracker = self.performance_tracker.read().unwrap();
        if tracker.predictions_count > 100 && tracker.win_rate < 0.4 {
            return prediction.confidence > 0.85;
        }
        
        true
    }

    pub fn update_performance(&self, prediction: &PredictionResult, actual_move_bps: f64) {
        let mut tracker = self.performance_tracker.write().unwrap();
        
        let predicted_direction = match prediction.signal {
            PredictionSignal::StrongBuy(_) | PredictionSignal::Buy(_) => 1.0,
            PredictionSignal::StrongSell(_) | PredictionSignal::Sell(_) => -1.0,
            PredictionSignal::Neutral => 0.0,
        };
        
        let actual_direction = if actual_move_bps > 0.0 { 1.0 } else if actual_move_bps < 0.0 { -1.0 } else { 0.0 };
        
        if predicted_direction * actual_direction > 0.0 {
            tracker.correct_predictions += 1;
        }
        
        tracker.total_pnl_bps += prediction.expected_move_bps.signum() * actual_move_bps;
        tracker.win_rate = tracker.correct_predictions as f64 / tracker.predictions_count as f64;
        
        // Update Sharpe ratio
        self.update_sharpe_ratio(&mut tracker, actual_move_bps);
        
        // Update max drawdown
        self.update_max_drawdown(&mut tracker);
    }

    fn update_sharpe_ratio(&self, tracker: &mut PerformanceMetrics, return_bps: f64) {
        let cache = self.prediction_cache.read().unwrap();
        if cache.len() < 2 {
            return;
        }
        
        let returns: Vec<f64> = cache.windows(2)
            .map(|w| {
                let time_diff = (w[1].timestamp_ms - w[0].timestamp_ms) as f64 / 1000.0;
                return_bps / time_diff.max(0.1)
            })
            .collect();
        
        if returns.len() > 20 {
            let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
            let std_return = returns.std_dev();
            
            if std_return > 0.0 {
                tracker.sharpe_ratio = (mean_return * 252.0_f64.sqrt()) / std_return;
            }
        }
    }

    fn update_max_drawdown(&self, tracker: &mut PerformanceMetrics) {
        let equity_curve = self.calculate_equity_curve();
        if equity_curve.len() < 2 {
            return;
        }
        
        let mut peak = equity_curve[0];
        let mut max_dd = 0.0;
        
        for &equity in &equity_curve[1..] {
            if equity > peak {
                peak = equity;
            } else {
                let drawdown = (peak - equity) / peak;
                max_dd = max_dd.max(drawdown);
            }
        }
        
        tracker.max_drawdown = max_dd;
    }

    fn calculate_equity_curve(&self) -> Vec<f64> {
        let cache = self.prediction_cache.read().unwrap();
        let mut equity = 100.0;
        let mut curve = vec![equity];
        
        for prediction in cache.iter() {
            let position_size = self.get_position_size(&prediction.signal, prediction.confidence, prediction.risk_score);
            let pnl = position_size * prediction.expected_move_bps / 100.0;
            equity *= 1.0 + pnl;
            curve.push(equity);
        }
        
        curve
    }

    pub fn get_feature_importance(&self) -> Vec<(&'static str, f64)> {
        let weights = self.feature_weights.read().unwrap();
        vec![
            ("orderflow", weights.orderflow_weight),
            ("microstructure", weights.microstructure_weight),
            ("momentum", weights.momentum_weight),
            ("technical", weights.technical_weight),
        ]
    }

    pub fn reset_adaptive_weights(&self) {
        let mut weights = self.feature_weights.write().unwrap();
        weights.momentum_weight = 0.25;
        weights.orderflow_weight = 0.35;
        weights.microstructure_weight = 0.25;
        weights.technical_weight = 0.15;
        weights.adaptive_factor = 1.0;
    }

    pub fn get_latest_prediction(&self) -> Option<PredictionResult> {
        self.prediction_cache.read().unwrap().last().cloned()
    }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        self.performance_tracker.read().unwrap().clone()
    }

    pub fn clear_old_predictions(&self, keep_last: usize) {
        let mut cache = self.prediction_cache.write().unwrap();
        if cache.len() > keep_last {
            let drain_count = cache.len() - keep_last;
            cache.drain(0..drain_count);
        }
    }
}

impl Default for MicroPricePredictor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predictor_initialization() {
        let predictor = MicroPricePredictor::new();
        assert!(predictor.get_latest_prediction().is_none());
    }

    #[test]
    fn test_price_update() {
        let predictor = MicroPricePredictor::new();
        let price_point = PricePoint {
            price: 100.0,
            volume: 1000.0,
            timestamp_ms: 1000,
            bid: 99.95,
            ask: 100.05,
            bid_volume: 500.0,
            ask_volume: 500.0,
            trades_count: 10,
        };
        
        assert!(predictor.update_price_data(price_point).is_ok());
    }

    #[test]
    fn test_prediction_generation() {
        let predictor = MicroPricePredictor::new();
        
        // Add enough data points
        for i in 0..150 {
            let price = 100.0 + (i as f64 * 0.01).sin();
            let price_point = PricePoint {
                price,
                volume: 1000.0 + (i as f64 * 10.0),
                timestamp_ms: 1000 + i * 100,
                bid: price - 0.05,
                ask: price + 0.05,
                bid_volume: 500.0,
                ask_volume: 500.0,
                trades_count: 10,
            };
            predictor.update_price_data(price_point).unwrap();
        }
        
        let prediction = predictor.predict_movement();
        assert!(prediction.is_some());
    }
}


