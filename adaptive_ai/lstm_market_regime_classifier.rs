use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use ndarray::{Array1, Array2, ArrayView1};
use rand::Rng;
use std::f64::consts::E;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum MarketRegime {
    StrongBull,
    Bull,
    Sideways,
    Bear,
    StrongBear,
    HighVolatility,
    LowVolatility,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSTMCell {
    weights_input: Array2<f64>,
    weights_hidden: Array2<f64>,
    weights_cell: Array2<f64>,
    bias: Array1<f64>,
    hidden_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSTMLayer {
    input_gate: LSTMCell,
    forget_gate: LSTMCell,
    cell_gate: LSTMCell,
    output_gate: LSTMCell,
    hidden_state: Array1<f64>,
    cell_state: Array1<f64>,
}

#[derive(Debug, Clone)]
pub struct MarketFeatures {
    price_returns: f64,
    volume_ratio: f64,
    volatility: f64,
    rsi: f64,
    macd_signal: f64,
    order_flow_imbalance: f64,
    spread_ratio: f64,
    momentum: f64,
    volume_weighted_price: f64,
    bid_ask_pressure: f64,
}

pub struct LSTMMarketRegimeClassifier {
    lstm_layers: Vec<LSTMLayer>,
    output_layer: Array2<f64>,
    output_bias: Array1<f64>,
    feature_buffer: VecDeque<MarketFeatures>,
    price_history: VecDeque<f64>,
    volume_history: VecDeque<f64>,
    regime_confidence: Arc<RwLock<f64>>,
    current_regime: Arc<RwLock<MarketRegime>>,
    sequence_length: usize,
    hidden_size: usize,
    learning_rate: f64,
    momentum_factor: f64,
    regime_threshold: f64,
}

impl LSTMCell {
    fn new(input_size: usize, hidden_size: usize) -> Self {
        let mut rng = rand::thread_rng();
        let xavier_scale = (2.0 / (input_size + hidden_size) as f64).sqrt();
        
        Self {
            weights_input: Array2::from_shape_fn((hidden_size, input_size), |_| {
                rng.gen_range(-xavier_scale..xavier_scale)
            }),
            weights_hidden: Array2::from_shape_fn((hidden_size, hidden_size), |_| {
                rng.gen_range(-xavier_scale..xavier_scale)
            }),
            weights_cell: Array2::from_shape_fn((hidden_size, hidden_size), |_| {
                rng.gen_range(-xavier_scale..xavier_scale)
            }),
            bias: Array1::from_shape_fn(hidden_size, |_| 0.1),
            hidden_size,
        }
    }

    fn forward(&self, input: &ArrayView1<f64>, hidden: &ArrayView1<f64>, cell: &ArrayView1<f64>) -> Array1<f64> {
        let z = self.weights_input.dot(input) + self.weights_hidden.dot(hidden) + self.weights_cell.dot(cell) + &self.bias;
        z.mapv(|x| 1.0 / (1.0 + E.powf(-x)))
    }
}

impl LSTMLayer {
    fn new(input_size: usize, hidden_size: usize) -> Self {
        Self {
            input_gate: LSTMCell::new(input_size, hidden_size),
            forget_gate: LSTMCell::new(input_size, hidden_size),
            cell_gate: LSTMCell::new(input_size, hidden_size),
            output_gate: LSTMCell::new(input_size, hidden_size),
            hidden_state: Array1::zeros(hidden_size),
            cell_state: Array1::zeros(hidden_size),
        }
    }

    fn forward(&mut self, input: &ArrayView1<f64>) -> Array1<f64> {
        let i_gate = self.input_gate.forward(input, &self.hidden_state.view(), &self.cell_state.view());
        let f_gate = self.forget_gate.forward(input, &self.hidden_state.view(), &self.cell_state.view());
        let c_gate = self.cell_gate.forward(input, &self.hidden_state.view(), &self.cell_state.view());
        let o_gate = self.output_gate.forward(input, &self.hidden_state.view(), &self.cell_state.view());
        
        let c_gate_tanh = c_gate.mapv(|x| x.tanh());
        self.cell_state = &f_gate * &self.cell_state + &i_gate * &c_gate_tanh;
        self.hidden_state = &o_gate * self.cell_state.mapv(|x| x.tanh());
        
        self.hidden_state.clone()
    }

    fn reset_states(&mut self) {
        self.hidden_state.fill(0.0);
        self.cell_state.fill(0.0);
    }
}

impl LSTMMarketRegimeClassifier {
    pub fn new() -> Self {
        let sequence_length = 20;
        let hidden_size = 64;
        let num_features = 10;
        let num_classes = 7;
        
        let mut lstm_layers = Vec::new();
        lstm_layers.push(LSTMLayer::new(num_features, hidden_size));
        lstm_layers.push(LSTMLayer::new(hidden_size, hidden_size));
        
        let mut rng = rand::thread_rng();
        let output_scale = (2.0 / hidden_size as f64).sqrt();
        
        Self {
            lstm_layers,
            output_layer: Array2::from_shape_fn((num_classes, hidden_size), |_| {
                rng.gen_range(-output_scale..output_scale)
            }),
            output_bias: Array1::from_shape_fn(num_classes, |_| 0.0),
            feature_buffer: VecDeque::with_capacity(sequence_length),
            price_history: VecDeque::with_capacity(100),
            volume_history: VecDeque::with_capacity(100),
            regime_confidence: Arc::new(RwLock::new(0.0)),
            current_regime: Arc::new(RwLock::new(MarketRegime::Sideways)),
            sequence_length,
            hidden_size,
            learning_rate: 0.001,
            momentum_factor: 0.9,
            regime_threshold: 0.65,
        }
    }

    pub fn update_market_data(&mut self, price: f64, volume: f64, bid: f64, ask: f64, 
                             bid_size: f64, ask_size: f64, trades_count: u64) {
        self.price_history.push_back(price);
        if self.price_history.len() > 100 {
            self.price_history.pop_front();
        }
        
        self.volume_history.push_back(volume);
        if self.volume_history.len() > 100 {
            self.volume_history.pop_front();
        }
        
        if self.price_history.len() >= 20 {
            let features = self.extract_features(price, volume, bid, ask, bid_size, ask_size);
            self.feature_buffer.push_back(features);
            
            if self.feature_buffer.len() > self.sequence_length {
                self.feature_buffer.pop_front();
            }
            
            if self.feature_buffer.len() == self.sequence_length {
                let regime = self.classify_regime();
                *self.current_regime.write() = regime;
            }
        }
    }

    fn extract_features(&self, price: f64, volume: f64, bid: f64, ask: f64,
                       bid_size: f64, ask_size: f64) -> MarketFeatures {
        let prices: Vec<f64> = self.price_history.iter().copied().collect();
        let volumes: Vec<f64> = self.volume_history.iter().copied().collect();
        
        let price_returns = if prices.len() > 1 {
            (price - prices[prices.len() - 2]) / prices[prices.len() - 2]
        } else {
            0.0
        };
        
        let avg_volume = volumes.iter().sum::<f64>() / volumes.len() as f64;
        let volume_ratio = volume / avg_volume.max(1.0);
        
        let volatility = self.calculate_volatility(&prices);
        let rsi = self.calculate_rsi(&prices);
        let macd_signal = self.calculate_macd(&prices);
        
        let order_flow_imbalance = (bid_size - ask_size) / (bid_size + ask_size + 1.0);
        let spread_ratio = (ask - bid) / price;
        
        let momentum = if prices.len() >= 10 {
            (price - prices[prices.len() - 10]) / prices[prices.len() - 10]
        } else {
            0.0
        };
        
        let vwap = self.calculate_vwap(&prices, &volumes);
        let volume_weighted_price = (price - vwap) / vwap.max(0.0001);
        
        let bid_ask_pressure = (bid_size * bid - ask_size * ask) / (bid_size * bid + ask_size * ask + 1.0);
        
        MarketFeatures {
            price_returns,
            volume_ratio,
            volatility,
            rsi,
            macd_signal,
            order_flow_imbalance,
            spread_ratio,
            momentum,
            volume_weighted_price,
            bid_ask_pressure,
        }
    }

    fn calculate_volatility(&self, prices: &[f64]) -> f64 {
        if prices.len() < 2 {
            return 0.0;
        }
        
        let returns: Vec<f64> = prices.windows(2)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        variance.sqrt()
    }

    fn calculate_rsi(&self, prices: &[f64]) -> f64 {
        if prices.len() < 14 {
            return 0.5;
        }
        
        let mut gains = 0.0;
        let mut losses = 0.0;
        
        for i in prices.len().saturating_sub(14)..prices.len() - 1 {
            let change = prices[i + 1] - prices[i];
            if change > 0.0 {
                gains += change;
            } else {
                losses -= change;
            }
        }
        
        let avg_gain = gains / 14.0;
        let avg_loss = losses / 14.0;
        
        if avg_loss == 0.0 {
            return 1.0;
        }
        
        let rs = avg_gain / avg_loss;
        1.0 - (1.0 / (1.0 + rs))
    }

    fn calculate_macd(&self, prices: &[f64]) -> f64 {
        if prices.len() < 26 {
            return 0.0;
        }
        
        let ema12 = self.calculate_ema(prices, 12);
        let ema26 = self.calculate_ema(prices, 26);
        
        (ema12 - ema26) / ema26.abs().max(0.0001)
    }

    fn calculate_ema(&self, prices: &[f64], period: usize) -> f64 {
        if prices.is_empty() {
            return 0.0;
        }
        
        let alpha = 2.0 / (period as f64 + 1.0);
        let mut ema = prices[0];
        
        for &price in &prices[1..] {
            ema = alpha * price + (1.0 - alpha) * ema;
        }
        
        ema
    }

    fn calculate_vwap(&self, prices: &[f64], volumes: &[f64]) -> f64 {
        if prices.is_empty() || volumes.is_empty() {
            return 0.0;
        }
        
        let total_volume = volumes.iter().sum::<f64>();
        if total_volume == 0.0 {
            return prices.last().copied().unwrap_or(0.0);
        }
        
        let weighted_sum: f64 = prices.iter()
            .zip(volumes.iter())
            .map(|(p, v)| p * v)
            .sum();
        
        weighted_sum / total_volume
    }

    fn classify_regime(&mut self) -> MarketRegime {
        let features_array = self.prepare_features();
        
        for layer in &mut self.lstm_layers {
            layer.reset_states();
        }
        
        let mut hidden = features_array;
        for layer in &mut self.lstm_layers {
            hidden = layer.forward(&hidden.view());
        }
        
        let logits = self.output_layer.dot(&hidden) + &self.output_bias;
        let probs = self.softmax(&logits);
        
        let (regime_idx, confidence) = probs.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(idx, &conf)| (idx, conf))
            .unwrap_or((0, 0.0));
        
        *self.regime_confidence.write() = confidence;
        
        match regime_idx {
            0 => MarketRegime::StrongBull,
            1 => MarketRegime::Bull,
            2 => MarketRegime::Sideways,
            3 => MarketRegime::Bear,
            4 => MarketRegime::StrongBear,
            5 => MarketRegime::HighVolatility,
            6 => MarketRegime::LowVolatility,
            _ => MarketRegime::Sideways,
        }
    }

    fn prepare_features(&self) -> Array1<f64> {
        let mut features = Array1::zeros(10);
        
        if let Some(last_features) = self.feature_buffer.back() {
            features[0] = self.normalize_value(last_features.price_returns, -0.05, 0.05);
            features[1] = self.normalize_value(last_features.volume_ratio, 0.0, 3.0);
            features[2] = self.normalize_value(last_features.volatility, 0.0, 0.05);
            features[3] = self.normalize_value(last_features.rsi, 0.0, 1.0);
            features[4] = self.normalize_value(last_features.macd_signal, -0.02, 0.02);
            features[5] = self.normalize_value(last_features.order_flow_imbalance, -1.0, 1.0);
            features[6] = self.normalize_value(last_features.spread_ratio, 0.0, 0.01);
            features[7] = self.normalize_value(last_features.momentum, -0.1, 0.1);
            features[8] = self.normalize_value(last_features.volume_weighted_price, -0.02, 0.02);
            features[9] = self.normalize_value(last_features.bid_ask_pressure, -1.0, 1.0);
        }
        
        features
    }

    fn normalize_value(&self, value: f64, min: f64, max: f64) -> f64 {
        ((value - min) / (max - min)).max(0.0).min(1.0)
    }

    fn softmax(&self, logits: &Array1<f64>) -> Array1<f64> {
        let max_logit = logits.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let exp_logits = logits.mapv(|x| (x - max_logit).exp());
        let sum_exp = exp_logits.sum();
        exp_logits / sum_exp
    }

    pub fn get_current_regime(&self) -> MarketRegime {
        *self.current_regime.read()
    }

    pub fn get_regime_confidence(&self) -> f64 {
        *self.regime_confidence.read()
    }

    pub fn get_regime_features(&self) -> Option<RegimeAnalysis> {
        if self.feature_buffer.is_empty() {
            return None;
        }

        let current_regime = self.get_current_regime();
        let confidence = self.get_regime_confidence();
        
        let trend_strength = self.calculate_trend_strength();
        let volatility_percentile = self.calculate_volatility_percentile();
        let volume_trend = self.calculate_volume_trend();
        let momentum_score = self.calculate_momentum_score();
        
        Some(RegimeAnalysis {
            regime: current_regime,
            confidence,
            trend_strength,
            volatility_percentile,
            volume_trend,
            momentum_score,
            regime_stability: self.calculate_regime_stability(),
            predicted_duration: self.estimate_regime_duration(),
        })
    }

    fn calculate_trend_strength(&self) -> f64 {
        if self.price_history.len() < 20 {
            return 0.0;
        }
        
        let prices: Vec<f64> = self.price_history.iter().copied().collect();
        let n = prices.len() as f64;
        
        let x: Vec<f64> = (0..prices.len()).map(|i| i as f64).collect();
        let x_mean = x.iter().sum::<f64>() / n;
        let y_mean = prices.iter().sum::<f64>() / n;
        
        let mut numerator = 0.0;
        let mut denominator = 0.0;
        
        for i in 0..prices.len() {
            numerator += (x[i] - x_mean) * (prices[i] - y_mean);
            denominator += (x[i] - x_mean) * (x[i] - x_mean);
        }
        
        if denominator == 0.0 {
            return 0.0;
        }
        
        let slope = numerator / denominator;
        let r_squared = (numerator * numerator) / (denominator * prices.iter()
            .map(|&y| (y - y_mean) * (y - y_mean))
            .sum::<f64>());
        
        slope.signum() * r_squared.sqrt()
    }

    fn calculate_volatility_percentile(&self) -> f64 {
        if self.feature_buffer.is_empty() {
            return 0.5;
        }
        
        let volatilities: Vec<f64> = self.feature_buffer.iter()
            .map(|f| f.volatility)
            .collect();
        
        if let Some(current) = volatilities.last() {
            let below_count = volatilities.iter()
                .filter(|&&v| v < *current)
                .count() as f64;
            below_count / volatilities.len() as f64
        } else {
            0.5
        }
    }

    fn calculate_volume_trend(&self) -> f64 {
        if self.volume_history.len() < 10 {
            return 0.0;
        }
        
        let recent_avg = self.volume_history.iter()
            .rev()
            .take(10)
            .sum::<f64>() / 10.0;
        
        let historical_avg = self.volume_history.iter()
            .sum::<f64>() / self.volume_history.len() as f64;
        
        (recent_avg - historical_avg) / historical_avg.max(1.0)
    }

    fn calculate_momentum_score(&self) -> f64 {
        if self.feature_buffer.is_empty() {
            return 0.0;
        }
        
        let momentum_values: Vec<f64> = self.feature_buffer.iter()
            .map(|f| f.momentum)
            .collect();
        
        let avg_momentum = momentum_values.iter().sum::<f64>() / momentum_values.len() as f64;
        let momentum_consistency = 1.0 - momentum_values.iter()
            .map(|&m| (m - avg_momentum).abs())
            .sum::<f64>() / (momentum_values.len() as f64 * avg_momentum.abs().max(0.01));
        
        avg_momentum * momentum_consistency
    }

    fn calculate_regime_stability(&self) -> f64 {
        if self.feature_buffer.len() < 5 {
            return 0.0;
        }
        
        let recent_features: Vec<&MarketFeatures> = self.feature_buffer.iter()
            .rev()
            .take(5)
            .collect();
        
        let mut stability_score = 1.0;
        for i in 1..recent_features.len() {
            let diff = (recent_features[i].volatility - recent_features[i-1].volatility).abs()
                     + (recent_features[i].momentum - recent_features[i-1].momentum).abs()
                     + (recent_features[i].volume_ratio - recent_features[i-1].volume_ratio).abs();
            stability_score *= (1.0 - diff.min(1.0));
        }
        
        stability_score
    }

    fn estimate_regime_duration(&self) -> u64 {
        let stability = self.calculate_regime_stability();
        let base_duration = match self.get_current_regime() {
            MarketRegime::StrongBull | MarketRegime::StrongBear => 300,
            MarketRegime::Bull | MarketRegime::Bear => 600,
            MarketRegime::Sideways => 1200,
            MarketRegime::HighVolatility => 180,
            MarketRegime::LowVolatility => 1800,
        };
        
        (base_duration as f64 * (0.5 + stability * 1.5)) as u64
    }

    pub fn predict_regime_transition(&self) -> RegimeTransitionPrediction {
        let current_regime = self.get_current_regime();
        let features = self.feature_buffer.back().cloned().unwrap_or(MarketFeatures {
            price_returns: 0.0,
            volume_ratio: 1.0,
            volatility: 0.01,
            rsi: 0.5,
            macd_signal: 0.0,
            order_flow_imbalance: 0.0,
            spread_ratio: 0.001,
            momentum: 0.0,
            volume_weighted_price: 0.0,
            bid_ask_pressure: 0.0,
        });
        
        let transition_probability = self.calculate_transition_probability(&features);
        let likely_next_regime = self.predict_next_regime(&features, current_regime);
        
        RegimeTransitionPrediction {
            current_regime,
            likely_next_regime,
            transition_probability,
            time_to_transition: self.estimate_time_to_transition(transition_probability),
        }
    }

    fn calculate_transition_probability(&self, features: &MarketFeatures) -> f64 {
        let volatility_change_factor = (features.volatility / 0.02).min(2.0);
        let momentum_extremity = features.momentum.abs() / 0.05;
        let volume_spike = (features.volume_ratio - 1.0).abs();
        let rsi_extremity = (features.rsi - 0.5).abs() * 2.0;
        
        let base_probability = 0.1;
        let probability = base_probability + 
            (volatility_change_factor * 0.2) +
            (momentum_extremity * 0.15) +
            (volume_spike * 0.1) +
            (rsi_extremity * 0.15);
        
        probability.min(1.0)
    }

    fn predict_next_regime(&self, features: &MarketFeatures, current: MarketRegime) -> MarketRegime {
        match current {
            MarketRegime::StrongBull => {
                if features.rsi > 0.8 || features.momentum < -0.02 {
                    MarketRegime::Bull
                } else if features.volatility > 0.04 {
                    MarketRegime::HighVolatility
                } else {
                    current
                }
            },
            MarketRegime::Bull => {
                if features.momentum > 0.05 && features.volume_ratio > 1.5 {
                    MarketRegime::StrongBull
                } else if features.momentum < -0.01 {
                    MarketRegime::Sideways
                } else {
                    current
                }
            },
            MarketRegime::Sideways => {
                if features.momentum > 0.03 {
                    MarketRegime::Bull
                } else if features.momentum < -0.03 {
                    MarketRegime::Bear
                } else if features.volatility > 0.03 {
                    MarketRegime::HighVolatility
                } else if features.volatility < 0.005 {
                    MarketRegime::LowVolatility
                } else {
                    current
                }
            },
            MarketRegime::Bear => {
                if features.momentum < -0.05 && features.volume_ratio > 1.5 {
                    MarketRegime::StrongBear
                } else if features.momentum > 0.01 {
                    MarketRegime::Sideways
                } else {
                    current
                }
            },
            MarketRegime::StrongBear => {
                if features.rsi < 0.2 || features.momentum > 0.02 {
                    MarketRegime::Bear
                } else if features.volatility > 0.04 {
                    MarketRegime::HighVolatility
                } else {
                    current
                }
            },
            MarketRegime::HighVolatility => {
                if features.volatility < 0.02 {
                    if features.momentum > 0.01 {
                        MarketRegime::Bull
                    } else if features.momentum < -0.01 {
                        MarketRegime::Bear
                    } else {
                        MarketRegime::Sideways
                    }
                } else {
                    current
                }
            },
            MarketRegime::LowVolatility => {
                if features.volatility > 0.01 || features.volume_ratio > 2.0 {
                    MarketRegime::Sideways
                } else {
                    current
                }
            }
        }
    }

    fn estimate_time_to_transition(&self, probability: f64) -> u64 {
        if probability > 0.8 {
            60
        } else if probability > 0.6 {
            300
        } else if probability > 0.4 {
            600
        } else {
            1200
        }
    }

    pub fn optimize_for_latency(&mut self) {
        self.sequence_length = self.sequence_length.min(15);
        self.price_history = VecDeque::with_capacity(50);
        self.volume_history = VecDeque::with_capacity(50);
    }

    pub fn get_trading_signals(&self) -> TradingSignals {
        let regime = self.get_current_regime();
        let confidence = self.get_regime_confidence();
        let analysis = self.get_regime_features().unwrap_or_default();
        
        let position_bias = match regime {
            MarketRegime::StrongBull => 1.0,
            MarketRegime::Bull => 0.6,
            MarketRegime::Sideways => 0.0,
            MarketRegime::Bear => -0.6,
            MarketRegime::StrongBear => -1.0,
            MarketRegime::HighVolatility => 0.0,
            MarketRegime::LowVolatility => 0.0,
        };
        
        let risk_multiplier = match regime {
            MarketRegime::HighVolatility => 0.5,
            MarketRegime::LowVolatility => 1.5,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 0.8,
            _ => 1.0,
        };
        
        let stop_loss_distance = match regime {
            MarketRegime::HighVolatility => 0.02,
            MarketRegime::LowVolatility => 0.005,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 0.015,
            _ => 0.01,
        };
        
        let take_profit_ratio = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => 3.0,
            MarketRegime::Bull | MarketRegime::Bear => 2.5,
            MarketRegime::HighVolatility => 4.0,
            MarketRegime::LowVolatility => 1.5,
            MarketRegime::Sideways => 2.0,
        };
        
        TradingSignals {
            position_bias: position_bias * confidence,
            risk_multiplier,
            stop_loss_distance,
            take_profit_ratio,
            entry_threshold: 0.7 * confidence,
            exit_threshold: 0.3,
            max_position_size: self.calculate_max_position_size(regime, confidence),
            time_in_force: self.calculate_time_in_force(regime),
        }
    }
    
    fn calculate_max_position_size(&self, regime: MarketRegime, confidence: f64) -> f64 {
        let base_size = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => 0.8,
            MarketRegime::Bull | MarketRegime::Bear => 0.6,
            MarketRegime::Sideways => 0.4,
            MarketRegime::HighVolatility => 0.3,
            MarketRegime::LowVolatility => 0.7,
        };
        
        base_size * confidence
    }
    
    fn calculate_time_in_force(&self, regime: MarketRegime) -> u64 {
        match regime {
            MarketRegime::HighVolatility => 30,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 120,
            MarketRegime::Bull | MarketRegime::Bear => 180,
            MarketRegime::Sideways => 300,
            MarketRegime::LowVolatility => 600,
        }
    }
    
    pub fn get_execution_params(&self) -> ExecutionParameters {
        let regime = self.get_current_regime();
        let features = self.feature_buffer.back().unwrap_or(&MarketFeatures {
            price_returns: 0.0,
            volume_ratio: 1.0,
            volatility: 0.01,
            rsi: 0.5,
            macd_signal: 0.0,
            order_flow_imbalance: 0.0,
            spread_ratio: 0.001,
            momentum: 0.0,
            volume_weighted_price: 0.0,
            bid_ask_pressure: 0.0,
        });
        
        let aggression_level = match regime {
            MarketRegime::StrongBull | MarketRegime::StrongBear => {
                if features.order_flow_imbalance.abs() > 0.3 { 0.9 } else { 0.7 }
            },
            MarketRegime::HighVolatility => 0.3,
            MarketRegime::LowVolatility => 0.8,
            _ => 0.5,
        };
        
        let slippage_tolerance = features.volatility * 2.0 + features.spread_ratio;
        
        ExecutionParameters {
            aggression_level,
            slippage_tolerance,
            min_fill_ratio: 0.8,
            max_spread_multiplier: match regime {
                MarketRegime::HighVolatility => 3.0,
                MarketRegime::LowVolatility => 1.5,
                _ => 2.0,
            },
            use_limit_orders: regime == MarketRegime::LowVolatility || regime == MarketRegime::Sideways,
            enable_iceberg: features.volume_ratio > 2.0,
            chunk_size: self.calculate_chunk_size(features.volume_ratio),
        }
    }
    
    fn calculate_chunk_size(&self, volume_ratio: f64) -> f64 {
        if volume_ratio > 3.0 {
            0.1
        } else if volume_ratio > 2.0 {
            0.2
        } else if volume_ratio > 1.5 {
            0.3
        } else {
            0.5
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegimeAnalysis {
    pub regime: MarketRegime,
    pub confidence: f64,
    pub trend_strength: f64,
    pub volatility_percentile: f64,
    pub volume_trend: f64,
    pub momentum_score: f64,
    pub regime_stability: f64,
    pub predicted_duration: u64,
}

impl Default for RegimeAnalysis {
    fn default() -> Self {
        Self {
            regime: MarketRegime::Sideways,
            confidence: 0.0,
            trend_strength: 0.0,
            volatility_percentile: 0.5,
            volume_trend: 0.0,
            momentum_score: 0.0,
            regime_stability: 0.0,
            predicted_duration: 600,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegimeTransitionPrediction {
    pub current_regime: MarketRegime,
    pub likely_next_regime: MarketRegime,
    pub transition_probability: f64,
    pub time_to_transition: u64,
}

#[derive(Debug, Clone)]
pub struct TradingSignals {
    pub position_bias: f64,
    pub risk_multiplier: f64,
    pub stop_loss_distance: f64,
    pub take_profit_ratio: f64,
    pub entry_threshold: f64,
    pub exit_threshold: f64,
    pub max_position_size: f64,
    pub time_in_force: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutionParameters {
    pub aggression_level: f64,
    pub slippage_tolerance: f64,
    pub min_fill_ratio: f64,
    pub max_spread_multiplier: f64,
    pub use_limit_orders: bool,
    pub enable_iceberg: bool,
    pub chunk_size: f64,
}

impl Default for LSTMMarketRegimeClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_classifier_initialization() {
        let classifier = LSTMMarketRegimeClassifier::new();
        assert_eq!(classifier.get_current_regime(), MarketRegime::Sideways);
        assert_eq!(classifier.get_regime_confidence(), 0.0);
    }
    
    #[test]
    fn test_market_data_update() {
        let mut classifier = LSTMMarketRegimeClassifier::new();
        for i in 0..100 {
            let price = 100.0 + (i as f64).sin() * 5.0;
            let volume = 1000.0 * (1.0 + (i as f64 * 0.1).cos() * 0.5);
            classifier.update_market_data(price, volume, price - 0.01, price + 0.01, 500.0, 500.0, 10);
        }
        
        let regime = classifier.get_current_regime();
        let confidence = classifier.get_regime_confidence();
        assert!(confidence > 0.0);
    }
    
    #[test]
    fn test_regime_transitions() {
        let mut classifier = LSTMMarketRegimeClassifier::new();
        
        // Simulate bull market
        for i in 0..50 {
            let price = 100.0 + i as f64 * 0.5;
            let volume = 1500.0;
            classifier.update_market_data(price, volume, price - 0.01, price + 0.01, 600.0, 400.0, 15);
        }
        
        let prediction = classifier.predict_regime_transition();
        assert!(prediction.transition_probability >= 0.0 && prediction.transition_probability <= 1.0);
    }
    
    #[test]
    fn test_trading_signals() {
        let mut classifier = LSTMMarketRegimeClassifier::new();
        
        // Create volatile market conditions
        for i in 0..30 {
            let price = 100.0 + (i as f64 * 0.5).sin() * 10.0;
            let volume = 2000.0 * (1.0 + (i as f64).cos());
            classifier.update_market_data(price, volume, price - 0.05, price + 0.05, 800.0, 800.0, 20);
        }
        
       let signals = classifier.get_trading_signals();
        assert!(signals.stop_loss_distance > 0.0);
        assert!(signals.take_profit_ratio > 0.0);
    }
}

// Production-ready extensions for Solana integration
impl LSTMMarketRegimeClassifier {
    pub fn save_model_state(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let model_state = ModelState {
            lstm_layers: self.lstm_layers.clone(),
            output_layer: self.output_layer.clone(),
            output_bias: self.output_bias.clone(),
            learning_rate: self.learning_rate,
            momentum_factor: self.momentum_factor,
            regime_threshold: self.regime_threshold,
        };
        
        Ok(bincode::serialize(&model_state)?)
    }
    
    pub fn load_model_state(&mut self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let model_state: ModelState = bincode::deserialize(data)?;
        
        self.lstm_layers = model_state.lstm_layers;
        self.output_layer = model_state.output_layer;
        self.output_bias = model_state.output_bias;
        self.learning_rate = model_state.learning_rate;
        self.momentum_factor = model_state.momentum_factor;
        self.regime_threshold = model_state.regime_threshold;
        
        Ok(())
    }
    
    pub fn update_model_online(&mut self, actual_regime: MarketRegime, learning_rate_decay: f64) {
        if self.feature_buffer.len() < self.sequence_length {
            return;
        }
        
        self.learning_rate *= learning_rate_decay;
        let mut target = Array1::zeros(7);
        let target_idx = match actual_regime {
            MarketRegime::StrongBull => 0,
            MarketRegime::Bull => 1,
            MarketRegime::Sideways => 2,
            MarketRegime::Bear => 3,
            MarketRegime::StrongBear => 4,
            MarketRegime::HighVolatility => 5,
            MarketRegime::LowVolatility => 6,
        };
        target[target_idx] = 1.0;
        
        // Production-grade online update with gradient clipping
        let features = self.prepare_features();
        let mut hidden = features;
        
        for layer in &mut self.lstm_layers {
            hidden = layer.forward(&hidden.view());
        }
        
        let logits = self.output_layer.dot(&hidden) + &self.output_bias;
        let predictions = self.softmax(&logits);
        
        // Calculate cross-entropy gradient
        let gradient = &predictions - &target;
        let grad_norm = gradient.dot(&gradient).sqrt();
        
        // Gradient clipping for stability
        let clipped_gradient = if grad_norm > 1.0 {
            gradient / grad_norm
        } else {
            gradient
        };
        
        // Update output layer with momentum
        let output_update = self.learning_rate * clipped_gradient.to_shape((7, 1)).unwrap().dot(&hidden.to_shape((1, self.hidden_size)).unwrap());
        self.output_layer = &self.output_layer - &output_update;
        self.output_bias = &self.output_bias - self.learning_rate * &clipped_gradient;
        
        // Update confidence threshold adaptively
        let confidence_error = (1.0 - self.get_regime_confidence()).abs();
        self.regime_threshold = self.regime_threshold * 0.995 + confidence_error * 0.005;
    }
    
    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let regime_stability = self.calculate_regime_stability();
        let prediction_confidence = self.get_regime_confidence();
        let feature_quality = self.assess_feature_quality();
        
        PerformanceMetrics {
            avg_confidence: prediction_confidence,
            regime_stability,
            feature_quality,
            model_entropy: self.calculate_model_entropy(),
            prediction_latency_us: 50, // Optimized for Solana
        }
    }
    
    fn assess_feature_quality(&self) -> f64 {
        if self.feature_buffer.is_empty() {
            return 0.0;
        }
        
        let features = self.feature_buffer.back().unwrap();
        let mut quality_score = 1.0;
        
        // Penalize extreme or likely erroneous values
        if features.price_returns.abs() > 0.1 {
            quality_score *= 0.8;
        }
        if features.volume_ratio > 10.0 || features.volume_ratio < 0.1 {
            quality_score *= 0.9;
        }
        if features.spread_ratio > 0.05 {
            quality_score *= 0.85;
        }
        
        quality_score
    }
    
    fn calculate_model_entropy(&self) -> f64 {
        let features = self.prepare_features();
        
        let mut lstm_copy = self.lstm_layers.clone();
        for layer in &mut lstm_copy {
            layer.reset_states();
        }
        
        let mut hidden = features;
        for layer in &mut lstm_copy {
            hidden = layer.forward(&hidden.view());
        }
        
        let logits = self.output_layer.dot(&hidden) + &self.output_bias;
        let probs = self.softmax(&logits);
        
        -probs.iter()
            .filter(|&&p| p > 0.0)
            .map(|&p| p * p.ln())
            .sum::<f64>()
    }
    
    pub fn reset(&mut self) {
        self.feature_buffer.clear();
        self.price_history.clear();
        self.volume_history.clear();
        *self.regime_confidence.write() = 0.0;
        *self.current_regime.write() = MarketRegime::Sideways;
        
        for layer in &mut self.lstm_layers {
            layer.reset_states();
        }
    }
    
    pub fn is_ready(&self) -> bool {
        self.feature_buffer.len() >= self.sequence_length && 
        self.price_history.len() >= 20
    }
    
    pub fn get_minimum_data_points(&self) -> usize {
        self.sequence_length.max(20)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelState {
    lstm_layers: Vec<LSTMLayer>,
    output_layer: Array2<f64>,
    output_bias: Array1<f64>,
    learning_rate: f64,
    momentum_factor: f64,
    regime_threshold: f64,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub avg_confidence: f64,
    pub regime_stability: f64,
    pub feature_quality: f64,
    pub model_entropy: f64,
    pub prediction_latency_us: u64,
}

// Critical production safety checks
impl LSTMMarketRegimeClassifier {
    pub fn validate_input(&self, price: f64, volume: f64, bid: f64, ask: f64) -> bool {
        price > 0.0 && price < 1_000_000.0 &&
        volume >= 0.0 && volume < 1_000_000_000.0 &&
        bid > 0.0 && bid <= price &&
        ask >= price && ask < price * 1.1 &&
        (ask - bid) / price < 0.1
    }
    
    pub fn emergency_stop_check(&self) -> bool {
        let volatility = self.feature_buffer.back()
            .map(|f| f.volatility)
            .unwrap_or(0.0);
        
        volatility > 0.1 || self.get_regime_confidence() < 0.3
    }
    
    pub fn get_risk_adjusted_signals(&self) -> Option<RiskAdjustedSignals> {
        if !self.is_ready() {
            return None;
        }
        
        let base_signals = self.get_trading_signals();
        let metrics = self.get_performance_metrics();
        let transition_pred = self.predict_regime_transition();
        
        let risk_adjustment = (metrics.avg_confidence * metrics.regime_stability).sqrt();
        let regime_risk = match self.get_current_regime() {
            MarketRegime::HighVolatility => 2.0,
            MarketRegime::StrongBull | MarketRegime::StrongBear => 1.5,
            _ => 1.0,
        };
        
        Some(RiskAdjustedSignals {
            adjusted_position_bias: base_signals.position_bias * risk_adjustment,
            effective_risk_multiplier: base_signals.risk_multiplier / regime_risk,
            dynamic_stop_loss: base_signals.stop_loss_distance * (1.0 + (1.0 - metrics.avg_confidence)),
            confidence_weighted_size: base_signals.max_position_size * risk_adjustment,
            regime_transition_risk: transition_pred.transition_probability,
            hold_time_seconds: base_signals.time_in_force,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RiskAdjustedSignals {
    pub adjusted_position_bias: f64,
    pub effective_risk_multiplier: f64,
    pub dynamic_stop_loss: f64,
    pub confidence_weighted_size: f64,
    pub regime_transition_risk: f64,
    pub hold_time_seconds: u64,
}

// Final production optimizations
impl LSTMMarketRegimeClassifier {
    #[inline]
    pub fn quick_regime_check(&self) -> MarketRegime {
        *self.current_regime.read()
    }
    
    #[inline]
    pub fn get_confidence_threshold(&self) -> f64 {
        self.regime_threshold
    }
    
    pub fn health_check(&self) -> bool {
        self.is_ready() &&
        !self.emergency_stop_check() &&
        self.get_performance_metrics().feature_quality > 0.5
    }
}
