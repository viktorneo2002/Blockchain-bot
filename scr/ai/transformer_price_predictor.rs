use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use anyhow::{Result, bail};
use sha2::{Sha256, Digest};
use rand_chacha::{ChaCha20Rng, rand_core::SeedableRng};
use rand::distributions::{Distribution, Uniform};
use bincode;
use std::fs;
use std::path::Path;

const SEQUENCE_LENGTH: usize = 128;
const EMBEDDING_DIM: usize = 64;
const NUM_HEADS: usize = 8;
const NUM_LAYERS: usize = 4;
const FF_DIM: usize = 256;
const MAX_PRICE_HISTORY: usize = 2048;
const PREDICTION_HORIZON: usize = 10;
const FEATURE_DIM: usize = 16;
const DROPOUT_RATE: f64 = 0.1;
const EPSILON: f64 = 1e-8;
const MODEL_VERSION: u32 = 3;
const LEARNING_RATE: f64 = 1e-4;
const GRADIENT_CLIP: f64 = 1.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceDataPoint {
    pub timestamp: u64,
    pub price: f64,
    pub volume: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub spread: f64,
    pub volatility: f64,
    pub momentum: f64,
    pub rsi: f64,
    pub macd: f64,
    pub bb_upper: f64,
    pub bb_lower: f64,
    pub vwap: f64,
    pub order_flow_imbalance: f64,
    pub tick_direction: f64,
    pub trade_intensity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelWeights {
    version: u32,
    embedding_weights: DenseWeights,
    transformer_layers: Vec<TransformerWeights>,
    output_weights: DenseWeights,
    positional_encoding: Vec<Vec<f64>>,
    training_steps: u64,
    performance_metrics: PerformanceMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerformanceMetrics {
    avg_prediction_error: f64,
    sharpe_ratio: f64,
    win_rate: f64,
    profit_factor: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransformerWeights {
    attention: MultiHeadAttentionWeights,
    feed_forward: FeedForwardWeights,
    norm1: LayerNormWeights,
    norm2: LayerNormWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultiHeadAttentionWeights {
    heads: Vec<AttentionHeadWeights>,
    output_projection: DenseWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AttentionHeadWeights {
    query_projection: DenseWeights,
    key_projection: DenseWeights,
    value_projection: DenseWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FeedForwardWeights {
    dense1: DenseWeights,
    dense2: DenseWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DenseWeights {
    weights: Vec<Vec<f64>>,
    bias: Vec<f64>,
    gradients: Option<GradientInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GradientInfo {
    weight_grads: Vec<Vec<f64>>,
    bias_grads: Vec<f64>,
    momentum: Vec<Vec<f64>>,
    velocity: Vec<Vec<f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LayerNormWeights {
    gamma: Vec<f64>,
    beta: Vec<f64>,
    running_mean: Vec<f64>,
    running_var: Vec<f64>,
}

pub struct TransformerPricePredictor {
    model_weights: Arc<RwLock<ModelWeights>>,
    price_history: Arc<RwLock<VecDeque<PriceDataPoint>>>,
    prediction_cache: Arc<RwLock<Vec<f64>>>,
    feature_stats: Arc<RwLock<FeatureStatistics>>,
    model_path: String,
}

#[derive(Debug, Clone)]
struct FeatureStatistics {
    means: Vec<f64>,
    stds: Vec<f64>,
    mins: Vec<f64>,
    maxs: Vec<f64>,
    update_count: u64,
}

impl TransformerPricePredictor {
    pub fn new(model_path: &str) -> Result<Self> {
        let model_weights = if Path::new(model_path).exists() {
            Self::load_model(model_path)?
        } else {
            Self::initialize_model()?
        };
        
        let feature_stats = Arc::new(RwLock::new(FeatureStatistics {
            means: vec![0.0; FEATURE_DIM],
            stds: vec![1.0; FEATURE_DIM],
            mins: vec![f64::MAX; FEATURE_DIM],
            maxs: vec![f64::MIN; FEATURE_DIM],
            update_count: 0,
        }));
        
        Ok(Self {
            model_weights: Arc::new(RwLock::new(model_weights)),
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_PRICE_HISTORY))),
            prediction_cache: Arc::new(RwLock::new(Vec::with_capacity(PREDICTION_HORIZON))),
            feature_stats,
            model_path: model_path.to_string(),
        })
    }
    
    fn initialize_model() -> Result<ModelWeights> {
        let mut rng = ChaCha20Rng::from_entropy();
        
        let embedding_weights = Self::xavier_init_dense(&mut rng, FEATURE_DIM, EMBEDDING_DIM);
        let positional_encoding = Self::create_sinusoidal_encoding(SEQUENCE_LENGTH, EMBEDDING_DIM);
        
        let mut transformer_layers = Vec::with_capacity(NUM_LAYERS);
        for _ in 0..NUM_LAYERS {
            transformer_layers.push(TransformerWeights {
                attention: Self::init_multi_head_attention(&mut rng, EMBEDDING_DIM, NUM_HEADS),
                feed_forward: Self::init_feed_forward(&mut rng, EMBEDDING_DIM, FF_DIM),
                norm1: Self::init_layer_norm(EMBEDDING_DIM),
                norm2: Self::init_layer_norm(EMBEDDING_DIM),
            });
        }
        
        let output_weights = Self::he_init_dense(&mut rng, EMBEDDING_DIM, PREDICTION_HORIZON);
        
        Ok(ModelWeights {
            version: MODEL_VERSION,
            embedding_weights,
            transformer_layers,
            output_weights,
            positional_encoding,
            training_steps: 0,
            performance_metrics: PerformanceMetrics {
                avg_prediction_error: 1.0,
                sharpe_ratio: 0.0,
                win_rate: 0.5,
                profit_factor: 1.0,
            },
        })
    }
    
    fn load_model(path: &str) -> Result<ModelWeights> {
        let data = fs::read(path)?;
        let weights: ModelWeights = bincode::deserialize(&data)?;
        if weights.version != MODEL_VERSION {
            bail!("Model version mismatch");
        }
        Ok(weights)
    }
    
    pub fn save_model(&self) -> Result<()> {
        let weights = self.model_weights.read();
        let data = bincode::serialize(&*weights)?;
        fs::write(&self.model_path, data)?;
        Ok(())
    }
    
    pub fn add_price_data(&self, data: PriceDataPoint) -> Result<()> {
        let mut history = self.price_history.write();
        if history.len() >= MAX_PRICE_HISTORY {
            history.pop_front();
        }
        history.push_back(data.clone());
        drop(history);
        
        self.update_feature_statistics(&data)?;
        Ok(())
    }
    
    fn update_feature_statistics(&self, data: &PriceDataPoint) -> Result<()> {
        let features = self.extract_features(data);
        let mut stats = self.feature_stats.write();
        
        stats.update_count += 1;
        let alpha = 1.0 / stats.update_count as f64;
        
        for (i, &feat) in features.iter().enumerate() {
            stats.means[i] = stats.means[i] * (1.0 - alpha) + feat * alpha;
            let variance = (feat - stats.means[i]).powi(2);
            let old_var = stats.stds[i].powi(2);
            let new_var = old_var * (1.0 - alpha) + variance * alpha;
            stats.stds[i] = new_var.sqrt();
            stats.mins[i] = stats.mins[i].min(feat);
            stats.maxs[i] = stats.maxs[i].max(feat);
        }
        
        Ok(())
    }
    
    pub fn predict_prices(&self) -> Result<Vec<f64>> {
        let history = self.price_history.read();
        if history.len() < SEQUENCE_LENGTH {
            bail!("Insufficient price history");
        }
        
        let input_sequence = self.prepare_input_sequence(&history)?;
        let weights = self.model_weights.read();
        
        let embeddings = self.embed_sequence(&input_sequence, &weights.embedding_weights)?;
        let mut hidden = self.add_positional_encoding(embeddings, &weights.positional_encoding)?;
        
        for layer_weights in &weights.transformer_layers {
            hidden = self.apply_transformer_layer(layer_weights, hidden)?;
        }
        
        let predictions = self.generate_predictions(hidden, &weights.output_weights)?;
        
        let mut cache = self.prediction_cache.write();
        *cache = predictions.clone();
        
        Ok(predictions)
    }
    
    fn extract_features(&self, data: &PriceDataPoint) -> Vec<f64> {
        vec![
            data.price,
            data.volume.ln(),
            data.bid_volume / (data.bid_volume + data.ask_volume + EPSILON),
            data.spread / data.price,
            data.volatility,
            data.momentum,
            (data.rsi - 50.0) / 50.0,
            data.macd / data.price,
            (data.price - data.bb_lower) / (data.bb_upper - data.bb_lower + EPSILON),
            data.vwap / data.price,
            data.order_flow_imbalance,
            data.tick_direction,
            data.trade_intensity.ln(),
            (data.timestamp % 86400000) as f64 / 86400000.0,
            (data.timestamp % 604800000) as f64 / 604800000.0,
            ((data.volume * data.price).ln() - 10.0) / 5.0,
        ]
    }
    
    fn prepare_input_sequence(&self, history: &VecDeque<PriceDataPoint>) -> Result<Vec<Vec<f64>>> {
        let start = history.len().saturating_sub(SEQUENCE_LENGTH);
        let stats = self.feature_stats.read();
        
        let sequence: Vec<Vec<f64>> = history.iter()
            .skip(start)
            .take(SEQUENCE_LENGTH)
            .map(|dp| {
                let features = self.extract_features(dp);
                features.iter().enumerate().map(|(i, &feat)| {
                    (feat - stats.means[i]) / (stats.stds[i] + EPSILON)
                }).collect()
            })
            .collect();
        
        Ok(sequence)
    }
    
    fn embed_sequence(&self, sequence: &[Vec<f64>], weights: &DenseWeights) -> Result<Vec<Vec<f64>>> {
        sequence.par_iter()
            .map(|features| self.apply_dense_layer(features, weights))
            .collect()
    }
    
    fn add_positional_encoding(&self, mut embeddings: Vec<Vec<f64>>, encoding: &[Vec<f64>]) -> Result<Vec<Vec<f64>>> {
        embeddings.par_iter_mut()
            .zip(encoding.par_iter())
            .for_each(|(emb, enc)| {
                emb.iter_mut().zip(enc.iter()).for_each(|(e, p)| *e += p);
            });
        Ok(embeddings)
    }
    
    fn apply_transformer_layer(&self, layer: &TransformerWeights, input: Vec<Vec<f64>>) -> Result<Vec<Vec<f64>>> {
        let normed1 = self.apply_layer_norm(&input, &layer.norm1)?;
        let attention_output = self.apply_multi_head_attention(&normed1, &layer.attention)?;
        let residual1 = self.add_residual(&input, &attention_output)?;
        
        let normed2 = self.apply_layer_norm(&residual1, &layer.norm2)?;
        let ff_output = self.apply_feed_forward(&normed2, &layer.feed_forward)?;
        let residual2 = self.add_residual(&residual1, &ff_output)?;
        
        Ok(residual2)
    }
    
    fn apply_multi_head_attention(&self, input: &[Vec<f64>], weights: &MultiHeadAttentionWeights) -> Result<Vec<Vec<f64>>> {
        let head_outputs: Result<Vec<_>> = weights.heads.par_iter()
            .map(|head| self.apply_attention_head(input, head))
            .collect();
        
        let concatenated = self.concatenate_heads(head_outputs?)?;
        concatenated.par_iter()
                        .map(|x| self.apply_dense_layer(x, &weights.output_projection))
            .collect()
    }
    
    fn apply_attention_head(&self, input: &[Vec<f64>], head: &AttentionHeadWeights) -> Result<Vec<Vec<f64>>> {
        let queries: Vec<Vec<f64>> = input.par_iter()
            .map(|x| self.apply_dense_layer(x, &head.query_projection))
            .collect::<Result<Vec<_>>>()?;
        
        let keys: Vec<Vec<f64>> = input.par_iter()
            .map(|x| self.apply_dense_layer(x, &head.key_projection))
            .collect::<Result<Vec<_>>>()?;
        
        let values: Vec<Vec<f64>> = input.par_iter()
            .map(|x| self.apply_dense_layer(x, &head.value_projection))
            .collect::<Result<Vec<_>>>()?;
        
        let dim = queries[0].len() as f64;
        let attention_weights = self.compute_scaled_attention(&queries, &keys, 1.0 / dim.sqrt())?;
        self.apply_attention_weights(&attention_weights, &values)
    }
    
    fn compute_scaled_attention(&self, queries: &[Vec<f64>], keys: &[Vec<f64>], scale: f64) -> Result<Vec<Vec<f64>>> {
        let seq_len = queries.len();
        let mut scores = vec![vec![0.0; seq_len]; seq_len];
        
        scores.par_iter_mut().enumerate().for_each(|(i, row)| {
            for j in 0..seq_len {
                row[j] = queries[i].iter()
                    .zip(keys[j].iter())
                    .map(|(q, k)| q * k)
                    .sum::<f64>() * scale;
            }
        });
        
        scores.into_par_iter()
            .map(|row| self.stable_softmax(&row))
            .collect()
    }
    
    fn stable_softmax(&self, logits: &[f64]) -> Result<Vec<f64>> {
        let max = logits.par_iter().cloned().reduce(|| f64::NEG_INFINITY, f64::max);
        let exp_sum: f64 = logits.par_iter().map(|&x| (x - max).exp()).sum();
        
        if !exp_sum.is_finite() || exp_sum == 0.0 {
            bail!("Numerical instability in softmax");
        }
        
        Ok(logits.par_iter()
            .map(|&x| ((x - max).exp() / exp_sum).max(1e-10))
            .collect())
    }
    
    fn apply_attention_weights(&self, weights: &[Vec<f64>], values: &[Vec<f64>]) -> Result<Vec<Vec<f64>>> {
        let seq_len = weights.len();
        let hidden_dim = values[0].len();
        
        Ok((0..seq_len).into_par_iter()
            .map(|i| {
                (0..hidden_dim).map(|k| {
                    weights[i].iter().zip(values.iter())
                        .map(|(&w, v)| w * v[k])
                        .sum()
                }).collect()
            })
            .collect())
    }
    
    fn concatenate_heads(&self, head_outputs: Vec<Vec<Vec<f64>>>) -> Result<Vec<Vec<f64>>> {
        let seq_len = head_outputs[0].len();
        Ok((0..seq_len).into_par_iter()
            .map(|i| {
                head_outputs.iter()
                    .flat_map(|head| head[i].clone())
                    .collect()
            })
            .collect())
    }
    
    fn apply_feed_forward(&self, input: &[Vec<f64>], weights: &FeedForwardWeights) -> Result<Vec<Vec<f64>>> {
        let hidden: Vec<Vec<f64>> = input.par_iter()
            .map(|x| {
                let h = self.apply_dense_layer(x, &weights.dense1)?;
                Ok(self.gelu_activation(h))
            })
            .collect::<Result<Vec<_>>>()?;
        
        hidden.par_iter()
            .map(|h| self.apply_dense_layer(h, &weights.dense2))
            .collect()
    }
    
    fn apply_layer_norm(&self, input: &[Vec<f64>], norm: &LayerNormWeights) -> Result<Vec<Vec<f64>>> {
        input.par_iter().enumerate()
            .map(|(seq_idx, x)| {
                let mean = norm.running_mean[seq_idx % norm.running_mean.len()];
                let var = norm.running_var[seq_idx % norm.running_var.len()];
                let std = (var + EPSILON).sqrt();
                
                Ok(x.iter()
                    .zip(&norm.gamma)
                    .zip(&norm.beta)
                    .map(|((x, gamma), beta)| gamma * (x - mean) / std + beta)
                    .collect())
            })
            .collect()
    }
    
    fn add_residual(&self, a: &[Vec<f64>], b: &[Vec<f64>]) -> Result<Vec<Vec<f64>>> {
        Ok(a.par_iter()
            .zip(b.par_iter())
            .map(|(x, y)| x.iter().zip(y).map(|(a, b)| a + b).collect())
            .collect())
    }
    
    fn apply_dense_layer(&self, input: &[f64], weights: &DenseWeights) -> Result<Vec<f64>> {
        Ok(weights.weights.par_iter()
            .zip(&weights.bias)
            .map(|(w, b)| {
                w.iter().zip(input).map(|(wi, xi)| wi * xi).sum::<f64>() + b
            })
            .collect())
    }
    
    fn gelu_activation(&self, x: Vec<f64>) -> Vec<f64> {
        const SQRT_2_OVER_PI: f64 = 0.7978845608028654;
        x.into_par_iter()
            .map(|val| 0.5 * val * (1.0 + (SQRT_2_OVER_PI * (val + 0.044715 * val.powi(3))).tanh()))
            .collect()
    }
    
    fn generate_predictions(&self, hidden_states: Vec<Vec<f64>>, output_weights: &DenseWeights) -> Result<Vec<f64>> {
        let pooled = self.weighted_average_pool(hidden_states)?;
        let raw_predictions = self.apply_dense_layer(&pooled, output_weights)?;
        
        let history = self.price_history.read();
        let recent_data: Vec<&PriceDataPoint> = history.iter().rev().take(20).collect();
        
        let base_price = recent_data[0].price;
        let volatility = self.calculate_realized_volatility(&recent_data)?;
        let trend = self.calculate_trend_strength(&recent_data)?;
        
        Ok(raw_predictions.iter().enumerate()
            .map(|(i, &delta)| {
                let time_factor = (i + 1) as f64;
                let drift = trend * time_factor * 0.001;
                let diffusion = volatility * (time_factor.sqrt()) * delta * 0.01;
                base_price * (1.0 + drift + diffusion)
            })
            .collect())
    }
    
    fn weighted_average_pool(&self, states: Vec<Vec<f64>>) -> Result<Vec<f64>> {
        let seq_len = states.len() as f64;
        let weights: Vec<f64> = (0..states.len())
            .map(|i| ((i as f64 - seq_len/2.0) / (seq_len/2.0)).exp())
            .collect();
        
        let weight_sum: f64 = weights.iter().sum();
        let dim = states[0].len();
        
        Ok((0..dim).into_par_iter()
            .map(|d| {
                states.iter().zip(&weights)
                    .map(|(state, &w)| state[d] * w)
                    .sum::<f64>() / weight_sum
            })
            .collect())
    }
    
    fn calculate_realized_volatility(&self, recent: &[&PriceDataPoint]) -> Result<f64> {
        if recent.len() < 2 {
            return Ok(0.02);
        }
        
        let returns: Vec<f64> = recent.windows(2)
            .map(|w| (w[0].price / w[1].price).ln())
            .collect();
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        
        Ok((variance.sqrt() * (252.0_f64).sqrt()).min(2.0))
    }
    
    fn calculate_trend_strength(&self, recent: &[&PriceDataPoint]) -> Result<f64> {
        if recent.len() < 5 {
            return Ok(0.0);
        }
        
        let prices: Vec<f64> = recent.iter().map(|d| d.price).collect();
        let x_values: Vec<f64> = (0..prices.len()).map(|i| i as f64).collect();
        
        let x_mean = x_values.iter().sum::<f64>() / x_values.len() as f64;
        let y_mean = prices.iter().sum::<f64>() / prices.len() as f64;
        
        let cov: f64 = x_values.iter().zip(&prices)
            .map(|(x, y)| (x - x_mean) * (y - y_mean))
            .sum::<f64>() / x_values.len() as f64;
        
        let var_x: f64 = x_values.iter()
            .map(|x| (x - x_mean).powi(2))
            .sum::<f64>() / x_values.len() as f64;
        
        Ok((cov / (var_x + EPSILON)).tanh())
    }
    
    pub fn get_prediction_confidence(&self) -> Result<f64> {
        let history = self.price_history.read();
        if history.len() < 50 {
            return Ok(0.3);
        }
        
        let recent: Vec<&PriceDataPoint> = history.iter().rev().take(50).collect();
        let volatility = self.calculate_realized_volatility(&recent)?;
        let spread_quality = self.calculate_spread_quality(&recent)?;
        let volume_consistency = self.calculate_volume_consistency(&recent)?;
        let model_metrics = &self.model_weights.read().performance_metrics;
        
        let base_confidence = 0.25 * (1.0 - volatility.min(1.0))
            + 0.25 * spread_quality
            + 0.25 * volume_consistency
            + 0.25 * model_metrics.win_rate;
        
        Ok(base_confidence.max(0.1).min(0.95))
    }
    
    fn calculate_spread_quality(&self, recent: &[&PriceDataPoint]) -> Result<f64> {
        let avg_spread_pct = recent.iter()
            .map(|d| d.spread / d.price)
            .sum::<f64>() / recent.len() as f64;
        
        Ok((1.0 - avg_spread_pct * 100.0).max(0.0).min(1.0))
    }
    
    fn calculate_volume_consistency(&self, recent: &[&PriceDataPoint]) -> Result<f64> {
        let volumes: Vec<f64> = recent.iter().map(|d| d.volume.ln()).collect();
        let mean = volumes.iter().sum::<f64>() / volumes.len() as f64;
        let std = (volumes.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / volumes.len() as f64).sqrt();
        let cv = std / (mean + EPSILON);
        
        Ok((1.0 - cv).max(0.0).min(1.0))
    }
    
    pub fn calculate_mev_opportunity_score(&self, predictions: &[f64]) -> Result<f64> {
        let history = self.price_history.read();
        if history.is_empty() || predictions.is_empty() {
            return Ok(0.0);
        }
        
        let current = &history[history.len() - 1];
        let expected_profit = predictions.iter().enumerate()
            .map(|(i, &p)| {
                let ret = (p - current.price) / current.price;
                let discount = (-(i as f64) / 5.0).exp();
                ret * discount
            })
            .sum::<f64>();
        
        let confidence = self.get_prediction_confidence()?;
        let volatility = current.volatility;
        let risk_adjusted_score = expected_profit * confidence / (1.0 + volatility);
        
        Ok(risk_adjusted_score.tanh())
    }
    
    fn xavier_init_dense(rng: &mut ChaCha20Rng, input_dim: usize, output_dim: usize) -> DenseWeights {
        let scale = (2.0 / (input_dim + output_dim) as f64).sqrt();
        let dist = Uniform::new(-scale, scale);
        
        let weights = (0..output_dim)
            .map(|_| (0..input_dim).map(|_| dist.sample(rng)).collect())
            .collect();
        
        let bias = vec![0.0; output_dim];
        
        DenseWeights { weights, bias, gradients: None }
    }
    
    fn he_init_dense(rng: &mut ChaCha20Rng, input_dim: usize, output_dim: usize) -> DenseWeights {
        let scale = (2.0 / input_dim as f64).sqrt();
        let dist = Uniform::new(-scale, scale);
        
        let weights = (0..output_dim)
            .map(|_| (0..input_dim).map(|_| dist.sample(rng)).collect())
            .collect();
        
        let bias = vec![0.0; output_dim];
        
        DenseWeights { weights, bias, gradients: None }
    }
    
        fn init_multi_head_attention(rng: &mut ChaCha20Rng, embed_dim: usize, num_heads: usize) -> MultiHeadAttentionWeights {
        let head_dim = embed_dim / num_heads;
        
        let heads = (0..num_heads)
            .map(|_| AttentionHeadWeights {
                query_projection: Self::xavier_init_dense(rng, embed_dim, head_dim),
                key_projection: Self::xavier_init_dense(rng, embed_dim, head_dim),
                value_projection: Self::xavier_init_dense(rng, embed_dim, head_dim),
            })
            .collect();
        
        let output_projection = Self::xavier_init_dense(rng, embed_dim, embed_dim);
        
        MultiHeadAttentionWeights { heads, output_projection }
    }
    
    fn init_feed_forward(rng: &mut ChaCha20Rng, embed_dim: usize, ff_dim: usize) -> FeedForwardWeights {
        FeedForwardWeights {
            dense1: Self::he_init_dense(rng, embed_dim, ff_dim),
            dense2: Self::xavier_init_dense(rng, ff_dim, embed_dim),
        }
    }
    
    fn init_layer_norm(dim: usize) -> LayerNormWeights {
        LayerNormWeights {
            gamma: vec![1.0; dim],
            beta: vec![0.0; dim],
            running_mean: vec![0.0; dim],
            running_var: vec![1.0; dim],
        }
    }
    
    fn create_sinusoidal_encoding(seq_len: usize, embed_dim: usize) -> Vec<Vec<f64>> {
        let mut encoding = vec![vec![0.0; embed_dim]; seq_len];
        
        for pos in 0..seq_len {
            for i in 0..embed_dim {
                let angle_rate = 1.0 / 10000_f64.powf(2.0 * (i / 2) as f64 / embed_dim as f64);
                let angle = pos as f64 * angle_rate;
                encoding[pos][i] = if i % 2 == 0 { angle.sin() } else { angle.cos() };
            }
        }
        
        encoding
    }
    
    pub fn update_model_performance(&self, prediction_error: f64, profit: f64, won: bool) -> Result<()> {
        let mut weights = self.model_weights.write();
        let metrics = &mut weights.performance_metrics;
        
        let alpha = 0.001;
        metrics.avg_prediction_error = metrics.avg_prediction_error * (1.0 - alpha) + prediction_error * alpha;
        metrics.win_rate = metrics.win_rate * (1.0 - alpha) + if won { 1.0 } else { 0.0 } * alpha;
        
        if profit != 0.0 {
            let profit_factor_update = if profit > 0.0 { profit.abs() } else { 1.0 / profit.abs() };
            metrics.profit_factor = metrics.profit_factor * (1.0 - alpha) + profit_factor_update * alpha;
        }
        
        weights.training_steps += 1;
        
        if weights.training_steps % 1000 == 0 {
            drop(weights);
            self.save_model()?;
        }
        
        Ok(())
    }
    
    pub fn fine_tune(&self, actual_price: f64, predicted_idx: usize) -> Result<()> {
        let predictions = self.prediction_cache.read().clone();
        if predicted_idx >= predictions.len() {
            bail!("Invalid prediction index");
        }
        
        let predicted = predictions[predicted_idx];
        let error = (predicted - actual_price) / actual_price;
        
        if error.abs() > 0.001 {
            self.backpropagate_error(error, predicted_idx)?;
        }
        
        self.update_model_performance(error.abs(), 0.0, error.abs() < 0.002)?;
        Ok(())
    }
    
    fn backpropagate_error(&self, error: f64, pred_idx: usize) -> Result<()> {
        let mut weights = self.model_weights.write();
        
        let grad_scale = error.tanh() * LEARNING_RATE;
        let clipped_grad = grad_scale.max(-GRADIENT_CLIP).min(GRADIENT_CLIP);
        
        for i in 0..weights.output_weights.weights[pred_idx].len() {
            weights.output_weights.weights[pred_idx][i] -= clipped_grad * 0.001;
        }
        weights.output_weights.bias[pred_idx] -= clipped_grad * 0.0001;
        
        Ok(())
    }
    
    pub fn get_feature_importance(&self) -> Result<Vec<f64>> {
        let weights = self.model_weights.read();
        let embedding_weights = &weights.embedding_weights.weights;
        
        let importance: Vec<f64> = (0..FEATURE_DIM)
            .map(|feat_idx| {
                embedding_weights.iter()
                    .map(|w| w[feat_idx].abs())
                    .sum::<f64>() / embedding_weights.len() as f64
            })
            .collect();
        
        let sum: f64 = importance.iter().sum();
        Ok(importance.into_iter().map(|i| i / sum).collect())
    }
    
    pub fn calculate_arbitrage_score(&self, exchange_prices: &[(f64, f64)]) -> Result<f64> {
        if exchange_prices.len() < 2 {
            return Ok(0.0);
        }
        
        let predictions = self.prediction_cache.read();
        if predictions.is_empty() {
            return Ok(0.0);
        }
        
        let mut max_arb = 0.0;
        for i in 0..exchange_prices.len() {
            for j in i+1..exchange_prices.len() {
                let (price_i, vol_i) = exchange_prices[i];
                let (price_j, vol_j) = exchange_prices[j];
                
                let spread_pct = ((price_i - price_j).abs() / price_i.min(price_j)) * 100.0;
                let volume_weight = (vol_i * vol_j).sqrt() / (vol_i + vol_j);
                
                let future_spread = predictions.iter()
                    .map(|&p| {
                        let future_i = price_i + (p - price_i) * 0.8;
                        let future_j = price_j + (p - price_j) * 0.8;
                        ((future_i - future_j).abs() / future_i.min(future_j)) * 100.0
                    })
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap_or(spread_pct);
                
                let arb_score = spread_pct * volume_weight * (1.0 + (future_spread - spread_pct).max(0.0));
                max_arb = max_arb.max(arb_score);
            }
        }
        
        Ok(max_arb.tanh())
    }
    
    pub fn get_optimal_entry_exit(&self, risk_tolerance: f64) -> Result<(f64, f64)> {
        let predictions = self.prediction_cache.read();
        if predictions.is_empty() {
            bail!("No predictions available");
        }
        
        let history = self.price_history.read();
        let current_price = history.back().ok_or_else(|| anyhow::anyhow!("No price history"))?.price;
        
        let confidence = self.get_prediction_confidence()?;
        let volatility = self.calculate_realized_volatility(
            &history.iter().rev().take(20).collect::<Vec<_>>()
        )?;
        
        let risk_adjusted_confidence = confidence * (1.0 - risk_tolerance * volatility);
        
        let max_return_idx = predictions.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                let ret_a = (*a - current_price) / current_price;
                let ret_b = (*b - current_price) / current_price;
                ret_a.partial_cmp(&ret_b).unwrap()
            })
            .map(|(i, _)| i)
            .unwrap_or(0);
        
        let entry_threshold = current_price * (1.0 - 0.001 * (1.0 - risk_adjusted_confidence));
        let exit_target = predictions[max_return_idx] * (1.0 - 0.002 * (1.0 - risk_adjusted_confidence));
        
        Ok((entry_threshold, exit_target))
    }
    
    pub fn validate_prediction_sanity(&self, predictions: &[f64]) -> Result<bool> {
        let history = self.price_history.read();
        if history.is_empty() || predictions.is_empty() {
            return Ok(false);
        }
        
        let current_price = history.back().unwrap().price;
        let recent_prices: Vec<f64> = history.iter().rev().take(100).map(|d| d.price).collect();
        
        let historical_min = recent_prices.iter().cloned().fold(f64::INFINITY, f64::min);
        let historical_max = recent_prices.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range = historical_max - historical_min;
        
        let reasonable_min = historical_min - range * 0.5;
        let reasonable_max = historical_max + range * 0.5;
        
        let all_reasonable = predictions.iter().all(|&p| p >= reasonable_min && p <= reasonable_max);
        
        let max_single_jump = predictions.windows(2)
            .map(|w| ((w[1] - w[0]) / w[0]).abs())
            .fold(0.0, f64::max);
        
        let max_total_change = predictions.iter()
            .map(|&p| ((p - current_price) / current_price).abs())
            .fold(0.0, f64::max);
        
        Ok(all_reasonable && max_single_jump < 0.1 && max_total_change < 0.3)
    }
}

impl Default for FeatureStatistics {
    fn default() -> Self {
        Self {
            means: vec![0.0; FEATURE_DIM],
            stds: vec![1.0; FEATURE_DIM],
            mins: vec![f64::MAX; FEATURE_DIM],
            maxs: vec![f64::MIN; FEATURE_DIM],
            update_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_model_initialization() {
        let predictor = TransformerPricePredictor::new("test_model.bin").unwrap();
        assert!(predictor.model_weights.read().transformer_layers.len() == NUM_LAYERS);
    }
    
    #[test]
    fn test_prediction_generation() {
        let predictor = TransformerPricePredictor::new("test_model.bin").unwrap();
        
        for i in 0..SEQUENCE_LENGTH + 10 {
            let data = PriceDataPoint {
                timestamp: 1700000000000 + i * 1000,
                price: 100.0 + (i as f64 * 0.1).sin() * 2.0,
                volume: 1000000.0 * (1.0 + (i as f64 * 0.05).cos() * 0.2),
                bid_volume: 500000.0,
                ask_volume: 500000.0,
                spread: 0.01,
                volatility: 0.015,
                momentum: 0.5,
                rsi: 50.0 + (i as f64 * 0.2).sin() * 20.0,
                macd: 0.1,
                bb_upper: 102.0,
                bb_lower: 98.0,
                vwap: 100.1,
                order_flow_imbalance: 0.1,
                tick_direction: 1.0,
                trade_intensity: 100.0,
            };
            predictor.add_price_data(data).unwrap();
        }
        
        let predictions = predictor.predict_prices().unwrap();
        assert_eq!(predictions.len(), PREDICTION_HORIZON);
        assert!(predictor.validate_prediction_sanity(&predictions).unwrap());
    }
}

