use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use rand::Rng;
use rayon::prelude::*;

const FEATURE_DIM: usize = 12;
const HIDDEN_DIM: usize = 32;
const OUTPUT_DIM: usize = 3;
const LEARNING_RATE: f64 = 0.001;
const MOMENTUM: f64 = 0.9;
const WEIGHT_DECAY: f64 = 0.0001;
const BATCH_SIZE: usize = 64;
const HISTORY_SIZE: usize = 10000;
const UPDATE_FREQUENCY: usize = 100;
const MIN_SAMPLES: usize = 500;
const ADAPTIVE_RATE_FACTOR: f64 = 0.95;
const GRADIENT_CLIP: f64 = 1.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyFeatures {
    pub slot_height: f64,
    pub time_of_day: f64,
    pub day_of_week: f64,
    pub recent_avg_latency: f64,
    pub recent_std_latency: f64,
    pub tx_size_kb: f64,
    pub compute_units: f64,
    pub priority_fee: f64,
    pub network_congestion: f64,
    pub validator_stake_weight: f64,
    pub geographic_distance: f64,
    pub recent_success_rate: f64,
}

#[derive(Debug, Clone)]
pub struct LatencyObservation {
    pub features: LatencyFeatures,
    pub actual_latency_ms: f64,
    pub propagation_time_ms: f64,
    pub confirmation_time_ms: f64,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
struct NeuralLayer {
    weights: Vec<Vec<f64>>,
    bias: Vec<f64>,
    momentum_w: Vec<Vec<f64>>,
    momentum_b: Vec<f64>,
}

impl NeuralLayer {
    fn new(input_dim: usize, output_dim: usize) -> Self {
        let mut rng = rand::thread_rng();
        let scale = (2.0 / input_dim as f64).sqrt();
        
        let weights = (0..output_dim)
            .map(|_| {
                (0..input_dim)
                    .map(|_| rng.gen_range(-scale..scale))
                    .collect()
            })
            .collect();
        
        let bias = vec![0.0; output_dim];
        let momentum_w = vec![vec![0.0; input_dim]; output_dim];
        let momentum_b = vec![0.0; output_dim];
        
        Self { weights, bias, momentum_w, momentum_b }
    }
    
    fn forward(&self, input: &[f64]) -> Vec<f64> {
        self.weights.par_iter()
            .zip(&self.bias)
            .map(|(w, b)| {
                let sum: f64 = w.iter()
                    .zip(input)
                    .map(|(wi, xi)| wi * xi)
                    .sum::<f64>() + b;
                sum
            })
            .collect()
    }
    
    fn backward(&mut self, input: &[f64], grad_output: &[f64], learning_rate: f64) -> Vec<f64> {
        let batch_size = 1.0;
        let mut grad_input = vec![0.0; input.len()];
        
        for (i, (w_row, mom_w_row)) in self.weights.iter_mut()
            .zip(self.momentum_w.iter_mut())
            .enumerate() 
        {
            let grad = grad_output[i];
            
            for (j, (w, mom_w)) in w_row.iter_mut()
                .zip(mom_w_row.iter_mut())
                .enumerate() 
            {
                let weight_grad = (grad * input[j] / batch_size) + WEIGHT_DECAY * *w;
                let clipped_grad = weight_grad.max(-GRADIENT_CLIP).min(GRADIENT_CLIP);
                
                *mom_w = MOMENTUM * *mom_w + (1.0 - MOMENTUM) * clipped_grad;
                *w -= learning_rate * *mom_w;
                
                grad_input[j] += grad * *w;
            }
            
            let bias_grad = grad / batch_size;
            let clipped_bias_grad = bias_grad.max(-GRADIENT_CLIP).min(GRADIENT_CLIP);
            self.momentum_b[i] = MOMENTUM * self.momentum_b[i] + (1.0 - MOMENTUM) * clipped_bias_grad;
            self.bias[i] -= learning_rate * self.momentum_b[i];
        }
        
        grad_input
    }
}

pub struct NeuralLatencyPredictor {
    input_layer: Arc<RwLock<NeuralLayer>>,
    hidden_layer: Arc<RwLock<NeuralLayer>>,
    output_layer: Arc<RwLock<NeuralLayer>>,
    history: Arc<RwLock<VecDeque<LatencyObservation>>>,
    feature_stats: Arc<RwLock<FeatureStatistics>>,
    learning_rate: Arc<RwLock<f64>>,
    update_counter: Arc<RwLock<usize>>,
    performance_metrics: Arc<RwLock<PerformanceMetrics>>,
}

#[derive(Debug, Clone)]
struct FeatureStatistics {
    means: Vec<f64>,
    stds: Vec<f64>,
    mins: Vec<f64>,
    maxs: Vec<f64>,
}

impl FeatureStatistics {
    fn new() -> Self {
        Self {
            means: vec![0.0; FEATURE_DIM],
            stds: vec![1.0; FEATURE_DIM],
            mins: vec![f64::MAX; FEATURE_DIM],
            maxs: vec![f64::MIN; FEATURE_DIM],
        }
    }
    
    fn update(&mut self, observations: &[LatencyObservation]) {
        if observations.is_empty() { return; }
        
        let features: Vec<Vec<f64>> = observations.iter()
            .map(|obs| Self::features_to_vec(&obs.features))
            .collect();
        
        for i in 0..FEATURE_DIM {
            let values: Vec<f64> = features.iter().map(|f| f[i]).collect();
            let n = values.len() as f64;
            
            self.means[i] = values.iter().sum::<f64>() / n;
            
            let variance = values.iter()
                .map(|x| (x - self.means[i]).powi(2))
                .sum::<f64>() / n;
            self.stds[i] = variance.sqrt().max(1e-6);
            
            self.mins[i] = values.iter().cloned().fold(f64::INFINITY, f64::min);
            self.maxs[i] = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        }
    }
    
    fn normalize(&self, features: &[f64]) -> Vec<f64> {
        features.iter()
            .zip(&self.means)
            .zip(&self.stds)
            .map(|((f, m), s)| (f - m) / s)
            .collect()
    }
    
    fn features_to_vec(features: &LatencyFeatures) -> Vec<f64> {
        vec![
            features.slot_height,
            features.time_of_day,
            features.day_of_week,
            features.recent_avg_latency,
            features.recent_std_latency,
            features.tx_size_kb,
            features.compute_units,
            features.priority_fee,
            features.network_congestion,
            features.validator_stake_weight,
            features.geographic_distance,
            features.recent_success_rate,
        ]
    }
}

#[derive(Debug, Clone)]
struct PerformanceMetrics {
    mae_latency: f64,
    mae_propagation: f64,
    mae_confirmation: f64,
    mse_latency: f64,
    mse_propagation: f64,
    mse_confirmation: f64,
    prediction_count: usize,
}

impl NeuralLatencyPredictor {
    pub fn new() -> Self {
        Self {
            input_layer: Arc::new(RwLock::new(NeuralLayer::new(FEATURE_DIM, HIDDEN_DIM))),
            hidden_layer: Arc::new(RwLock::new(NeuralLayer::new(HIDDEN_DIM, HIDDEN_DIM))),
            output_layer: Arc::new(RwLock::new(NeuralLayer::new(HIDDEN_DIM, OUTPUT_DIM))),
            history: Arc::new(RwLock::new(VecDeque::with_capacity(HISTORY_SIZE))),
            feature_stats: Arc::new(RwLock::new(FeatureStatistics::new())),
            learning_rate: Arc::new(RwLock::new(LEARNING_RATE)),
            update_counter: Arc::new(RwLock::new(0)),
            performance_metrics: Arc::new(RwLock::new(PerformanceMetrics {
                mae_latency: 0.0,
                mae_propagation: 0.0,
                mae_confirmation: 0.0,
                mse_latency: 0.0,
                mse_propagation: 0.0,
                mse_confirmation: 0.0,
                prediction_count: 0,
            })),
        }
    }
    
    pub fn predict(&self, features: &LatencyFeatures) -> Result<(f64, f64, f64), String> {
        let feature_vec = FeatureStatistics::features_to_vec(features);
        let normalized = self.feature_stats.read()
            .map_err(|_| "Failed to acquire read lock")?
            .normalize(&feature_vec);
        
        let h1 = {
            let layer = self.input_layer.read().map_err(|_| "Failed to read input layer")?;
            Self::relu(&layer.forward(&normalized))
        };
        
        let h2 = {
            let layer = self.hidden_layer.read().map_err(|_| "Failed to read hidden layer")?;
            Self::relu(&layer.forward(&h1))
        };
        
        let output = {
            let layer = self.output_layer.read().map_err(|_| "Failed to read output layer")?;
            layer.forward(&h2)
        };
        
        let predictions = Self::softplus(&output);
        
        Ok((
            predictions[0].max(0.1).min(1000.0),
            predictions[1].max(0.1).min(5000.0),
            predictions[2].max(0.1).min(10000.0),
        ))
    }
    
    pub fn record_observation(&self, observation: LatencyObservation) -> Result<(), String> {
        let mut history = self.history.write().map_err(|_| "Failed to acquire write lock")?;
        
        if history.len() >= HISTORY_SIZE {
            history.pop_front();
        }
        history.push_back(observation);
        
        let mut counter = self.update_counter.write().map_err(|_| "Failed to acquire counter lock")?;
        *counter += 1;
        
        if *counter >= UPDATE_FREQUENCY && history.len() >= MIN_SAMPLES {
            *counter = 0;
            drop(counter);
            drop(history);
            self.train_batch()?;
        }
        
        Ok(())
    }
    
    fn train_batch(&self) -> Result<(), String> {
        let history = self.history.read().map_err(|_| "Failed to read history")?;
        if history.len() < BATCH_SIZE { return Ok(()); }
        
        let mut rng = rand::thread_rng();
        let indices: Vec<usize> = (0..BATCH_SIZE)
            .map(|_| rng.gen_range(0..history.len()))
            .collect();
        
        let batch: Vec<LatencyObservation> = indices.iter()
            .map(|&i| history[i].clone())
            .collect();
        
        drop(history);
        
        self.feature_stats.write()
            .map_err(|_| "Failed to update feature stats")?
            .update(&batch);
        
        let current_lr = *self.learning_rate.read().map_err(|_| "Failed to read learning rate")?;
        
        let mut total_loss = 0.0;
        let mut metrics = PerformanceMetrics {
            mae_latency: 0.0,
            mae_propagation: 0.0,
            mae_confirmation: 0.0,
            mse_latency: 0.0,
            mse_propagation: 0.0,
            mse_confirmation: 0.0,
            prediction_count: batch.len(),
        };
        
        for obs in &batch {
            let features = FeatureStatistics::features_to_vec(&obs.features);
            let normalized = self.feature_stats.read()
                .map_err(|_| "Failed to normalize features")?
                .normalize(&features);
            
            let targets = vec![
                obs.actual_latency_ms,
                obs.propagation_time_ms,
                obs.confirmation_time_ms,
            ];
            
            let (predictions, loss) = self.forward_backward(&normalized, &targets, current_lr)?;
            total_loss += loss;
            
            metrics.mae_latency += (predictions[0] - targets[0]).abs();
            metrics.mae_propagation += (predictions[1] - targets[1]).abs();
            metrics.mae_confirmation += (predictions[2] - targets[2]).abs();
            metrics.mse_latency += (predictions[0] - targets[0]).powi(2);
            metrics.mse_propagation += (predictions[1] - targets[1]).powi(2);
            metrics.mse_confirmation += (predictions[2] - targets[2]).powi(2);
        }
        
                let n = batch.len() as f64;
        metrics.mae_latency /= n;
        metrics.mae_propagation /= n;
        metrics.mae_confirmation /= n;
        metrics.mse_latency /= n;
        metrics.mse_propagation /= n;
        metrics.mse_confirmation /= n;
        
        *self.performance_metrics.write().map_err(|_| "Failed to update metrics")? = metrics;
        
        // Adaptive learning rate based on performance
        if metrics.mse_latency < 10.0 && metrics.mse_propagation < 50.0 {
            *self.learning_rate.write().map_err(|_| "Failed to update learning rate")? *= ADAPTIVE_RATE_FACTOR;
        }
        
        Ok(())
    }
    
    fn forward_backward(&self, input: &[f64], targets: &[f64], learning_rate: f64) -> Result<(Vec<f64>, f64), String> {
        // Forward pass
        let h1_linear = self.input_layer.read()
            .map_err(|_| "Failed to read input layer")?
            .forward(input);
        let h1 = Self::relu(&h1_linear);
        
        let h2_linear = self.hidden_layer.read()
            .map_err(|_| "Failed to read hidden layer")?
            .forward(&h1);
        let h2 = Self::relu(&h2_linear);
        
        let output_linear = self.output_layer.read()
            .map_err(|_| "Failed to read output layer")?
            .forward(&h2);
        let predictions = Self::softplus(&output_linear);
        
        // Calculate loss (MSE)
        let loss: f64 = predictions.iter()
            .zip(targets)
            .map(|(pred, target)| (pred - target).powi(2))
            .sum::<f64>() / targets.len() as f64;
        
        // Backward pass
        let grad_output: Vec<f64> = predictions.iter()
            .zip(targets)
            .zip(&output_linear)
            .map(|((pred, target), linear)| {
                2.0 * (pred - target) * Self::softplus_derivative(*linear) / targets.len() as f64
            })
            .collect();
        
        let grad_h2 = self.output_layer.write()
            .map_err(|_| "Failed to write output layer")?
            .backward(&h2, &grad_output, learning_rate);
        
        let grad_h2_relu: Vec<f64> = grad_h2.iter()
            .zip(&h2_linear)
            .map(|(g, h)| g * Self::relu_derivative(*h))
            .collect();
        
        let grad_h1 = self.hidden_layer.write()
            .map_err(|_| "Failed to write hidden layer")?
            .backward(&h1, &grad_h2_relu, learning_rate);
        
        let grad_h1_relu: Vec<f64> = grad_h1.iter()
            .zip(&h1_linear)
            .map(|(g, h)| g * Self::relu_derivative(*h))
            .collect();
        
        self.input_layer.write()
            .map_err(|_| "Failed to write input layer")?
            .backward(input, &grad_h1_relu, learning_rate);
        
        Ok((predictions, loss))
    }
    
    pub fn get_confidence_interval(&self, features: &LatencyFeatures) -> Result<LatencyConfidenceInterval, String> {
        let base_prediction = self.predict(features)?;
        let mut predictions = Vec::with_capacity(10);
        
        // Monte Carlo dropout approximation
        for _ in 0..10 {
            let feature_vec = FeatureStatistics::features_to_vec(features);
            let mut normalized = self.feature_stats.read()
                .map_err(|_| "Failed to normalize")?
                .normalize(&feature_vec);
            
            // Add small noise for uncertainty estimation
            let mut rng = rand::thread_rng();
            for val in &mut normalized {
                *val += rng.gen_range(-0.01..0.01);
            }
            
            let pred = self.predict_raw(&normalized)?;
            predictions.push(pred);
        }
        
        let calculate_stats = |values: Vec<f64>| -> (f64, f64) {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values.iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>() / values.len() as f64;
            (mean, variance.sqrt())
        };
        
        let latency_vals: Vec<f64> = predictions.iter().map(|p| p.0).collect();
        let prop_vals: Vec<f64> = predictions.iter().map(|p| p.1).collect();
        let conf_vals: Vec<f64> = predictions.iter().map(|p| p.2).collect();
        
        let (_, latency_std) = calculate_stats(latency_vals);
        let (_, prop_std) = calculate_stats(prop_vals);
        let (_, conf_std) = calculate_stats(conf_vals);
        
        Ok(LatencyConfidenceInterval {
            latency_ms: base_prediction.0,
            latency_lower: (base_prediction.0 - 2.0 * latency_std).max(0.1),
            latency_upper: base_prediction.0 + 2.0 * latency_std,
            propagation_ms: base_prediction.1,
            propagation_lower: (base_prediction.1 - 2.0 * prop_std).max(0.1),
            propagation_upper: base_prediction.1 + 2.0 * prop_std,
            confirmation_ms: base_prediction.2,
            confirmation_lower: (base_prediction.2 - 2.0 * conf_std).max(0.1),
            confirmation_upper: base_prediction.2 + 2.0 * conf_std,
            confidence_score: 1.0 / (1.0 + latency_std + prop_std + conf_std),
        })
    }
    
    fn predict_raw(&self, normalized_features: &[f64]) -> Result<(f64, f64, f64), String> {
        let h1 = {
            let layer = self.input_layer.read().map_err(|_| "Failed to read input layer")?;
            Self::relu(&layer.forward(normalized_features))
        };
        
        let h2 = {
            let layer = self.hidden_layer.read().map_err(|_| "Failed to read hidden layer")?;
            Self::relu(&layer.forward(&h1))
        };
        
        let output = {
            let layer = self.output_layer.read().map_err(|_| "Failed to read output layer")?;
            layer.forward(&h2)
        };
        
        let predictions = Self::softplus(&output);
        
        Ok((
            predictions[0].max(0.1).min(1000.0),
            predictions[1].max(0.1).min(5000.0),
            predictions[2].max(0.1).min(10000.0),
        ))
    }
    
    pub fn predict_batch(&self, features_batch: &[LatencyFeatures]) -> Result<Vec<(f64, f64, f64)>, String> {
        features_batch.par_iter()
            .map(|features| self.predict(features))
            .collect::<Result<Vec<_>, _>>()
    }
    
    pub fn get_performance_metrics(&self) -> Result<PerformanceMetrics, String> {
        self.performance_metrics.read()
            .map(|metrics| metrics.clone())
            .map_err(|_| "Failed to read performance metrics".to_string())
    }
    
    pub fn optimize_for_endpoint(&self, endpoint_id: &str, observations: &[LatencyObservation]) -> Result<(), String> {
        if observations.len() < MIN_SAMPLES / 10 {
            return Err("Insufficient observations for endpoint optimization".to_string());
        }
        
        // Fine-tune for specific endpoint
        let current_lr = *self.learning_rate.read().map_err(|_| "Failed to read learning rate")? * 0.1;
        
        for obs in observations.iter().take(BATCH_SIZE) {
            let features = FeatureStatistics::features_to_vec(&obs.features);
            let normalized = self.feature_stats.read()
                .map_err(|_| "Failed to normalize")?
                .normalize(&features);
            
            let targets = vec![
                obs.actual_latency_ms,
                obs.propagation_time_ms,
                obs.confirmation_time_ms,
            ];
            
            self.forward_backward(&normalized, &targets, current_lr)?;
        }
        
        Ok(())
    }
    
    pub fn export_model(&self) -> Result<SerializedModel, String> {
        Ok(SerializedModel {
            input_weights: self.export_layer_weights(&self.input_layer)?,
            hidden_weights: self.export_layer_weights(&self.hidden_layer)?,
            output_weights: self.export_layer_weights(&self.output_layer)?,
            feature_means: self.feature_stats.read()
                .map_err(|_| "Failed to read feature stats")?
                .means.clone(),
            feature_stds: self.feature_stats.read()
                .map_err(|_| "Failed to read feature stats")?
                .stds.clone(),
            learning_rate: *self.learning_rate.read().map_err(|_| "Failed to read learning rate")?,
            version: 1,
        })
    }
    
    pub fn import_model(&self, model: &SerializedModel) -> Result<(), String> {
        if model.version != 1 {
            return Err("Unsupported model version".to_string());
        }
        
        self.import_layer_weights(&self.input_layer, &model.input_weights)?;
        self.import_layer_weights(&self.hidden_layer, &model.hidden_weights)?;
        self.import_layer_weights(&self.output_layer, &model.output_weights)?;
        
        let mut stats = self.feature_stats.write().map_err(|_| "Failed to write feature stats")?;
        stats.means = model.feature_means.clone();
        stats.stds = model.feature_stds.clone();
        
        *self.learning_rate.write().map_err(|_| "Failed to write learning rate")? = model.learning_rate;
        
        Ok(())
    }
    
    fn export_layer_weights(&self, layer: &Arc<RwLock<NeuralLayer>>) -> Result<LayerWeights, String> {
        let layer_guard = layer.read().map_err(|_| "Failed to read layer")?;
        Ok(LayerWeights {
            weights: layer_guard.weights.clone(),
            bias: layer_guard.bias.clone(),
        })
    }
    
    fn import_layer_weights(&self, layer: &Arc<RwLock<NeuralLayer>>, weights: &LayerWeights) -> Result<(), String> {
        let mut layer_guard = layer.write().map_err(|_| "Failed to write layer")?;
        layer_guard.weights = weights.weights.clone();
        layer_guard.bias = weights.bias.clone();
        Ok(())
    }
    
    #[inline]
    fn relu(x: &[f64]) -> Vec<f64> {
        x.iter().map(|&v| v.max(0.0)).collect()
    }
    
    #[inline]
    fn relu_derivative(x: f64) -> f64 {
        if x > 0.0 { 1.0 } else { 0.0 }
    }
    
    #[inline]
    fn softplus(x: &[f64]) -> Vec<f64> {
        x.iter().map(|&v| (1.0 + v.exp()).ln()).collect()
    }
    
    #[inline]
    fn softplus_derivative(x: f64) -> f64 {
        1.0 / (1.0 + (-x).exp())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyConfidenceInterval {
    pub latency_ms: f64,
    pub latency_lower: f64,
    pub latency_upper: f64,
    pub propagation_ms: f64,
    pub propagation_lower: f64,
    pub propagation_upper: f64,
    pub confirmation_ms: f64,
    pub confirmation_lower: f64,
    pub confirmation_upper: f64,
    pub confidence_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedModel {
    pub input_weights: LayerWeights,
    pub hidden_weights: LayerWeights,
    pub output_weights: LayerWeights,
    pub feature_means: Vec<f64>,
    pub feature_stds: Vec<f64>,
    pub learning_rate: f64,
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerWeights {
    pub weights: Vec<Vec<f64>>,
    pub bias: Vec<f64>,
}

impl Default for NeuralLatencyPredictor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_predictor_initialization() {
        let predictor = NeuralLatencyPredictor::new();
        assert!(predictor.predict(&create_test_features()).is_ok());
    }
    
    fn create_test_features() -> LatencyFeatures {
        LatencyFeatures {
            slot_height: 150_000_000.0,
            time_of_day: 0.5,
            day_of_week: 3.0,
            recent_avg_latency: 50.0,
            recent_std_latency: 10.0,
            tx_size_kb: 1.5,
            compute_units: 200_000.0,
            priority_fee: 10_000.0,
            network_congestion: 0.7,
            validator_stake_weight: 0.85,
            geographic_distance: 2500.0,
            recent_success_rate: 0.95,
        }
    }
}

