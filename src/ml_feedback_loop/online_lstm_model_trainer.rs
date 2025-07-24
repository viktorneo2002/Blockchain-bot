use std::sync::{Arc, RwLock};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use ndarray::{Array2, Array3, Axis};
use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::interval;

const LSTM_HIDDEN_SIZE: usize = 128;
const LSTM_LAYERS: usize = 2;
const INPUT_FEATURES: usize = 24;
const OUTPUT_SIZE: usize = 3;
const SEQUENCE_LENGTH: usize = 32;
const BATCH_SIZE: usize = 16;
const LEARNING_RATE: f64 = 0.001;
const GRADIENT_CLIP: f64 = 5.0;
const DROPOUT_RATE: f64 = 0.15;
const L2_REGULARIZATION: f64 = 1e-5;
const ADAM_BETA1: f64 = 0.9;
const ADAM_BETA2: f64 = 0.999;
const ADAM_EPSILON: f64 = 1e-8;
const MAX_MEMORY_BUFFER: usize = 10000;
const UPDATE_FREQUENCY_MS: u64 = 100;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSTMCell {
    w_ih: Array2<f64>,
    w_hh: Array2<f64>,
    b_ih: Array2<f64>,
    b_hh: Array2<f64>,
    hidden_state: Array2<f64>,
    cell_state: Array2<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSTMLayer {
    cells: Vec<LSTMCell>,
    dropout_mask: Option<Array2<f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineLSTMModel {
    layers: Vec<LSTMLayer>,
    output_layer: Array2<f64>,
    output_bias: Array2<f64>,
    adam_m: AdamMoments,
    adam_v: AdamMoments,
    iteration: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdamMoments {
    layers: Vec<LayerMoments>,
    output_m: Array2<f64>,
    output_v: Array2<f64>,
    output_bias_m: Array2<f64>,
    output_bias_v: Array2<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LayerMoments {
    cells: Vec<CellMoments>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CellMoments {
    w_ih_m: Array2<f64>,
    w_ih_v: Array2<f64>,
    w_hh_m: Array2<f64>,
    w_hh_v: Array2<f64>,
    b_ih_m: Array2<f64>,
    b_ih_v: Array2<f64>,
    b_hh_m: Array2<f64>,
    b_hh_v: Array2<f64>,
}

#[derive(Debug, Clone)]
pub struct TrainingData {
    pub features: Array2<f64>,
    pub targets: Array2<f64>,
    pub timestamp: Instant,
    pub priority: f64,
}

pub struct OnlineLSTMTrainer {
    model: Arc<RwLock<OnlineLSTMModel>>,
    memory_buffer: Arc<RwLock<VecDeque<TrainingData>>>,
    performance_metrics: Arc<RwLock<PerformanceMetrics>>,
    training_active: Arc<RwLock<bool>>,
}

#[derive(Debug, Default)]
struct PerformanceMetrics {
    total_updates: u64,
    avg_loss: f64,
    avg_accuracy: f64,
    last_checkpoint: Instant,
    training_time_ms: f64,
}

impl LSTMCell {
    fn new(input_size: usize, hidden_size: usize) -> Self {
        let mut rng = thread_rng();
        let xavier_std = (2.0 / (input_size + hidden_size) as f64).sqrt();
        let dist = Uniform::new(-xavier_std, xavier_std);
        
        Self {
            w_ih: Array2::from_shape_fn((4 * hidden_size, input_size), |_| dist.sample(&mut rng)),
            w_hh: Array2::from_shape_fn((4 * hidden_size, hidden_size), |_| dist.sample(&mut rng)),
            b_ih: Array2::zeros((4 * hidden_size, 1)),
            b_hh: Array2::zeros((4 * hidden_size, 1)),
            hidden_state: Array2::zeros((hidden_size, 1)),
            cell_state: Array2::zeros((hidden_size, 1)),
        }
    }

    fn forward(&mut self, input: &Array2<f64>, training: bool) -> Array2<f64> {
        let gates = self.w_ih.dot(input) + &self.b_ih + self.w_hh.dot(&self.hidden_state) + &self.b_hh;
        
        let hidden_size = self.hidden_state.shape()[0];
        let input_gate = sigmoid(&gates.slice(s![0..hidden_size, ..]));
        let forget_gate = sigmoid(&gates.slice(s![hidden_size..2*hidden_size, ..]));
        let cell_gate = tanh(&gates.slice(s![2*hidden_size..3*hidden_size, ..]));
        let output_gate = sigmoid(&gates.slice(s![3*hidden_size.., ..]));
        
        self.cell_state = &forget_gate * &self.cell_state + &input_gate * &cell_gate;
        self.hidden_state = &output_gate * tanh(&self.cell_state);
        
        if training && rand::random::<f64>() < DROPOUT_RATE {
            &self.hidden_state * (1.0 / (1.0 - DROPOUT_RATE))
        } else {
            self.hidden_state.clone()
        }
    }

    fn backward(&mut self, grad_output: &Array2<f64>, input: &Array2<f64>, 
                prev_hidden: &Array2<f64>, prev_cell: &Array2<f64>) -> (Array2<f64>, Gradients) {
        let gates = self.w_ih.dot(input) + &self.b_ih + self.w_hh.dot(prev_hidden) + &self.b_hh;
        let hidden_size = self.hidden_state.shape()[0];
        
        let input_gate = sigmoid(&gates.slice(s![0..hidden_size, ..]));
        let forget_gate = sigmoid(&gates.slice(s![hidden_size..2*hidden_size, ..]));
        let cell_gate = tanh(&gates.slice(s![2*hidden_size..3*hidden_size, ..]));
        let output_gate = sigmoid(&gates.slice(s![3*hidden_size.., ..]));
        
        let grad_hidden = grad_output;
        let grad_output_gate = grad_hidden * tanh(&self.cell_state);
        let grad_tanh_cell = grad_hidden * &output_gate;
        let grad_cell = grad_tanh_cell * (1.0 - tanh(&self.cell_state).mapv(|x| x * x));
        
        let grad_forget = &grad_cell * prev_cell;
        let grad_input = &grad_cell * &cell_gate;
        let grad_cell_gate = &grad_cell * &input_gate;
        
        let grad_gates = stack![
            Axis(0),
            grad_input * &input_gate * (1.0 - &input_gate),
            grad_forget * &forget_gate * (1.0 - &forget_gate),
            grad_cell_gate * (1.0 - &cell_gate * &cell_gate),
            grad_output_gate * &output_gate * (1.0 - &output_gate)
        ];
        
        let grad_input = self.w_ih.t().dot(&grad_gates);
        let grad_w_ih = grad_gates.dot(&input.t());
        let grad_w_hh = grad_gates.dot(&prev_hidden.t());
        let grad_b_ih = grad_gates.sum_axis(Axis(1)).insert_axis(Axis(1));
        let grad_b_hh = grad_b_ih.clone();
        
        (grad_input, Gradients {
            w_ih: grad_w_ih,
            w_hh: grad_w_hh,
            b_ih: grad_b_ih,
            b_hh: grad_b_hh,
        })
    }
}

#[derive(Debug)]
struct Gradients {
    w_ih: Array2<f64>,
    w_hh: Array2<f64>,
    b_ih: Array2<f64>,
    b_hh: Array2<f64>,
}

impl OnlineLSTMModel {
    fn new() -> Self {
        let mut layers = Vec::with_capacity(LSTM_LAYERS);
        let mut input_size = INPUT_FEATURES;
        
        for _ in 0..LSTM_LAYERS {
            let layer = LSTMLayer {
                cells: vec![LSTMCell::new(input_size, LSTM_HIDDEN_SIZE)],
                dropout_mask: None,
            };
            layers.push(layer);
            input_size = LSTM_HIDDEN_SIZE;
        }
        
        let xavier_std = (2.0 / (LSTM_HIDDEN_SIZE + OUTPUT_SIZE) as f64).sqrt();
        let mut rng = thread_rng();
        let dist = Uniform::new(-xavier_std, xavier_std);
        
        let output_layer = Array2::from_shape_fn((OUTPUT_SIZE, LSTM_HIDDEN_SIZE), |_| dist.sample(&mut rng));
        let output_bias = Array2::zeros((OUTPUT_SIZE, 1));
        
        let adam_m = AdamMoments {
            layers: layers.iter().map(|layer| LayerMoments {
                cells: layer.cells.iter().map(|_| CellMoments {
                    w_ih_m: Array2::zeros((4 * LSTM_HIDDEN_SIZE, INPUT_FEATURES)),
                    w_ih_v: Array2::zeros((4 * LSTM_HIDDEN_SIZE, INPUT_FEATURES)),
                    w_hh_m: Array2::zeros((4 * LSTM_HIDDEN_SIZE, LSTM_HIDDEN_SIZE)),
                    w_hh_v: Array2::zeros((4 * LSTM_HIDDEN_SIZE, LSTM_HIDDEN_SIZE)),
                    b_ih_m: Array2::zeros((4 * LSTM_HIDDEN_SIZE, 1)),
                    b_ih_v: Array2::zeros((4 * LSTM_HIDDEN_SIZE, 1)),
                    b_hh_m: Array2::zeros((4 * LSTM_HIDDEN_SIZE, 1)),
                    b_hh_v: Array2::zeros((4 * LSTM_HIDDEN_SIZE, 1)),
                }).collect()
            }).collect(),
            output_m: Array2::zeros((OUTPUT_SIZE, LSTM_HIDDEN_SIZE)),
            output_v: Array2::zeros((OUTPUT_SIZE, LSTM_HIDDEN_SIZE)),
            output_bias_m: Array2::zeros((OUTPUT_SIZE, 1)),
            output_bias_v: Array2::zeros((OUTPUT_SIZE, 1)),
        };
        
        Self {
            layers,
            output_layer,
            output_bias,
            adam_m,
            adam_v: adam_m.clone(),
            iteration: 0,
        }
    }

    fn forward(&mut self, input: &Array3<f64>, training: bool) -> Array3<f64> {
        let (batch_size, seq_len, _) = input.dim();
        let mut outputs = Vec::with_capacity(seq_len);
        
        for t in 0..seq_len {
            let mut layer_input = input.slice(s![.., t, ..]).to_owned();
            
            for layer in &mut self.layers {
                let mut layer_output = Array2::zeros((batch_size, LSTM_HIDDEN_SIZE));
                
                for b in 0..batch_size {
                    let input_slice = layer_input.slice(s![b, ..]).insert_axis(Axis(1));
                    let output = layer.cells[0].forward(&input_slice, training);
                    layer_output.slice_mut(s![b, ..]).assign(&output.slice(s![.., 0]));
                }
                
                layer_input = layer_output;
            }
            
            outputs.push(layer_input);
        }
        
        let final_hidden = outputs.last().unwrap();
        let logits = self.output_layer.dot(&final_hidden.t()) + &self.output_bias;
        
        Array3::from_shape_fn((batch_size, seq_len, OUTPUT_SIZE), |(b, _, o)| {
            logits[[o, b]]
        })
    }

    fn compute_loss(&self, predictions: &Array3<f64>, targets: &Array2<f64>) -> f64 {
        let (batch_size, _, output_size) = predictions.dim();
        let final_predictions = predictions.slice(s![.., -1, ..]);
        
        let mut loss = 0.0;
        for b in 0..batch_size {
            for o in 0..output_size {
                let pred = final_predictions[[b, o]];
                let target = targets[[b, o]];
                loss += (pred - target).powi(2);
            }
        }
        
        loss /= (batch_size * output_size) as f64;
        
                // L2 regularization
        for layer in &self.layers {
            for cell in &layer.cells {
                loss += L2_REGULARIZATION * cell.w_ih.mapv(|x| x * x).sum();
                loss += L2_REGULARIZATION * cell.w_hh.mapv(|x| x * x).sum();
            }
        }
        loss += L2_REGULARIZATION * self.output_layer.mapv(|x| x * x).sum();
        
        loss
    }

    fn backward(&mut self, input: &Array3<f64>, targets: &Array2<f64>, predictions: &Array3<f64>) -> ModelGradients {
        let (batch_size, seq_len, _) = input.dim();
        let final_predictions = predictions.slice(s![.., -1, ..]);
        
        // Output layer gradients
        let mut grad_output = Array2::zeros((OUTPUT_SIZE, batch_size));
        for b in 0..batch_size {
            for o in 0..OUTPUT_SIZE {
                grad_output[[o, b]] = 2.0 * (final_predictions[[b, o]] - targets[[b, o]]) / batch_size as f64;
            }
        }
        
        let last_hidden = self.get_last_hidden_state(input);
        let grad_output_layer = grad_output.dot(&last_hidden);
        let grad_output_bias = grad_output.sum_axis(Axis(1)).insert_axis(Axis(1));
        
        // Backpropagate through LSTM layers
        let mut layer_gradients = Vec::new();
        let mut grad_hidden = self.output_layer.t().dot(&grad_output);
        
        for (layer_idx, layer) in self.layers.iter_mut().enumerate().rev() {
            let mut cell_gradients = Vec::new();
            
            for cell in &mut layer.cells {
                let layer_input = if layer_idx == 0 {
                    input.slice(s![.., -1, ..]).t().to_owned()
                } else {
                    self.get_layer_output(input, layer_idx - 1, seq_len - 1)
                };
                
                let (grad_input, grads) = cell.backward(&grad_hidden, &layer_input, 
                                                        &cell.hidden_state, &cell.cell_state);
                
                cell_gradients.push(grads);
                grad_hidden = grad_input;
            }
            
            layer_gradients.push(cell_gradients);
        }
        
        layer_gradients.reverse();
        
        ModelGradients {
            layer_gradients,
            output_layer_grad: clip_gradient(&grad_output_layer),
            output_bias_grad: clip_gradient(&grad_output_bias),
        }
    }

    fn get_last_hidden_state(&mut self, input: &Array3<f64>) -> Array2<f64> {
        let (batch_size, seq_len, _) = input.dim();
        let mut layer_input = input.slice(s![.., seq_len - 1, ..]).to_owned();
        
        for layer in &mut self.layers {
            let mut layer_output = Array2::zeros((batch_size, LSTM_HIDDEN_SIZE));
            
            for b in 0..batch_size {
                let input_slice = layer_input.slice(s![b, ..]).insert_axis(Axis(1));
                let output = layer.cells[0].forward(&input_slice, false);
                layer_output.slice_mut(s![b, ..]).assign(&output.slice(s![.., 0]));
            }
            
            layer_input = layer_output;
        }
        
        layer_input.t().to_owned()
    }

    fn get_layer_output(&mut self, input: &Array3<f64>, layer_idx: usize, time_step: usize) -> Array2<f64> {
        let batch_size = input.shape()[0];
        let mut layer_input = input.slice(s![.., time_step, ..]).to_owned();
        
        for (idx, layer) in self.layers[..=layer_idx].iter_mut().enumerate() {
            let mut layer_output = Array2::zeros((batch_size, LSTM_HIDDEN_SIZE));
            
            for b in 0..batch_size {
                let input_slice = layer_input.slice(s![b, ..]).insert_axis(Axis(1));
                let output = layer.cells[0].forward(&input_slice, false);
                layer_output.slice_mut(s![b, ..]).assign(&output.slice(s![.., 0]));
            }
            
            layer_input = layer_output;
        }
        
        layer_input.t().to_owned()
    }

    fn update_weights(&mut self, gradients: &ModelGradients) {
        self.iteration += 1;
        let lr = LEARNING_RATE * (1.0 - ADAM_BETA2.powi(self.iteration as i32)).sqrt() / 
                 (1.0 - ADAM_BETA1.powi(self.iteration as i32));
        
        // Update LSTM layers
        for (layer_idx, (layer, layer_grads)) in self.layers.iter_mut().zip(&gradients.layer_gradients).enumerate() {
            for (cell_idx, (cell, cell_grads)) in layer.cells.iter_mut().zip(layer_grads).enumerate() {
                let moments = &mut self.adam_m.layers[layer_idx].cells[cell_idx];
                let velocities = &mut self.adam_v.layers[layer_idx].cells[cell_idx];
                
                // Update w_ih
                moments.w_ih_m = ADAM_BETA1 * &moments.w_ih_m + (1.0 - ADAM_BETA1) * &cell_grads.w_ih;
                velocities.w_ih_v = ADAM_BETA2 * &velocities.w_ih_v + (1.0 - ADAM_BETA2) * cell_grads.w_ih.mapv(|x| x * x);
                cell.w_ih = &cell.w_ih - lr * &moments.w_ih_m / (velocities.w_ih_v.mapv(|x| x.sqrt() + ADAM_EPSILON));
                
                // Update w_hh
                moments.w_hh_m = ADAM_BETA1 * &moments.w_hh_m + (1.0 - ADAM_BETA1) * &cell_grads.w_hh;
                velocities.w_hh_v = ADAM_BETA2 * &velocities.w_hh_v + (1.0 - ADAM_BETA2) * cell_grads.w_hh.mapv(|x| x * x);
                cell.w_hh = &cell.w_hh - lr * &moments.w_hh_m / (velocities.w_hh_v.mapv(|x| x.sqrt() + ADAM_EPSILON));
                
                // Update biases
                moments.b_ih_m = ADAM_BETA1 * &moments.b_ih_m + (1.0 - ADAM_BETA1) * &cell_grads.b_ih;
                velocities.b_ih_v = ADAM_BETA2 * &velocities.b_ih_v + (1.0 - ADAM_BETA2) * cell_grads.b_ih.mapv(|x| x * x);
                cell.b_ih = &cell.b_ih - lr * &moments.b_ih_m / (velocities.b_ih_v.mapv(|x| x.sqrt() + ADAM_EPSILON));
                
                moments.b_hh_m = ADAM_BETA1 * &moments.b_hh_m + (1.0 - ADAM_BETA1) * &cell_grads.b_hh;
                velocities.b_hh_v = ADAM_BETA2 * &velocities.b_hh_v + (1.0 - ADAM_BETA2) * cell_grads.b_hh.mapv(|x| x * x);
                cell.b_hh = &cell.b_hh - lr * &moments.b_hh_m / (velocities.b_hh_v.mapv(|x| x.sqrt() + ADAM_EPSILON));
            }
        }
        
        // Update output layer
        self.adam_m.output_m = ADAM_BETA1 * &self.adam_m.output_m + (1.0 - ADAM_BETA1) * &gradients.output_layer_grad;
        self.adam_v.output_v = ADAM_BETA2 * &self.adam_v.output_v + (1.0 - ADAM_BETA2) * gradients.output_layer_grad.mapv(|x| x * x);
        self.output_layer = &self.output_layer - lr * &self.adam_m.output_m / (self.adam_v.output_v.mapv(|x| x.sqrt() + ADAM_EPSILON));
        
        self.adam_m.output_bias_m = ADAM_BETA1 * &self.adam_m.output_bias_m + (1.0 - ADAM_BETA1) * &gradients.output_bias_grad;
        self.adam_v.output_bias_v = ADAM_BETA2 * &self.adam_v.output_bias_v + (1.0 - ADAM_BETA2) * gradients.output_bias_grad.mapv(|x| x * x);
        self.output_bias = &self.output_bias - lr * &self.adam_m.output_bias_m / (self.adam_v.output_bias_v.mapv(|x| x.sqrt() + ADAM_EPSILON));
    }

    fn reset_hidden_states(&mut self) {
        for layer in &mut self.layers {
            for cell in &mut layer.cells {
                cell.hidden_state.fill(0.0);
                cell.cell_state.fill(0.0);
            }
        }
    }
}

#[derive(Debug)]
struct ModelGradients {
    layer_gradients: Vec<Vec<Gradients>>,
    output_layer_grad: Array2<f64>,
    output_bias_grad: Array2<f64>,
}

impl OnlineLSTMTrainer {
    pub fn new() -> Self {
        Self {
            model: Arc::new(RwLock::new(OnlineLSTMModel::new())),
            memory_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_MEMORY_BUFFER))),
            performance_metrics: Arc::new(RwLock::new(PerformanceMetrics {
                last_checkpoint: Instant::now(),
                ..Default::default()
            })),
            training_active: Arc::new(RwLock::new(true)),
        }
    }

    pub async fn start_online_training(&self, mut data_receiver: mpsc::Receiver<TrainingData>) {
        let model = Arc::clone(&self.model);
        let buffer = Arc::clone(&self.memory_buffer);
        let metrics = Arc::clone(&self.performance_metrics);
        let active = Arc::clone(&self.training_active);
        
        tokio::spawn(async move {
            let mut update_interval = interval(Duration::from_millis(UPDATE_FREQUENCY_MS));
            
            loop {
                tokio::select! {
                    Some(data) = data_receiver.recv() => {
                        let mut buffer_guard = buffer.write().unwrap();
                        if buffer_guard.len() >= MAX_MEMORY_BUFFER {
                            buffer_guard.pop_front();
                        }
                        buffer_guard.push_back(data);
                    }
                    _ = update_interval.tick() => {
                        if !*active.read().unwrap() {
                            continue;
                        }
                        
                        let batch_data = Self::sample_batch(&buffer);
                        if batch_data.len() >= BATCH_SIZE {
                            let start_time = Instant::now();
                            
                            let (input_batch, target_batch) = Self::prepare_batch(&batch_data);
                            let loss = Self::train_step(&model, &input_batch, &target_batch);
                            
                            let mut metrics_guard = metrics.write().unwrap();
                            metrics_guard.total_updates += 1;
                            metrics_guard.avg_loss = metrics_guard.avg_loss * 0.99 + loss * 0.01;
                            metrics_guard.training_time_ms = metrics_guard.training_time_ms * 0.99 + 
                                                            start_time.elapsed().as_secs_f64() * 1000.0 * 0.01;
                            
                            if metrics_guard.last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
                                Self::checkpoint_model(&model, &metrics_guard);
                                metrics_guard.last_checkpoint = Instant::now();
                            }
                        }
                    }
                }
            }
        });
    }

    fn sample_batch(buffer: &Arc<RwLock<VecDeque<TrainingData>>>) -> Vec<TrainingData> {
        let buffer_guard = buffer.read().unwrap();
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        
        if buffer_guard.len() < SEQUENCE_LENGTH + BATCH_SIZE {
            return batch;
        }
        
        // Priority-based sampling with temporal coherence
        let total_priority: f64 = buffer_guard.iter().map(|d| d.priority).sum();
        let mut cumulative = 0.0;
        
        for _ in 0..BATCH_SIZE {
            let target = rand::random::<f64>() * total_priority;
            let mut idx = 0;
            
            for (i, data) in buffer_guard.iter().enumerate() {
                cumulative += data.priority;
                if cumulative >= target {
                    idx = i;
                    break;
                }
            }
            
            if idx + SEQUENCE_LENGTH < buffer_guard.len() {
                let sequence: Vec<_> = buffer_guard.range(idx..idx + SEQUENCE_LENGTH)
                                                  .cloned()
                                                  .collect();
                if let Some(last) = sequence.last() {
                    batch.push(last.clone());
                }
            }
        }
        
        batch
    }

        fn prepare_batch(batch_data: &[TrainingData]) -> (Array3<f64>, Array2<f64>) {
        let batch_size = batch_data.len().min(BATCH_SIZE);
        let mut input = Array3::zeros((batch_size, SEQUENCE_LENGTH, INPUT_FEATURES));
        let mut targets = Array2::zeros((batch_size, OUTPUT_SIZE));
        
        for (i, data) in batch_data.iter().take(batch_size).enumerate() {
            // Normalize features for stable training
            let features = &data.features;
            let normalized = (features - features.mean().unwrap()) / (features.std(0.0) + 1e-8);
            
            for t in 0..SEQUENCE_LENGTH.min(normalized.shape()[0]) {
                for f in 0..INPUT_FEATURES {
                    input[[i, t, f]] = normalized[[t, f]];
                }
            }
            
            for o in 0..OUTPUT_SIZE {
                targets[[i, o]] = data.targets[[0, o]];
            }
        }
        
        (input, targets)
    }

    fn train_step(model: &Arc<RwLock<OnlineLSTMModel>>, input: &Array3<f64>, targets: &Array2<f64>) -> f64 {
        let mut model_guard = model.write().unwrap();
        
        // Forward pass
        let predictions = model_guard.forward(input, true);
        let loss = model_guard.compute_loss(&predictions, targets);
        
        // Backward pass
        let gradients = model_guard.backward(input, targets, &predictions);
        
        // Update weights
        model_guard.update_weights(&gradients);
        
        // Reset hidden states for next sequence
        model_guard.reset_hidden_states();
        
        loss
    }

    fn checkpoint_model(model: &Arc<RwLock<OnlineLSTMModel>>, metrics: &PerformanceMetrics) {
        let model_guard = model.read().unwrap();
        let checkpoint_path = format!("checkpoints/lstm_model_{}_loss_{:.6}.bin", 
                                     metrics.total_updates, metrics.avg_loss);
        
        if let Ok(serialized) = bincode::serialize(&*model_guard) {
            if let Ok(mut file) = std::fs::File::create(&checkpoint_path) {
                use std::io::Write;
                let _ = file.write_all(&serialized);
            }
        }
    }

    pub fn predict(&self, features: &Array2<f64>) -> Result<Array2<f64>, &'static str> {
        let model_guard = self.model.read().map_err(|_| "Failed to acquire model lock")?;
        
        // Prepare input with proper shape
        let mut input = Array3::zeros((1, SEQUENCE_LENGTH, INPUT_FEATURES));
        let normalized = (features - features.mean().unwrap()) / (features.std(0.0) + 1e-8);
        
        for t in 0..SEQUENCE_LENGTH.min(normalized.shape()[0]) {
            for f in 0..INPUT_FEATURES {
                input[[0, t, f]] = normalized[[t, f]];
            }
        }
        
        // Get predictions
        let mut model_clone = model_guard.clone();
        let predictions = model_clone.forward(&input, false);
        
        // Extract final predictions and apply softmax for probabilities
        let final_pred = predictions.slice(s![0, -1, ..]).to_owned();
        let exp_pred = final_pred.mapv(f64::exp);
        let sum_exp = exp_pred.sum();
        let probabilities = exp_pred / sum_exp;
        
        Ok(probabilities.insert_axis(Axis(0)))
    }

    pub fn update_priority(&self, timestamp: Instant, new_priority: f64) {
        let mut buffer_guard = self.memory_buffer.write().unwrap();
        
        for data in buffer_guard.iter_mut() {
            if data.timestamp == timestamp {
                data.priority = new_priority;
                break;
            }
        }
    }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        self.performance_metrics.read().unwrap().clone()
    }

    pub fn pause_training(&self) {
        *self.training_active.write().unwrap() = false;
    }

    pub fn resume_training(&self) {
        *self.training_active.write().unwrap() = true;
    }

    pub fn export_model(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let model_guard = self.model.read()?;
        let serialized = bincode::serialize(&*model_guard)?;
        std::fs::write(path, serialized)?;
        Ok(())
    }

    pub fn import_model(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let data = std::fs::read(path)?;
        let imported_model: OnlineLSTMModel = bincode::deserialize(&data)?;
        *self.model.write()? = imported_model;
        Ok(())
    }

    pub fn add_training_data(&self, features: Array2<f64>, targets: Array2<f64>, priority: f64) {
        let data = TrainingData {
            features,
            targets,
            timestamp: Instant::now(),
            priority,
        };
        
        let mut buffer_guard = self.memory_buffer.write().unwrap();
        if buffer_guard.len() >= MAX_MEMORY_BUFFER {
            buffer_guard.pop_front();
        }
        buffer_guard.push_back(data);
    }
}

// Activation functions
fn sigmoid(x: &Array2<f64>) -> Array2<f64> {
    x.mapv(|val| 1.0 / (1.0 + (-val).exp()))
}

fn tanh(x: &Array2<f64>) -> Array2<f64> {
    x.mapv(|val| val.tanh())
}

fn clip_gradient(grad: &Array2<f64>) -> Array2<f64> {
    grad.mapv(|val| val.max(-GRADIENT_CLIP).min(GRADIENT_CLIP))
}

// Feature extraction for MEV opportunities
pub fn extract_mev_features(
    pool_state: &[f64; 8],
    market_depth: &[f64; 6],
    gas_price: f64,
    block_space: f64,
    recent_txs: &[f64; 8]
) -> Array2<f64> {
    let mut features = Array2::zeros((1, INPUT_FEATURES));
    
    // Pool state features (reserves, fees, volume)
    for i in 0..8 {
        features[[0, i]] = pool_state[i];
    }
    
    // Market depth indicators
    for i in 0..6 {
        features[[0, 8 + i]] = market_depth[i];
    }
    
    // Network conditions
    features[[0, 14]] = gas_price;
    features[[0, 15]] = block_space;
    
    // Recent transaction patterns
    for i in 0..8 {
        features[[0, 16 + i]] = recent_txs[i];
    }
    
    features
}

// MEV prediction targets
pub fn create_mev_targets(
    profit_potential: f64,
    success_probability: f64,
    optimal_gas_price: f64
) -> Array2<f64> {
    let mut targets = Array2::zeros((1, OUTPUT_SIZE));
    targets[[0, 0]] = profit_potential.tanh(); // Normalized profit
    targets[[0, 1]] = success_probability; // Success probability [0, 1]
    targets[[0, 2]] = (optimal_gas_price / 1e9).tanh(); // Normalized gas price
    targets
}

// High-performance batch prediction for multiple MEV opportunities
pub fn batch_predict_mev_opportunities(
    trainer: &OnlineLSTMTrainer,
    opportunities: &[Array2<f64>]
) -> Vec<(f64, f64, f64)> {
    opportunities.iter()
        .filter_map(|features| {
            if let Ok(pred) = trainer.predict(features) {
                Some((
                    pred[[0, 0]].atanh(), // Denormalize profit
                    pred[[0, 1]], // Success probability
                    pred[[0, 2]].atanh() * 1e9 // Denormalize gas price
                ))
            } else {
                None
            }
        })
        .collect()
}

#[derive(Clone)]
impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            total_updates: 0,
            avg_loss: 0.0,
            avg_accuracy: 0.0,
            last_checkpoint: Instant::now(),
            training_time_ms: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_online_training() {
        let trainer = OnlineLSTMTrainer::new();
        let (tx, rx) = mpsc::channel(100);
        
        trainer.start_online_training(rx).await;
        
        // Simulate MEV data
        for _ in 0..10 {
            let features = Array2::from_shape_fn((SEQUENCE_LENGTH, INPUT_FEATURES), |(_, _)| rand::random());
            let targets = Array2::from_shape_fn((1, OUTPUT_SIZE), |(_, _)| rand::random());
            
            tx.send(TrainingData {
                features,
                targets,
                timestamp: Instant::now(),
                priority: rand::random(),
            }).await.unwrap();
        }
        
        tokio::time::sleep(Duration::from_secs(1)).await;
        
        let metrics = trainer.get_performance_metrics();
        assert!(metrics.total_updates > 0);
    }
}

