use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use rand::Rng;
use serde::{Deserialize, Serialize};
use ndarray::{Array1, Array2, ArrayView1};
use anyhow::{Result, Context};
use tokio::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

const STATE_DIM: usize = 128;
const ACTION_DIM: usize = 21;
const HIDDEN_SIZE: usize = 256;
const BATCH_SIZE: usize = 256;
const BUFFER_CAPACITY: usize = 100000;
const GAMMA: f64 = 0.995;
const TAU: f64 = 0.001;
const LEARNING_RATE: f64 = 0.0001;
const EPSILON_START: f64 = 1.0;
const EPSILON_END: f64 = 0.01;
const EPSILON_DECAY: f64 = 0.9995;
const UPDATE_TARGET_EVERY: usize = 1000;
const MIN_BUFFER_SIZE: usize = 10000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketState {
    pub price: f64,
    pub volume_24h: f64,
    pub bid_ask_spread: f64,
    pub order_book_imbalance: f64,
    pub volatility: f64,
    pub momentum: f64,
    pub rsi: f64,
    pub macd: f64,
    pub bollinger_position: f64,
    pub vwap_deviation: f64,
    pub funding_rate: f64,
    pub open_interest: f64,
    pub liquidation_volume: f64,
    pub whale_activity: f64,
    pub network_congestion: f64,
    pub gas_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioState {
    pub token_balance: f64,
    pub sol_balance: f64,
    pub unrealized_pnl: f64,
    pub position_size: f64,
    pub avg_entry_price: f64,
    pub position_duration: f64,
    pub win_rate: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub current_drawdown: f64,
}

#[derive(Debug, Clone)]
pub struct Experience {
    state: Array1<f64>,
    action: usize,
    reward: f64,
    next_state: Array1<f64>,
    done: bool,
    timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct NeuralNetwork {
    weights_1: Array2<f64>,
    bias_1: Array1<f64>,
    weights_2: Array2<f64>,
    bias_2: Array1<f64>,
    weights_3: Array2<f64>,
    bias_3: Array1<f64>,
    advantage_weights: Array2<f64>,
    advantage_bias: Array1<f64>,
    value_weights: Array2<f64>,
    value_bias: Array1<f64>,
}

impl NeuralNetwork {
    fn new() -> Self {
        let mut rng = rand::thread_rng();
        
        let xavier_init = |fan_in: usize, fan_out: usize| -> f64 {
            (2.0 / (fan_in + fan_out) as f64).sqrt()
        };
        
        Self {
            weights_1: Array2::from_shape_fn((STATE_DIM, HIDDEN_SIZE), |_| {
                rng.gen_range(-1.0..1.0) * xavier_init(STATE_DIM, HIDDEN_SIZE)
            }),
            bias_1: Array1::zeros(HIDDEN_SIZE),
            weights_2: Array2::from_shape_fn((HIDDEN_SIZE, HIDDEN_SIZE), |_| {
                rng.gen_range(-1.0..1.0) * xavier_init(HIDDEN_SIZE, HIDDEN_SIZE)
            }),
            bias_2: Array1::zeros(HIDDEN_SIZE),
            weights_3: Array2::from_shape_fn((HIDDEN_SIZE, HIDDEN_SIZE), |_| {
                rng.gen_range(-1.0..1.0) * xavier_init(HIDDEN_SIZE, HIDDEN_SIZE)
            }),
            bias_3: Array1::zeros(HIDDEN_SIZE),
            advantage_weights: Array2::from_shape_fn((HIDDEN_SIZE, ACTION_DIM), |_| {
                rng.gen_range(-1.0..1.0) * xavier_init(HIDDEN_SIZE, ACTION_DIM)
            }),
            advantage_bias: Array1::zeros(ACTION_DIM),
            value_weights: Array2::from_shape_fn((HIDDEN_SIZE, 1), |_| {
                rng.gen_range(-1.0..1.0) * xavier_init(HIDDEN_SIZE, 1)
            }),
            value_bias: Array1::zeros(1),
        }
    }
    
    fn forward(&self, state: &ArrayView1<f64>) -> Array1<f64> {
        let h1 = self.relu(&(state.dot(&self.weights_1) + &self.bias_1));
        let h2 = self.relu(&(h1.dot(&self.weights_2) + &self.bias_2));
        let h3 = self.relu(&(h2.dot(&self.weights_3) + &self.bias_3));
        
        let advantages = h3.dot(&self.advantage_weights) + &self.advantage_bias;
        let value = h3.dot(&self.value_weights) + &self.value_bias;
        
        let mean_advantage = advantages.mean().unwrap();
        advantages - mean_advantage + value[0]
    }
    
    fn relu(&self, x: &Array1<f64>) -> Array1<f64> {
        x.mapv(|v| v.max(0.0))
    }
    
    fn update_weights(&mut self, other: &NeuralNetwork, tau: f64) {
        self.weights_1 = &self.weights_1 * (1.0 - tau) + &other.weights_1 * tau;
        self.bias_1 = &self.bias_1 * (1.0 - tau) + &other.bias_1 * tau;
        self.weights_2 = &self.weights_2 * (1.0 - tau) + &other.weights_2 * tau;
        self.bias_2 = &self.bias_2 * (1.0 - tau) + &other.bias_2 * tau;
        self.weights_3 = &self.weights_3 * (1.0 - tau) + &other.weights_3 * tau;
        self.bias_3 = &self.bias_3 * (1.0 - tau) + &other.bias_3 * tau;
        self.advantage_weights = &self.advantage_weights * (1.0 - tau) + &other.advantage_weights * tau;
        self.advantage_bias = &self.advantage_bias * (1.0 - tau) + &other.advantage_bias * tau;
        self.value_weights = &self.value_weights * (1.0 - tau) + &other.value_weights * tau;
        self.value_bias = &self.value_bias * (1.0 - tau) + &other.value_bias * tau;
    }
}

pub struct DQNStrategyRewardOptimizer {
    q_network: Arc<RwLock<NeuralNetwork>>,
    target_network: Arc<RwLock<NeuralNetwork>>,
    replay_buffer: Arc<Mutex<VecDeque<Experience>>>,
    epsilon: Arc<RwLock<f64>>,
    steps: Arc<RwLock<usize>>,
    optimizer_state: Arc<RwLock<OptimizerState>>,
    performance_tracker: Arc<RwLock<PerformanceTracker>>,
}

#[derive(Debug, Clone)]
struct OptimizerState {
    m_weights_1: Array2<f64>,
    v_weights_1: Array2<f64>,
    m_bias_1: Array1<f64>,
    v_bias_1: Array1<f64>,
    m_weights_2: Array2<f64>,
    v_weights_2: Array2<f64>,
    m_bias_2: Array1<f64>,
    v_bias_2: Array1<f64>,
    m_weights_3: Array2<f64>,
    v_weights_3: Array2<f64>,
    m_bias_3: Array1<f64>,
    v_bias_3: Array1<f64>,
    m_advantage_weights: Array2<f64>,
    v_advantage_weights: Array2<f64>,
    m_advantage_bias: Array1<f64>,
    v_advantage_bias: Array1<f64>,
    m_value_weights: Array2<f64>,
    v_value_weights: Array2<f64>,
    m_value_bias: Array1<f64>,
    v_value_bias: Array1<f64>,
    beta1_t: f64,
    beta2_t: f64,
}

impl OptimizerState {
    fn new() -> Self {
        Self {
            m_weights_1: Array2::zeros((STATE_DIM, HIDDEN_SIZE)),
            v_weights_1: Array2::zeros((STATE_DIM, HIDDEN_SIZE)),
            m_bias_1: Array1::zeros(HIDDEN_SIZE),
            v_bias_1: Array1::zeros(HIDDEN_SIZE),
            m_weights_2: Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE)),
            v_weights_2: Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE)),
            m_bias_2: Array1::zeros(HIDDEN_SIZE),
            v_bias_2: Array1::zeros(HIDDEN_SIZE),
            m_weights_3: Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE)),
            v_weights_3: Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE)),
            m_bias_3: Array1::zeros(HIDDEN_SIZE),
            v_bias_3: Array1::zeros(HIDDEN_SIZE),
            m_advantage_weights: Array2::zeros((HIDDEN_SIZE, ACTION_DIM)),
            v_advantage_weights: Array2::zeros((HIDDEN_SIZE, ACTION_DIM)),
            m_advantage_bias: Array1::zeros(ACTION_DIM),
            v_advantage_bias: Array1::zeros(ACTION_DIM),
            m_value_weights: Array2::zeros((HIDDEN_SIZE, 1)),
            v_value_weights: Array2::zeros((HIDDEN_SIZE, 1)),
            m_value_bias: Array1::zeros(1),
            v_value_bias: Array1::zeros(1),
            beta1_t: 0.9,
            beta2_t: 0.999,
        }
    }
}

#[derive(Debug, Clone)]
struct PerformanceTracker {
    total_rewards: Vec<f64>,
    episode_rewards: Vec<f64>,
    win_rate: f64,
    sharpe_ratio: f64,
    max_drawdown: f64,
    avg_reward: f64,
    trades_executed: usize,
    profitable_trades: usize,
}

impl DQNStrategyRewardOptimizer {
    pub async fn new() -> Result<Self> {
        let q_network = Arc::new(RwLock::new(NeuralNetwork::new()));
        let target_network = Arc::new(RwLock::new(q_network.read().await.clone()));
        
        Ok(Self {
            q_network,
            target_network,
            replay_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(BUFFER_CAPACITY))),
            epsilon: Arc::new(RwLock::new(EPSILON_START)),
            steps: Arc::new(RwLock::new(0)),
            optimizer_state: Arc::new(RwLock::new(OptimizerState::new())),
            performance_tracker: Arc::new(RwLock::new(PerformanceTracker {
                total_rewards: Vec::new(),
                episode_rewards: Vec::new(),
                win_rate: 0.0,
                sharpe_ratio: 0.0,
                max_drawdown: 0.0,
                avg_reward: 0.0,
                trades_executed: 0,
                profitable_trades: 0,
            })),
        })
    }
    
    pub async fn get_action(&self, state: Array1<f64>) -> Result<usize> {
        let epsilon = *self.epsilon.read().await;
        let mut rng = rand::thread_rng();
        
        if rng.gen::<f64>() < epsilon {
            Ok(rng.gen_range(0..ACTION_DIM))
        } else {
            let q_values = self.q_network.read().await.forward(&state.view());
            Ok(q_values.iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                .map(|(idx, _)| idx)
                .unwrap())
        }
    }
    
    pub async fn store_experience(
        &self,
        state: Array1<f64>,
        action: usize,
        reward: f64,
        next_state: Array1<f64>,
        done: bool,
    ) -> Result<()> {
        let mut buffer = self.replay_buffer.lock().unwrap();
        
        if buffer.len() >= BUFFER_CAPACITY {
            buffer.pop_front();
        }
        
        buffer.push_back(Experience {
            state,
            action,
            reward,
            next_state,
            done,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        });
        
        let mut steps = self.steps.write().await;
        *steps += 1;
        
        if *steps % UPDATE_TARGET_EVERY == 0 {
            let q_net = self.q_network.read().await;
            let mut target_net = self.target_network.write().await;
            target_net.update_weights(&q_net, TAU);
        }
        
        let mut epsilon = self.epsilon.write().await;
        *epsilon = (*epsilon * EPSILON_DECAY).max(EPSILON_END);
        
        Ok(())
    }
    
    pub async fn optimize(&self) -> Result<()> {
        let buffer = self.replay_buffer.lock().unwrap();
        if buffer.len() < MIN_BUFFER_SIZE {
            return Ok(());
        }
        
        let mut rng = rand::thread_rng();
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        
        for _ in 0..BATCH_SIZE {
            let idx = rng.gen_range(0..buffer.len());
            batch.push(buffer[idx].clone());
        }
        drop(buffer);
        
        let mut states = Array2::zeros((BATCH_SIZE, STATE_DIM));
        let mut next_states = Array2::zeros((BATCH_SIZE, STATE_DIM));
        let mut actions = Vec::with_capacity(BATCH_SIZE);
        let mut rewards = Vec::with_capacity(BATCH_SIZE);
        let mut dones = Vec::with_capacity(BATCH_SIZE);
        
        for (i, exp) in batch.iter().enumerate() {
            states.row_mut(i).assign(&exp.state);
            next_states.row_mut(i).assign(&exp.next_state);
            actions.push(exp.action);
            rewards.push(exp.reward);
            dones.push(exp.done);
        }
        
        let q_network = self.q_network.read().await;
        let target_network = self.target_network.read().await;
        
        let mut current_q_values = Vec::with_capacity(BATCH_SIZE);
        let mut next_q_values = Vec::with_capacity(BATCH_SIZE);
        let mut next_q_values_current = Vec::with_capacity(BATCH_SIZE);
        
        for i in 0..BATCH_SIZE {
            let state_view = states.row(i);
            let next_state_view = next_states.row(i);
            
            current_q_values.push(q_network.forward(&state_view));
            next_q_values.push(target_network.forward(&next_state_view));
            next_q_values_current.push(q_network.forward(&next_state_view));
        }
        
        drop(target_network);
        drop(q_network);
        
        let mut targets = Vec::with_capacity(BATCH_SIZE);
        for i in 0..BATCH_SIZE {
            let best_action = next_q_values_current[i].iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                .map(|(idx, _)| idx)
                .unwrap();
            
            let target = if dones[i] {
                rewards[i]
            } else {
                rewards[i] + GAMMA * next_q_values[i][best_action]
            };
            targets.push(target);
        }
        
        self.update_network(states, actions, targets, current_q_values).await?;
        
        Ok(())
    }
    
    async fn update_network(
        &self,
        states: Array2<f64>,
        actions: Vec<usize>,
        targets: Vec<f64>,
        predictions: Vec<Array1<f64>>,
    ) -> Result<()> {
        let mut q_network = self.q_network.write().await;
        let mut optimizer_state = self.optimizer_state.write().await;
        
        let beta1 = 0.9;
        let beta2 = 0.999;
        let epsilon = 1e-8;
        
        optimizer_state.beta1_t *= beta1;
        optimizer_state.beta2_t *= beta2;
        
        let mut grad_weights_1 = Array2::zeros((STATE_DIM, HIDDEN_SIZE));
        let mut grad_bias_1 = Array1::zeros(HIDDEN_SIZE);
        let mut grad_weights_2 = Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE));
        let mut grad_bias_2 = Array1::zeros(HIDDEN_SIZE);
        let mut grad_weights_3 = Array2::zeros((HIDDEN_SIZE, HIDDEN_SIZE));
        let mut grad_bias_3 = Array1::zeros(HIDDEN_SIZE);
        let mut grad_advantage_weights = Array2::zeros((HIDDEN_SIZE, ACTION_DIM));
        let mut grad_advantage_bias = Array1::zeros(ACTION_DIM);
        let mut grad_value_weights = Array2::zeros((HIDDEN_SIZE, 1));
        let mut grad_value_bias = Array1::zeros(1);
        
        for i in 0..BATCH_SIZE {
            let state = states.row(i);
            let action = actions[i];
            let target = targets[i];
            let prediction = predictions[i][action];
            
            let loss_grad = 2.0 * (prediction - target) / BATCH_SIZE as f64;
            
            let h1 = q_network.relu(&(state.dot(&q_network.weights_1) + &q_network.bias_1));
            let h2 = q_network.relu(&(h1.dot(&q_network.weights_2) + &q_network.bias_2));
            let h3 = q_network.relu(&(h2.dot(&q_network.weights_3) + &q_network.bias_3));
            
            let mut advantage_grad = Array1::zeros(ACTION_DIM);
            advantage_grad[action] = loss_grad;
            
            let value_grad = loss_grad / ACTION_DIM as f64;
            
            for j in 0..HIDDEN_SIZE {
                for k in 0..ACTION_DIM {
                    grad_advantage_weights[[j, k]] += h3[j] * advantage_grad[k];
                }
                grad_value_weights[[j, 0]] += h3[j] * value_grad;
            }
            
            grad_advantage_bias = &grad_advantage_bias + &advantage_grad;
            grad_value_bias[0] += value_grad;
            
            let h3_grad = advantage_grad.dot(&q_network.advantage_weights.t()) + 
                         value_grad * &q_network.value_weights.column(0);
            let h3_grad = h3_grad * &h3.mapv(|x| if x > 0.0 { 1.0 } else { 0.0 });
            
            for j in 0..HIDDEN_SIZE {
                for k in 0..HIDDEN_SIZE {
                    grad_weights_3[[j, k]] += h2[j] * h3_grad[k];
                }
            }
            grad_bias_3 = &grad_bias_3 + &h3_grad;
            
            let h2_grad = h3_grad.dot(&q_network.weights_3.t());
            let h2_grad = h2_grad * &h2.mapv(|x| if x > 0.0 { 1.0 } else { 0.0 });
            
            for j in 0..HIDDEN_SIZE {
                for k in 0..HIDDEN_SIZE {
                    grad_weights_2[[j, k]] += h1[j] * h2_grad[k];
                }
            }
            grad_bias_2 = &grad_bias_2 + &h2_grad;
            
            let h1_grad = h2_grad.dot(&q_network.weights_2.t());
            let h1_grad = h1_grad * &h1.mapv(|x| if x > 0.0 { 1.0 } else { 0.0 });
            
            for j in 0..STATE_DIM {
                for k in 0..HIDDEN_SIZE {
                    grad_weights_1[[j, k]] += state[j] * h1_grad[k];
                }
            }
            grad_bias_1 = &grad_bias_1 + &h1_grad;
        }
        
        self.adam_update(&mut q_network.weights_1, &grad_weights_1, 
                        &mut optimizer_state.m_weights_1, &mut optimizer_state.v_weights_1, 
                        optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update_1d(&mut q_network.bias_1, &grad_bias_1,
                           &mut optimizer_state.m_bias_1, &mut optimizer_state.v_bias_1,
                           optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update(&mut q_network.weights_2, &grad_weights_2,
                        &mut optimizer_state.m_weights_2, &mut optimizer_state.v_weights_2,
                        optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update_1d(&mut q_network.bias_2, &grad_bias_2,
                           &mut optimizer_state.m_bias_2, &mut optimizer_state.v_bias_2,
                           optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update(&mut q_network.weights_3, &grad_weights_3,
                        &mut optimizer_state.m_weights_3, &mut optimizer_state.v_weights_3,
                        optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update_1d(&mut q_network.bias_3, &grad_bias_3,
                           &mut optimizer_state.m_bias_3, &mut optimizer_state.v_bias_3,
                           optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update(&mut q_network.advantage_weights, &grad_advantage_weights,
                        &mut optimizer_state.m_advantage_weights, &mut optimizer_state.v_advantage_weights,
                        optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update_1d(&mut q_network.advantage_bias, &grad_advantage_bias,
                           &mut optimizer_state.m_advantage_bias, &mut optimizer_state.v_advantage_bias,
                           optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update(&mut q_network.value_weights, &grad_value_weights,
                        &mut optimizer_state.m_value_weights, &mut optimizer_state.v_value_weights,
                        optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        self.adam_update_1d(&mut q_network.value_bias, &grad_value_bias,
                           &mut optimizer_state.m_value_bias, &mut optimizer_state.v_value_bias,
                           optimizer_state.beta1_t, optimizer_state.beta2_t, beta1, beta2, epsilon);
        
        Ok(())
    }
    
    fn adam_update(
        &self,
        param: &mut Array2<f64>,
        grad: &Array2<f64>,
        m: &mut Array2<f64>,
        v: &mut Array2<f64>,
        beta1_t: f64,
        beta2_t: f64,
        beta1: f64,
        beta2: f64,
        epsilon: f64,
    ) {
        *m = beta1 * &*m + (1.0 - beta1) * grad;
        *v = beta2 * &*v + (1.0 - beta2) * grad.mapv(|x| x * x);
        
        let m_hat = &*m / (1.0 - beta1_t);
        let v_hat = &*v / (1.0 - beta2_t);
        
        *param = &*param - LEARNING_RATE * &m_hat / (v_hat.mapv(|x| x.sqrt()) + epsilon);
    }
    
    fn adam_update_1d(
        &self,
        param: &mut Array1<f64>,
        grad: &Array1<f64>,
        m: &mut Array1<f64>,
        v: &mut Array1<f64>,
        beta1_t: f64,
        beta2_t: f64,
        beta1: f64,
        beta2: f64,
        epsilon: f64,
    ) {
        *m = beta1 * &*m + (1.0 - beta1) * grad;
        *v = beta2 * &*v + (1.0 - beta2) * grad.mapv(|x| x * x);
        
        let m_hat = &*m / (1.0 - beta1_t);
        let v_hat = &*v / (1.0 - beta2_t);
        
        *param = &*param - LEARNING_RATE * &m_hat / (v_hat.mapv(|x| x.sqrt()) + epsilon);
    }
    
    pub async fn encode_state(
        &self,
        market: &MarketState,
        portfolio: &PortfolioState,
        historical_prices: &[f64],
        historical_volumes: &[f64],
    ) -> Result<Array1<f64>> {
        let mut state = Array1::zeros(STATE_DIM);
        let mut idx = 0;
        
        state[idx] = self.normalize_price(market.price);
        state[idx + 1] = self.normalize_volume(market.volume_24h);
        state[idx + 2] = market.bid_ask_spread.tanh();
        state[idx + 3] = market.order_book_imbalance.tanh();
        state[idx + 4] = self.normalize_volatility(market.volatility);
        state[idx + 5] = market.momentum.tanh();
        state[idx + 6] = (market.rsi - 50.0) / 50.0;
        state[idx + 7] = market.macd.tanh();
        state[idx + 8] = market.bollinger_position.tanh();
        state[idx + 9] = market.vwap_deviation.tanh();
        state[idx + 10] = market.funding_rate.tanh();
        state[idx + 11] = self.normalize_volume(market.open_interest);
        state[idx + 12] = self.normalize_volume(market.liquidation_volume);
        state[idx + 13] = market.whale_activity.tanh();
        state[idx + 14] = market.network_congestion.tanh();
        state[idx + 15] = self.normalize_gas_price(market.gas_price);
        idx += 16;
        
        state[idx] = self.normalize_balance(portfolio.token_balance);
        state[idx + 1] = self.normalize_balance(portfolio.sol_balance);
        state[idx + 2] = portfolio.unrealized_pnl.tanh();
        state[idx + 3] = self.normalize_position_size(portfolio.position_size);
        state[idx + 4] = self.normalize_price(portfolio.avg_entry_price);
        state[idx + 5] = (portfolio.position_duration / 86400.0).tanh();
        state[idx + 6] = portfolio.win_rate * 2.0 - 1.0;
        state[idx + 7] = portfolio.sharpe_ratio.tanh();
        state[idx + 8] = portfolio.max_drawdown;
        state[idx + 9] = portfolio.current_drawdown;
        idx += 10;
        
        let price_features = self.extract_price_features(historical_prices);
        let volume_features = self.extract_volume_features(historical_volumes);
        
        for i in 0..price_features.len().min(50) {
            state[idx + i] = price_features[i];
        }
        idx += 50;
        
        for i in 0..volume_features.len().min(STATE_DIM - idx) {
            state[idx + i] = volume_features[i];
        }
        
        Ok(state)
    }
    
    pub fn calculate_reward(
        &self,
        pnl: f64,
        trade_cost: f64,
        slippage: f64,
        position_held_time: f64,
        market_impact: f64,
        risk_adjusted_return: f64,
    ) -> f64 {
        let base_reward = pnl - trade_cost - slippage * 2.0;
        
        let time_penalty = if position_held_time < 60.0 {
            -0.01 * (60.0 - position_held_time) / 60.0
        } else if position_held_time > 3600.0 {
            -0.005 * (position_held_time - 3600.0) / 3600.0
        } else {
            0.0
        };
        
        let market_impact_penalty = -market_impact.abs() * 0.5;
        
        let risk_bonus = if risk_adjusted_return > 2.0 {
            0.1 * (risk_adjusted_return - 2.0).min(1.0)
        } else if risk_adjusted_return < 0.5 {
            -0.2 * (0.5 - risk_adjusted_return)
        } else {
            0.0
        };
        
        let sharpe_multiplier = (1.0 + risk_adjusted_return.tanh() * 0.5).max(0.1);
        
        (base_reward + time_penalty + market_impact_penalty + risk_bonus) * sharpe_multiplier
    }
    
    pub fn map_action_to_trade(&self, action: usize, current_position: f64, balance: f64) -> (f64, bool) {
        match action {
            0..=6 => {
                let position_pct = (action as f64 + 1.0) / 10.0;
                let target_position = balance * position_pct;
                (target_position - current_position, true)
            }
            7..=13 => {
                let position_pct = (action as f64 - 6.0) / 10.0;
                let target_position = -balance * position_pct;
                (target_position - current_position, false)
            }
            14..=19 => {
                let reduce_pct = (action as f64 - 13.0) / 10.0;
                (-current_position * reduce_pct, current_position > 0.0)
            }
            _ => (0.0, true),
        }
    }
    
    pub async fn update_performance(&self, reward: f64, trade_profitable: bool) -> Result<()> {
        let mut tracker = self.performance_tracker.write().await;
        
        tracker.episode_rewards.push(reward);
        tracker.total_rewards.push(reward);
        
        if tracker.total_rewards.len() > 10000 {
            tracker.total_rewards.remove(0);
        }
        
        tracker.trades_executed += 1;
        if trade_profitable {
            tracker.profitable_trades += 1;
        }
        
        tracker.win_rate = tracker.profitable_trades as f64 / tracker.trades_executed as f64;
        
        if tracker.episode_rewards.len() >= 100 {
            let returns = &tracker.episode_rewards[tracker.episode_rewards.len()-100..];
            let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
            let std_return = (returns.iter()
                .map(|r| (r - mean_return).powi(2))
                .sum::<f64>() / returns.len() as f64)
                .sqrt();
            
            tracker.sharpe_ratio = if std_return > 0.0 {
                (mean_return * (252.0_f64).sqrt()) / std_return
            } else {
                0.0
            };
            
            let mut cumulative = 0.0;
            let mut peak = 0.0;
            let mut max_dd = 0.0;
            
            for r in returns {
                cumulative += r;
                peak = peak.max(cumulative);
                let dd = (peak - cumulative) / peak.max(1.0);
                max_dd = max_dd.max(dd);
            }
            
            tracker.max_drawdown = max_dd;
        }
        
        tracker.avg_reward = if tracker.total_rewards.is_empty() {
            0.0
        } else {
            tracker.total_rewards.iter().sum::<f64>() / tracker.total_rewards.len() as f64
        };
        
        Ok(())
    }
    
    pub async fn get_performance_metrics(&self) -> Result<(f64, f64, f64, f64)> {
        let tracker = self.performance_tracker.read().await;
        Ok((
            tracker.win_rate,
            tracker.sharpe_ratio,
            tracker.max_drawdown,
            tracker.avg_reward,
        ))
    }
    
    pub async fn should_trade(&self, confidence: f64, market_conditions: &MarketState) -> bool {
        let min_confidence = 0.65;
        let max_spread = 0.002;
        let min_volume = 100000.0;
        let max_volatility = 0.05;
        
        confidence > min_confidence &&
        market_conditions.bid_ask_spread < max_spread &&
        market_conditions.volume_24h > min_volume &&
        market_conditions.volatility < max_volatility &&
        market_conditions.network_congestion < 0.8
    }
    
    pub async fn get_model_confidence(&self, state: &Array1<f64>) -> Result<f64> {
        let q_network = self.q_network.read().await;
        let q_values = q_network.forward(&state.view());
        
        let max_q = q_values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let min_q = q_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        
        let q_range = max_q - min_q;
        let q_std = (q_values.iter()
            .map(|&q| (q - q_values.mean().unwrap()).powi(2))
            .sum::<f64>() / q_values.len() as f64)
            .sqrt();
        
        let confidence = if q_range > 0.0 {
            let normalized_range = (q_range / (max_q.abs() + min_q.abs() + 1e-8)).min(1.0);
            let normalized_std = (q_std / (q_range + 1e-8)).min(1.0);
            (normalized_range * 0.7 + (1.0 - normalized_std) * 0.3).min(1.0).max(0.0)
        } else {
            0.0
        };
        
        Ok(confidence)
    }
    
    fn normalize_price(&self, price: f64) -> f64 {
        ((price + 1e-8).ln() / 10.0).tanh()
    }
    
    fn normalize_volume(&self, volume: f64) -> f64 {
        ((volume + 1.0).ln() / 20.0).tanh()
    }
    
    fn normalize_volatility(&self, volatility: f64) -> f64 {
        (volatility * 100.0).tanh()
    }
    
    fn normalize_gas_price(&self, gas_price: f64) -> f64 {
        ((gas_price / 1e9).ln() / 5.0).tanh()
    }
    
    fn normalize_balance(&self, balance: f64) -> f64 {
        ((balance + 1.0).ln() / 10.0).tanh()
    }
    
    fn normalize_position_size(&self, size: f64) -> f64 {
        (size / 100000.0).tanh()
    }
    
    fn extract_price_features(&self, prices: &[f64]) -> Vec<f64> {
        let mut features = Vec::new();
        
        if prices.len() < 2 {
            return vec![0.0; 50];
        }
        
        let returns: Vec<f64> = prices.windows(2)
            .map(|w| (w[1] / w[0] - 1.0).tanh())
            .collect();
        
        for i in &[1, 5, 10, 20, 50] {
            if returns.len() >= *i {
                let mean_return = returns[returns.len()-i..].iter().sum::<f64>() / *i as f64;
                features.push(mean_return);
            } else {
                features.push(0.0);
            }
        }
        
        for i in &[5, 10, 20, 50] {
            if prices.len() >= *i {
                let sma = prices[prices.len()-i..].iter().sum::<f64>() / *i as f64;
                let current_price = prices.last().unwrap();
                features.push(((current_price / sma) - 1.0).tanh());
            } else {
                features.push(0.0);
            }
        }
        
        if prices.len() >= 20 {
            let mut sorted_prices: Vec<f64> = prices[prices.len()-20..].to_vec();
            sorted_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let median = sorted_prices[10];
            features.push(((prices.last().unwrap() / median) - 1.0).tanh());
        } else {
            features.push(0.0);
        }
        
        while features.len() < 50 {
            features.push(0.0);
        }
        
        features
    }
    
    fn extract_volume_features(&self, volumes: &[f64]) -> Vec<f64> {
        let mut features = Vec::new();
        
        if volumes.is_empty() {
            return vec![0.0; 52];
        }
        
        for i in &[1, 5, 10, 20] {
            if volumes.len() >= *i {
                let avg_volume = volumes[volumes.len()-i..].iter().sum::<f64>() / *i as f64;
                features.push(self.normalize_volume(avg_volume));
            } else {
                features.push(0.0);
            }
        }
        
        if volumes.len() >= 20 {
            let current_volume = volumes.last().unwrap();
            let avg_volume = volumes.iter().sum::<f64>() / volumes.len() as f64;
            features.push(((current_volume / (avg_volume + 1e-8)) - 1.0).tanh());
            
            let max_volume = volumes.iter().fold(0.0, |a, &b| a.max(b));
            features.push((current_volume / (max_volume + 1e-8)).tanh());
        } else {
            features.push(0.0);
            features.push(0.0);
        }
        
        while features.len() < 52 {
            features.push(0.0);
        }
        
        features
    }
    
    pub async fn save_checkpoint(&self, path: &str) -> Result<()> {
        let network = self.q_network.read().await;
        let checkpoint_data = serde_json::json!({
            "epsilon": *self.epsilon.read().await,
            "steps": *self.steps.read().await,
            "performance": {
                "win_rate": self.performance_tracker.read().await.win_rate,
                "sharpe_ratio": self.performance_tracker.read().await.sharpe_ratio,
                "max_drawdown": self.performance_tracker.read().await.max_drawdown,
            }
        });
        
        std::fs::write(path, serde_json::to_string_pretty(&checkpoint_data)?)?;
        Ok(())
    }
}
