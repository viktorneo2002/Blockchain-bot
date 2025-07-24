use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketState {
    pub token_mint: Pubkey,
    pub price: u64,
    pub volume_24h: u64,
    pub liquidity: u64,
    pub momentum: i32,
    pub volatility: u32,
    pub spread_bps: u16,
    pub block_height: u64,
    pub timestamp: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradingAction {
    Buy { amount: u64, slippage_bps: u16 },
    Sell { amount: u64, slippage_bps: u16 },
    Hold,
    AddLiquidity { amount: u64 },
    RemoveLiquidity { amount: u64 },
}

#[derive(Clone, Debug)]
pub struct MCTSNode {
    state: MarketState,
    action: Option<TradingAction>,
    parent: Option<usize>,
    children: Vec<usize>,
    visits: u64,
    total_reward: f64,
    untried_actions: Vec<TradingAction>,
    is_terminal: bool,
}

pub struct MCTSConfig {
    pub exploration_constant: f64,
    pub simulation_depth: usize,
    pub max_iterations: usize,
    pub time_limit_ms: u64,
    pub min_visits_threshold: u64,
    pub discount_factor: f64,
    pub volatility_weight: f64,
    pub momentum_weight: f64,
    pub liquidity_weight: f64,
}

impl Default for MCTSConfig {
    fn default() -> Self {
        Self {
            exploration_constant: 1.414,
            simulation_depth: 20,
            max_iterations: 10000,
            time_limit_ms: 50,
            min_visits_threshold: 10,
            discount_factor: 0.99,
            volatility_weight: 0.3,
            momentum_weight: 0.4,
            liquidity_weight: 0.3,
        }
    }
}

pub struct MonteCarloTreeSearcher {
    nodes: Vec<MCTSNode>,
    config: MCTSConfig,
    rng: ThreadRng,
    state_cache: HashMap<MarketState, Vec<f64>>,
}

impl MonteCarloTreeSearcher {
    pub fn new(config: MCTSConfig) -> Self {
        Self {
            nodes: Vec::with_capacity(100000),
            config,
            rng: thread_rng(),
            state_cache: HashMap::new(),
        }
    }

    pub fn search(&mut self, initial_state: MarketState) -> TradingAction {
        self.nodes.clear();
        self.state_cache.clear();
        
        let root_node = MCTSNode {
            state: initial_state.clone(),
            action: None,
            parent: None,
            children: Vec::new(),
            visits: 0,
            total_reward: 0.0,
            untried_actions: self.get_legal_actions(&initial_state),
            is_terminal: false,
        };
        
        self.nodes.push(root_node);
        let start_time = Instant::now();
        let mut iterations = 0;
        
        while iterations < self.config.max_iterations 
            && start_time.elapsed().as_millis() < self.config.time_limit_ms as u128 {
            
            let leaf_idx = self.tree_policy(0);
            let reward = self.simulate(leaf_idx);
            self.backpropagate(leaf_idx, reward);
            
            iterations += 1;
        }
        
        self.get_best_action(0)
    }

    fn tree_policy(&mut self, node_idx: usize) -> usize {
        let mut current_idx = node_idx;
        
        while !self.nodes[current_idx].is_terminal {
            if !self.nodes[current_idx].untried_actions.is_empty() {
                return self.expand(current_idx);
            } else if !self.nodes[current_idx].children.is_empty() {
                current_idx = self.best_child(current_idx, self.config.exploration_constant);
            } else {
                break;
            }
        }
        
        current_idx
    }

    fn expand(&mut self, node_idx: usize) -> usize {
        let action_idx = self.rng.gen_range(0..self.nodes[node_idx].untried_actions.len());
        let action = self.nodes[node_idx].untried_actions.swap_remove(action_idx);
        
        let new_state = self.apply_action(&self.nodes[node_idx].state, &action);
        let new_node = MCTSNode {
            state: new_state.clone(),
            action: Some(action),
            parent: Some(node_idx),
            children: Vec::new(),
            visits: 0,
            total_reward: 0.0,
            untried_actions: self.get_legal_actions(&new_state),
            is_terminal: self.is_terminal_state(&new_state),
        };
        
        let new_idx = self.nodes.len();
        self.nodes.push(new_node);
        self.nodes[node_idx].children.push(new_idx);
        
        new_idx
    }

    fn best_child(&self, node_idx: usize, exploration: f64) -> usize {
        let node = &self.nodes[node_idx];
        let ln_parent_visits = (node.visits as f64).ln();
        
        node.children.iter()
            .max_by(|&&a, &&b| {
                let child_a = &self.nodes[a];
                let child_b = &self.nodes[b];
                
                let ucb_a = self.calculate_ucb(child_a, ln_parent_visits, exploration);
                let ucb_b = self.calculate_ucb(child_b, ln_parent_visits, exploration);
                
                ucb_a.partial_cmp(&ucb_b).unwrap()
            })
            .copied()
            .unwrap()
    }

    fn calculate_ucb(&self, node: &MCTSNode, ln_parent_visits: f64, exploration: f64) -> f64 {
        if node.visits == 0 {
            f64::INFINITY
        } else {
            let exploitation = node.total_reward / node.visits as f64;
            let exploration_term = exploration * (ln_parent_visits / node.visits as f64).sqrt();
            exploitation + exploration_term
        }
    }

    fn simulate(&mut self, node_idx: usize) -> f64 {
        let mut state = self.nodes[node_idx].state.clone();
        let mut total_reward = 0.0;
        let mut discount = 1.0;
        
        for _ in 0..self.config.simulation_depth {
            if self.is_terminal_state(&state) {
                break;
            }
            
            let actions = self.get_legal_actions(&state);
            if actions.is_empty() {
                break;
            }
            
            let action = self.select_simulation_action(&state, &actions);
            let reward = self.evaluate_action(&state, &action);
            total_reward += discount * reward;
            discount *= self.config.discount_factor;
            
            state = self.apply_action(&state, &action);
        }
        
        total_reward
    }

    fn backpropagate(&mut self, mut node_idx: usize, reward: f64) {
        while let Some(node) = self.nodes.get_mut(node_idx) {
            node.visits += 1;
            node.total_reward += reward;
            
            if let Some(parent_idx) = node.parent {
                node_idx = parent_idx;
            } else {
                break;
            }
        }
    }

    fn get_best_action(&self, node_idx: usize) -> TradingAction {
        let node = &self.nodes[node_idx];
        
        node.children.iter()
            .filter(|&&child_idx| self.nodes[child_idx].visits >= self.config.min_visits_threshold)
            .max_by_key(|&&child_idx| {
                let child = &self.nodes[child_idx];
                let win_rate = if child.visits > 0 {
                    (child.total_reward / child.visits as f64 * 10000.0) as i64
                } else {
                    0
                };
                (child.visits as i64, win_rate)
            })
            .and_then(|&child_idx| self.nodes[child_idx].action.clone())
            .unwrap_or(TradingAction::Hold)
    }

    fn get_legal_actions(&self, state: &MarketState) -> Vec<TradingAction> {
        let mut actions = vec![TradingAction::Hold];
        
        let base_amount = state.liquidity / 100;
        let amounts = vec![
            base_amount / 10,
            base_amount / 4,
            base_amount / 2,
            base_amount,
        ];
        
        let slippages = vec![10, 25, 50, 100];
        
        for &amount in &amounts {
            if amount > 0 && amount < state.liquidity / 2 {
                for &slippage in &slippages {
                    actions.push(TradingAction::Buy { amount, slippage_bps: slippage });
                    actions.push(TradingAction::Sell { amount, slippage_bps: slippage });
                }
            }
        }
        
        if state.volatility > 50 && state.liquidity > 1000000 {
            actions.push(TradingAction::AddLiquidity { amount: base_amount / 5 });
        }
        
        actions
    }

    fn apply_action(&self, state: &MarketState, action: &TradingAction) -> MarketState {
        let mut new_state = state.clone();
        
        match action {
            TradingAction::Buy { amount, slippage_bps } => {
                let impact = (*amount as f64 / state.liquidity as f64).min(0.1);
                let price_impact = 1.0 + impact * (1.0 + *slippage_bps as f64 / 10000.0);
                new_state.price = (state.price as f64 * price_impact) as u64;
                new_state.volume_24h = state.volume_24h.saturating_add(*amount);
                new_state.momentum = (state.momentum + (impact * 100.0) as i32).min(100);
                new_state.volatility = (state.volatility as f64 * 1.05) as u32;
            },
            TradingAction::Sell { amount, slippage_bps } => {
                let impact = (*amount as f64 / state.liquidity as f64).min(0.1);
                let price_impact = 1.0 - impact * (1.0 + *slippage_bps as f64 / 10000.0);
                new_state.price = (state.price as f64 * price_impact.max(0.5)) as u64;
                new_state.volume_24h = state.volume_24h.saturating_add(*amount);
                new_state.momentum = (state.momentum - (impact * 100.0) as i32).max(-100);
                new_state.volatility = (state.volatility as f64 * 1.05) as u32;
            },
            TradingAction::AddLiquidity { amount } => {
                new_state.liquidity = state.liquidity.saturating_add(*amount);
                new_state.volatility = (state.volatility as f64 * 0.95) as u32;
                new_state.spread_bps = (state.spread_bps as f64 * 0.9) as u16;
            },
            TradingAction::RemoveLiquidity { amount } => {
                new_state.liquidity = state.liquidity.saturating_sub(*amount);
                new_state.volatility = (state.volatility as f64 * 1.1) as u32;
                new_state.spread_bps = (state.spread_bps as f64 * 1.1) as u16;
            },
            TradingAction::Hold => {},
        }
        
        new_state.block_height = state.block_height + 1;
        new_state.timestamp = state.timestamp + 400;
        
        new_state
    }

    fn evaluate_action(&mut self, state: &MarketState, action: &TradingAction) -> f64 {
        let cache_key = state.clone();
        if let Some(cached_rewards) = self.state_cache.get(&cache_key) {
            if let Some(&reward) = cached_rewards.get(action_hash(action)) {
                return reward;
            }
        }
        
        let mut reward = 0.0;
        
  match action {
            TradingAction::Buy { amount, slippage_bps } => {
                let impact = (*amount as f64 / state.liquidity as f64).min(0.1);
                let momentum_score = (state.momentum as f64 / 100.0) * self.config.momentum_weight;
                let volatility_score = (state.volatility as f64 / 100.0) * self.config.volatility_weight;
                let liquidity_score = (state.liquidity as f64 / 10000000.0).min(1.0) * self.config.liquidity_weight;
                let slippage_penalty = *slippage_bps as f64 / 10000.0;
                
                reward = momentum_score + volatility_score + liquidity_score - slippage_penalty - impact;
                
                if state.momentum > 50 && state.volatility < 30 {
                    reward *= 1.5;
                }
            },
            TradingAction::Sell { amount, slippage_bps } => {
                let impact = (*amount as f64 / state.liquidity as f64).min(0.1);
                let momentum_score = (-state
            TradingAction::Sell { amount, slippage_bps } => {
                let impact = (*amount as f64 / state.liquidity as f64).min(0.1);
                let momentum_score = (-state.momentum as f64 / 100.0) * self.config.momentum_weight;
                let volatility_score = (state.volatility as f64 / 100.0) * self.config.volatility_weight;
                let liquidity_score = (state.liquidity as f64 / 10000000.0).min(1.0) * self.config.liquidity_weight;
                let slippage_penalty = *slippage_bps as f64 / 10000.0;
                
                reward = momentum_score + volatility_score * 0.5 + liquidity_score - slippage_penalty - impact;
                
                if state.momentum < -50 && state.volatility > 70 {
                    reward *= 1.8;
                }
                
                if state.spread_bps > 100 {
                    reward *= 1.2;
                }
            },
            TradingAction::AddLiquidity { amount } => {
                let liquidity_ratio = *amount as f64 / state.liquidity as f64;
                let fee_potential = (state.volume_24h as f64 * 0.003) / state.liquidity as f64;
                let volatility_bonus = if state.volatility > 50 { 0.2 } else { 0.0 };
                
                reward = fee_potential * liquidity_ratio + volatility_bonus;
                
                if state.liquidity < 1000000 {
                    reward *= 2.0;
                }
            },
            TradingAction::RemoveLiquidity { amount } => {
                let liquidity_ratio = *amount as f64 / state.liquidity as f64;
                let impermanent_loss_risk = (state.volatility as f64 / 100.0) * liquidity_ratio;
                
                reward = -impermanent_loss_risk;
                
                if state.momentum.abs() > 80 {
                    reward -= 0.5;
                }
            },
            TradingAction::Hold => {
                reward = 0.0;
                
                if state.volatility > 80 || state.spread_bps > 200 {
                    reward = 0.1;
                }
            },
        }
        
        let spread_adjustment = (state.spread_bps as f64 / 10000.0).min(0.02);
        reward = reward * (1.0 - spread_adjustment);
        
        self.state_cache.entry(cache_key)
            .or_insert_with(|| vec![0.0; 32])
            [action_hash(action)] = reward;
        
        reward
    }

    fn select_simulation_action(&mut self, state: &MarketState, actions: &[TradingAction]) -> TradingAction {
        let mut best_action = TradingAction::Hold;
        let mut best_score = f64::NEG_INFINITY;
        
        let exploration_noise = self.rng.gen::<f64>() * 0.1;
        
        for action in actions {
            let base_score = self.evaluate_action(state, action);
            let noise = self.rng.gen::<f64>() * exploration_noise;
            let score = base_score + noise;
            
            if score > best_score {
                best_score = score;
                best_action = action.clone();
            }
        }
        
        if self.rng.gen::<f64>() < 0.05 {
            actions[self.rng.gen_range(0..actions.len())].clone()
        } else {
            best_action
        }
    }

    fn is_terminal_state(&self, state: &MarketState) -> bool {
        state.liquidity < 1000 || 
        state.price == 0 || 
        state.price > 1_000_000_000_000 ||
        state.volatility > 200 ||
        state.momentum.abs() >= 100
    }

    pub fn get_action_confidence(&self, node_idx: usize) -> HashMap<TradingAction, f64> {
        let node = &self.nodes[node_idx];
        let mut confidences = HashMap::new();
        
        for &child_idx in &node.children {
            let child = &self.nodes[child_idx];
            if let Some(action) = &child.action {
                let confidence = if child.visits > 0 {
                    (child.total_reward / child.visits as f64 + 1.0) / 2.0
                } else {
                    0.5
                };
                confidences.insert(action.clone(), confidence);
            }
        }
        
        confidences
    }

    pub fn get_expected_value(&self, node_idx: usize) -> f64 {
        let node = &self.nodes[node_idx];
        if node.visits > 0 {
            node.total_reward / node.visits as f64
        } else {
            0.0
        }
    }
}

fn action_hash(action: &TradingAction) -> usize {
    match action {
        TradingAction::Buy { amount, slippage_bps } => {
            ((*amount as usize) ^ (*slippage_bps as usize)) % 32
        },
        TradingAction::Sell { amount, slippage_bps } => {
            (((*amount as usize) ^ (*slippage_bps as usize)) + 8) % 32
        },
        TradingAction::Hold => 16,
        TradingAction::AddLiquidity { amount } => {
            ((*amount as usize) + 20) % 32
        },
        TradingAction::RemoveLiquidity { amount } => {
            ((*amount as usize) + 24) % 32
        },
    }
}

#[derive(Clone)]
pub struct ParallelMCTS {
    searchers: Vec<Arc<Mutex<MonteCarloTreeSearcher>>>,
    thread_count: usize,
}

impl ParallelMCTS {
    pub fn new(config: MCTSConfig, thread_count: usize) -> Self {
        let searchers = (0..thread_count)
            .map(|_| Arc::new(Mutex::new(MonteCarloTreeSearcher::new(config.clone()))))
            .collect();
        
        Self {
            searchers,
            thread_count,
        }
    }

    pub async fn search_parallel(&self, initial_state: MarketState) -> TradingAction {
        use tokio::task;
        
        let mut handles = vec![];
        
        for searcher in &self.searchers {
            let searcher_clone = searcher.clone();
            let state_clone = initial_state.clone();
            
            let handle = task::spawn_blocking(move || {
                let mut searcher = searcher_clone.lock().unwrap();
                searcher.search(state_clone)
            });
            
            handles.push(handle);
        }
        
        let mut action_votes: HashMap<TradingAction, usize> = HashMap::new();
        
        for handle in handles {
            if let Ok(action) = handle.await {
                *action_votes.entry(action).or_insert(0) += 1;
            }
        }
        
        action_votes.into_iter()
            .max_by_key(|(_, votes)| *votes)
            .map(|(action, _)| action)
            .unwrap_or(TradingAction::Hold)
    }
}

pub struct AdaptiveMCTS {
    base_searcher: MonteCarloTreeSearcher,
    performance_history: Vec<f64>,
    config_adjustments: MCTSConfig,
}

impl AdaptiveMCTS {
    pub fn new(initial_config: MCTSConfig) -> Self {
        Self {
            base_searcher: MonteCarloTreeSearcher::new(initial_config.clone()),
            performance_history: Vec::with_capacity(1000),
            config_adjustments: initial_config,
        }
    }

    pub fn search_adaptive(&mut self, state: MarketState, last_reward: Option<f64>) -> TradingAction {
        if let Some(reward) = last_reward {
            self.performance_history.push(reward);
            self.adjust_parameters();
        }
        
        self.base_searcher.config = self.config_adjustments.clone();
        self.base_searcher.search(state)
    }

    fn adjust_parameters(&mut self) {
        if self.performance_history.len() < 10 {
            return;
        }
        
        let recent_performance = self.performance_history
            .iter()
            .rev()
            .take(10)
            .sum::<f64>() / 10.0;
        
        let overall_performance = self.performance_history
            .iter()
            .sum::<f64>() / self.performance_history.len() as f64;
        
        if recent_performance < overall_performance * 0.8 {
            self.config_adjustments.exploration_constant *= 1.1;
            self.config_adjustments.simulation_depth = 
                (self.config_adjustments.simulation_depth as f64 * 1.2) as usize;
        } else if recent_performance > overall_performance * 1.2 {
            self.config_adjustments.exploration_constant *= 0.95;
        }
        
        self.config_adjustments.exploration_constant = 
            self.config_adjustments.exploration_constant.clamp(0.5, 3.0);
        self.config_adjustments.simulation_depth = 
            self.config_adjustments.simulation_depth.clamp(10, 50);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcts_search() {
        let config = MCTSConfig::default();
        let mut searcher = MonteCarloTreeSearcher::new(config);
        
        let state = MarketState {
            token_mint: Pubkey::new_unique(),
            price: 1000000,
            volume_24h: 10000000,
            liquidity: 5000000,
            momentum: 20,
            volatility: 30,
            spread_bps: 50,
            block_height: 1000,
            timestamp: 1700000000,
        };
        
        let action = searcher.search(state);
        assert!(!matches!(action, TradingAction::Hold));
    }

    #[tokio::test]
    async fn test_parallel_search() {
        let config = MCTSConfig::default();
        let parallel_mcts = ParallelMCTS::new(config, 4);
        
        let state = MarketState {
            token_mint: Pubkey::new_unique(),
            price: 2000000,
            volume_24h: 20000000,
            liquidity: 10000000,
            momentum: -30,
            volatility: 60,
            spread_bps: 75,
            block_height: 2000,
            timestamp: 1700001000,
        };
        
        let action = parallel_mcts.search_parallel(state).await;
        assert!(matches!(
            action,
            TradingAction::Buy { .. } | TradingAction::Sell { .. } | TradingAction::Hold
        ));
    }
}
        match action {
            TradingAction::Buy { amount, slippage_bps } => {
                let impact = (*amount as f64 / state.liquidity as f64).min(0.1);
                let momentum_score = (state.momentum as f64 / 100.0) * self.config.momentum_weight;
                let volatility_score = (state.volatility as f64 / 100.0) * self.config.volatility_weight;
                let liquidity_score = (state.liquidity as f64 / 10000000.0).min(1.0) * self.config.liquidity_weight;
                let slippage_penalty = *slippage_bps as f64 / 10000.0;
                
                reward = momentum_score + volatility_score + liquidity_score - slippage_penalty - impact;
                
                if state.momentum > 50 && state.volatility < 30 {
                    reward *= 1.5;
                }
            },
            TradingAction::Sell { amount, slippage_bps } => {
                let impact = (*amount as f64 / state.liquidity as f64).min(0.1);
                let momentum_score = (-state
