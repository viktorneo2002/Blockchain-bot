use solana_program::{pubkey::Pubkey, instruction::Instruction, program_pack::Pack};
use solana_sdk::{signature::Keypair, transaction::Transaction, hash::Hash};
use std::collections::{HashMap, BTreeMap, VecDeque};
use std::sync::Arc;
use parking_lot::RwLock;
use spl_token::state::Account as TokenAccount;
use anchor_lang::prelude::*;
use raydium_amm::state::{AmmInfo, AmmStatus};
use serum_dex::state::{Market, MarketState};
use std::str::FromStr;

const RAYDIUM_AMM_V4: Pubkey = solana_program::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
const SERUM_DEX_V3: Pubkey = solana_program::pubkey!("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin");
const ORCA_WHIRLPOOL: Pubkey = solana_program::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
const MARINADE_FINANCE: Pubkey = solana_program::pubkey!("MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD");
const MANGO_V4: Pubkey = solana_program::pubkey!("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg");

#[derive(Debug, Clone)]
pub struct StackelbergGameState {
    pub slot: u64,
    pub blockhash: Hash,
    pub leader: LeaderState,
    pub followers: Vec<FollowerState>,
    pub market_snapshot: MarketSnapshot,
    pub historical_data: HistoricalGameData,
}

#[derive(Debug, Clone)]
pub struct LeaderState {
    pub wallet: Pubkey,
    pub token_accounts: HashMap<Pubkey, TokenAccountInfo>,
    pub sol_balance: u64,
    pub active_positions: Vec<ActivePosition>,
    pub gas_budget: u64,
    pub risk_params: RiskParameters,
}

#[derive(Debug, Clone)]
pub struct TokenAccountInfo {
    pub mint: Pubkey,
    pub amount: u64,
    pub decimals: u8,
}

#[derive(Debug, Clone)]
pub struct ActivePosition {
    pub pool: Pubkey,
    pub entry_price: f64,
    pub size: u64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone)]
pub struct RiskParameters {
    pub max_position_size: u64,
    pub max_slippage: f64,
    pub min_profit_threshold: f64,
    pub gas_multiplier: f64,
}

#[derive(Debug, Clone)]
pub struct FollowerState {
    pub address: Pubkey,
    pub strategy_type: FollowerType,
    pub estimated_capital: u64,
    pub reaction_latency: u64,
    pub historical_behavior: BehaviorMetrics,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FollowerType {
    ArbitrageurBot,
    LiquidationBot,
    MarketMaker,
    RetailTrader,
    Whale,
}

#[derive(Debug, Clone)]
pub struct BehaviorMetrics {
    pub avg_txn_size: u64,
    pub success_rate: f64,
    pub avg_priority_fee: u64,
    pub front_run_attempts: u32,
    pub sandwich_frequency: f64,
}

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub amm_pools: HashMap<Pubkey, AmmPoolState>,
    pub serum_markets: HashMap<Pubkey, SerumMarketState>,
    pub lending_markets: HashMap<Pubkey, LendingMarketState>,
    pub gas_price: GasPriceInfo,
    pub mempool_density: f64,
}

#[derive(Debug, Clone)]
pub struct AmmPoolState {
    pub pool_type: PoolType,
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub reserve_a: u64,
    pub reserve_b: u64,
    pub fee_rate: u64,
    pub volume_24h: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PoolType {
    RaydiumV4,
    OrcaWhirlpool,
    SerumMarket,
}

#[derive(Debug, Clone)]
pub struct SerumMarketState {
    pub market_address: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub bids: OrderBookSide,
    pub asks: OrderBookSide,
}

#[derive(Debug, Clone)]
pub struct OrderBookSide {
    pub orders: Vec<(f64, u64)>, // (price, size)
}

#[derive(Debug, Clone)]
pub struct LendingMarketState {
    pub protocol: LendingProtocol,
    pub market: Pubkey,
    pub utilization_rate: f64,
    pub liquidation_threshold: f64,
    pub unhealthy_accounts: Vec<Pubkey>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LendingProtocol {
    Mango,
    Solend,
    Marginfi,
}

#[derive(Debug, Clone)]
pub struct GasPriceInfo {
    pub base_fee: u64,
    pub priority_fee_percentiles: [u64; 5], // 50th, 75th, 90th, 95th, 99th
}

#[derive(Debug, Clone)]
pub struct HistoricalGameData {
    pub outcomes: VecDeque<GameOutcome>,
    pub strategy_performance: HashMap<StrategyType, PerformanceMetrics>,
}

#[derive(Debug, Clone)]
pub struct GameOutcome {
    pub slot: u64,
    pub leader_action: ExecutedStrategy,
    pub follower_actions: Vec<(Pubkey, ExecutedStrategy)>,
    pub realized_profit: f64,
    pub gas_cost: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutedStrategy {
    pub strategy_type: StrategyType,
    pub target_pools: Vec<Pubkey>,
    pub amount: u64,
    pub priority_fee: u64,
    pub success: bool,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum StrategyType {
    SandwichAttack,
    AtomicArbitrage,
    Liquidation,
    JitLiquidity,
    FlashLoan,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_attempts: u64,
    pub success_count: u64,
    pub total_profit: f64,
    pub avg_gas_cost: f64,
    pub win_rate: f64,
}

pub struct StackelbergGameSolver {
    state: Arc<RwLock<StackelbergGameState>>,
    strategy_evaluator: StrategyEvaluator,
    equilibrium_finder: EquilibriumFinder,
    execution_engine: ExecutionEngine,
}

impl StackelbergGameSolver {
    pub fn new(initial_state: StackelbergGameState) -> Self {
        Self {
            state: Arc::new(RwLock::new(initial_state)),
            strategy_evaluator: StrategyEvaluator::new(),
            equilibrium_finder: EquilibriumFinder::new(),
            execution_engine: ExecutionEngine::new(),
        }
    }

    pub fn solve(&self) -> Result<OptimalStrategy, GameError> {
        let state = self.state.read();
        
        let candidate_strategies = self.generate_strategies(&state)?;
        let payoff_matrix = self.compute_payoff_matrix(&state, &candidate_strategies)?;
        let follower_responses = self.predict_follower_responses(&state, &candidate_strategies)?;
        
        let equilibrium = self.equilibrium_finder.find_stackelberg_equilibrium(
            &payoff_matrix,
            &follower_responses,
            &state.leader.risk_params,
        )?;
        
        self.select_optimal_strategy(equilibrium, &candidate_strategies, &state)
    }

    fn generate_strategies(&self, state: &StackelbergGameState) -> Result<Vec<CandidateStrategy>, GameError> {
        let mut strategies = Vec::new();
        
        // Sandwich opportunities
        for (pool_key, pool) in &state.market_snapshot.amm_pools {
            if let Some(sandwich) = self.strategy_evaluator.evaluate_sandwich(pool, &state.market_snapshot) {
                strategies.push(sandwich);
            }
        }
        
        // Arbitrage paths
        let arb_paths = self.find_arbitrage_paths(&state.market_snapshot)?;
        for path in arb_paths {
            if let Some(arb) = self.strategy_evaluator.evaluate_arbitrage(path, &state.leader) {
                strategies.push(arb);
            }
        }
        
        // Liquidation opportunities
        for (market_key, lending_state) in &state.market_snapshot.lending_markets {
            for account in &lending_state.unhealthy_accounts {
                if let Some(liq) = self.strategy_evaluator.evaluate_liquidation(
                    account,
                    lending_state,
                    &state.leader,
                ) {
                    strategies.push(liq);
                }
            }
        }
        
        // JIT liquidity
        for (pool_key, pool) in &state.market_snapshot.amm_pools {
            if pool.volume_24h > 1_000_000_000_000 { // High volume pools only
                if let Some(jit) = self.strategy_evaluator.evaluate_jit_liquidity(pool, &state.leader) {
                    strategies.push(jit);
                }
            }
        }
        
        Ok(strategies)
    }

    fn find_arbitrage_paths(&self, snapshot: &MarketSnapshot) -> Result<Vec<ArbitragePath>, GameError> {
        let mut paths = Vec::new();
        let mut visited = HashMap::new();
        
        for (pool_a_key, pool_a) in &snapshot.amm_pools {
            for (pool_b_key, pool_b) in &snapshot.amm_pools {
                if pool_a_key == pool_b_key { continue; }
                
                // Check for common tokens
                if pool_a.token_b == pool_b.token_a || pool_a.token_a == pool_b.token_b {
                    let path = ArbitragePath {
                        pools: vec![*pool_a_key, *pool_b_key],
                        expected_profit: self.calculate_arbitrage_profit(pool_a, pool_b)?,
                        gas_estimate: 200_000,
                    };
                    
                    if path.expected_profit > 0.0 {
                        paths.push(path);
                    }
                }
            }
        }
        
        paths.sort_by(|a, b| b.expected_profit.partial_cmp(&a.expected_profit).unwrap());
        Ok(paths.into_iter().take(10).collect())
    }

    fn calculate_arbitrage_profit(&self, pool_a: &AmmPoolState, pool_b: &AmmPoolState) -> Result<f64, GameError> {
        let price_a = pool_a.reserve_b as f64 / pool_a.reserve_a as f64;
        let price_b = pool_b.reserve_b as f64 / pool_b.reserve_a as f64;
        
        let price_diff = (price_a - price_b).abs() / price_a.min(price_b);
        let fee_impact = (pool_a.fee_rate + pool_b.fee_rate) as f64 / 10000.0;
        
        Ok((price_diff - fee_impact).max(0.0))
    }

    fn compute_payoff_matrix(
        &self,
        state: &StackelbergGameState,
        strategies: &[CandidateStrategy],
    ) -> Result<PayoffMatrix, GameError> {
        let n_strategies = strategies.len();
        let n_followers = state.followers.len();
        
        let mut leader_payoffs = vec![vec![0.0; n_followers * n_strategies]; n_strategies];
        let mut follower_payoffs = vec![vec![vec![0.0; n_strategies]; n_strategies]; n_followers];
        
        for (i, leader_strategy) in strategies.iter().enumerate() {
            for (j, follower) in state.followers.iter().enumerate() {
                for (k, follower_strategy) in strategies.iter().enumerate() {
                    let (leader_payoff, follower_payoff) = self.calculate_interaction_payoff(
                        leader_strategy,
                        follower_strategy,
                        follower,
                        state,
                    )?;
                    
                    leader_payoffs[i][j * n_strategies + k] = leader_payoff;
                    follower_payoffs[j][i][k] = follower_payoff;
                }
            }
        }
        
        Ok(PayoffMatrix {
            leader_payoffs,
            follower_payoffs,
        })
    }

    fn calculate_interaction_payoff(
        &self,
        leader_strategy: &CandidateStrategy,
        follower_strategy: &CandidateStrategy,
        follower: &FollowerState,
        state: &StackelbergGameState,
    ) -> Result<(f64, f64), GameError> {
        let competition_factor = match (&leader_strategy.strategy_type, &follower_strategy.strategy_type) {
            (StrategyType::SandwichAttack, StrategyType::SandwichAttack) => {
                if leader_strategy.target_pools[0] == follower_strategy.target_pools[0] { 0.2 } else { 0.9 }
            },
                        (StrategyType::AtomicArbitrage, StrategyType::AtomicArbitrage) => {
                let overlap = leader_strategy.target_pools.iter()
                    .filter(|pool| follower_strategy.target_pools.contains(pool))
                    .count() as f64;
                1.0 - (overlap / leader_strategy.target_pools.len().max(1) as f64) * 0.8
            },
            (StrategyType::Liquidation, StrategyType::Liquidation) => {
                if leader_strategy.target_pools[0] == follower_strategy.target_pools[0] { 0.0 } else { 1.0 }
            },
            (StrategyType::JitLiquidity, StrategyType::JitLiquidity) => {
                if leader_strategy.target_pools[0] == follower_strategy.target_pools[0] { 0.3 } else { 0.95 }
            },
            _ => 0.95,
        };
        
        let gas_competition = if follower.reaction_latency < 100 { 1.5 } else { 1.1 };
        let priority_ratio = leader_strategy.priority_fee as f64 / 
            (follower_strategy.priority_fee as f64 + leader_strategy.priority_fee as f64).max(1.0);
        
        let leader_base = leader_strategy.expected_profit * competition_factor * priority_ratio;
        let leader_cost = (leader_strategy.gas_estimate as f64 * 
            state.market_snapshot.gas_price.base_fee as f64 * gas_competition) / 1e9;
        
        let follower_base = follower_strategy.expected_profit * (1.0 - competition_factor) * (1.0 - priority_ratio);
        let follower_cost = (follower_strategy.gas_estimate as f64 * 
            state.market_snapshot.gas_price.base_fee as f64) / 1e9;
        
        Ok((leader_base - leader_cost, follower_base - follower_cost))
    }

    fn predict_follower_responses(
        &self,
        state: &StackelbergGameState,
        strategies: &[CandidateStrategy],
    ) -> Result<Vec<FollowerResponse>, GameError> {
        let mut responses = Vec::new();
        
        for follower in &state.followers {
            let best_response = match follower.strategy_type {
                FollowerType::ArbitrageurBot => self.predict_arbitrageur_response(follower, strategies, state)?,
                FollowerType::LiquidationBot => self.predict_liquidator_response(follower, strategies, state)?,
                FollowerType::MarketMaker => self.predict_mm_response(follower, strategies, state)?,
                FollowerType::RetailTrader => self.predict_retail_response(follower, strategies, state)?,
                FollowerType::Whale => self.predict_whale_response(follower, strategies, state)?,
            };
            responses.push(best_response);
        }
        
        Ok(responses)
    }

    fn predict_arbitrageur_response(
        &self,
        follower: &FollowerState,
        strategies: &[CandidateStrategy],
        state: &StackelbergGameState,
    ) -> Result<FollowerResponse, GameError> {
        let mut best_strategy = None;
        let mut best_payoff = 0.0;
        
        for (idx, strategy) in strategies.iter().enumerate() {
            if matches!(strategy.strategy_type, StrategyType::AtomicArbitrage | StrategyType::SandwichAttack) {
                let reaction_probability = 1.0 / (1.0 + (follower.reaction_latency as f64 / 50.0));
                let capital_constraint = (follower.estimated_capital as f64 / strategy.capital_required as f64).min(1.0);
                let expected_payoff = strategy.expected_profit * reaction_probability * capital_constraint;
                
                if expected_payoff > best_payoff {
                    best_payoff = expected_payoff;
                    best_strategy = Some(idx);
                }
            }
        }
        
        Ok(FollowerResponse {
            follower_address: follower.address,
            strategy_index: best_strategy,
            reaction_probability: if best_strategy.is_some() { 0.8 } else { 0.0 },
        })
    }

    fn predict_liquidator_response(
        &self,
        follower: &FollowerState,
        strategies: &[CandidateStrategy],
        state: &StackelbergGameState,
    ) -> Result<FollowerResponse, GameError> {
        for (idx, strategy) in strategies.iter().enumerate() {
            if matches!(strategy.strategy_type, StrategyType::Liquidation) {
                return Ok(FollowerResponse {
                    follower_address: follower.address,
                    strategy_index: Some(idx),
                    reaction_probability: 0.95,
                });
            }
        }
        
        Ok(FollowerResponse {
            follower_address: follower.address,
            strategy_index: None,
            reaction_probability: 0.0,
        })
    }

    fn predict_mm_response(
        &self,
        follower: &FollowerState,
        strategies: &[CandidateStrategy],
        state: &StackelbergGameState,
    ) -> Result<FollowerResponse, GameError> {
        Ok(FollowerResponse {
            follower_address: follower.address,
            strategy_index: None,
            reaction_probability: 0.1,
        })
    }

    fn predict_retail_response(
        &self,
        follower: &FollowerState,
        strategies: &[CandidateStrategy],
        state: &StackelbergGameState,
    ) -> Result<FollowerResponse, GameError> {
        Ok(FollowerResponse {
            follower_address: follower.address,
            strategy_index: None,
            reaction_probability: 0.0,
        })
    }

    fn predict_whale_response(
        &self,
        follower: &FollowerState,
        strategies: &[CandidateStrategy],
        state: &StackelbergGameState,
    ) -> Result<FollowerResponse, GameError> {
        let mut best_strategy = None;
        let mut best_payoff = 0.0;
        
        for (idx, strategy) in strategies.iter().enumerate() {
            if strategy.capital_required > 100_000_000_000 {
                let expected_payoff = strategy.expected_profit * 0.7;
                if expected_payoff > best_payoff {
                    best_payoff = expected_payoff;
                    best_strategy = Some(idx);
                }
            }
        }
        
        Ok(FollowerResponse {
            follower_address: follower.address,
            strategy_index: best_strategy,
            reaction_probability: if best_strategy.is_some() { 0.6 } else { 0.0 },
        })
    }

    fn select_optimal_strategy(
        &self,
        equilibrium: StackelbergEquilibrium,
        strategies: &[CandidateStrategy],
        state: &StackelbergGameState,
    ) -> Result<OptimalStrategy, GameError> {
        let strategy = &strategies[equilibrium.leader_strategy_index];
        let risk_adjusted_profit = strategy.expected_profit * 
            (1.0 - state.leader.risk_params.max_slippage);
        
        if risk_adjusted_profit < state.leader.risk_params.min_profit_threshold {
            return Err(GameError::BelowProfitThreshold);
        }
        
        let instructions = self.execution_engine.build_instructions(strategy, state)?;
        
        Ok(OptimalStrategy {
            strategy_type: strategy.strategy_type.clone(),
            instructions,
            priority_fee: self.calculate_optimal_priority_fee(strategy, &equilibrium, state),
            expected_profit: risk_adjusted_profit,
            execution_probability: equilibrium.success_probability,
        })
    }

    fn calculate_optimal_priority_fee(&self, strategy: &CandidateStrategy, equilibrium: &StackelbergEquilibrium, state: &StackelbergGameState) -> u64 {
        let base_priority = state.market_snapshot.gas_price.priority_fee_percentiles[3];
        let competition_multiplier = 1.0 + equilibrium.competition_intensity;
        let urgency_factor = match strategy.strategy_type {
            StrategyType::Liquidation => 2.0,
            StrategyType::SandwichAttack => 1.8,
            StrategyType::AtomicArbitrage => 1.5,
            _ => 1.2,
        };
        
        (base_priority as f64 * competition_multiplier * urgency_factor * state.leader.risk_params.gas_multiplier) as u64
    }

    pub fn update_state(&self, outcome: GameOutcome) -> Result<(), GameError> {
        let mut state = self.state.write();
        
        state.historical_data.outcomes.push_back(outcome.clone());
        if state.historical_data.outcomes.len() > 10000 {
            state.historical_data.outcomes.pop_front();
        }
        
        let metrics = state.historical_data.strategy_performance
            .entry(outcome.leader_action.strategy_type.clone())
            .or_insert(PerformanceMetrics {
                total_attempts: 0,
                success_count: 0,
                total_profit: 0.0,
                avg_gas_cost: 0.0,
                win_rate: 0.0,
            });
        
        metrics.total_attempts += 1;
        if outcome.leader_action.success {
            metrics.success_count += 1;
        }
        metrics.total_profit += outcome.realized_profit;
        metrics.avg_gas_cost = (metrics.avg_gas_cost * (metrics.total_attempts - 1) as f64 + outcome.gas_cost as f64) 
            / metrics.total_attempts as f64;
        metrics.win_rate = metrics.success_count as f64 / metrics.total_attempts as f64;
        
        Ok(())
    }
}

pub struct StrategyEvaluator;

impl StrategyEvaluator {
    pub fn new() -> Self {
        Self
    }

    pub fn evaluate_sandwich(&self, pool: &AmmPoolState, snapshot: &MarketSnapshot) -> Option<CandidateStrategy> {
        let min_liquidity = 100_000_000_000;
        if pool.reserve_a < min_liquidity || pool.reserve_b < min_liquidity {
            return None;
        }

        let price_impact = self.calculate_price_impact(pool, 1_000_000_000)?;
        if price_impact < 0.002 {
            return None;
        }

        Some(CandidateStrategy {
            strategy_type: StrategyType::SandwichAttack,
            target_pools: vec![pool.pool_type.get_address()],
            expected_profit: price_impact * 1_000_000_000.0 * 0.3,
            capital_required: 2_000_000_000,
            gas_estimate: 300_000,
            priority_fee: 100_000,
        })
    }

    pub fn evaluate_arbitrage(&self, path: ArbitragePath, leader: &LeaderState) -> Option<CandidateStrategy> {
        if path.expected_profit < 0.001 {
            return None;
        }

        let capital_needed = self.calculate_optimal_arb_amount(&path)?;
        if capital_needed > leader.sol_balance {
            return None;
        }

        Some(CandidateStrategy {
            strategy_type: StrategyType::AtomicArbitrage,
            target_pools: path.pools,
            expected_profit: path.expected_profit * capital_needed as f64,
            capital_required: capital_needed,
            gas_estimate: path.gas_estimate,
            priority_fee: 50_000,
        })
    }

    pub fn evaluate_liquidation(&self, account: &Pubkey, market: &LendingMarketState, leader: &LeaderState) -> Option<CandidateStrategy> {
        if market.utilization_rate < market.liquidation_threshold {
            return None;
        }

        let liquidation_bonus = 0.05;
        let collateral_value = 10_000_000_000;
        let expected_profit = collateral_value as f64 * liquidation_bonus;

        Some(CandidateStrategy {
            strategy_type: StrategyType::Liquidation,
            target_pools: vec![market.market],
            expected_profit,
            capital_required: collateral_value,
            gas_estimate: 400_000,
            priority_fee: 200_000,
        })
    }

    pub fn evaluate_jit_liquidity(&self, pool: &AmmPoolState, leader: &LeaderState) -> Option<CandidateStrategy> {
        let fee_apr = (pool.fee_rate as f64 / 10000.0) * (pool.volume_24h as f64 / pool.reserve_a as f64) * 365.0;
        if fee_apr < 0.5 {
            return None;
        }

        Some(CandidateStrategy {
            strategy_type: StrategyType::JitLiquidity,
            target_pools: vec![pool.pool_type.get_address()],
            expected_profit: pool.volume_24h as f64 * 0.0001,
            capital_required: leader.sol_balance / 4,
            gas_estimate: 250_000,
            priority_fee: 75_000,
        })
    }

    fn calculate_price_impact(&self, pool: &AmmPoolState, amount: u64) -> Option<f64> {
        let k = pool.reserve_a as f64 * pool.reserve_b as f64;
        let new_reserve_a = pool.reserve_a as f64 + amount as f64;
        let new_reserve_b = k / new_reserve_a;
        let price_before = pool.reserve_b as f64 / pool.reserve_a as f64;
        let price_after = new_reserve_b / new_reserve_a;
        Some((price_before - price_after).abs() / price_before)
    }

    fn calculate_optimal_arb_amount(&self, path: &ArbitragePath) -> Option<u64> {
        Some(1_000_000_000)
    }
}

pub struct EquilibriumFinder;

impl EquilibriumFinder {
    pub fn new() -> Self {
        Self
    }

    pub fn find_stackelberg_equilibrium(
        &self,
        payoff_matrix: &PayoffMatrix,
        follower_responses: &[FollowerResponse],
        risk_params: &RiskParameters,
    ) -> Result<StackelbergEquilibrium, GameError> {
        let n_strategies = payoff_matrix.leader_payoffs.len();
                let mut best_strategy = 0;
        let mut best_payoff = f64::NEG_INFINITY;
        let mut competition_scores = vec![0.0; n_strategies];
        
        for leader_idx in 0..n_strategies {
            let mut expected_payoff = 0.0;
            let mut competition_intensity = 0.0;
            
            for (follower_idx, response) in follower_responses.iter().enumerate() {
                if let Some(follower_strategy) = response.strategy_index {
                    let payoff_idx = follower_idx * n_strategies + follower_strategy;
                    let interaction_payoff = payoff_matrix.leader_payoffs[leader_idx][payoff_idx];
                    expected_payoff += interaction_payoff * response.reaction_probability;
                    competition_intensity += response.reaction_probability;
                } else {
                    let payoff_idx = follower_idx * n_strategies;
                    expected_payoff += payoff_matrix.leader_payoffs[leader_idx][payoff_idx];
                }
            }
            
            competition_scores[leader_idx] = competition_intensity;
            let risk_adjusted = expected_payoff * (1.0 - risk_params.max_slippage * competition_intensity);
            
            if risk_adjusted > best_payoff {
                best_payoff = risk_adjusted;
                best_strategy = leader_idx;
            }
        }
        
        Ok(StackelbergEquilibrium {
            leader_strategy_index: best_strategy,
            expected_payoff: best_payoff,
            competition_intensity: competition_scores[best_strategy] / follower_responses.len().max(1) as f64,
            success_probability: 1.0 / (1.0 + competition_scores[best_strategy]),
        })
    }
}

pub struct ExecutionEngine;

impl ExecutionEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn build_instructions(
        &self,
        strategy: &CandidateStrategy,
        state: &StackelbergGameState,
    ) -> Result<Vec<Instruction>, GameError> {
        match strategy.strategy_type {
            StrategyType::SandwichAttack => self.build_sandwich_instructions(strategy, state),
            StrategyType::AtomicArbitrage => self.build_arbitrage_instructions(strategy, state),
            StrategyType::Liquidation => self.build_liquidation_instructions(strategy, state),
            StrategyType::JitLiquidity => self.build_jit_instructions(strategy, state),
            StrategyType::FlashLoan => self.build_flashloan_instructions(strategy, state),
        }
    }

    fn build_sandwich_instructions(
        &self,
        strategy: &CandidateStrategy,
        state: &StackelbergGameState,
    ) -> Result<Vec<Instruction>, GameError> {
        let pool_address = strategy.target_pools[0];
        let pool = state.market_snapshot.amm_pools.get(&pool_address)
            .ok_or(GameError::PoolNotFound)?;
        
        let mut instructions = Vec::new();
        
        // Front-run swap
        let front_run_amount = strategy.capital_required / 2;
        instructions.push(self.create_swap_instruction(
            pool_address,
            pool.token_a,
            pool.token_b,
            front_run_amount,
            state.leader.wallet,
        )?);
        
        // Back-run swap (reverse)
        instructions.push(self.create_swap_instruction(
            pool_address,
            pool.token_b,
            pool.token_a,
            front_run_amount,
            state.leader.wallet,
        )?);
        
        Ok(instructions)
    }

    fn build_arbitrage_instructions(
        &self,
        strategy: &CandidateStrategy,
        state: &StackelbergGameState,
    ) -> Result<Vec<Instruction>, GameError> {
        let mut instructions = Vec::new();
        let mut current_token = None;
        let mut current_amount = strategy.capital_required;
        
        for (i, pool_address) in strategy.target_pools.iter().enumerate() {
            let pool = state.market_snapshot.amm_pools.get(pool_address)
                .ok_or(GameError::PoolNotFound)?;
            
            let (input_token, output_token) = if i == 0 {
                (pool.token_a, pool.token_b)
            } else if current_token == Some(pool.token_a) {
                (pool.token_a, pool.token_b)
            } else {
                (pool.token_b, pool.token_a)
            };
            
            instructions.push(self.create_swap_instruction(
                *pool_address,
                input_token,
                output_token,
                current_amount,
                state.leader.wallet,
            )?);
            
            current_token = Some(output_token);
            current_amount = self.calculate_output_amount(pool, current_amount)?;
        }
        
        Ok(instructions)
    }

    fn build_liquidation_instructions(
        &self,
        strategy: &CandidateStrategy,
        state: &StackelbergGameState,
    ) -> Result<Vec<Instruction>, GameError> {
        let market_address = strategy.target_pools[0];
        let lending_market = state.market_snapshot.lending_markets.get(&market_address)
            .ok_or(GameError::MarketNotFound)?;
        
        match lending_market.protocol {
            LendingProtocol::Mango => {
                Ok(vec![
                    self.create_mango_liquidation_instruction(
                        market_address,
                        lending_market.unhealthy_accounts[0],
                        state.leader.wallet,
                        strategy.capital_required,
                    )?
                ])
            },
            LendingProtocol::Solend => {
                Ok(vec![
                    self.create_solend_liquidation_instruction(
                        market_address,
                        lending_market.unhealthy_accounts[0],
                        state.leader.wallet,
                        strategy.capital_required,
                    )?
                ])
            },
            LendingProtocol::Marginfi => {
                Ok(vec![
                    self.create_marginfi_liquidation_instruction(
                        market_address,
                        lending_market.unhealthy_accounts[0],
                        state.leader.wallet,
                        strategy.capital_required,
                    )?
                ])
            },
        }
    }

    fn build_jit_instructions(
        &self,
        strategy: &CandidateStrategy,
        state: &StackelbergGameState,
    ) -> Result<Vec<Instruction>, GameError> {
        let pool_address = strategy.target_pools[0];
        let pool = state.market_snapshot.amm_pools.get(&pool_address)
            .ok_or(GameError::PoolNotFound)?;
        
        Ok(vec![
            self.create_add_liquidity_instruction(
                pool_address,
                pool.token_a,
                pool.token_b,
                strategy.capital_required / 2,
                strategy.capital_required / 2,
                state.leader.wallet,
            )?,
            self.create_remove_liquidity_instruction(
                pool_address,
                state.leader.wallet,
            )?,
        ])
    }

    fn build_flashloan_instructions(
        &self,
        strategy: &CandidateStrategy,
        state: &StackelbergGameState,
    ) -> Result<Vec<Instruction>, GameError> {
        Ok(vec![
            self.create_flashloan_borrow_instruction(
                strategy.target_pools[0],
                strategy.capital_required,
                state.leader.wallet,
            )?,
            self.create_flashloan_repay_instruction(
                strategy.target_pools[0],
                strategy.capital_required,
                state.leader.wallet,
            )?,
        ])
    }

    fn create_swap_instruction(
        &self,
        pool: Pubkey,
        token_in: Pubkey,
        token_out: Pubkey,
        amount: u64,
        wallet: Pubkey,
    ) -> Result<Instruction, GameError> {
        Ok(Instruction {
            program_id: RAYDIUM_AMM_V4,
            accounts: vec![
                AccountMeta::new(pool, false),
                AccountMeta::new(token_in, false),
                AccountMeta::new(token_out, false),
                AccountMeta::new(wallet, true),
            ],
            data: bincode::serialize(&amount).map_err(|_| GameError::SerializationError)?,
        })
    }

    fn create_mango_liquidation_instruction(
        &self,
        market: Pubkey,
        target_account: Pubkey,
        liquidator: Pubkey,
        amount: u64,
    ) -> Result<Instruction, GameError> {
        Ok(Instruction {
            program_id: MANGO_V4,
            accounts: vec![
                AccountMeta::new(market, false),
                AccountMeta::new(target_account, false),
                AccountMeta::new(liquidator, true),
            ],
            data: bincode::serialize(&amount).map_err(|_| GameError::SerializationError)?,
        })
    }

    fn create_solend_liquidation_instruction(
        &self,
        market: Pubkey,
        target_account: Pubkey,
        liquidator: Pubkey,
        amount: u64,
    ) -> Result<Instruction, GameError> {
        Ok(Instruction {
            program_id: solana_program::pubkey!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo"),
            accounts: vec![
                AccountMeta::new(market, false),
                AccountMeta::new(target_account, false),
                AccountMeta::new(liquidator, true),
            ],
            data: bincode::serialize(&amount).map_err(|_| GameError::SerializationError)?,
        })
    }

    fn create_marginfi_liquidation_instruction(
        &self,
        market: Pubkey,
        target_account: Pubkey,
        liquidator: Pubkey,
        amount: u64,
    ) -> Result<Instruction, GameError> {
        Ok(Instruction {
            program_id: solana_program::pubkey!("MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA"),
            accounts: vec![
                AccountMeta::new(market, false),
                AccountMeta::new(target_account, false),
                AccountMeta::new(liquidator, true),
            ],
            data: bincode::serialize(&amount).map_err(|_| GameError::SerializationError)?,
        })
    }

    fn create_add_liquidity_instruction(
        &self,
        pool: Pubkey,
        token_a: Pubkey,
        token_b: Pubkey,
        amount_a: u64,
        amount_b: u64,
        wallet: Pubkey,
    ) -> Result<Instruction, GameError> {
        Ok(Instruction {
            program_id: RAYDIUM_AMM_V4,
            accounts: vec![
                AccountMeta::new(pool, false),
                AccountMeta::new(token_a, false),
                AccountMeta::new(token_b, false),
                AccountMeta::new(wallet, true),
            ],
            data: bincode::serialize(&(amount_a, amount_b)).map_err(|_| GameError::SerializationError)?,
        })
    }

    fn create_remove_liquidity_instruction(
        &self,
        pool: Pubkey,
        wallet: Pubkey,
    ) -> Result<Instruction, GameError> {
        Ok(Instruction {
            program_id: RAYDIUM_AMM_V4,
            accounts: vec![
                AccountMeta::new(pool, false),
                AccountMeta::new(wallet, true),
            ],
            data: vec![2], // Remove liquidity opcode
        })
    }

    fn create_flashloan_borrow_instruction(
        &self,
        lender: Pubkey,
        amount: u64,
        borrower: Pubkey,
    ) -> Result<Instruction, GameError> {
        Ok(Instruction {
            program_id: solana_program::pubkey!("FLASHFiWDm1dEfnByNKLrLkRV3AbQWsqAF61GzRBLft"),
            accounts: vec![
                AccountMeta::new(lender, false),
                AccountMeta::new(borrower, true),
            ],
            data: bincode::serialize(&amount).map_err(|_| GameError::SerializationError)?,
        })
    }

    fn create_flashloan_repay_instruction(
        &self,
        lender: Pubkey,
        amount: u64,
        borrower: Pubkey,
    ) -> Result<Instruction, GameError> {
        Ok(Instruction {
            program_id: solana_program::pubkey!("FLASHFiWDm1dEfnByNKLrLkRV3AbQWsqAF61GzRBLft"),
            accounts: vec![
                AccountMeta::new(lender, false),
                AccountMeta::new(borrower, true),
            ],
            data: bincode::serialize(&(amount + amount / 1000)).map_err(|_| GameError::SerializationError)?,
        })
    }

    fn calculate_output_amount(&self, pool: &AmmPoolState, input: u64) -> Result<u64, GameError> {
        let fee_amount = input * pool.fee_rate / 10000;
        let input_after_fee = input - fee_amount;
        let output = (input_after_fee * pool.reserve_b) / (pool.reserve_a + input_after_fee);
        Ok(output)
    }
}

impl PoolType {
    pub fn get_address(&self) -> Pubkey {
        match self {
            PoolType::RaydiumV4 => RAYDIUM_AMM_V4,
            PoolType::OrcaWhirlpool => ORCA_WHIRLPOOL,
            PoolType::SerumMarket => SERUM_DEX_V3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CandidateStrategy {
    pub strategy_type: StrategyType,
    pub target_pools: Vec<Pubkey>,
    pub expected_profit: f64,
    pub capital_required: u64,
    pub gas_estimate: u64,
    pub priority_fee: u64,
}

#[derive(Debug, Clone)]
pub struct ArbitragePath {
    pub pools: Vec<Pubkey>,
    pub expected_profit: f64,
    pub gas_estimate: u64,
}

#[derive(Debug, Clone)]
pub struct PayoffMatrix {
        pub leader_payoffs: Vec<Vec<f64>>,
    pub follower_payoffs: Vec<Vec<Vec<f64>>>,
}

#[derive(Debug, Clone)]
pub struct FollowerResponse {
    pub follower_address: Pubkey,
    pub strategy_index: Option<usize>,
    pub reaction_probability: f64,
}

#[derive(Debug, Clone)]
pub struct StackelbergEquilibrium {
    pub leader_strategy_index: usize,
    pub expected_payoff: f64,
    pub competition_intensity: f64,
    pub success_probability: f64,
}

#[derive(Debug, Clone)]
pub struct OptimalStrategy {
    pub strategy_type: StrategyType,
    pub instructions: Vec<Instruction>,
    pub priority_fee: u64,
    pub expected_profit: f64,
    pub execution_probability: f64,
}

#[derive(Debug)]
pub enum GameError {
    InvalidState(String),
    ComputationError(String),
    PoolNotFound,
    MarketNotFound,
    InsufficientBalance,
    SerializationError,
    BelowProfitThreshold,
    NetworkError(String),
    SimulationFailed(String),
}

impl std::fmt::Display for GameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GameError::InvalidState(msg) => write!(f, "Invalid game state: {}", msg),
            GameError::ComputationError(msg) => write!(f, "Computation error: {}", msg),
            GameError::PoolNotFound => write!(f, "Pool not found"),
            GameError::MarketNotFound => write!(f, "Market not found"),
            GameError::InsufficientBalance => write!(f, "Insufficient balance"),
            GameError::SerializationError => write!(f, "Serialization error"),
            GameError::BelowProfitThreshold => write!(f, "Below profit threshold"),
            GameError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            GameError::SimulationFailed(msg) => write!(f, "Simulation failed: {}", msg),
        }
    }
}

impl std::error::Error for GameError {}

pub struct AdaptiveGameController {
    solver: Arc<StackelbergGameSolver>,
    market_monitor: MarketMonitor,
    execution_manager: ExecutionManager,
    performance_tracker: PerformanceTracker,
}

impl AdaptiveGameController {
    pub fn new(initial_state: StackelbergGameState) -> Self {
        Self {
            solver: Arc::new(StackelbergGameSolver::new(initial_state)),
            market_monitor: MarketMonitor::new(),
            execution_manager: ExecutionManager::new(),
            performance_tracker: PerformanceTracker::new(),
        }
    }

    pub async fn run(&self) -> Result<(), GameError> {
        loop {
            let market_update = self.market_monitor.get_latest_snapshot().await?;
            self.update_market_state(market_update)?;
            
            if let Some(strategy) = self.find_profitable_strategy().await? {
                self.execute_strategy(strategy).await?;
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    async fn find_profitable_strategy(&self) -> Result<Option<OptimalStrategy>, GameError> {
        let optimal = self.solver.solve()?;
        
        if optimal.expected_profit > 0.0 && optimal.execution_probability > 0.7 {
            let simulation_result = self.execution_manager.simulate(&optimal).await?;
            if simulation_result.success {
                return Ok(Some(optimal));
            }
        }
        
        Ok(None)
    }

    async fn execute_strategy(&self, strategy: OptimalStrategy) -> Result<(), GameError> {
        let start_slot = self.solver.state.read().slot;
        let execution_result = self.execution_manager.execute(strategy.clone()).await?;
        
        let outcome = GameOutcome {
            slot: start_slot,
            leader_action: ExecutedStrategy {
                strategy_type: strategy.strategy_type,
                target_pools: strategy.instructions.iter()
                    .filter_map(|ix| ix.accounts.first().map(|acc| acc.pubkey))
                    .collect(),
                amount: 0,
                priority_fee: strategy.priority_fee,
                success: execution_result.success,
            },
            follower_actions: vec![],
            realized_profit: execution_result.profit,
            gas_cost: execution_result.gas_used,
        };
        
        self.solver.update_state(outcome)?;
        self.performance_tracker.record(execution_result);
        
        Ok(())
    }

    fn update_market_state(&self, snapshot: MarketSnapshot) -> Result<(), GameError> {
        let mut state = self.solver.state.write();
        state.market_snapshot = snapshot;
        state.slot += 1;
        Ok(())
    }
}

pub struct MarketMonitor {
    rpc_client: Arc<solana_client::rpc_client::RpcClient>,
    pool_monitors: HashMap<Pubkey, PoolMonitor>,
}

impl MarketMonitor {
    pub fn new() -> Self {
        Self {
            rpc_client: Arc::new(solana_client::rpc_client::RpcClient::new(
                "https://api.mainnet-beta.solana.com".to_string()
            )),
            pool_monitors: HashMap::new(),
        }
    }

    pub async fn get_latest_snapshot(&self) -> Result<MarketSnapshot, GameError> {
        let slot = self.rpc_client.get_slot()
            .map_err(|e| GameError::NetworkError(e.to_string()))?;
        
        let mut amm_pools = HashMap::new();
        let mut serum_markets = HashMap::new();
        let mut lending_markets = HashMap::new();
        
        // Monitor key pools
        let key_pools = vec![
            solana_program::pubkey!("58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2"), // SOL/USDC
            solana_program::pubkey!("7XawhbbxtsRcQA8KTkHT9f9nc6d69UwqCDh6U5EEbEmX"), // USDT/USDC
        ];
        
        for pool_address in key_pools {
            if let Ok(account) = self.rpc_client.get_account(&pool_address) {
                let pool_state = self.parse_pool_state(&account.data)?;
                amm_pools.insert(pool_address, pool_state);
            }
        }
        
        let gas_price = self.fetch_gas_price_info()?;
        let mempool_density = self.estimate_mempool_density()?;
        
        Ok(MarketSnapshot {
            amm_pools,
            serum_markets,
            lending_markets,
            gas_price,
            mempool_density,
        })
    }

    fn parse_pool_state(&self, data: &[u8]) -> Result<AmmPoolState, GameError> {
        Ok(AmmPoolState {
            pool_type: PoolType::RaydiumV4,
            token_a: Pubkey::default(),
            token_b: Pubkey::default(),
            reserve_a: u64::from_le_bytes(data[64..72].try_into().unwrap_or_default()),
            reserve_b: u64::from_le_bytes(data[72..80].try_into().unwrap_or_default()),
            fee_rate: 30,
            volume_24h: 1_000_000_000_000,
        })
    }

    fn fetch_gas_price_info(&self) -> Result<GasPriceInfo, GameError> {
        let recent_fees = self.rpc_client.get_recent_prioritization_fees(&[])
            .map_err(|e| GameError::NetworkError(e.to_string()))?;
        
        let mut priority_fees: Vec<u64> = recent_fees.iter().map(|f| f.prioritization_fee).collect();
        priority_fees.sort();
        
        let percentiles = [
            priority_fees.get(priority_fees.len() / 2).copied().unwrap_or(1000),
            priority_fees.get(priority_fees.len() * 3 / 4).copied().unwrap_or(5000),
            priority_fees.get(priority_fees.len() * 9 / 10).copied().unwrap_or(10000),
            priority_fees.get(priority_fees.len() * 95 / 100).copied().unwrap_or(20000),
            priority_fees.get(priority_fees.len() * 99 / 100).copied().unwrap_or(50000),
        ];
        
        Ok(GasPriceInfo {
            base_fee: 5000,
            priority_fee_percentiles: percentiles,
        })
    }

    fn estimate_mempool_density(&self) -> Result<f64, GameError> {
        Ok(0.5)
    }
}

pub struct PoolMonitor;

pub struct ExecutionManager {
    rpc_client: Arc<solana_client::rpc_client::RpcClient>,
    keypair: Arc<Keypair>,
}

impl ExecutionManager {
    pub fn new() -> Self {
        Self {
            rpc_client: Arc::new(solana_client::rpc_client::RpcClient::new(
                "https://api.mainnet-beta.solana.com".to_string()
            )),
            keypair: Arc::new(Keypair::new()),
        }
    }

    pub async fn simulate(&self, strategy: &OptimalStrategy) -> Result<SimulationResult, GameError> {
        let blockhash = self.rpc_client.get_latest_blockhash()
            .map_err(|e| GameError::NetworkError(e.to_string()))?;
        
        let mut transaction = Transaction::new_with_payer(
            &strategy.instructions,
            Some(&self.keypair.pubkey()),
        );
        
        transaction.sign(&[&*self.keypair], blockhash);
        
        let result = self.rpc_client.simulate_transaction(&transaction)
            .map_err(|e| GameError::SimulationFailed(e.to_string()))?;
        
        Ok(SimulationResult {
            success: result.value.err.is_none(),
            units_consumed: result.value.units_consumed.unwrap_or(0),
        })
    }

    pub async fn execute(&self, strategy: OptimalStrategy) -> Result<ExecutionResult, GameError> {
        let blockhash = self.rpc_client.get_latest_blockhash()
            .map_err(|e| GameError::NetworkError(e.to_string()))?;
        
        let mut transaction = Transaction::new_with_payer(
            &strategy.instructions,
            Some(&self.keypair.pubkey()),
        );
        
        transaction.sign(&[&*self.keypair], blockhash);
        
        let signature = self.rpc_client.send_and_confirm_transaction(&transaction)
            .map_err(|e| GameError::NetworkError(e.to_string()))?;
        
        Ok(ExecutionResult {
            signature,
            success: true,
            profit: strategy.expected_profit,
            gas_used: strategy.priority_fee,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub success: bool,
    pub units_consumed: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub signature: solana_sdk::signature::Signature,
    pub success: bool,
    pub profit: f64,
    pub gas_used: u64,
}

pub struct PerformanceTracker {
    results: Arc<RwLock<VecDeque<ExecutionResult>>>,
}

impl PerformanceTracker {
    pub fn new() -> Self {
        Self {
            results: Arc::new(RwLock::new(VecDeque::with_capacity(10000))),
        }
    }

    pub fn record(&self, result: ExecutionResult) {
        let mut results = self.results.write();
        results.push_back(result);
        if results.len() > 10000 {
            results.pop_front();
        }
    }

    pub fn get_statistics(&self) -> PerformanceStatistics {
        let results = self.results.read();
        let total = results.len() as f64;
        let success_count = results.iter().filter(|r| r.success).count() as f64;
        let total_profit: f64 = results.iter().map(|r| r.profit).sum();
        let total_gas: u64 = results.iter().map(|r| r.gas_used).sum();
        
        PerformanceStatistics {
            win_rate: success_count / total.max(1.0),
            total_profit,
            avg_profit: total_profit / total.max(1.0),
            total_gas_spent: total_gas,
            avg_gas_per_trade: (total_gas as f64 / total.max(1.0)) as u64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceStatistics {
    pub win_rate: f64,
    pub total_profit: f64,
    pub avg_profit: f64,
    pub total_gas_spent: u64,
    pub avg_gas_per_trade: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_game_solver_integration() {
        let initial_state = create_test_game_state();
        let solver = StackelbergGameSolver::new(initial_state);
        let result = solver.solve();
        assert!(result.is_ok());
    }

    fn create_test_game_state() -> StackelbergGameState {
        StackelbergGameState {
            slot: 200_000_000,
            blockhash: Hash::default(),
            leader: LeaderState {
                wallet: Pubkey::new_unique(),
                token_accounts: HashMap::new(),
                sol_balance: 10_000_000_000,
                active_positions: vec![],
                gas_budget: 1_000_000_000,
                risk_params: RiskParameters {
                    max_position_size: 1_000_000_000,
                    max_slippage: 0.02,
                    min_profit_threshold: 0.001,
                    gas_multiplier: 1.5,
                },
            },
            followers: vec![],
            market_snapshot: MarketSnapshot {
                amm_pools: HashMap::new(),
                serum_markets: HashMap::new(),
                lending_markets: HashMap::new(),
                gas_price: GasPriceInfo {
                    base_fee: 5000,
                                        priority_fee_percentiles: [1000, 5000, 10000, 20000, 50000],
                },
                mempool_density: 0.5,
            },
            historical_data: HistoricalGameData {
                outcomes: VecDeque::new(),
                strategy_performance: HashMap::new(),
            },
        }
    }

    #[test]
    fn test_strategy_evaluation() {
        let evaluator = StrategyEvaluator::new();
        let pool = AmmPoolState {
            pool_type: PoolType::RaydiumV4,
            token_a: Pubkey::new_unique(),
            token_b: Pubkey::new_unique(),
            reserve_a: 1_000_000_000_000,
            reserve_b: 2_000_000_000_000,
            fee_rate: 30,
            volume_24h: 10_000_000_000_000,
        };
        
        let snapshot = MarketSnapshot {
            amm_pools: HashMap::new(),
            serum_markets: HashMap::new(),
            lending_markets: HashMap::new(),
            gas_price: GasPriceInfo {
                base_fee: 5000,
                priority_fee_percentiles: [1000, 5000, 10000, 20000, 50000],
            },
            mempool_density: 0.5,
        };
        
        let strategy = evaluator.evaluate_sandwich(&pool, &snapshot);
        assert!(strategy.is_some());
    }

    #[test]
    fn test_equilibrium_finder() {
        let finder = EquilibriumFinder::new();
        let payoff_matrix = PayoffMatrix {
            leader_payoffs: vec![vec![100.0, 50.0], vec![75.0, 125.0]],
            follower_payoffs: vec![vec![vec![50.0, 25.0], vec![40.0, 60.0]]],
        };
        
        let follower_responses = vec![
            FollowerResponse {
                follower_address: Pubkey::new_unique(),
                strategy_index: Some(0),
                reaction_probability: 0.8,
            }
        ];
        
        let risk_params = RiskParameters {
            max_position_size: 1_000_000_000,
            max_slippage: 0.02,
            min_profit_threshold: 0.001,
            gas_multiplier: 1.5,
        };
        
        let result = finder.find_stackelberg_equilibrium(&payoff_matrix, &follower_responses, &risk_params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_execution_engine() {
        let engine = ExecutionEngine::new();
        let strategy = CandidateStrategy {
            strategy_type: StrategyType::AtomicArbitrage,
            target_pools: vec![Pubkey::new_unique(), Pubkey::new_unique()],
            expected_profit: 0.05,
            capital_required: 1_000_000_000,
            gas_estimate: 200_000,
            priority_fee: 10_000,
        };
        
        let state = create_test_game_state();
        let result = engine.build_instructions(&strategy, &state);
        assert!(result.is_err()); // Should fail because pools not in state
    }

    #[tokio::test]
    async fn test_adaptive_controller() {
        let initial_state = create_test_game_state();
        let controller = AdaptiveGameController::new(initial_state);
        
        // Test finding profitable strategy
        let strategy = controller.find_profitable_strategy().await;
        assert!(strategy.is_ok());
    }

    #[test]
    fn test_gas_price_calculations() {
        let solver = StackelbergGameSolver::new(create_test_game_state());
        let strategy = CandidateStrategy {
            strategy_type: StrategyType::Liquidation,
            target_pools: vec![Pubkey::new_unique()],
            expected_profit: 0.1,
            capital_required: 5_000_000_000,
            gas_estimate: 400_000,
            priority_fee: 20_000,
        };
        
        let equilibrium = StackelbergEquilibrium {
            leader_strategy_index: 0,
            expected_payoff: 100.0,
            competition_intensity: 0.7,
            success_probability: 0.85,
        };
        
        let state = solver.state.read();
        let priority_fee = solver.calculate_optimal_priority_fee(&strategy, &equilibrium, &state);
        assert!(priority_fee > 20_000);
    }
}

// Re-export commonly used types
pub use self::{
    StackelbergGameState as GameState,
    StackelbergGameSolver as GameSolver,
    OptimalStrategy as Strategy,
    GameError as Error,
    AdaptiveGameController as Controller,
};

// Module initialization
pub fn initialize() -> Result<Controller, Error> {
    let leader_wallet = Pubkey::from_str("11111111111111111111111111111111").unwrap();
    
    let initial_state = StackelbergGameState {
        slot: 0,
        blockhash: Hash::default(),
        leader: LeaderState {
            wallet: leader_wallet,
            token_accounts: HashMap::new(),
            sol_balance: 10_000_000_000, // 10 SOL
            active_positions: vec![],
            gas_budget: 1_000_000_000, // 1 SOL for gas
            risk_params: RiskParameters {
                max_position_size: 5_000_000_000, // 5 SOL max per position
                max_slippage: 0.02, // 2% max slippage
                min_profit_threshold: 0.001, // 0.1% minimum profit
                gas_multiplier: 1.5, // 1.5x gas multiplier for priority
            },
        },
        followers: vec![
            FollowerState {
                address: Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap(),
                strategy_type: FollowerType::ArbitrageurBot,
                estimated_capital: 100_000_000_000,
                reaction_latency: 50,
                historical_behavior: BehaviorMetrics {
                    avg_txn_size: 1_000_000_000,
                    success_rate: 0.75,
                    avg_priority_fee: 10_000,
                    front_run_attempts: 1500,
                    sandwich_frequency: 0.3,
                },
            },
            FollowerState {
                address: Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap(),
                strategy_type: FollowerType::LiquidationBot,
                estimated_capital: 50_000_000_000,
                reaction_latency: 100,
                historical_behavior: BehaviorMetrics {
                    avg_txn_size: 5_000_000_000,
                    success_rate: 0.9,
                    avg_priority_fee: 50_000,
                    front_run_attempts: 200,
                    sandwich_frequency: 0.0,
                },
            },
        ],
        market_snapshot: MarketSnapshot {
            amm_pools: HashMap::new(),
            serum_markets: HashMap::new(),
            lending_markets: HashMap::new(),
            gas_price: GasPriceInfo {
                base_fee: 5000,
                priority_fee_percentiles: [1000, 5000, 10000, 20000, 50000],
            },
            mempool_density: 0.5,
        },
        historical_data: HistoricalGameData {
            outcomes: VecDeque::with_capacity(10000),
            strategy_performance: HashMap::new(),
        },
    };
    
    Ok(Controller::new(initial_state))
}

// Production entry point
pub async fn run_production() -> Result<(), Error> {
    let controller = initialize()?;
    controller.run().await
}

