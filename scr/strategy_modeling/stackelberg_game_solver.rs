use solana_program::pubkey::Pubkey;
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use parking_lot::RwLock;
use arrayvec::ArrayVec;
use fixed::types::I80F48;
use ahash::AHashMap;

const MAX_STRATEGIES: usize = 64;
const CONVERGENCE_THRESHOLD: f64 = 1e-12;
const MAX_ITERATIONS: usize = 2000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.995;
const MIN_PROFIT_THRESHOLD: u64 = 50_000; // 0.00005 SOL
const SLIPPAGE_BUFFER_BPS: u64 = 30; // 0.3%
const GAS_SAFETY_MARGIN: u64 = 15_000;
const NASH_EQUILIBRIUM_EPSILON: f64 = 1e-10;
const HISTORICAL_WINDOW: usize = 2048;
const VOLATILITY_DECAY: f64 = 0.94;
const COMPETITION_DECAY: f64 = 0.88;
const MAX_POSITION_SIZE_BPS: u64 = 500; // 5% of pool
const REORG_RISK_THRESHOLD: f64 = 0.001;
const INFORMATION_DECAY_RATE: f64 = 0.997;
const MEV_EXTRACTION_CAP: f64 = 0.95;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Strategy {
    pub action_type: ActionType,
    pub amount: u64,
    pub priority_fee: u64,
    pub compute_units: u32,
    pub target_pool: Pubkey,
    pub entry_price: I80F48,
    pub expected_profit: I80F48,
    pub risk_score: f64,
    pub execution_probability: f64,
    pub time_sensitivity: f64,
    pub competition_level: u8,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ActionType {
    Arbitrage = 0,
    Sandwich = 1,
    Liquidation = 2,
    JitLiquidity = 3,
    AtomicArbitrage = 4,
    FlashLoan = 5,
}

#[derive(Clone, Debug)]
pub struct GameState {
    pub leader_strategies: ArrayVec<Strategy, MAX_STRATEGIES>,
    pub follower_responses: AHashMap<u64, ArrayVec<Strategy, MAX_STRATEGIES>>,
    pub market_conditions: MarketConditions,
    pub historical_outcomes: CircularBuffer<Outcome>,
    pub competitor_models: BTreeMap<Pubkey, CompetitorModel>,
    pub network_state: NetworkState,
}

#[derive(Clone, Copy, Debug)]
pub struct MarketConditions {
    pub liquidity_depth: [u64; 8],
    pub volatility: f64,
    pub realized_volatility: f64,
    pub competitor_count: u32,
    pub avg_priority_fee: u64,
    pub p99_priority_fee: u64,
    pub block_space_demand: f64,
    pub slot_time_remaining: u64,
    pub oracle_confidence: f64,
    pub market_impact_coefficient: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct NetworkState {
    pub slot: u64,
    pub recent_blockhash: [u8; 32],
    pub leader_schedule: [u8; 32],
    pub network_congestion: f64,
    pub compute_unit_price: u64,
    pub account_write_locks: u32,
}

#[derive(Clone, Debug)]
pub struct CompetitorModel {
    pub pubkey: Pubkey,
    pub avg_reaction_time: u64,
    pub strategy_distribution: [f64; 6],
    pub capital_estimate: u64,
    pub success_rate: f64,
    pub aggressiveness: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct Outcome {
    pub profit: i64,
    pub gas_used: u64,
    pub compute_units: u32,
    pub success: bool,
    pub competitor_actions: u32,
    pub actual_slippage: f64,
    pub execution_time: u64,
    pub reorg_occurred: bool,
}

pub struct CircularBuffer<T> {
    buffer: Vec<T>,
    head: usize,
    size: usize,
    capacity: usize,
}

impl<T: Clone> CircularBuffer<T> {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            head: 0,
            size: 0,
            capacity,
        }
    }

    fn push(&mut self, item: T) {
        if self.size < self.capacity {
            self.buffer.push(item);
            self.size += 1;
        } else {
            self.buffer[self.head] = item;
            self.head = (self.head + 1) % self.capacity;
        }
    }

    fn iter(&self) -> impl Iterator<Item = &T> {
        let start = if self.size < self.capacity { 0 } else { self.head };
        (0..self.size).map(move |i| &self.buffer[(start + i) % self.capacity])
    }

    fn recent(&self, n: usize) -> impl Iterator<Item = &T> {
        let take = n.min(self.size);
        let start = (self.head + self.size - take) % self.capacity;
        (0..take).map(move |i| &self.buffer[(start + i) % self.capacity])
    }
}

pub struct StackelbergGameSolver {
    game_state: Arc<RwLock<GameState>>,
    payoff_calculator: PayoffCalculator,
    equilibrium_finder: EquilibriumFinder,
    risk_engine: RiskEngine,
    optimization_cache: Arc<RwLock<OptimizationCache>>,
}

struct OptimizationCache {
    strategy_cache: AHashMap<u64, (Strategy, u64)>,
    equilibrium_cache: AHashMap<u64, (Strategy, u64)>,
    cache_hits: u64,
    cache_misses: u64,
}

impl StackelbergGameSolver {
    pub fn new() -> Self {
        Self {
            game_state: Arc::new(RwLock::new(GameState {
                leader_strategies: ArrayVec::new(),
                follower_responses: AHashMap::new(),
                market_conditions: MarketConditions {
                    liquidity_depth: [0; 8],
                    volatility: 0.0,
                    realized_volatility: 0.0,
                    competitor_count: 0,
                    avg_priority_fee: 0,
                    p99_priority_fee: 0,
                    block_space_demand: 0.0,
                    slot_time_remaining: 400_000,
                    oracle_confidence: 1.0,
                    market_impact_coefficient: 0.1,
                },
                historical_outcomes: CircularBuffer::new(HISTORICAL_WINDOW),
                competitor_models: BTreeMap::new(),
                network_state: NetworkState {
                    slot: 0,
                    recent_blockhash: [0; 32],
                    leader_schedule: [0; 32],
                    network_congestion: 0.0,
                    compute_unit_price: 1,
                    account_write_locks: 0,
                },
            })),
            payoff_calculator: PayoffCalculator::new(),
            equilibrium_finder: EquilibriumFinder::new(),
            risk_engine: RiskEngine::new(),
            optimization_cache: Arc::new(RwLock::new(OptimizationCache {
                strategy_cache: AHashMap::new(),
                equilibrium_cache: AHashMap::new(),
                cache_hits: 0,
                cache_misses: 0,
            })),
        }
    }

    pub fn solve_optimal_strategy(
        &self,
        opportunities: &[OpportunityInfo],
        market_state: &MarketState,
    ) -> Result<Strategy, SolverError> {
        if opportunities.is_empty() {
            return Err(SolverError::InsufficientData);
        }

        let cache_key = self.compute_cache_key(opportunities, market_state);
        
        {
            let cache = self.optimization_cache.read();
            if let Some((strategy, timestamp)) = cache.equilibrium_cache.get(&cache_key) {
                if market_state.timestamp - timestamp < 100 {
                    return Ok(*strategy);
                }
            }
        }

        let mut game_state = self.game_state.write();
        
        game_state.market_conditions = self.extract_market_conditions(market_state);
        game_state.network_state = self.extract_network_state(market_state);
        
        self.update_competitor_models(&mut game_state, market_state);
        
        let leader_strategies = self.generate_leader_strategies(
            opportunities,
            &game_state.market_conditions,
            &game_state.network_state,
        )?;
        
        if leader_strategies.is_empty() {
            return Err(SolverError::NoViableStrategy);
        }
        
        game_state.leader_strategies = leader_strategies;
        
        let follower_responses = self.predict_follower_responses(
            &game_state.leader_strategies,
            &game_state.market_conditions,
            &game_state.competitor_models,
        );
        game_state.follower_responses = follower_responses;
        
        let payoff_matrix = self.payoff_calculator.calculate_payoffs(
            &game_state.leader_strategies,
            &game_state.follower_responses,
            &game_state.market_conditions,
            &game_state.network_state,
        )?;
        
        let equilibrium = self.equilibrium_finder.find_stackelberg_equilibrium(
            &payoff_matrix,
            &game_state.leader_strategies,
            &game_state.follower_responses,
        )?;
        
        let optimal_strategy = self.refine_strategy(
            equilibrium,
            &game_state.historical_outcomes,
            &game_state.market_conditions,
        )?;
        
        let risk_adjusted = self.risk_engine.adjust_strategy(
            optimal_strategy,
            &game_state.historical_outcomes,
            &game_state.market_conditions,
        )?;
        
        {
            let mut cache = self.optimization_cache.write();
            cache.equilibrium_cache.insert(cache_key, (risk_adjusted, market_state.timestamp));
        }
        
        Ok(risk_adjusted)
    }

    fn compute_cache_key(&self, opportunities: &[OpportunityInfo], market_state: &MarketState) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        let mut hasher = DefaultHasher::new();
        opportunities.len().hash(&mut hasher);
        for opp in opportunities.iter().take(3) {
            opp.opportunity_type.hash(&mut hasher);
            opp.amount.hash(&mut hasher);
        }
        market_state.slot.hash(&mut hasher);
        hasher.finish()
    }

    fn extract_market_conditions(&self, market_state: &MarketState) -> MarketConditions {
        let volatility = self.calculate_garch_volatility(&market_state.price_history);
        let realized_vol = self.calculate_realized_volatility(&market_state.price_history);
        let block_space_demand = self.estimate_block_space_demand(market_state);
        let market_impact = self.calibrate_market_impact(&market_state.trade_history);
        
        MarketConditions {
            liquidity_depth: market_state.liquidity_distribution,
            volatility,
            realized_volatility: realized_vol,
            competitor_count: market_state.active_bots,
            avg_priority_fee: market_state.avg_priority_fee,
            p99_priority_fee: market_state.p99_priority_fee,
            block_space_demand,
            slot_time_remaining: market_state.slot_time_remaining,
            oracle_confidence: market_state.oracle_confidence,
            market_impact_coefficient: market_impact,
        }
    }

    fn extract_network_state(&self, market_state: &MarketState) -> NetworkState {
        NetworkState {
            slot: market_state.slot,
            recent_blockhash: market_state.recent_blockhash,
            leader_schedule: market_state.leader_schedule,
            network_congestion: market_state.network_congestion,
            compute_unit_price: market_state.compute_unit_price,
            account_write_locks: market_state.account_write_locks,
        }
    }

    fn calculate_garch_volatility(&self, price_history: &[u64]) -> f64 {
        if price_history.len() < 20 {
            return 0.02;
        }
        
        let returns: Vec<f64> = price_history.windows(2)
            .map(|w| ((w[1] as f64) / (w[0] as f64)).ln())
            .collect();
        
        let omega = 0.00001;
        let alpha = 0.15;
        let beta = 0.85;
        
        let mut variance = returns.iter()
            .map(|r| r.powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        for ret in returns.iter().rev().take(10) {
            variance = omega + alpha * ret.powi(2) + beta * variance;
        }
        
        variance.sqrt()
    }

    fn calculate_realized_volatility(&self, price_history: &[u64]) -> f64 {
        if price_history.len() < 5 {
            return 0.01;
        }
        
        let returns: Vec<f64> = price_history.windows(2)
            .map(|w| ((w[1] as f64) / (w[0] as f64)).ln())
            .collect();
        
                let squared_returns: f64 = returns.iter().map(|r| r.powi(2)).sum::<f64>();
        (squared_returns / returns.len() as f64).sqrt() * (252.0_f64).sqrt()
    }

    fn calibrate_market_impact(&self, trade_history: &[TradeInfo]) -> f64 {
        if trade_history.len() < 50 {
            return 0.1;
        }
        
        let mut impacts = Vec::new();
        for trade in trade_history.iter() {
            let impact = (trade.price_after - trade.price_before).abs() as f64 / trade.price_before as f64;
            let normalized_size = trade.size as f64 / trade.pool_liquidity as f64;
            if normalized_size > 0.0 {
                impacts.push(impact / normalized_size);
            }
        }
        
        if impacts.is_empty() {
            return 0.1;
        }
        
        impacts.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        impacts[impacts.len() * 75 / 100].min(1.0)
    }

    fn estimate_block_space_demand(&self, market_state: &MarketState) -> f64 {
        let base_demand = market_state.pending_transactions as f64 / market_state.block_capacity as f64;
        let fee_pressure = (market_state.p99_priority_fee as f64) / 10_000_000.0;
        let compute_pressure = market_state.used_compute_units as f64 / market_state.max_compute_units as f64;
        
        (base_demand * 0.4 + fee_pressure * 0.4 + compute_pressure * 0.2).min(1.0).max(0.0)
    }

    fn update_competitor_models(&self, game_state: &mut GameState, market_state: &MarketState) {
        for competitor in market_state.recent_competitors.iter() {
            let model = game_state.competitor_models
                .entry(competitor.pubkey)
                .or_insert(CompetitorModel {
                    pubkey: competitor.pubkey,
                    avg_reaction_time: 0,
                    strategy_distribution: [0.0; 6],
                    capital_estimate: 0,
                    success_rate: 0.0,
                    aggressiveness: 0.5,
                });
            
            model.avg_reaction_time = (model.avg_reaction_time as f64 * 0.9 + competitor.reaction_time as f64 * 0.1) as u64;
            model.success_rate = model.success_rate * 0.95 + competitor.success_rate * 0.05;
            model.capital_estimate = competitor.observed_capital.max(model.capital_estimate);
            model.aggressiveness = model.aggressiveness * 0.9 + competitor.fee_aggressiveness * 0.1;
            
            if let Some(action_idx) = ActionType::to_index(competitor.last_action) {
                model.strategy_distribution[action_idx] += 0.1;
                let sum: f64 = model.strategy_distribution.iter().sum();
                if sum > 0.0 {
                    for i in 0..6 {
                        model.strategy_distribution[i] /= sum;
                    }
                }
            }
        }
    }

    fn generate_leader_strategies(
        &self,
        opportunities: &[OpportunityInfo],
        market_conditions: &MarketConditions,
        network_state: &NetworkState,
    ) -> Result<ArrayVec<Strategy, MAX_STRATEGIES>, SolverError> {
        let mut strategies = ArrayVec::new();
        let mut opportunity_scores: Vec<(usize, f64)> = Vec::new();
        
        for (idx, opp) in opportunities.iter().enumerate() {
            let score = self.score_opportunity(opp, market_conditions, network_state);
            if score > 0.0 {
                opportunity_scores.push((idx, score));
            }
        }
        
        opportunity_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        for (idx, _score) in opportunity_scores.iter().take(MAX_STRATEGIES / 4) {
            let opp = &opportunities[*idx];
            let base_strategies = match opp.opportunity_type {
                OpportunityType::Arbitrage => self.generate_arbitrage_strategies(opp, market_conditions, network_state)?,
                OpportunityType::Sandwich => self.generate_sandwich_strategies(opp, market_conditions, network_state)?,
                OpportunityType::Liquidation => self.generate_liquidation_strategies(opp, market_conditions, network_state)?,
                OpportunityType::JitLiquidity => self.generate_jit_strategies(opp, market_conditions, network_state)?,
                OpportunityType::AtomicArbitrage => self.generate_atomic_strategies(opp, market_conditions, network_state)?,
                OpportunityType::FlashLoan => self.generate_flashloan_strategies(opp, market_conditions, network_state)?,
            };
            
            for strategy in base_strategies {
                if self.validate_strategy_constraints(&strategy, market_conditions, network_state) {
                    if strategies.try_push(strategy).is_err() {
                        break;
                    }
                }
            }
        }
        
        Ok(strategies)
    }

    fn score_opportunity(&self, opp: &OpportunityInfo, market: &MarketConditions, network: &NetworkState) -> f64 {
        let profit_score = (opp.profit_estimate as f64 / 1_000_000.0).min(100.0) / 100.0;
        let confidence_score = opp.confidence;
        let timing_score = ((opp.deadline_slot - network.slot) as f64 / 100.0).min(1.0).max(0.0);
        let competition_score = 1.0 / (1.0 + market.competitor_count as f64 * 0.05);
        
        profit_score * 0.4 + confidence_score * 0.3 + timing_score * 0.2 + competition_score * 0.1
    }

    fn validate_strategy_constraints(&self, strategy: &Strategy, market: &MarketConditions, network: &NetworkState) -> bool {
        if strategy.amount == 0 || strategy.priority_fee == 0 {
            return false;
        }
        
        let max_position = market.liquidity_depth[0] * MAX_POSITION_SIZE_BPS / 10000;
        if strategy.amount > max_position {
            return false;
        }
        
        if strategy.compute_units > network.max_compute_units {
            return false;
        }
        
        if strategy.risk_score > 0.95 {
            return false;
        }
        
        true
    }

    fn generate_arbitrage_strategies(
        &self,
        opp: &OpportunityInfo,
        market: &MarketConditions,
        network: &NetworkState,
    ) -> Result<Vec<Strategy>, SolverError> {
        let mut strategies = Vec::new();
        
        let optimal_amount = self.calculate_optimal_arbitrage_size(opp, market);
        let amounts = [
            optimal_amount,
            (optimal_amount as f64 * 0.8) as u64,
            (optimal_amount as f64 * 0.6) as u64,
            (optimal_amount as f64 * 1.2) as u64,
        ];
        
        for &amount in &amounts {
            if amount == 0 { continue; }
            
            let (expected_profit, entry_price) = self.calculate_precise_arbitrage_profit(amount, opp, market);
            
            if expected_profit.to_num::<i64>() > MIN_PROFIT_THRESHOLD as i64 {
                let priority_fee = self.calculate_dynamic_priority_fee(
                    expected_profit,
                    market,
                    network,
                    ActionType::Arbitrage,
                );
                
                let compute_units = self.estimate_compute_units(ActionType::Arbitrage, amount);
                let execution_prob = self.calculate_execution_probability(priority_fee, compute_units, market, network);
                
                strategies.push(Strategy {
                    action_type: ActionType::Arbitrage,
                    amount,
                    priority_fee,
                    compute_units,
                    target_pool: opp.target_pool,
                    entry_price,
                    expected_profit,
                    risk_score: self.calculate_comprehensive_risk(amount, market, ActionType::Arbitrage),
                    execution_probability: execution_prob,
                    time_sensitivity: self.calculate_time_sensitivity(opp, network),
                    competition_level: self.estimate_competition_level(opp, market),
                });
            }
        }
        
        Ok(strategies)
    }

    fn calculate_optimal_arbitrage_size(&self, opp: &OpportunityInfo, market: &MarketConditions) -> u64 {
        let available_liquidity = market.liquidity_depth[0].min(market.liquidity_depth[1]);
        let price_impact_limit = 0.005; // 0.5% max impact
        
        let mut low = 0u64;
        let mut high = available_liquidity.min(opp.amount * 2);
        
        while high - low > 1000 {
            let mid = (low + high) / 2;
            let impact = self.calculate_aggregate_price_impact(mid, market);
            
            if impact < price_impact_limit {
                low = mid;
            } else {
                high = mid;
            }
        }
        
        low
    }

    fn calculate_precise_arbitrage_profit(&self, amount: u64, opp: &OpportunityInfo, market: &MarketConditions) -> (I80F48, I80F48) {
        let amount_f = I80F48::from_num(amount) / I80F48::from_num(1_000_000_000);
        
        let buy_impact = self.calculate_precise_price_impact(amount, market.liquidity_depth[0], true);
        let sell_impact = self.calculate_precise_price_impact(amount, market.liquidity_depth[1], false);
        
        let entry_price = I80F48::from_num(opp.price_a) * (I80F48::ONE + buy_impact);
        let exit_price = I80F48::from_num(opp.price_b) * (I80F48::ONE - sell_impact);
        
        let gross_profit = amount_f * (exit_price - entry_price);
        let gas_cost = I80F48::from_num(GAS_SAFETY_MARGIN + market.avg_priority_fee) / I80F48::from_num(1_000_000_000);
        let net_profit = gross_profit - gas_cost;
        
        (net_profit, entry_price)
    }

    fn calculate_precise_price_impact(&self, amount: u64, liquidity: u64, is_buy: bool) -> I80F48 {
        let x = I80F48::from_num(amount);
        let l = I80F48::from_num(liquidity);
        let k = I80F48::from_num(0.997); // 0.3% fee
        
        let raw_impact = (x * k) / (l + x * k);
        
        if is_buy {
            raw_impact
        } else {
            raw_impact * I80F48::from_num(1.02) // Asymmetric impact for sells
        }
    }

    fn calculate_aggregate_price_impact(&self, amount: u64, market: &MarketConditions) -> f64 {
        let mut total_impact = 0.0;
        let mut remaining = amount;
        
        for (i, &depth) in market.liquidity_depth.iter().enumerate() {
            if remaining == 0 || depth == 0 { break; }
            
            let take = remaining.min(depth / 10);
            let impact = self.calculate_precise_price_impact(take, depth, true).to_num::<f64>();
            total_impact += impact * (take as f64 / amount as f64);
            remaining -= take;
        }
        
        total_impact
    }

    fn generate_sandwich_strategies(
        &self,
        opp: &OpportunityInfo,
        market: &MarketConditions,
        network: &NetworkState,
    ) -> Result<Vec<Strategy>, SolverError> {
        let mut strategies = Vec::new();
        
        let victim_amount = opp.amount;
        let victim_direction = opp.direction;
        let max_sandwich = self.calculate_max_sandwich_size(victim_amount, market);
        
        let sandwich_sizes = [
            (victim_amount as f64 * 0.5) as u64,
            victim_amount,
            (victim_amount as f64 * 1.5) as u64,
            max_sandwich,
        ];
        
        for &size in &sandwich_sizes {
            if size == 0 || size > max_sandwich { continue; }
            
            let (profit, entry_price) = self.calculate_sandwich_profit_advanced(size, victim_amount, victim_direction, market);
            
            if profit.to_num::<i64>() > MIN_PROFIT_THRESHOLD as i64 * 2 {
                let priority_fee = self.calculate_sandwich_priority_fee(profit, market, network);
                let compute_units = self.estimate_compute_units(ActionType::Sandwich, size) * 2;
                
                strategies.push(Strategy {
                    action_type: ActionType::Sandwich,
                    amount: size,
                    priority_fee,
                    compute_units,
                    target_pool: opp.target_pool,
                    entry_price,
                    expected_profit: profit,
                    risk_score: self.calculate_sandwich_risk(size, victim_amount, market),
                    execution_probability: self.calculate_sandwich_execution_prob(priority_fee, market, network),
                    time_sensitivity: 0.95,
                    competition_level: self.estimate_sandwich_competition(market),
                });
            }
        }
        
        Ok(strategies)
    }

    fn calculate_max_sandwich_size(&self, victim_amount: u64, market: &MarketConditions) -> u64 {
        let pool_limit = market.liquidity_depth[0] * 15 / 100; // 15% of pool
        let capital_limit = victim_amount * 3; // 3x victim size
        pool_limit.min(capital_limit)
    }

        fn calculate_sandwich_profit_advanced(
        &self,
        sandwich_size: u64,
        victim_size: u64,
        victim_buy: bool,
        market: &MarketConditions,
    ) -> (I80F48, I80F48) {
        let sandwich_f = I80F48::from_num(sandwich_size) / I80F48::from_num(1_000_000_000);
        let victim_f = I80F48::from_num(victim_size) / I80F48::from_num(1_000_000_000);
        
        // Model the three-step sandwich
        let initial_liquidity = I80F48::from_num(market.liquidity_depth[0]);
        
        // Step 1: Front-run transaction
        let front_impact = if victim_buy {
            self.calculate_precise_price_impact(sandwich_size, market.liquidity_depth[0], true)
        } else {
            self.calculate_precise_price_impact(sandwich_size, market.liquidity_depth[0], false)
        };
        
        let entry_price = I80F48::from_num(market.current_price) * (I80F48::ONE + front_impact);
        let liquidity_after_front = initial_liquidity - sandwich_f;
        
        // Step 2: Victim transaction
        let victim_impact = if victim_buy {
            self.calculate_precise_price_impact(victim_size, liquidity_after_front.to_num::<u64>(), true)
        } else {
            self.calculate_precise_price_impact(victim_size, liquidity_after_front.to_num::<u64>(), false)
        };
        
        let price_after_victim = entry_price * (I80F48::ONE + victim_impact);
        
        // Step 3: Back-run transaction
        let back_impact = if victim_buy {
            self.calculate_precise_price_impact(sandwich_size, (liquidity_after_front + victim_f).to_num::<u64>(), false)
        } else {
            self.calculate_precise_price_impact(sandwich_size, (liquidity_after_front - victim_f).to_num::<u64>(), true)
        };
        
        let exit_price = price_after_victim * (I80F48::ONE - back_impact);
        
        // Calculate profit
        let position_profit = sandwich_f * (exit_price - entry_price);
        let gas_cost = I80F48::from_num(GAS_SAFETY_MARGIN * 2 + market.avg_priority_fee * 2) / I80F48::from_num(1_000_000_000);
        let slippage_cost = sandwich_f * exit_price * I80F48::from_num(SLIPPAGE_BUFFER_BPS) / I80F48::from_num(10000);
        
        let net_profit = position_profit - gas_cost - slippage_cost;
        
        (net_profit, entry_price)
    }

    fn calculate_sandwich_priority_fee(&self, expected_profit: I80F48, market: &MarketConditions, network: &NetworkState) -> u64 {
        let base_fee = market.p99_priority_fee;
        let profit_share = expected_profit.to_num::<u64>() * 35 / 100; // 35% of profit for priority
        let congestion_multiplier = 1.0 + network.network_congestion * 0.5;
        let competition_boost = 1.0 + (market.competitor_count as f64 * 0.15).min(2.0);
        
        let dynamic_fee = (base_fee as f64 * congestion_multiplier * competition_boost) as u64;
        dynamic_fee.max(base_fee).min(profit_share)
    }

    fn calculate_sandwich_risk(&self, sandwich_size: u64, victim_size: u64, market: &MarketConditions) -> f64 {
        let size_ratio = sandwich_size as f64 / victim_size as f64;
        let liquidity_usage = sandwich_size as f64 / market.liquidity_depth[0] as f64;
        let volatility_risk = market.volatility * 50.0;
        let competition_risk = (market.competitor_count as f64 / 10.0).min(1.0);
        
        let base_risk = (size_ratio * 0.2 + liquidity_usage * 0.3 + volatility_risk * 0.3 + competition_risk * 0.2).min(1.0);
        
        // Adjust for market conditions
        let oracle_adjustment = 1.0 + (1.0 - market.oracle_confidence) * 0.5;
        (base_risk * oracle_adjustment).min(0.95)
    }

    fn calculate_sandwich_execution_prob(&self, priority_fee: u64, market: &MarketConditions, network: &NetworkState) -> f64 {
        let fee_percentile = self.calculate_fee_percentile(priority_fee, market);
        let timing_factor = (network.slot_time_remaining as f64 / 400_000.0).max(0.1);
        let congestion_penalty = 1.0 - network.network_congestion * 0.3;
        
        (fee_percentile * 0.5 + timing_factor * 0.3 + congestion_penalty * 0.2).min(0.98)
    }

    fn estimate_sandwich_competition(&self, market: &MarketConditions) -> u8 {
        ((market.competitor_count as f64 * 0.7) as u8).min(10)
    }

    fn generate_liquidation_strategies(
        &self,
        opp: &OpportunityInfo,
        market: &MarketConditions,
        network: &NetworkState,
    ) -> Result<Vec<Strategy>, SolverError> {
        let mut strategies = Vec::new();
        
        let collateral_value = opp.collateral_amount;
        let debt_value = opp.debt_amount;
        let liquidation_bonus = self.calculate_liquidation_bonus(opp.protocol_id);
        
        let repay_amounts = [
            debt_value / 2,
            debt_value * 3 / 4,
            debt_value,
        ];
        
        for &repay_amount in &repay_amounts {
            let collateral_received = self.calculate_liquidation_collateral(repay_amount, debt_value, collateral_value, liquidation_bonus);
            let (profit, entry_price) = self.calculate_liquidation_profit(repay_amount, collateral_received, market);
            
            if profit.to_num::<i64>() > MIN_PROFIT_THRESHOLD as i64 * 3 {
                let priority_fee = self.calculate_liquidation_priority_fee(profit, market, network);
                let compute_units = self.estimate_liquidation_compute(opp.protocol_id, repay_amount);
                
                strategies.push(Strategy {
                    action_type: ActionType::Liquidation,
                    amount: repay_amount,
                    priority_fee,
                    compute_units,
                    target_pool: opp.target_pool,
                    entry_price,
                    expected_profit: profit,
                    risk_score: self.calculate_liquidation_risk(repay_amount, market, opp),
                    execution_probability: self.calculate_liquidation_execution_prob(priority_fee, market, network),
                    time_sensitivity: 0.99, // Very time sensitive
                    competition_level: self.estimate_liquidation_competition(opp.health_factor),
                });
            }
        }
        
        Ok(strategies)
    }

    fn calculate_liquidation_bonus(&self, protocol_id: u8) -> I80F48 {
        match protocol_id {
            0 => I80F48::from_num(1.05), // 5% bonus
            1 => I80F48::from_num(1.08), // 8% bonus
            2 => I80F48::from_num(1.10), // 10% bonus
            _ => I80F48::from_num(1.05),
        }
    }

    fn calculate_liquidation_collateral(&self, repay: u64, total_debt: u64, total_collateral: u64, bonus: I80F48) -> u64 {
        let repay_ratio = I80F48::from_num(repay) / I80F48::from_num(total_debt);
        let base_collateral = I80F48::from_num(total_collateral) * repay_ratio;
        (base_collateral * bonus).to_num::<u64>()
    }

    fn calculate_liquidation_profit(&self, repay_amount: u64, collateral_received: u64, market: &MarketConditions) -> (I80F48, I80F48) {
        let repay_f = I80F48::from_num(repay_amount) / I80F48::from_num(1_000_000_000);
        let collateral_f = I80F48::from_num(collateral_received) / I80F48::from_num(1_000_000_000);
        
        let debt_cost = repay_f * I80F48::from_num(market.current_price);
        let collateral_value = collateral_f * I80F48::from_num(market.collateral_price);
        
        let swap_impact = self.calculate_precise_price_impact(collateral_received, market.liquidity_depth[2], false);
        let net_collateral_value = collateral_value * (I80F48::ONE - swap_impact);
        
        let gross_profit = net_collateral_value - debt_cost;
        let gas_cost = I80F48::from_num(GAS_SAFETY_MARGIN * 3) / I80F48::from_num(1_000_000_000);
        
        (gross_profit - gas_cost, debt_cost / repay_f)
    }

    fn calculate_liquidation_priority_fee(&self, profit: I80F48, market: &MarketConditions, network: &NetworkState) -> u64 {
        // Liquidations are extremely competitive
        let profit_u64 = profit.to_num::<u64>();
        let max_fee = profit_u64 * 40 / 100; // Up to 40% of profit
        let competitive_fee = market.p99_priority_fee * 150 / 100; // 1.5x p99
        
        max_fee.min(competitive_fee).max(market.p99_priority_fee)
    }

    fn calculate_liquidation_risk(&self, amount: u64, market: &MarketConditions, opp: &OpportunityInfo) -> f64 {
        let health_risk = (1.0 / opp.health_factor).min(1.0);
        let oracle_risk = 1.0 - market.oracle_confidence;
        let liquidity_risk = (amount as f64 / market.liquidity_depth[2] as f64).min(1.0);
        let volatility_risk = market.volatility * 20.0;
        
        (health_risk * 0.3 + oracle_risk * 0.2 + liquidity_risk * 0.3 + volatility_risk * 0.2).min(0.9)
    }

    fn calculate_liquidation_execution_prob(&self, priority_fee: u64, market: &MarketConditions, network: &NetworkState) -> f64 {
        let fee_score = (priority_fee as f64 / market.p99_priority_fee as f64).min(2.0) / 2.0;
        let timing_score = (network.slot_time_remaining as f64 / 400_000.0).max(0.05);
        let competition_score = 1.0 / (1.0 + market.competitor_count as f64 * 0.2);
        
        (fee_score * 0.6 + timing_score * 0.2 + competition_score * 0.2).min(0.95)
    }

    fn estimate_liquidation_compute(&self, protocol_id: u8, amount: u64) -> u32 {
        let base_compute = match protocol_id {
            0 => 400_000,
            1 => 600_000,
            2 => 800_000,
            _ => 1_000_000,
        };
        
        let size_multiplier = (amount as f64 / 1_000_000_000.0).sqrt() + 1.0;
        (base_compute as f64 * size_multiplier) as u32
    }

    fn estimate_liquidation_competition(&self, health_factor: f64) -> u8 {
        if health_factor < 1.01 {
            10 // Maximum competition
        } else if health_factor < 1.05 {
            7
        } else if health_factor < 1.10 {
            4
        } else {
            2
        }
    }

    fn generate_jit_strategies(
        &self,
        opp: &OpportunityInfo,
        market: &MarketConditions,
        network: &NetworkState,
    ) -> Result<Vec<Strategy>, SolverError> {
        let mut strategies = Vec::new();
        
        let expected_volume = opp.amount;
        let pool_liquidity = market.liquidity_depth[0];
        let optimal_liquidity = self.calculate_optimal_jit_liquidity(expected_volume, pool_liquidity, market);
        
        let liquidity_amounts = [
            optimal_liquidity / 2,
            optimal_liquidity,
            optimal_liquidity * 3 / 2,
        ];
        
        for &liquidity in &liquidity_amounts {
            if liquidity == 0 { continue; }
            
            let (profit, entry_price) = self.calculate_jit_profit_advanced(liquidity, expected_volume, market);
            
            if profit.to_num::<i64>() > MIN_PROFIT_THRESHOLD as i64 {
                let priority_fee = self.calculate_jit_priority_fee(profit, market, network);
                let compute_units = self.estimate_compute_units(ActionType::JitLiquidity, liquidity);
                
                strategies.push(Strategy {
                    action_type: ActionType::JitLiquidity,
                    amount: liquidity,
                    priority_fee,
                    compute_units: compute_units * 2, // Add and remove
                    target_pool: opp.target_pool,
                    entry_price,
                    expected_profit: profit,
                    risk_score: self.calculate_jit_risk(liquidity, expected_volume, market),
                    execution_probability: self.calculate_jit_execution_prob(priority_fee, market, network),
                    time_sensitivity: 0.9,
                    competition_level: self.estimate_jit_competition(market),
                });
            }
        }
        
        Ok(strategies)
    }

    fn calculate_optimal_jit_liquidity(&self, volume: u64, current_liquidity: u64, market: &MarketConditions) -> u64 {
        let volume_f = volume as f64;
        let liquidity_f = current_liquidity as f64;
         let fee_tier = 0.003; // 0.3% fee tier
        
        // Optimal JIT liquidity to maximize fee capture
        let optimal = volume_f * 2.5 / (1.0 + market.volatility * 10.0);
        let max_allowed = liquidity_f * 0.4; // Don't exceed 40% of current liquidity
        
        optimal.min(max_allowed) as u64
    }

    fn calculate_jit_profit_advanced(&self, liquidity: u64, volume: u64, market: &MarketConditions) -> (I80F48, I80F48) {
        let liquidity_f = I80F48::from_num(liquidity) / I80F48::from_num(1_000_000_000);
        let volume_f = I80F48::from_num(volume) / I80F48::from_num(1_000_000_000);
        let current_liquidity = I80F48::from_num(market.liquidity_depth[0]) / I80F48::from_num(1_000_000_000);
        
        // Calculate share of fees
        let liquidity_share = liquidity_f / (current_liquidity + liquidity_f);
        let fee_rate = I80F48::from_num(0.003); // 0.3%
        let expected_fees = volume_f * fee_rate * liquidity_share;
        
        // Calculate impermanent loss
        let price_impact = self.calculate_precise_price_impact(volume, (current_liquidity + liquidity_f).to_num::<u64>(), true);
        let il_factor = price_impact * price_impact / I80F48::from_num(2);
        let impermanent_loss = liquidity_f * il_factor;
        
        // Gas costs for add and remove liquidity
        let gas_cost = I80F48::from_num(GAS_SAFETY_MARGIN * 2 + market.avg_priority_fee * 2) / I80F48::from_num(1_000_000_000);
        
        let net_profit = expected_fees - impermanent_loss - gas_cost;
        let entry_price = I80F48::from_num(market.current_price);
        
        (net_profit, entry_price)
    }

    fn calculate_jit_priority_fee(&self, profit: I80F48, market: &MarketConditions, network: &NetworkState) -> u64 {
        let profit_u64 = profit.to_num::<u64>();
        let base_fee = market.avg_priority_fee * 120 / 100; // 1.2x average
        let profit_share = profit_u64 * 25 / 100; // 25% of profit
        
        base_fee.max(profit_share).min(market.p99_priority_fee * 110 / 100)
    }

    fn calculate_jit_risk(&self, liquidity: u64, expected_volume: u64, market: &MarketConditions) -> f64 {
        let volume_uncertainty = 0.3; // Base uncertainty in volume prediction
        let il_risk = (market.volatility * 15.0).min(0.5);
        let timing_risk = 0.2; // Risk of missing the window
        let liquidity_ratio = liquidity as f64 / market.liquidity_depth[0] as f64;
        
        (volume_uncertainty * 0.3 + il_risk * 0.4 + timing_risk * 0.2 + liquidity_ratio * 0.1).min(0.85)
    }

    fn calculate_jit_execution_prob(&self, priority_fee: u64, market: &MarketConditions, network: &NetworkState) -> f64 {
        let fee_score = (priority_fee as f64 / market.avg_priority_fee as f64).min(2.0) / 2.0;
        let timing_score = (network.slot_time_remaining as f64 / 400_000.0).max(0.2);
        
        (fee_score * 0.6 + timing_score * 0.4).min(0.9)
    }

    fn estimate_jit_competition(&self, market: &MarketConditions) -> u8 {
        ((market.competitor_count as f64 * 0.5) as u8).min(8)
    }

    fn generate_atomic_strategies(
        &self,
        opp: &OpportunityInfo,
        market: &MarketConditions,
        network: &NetworkState,
    ) -> Result<Vec<Strategy>, SolverError> {
        let mut strategies = Vec::new();
        
        let paths = self.find_atomic_arbitrage_paths(opp, market);
        
        for path in paths.iter().take(3) {
            let optimal_amount = self.calculate_optimal_atomic_amount(path, market);
            let amounts = [optimal_amount * 8 / 10, optimal_amount, optimal_amount * 12 / 10];
            
            for &amount in &amounts {
                if amount == 0 { continue; }
                
                let (profit, entry_price) = self.calculate_atomic_profit(amount, path, market);
                
                if profit.to_num::<i64>() > MIN_PROFIT_THRESHOLD as i64 * 2 {
                    let priority_fee = self.calculate_atomic_priority_fee(profit, path.len(), market, network);
                    let compute_units = self.estimate_atomic_compute(path);
                    
                    strategies.push(Strategy {
                        action_type: ActionType::AtomicArbitrage,
                        amount,
                        priority_fee,
                        compute_units,
                        target_pool: path[0].pool,
                        entry_price,
                        expected_profit: profit,
                        risk_score: self.calculate_atomic_risk(amount, path, market),
                        execution_probability: self.calculate_atomic_execution_prob(priority_fee, compute_units, market, network),
                        time_sensitivity: 0.85,
                        competition_level: self.estimate_atomic_competition(market),
                    });
                }
            }
        }
        
        Ok(strategies)
    }
    fn find_atomic_arbitrage_paths(&self, opp: &OpportunityInfo, market: &MarketConditions) -> Vec<ArbitragePath> {
        let mut paths = Vec::new();
        let mut visited = AHashMap::new();
        let max_depth = 4;
        
        // Build adjacency list of pool connections
        let graph = self.build_pool_graph(opp, market);
        
        // DFS to find profitable cycles
        for (start_pool, edges) in graph.iter() {
            let mut current_path = vec![PathHop {
                pool: *start_pool,
                token_in: opp.token_a,
                token_out: opp.token_b,
                price: edges[0].price,
                liquidity: edges[0].liquidity,
                is_buy: true,
            }];
            
            self.dfs_find_cycles(
                *start_pool,
                *start_pool,
                opp.token_a,
                &graph,
                &mut visited,
                &mut current_path,
                &mut paths,
                1,
                max_depth,
                I80F48::ONE,
            );
        }
        
        // Sort by expected profitability
        paths.sort_by(|a, b| {
            let profit_a = self.estimate_path_profit(a, opp.amount);
            let profit_b = self.estimate_path_profit(b, opp.amount);
            profit_b.partial_cmp(&profit_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        paths.truncate(5); // Keep top 5 paths
        paths
    }

    fn build_pool_graph(&self, opp: &OpportunityInfo, market: &MarketConditions) -> AHashMap<Pubkey, Vec<PoolEdge>> {
        let mut graph = AHashMap::new();
        
        // Add known pool connections from opportunity info
        for pool in opp.connected_pools.iter() {
            let edges = pool.edges.iter().map(|e| PoolEdge {
                to_pool: e.to_pool,
                token_in: e.token_in,
                token_out: e.token_out,
                price: e.price,
                liquidity: e.liquidity,
                fee_bps: e.fee_bps,
            }).collect();
            graph.insert(pool.pubkey, edges);
        }
        
        graph
    }

    fn dfs_find_cycles(
        &self,
        current: Pubkey,
        start: Pubkey,
        target_token: Pubkey,
        graph: &AHashMap<Pubkey, Vec<PoolEdge>>,
        visited: &mut AHashMap<Pubkey, bool>,
        path: &mut Vec<PathHop>,
        results: &mut Vec<ArbitragePath>,
        depth: usize,
        max_depth: usize,
        rate_product: I80F48,
    ) {
        if depth > max_depth {
            return;
        }
        
        visited.insert(current, true);
        
        if let Some(edges) = graph.get(&current) {
            for edge in edges {
                if depth > 1 && edge.to_pool == start && edge.token_out == target_token {
                    // Found a cycle
                    let mut complete_path = path.clone();
                    complete_path.push(PathHop {
                        pool: edge.to_pool,
                        token_in: edge.token_in,
                        token_out: edge.token_out,
                        price: edge.price,
                        liquidity: edge.liquidity,
                        is_buy: true,
                    });
                    
                    let final_rate = rate_product * I80F48::from_num(edge.price) * I80F48::from_num(1.0 - edge.fee_bps as f64 / 10000.0);
                    
                    if final_rate > I80F48::from_num(1.001) { // At least 0.1% profit before gas
                        results.push(ArbitragePath {
                            hops: complete_path,
                            expected_rate: final_rate,
                            max_liquidity: path.iter().map(|h| h.liquidity).min().unwrap_or(0),
                        });
                    }
                } else if !visited.contains_key(&edge.to_pool) {
                    path.push(PathHop {
                        pool: edge.to_pool,
                        token_in: edge.token_in,
                        token_out: edge.token_out,
                        price: edge.price,
                        liquidity: edge.liquidity,
                        is_buy: true,
                    });
                    
                    let new_rate = rate_product * I80F48::from_num(edge.price) * I80F48::from_num(1.0 - edge.fee_bps as f64 / 10000.0);
                    
                    self.dfs_find_cycles(
                        edge.to_pool,
                        start,
                        target_token,
                        graph,
                        visited,
                        path,
                        results,
                        depth + 1,
                        max_depth,
                        new_rate,
                    );
                    
                    path.pop();
                }
            }
        }
        
        visited.remove(&current);
    }

    fn estimate_path_profit(&self, path: &ArbitragePath, amount: u64) -> f64 {
        let (profit, _) = self.calculate_atomic_profit(amount, path, &MarketConditions::default());
        profit.to_num::<f64>()
    }

    fn calculate_leveraged_strategy_profit(&self, loan_amount: u64, opp: &OpportunityInfo, market: &MarketConditions) -> I80F48 {
        let loan_f = I80F48::from_num(loan_amount) / I80F48::from_num(1_000_000_000);
        
        match opp.strategy_type {
            StrategyType::LeveragedArbitrage => {
                let total_capital = loan_f + I80F48::from_num(opp.own_capital) / I80F48::from_num(1_000_000_000);
                let (arb_profit, _) = self.calculate_precise_arbitrage_profit(total_capital.to_num::<u64>(), opp, market);
                arb_profit
            },
            StrategyType::LeveragedLiquidation => {
                let total_repay = loan_amount + opp.own_capital;
                let collateral = self.calculate_liquidation_collateral(
                    total_repay,
                    opp.debt_amount,
                    opp.collateral_amount,
                    self.calculate_liquidation_bonus(opp.protocol_id)
                );
                let (liq_profit, _) = self.calculate_liquidation_profit(total_repay, collateral, market);
                liq_profit
            },
            _ => I80F48::ZERO,
        }
    }

    fn calculate_flashloan_priority_fee(&self, profit: I80F48, market: &MarketConditions, network: &NetworkState) -> u64 {
        let profit_u64 = profit.to_num::<u64>();
        let base_fee = market.p99_priority_fee;
        let profit_share = profit_u64 * 35 / 100;
        let congestion_boost = (network.network_congestion * 0.25 + 1.0) as u64 * base_fee;
        
        base_fee.max(profit_share).max(congestion_boost)
    }

    fn calculate_flashloan_risk(&self, loan_amount: u64, opp: &OpportunityInfo, market: &MarketConditions) -> f64 {
        let leverage_ratio = loan_amount as f64 / opp.own_capital as f64;
        let leverage_risk = (leverage_ratio / 10.0).min(0.5);
        let oracle_risk = 1.0 - market.oracle_confidence;
        let execution_risk = 0.2; // Multi-step execution risk
        let market_risk = market.volatility * 25.0;
        
        (leverage_risk * 0.3 + oracle_risk * 0.2 + execution_risk * 0.2 + market_risk * 0.3).min(0.95)
    }

    fn calculate_flashloan_execution_prob(&self, priority_fee: u64, compute_units: u32, market: &MarketConditions, network: &NetworkState) -> f64 {
        let fee_score = (priority_fee as f64 / market.p99_priority_fee as f64).min(2.0) / 2.0;
        let compute_score = 1.0 - (compute_units as f64 / network.max_compute_units as f64).min(0.9);
        let timing_score = (network.slot_time_remaining as f64 / 400_000.0).max(0.05);
        
        (fee_score * 0.6 + compute_score * 0.25 + timing_score * 0.15).min(0.88)
    }

    fn estimate_flashloan_compute(&self, opp: &OpportunityInfo) -> u32 {
        match opp.strategy_type {
            StrategyType::LeveragedArbitrage => 800_000,
            StrategyType::LeveragedLiquidation => 1_200_000,
            _ => 1_000_000,
        }
    }

    fn estimate_flashloan_competition(&self, market: &MarketConditions) -> u8 {
        ((market.competitor_count as f64 * 0.6) as u8).min(10)
    }

    fn calculate_dynamic_priority_fee(
        &self,
        expected_profit: I80F48,
        market: &MarketConditions,
        network: &NetworkState,
        action_type: ActionType,
    ) -> u64 {
        let profit_u64 = expected_profit.to_num::<u64>();
        
        // Base fee calculation
        let percentile_target = match action_type {
            ActionType::Liquidation => 0.99,
            ActionType::Sandwich => 0.95,
            ActionType::Arbitrage => 0.90,
            _ => 0.85,
        };
        
        let base_fee = self.interpolate_percentile_fee(percentile_target, market);
        
        // Dynamic adjustments
        let congestion_multiplier = 1.0 + network.network_congestion * 0.5;
        let competition_multiplier = 1.0 + (market.competitor_count as f64 / 20.0).min(1.0);
        let time_multiplier = 2.0 - (network.slot_time_remaining as f64 / 400_000.0).max(0.1);
        
        let adjusted_fee = (base_fee as f64 * congestion_multiplier * competition_multiplier * time_multiplier) as u64;
        
        // Cap at percentage of profit
        let max_fee_ratio = match action_type {
            ActionType::Liquidation => 0.40,
            ActionType::Sandwich => 0.35,
            _ => 0.30,
        };
        
        adjusted_fee.min((profit_u64 as f64 * max_fee_ratio) as u64)
    }

    fn interpolate_percentile_fee(&self, percentile: f64, market: &MarketConditions) -> u64 {
        let avg = market.avg_priority_fee as f64;
        let p99 = market.p99_priority_fee as f64;
        
        if percentile <= 0.5 {
            (avg * percentile * 2.0) as u64
        } else {
            let t = (percentile - 0.5) * 2.0;
            (avg + (p99 - avg) * t) as u64
        }
    }

    fn estimate_compute_units(&self, action_type: ActionType, amount: u64) -> u32 {
        let base_units = match action_type {
            ActionType::Arbitrage => 300_000,
            ActionType::Sandwich => 500_000,
            ActionType::Liquidation => 700_000,
            ActionType::JitLiquidity => 400_000,
            ActionType::AtomicArbitrage => 600_000,
            ActionType::FlashLoan => 900_000,
        };
        
        let size_factor = ((amount as f64 / 1_000_000_000.0).ln().max(0.0) + 1.0).min(2.0);
        (base_units as f64 * size_factor) as u32
    }

    fn calculate_execution_probability(
        &self,
        priority_fee: u64,
        compute_units: u32,
        market: &MarketConditions,
        network: &NetworkState,
    ) -> f64 {
        let fee_percentile = self.calculate_fee_percentile(priority_fee, market);
        let compute_ratio = compute_units as f64 / network.max_compute_units as f64;
        let time_factor = (network.slot_time_remaining as f64 / 400_000.0).max(0.0);
        let congestion_factor = 1.0 - network.network_congestion;
        
        let base_prob = fee_percentile * 0.5 + (1.0 - compute_ratio) * 0.2 + time_factor * 0.2 + congestion_factor * 0.1;
        
        // Apply sigmoid to smooth probability
        let k = 10.0;
        let x0 = 0.5;
        1.0 / (1.0 + ((-(base_prob - x0) * k).exp()))
    }

    fn calculate_fee_percentile(&self, fee: u64, market: &MarketConditions) -> f64 {
        if fee <= market.avg_priority_fee {
            (fee as f64 / market.avg_priority_fee as f64) * 0.5
        } else if fee <= market.p99_priority_fee {
            0.5 + (fee as f64 - market.avg_priority_fee as f64) / (market.p99_priority_fee as f64 - market.avg_priority_fee as f64) * 0.49
        } else {
            0.99.min(0.99 + (fee as f64 - market.p99_priority_fee as f64) / market.p99_priority_fee as f64 * 0.01)
        }
    }

        fn calculate_comprehensive_risk(&self, amount: u64, market: &MarketConditions, action_type: ActionType) -> f64 {
        let position_risk = (amount as f64 / market.liquidity_depth[0] as f64).min(1.0);
        let volatility_risk = market.volatility * match action_type {
            ActionType::Sandwich => 30.0,
            ActionType::Liquidation => 20.0,
            ActionType::FlashLoan => 25.0,
            _ => 15.0,
        };
        
        let oracle_risk = (1.0 - market.oracle_confidence) * 0.5;
        let competition_risk = (market.competitor_count as f64 / 50.0).min(1.0);
        let protocol_risk = match action_type {
            ActionType::FlashLoan | ActionType::AtomicArbitrage => 0.2,
            ActionType::Liquidation => 0.15,
            _ => 0.1,
        };
        
        let time_risk = if market.time_to_next_update < 1000 { 0.3 } else { 0.1 };
        
        let weights = match action_type {
            ActionType::Sandwich => (0.25, 0.30, 0.15, 0.20, 0.05, 0.05),
            ActionType::Liquidation => (0.20, 0.25, 0.20, 0.15, 0.10, 0.10),
            ActionType::FlashLoan => (0.30, 0.20, 0.15, 0.15, 0.15, 0.05),
            _ => (0.20, 0.20, 0.20, 0.20, 0.10, 0.10),
        };
        
        let total_risk = position_risk * weights.0 +
                        volatility_risk * weights.1 +
                        oracle_risk * weights.2 +
                        competition_risk * weights.3 +
                        protocol_risk * weights.4 +
                        time_risk * weights.5;
        
        total_risk.min(0.95)
    }

    fn select_optimal_strategy(&self, strategies: &[Strategy]) -> Option<Strategy> {
        if strategies.is_empty() {
            return None;
        }
        
        // Multi-criteria optimization
        let mut scored_strategies: Vec<(f64, &Strategy)> = strategies.iter()
            .map(|s| {
                let profit_score = (s.expected_profit.to_num::<f64>() / 1_000_000_000.0).min(100.0) / 100.0;
                let risk_score = 1.0 - s.risk_score;
                let exec_score = s.execution_probability;
                let time_score = s.time_sensitivity;
                
                let weights = match s.action_type {
                    ActionType::Liquidation => (0.35, 0.15, 0.35, 0.15),
                    ActionType::Sandwich => (0.40, 0.20, 0.30, 0.10),
                    ActionType::FlashLoan => (0.30, 0.30, 0.25, 0.15),
                    _ => (0.35, 0.25, 0.25, 0.15),
                };
                
                let total_score = profit_score * weights.0 +
                                 risk_score * weights.1 +
                                 exec_score * weights.2 +
                                 time_score * weights.3;
                
                (total_score, s)
            })
            .collect();
        
        scored_strategies.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Apply additional filters
        let best_score = scored_strategies[0].0;
        let threshold = best_score * 0.95;
        
        let viable_strategies: Vec<&Strategy> = scored_strategies.iter()
            .filter(|(score, _)| *score >= threshold)
            .map(|(_, s)| *s)
            .collect();
        
        // Final selection considering competition
        let min_competition = viable_strategies.iter()
            .map(|s| s.competition_level)
            .min()
            .unwrap_or(0);
        
        viable_strategies.into_iter()
            .find(|s| s.competition_level <= min_competition + 2)
            .cloned()
    }

    fn create_equilibrium_response(&self, strategy: &Strategy, market: &MarketConditions) -> EquilibriumResponse {
        let followers_expected = self.estimate_follower_count(strategy, market);
        let leader_action = self.determine_leader_action(strategy);
        let followers_best_response = self.calculate_followers_response(strategy, followers_expected, market);
        let market_impact = self.estimate_total_market_impact(strategy, followers_expected, market);
        
        EquilibriumResponse {
            leader_action,
            followers_best_response,
            expected_payoff: self.calculate_equilibrium_payoff(strategy, followers_expected, market),
            market_impact,
            stability_score: self.calculate_equilibrium_stability(strategy, followers_expected, market),
        }
    }

    fn estimate_follower_count(&self, strategy: &Strategy, market: &MarketConditions) -> u32 {
        let base_followers = market.competitor_count as u32;
        let profitability_factor = (strategy.expected_profit.to_num::<f64>() / 1_000_000_000.0 / 10.0).min(2.0);
        let visibility_factor = match strategy.action_type {
            ActionType::Sandwich | ActionType::Liquidation => 1.5,
            ActionType::AtomicArbitrage => 1.2,
            _ => 1.0,
        };
        
        ((base_followers as f64 * profitability_factor * visibility_factor) as u32).min(50)
    }

    fn determine_leader_action(&self, strategy: &Strategy) -> LeaderAction {
        LeaderAction {
            action_type: strategy.action_type,
            amount: strategy.amount,
            priority_fee: strategy.priority_fee,
            timing: self.calculate_optimal_timing(strategy),
        }
    }

    fn calculate_optimal_timing(&self, strategy: &Strategy) -> u64 {
        match strategy.action_type {
            ActionType::Sandwich => 50, // Execute immediately
            ActionType::Liquidation => 100, // Very quick
            ActionType::FlashLoan => 200,
            _ => 150,
        }
    }

    fn calculate_followers_response(&self, leader_strategy: &Strategy, follower_count: u32, market: &MarketConditions) -> Vec<FollowerAction> {
        let mut responses = Vec::new();
        
        for i in 0..follower_count.min(10) {
            let delay = 50 + i * 20;
            let size_ratio = 0.8 - (i as f64 * 0.05);
            let fee_multiplier = 1.0 - (i as f64 * 0.03);
            
            responses.push(FollowerAction {
                action_type: leader_strategy.action_type,
                amount: (leader_strategy.amount as f64 * size_ratio) as u64,
                priority_fee: (leader_strategy.priority_fee as f64 * fee_multiplier) as u64,
                expected_delay: delay,
            });
        }
        
        responses
    }

    fn calculate_equilibrium_payoff(&self, strategy: &Strategy, followers: u32, market: &MarketConditions) -> I80F48 {
        let base_profit = strategy.expected_profit;
        let competition_impact = I80F48::from_num(followers) * I80F48::from_num(0.02);
        let market_depth_impact = self.calculate_cumulative_depth_impact(strategy, followers, market);
        
        base_profit * (I80F48::ONE - competition_impact - market_depth_impact).max(I80F48::from_num(0.3))
    }

    fn calculate_cumulative_depth_impact(&self, strategy: &Strategy, followers: u32, market: &MarketConditions) -> I80F48 {
        let total_volume = strategy.amount * (1 + followers) as u64;
        let depth_ratio = I80F48::from_num(total_volume) / I80F48::from_num(market.liquidity_depth[0]);
        
        (depth_ratio * I80F48::from_num(0.5)).min(I80F48::from_num(0.3))
    }

    fn estimate_total_market_impact(&self, strategy: &Strategy, followers: u32, market: &MarketConditions) -> MarketImpact {
        let total_volume = strategy.amount * (1 + followers) as u64;
        let price_impact_bps = self.calculate_total_price_impact(total_volume, market);
        let liquidity_reduction = (total_volume as f64 / market.liquidity_depth[0] as f64 * 100.0) as u32;
        let volatility_increase = (followers as f64 * 0.02).min(0.5);
        
        MarketImpact {
            price_impact_bps,
            liquidity_reduction_pct: liquidity_reduction,
            volatility_increase,
            recovery_time_ms: self.estimate_recovery_time(total_volume, market),
        }
    }

    fn calculate_total_price_impact(&self, volume: u64, market: &MarketConditions) -> u32 {
        let impact = self.calculate_precise_price_impact(volume, market.liquidity_depth[0], true);
        (impact.to_num::<f64>() * 10000.0) as u32
    }

    fn estimate_recovery_time(&self, volume: u64, market: &MarketConditions) -> u64 {
        let volume_ratio = volume as f64 / market.avg_volume_24h as f64;
        let base_time = 5000; // 5 seconds base
        (base_time as f64 * (1.0 + volume_ratio * 10.0)) as u64
    }

    fn calculate_equilibrium_stability(&self, strategy: &Strategy, followers: u32, market: &MarketConditions) -> f64 {
        let profit_margin = strategy.expected_profit.to_num::<f64>() / strategy.amount as f64;
        let follower_ratio = followers as f64 / market.competitor_count as f64;
        let depth_buffer = market.liquidity_depth[0] as f64 / (strategy.amount * (1 + followers) as u64) as f64;
        
        let stability = (profit_margin * 100.0).min(1.0) * 0.3 +
                       (1.0 - follower_ratio).max(0.0) * 0.3 +
                       depth_buffer.min(5.0) / 5.0 * 0.4;
        
        stability.min(1.0)
    }

    pub fn update_historical_data(&mut self, result: &ExecutionResult) {
        self.update_action_history(result);
        self.update_success_rates(result);
        self.update_profit_history(result);
        self.update_competition_metrics(result);
    }

    fn update_action_history(&mut self, result: &ExecutionResult) {
        self.historical_actions.push(HistoricalAction {
            timestamp: result.timestamp,
            action_type: result.action_type,
            amount: result.amount,
            profit: result.actual_profit,
            success: result.success,
            priority_fee: result.priority_fee,
            competition_level: result.observed_competition,
        });
        
        // Keep only last 10000 actions
        if self.historical_actions.len() > 10000 {
            self.historical_actions.drain(0..1000);
        }
    }

    fn update_success_rates(&mut self, result: &ExecutionResult) {
        let key = result.action_type;
        let entry = self.action_success_rates.entry(key).or_insert((0, 0));
        entry.1 += 1;
        if result.success {
            entry.0 += 1;
        }
    }

    fn update_profit_history(&mut self, result: &ExecutionResult) {
        self.profit_history.push((result.timestamp, result.actual_profit));
        
        // Keep only last 24 hours
        let cutoff = result.timestamp - 86400;
        self.profit_history.retain(|(ts, _)| *ts > cutoff);
    }

    fn update_competition_metrics(&mut self, result: &ExecutionResult) {
        self.competition_observations.push(CompetitionObservation {
            timestamp: result.timestamp,
            action_type: result.action_type,
            observed_competitors: result.observed_competition,
            winning_priority_fee: result.priority_fee,
            market_impact: result.market_impact,
        });
        
        // Keep only last 1000 observations
        if self.competition_observations.len() > 1000 {
            self.competition_observations.drain(0..100);
        }
    }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let total_profit = self.profit_history.iter().map(|(_, p)| p.to_num::<i64>()).sum::<i64>();
        let total_actions = self.historical_actions.len();
        let successful_actions = self.historical_actions.iter().filter(|a| a.success).count();
        
        let success_rate_by_type = self.action_success_rates.iter()
            .map(|(k, v)| (*k, if v.1 > 0 { v.0 as f64 / v.1 as f64 } else { 0.0 }))
            .collect();
        
        let avg_competition = if self.competition_observations.is_empty() {
            0.0
        } else {
            self.competition_observations.iter().map(|c| c.observed_competitors as f64).sum::<f64>() 
                / self.competition_observations.len() as f64
        };
        
        PerformanceMetrics {
            total_profit_lamports: total_profit,
            total_actions,
            successful_actions,
            success_rate: if total_actions > 0 { successful_actions as f64 / total_actions as f64 } else { 0.0 },
            success_rate_by_type,
            average_competition: avg_competition,
            last_update: self.historical_actions.last().map(|a| a.timestamp).unwrap_or(0),
        }
    }
}

// Helper structures
#[derive(Clone)]
struct PathHop {
    pool: Pubkey,
    token_in: Pubkey,
    token_out: Pubkey,
    price: f64,
    liquidity: u64,
    is_buy: bool,
}

#[derive(Clone)]
struct ArbitragePath {
    hops: Vec<PathHop>,
    expected_rate: I80F48,
    max_liquidity: u64,
}

#[derive(Clone)]
struct PoolEdge {
    to_pool: Pubkey,
    token_in: Pubkey,
    token_out: Pubkey,
    price: f64,
    liquidity: u64,
    fee_bps: u16,
}

#[derive(Clone, Copy)]
struct LeaderAction {
    action_type: ActionType,
    amount: u64,
    priority_fee: u64,
    timing: u64,
}

#[derive(Clone)]
struct FollowerAction {
    action_type: ActionType,
    amount: u64,
    priority_fee: u64,
    expected_delay: u64,
}

#[derive(Clone)]
struct MarketImpact {
    price_impact_bps: u32,
    liquidity_reduction_pct: u32,
    volatility_increase: f64,
    recovery_time_ms: u64,
}

#[derive(Clone)]
struct HistoricalAction {
    timestamp: u64,
    action_type: ActionType,
    amount: u64,
    profit: I80F48,
    success: bool,
    priority_fee: u64,
    competition_level: u8,
}

#[derive(Clone)]
struct CompetitionObservation {
    timestamp: u64,
    action_type: ActionType,
    observed_competitors: u8,
    winning_priority_fee: u64,
    market_impact: f64,
}

#[derive(Clone)]
pub struct ExecutionResult {
    pub timestamp: u64,
    pub action_type: ActionType,
    pub amount: u64,
    pub actual_profit: I80F48,
    pub success: bool,
    pub priority_fee: u64,
    pub observed_competition: u8,
    pub market_impact: f64,
}

#[derive(Clone)]
pub struct PerformanceMetrics {
    pub total_profit_lamports: i64,
    pub total_actions: usize,
    pub successful_actions: usize,
    pub success_rate: f64,
    pub success_rate_by_type: AHashMap<ActionType, f64>,
    pub average_competition: f64,
    pub last_update: u64,
}

#[derive(Clone, Copy, PartialEq)]
pub enum StrategyType {
    StandardArbitrage,
    LeveragedArbitrage,
    StandardLiquidation,
    LeveragedLiquidation,
    MultiPoolArbitrage,
}

// Protocol-specific structures
#[derive(Clone)]
struct ProtocolConfig {
    id: u32,
    liquidation_bonus: f64,
    max_ltv: f64,
    oracle_type: OracleType,
    flash_loan_fee: f64,
}

#[derive(Clone, Copy)]
enum OracleType {
    Pyth,
    Switchboard,
    Custom,
}

// Cache structures for performance
#[derive(Clone)]
struct ComputeCache {
    last_update: u64,
    cached_paths: Vec<ArbitragePath>,
    cached_prices: AHashMap<Pubkey, f64>,
    cached_liquidity: AHashMap<Pubkey, u64>,
}

impl ComputeCache {
    fn new() -> Self {
        Self {
            last_update: 0,
            cached_paths: Vec::new(),
            cached_prices: AHashMap::new(),
            cached_liquidity: AHashMap::new(),
        }
    }

    fn is_stale(&self, current_time: u64) -> bool {
        current_time - self.last_update > 1000 // 1 second
    }

    fn update(&mut self, current_time: u64) {
        self.last_update = current_time;
        self.cached_paths.clear();
        self.cached_prices.clear();
        self.cached_liquidity.clear();
    }
}

// Error handling
#[derive(Debug, Clone)]
pub enum SolverError {
    InsufficientProfit,
    ExcessiveRisk,
    NoViableStrategy,
    ComputeLimitExceeded,
    InvalidMarketConditions,
    PathNotFound,
    SimulationFailed,
}

impl std::fmt::Display for SolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SolverError::InsufficientProfit => write!(f, "Insufficient profit for execution"),
            SolverError::ExcessiveRisk => write!(f, "Risk exceeds acceptable threshold"),
            SolverError::NoViableStrategy => write!(f, "No viable strategy found"),
            SolverError::ComputeLimitExceeded => write!(f, "Compute limit would be exceeded"),
            SolverError::InvalidMarketConditions => write!(f, "Invalid market conditions"),
            SolverError::PathNotFound => write!(f, "No profitable path found"),
            SolverError::SimulationFailed => write!(f, "Strategy simulation failed"),
        }
    }
}

impl std::error::Error for SolverError {}

// Tests module
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stackelberg_solver_initialization() {
        let solver = StackelbergGameSolver::new(0.05);
        assert_eq!(solver.risk_tolerance, 0.05);
        assert!(solver.historical_actions.is_empty());
    }

    #[test]
    fn test_price_impact_calculation() {
        let solver = StackelbergGameSolver::new(0.05);
        let impact = solver.calculate_precise_price_impact(1_000_000_000, 100_000_000_000, true);
        assert!(impact > I80F48::ZERO);
        assert!(impact < I80F48::from_num(0.1)); // Less than 10% impact
    }

    #[test]
    fn test_strategy_selection() {
        let solver = StackelbergGameSolver::new(0.05);
        let strategies = vec![
            Strategy {
                action_type: ActionType::Arbitrage,
                amount: 1_000_000_000,
                priority_fee: 100_000,
                compute_units: 300_000,
                target_pool: Pubkey::default(),
                entry_price: I80F48::from_num(100),
                expected_profit: I80F48::from_num(1_000_000),
                risk_score: 0.3,
                execution_probability: 0.8,
                time_sensitivity: 0.7,
                competition_level: 5,
            },
            Strategy {
                action_type: ActionType::Liquidation,
                amount: 2_000_000_000,
                priority_fee: 200_000,
                compute_units: 500_000,
                target_pool: Pubkey::default(),
                entry_price: I80F48::from_num(100),
                expected_profit: I80F48::from_num(2_000_000),
                risk_score: 0.5,
                execution_probability: 0.6,
                time_sensitivity: 0.9,
                competition_level: 8,
            },
        ];

        let selected = solver.select_optimal_strategy(&strategies);
        assert!(selected.is_some());
    }

    #[test]
    fn test_equilibrium_response() {
        let solver = StackelbergGameSolver::new(0.05);
        let strategy = Strategy {
            action_type: ActionType::Arbitrage,
            amount: 1_000_000_000,
            priority_fee: 100_000,
            compute_units: 300_000,
            target_pool: Pubkey::default(),
            entry_price: I80F48::from_num(100),
            expected_profit: I80F48::from_num(1_000_000),
            risk_score: 0.3,
            execution_probability: 0.8,
            time_sensitivity: 0.7,
            competition_level: 5,
        };

        let market = MarketConditions {
            current_price: 100.0,
            volatility: 0.02,
            liquidity_depth: vec![100_000_000_000, 80_000_000_000, 60_000_000_000],
            order_flow_imbalance: 0.1,
            recent_volume: 10_000_000_000,
            avg_volume_24h: 1_000_000_000_000,
            bid_ask_spread: 0.001,
            oracle_confidence: 0.99,
            time_to_next_update: 5000,
            competitor_count: 10,
            avg_priority_fee: 50_000,
            p99_priority_fee: 200_000,
        };

        let response = solver.create_equilibrium_response(&strategy, &market);
        assert!(response.expected_payoff > I80F48::ZERO);
        assert!(response.stability_score > 0.0);
        assert!(response.stability_score <= 1.0);
    }

    #[test]
    fn test_performance_metrics() {
        let mut solver = StackelbergGameSolver::new(0.05);
        
        let result = ExecutionResult {
            timestamp: 1000,
            action_type: ActionType::Arbitrage,
            amount: 1_000_000_000,
            actual_profit: I80F48::from_num(500_000),
            success: true,
            priority_fee: 100_000,
            observed_competition: 5,
            market_impact: 0.001,
        };

        solver.update_historical_data(&result);
        let metrics = solver.get_performance_metrics();
        
        assert_eq!(metrics.total_actions, 1);
        assert_eq!(metrics.successful_actions, 1);
        assert_eq!(metrics.success_rate, 1.0);
    }
}

