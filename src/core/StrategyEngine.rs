use anchor_lang::prelude::*;
use solana_sdk::{
    signature::Keypair,
    transaction::Transaction,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    hash::Hash,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use raydium_amm::state::{AmmInfo, TargetOrders};
use serum_dex::state::{Market, OrderBook};
use pyth_sdk_solana::{Price, PriceFeed};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StrategyType {
    Arbitrage,
    JitLiquidity,
    Liquidation,
    MarketMaking,
    Sandwich,
    CrossDexArb,
    StatArb,
}

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub max_position_size: u64,
    pub min_profit_threshold: u64,
    pub max_slippage_bps: u16,
    pub priority_fee_percentile: u8,
    pub max_gas_price: u64,
    pub emergency_exit_loss_bps: u16,
    pub strategy_weights: HashMap<StrategyType, f64>,
    pub risk_limit: u64,
    pub max_concurrent_txs: usize,
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub amm_pools: HashMap<Pubkey, AmmInfo>,
    pub serum_markets: HashMap<Pubkey, Market>,
    pub order_books: HashMap<Pubkey, OrderBook>,
    pub price_feeds: HashMap<Pubkey, PriceFeed>,
    pub recent_blocks: VecDeque<BlockInfo>,
    pub mempool_txs: Vec<PendingTx>,
    pub network_congestion: f64,
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub slot: u64,
    pub timestamp: i64,
    pub txs: Vec<TransactionInfo>,
    pub block_hash: Hash,
}

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub signature: String,
    pub accounts: Vec<Pubkey>,
    pub program_ids: Vec<Pubkey>,
    pub compute_units: u64,
}

#[derive(Debug, Clone)]
pub struct PendingTx {
    pub signature: String,
    pub priority_fee: u64,
    pub accounts: Vec<Pubkey>,
    pub estimated_profit: u64,
}

#[derive(Debug)]
pub struct StrategyResult {
    pub strategy_type: StrategyType,
    pub instructions: Vec<Instruction>,
    pub expected_profit: u64,
    pub risk_score: f64,
    pub priority_fee: u64,
    pub execution_probability: f64,
    pub gas_estimate: u64,
}

pub struct StrategyEngine {
    config: Arc<RwLock<StrategyConfig>>,
    market_state: Arc<RwLock<MarketState>>,
    performance_tracker: Arc<RwLock<PerformanceTracker>>,
    risk_manager: Arc<RwLock<RiskManager>>,
    tx_sender: mpsc::Sender<StrategyResult>,
}

#[derive(Debug, Default)]
struct PerformanceTracker {
    strategy_success: HashMap<StrategyType, (u64, u64)>,
    profit_history: VecDeque<(i64, Instant)>,
    gas_spent: u64,
    total_profit: i64,
}

#[derive(Debug)]
struct RiskManager {
    current_exposure: u64,
    daily_loss: i64,
    circuit_breaker_triggered: bool,
    last_reset: Instant,
}

impl StrategyEngine {
    pub fn new(
        config: StrategyConfig,
        tx_sender: mpsc::Sender<StrategyResult>,
    ) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            market_state: Arc::new(RwLock::new(MarketState {
                amm_pools: HashMap::new(),
                serum_markets: HashMap::new(),
                order_books: HashMap::new(),
                price_feeds: HashMap::new(),
                recent_blocks: VecDeque::with_capacity(64),
                mempool_txs: Vec::new(),
                network_congestion: 0.5,
            })),
            performance_tracker: Arc::new(RwLock::new(PerformanceTracker::default())),
            risk_manager: Arc::new(RwLock::new(RiskManager {
                current_exposure: 0,
                daily_loss: 0,
                circuit_breaker_triggered: false,
                last_reset: Instant::now(),
            })),
            tx_sender,
        }
    }

    pub async fn analyze_opportunity(&self) -> Result<(), Box<dyn std::error::Error>> {
        let market_state = self.market_state.read().map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to acquire read lock on market_state: {}", e)))
        })?;
        let config = self.config.read().map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to acquire read lock on config: {}", e)))
        })?;
        
        let risk_manager = self.risk_manager.read().map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to acquire read lock on risk_manager: {}", e)))
        })?;
        
        if risk_manager.circuit_breaker_triggered {
            return Ok(());
        }

        let strategies = self.rank_strategies(&market_state, &config)?;
        
        for strategy_type in strategies {
            let result = match strategy_type {
                StrategyType::Arbitrage => self.find_arbitrage(&market_state, &config),
                StrategyType::JitLiquidity => self.find_jit_opportunity(&market_state, &config),
                StrategyType::Liquidation => self.find_liquidation(&market_state, &config),
                StrategyType::MarketMaking => self.create_market_making(&market_state, &config),
                StrategyType::Sandwich => self.detect_sandwich(&market_state, &config),
                StrategyType::CrossDexArb => self.find_cross_dex_arb(&market_state, &config),
                StrategyType::StatArb => self.find_stat_arb(&market_state, &config),
            };

            if let Some(mut strategy_result) = result {
                if self.validate_strategy(&strategy_result, &config) {
                    strategy_result.priority_fee = self.calculate_optimal_priority_fee(
                        &market_state,
                        &strategy_result,
                    )?;
                    
                    self.tx_sender.send(strategy_result).await.ok();
                    break;
                }
            }
        }

        Ok(())
    }

    fn rank_strategies(
        &self,
        market_state: &MarketState,
        config: &StrategyConfig,
    ) -> Result<Vec<StrategyType>, Box<dyn std::error::Error>> {
        let mut strategy_scores: Vec<(StrategyType, f64)> = vec![
            (StrategyType::Arbitrage, self.score_arbitrage_opportunity(market_state)),
            (StrategyType::JitLiquidity, self.score_jit_opportunity(market_state)),
            (StrategyType::Liquidation, self.score_liquidation_opportunity(market_state)),
            (StrategyType::MarketMaking, self.score_market_making_opportunity(market_state)),
            (StrategyType::Sandwich, self.score_sandwich_opportunity(market_state)),
            (StrategyType::CrossDexArb, self.score_cross_dex_opportunity(market_state)),
            (StrategyType::StatArb, self.score_stat_arb_opportunity(market_state)),
        ];

        let perf_tracker = self.performance_tracker.read().map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to acquire read lock on performance_tracker: {}", e)))
        })?;
        
        for (strategy_type, score) in &mut strategy_scores {
            if let Some((success, total)) = perf_tracker.strategy_success.get(strategy_type) {
                let success_rate = if *total > 0 { *success as f64 / *total as f64 } else { 0.5 };
                *score *= success_rate * config.strategy_weights.get(strategy_type).copied().unwrap_or(1.0);
            }
        }

        strategy_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(strategy_scores.into_iter().map(|(s, _)| s).collect())
    }

    fn find_arbitrage(
        &self,
        market_state: &MarketState,
        config: &StrategyConfig,
    ) -> Option<StrategyResult> {
        let mut best_arb: Option<(Pubkey, Pubkey, u64, f64)> = None;
        
        for (pool1_key, pool1) in &market_state.amm_pools {
            for (pool2_key, pool2) in &market_state.amm_pools {
                if pool1_key == pool2_key { continue; }
                
                let price1 = self.calculate_amm_price(pool1);
                let price2 = self.calculate_amm_price(pool2);
                
                if price1 > 0.0 && price2 > 0.0 {
                    let price_diff = ((price1 - price2) / price1).abs();
                    
                    if price_diff > 0.002 {
                        let optimal_amount = self.calculate_optimal_arb_amount(pool1, pool2, config);
                        let profit = self.calculate_arb_profit(pool1, pool2, optimal_amount, config);
                        
                        if profit > config.min_profit_threshold {
                            let current_score = profit as f64 / optimal_amount as f64;
                            if best_arb.is_none() || best_arb.as_ref().map(|b| b.3 < current_score).unwrap_or(false) {
                                best_arb = Some((*pool1_key, *pool2_key, optimal_amount, current_score));
                            }
                        }
                    }
                }
            }
        }

        best_arb.map(|(pool1, pool2, amount, _)| {
            let instructions = self.build_arbitrage_instructions(pool1, pool2, amount);
            let gas_estimate = instructions.len() as u64 * 25000;
            
            StrategyResult {
                strategy_type: StrategyType::Arbitrage,
                instructions,
                expected_profit: self.calculate_arb_profit(
                    &market_state.amm_pools[&pool1],
                    &market_state.amm_pools[&pool2],
                    amount,
                    config,
                ),
                risk_score: 0.3,
                priority_fee: 0,
                execution_probability: self.estimate_execution_probability(market_state, 0.3),
                gas_estimate,
            }
        })
    }

    fn find_jit_opportunity(
        &self,
        market_state: &MarketState,
        config: &StrategyConfig,
    ) -> Option<StrategyResult> {
        for tx in &market_state.mempool_txs {
            if let Some((pool_key, swap_amount, direction)) = self.detect_large_swap(&tx) {
                if let Some(pool) = market_state.amm_pools.get(&pool_key) {
                    let impact = self.calculate_price_impact(pool, swap_amount, direction);
                    
                    if impact > 0.01 {
                        let liquidity_amount = self.calculate_jit_liquidity(pool, swap_amount, config);
                        let expected_fees = (swap_amount as f64 * 0.003 * 0.8) as u64;
                        
                        if expected_fees > config.min_profit_threshold {
                            let instructions = self.build_jit_instructions(pool_key, liquidity_amount);
                            
                            return Some(StrategyResult {
                                strategy_type: StrategyType::JitLiquidity,
                                instructions,
                                expected_profit: expected_fees,
                                risk_score: 0.5,
                                priority_fee: 0,
                                execution_probability: self.estimate_execution_probability(market_state, 0.5),
                                gas_estimate: 150000,
                            });
                        }
                    }
                }
            }
        }
        None
    }

    fn find_liquidation(
        &self,
        market_state: &MarketState,
        config: &StrategyConfig,
    ) -> Option<StrategyResult> {
        for (market_key, market) in &market_state.serum_markets {
            if let Some(price_feed) = market_state.price_feeds.get(market_key) {
                let current_price = price_feed.get_current_price().unwrap_or(0) as f64 / 1e6;
                
                for position in self.scan_liquidatable_positions(market, current_price) {
                    let liquidation_bonus = position.collateral * 5 / 100;
                    
                    if liquidation_bonus > config.min_profit_threshold {
                        let instructions = self.build_liquidation_instructions(
                            *market_key,
                            position.owner,
                            position.amount,
                        );
                        
                        return Some(StrategyResult {
                            strategy_type: StrategyType::Liquidation,
                            instructions,
                            expected_profit: liquidation_bonus,
                            risk_score: 0.4,
                            priority_fee: 0,
                            execution_probability: self.estimate_execution_probability(market_state, 0.4),
                            gas_estimate: 200000,
                        });
                    }
                }
            }
        }
        None
    }

    fn create_market_making(
        &self,
        market_state: &MarketState,
        config: &StrategyConfig,
    ) -> Option<StrategyResult> {
        let best_market = market_state.serum_markets.iter()
            .filter_map(|(key, market)| {
                market_state.order_books.get(key).map(|ob| (key, market, ob))
            })
            .max_by_key(|(_, _, ob)| self.calculate_spread_profit(ob));

        if let Some((market_key, _, order_book)) = best_market {
            let spread = self.calculate_optimal_spread(order_book, market_state.network_congestion);
            let size = self.calculate_optimal_order_size(order_book, config);
            
            if spread.0 > 0 && spread.1 > 0 {
                let expected_profit = size * (spread.0 + spread.1) / 2000;
                
                if expected_profit > config.min_profit_threshold {
                    let instructions = self.build_market_making_instructions(
                        *market_key,
                        spread,
                        size,
                    );
                    
                    return Some(StrategyResult {
                        strategy_type: StrategyType::MarketMaking,
                        instructions,
                        expected_profit,
                        risk_score: 0.6,
                        priority_fee: 0,
                        execution_probability: self.estimate_execution_probability(market_state, 0.6),
                        gas_estimate: 180000,
                    });
                }
            }
        }
        None
    }

    fn detect_sandwich(
        &self,
        market_state: &MarketState,
        config: &StrategyConfig,
    ) -> Option<StrategyResult> {
        for tx in &market_state.mempool_txs {
            if let Some((pool_key, victim_amount, direction)) = self.detect_sandwichable_tx(&tx) {
                if let Some(pool) = market_state.amm_pools.get(&pool_key) {
                    let impact = self.calculate_price_impact(pool, victim_amount, direction);
                    
                    if impact > 0.005 && impact < 0.05 {
                        let (front_amount, back_amount) = self.calculate_sandwich_amounts(
                            pool,
                            victim_amount,
                            direction,
                            config,
                        );
                        
                        let profit = self.calculate_sandwich_profit(
                            pool,
                            front_amount,
                            back_amount,
                            victim_amount,
                            direction,
                        );
                        
                        if profit > config.min_profit_threshold * 2 {
                            let instructions = self.build_sandwich_instructions(
                                pool_key,
                                front_amount,
                                back_amount,
                                direction,
                            );
                            
                            return Some(StrategyResult {
                                strategy_type: StrategyType::Sandwich,
                                instructions,
                                expected_profit: profit,
                                risk_score: 0.7,
                                priority_fee: 0,
                                execution_probability: self.estimate_execution_probability(market_state, 0.7),
                                gas_estimate: 250000,
                            });
                        }
                    }
                }
            }
        }
        None
    }

    fn find_cross_dex_arb(
        &self,
        market_state: &MarketState,
        config: &StrategyConfig,
    ) -> Option<StrategyResult> {
        let mut best_opportunity: Option<(Vec<Pubkey>, u64, u64)> = None;
        
        for (amm_key, amm_pool) in &market_state.amm_pools {
            for (serum_key, _) in &market_state.serum_markets {
                if let Some(order_book) = market_state.order_books.get(serum_key) {
                    let amm_price = self.calculate_amm_price(amm_pool);
                    let serum_price = self.calculate_serum_mid_price(order_book);
                    
                    if amm_price > 0.0 && serum_price > 0.0 {
                        let price_diff = ((amm_price - serum_price) / amm_price).abs();
                        
                        if price_diff > 0.003 {
                            let route = vec![*amm_key, *serum_key];
                            let amount = self.calculate_cross_dex_amount(
                                amm_pool,
                                order_book,
                                config,
                            );
                            let profit = self.calculate_route_profit(&route, amount, market_state);
                            
                            if profit > config.min_profit_threshold {
                                if best_opportunity.is_none() || best_opportunity.as_ref().map(|b| b.2 < profit).unwrap_or(false) {
                                    best_opportunity = Some((route, amount, profit));
                                }
                            }
                        }
                    }
                }
            }
        }

        best_opportunity.map(|(route, amount, profit)| {
            let instructions = self.build_cross_dex_instructions(route.clone(), amount);
            
            StrategyResult {
                strategy_type: StrategyType::CrossDexArb,
                instructions,
                expected_profit: profit,
                risk_score: 0.5,
                priority_fee: 0,
                execution_probability: self.estimate_execution_probability(market_state, 0.5),
                gas_estimate: 300000,
            }
        })
    }

    fn find_stat_arb(
        &self,
        market_state: &MarketState,
        config: &StrategyConfig,
    ) -> Option<StrategyResult> {
        let correlations = self.calculate_pair_correlations(market_state);
        
        for ((asset1, asset2), correlation) in correlations {
            if correlation > 0.85 {
                if let (Some(price1), Some(price2)) = (
                    self.get_asset_price(&asset1, market_state),
                    self.get_asset_price(&asset2, market_state),
                ) {
                    let ratio = price1 / price2;
                    let historical_ratio = self.get_historical_ratio(&asset1, &asset2);
                    let deviation = ((ratio - historical_ratio) / historical_ratio).abs();
                    
                    if deviation > 0.02 {
                        let (long_asset, short_asset, amount) = if ratio > historical_ratio {
                            (asset2, asset1, config.max_position_size / 2)
                        } else {
                            (asset1, asset2, config.max_position_size / 2)
                        };
                        
                        let expected_profit = (amount as f64 * deviation * 0.7) as u64;
                        
                        if expected_profit > config.min_profit_threshold {
                            let instructions = self.build_stat_arb_instructions(
                                long_asset,
                                short_asset,
                                amount,
                            );
                            
                            return Some(StrategyResult {
                                strategy_type: StrategyType::StatArb,
                                instructions,
                                expected_profit,
                                risk_score: 0.8,
                                priority_fee: 0,
                                execution_probability: self.estimate_execution_probability(market_state, 0.8),
                                gas_estimate: 350000,
                            });
                        }
                    }
                }
            }
        }
        None
    }

    fn validate_strategy(&self, strategy: &StrategyResult, config: &StrategyConfig) -> bool {
        let risk_manager = match self.risk_manager.read() {
            Ok(rm) => rm,
            Err(_) => return false, // If we can't read the risk manager, don't validate
        };
        
        if risk_manager.circuit_breaker_triggered {
            return false;
        }
        
        if strategy.expected_profit < config.min_profit_threshold {
            return false;
        }
        
        if strategy.gas_estimate > config.max_gas_price {
            return false;
        }
        
        if risk_manager.current_exposure + strategy.expected_profit > config.risk_limit {
            return false;
        }
        
        if strategy.risk_score > 0.9 {
            return false;
        }
        
        true
    }
        if pool.coin_vault_balance > 0 && pool.pc_vault_balance > 0 {
            pool.pc_vault_balance as f64 / pool.coin_vault_balance as f64
        } else {
            0.0
        }
    }

    fn calculate_optimal_arb_amount(
        &self,
        pool1: &AmmInfo,
        pool2: &AmmInfo,
        config: &StrategyConfig,
    ) -> u64 {
        let max_amount = config.max_position_size;
        let pool1_liquidity = pool1.coin_vault_balance.min(pool1.pc_vault_balance);
        let pool2_liquidity = pool2.coin_vault_balance.min(pool2.pc_vault_balance);
        
        let max_impact_amount1 = pool1_liquidity / 50;
        let max_impact_amount2 = pool2_liquidity / 50;
        
        max_amount.min(max_impact_amount1).min(max_impact_amount2)
    }

    fn calculate_arb_profit(
        &self,
        pool1: &AmmInfo,
        pool2: &AmmInfo,
        amount: u64,
        config: &StrategyConfig,
    ) -> u64 {
        let price1 = self.calculate_amm_price(pool1);
        let price2 = self.calculate_amm_price(pool2);
        
        if price1 > price2 {
            let buy_cost = self.calculate_swap_output(pool2, amount, true);
            let sell_revenue = self.calculate_swap_output(pool1, buy_cost, false);
            
            if sell_revenue > amount {
                let gross_profit = sell_revenue - amount;
                let fees = amount * 30 / 10000;
                let slippage = amount * config.max_slippage_bps as u64 / 10000;
                
                gross_profit.saturating_sub(fees).saturating_sub(slippage)
            } else {
                0
            }
        } else {
            0
        }
    }

    fn calculate_swap_output(&self, pool: &AmmInfo, amount_in: u64, coin_to_pc: bool) -> u64 {
        let (in_balance, out_balance) = if coin_to_pc {
            (pool.coin_vault_balance, pool.pc_vault_balance)
        } else {
            (pool.pc_vault_balance, pool.coin_vault_balance)
        };
        
        let amount_in_with_fee = amount_in * 997;
        let numerator = amount_in_with_fee * out_balance;
        let denominator = in_balance * 1000 + amount_in_with_fee;
        
        numerator / denominator
    }

    fn build_arbitrage_instructions(
        &self,
        pool1: Pubkey,
        pool2: Pubkey,
        amount: u64,
    ) -> Vec<Instruction> {
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(400000),
            ComputeBudgetInstruction::set_compute_unit_price(10000),
        ]
    }

    fn estimate_execution_probability(
        &self,
        market_state: &MarketState,
        base_risk: f64,
    ) -> f64 {
        let congestion_penalty = market_state.network_congestion * 0.3;
        let recent_success_rate = self.get_recent_success_rate();
        
        ((1.0 - base_risk) * recent_success_rate * (1.0 - congestion_penalty)).max(0.1)
    }

    fn get_recent_success_rate(&self) -> f64 {
        let tracker = match self.performance_tracker.read() {
            Ok(tracker) => tracker,
            Err(_) => return 0.7, // If we can't read the tracker, return default success rate
        };
        
        let total: u64 = tracker.strategy_success.values().map(|(_, t)| t).sum();
        let success: u64 = tracker.strategy_success.values().map(|(s, _)| s).sum();
        
        if total > 0 {
            success as f64 / total as f64
        } else {
            0.7
        }
    }

    fn calculate_price_impact(&self, pool: &AmmInfo, amount: u64, buy: bool) -> f64 {
        let output = self.calculate_swap_output(pool, amount, buy);
        let ideal_price = self.calculate_amm_price(pool);
        let actual_price = if buy {
            amount as f64 / output as f64
        } else {
            output as f64 / amount as f64
        };
        
        ((actual_price - ideal_price) / ideal_price).abs()
    }

    fn score_arbitrage_opportunity(&self, market_state: &MarketState) -> f64 {
        let mut score = 0.0;
        let mut count = 0;
        
        for (_, pool1) in &market_state.amm_pools {
            for (_, pool2) in &market_state.amm_pools {
                let price1 = self.calculate_amm_price(pool1);
                let price2 = self.calculate_amm_price(pool2);
                
                if price1 > 0.0 && price2 > 0.0 {
                    let diff = ((price1 - price2) / price1).abs();
                    if diff > 0.002 {
                        score += diff;
                        count += 1;
                    }
                }
            }
        }
        
        if count > 0 { score / count as f64 } else { 0.0 }
    }

    fn score_jit_opportunity(&self, market_state: &MarketState) -> f64 {
        market_state.mempool_txs.iter()
            .filter_map(|tx| {
                if tx.estimated_profit > 100000 {
                    Some(tx.estimated_profit as f64 / 1e9)
                } else {
                    None
                }
            })
            .sum::<f64>() / 10.0
    }

    fn score_liquidation_opportunity(&self, _market_state: &MarketState) -> f64 {
        // Score based on market volatility and liquidation thresholds
        let volatility = self.calculate_market_volatility();
        (volatility * 100.0).min(1.0)
    }

    fn score_market_making_opportunity(&self, market_state: &MarketState) -> f64 {
        market_state.order_books.values()
            .map(|ob| self.calculate_spread_profit(ob) as f64 / 1e9)
            .sum::<f64>() / market_state.order_books.len().max(1) as f64
    }

    fn score_sandwich_opportunity(&self, market_state: &MarketState) -> f64 {
        market_state.mempool_txs.iter()
            .filter(|tx| tx.estimated_profit > 50000)
            .count() as f64 * 0.1
    }

    fn score_cross_dex_opportunity(&self, market_state: &MarketState) -> f64 {
        let amm_count = market_state.amm_pools.len();
        let serum_count = market_state.serum_markets.len();
        ((amm_count * serum_count) as f64).sqrt() * 0.05
    }

    fn score_stat_arb_opportunity(&self, _market_state: &MarketState) -> f64 {
        0.3 // Base score for statistical arbitrage
    }
    fn calculate_market_volatility(&self) -> f64 {
        let state = self.market_state.read().unwrap();
        let mut price_changes: Vec<f64> = Vec::new();
        
        for (_, feed) in state.price_feeds.iter().take(10) {
            if let (Some(current), Some(ema)) = (feed.get_current_price(), feed.get_ema_price()) {
                let change = ((current as f64 - ema as f64) / ema as f64).abs();
                price_changes.push(change);
            }
        }
        
        if price_changes.is_empty() {
            return 0.02;
        }
        
        let mean = price_changes.iter().sum::<f64>() / price_changes.len() as f64;
        let variance = price_changes.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / price_changes.len() as f64;
        
        variance.sqrt()
    }

    fn calculate_spread_profit(&self, order_book: &OrderBook) -> u64 {
        let (best_bid, best_ask) = self.get_best_bid_ask(order_book);
        
        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            let spread_bps = ((ask - bid) * 10000 / bid) as u64;
            let depth = self.calculate_order_book_depth(order_book);
            let volume_estimate = depth / 100;
            
            (volume_estimate * spread_bps / 10000).max(100000)
        } else {
            0
        }
    }

    fn get_best_bid_ask(&self, order_book: &OrderBook) -> (Option<u64>, Option<u64>) {
        let bids = order_book.bids();
        let asks = order_book.asks();
        
        let best_bid = bids.iter().find(|&&price| price > 0);
        let best_ask = asks.iter().find(|&&price| price > 0);
        
        (best_bid.copied(), best_ask.copied())
    }

    fn calculate_order_book_depth(&self, order_book: &OrderBook) -> u64 {
        let bids = order_book.bids();
        let asks = order_book.asks();
        
        let bid_depth: u64 = bids.iter().take(10).sum();
        let ask_depth: u64 = asks.iter().take(10).sum();
        
        bid_depth.min(ask_depth)
    }

    fn detect_large_swap(&self, tx: &PendingTx) -> Option<(Pubkey, u64, bool)> {
        if tx.accounts.len() < 3 {
            return None;
        }
        
        let state = self.market_state.read().unwrap();
        for (pool_key, pool) in &state.amm_pools {
            if tx.accounts.contains(pool_key) {
                let pool_tvl = pool.coin_vault_balance + pool.pc_vault_balance;
                let threshold = pool_tvl / 200;
                
                if tx.estimated_profit > threshold {
                    let direction = tx.accounts[1] == pool.coin_vault;
                    return Some((*pool_key, tx.estimated_profit, direction));
                }
            }
        }
        None
    }

    fn calculate_jit_liquidity(&self, pool: &AmmInfo, swap_amount: u64, config: &StrategyConfig) -> u64 {
        let current_liquidity = (pool.coin_vault_balance * pool.pc_vault_balance).integer_sqrt();
        let swap_impact = swap_amount * 100 / current_liquidity.max(1);
        
        let optimal_liquidity = if swap_impact > 5 {
            swap_amount * 3
        } else {
            swap_amount * 2
        };
        
        optimal_liquidity.min(config.max_position_size).min(current_liquidity / 10)
    }

    fn build_jit_instructions(&self, pool_key: Pubkey, liquidity_amount: u64) -> Vec<Instruction> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(300000),
            ComputeBudgetInstruction::set_compute_unit_price(15000),
        ];
        
        let deposit_data = raydium_amm::instruction::DepositData {
            instruction: 3,
            max_coin_amount: liquidity_amount / 2,
            max_pc_amount: liquidity_amount / 2,
            base_side: 0,
        };
        
        instructions.push(Instruction {
            program_id: raydium_amm::ID,
            accounts: vec![],
            data: deposit_data.try_to_vec().unwrap_or_default(),
        });
        
        instructions
    }

    fn scan_liquidatable_positions(&self, market: &Market, price: f64) -> Vec<LiquidatablePosition> {
        let mut positions = Vec::new();
        let liquidation_threshold = 1.15;
        
        for i in 0..market.open_orders_accounts.len() {
            if let Some(open_orders) = &market.open_orders_accounts[i] {
                let position_value = open_orders.native_coin_total + 
                    (open_orders.native_pc_total as f64 / price) as u64;
                
                if position_value > 0 {
                    let collateral_value = open_orders.native_coin_free + 
                        (open_orders.native_pc_free as f64 / price) as u64;
                    
                    let health_ratio = collateral_value as f64 / position_value as f64;
                    
                    if health_ratio < liquidation_threshold {
                        positions.push(LiquidatablePosition {
                            owner: open_orders.owner,
                            amount: position_value,
                            collateral: collateral_value,
                        });
                    }
                }
            }
        }
        
        positions.sort_by_key(|p| std::cmp::Reverse(p.collateral));
        positions.into_iter().take(5).collect()
    }

    fn build_liquidation_instructions(
        &self,
        market_key: Pubkey,
        owner: Pubkey,
        amount: u64,
    ) -> Vec<Instruction> {
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(400000),
            ComputeBudgetInstruction::set_compute_unit_price(20000),
            serum_dex::instruction::init_open_orders(
                &serum_dex::ID,
                &market_key,
                &owner,
                &owner,
                &market_key,
                None,
            ).unwrap_or_else(|_| Instruction {
                program_id: Pubkey::default(),
                accounts: vec![],
                data: vec![],
            }),
        ]
    }

    fn calculate_optimal_spread(&self, order_book: &OrderBook, congestion: f64) -> (u64, u64) {
        let (best_bid, best_ask) = self.get_best_bid_ask(order_book);
        
        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            let natural_spread = ask - bid;
            let min_spread = (bid * 10 / 10000).max(1);
            let congestion_adjustment = (min_spread as f64 * congestion) as u64;
            
            let optimal_bid_offset = natural_spread / 4 + congestion_adjustment;
            let optimal_ask_offset = natural_spread / 4 + congestion_adjustment;
            
            (optimal_bid_offset, optimal_ask_offset)
        } else {
            (100, 100)
        }
    }

    fn calculate_optimal_order_size(&self, order_book: &OrderBook, config: &StrategyConfig) -> u64 {
        let depth = self.calculate_order_book_depth(order_book);
        let avg_order_size = depth / 20;
        
        avg_order_size.min(config.max_position_size / 10)
    }

    fn build_market_making_instructions(
        &self,
        market_key: Pubkey,
        spread: (u64, u64),
        size: u64,
    ) -> Vec<Instruction> {
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(350000),
            ComputeBudgetInstruction::set_compute_unit_price(12000),
            serum_dex::instruction::new_order_v3(
                &market_key,
                &market_key,
                &market_key,
                &market_key,
                &market_key,
                &market_key,
                &market_key,
                &market_key,
                &market_key,
                None,
                serum_dex::matching::Side::Bid,
                spread.0,
                size,
                serum_dex::matching::OrderType::PostOnly,
                0,
                serum_dex::instruction::SelfTradeBehavior::CancelProvide,
                u16::MAX,
                u64::MAX,
                32,
            ).map_err(|e| anyhow::anyhow!("Failed to create Serum instruction: {}", e))??,
        ]
    }

    fn detect_sandwichable_tx(&self, tx: &PendingTx) -> Option<(Pubkey, u64, bool)> {
        if tx.priority_fee > 100000 || tx.accounts.len() < 5 {
            return None;
        }
        
        let state = self.market_state.read().unwrap();
        for (pool_key, pool) in &state.amm_pools {
            if tx.accounts.contains(pool_key) && tx.accounts.contains(&pool.coin_vault) {
                let estimated_amount = tx.estimated_profit * 20;
                let pool_liquidity = pool.coin_vault_balance.min(pool.pc_vault_balance);
                
                if estimated_amount > pool_liquidity / 500 && estimated_amount < pool_liquidity / 50 {
                    let direction = tx.accounts[2] == pool.coin_vault;
                    return Some((*pool_key, estimated_amount, direction));
                }
            }
        }
        None
    }

    fn calculate_sandwich_amounts(
        &self,
        pool: &AmmInfo,
        victim_amount: u64,
        direction: bool,
        config: &StrategyConfig,
    ) -> (u64, u64) {
        let pool_liquidity = if direction {
            pool.coin_vault_balance
        } else {
            pool.pc_vault_balance
        };
        
        let max_front = pool_liquidity / 20;
        let optimal_front = (victim_amount as f64 * 0.7) as u64;
        
        let front = optimal_front.min(max_front).min(config.max_position_size / 3);
        let back = front + (victim_amount * 95 / 100);
        
        (front, back)
    }

    fn calculate_sandwich_profit(
        &self,
        pool: &AmmInfo,
        front_amount: u64,
        back_amount: u64,
        victim_amount: u64,
        direction: bool,
    ) -> u64 {
        let initial_price = self.calculate_amm_price(pool);
        
        let front_output = self.calculate_swap_output(pool, front_amount, direction);
        let victim_impact = self.calculate_price_impact(pool, front_amount + victim_amount, direction);
        
        let effective_price_after = initial_price * (1.0 + victim_impact);
        let back_output = (back_amount as f64 / effective_price_after) as u64;
        
        if direction {
            back_output.saturating_sub(front_amount)
        } else {
            front_output.saturating_sub(back_amount)
        }
    }

    fn build_sandwich_instructions(
        &self,
        pool_key: Pubkey,
        front_amount: u64,
        back_amount: u64,
        direction: bool,
    ) -> Vec<Instruction> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(500000),
            ComputeBudgetInstruction::set_compute_unit_price(50000),
        ];
        
        let swap_instruction = |amount: u64, dir: bool| {
            raydium_amm::instruction::swap_base_in(
                &raydium_amm::ID,
                &pool_key,
                &pool_key,
                &pool_key,
                &pool_key,
                &pool_key,
                &pool_key,
                &pool_key,
                &pool_key,
                &pool_key,
                &pool_key,
                amount,
                0,
            ).map_err(|e| anyhow::anyhow!("Failed to create swap instruction: {}", e))??
        };
        
        instructions.push(swap_instruction(front_amount, direction));
        instructions.push(swap_instruction(back_amount, !direction));
        
        instructions
    }

    fn calculate_serum_mid_price(&self, order_book: &OrderBook) -> f64 {
        let (best_bid, best_ask) = self.get_best_bid_ask(order_book);
        
        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => (bid + ask) as f64 / 2.0 / 1e6,
            (Some(bid), None) => bid as f64 / 1e6,
            (None, Some(ask)) => ask as f64 / 1e6,
            _ => 0.0,
        }
    }

    fn calculate_cross_dex_amount(
        &self,
        amm_pool: &AmmInfo,
        order_book: &OrderBook,
        config: &StrategyConfig,
    ) -> u64 {
        let amm_liquidity = amm_pool.coin_vault_balance.min(amm_pool.pc_vault_balance);
        let serum_depth = self.calculate_order_book_depth(order_book);
        
        let max_amount = amm_liquidity.min(serum_depth) / 20;
        max_amount.min(config.max_position_size / 4)
    }

    fn calculate_route_profit(&self, route: &[Pubkey], amount: u64, market_state: &MarketState) -> u64 {
        let mut current_amount = amount;
        let mut total_fees = 0u64;
        
        for i in 0..route.len() - 1 {
            if let Some(pool) = market_state.amm_pools.get(&route[i]) {
                let output = self.calculate_swap_output(pool, current_amount, i % 2 == 0);
                let fee = current_amount * 25 / 10000;
                total_fees += fee;
                current_amount = output;
            } else if let Some(order_book) = market_state.order_books.get(&route[i]) {
                let slippage = self.calculate_order_book_slippage(order_book, current_amount);
                current_amount = (current_amount as f64 * (1.0 - slippage)) as u64;
                total_fees += current_amount * 10 / 10000;
            }
        }
        
        current_amount.saturating_sub(amount).saturating_sub(total_fees)
    }

    fn calculate_order_book_slippage(&self, order_book: &OrderBook, amount: u64) -> f64 {
        let depth = self.calculate_order_book_depth(order_book);
        let impact_ratio = amount as f64 / depth as f64;
        (impact_ratio * 0.1).min(0.05)
    }

    fn build_cross_dex_instructions(&self, route: Vec<Pubkey>, amount: u64) -> Vec<Instruction> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(600000),
            ComputeBudgetInstruction::set_compute_unit_price(30000),
        ];
        
        for (i, &dex) in route.iter().enumerate() {
            if i < route.len() - 1 {
                instructions.push(Instruction {
                    program_id: if i % 2 == 0 { raydium_amm::ID } else { serum_dex::ID },
                    accounts: vec![dex],
                    data: vec![3, i as u8],
                });
            }
        }
        
        instructions
    }

    fn calculate_pair_correlations(&self, market_state: &MarketState) -> HashMap<(Pubkey, Pubkey), f64> {
        let mut correlations = HashMap::new();
        let price_history_len = 100;
        
        let assets: Vec<_> = market_state.price_feeds.keys().cloned().collect();
        
        for i in 0..assets.len() {
            for j in i+1..assets.len() {
                if let (Some(feed1), Some(feed2)) = (
                    market_state.price_feeds.get(&assets[i]),
                    market_state.price_feeds.get(&assets[j])
                ) {
                    let correlation = self.calculate_price_correlation(feed1, feed2, price_history_len);
                    if correlation.abs() > 0.7 {
                        correlations.insert((assets[i], assets[j]), correlation);
                    }
                }
            }
        }
        
        correlations
    }

    fn calculate_price_correlation(&self, feed1: &PriceFeed, feed2: &PriceFeed, _len: usize) -> f64 {
        if let (Some(p1), Some(p2)) = (feed1.get_current_price(), feed2.get_current_price()) {
            let ratio = p1 as f64 / p2 as f64;
            if ratio > 0.9 && ratio < 1.1 {
                0.85
            } else {
                0.3
            }
        } else {
            0.0
        }
    }

    fn get_historical_ratio(&self, asset1: &Pubkey, asset2: &Pubkey) -> f64 {
        let state = match self.market_state.read() {
            Ok(state) => state,
            Err(_) => return 1.0, // If we can't read the market state, return default ratio
        };
        
        if let (Some(feed1), Some(feed2)) = (
            state.price_feeds.get(asset1),
            state.price_feeds.get(asset2)
        ) {
            if let (Some(ema1), Some(ema2)) = (feed1.get_ema_price(), feed2.get_ema_price()) {
                return ema1 as f64 / ema2 as f64;
            }
        }
        
        1.0
    }

    fn build_stat_arb_instructions(
        &self,
        long_asset: Pubkey,
        short_asset: Pubkey,
        amount: u64,
    ) -> Vec<Instruction> {
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(700000),
            ComputeBudgetInstruction::set_compute_unit_price(40000),
            Instruction {
                program_id: serum_dex::ID,
                accounts: vec![long_asset, short_asset],
                data: vec![5, 0, 1],
            },
        ]
    }

    fn estimate_competition_level(&self, market_state: &MarketState, strategy_type: &StrategyType) -> f64 {
        let base_competition = match strategy_type {
            StrategyType::Sandwich => 2.5,
            StrategyType::Arbitrage => 2.0,
            StrategyType::JitLiquidity => 1.8,
            StrategyType::Liquidation => 1.5,
            StrategyType::CrossDexArb => 1.6,
            StrategyType::MarketMaking => 1.2,
            StrategyType::StatArb => 1.3,
        };
        
        let recent_blocks_competition = market_state.recent_blocks.iter()
            .flat_map(|block| &block.txs)
            .filter(|tx| tx.compute_units > 200000)
            .count() as f64 / market_state.recent_blocks.len().max(1) as f64;
        
        let mempool_competition = market_state.mempool_txs.iter()
            .filter(|tx| tx.priority_fee > 50000)
            .count() as f64 / 100.0;
        
        base_competition * (1.0 + market_state.network_congestion) * (1.0 + recent_blocks_competition * 0.1) * (1.0 + mempool_competition)
    }

    pub fn update_performance(&self, strategy_type: StrategyType, success: bool, profit: i64) {
        let mut tracker = match self.performance_tracker.write() {
            Ok(tracker) => tracker,
            Err(_) => return, // If we can't write to the tracker, return early
        };
        
        let entry = tracker.strategy_success.entry(strategy_type).or_insert((0, 0));
        entry.1 += 1;
        if success {
            entry.0 += 1;
        }
        
        tracker.total_profit += profit;
        tracker.profit_history.push_back((profit, Instant::now()));
        
        if tracker.profit_history.len() > 1000 {
            tracker.profit_history.pop_front();
        }
        
        tracker.gas_spent += if success { 150000 } else { 50000 };
        
        if profit < 0 {
            let mut risk_manager = match self.risk_manager.write() {
                Ok(rm) => rm,
                Err(_) => return, // If we can't write to risk manager, return early
            };
            risk_manager.daily_loss += profit;
            risk_manager.current_exposure = risk_manager.current_exposure.saturating_sub(profit.abs() as u64);
            
            if risk_manager.daily_loss < -1000000000 {
                risk_manager.circuit_breaker_triggered = true;
            }
        } else {
            let mut risk_manager = match self.risk_manager.write() {
                Ok(rm) => rm,
                Err(_) => return, // If we can't write to risk manager, return early
            };
            risk_manager.current_exposure = risk_manager.current_exposure.saturating_add(profit as u64 / 2);
        }
    }

    pub fn update_market_state(&self, update: MarketStateUpdate) {
        let mut state = match self.market_state.write() {
            Ok(state) => state,
            Err(_) => return, // If we can't write to the market state, return early
        };
        
        match update {
            MarketStateUpdate::AmmPool(key, info) => {
                state.amm_pools.insert(key, info);
            }
            MarketStateUpdate::SerumMarket(key, market) => {
                state.serum_markets.insert(key, market);
            }
            MarketStateUpdate::OrderBook(key, book) => {
                state.order_books.insert(key, book);
            }
            MarketStateUpdate::PriceFeed(key, feed) => {
                state.price_feeds.insert(key, feed);
            }
            MarketStateUpdate::NetworkCongestion(level) => {
                state.network_congestion = level.clamp(0.0, 1.0);
            }
            MarketStateUpdate::MempoolTx(tx) => {
                state.mempool_txs.push(tx);
                if state.mempool_txs.len() > 1000 {
                    state.mempool_txs.remove(0);
                }
                state.mempool_txs.sort_by_key(|tx| std::cmp::Reverse(tx.priority_fee));
            }
            MarketStateUpdate::Block(block) => {
                state.recent_blocks.push_back(block);
                if state.recent_blocks.len() > 64 {
                    state.recent_blocks.pop_front();
                }
            }
        }
    }

    pub fn reset_daily_limits(&self) {
        let mut risk_manager = match self.risk_manager.write() {
            Ok(rm) => rm,
            Err(_) => return, // If we can't write to risk manager, return early
        };
        
        if risk_manager.last_reset.elapsed() > Duration::from_secs(86400) {
            risk_manager.daily_loss = 0;
            risk_manager.circuit_breaker_triggered = false;
            risk_manager.current_exposure = 0;
            risk_manager.last_reset = Instant::now();
        }
    }

    pub fn get_strategy_metrics(&self) -> HashMap<StrategyType, (f64, f64, u64)> {
        let tracker = match self.performance_tracker.read() {
            Ok(tracker) => tracker,
            Err(_) => return HashMap::new(), // If we can't read the tracker, return empty metrics
        };
        
        let mut metrics = HashMap::new();
        
        for (strategy, (success, total)) in &tracker.strategy_success {
            let success_rate = if *total > 0 { *success as f64 / *total as f64 } else { 0.0 };
            let avg_gas = tracker.gas_spent / total.max(&1);
            metrics.insert(*strategy, (success_rate, 0.0, avg_gas));
        }
        
        let recent_profits: HashMap<StrategyType, Vec<i64>> = HashMap::new();
        for (profit, _) in tracker.profit_history.iter().rev().take(100) {
            // In production, would track profit by strategy
        }
        
        metrics
    }
}

#[derive(Debug)]
struct LiquidatablePosition {
    owner: Pubkey,
    amount: u64,
    collateral: u64,
}

#[derive(Debug)]
pub enum MarketStateUpdate {
    AmmPool(Pubkey, AmmInfo),
    SerumMarket(Pubkey, Market),
    OrderBook(Pubkey, OrderBook),
    PriceFeed(Pubkey, PriceFeed),
    NetworkCongestion(f64),
    MempoolTx(PendingTx),
    Block(BlockInfo),
}

impl Default for StrategyConfig {
    fn default() -> Self {
        let mut strategy_weights = HashMap::new();
        strategy_weights.insert(StrategyType::Arbitrage, 1.5);
        strategy_weights.insert(StrategyType::JitLiquidity, 1.3);
        strategy_weights.insert(StrategyType::Liquidation, 1.2);
        strategy_weights.insert(StrategyType::MarketMaking, 0.8);
        strategy_weights.insert(StrategyType::Sandwich, 1.4);
        strategy_weights.insert(StrategyType::CrossDexArb, 1.1);
        strategy_weights.insert(StrategyType::StatArb, 0.7);

        Self {
            max_position_size: 100_000_000_000,
            min_profit_threshold: 1_000_000,
            max_slippage_bps: 50,
            priority_fee_percentile: 75,
            max_gas_price: 5_000_000,
            emergency_exit_loss_bps: 500,
            strategy_weights,
            risk_limit: 500_000_000_000,
            max_concurrent_txs: 10,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_engine_creation() {
        let (tx, _rx) = mpsc::channel(100);
        let engine = StrategyEngine::new(StrategyConfig::default(), tx);
        assert!(!engine.risk_manager.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on risk manager: {}", e))?.circuit_breaker_triggered);
    }
}
