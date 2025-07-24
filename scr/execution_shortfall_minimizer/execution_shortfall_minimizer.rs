use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    clock::Clock,
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use spl_token::state::Mint;
use anchor_lang::prelude::*;
use rayon::prelude::*;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

const SLOT_DURATION_MS: u64 = 400;
const MAX_PRIORITY_FEE: u64 = 50_000_000;
const MIN_PRIORITY_FEE: u64 = 1_000;
const PRICE_IMPACT_THRESHOLD: f64 = 0.003;
const SLIPPAGE_BUFFER: f64 = 0.0005;
const GAS_PRICE_PERCENTILE: f64 = 0.75;
const MAX_RETRY_ATTEMPTS: u8 = 3;
const VOLATILITY_WINDOW: usize = 20;
const LIQUIDITY_DEPTH_LEVELS: usize = 5;
const MIN_PROFIT_THRESHOLD: u64 = 100_000;
const MAX_POSITION_SIZE: u64 = 1_000_000_000_000;
const TICK_SIZE_BASIS_POINTS: u64 = 1;

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub max_slippage_bps: u16,
    pub priority_fee_percentile: f64,
    pub execution_window_ms: u64,
    pub min_liquidity_ratio: f64,
    pub max_price_impact_bps: u16,
    pub adaptive_fee_enabled: bool,
    pub split_threshold: u64,
    pub urgency_multiplier: f64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_slippage_bps: 50,
            priority_fee_percentile: 0.75,
            execution_window_ms: 800,
            min_liquidity_ratio: 0.1,
            max_price_impact_bps: 30,
            adaptive_fee_enabled: true,
            split_threshold: 500_000_000_000,
            urgency_multiplier: 1.5,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketMicrostructure {
    pub bid_ask_spread: f64,
    pub depth_imbalance: f64,
    pub order_flow_toxicity: f64,
    pub realized_volatility: f64,
    pub tick_size: f64,
    pub lot_size: u64,
    pub maker_rebate: f64,
    pub taker_fee: f64,
}

#[derive(Debug, Clone)]
pub struct ExecutionMetrics {
    pub expected_price: f64,
    pub worst_case_price: f64,
    pub price_impact: f64,
    pub timing_cost: f64,
    pub opportunity_cost: f64,
    pub total_cost: f64,
    pub success_probability: f64,
    pub optimal_size: u64,
}

#[derive(Debug)]
pub struct PriceLevel {
    pub price: f64,
    pub size: u64,
    pub cumulative_size: u64,
    pub providers: Vec<Pubkey>,
}

#[derive(Debug)]
pub struct OrderBook {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub last_update: Instant,
    pub mid_price: f64,
}

pub struct VolatilityEstimator {
    price_history: VecDeque<(f64, Instant)>,
    window_size: usize,
}

impl VolatilityEstimator {
    pub fn new(window_size: usize) -> Self {
        Self {
            price_history: VecDeque::with_capacity(window_size),
            window_size,
        }
    }

    pub fn update(&mut self, price: f64) {
        let now = Instant::now();
        if self.price_history.len() >= self.window_size {
            self.price_history.pop_front();
        }
        self.price_history.push_back((price, now));
    }

    pub fn calculate_volatility(&self) -> f64 {
        if self.price_history.len() < 2 {
            return 0.0;
        }

        let returns: Vec<f64> = self.price_history
            .windows(2)
            .map(|w| (w[1].0 / w[0].0).ln())
            .collect();

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        variance.sqrt() * (365.25_f64).sqrt()
    }

    pub fn calculate_garch_volatility(&self) -> f64 {
        const ALPHA: f64 = 0.1;
        const BETA: f64 = 0.85;
        const OMEGA: f64 = 0.000001;

        let returns: Vec<f64> = self.price_history
            .windows(2)
            .map(|w| (w[1].0 / w[0].0).ln())
            .collect();

        if returns.is_empty() {
            return 0.0;
        }

        let mut conditional_variance = returns.iter()
            .map(|r| r.powi(2))
            .sum::<f64>() / returns.len() as f64;

        for &ret in returns.iter().rev() {
            conditional_variance = OMEGA + ALPHA * ret.powi(2) + BETA * conditional_variance;
        }

        conditional_variance.sqrt() * (365.25_f64).sqrt()
    }
}

pub struct ExecutionShortfallMinimizer {
    rpc_client: Arc<RpcClient>,
    config: ExecutionConfig,
    market_data: Arc<RwLock<HashMap<Pubkey, OrderBook>>>,
    volatility_estimators: Arc<RwLock<HashMap<Pubkey, VolatilityEstimator>>>,
    gas_price_history: Arc<RwLock<VecDeque<(u64, Instant)>>>,
}

impl ExecutionShortfallMinimizer {
    pub fn new(rpc_endpoint: &str, config: ExecutionConfig) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_endpoint.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            config,
            market_data: Arc::new(RwLock::new(HashMap::new())),
            volatility_estimators: Arc::new(RwLock::new(HashMap::new())),
            gas_price_history: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
        }
    }

    pub fn calculate_optimal_execution(
        &self,
        token_mint: &Pubkey,
        amount: u64,
        side: bool,
        urgency: f64,
    ) -> Result<ExecutionMetrics, Box<dyn std::error::Error>> {
        let market_data = self.market_data.read().unwrap();
        let order_book = market_data.get(token_mint)
            .ok_or("No market data available")?;

        let microstructure = self.analyze_microstructure(order_book)?;
        let volatility = self.get_volatility(token_mint);
        let current_gas_price = self.estimate_optimal_gas_price();

        let price_impact = self.calculate_price_impact(
            order_book,
            amount,
            side,
            &microstructure,
        );

        let timing_risk = self.calculate_timing_risk(
            volatility,
            urgency,
            self.config.execution_window_ms,
        );

        let split_sizes = self.calculate_optimal_split(
            amount,
            &microstructure,
            volatility,
            price_impact,
        );

        let execution_cost = self.calculate_total_execution_cost(
            amount,
            price_impact,
            timing_risk,
            current_gas_price,
            &microstructure,
        );

        let success_prob = self.estimate_success_probability(
            &microstructure,
            volatility,
            current_gas_price,
            urgency,
        );

        let expected_price = if side {
            order_book.mid_price * (1.0 + price_impact)
        } else {
            order_book.mid_price * (1.0 - price_impact)
        };

        let worst_case_price = if side {
            expected_price * (1.0 + self.config.max_slippage_bps as f64 / 10000.0)
        } else {
            expected_price * (1.0 - self.config.max_slippage_bps as f64 / 10000.0)
        };

        Ok(ExecutionMetrics {
            expected_price,
            worst_case_price,
            price_impact,
            timing_cost: timing_risk * order_book.mid_price,
            opportunity_cost: self.calculate_opportunity_cost(volatility, urgency),
            total_cost: execution_cost,
            success_probability: success_prob,
            optimal_size: split_sizes.get(0).cloned().unwrap_or(amount),
        })
    }

    fn analyze_microstructure(&self, order_book: &OrderBook) -> Result<MarketMicrostructure, Box<dyn std::error::Error>> {
        let best_bid = order_book.bids.first()
            .ok_or("No bids available")?;
        let best_ask = order_book.asks.first()
            .ok_or("No asks available")?;

        let spread = (best_ask.price - best_bid.price) / order_book.mid_price;
        
        let bid_depth: u64 = order_book.bids.iter()
            .take(LIQUIDITY_DEPTH_LEVELS)
            .map(|level| level.size)
            .sum();
        
        let ask_depth: u64 = order_book.asks.iter()
            .take(LIQUIDITY_DEPTH_LEVELS)
            .map(|level| level.size)
            .sum();

        let depth_imbalance = (bid_depth as f64 - ask_depth as f64) / (bid_depth + ask_depth) as f64;

        let tick_size = self.calculate_tick_size(order_book.mid_price);
        
        let order_flow_toxicity = self.calculate_order_flow_toxicity(order_book);

        Ok(MarketMicrostructure {
            bid_ask_spread: spread,
            depth_imbalance,
            order_flow_toxicity,
            realized_volatility: self.get_volatility(&Pubkey::default()),
            tick_size,
            lot_size: 1,
            maker_rebate: -0.0002,
            taker_fee: 0.0005,
        })
    }

    fn calculate_price_impact(
        &self,
        order_book: &OrderBook,
        amount: u64,
        side: bool,
        microstructure: &MarketMicrostructure,
    ) -> f64 {
        let levels = if side { &order_book.asks } else { &order_book.bids };
        
        let mut remaining_amount = amount;
        let mut weighted_price = 0.0;
        let mut total_cost = 0.0;

        for level in levels.iter() {
            if remaining_amount == 0 {
                break;
            }

            let fill_amount = remaining_amount.min(level.size);
            let level_cost = fill_amount as f64 * level.price;
            
            total_cost += level_cost;
            weighted_price += level_cost;
            remaining_amount -= fill_amount;
        }

        if amount > 0 {
            weighted_price /= amount as f64;
        }

        let price_impact = ((weighted_price / order_book.mid_price) - 1.0).abs();
        
        let permanent_impact = price_impact * 0.5;
        let temporary_impact = price_impact * 0.5 * (1.0 - (-amount as f64 / 1e12).exp());
        
        permanent_impact + temporary_impact + microstructure.bid_ask_spread * 0.5
    }

    fn calculate_timing_risk(
        &self,
        volatility: f64,
        urgency: f64,
        execution_window_ms: u64,
    ) -> f64 {
        let time_factor = (execution_window_ms as f64 / 1000.0) / (252.0 * 24.0 * 3600.0);
        let drift_risk = volatility * time_factor.sqrt();
        
        let adverse_selection = urgency * 0.001;
        let market_impact_decay = (1.0 - (-time_factor * 100.0).exp()) * 0.0005;
        
        drift_risk + adverse_selection + market_impact_decay
    }

    fn calculate_optimal_split(
        &self,
        total_amount: u64,
        microstructure: &MarketMicrostructure,
        volatility: f64,
        price_impact: f64,
    ) -> Vec<u64> {
        if total_amount < self.config.split_threshold {
            return vec![total_amount];
        }

        let optimal_child_size = ((2.0 * microstructure.taker_fee * total_amount as f64) /
            (price_impact * volatility.sqrt())).sqrt() as u64;

        let n_splits = ((total_amount as f64 / optimal_child_size as f64).ceil() as usize).max(1).min(10);
        
        let base_size = total_amount / n_splits as u64;
        let remainder = total_amount % n_splits as u64;

                let mut splits = vec![base_size; n_splits];
        if remainder > 0 {
            splits[0] += remainder;
        }

        splits.into_iter()
            .filter(|&size| size > 0)
            .collect()
    }

    fn calculate_total_execution_cost(
        &self,
        amount: u64,
        price_impact: f64,
        timing_risk: f64,
        gas_price: u64,
        microstructure: &MarketMicrostructure,
    ) -> f64 {
        let spread_cost = microstructure.bid_ask_spread * 0.5;
        let market_impact_cost = price_impact;
        let timing_cost = timing_risk;
        
        let gas_cost_lamports = gas_price * 5000;
        let gas_cost_percentage = gas_cost_lamports as f64 / amount as f64;
        
        let exchange_fees = if microstructure.maker_rebate < 0.0 {
            microstructure.taker_fee
        } else {
            microstructure.taker_fee - microstructure.maker_rebate
        };

        spread_cost + market_impact_cost + timing_cost + gas_cost_percentage + exchange_fees
    }

    fn estimate_success_probability(
        &self,
        microstructure: &MarketMicrostructure,
        volatility: f64,
        gas_price: u64,
        urgency: f64,
    ) -> f64 {
        let base_success_rate = 0.95;
        
        let volatility_penalty = (volatility / 0.5).min(1.0) * 0.1;
        let gas_price_penalty = ((gas_price as f64 / MAX_PRIORITY_FEE as f64) * 0.05).min(0.05);
        let urgency_penalty = (urgency * 0.02).min(0.1);
        let toxicity_penalty = (microstructure.order_flow_toxicity * 0.1).min(0.1);
        
        (base_success_rate - volatility_penalty - gas_price_penalty - urgency_penalty - toxicity_penalty)
            .max(0.5)
            .min(0.99)
    }

    fn calculate_opportunity_cost(&self, volatility: f64, urgency: f64) -> f64 {
        let drift_component = volatility * (self.config.execution_window_ms as f64 / 1000.0).sqrt() / (252.0_f64).sqrt();
        let urgency_component = urgency * 0.001;
        
        (drift_component + urgency_component) * 0.5
    }

    fn get_volatility(&self, token_mint: &Pubkey) -> f64 {
        let estimators = self.volatility_estimators.read().unwrap();
        estimators.get(token_mint)
            .map(|e| e.calculate_garch_volatility())
            .unwrap_or(0.02)
    }

    fn calculate_tick_size(&self, price: f64) -> f64 {
        if price < 0.0001 {
            0.00000001
        } else if price < 0.001 {
            0.0000001
        } else if price < 0.01 {
            0.000001
        } else if price < 0.1 {
            0.00001
        } else if price < 1.0 {
            0.0001
        } else if price < 10.0 {
            0.001
        } else if price < 100.0 {
            0.01
        } else {
            0.1
        }
    }

    fn calculate_order_flow_toxicity(&self, order_book: &OrderBook) -> f64 {
        let spread = if let (Some(bid), Some(ask)) = (order_book.bids.first(), order_book.asks.first()) {
            (ask.price - bid.price) / order_book.mid_price
        } else {
            return 0.5;
        };

        let depth_ratio = if order_book.bids.len() > 1 && order_book.asks.len() > 1 {
            let bid_depth: u64 = order_book.bids.iter().take(3).map(|l| l.size).sum();
            let ask_depth: u64 = order_book.asks.iter().take(3).map(|l| l.size).sum();
            let total_depth = (bid_depth + ask_depth) as f64;
            if total_depth > 0.0 {
                (bid_depth as f64 - ask_depth as f64).abs() / total_depth
            } else {
                0.5
            }
        } else {
            0.5
        };

        let age_factor = order_book.last_update.elapsed().as_secs_f64() / 60.0;
        let staleness = (age_factor / 5.0).min(1.0);

        (spread * 10.0 + depth_ratio + staleness) / 3.0
    }

    pub fn estimate_optimal_gas_price(&self) -> u64 {
        let history = self.gas_price_history.read().unwrap();
        if history.is_empty() {
            return 10_000;
        }

        let recent_prices: Vec<u64> = history.iter()
            .filter(|(_, time)| time.elapsed().as_secs() < 60)
            .map(|(price, _)| *price)
            .collect();

        if recent_prices.is_empty() {
            return 10_000;
        }

        let mut sorted_prices = recent_prices.clone();
        sorted_prices.sort_unstable();
        
        let percentile_idx = ((sorted_prices.len() as f64 * self.config.priority_fee_percentile) as usize)
            .min(sorted_prices.len() - 1);
        
        let base_fee = sorted_prices[percentile_idx];
        
        if self.config.adaptive_fee_enabled {
            let volatility = self.calculate_gas_volatility(&recent_prices);
            let adjustment_factor = 1.0 + (volatility * 0.5).min(0.5);
            ((base_fee as f64 * adjustment_factor) as u64)
                .max(MIN_PRIORITY_FEE)
                .min(MAX_PRIORITY_FEE)
        } else {
            base_fee.max(MIN_PRIORITY_FEE).min(MAX_PRIORITY_FEE)
        }
    }

    fn calculate_gas_volatility(&self, prices: &[u64]) -> f64 {
        if prices.len() < 2 {
            return 0.0;
        }

        let mean = prices.iter().sum::<u64>() as f64 / prices.len() as f64;
        let variance = prices.iter()
            .map(|&p| {
                let diff = p as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / prices.len() as f64;

        (variance.sqrt() / mean).min(1.0)
    }

    pub fn update_market_data(&self, token_mint: Pubkey, order_book: OrderBook) {
        let mut market_data = self.market_data.write().unwrap();
        market_data.insert(token_mint, order_book);
        
        let mid_price = market_data.get(&token_mint).map(|ob| ob.mid_price).unwrap_or(0.0);
        if mid_price > 0.0 {
            let mut estimators = self.volatility_estimators.write().unwrap();
            estimators.entry(token_mint)
                .or_insert_with(|| VolatilityEstimator::new(VOLATILITY_WINDOW))
                .update(mid_price);
        }
    }

    pub fn update_gas_price(&self, gas_price: u64) {
        let mut history = self.gas_price_history.write().unwrap();
        if history.len() >= 100 {
            history.pop_front();
        }
        history.push_back((gas_price, Instant::now()));
    }

    pub fn should_execute_now(
        &self,
        metrics: &ExecutionMetrics,
        current_slot: u64,
        target_slot: u64,
    ) -> bool {
        if current_slot >= target_slot {
            return true;
        }

        let slots_remaining = target_slot - current_slot;
        let time_remaining_ms = slots_remaining * SLOT_DURATION_MS;
        
        if time_remaining_ms < 800 {
            return true;
        }

        let urgency_threshold = 1.0 - (time_remaining_ms as f64 / 5000.0);
        let cost_threshold = PRICE_IMPACT_THRESHOLD + (urgency_threshold * 0.001);
        
        metrics.price_impact < cost_threshold && 
        metrics.success_probability > 0.8 &&
        metrics.total_cost < cost_threshold * 1.5
    }

    pub fn calculate_adaptive_slippage(
        &self,
        base_slippage_bps: u16,
        volatility: f64,
        urgency: f64,
        market_depth: f64,
    ) -> u16 {
        let volatility_adjustment = (volatility * 10000.0) as u16;
        let urgency_adjustment = (urgency * 50.0) as u16;
        let depth_adjustment = if market_depth < 0.5 {
            20
        } else if market_depth < 1.0 {
            10
        } else {
            0
        };

        (base_slippage_bps + volatility_adjustment + urgency_adjustment + depth_adjustment)
            .min(self.config.max_slippage_bps * 2)
    }

    pub fn optimize_execution_path(
        &self,
        token_a: &Pubkey,
        token_b: &Pubkey,
        amount: u64,
        dexes: &[Pubkey],
    ) -> Result<Vec<(Pubkey, u64)>, Box<dyn std::error::Error>> {
        let mut best_path = Vec::new();
        let mut best_price = f64::MAX;

        for dex in dexes {
            let market_data = self.market_data.read().unwrap();
            if let Some(order_book) = market_data.get(dex) {
                let microstructure = self.analyze_microstructure(order_book)?;
                let impact = self.calculate_price_impact(
                    order_book,
                    amount,
                    true,
                    &microstructure,
                );
                
                let effective_price = order_book.mid_price * (1.0 + impact);
                if effective_price < best_price {
                    best_price = effective_price;
                    best_path = vec![(*dex, amount)];
                }
            }
        }

        if best_path.is_empty() {
            return Err("No valid execution path found".into());
        }

        let splits = self.calculate_optimal_split(
            amount,
            &self.analyze_microstructure(
                &self.market_data.read().unwrap().get(&best_path[0].0).unwrap()
            )?,
            self.get_volatility(token_a),
            best_price,
        );

        Ok(splits.into_iter()
            .enumerate()
            .map(|(i, size)| (dexes[i % dexes.len()], size))
            .collect())
    }

    pub fn estimate_sandwich_risk(
        &self,
        token_mint: &Pubkey,
        amount: u64,
        current_slot: u64,
    ) -> f64 {
        let market_data = self.market_data.read().unwrap();
        let order_book = match market_data.get(token_mint) {
            Some(ob) => ob,
            None => return 0.5,
        };

        let microstructure = match self.analyze_microstructure(order_book) {
            Ok(ms) => ms,
            Err(_) => return 0.5,
        };

        let price_impact = self.calculate_price_impact(
            order_book,
            amount,
            true,
            &microstructure,
        );

        let profitability = price_impact * amount as f64 * order_book.mid_price;
        let gas_cost = self.estimate_optimal_gas_price() as f64 * 2.0 * 5000.0;
        
        if profitability < gas_cost + MIN_PROFIT_THRESHOLD as f64 {
            return 0.1;
        }

        let profit_ratio = (profitability - gas_cost) / gas_cost;
        let base_risk = 0.1 + (profit_ratio * 0.2).min(0.4);
        
        let volatility_factor = self.get_volatility(token_mint) * 2.0;
        let toxicity_factor = microstructure.order_flow_toxicity;
        
        (base_risk + volatility_factor + toxicity_factor).min(0.9)
    }

    pub fn calculate_max_position_size(
        &self,
        token_mint: &Pubkey,
        max_impact_bps: u16,
        available_capital: u64,
    ) -> u64 {
        let market_data = self.market_data.read().unwrap();
        let order_book = match market_data.get(token_mint) {
            Some(ob) => ob,
            None => return 0,
        };

        let max_impact = max_impact_bps as f64 / 10000.0;
        let mut cumulative_size = 0u64;
        let mut cumulative_cost = 0.0;

        for level in &order_book.asks {
            let level_impact = ((level.price / order_book.mid_price) - 1.0).abs();
            if level_impact > max_impact {
                break;
            }
            
            cumulative_size += level.size;
            cumulative_cost += level.size as f64 * level.price;
            
            if cumulative_cost > available_capital as f64 {
                let remaining_capital = available_capital as f64 - (cumulative_cost - level.size as f64 * level.price);
                let final_size = cumulative_size - level.size + (remaining_capital / level.price) as u64;
                return final_size.min(MAX_POSITION_SIZE);
            }
        }

                cumulative_size.min(available_capital).min(MAX_POSITION_SIZE)
    }

    pub fn build_optimized_transaction(
        &self,
        instructions: Vec<Instruction>,
        payer: &Keypair,
        recent_blockhash: solana_sdk::hash::Hash,
        priority_fee: u64,
    ) -> Transaction {
        let compute_unit_limit = 400_000u32;
        let compute_unit_price = priority_fee.min(MAX_PRIORITY_FEE);

        let priority_fee_ix = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
            compute_unit_price
        );
        
        let compute_limit_ix = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(
            compute_unit_limit
        );

        let mut all_instructions = vec![priority_fee_ix, compute_limit_ix];
        all_instructions.extend(instructions);

        Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&payer.pubkey()),
            &[payer],
            recent_blockhash,
        )
    }

    pub fn validate_execution_params(
        &self,
        token_mint: &Pubkey,
        amount: u64,
        max_slippage_bps: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if amount == 0 {
            return Err("Amount must be greater than zero".into());
        }

        if amount > MAX_POSITION_SIZE {
            return Err(format!("Amount exceeds maximum position size of {}", MAX_POSITION_SIZE).into());
        }

        if max_slippage_bps > 1000 {
            return Err("Slippage tolerance exceeds 10%".into());
        }

        let market_data = self.market_data.read().unwrap();
        if !market_data.contains_key(token_mint) {
            return Err("No market data available for token".into());
        }

        let order_book = &market_data[token_mint];
        if order_book.last_update.elapsed().as_secs() > 30 {
            return Err("Market data is stale".into());
        }

        if order_book.bids.is_empty() || order_book.asks.is_empty() {
            return Err("Insufficient market depth".into());
        }

        Ok(())
    }

    pub fn calculate_reversion_probability(
        &self,
        token_mint: &Pubkey,
        transaction_size: u64,
        gas_price: u64,
    ) -> f64 {
        let base_reversion_rate = 0.02;
        
        let size_factor = (transaction_size as f64 / 1_000_000_000.0).min(1.0) * 0.05;
        
        let gas_factor = if gas_price < 10_000 {
            0.1
        } else if gas_price < 100_000 {
            0.05
        } else {
            0.02
        };
        
        let volatility = self.get_volatility(token_mint);
        let volatility_factor = (volatility / 0.5).min(1.0) * 0.03;
        
        let market_data = self.market_data.read().unwrap();
        let congestion_factor = if let Some(order_book) = market_data.get(token_mint) {
            if order_book.last_update.elapsed().as_millis() > 500 {
                0.05
            } else {
                0.0
            }
        } else {
            0.1
        };
        
        (base_reversion_rate + size_factor + gas_factor + volatility_factor + congestion_factor).min(0.5)
    }

    pub fn get_execution_analytics(&self, token_mint: &Pubkey) -> ExecutionAnalytics {
        let market_data = self.market_data.read().unwrap();
        let volatility = self.get_volatility(token_mint);
        let gas_price = self.estimate_optimal_gas_price();

        let order_book = market_data.get(token_mint);
        let (spread, depth, mid_price) = if let Some(ob) = order_book {
            let spread = if let (Some(bid), Some(ask)) = (ob.bids.first(), ob.asks.first()) {
                (ask.price - bid.price) / ob.mid_price
            } else {
                0.01
            };
            
            let depth: u64 = ob.bids.iter().chain(ob.asks.iter())
                .take(10)
                .map(|l| l.size)
                .sum();
            
            (spread, depth, ob.mid_price)
        } else {
            (0.01, 0, 0.0)
        };

        ExecutionAnalytics {
            average_spread: spread,
            total_depth: depth,
            volatility,
            optimal_gas_price: gas_price,
            mid_price,
            last_update: Instant::now(),
        }
    }

    pub fn monitor_execution_quality(
        &self,
        expected_metrics: &ExecutionMetrics,
        actual_price: f64,
        actual_gas_used: u64,
    ) -> ExecutionQuality {
        let price_deviation = ((actual_price - expected_metrics.expected_price) / expected_metrics.expected_price).abs();
        let price_quality = if price_deviation < 0.001 {
            1.0
        } else if price_deviation < 0.005 {
            0.8
        } else if price_deviation < 0.01 {
            0.6
        } else {
            0.4
        };

        let gas_efficiency = if actual_gas_used < 100_000 {
            1.0
        } else if actual_gas_used < 200_000 {
            0.8
        } else {
            0.6
        };

        let total_cost_realized = price_deviation + (actual_gas_used as f64 * 0.000001);
        let cost_efficiency = (expected_metrics.total_cost / total_cost_realized).min(1.0);

        ExecutionQuality {
            price_quality,
            gas_efficiency,
            cost_efficiency,
            overall_score: (price_quality + gas_efficiency + cost_efficiency) / 3.0,
        }
    }

    pub fn adjust_strategy_parameters(&mut self, quality: &ExecutionQuality) {
        if quality.overall_score < 0.7 {
            self.config.max_slippage_bps = (self.config.max_slippage_bps as f64 * 1.1) as u16;
            self.config.priority_fee_percentile = (self.config.priority_fee_percentile * 1.05).min(0.95);
        } else if quality.overall_score > 0.9 {
            self.config.max_slippage_bps = (self.config.max_slippage_bps as f64 * 0.95) as u16;
            self.config.priority_fee_percentile = (self.config.priority_fee_percentile * 0.98).max(0.5);
        }

        if quality.gas_efficiency < 0.7 {
            self.config.adaptive_fee_enabled = true;
            self.config.urgency_multiplier = (self.config.urgency_multiplier * 0.9).max(1.0);
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionAnalytics {
    pub average_spread: f64,
    pub total_depth: u64,
    pub volatility: f64,
    pub optimal_gas_price: u64,
    pub mid_price: f64,
    pub last_update: Instant,
}

#[derive(Debug, Clone)]
pub struct ExecutionQuality {
    pub price_quality: f64,
    pub gas_efficiency: f64,
    pub cost_efficiency: f64,
    pub overall_score: f64,
}

impl ExecutionShortfallMinimizer {
    pub fn estimate_frontrun_protection_fee(
        &self,
        base_fee: u64,
        transaction_value: u64,
        urgency: f64,
    ) -> u64 {
        let value_factor = (transaction_value as f64 / 1_000_000_000.0).sqrt();
        let urgency_boost = 1.0 + (urgency * 0.5);
        let protection_multiplier = 1.2 + (value_factor * 0.1);
        
        let protected_fee = (base_fee as f64 * protection_multiplier * urgency_boost) as u64;
        protected_fee.max(base_fee + 1000).min(MAX_PRIORITY_FEE)
    }

    pub fn calculate_mev_resistance_score(
        &self,
        token_mint: &Pubkey,
        transaction_size: u64,
        execution_strategy: &str,
    ) -> f64 {
        let base_resistance = match execution_strategy {
            "atomic" => 0.9,
            "split" => 0.7,
            "twap" => 0.8,
            _ => 0.5,
        };

        let size_vulnerability = (transaction_size as f64 / MAX_POSITION_SIZE as f64).min(1.0) * 0.2;
        let volatility = self.get_volatility(token_mint);
        let volatility_vulnerability = (volatility / 0.5).min(1.0) * 0.1;
        
        (base_resistance - size_vulnerability - volatility_vulnerability).max(0.3)
    }

    pub fn optimize_for_latency(&self, current_slot: u64, target_slot: u64) -> ExecutionTiming {
        let slots_until_target = target_slot.saturating_sub(current_slot);
        let ms_until_target = slots_until_target * SLOT_DURATION_MS;
        
        let optimal_send_time = if ms_until_target > 2000 {
            ms_until_target - 1600
        } else if ms_until_target > 800 {
            ms_until_target - 600
        } else {
            0
        };

        let priority_boost = if ms_until_target < 800 {
            2.0
        } else if ms_until_target < 1600 {
            1.5
        } else {
            1.0
        };

        ExecutionTiming {
            send_after_ms: optimal_send_time,
            priority_multiplier: priority_boost,
            use_skip_preflight: ms_until_target < 800,
            estimated_confirmation_ms: 400 + (slots_until_target * 50).min(200),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionTiming {
    pub send_after_ms: u64,
    pub priority_multiplier: f64,
    pub use_skip_preflight: bool,
    pub estimated_confirmation_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_shortfall_minimizer() {
        let config = ExecutionConfig::default();
        let minimizer = ExecutionShortfallMinimizer::new("https://api.mainnet-beta.solana.com", config);
        
        let token_mint = Pubkey::new_unique();
        let order_book = OrderBook {
            bids: vec![
                PriceLevel { price: 0.99, size: 1000000, cumulative_size: 1000000, providers: vec![] },
                PriceLevel { price: 0.98, size: 2000000, cumulative_size: 3000000, providers: vec![] },
            ],
            asks: vec![
                PriceLevel { price: 1.01, size: 1500000, cumulative_size: 1500000, providers: vec![] },
                PriceLevel { price: 1.02, size: 2500000, cumulative_size: 4000000, providers: vec![] },
            ],
            last_update: Instant::now(),
            mid_price: 1.0,
        };
        
        minimizer.update_market_data(token_mint, order_book);
        
        let metrics = minimizer.calculate_optimal_execution(&token_mint, 500000, true, 0.5).unwrap();
        assert!(metrics.success_probability > 0.0);
        assert!(metrics.price_impact >= 0.0);
    }
}

