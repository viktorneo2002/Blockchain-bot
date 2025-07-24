use borsh::{BorshDeserialize, BorshSerialize};
use solana_program::pubkey::Pubkey;
use std::cmp::{max, min, Ordering};
use std::collections::{BinaryHeap, HashMap};

const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const BASIS_POINTS: u64 = 10_000;
const PRECISION_FACTOR: u128 = 1_000_000_000_000_000_000;
const MAX_LEVERAGE: u64 = 20;
const MIN_LIQUIDATION_BONUS: u64 = 100; // 1% in basis points
const MAX_LIQUIDATION_BONUS: u64 = 2000; // 20% in basis points
const ORACLE_CONFIDENCE_THRESHOLD: u64 = 100; // 1% max deviation
const GAS_BUFFER_LAMPORTS: u64 = 5_000_000;
const MAX_CASCADE_DEPTH: usize = 50;
const PRICE_IMPACT_THRESHOLD: u64 = 500; // 5% in basis points

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeveragePosition {
    pub owner: Pubkey,
    pub collateral_mint: Pubkey,
    pub debt_mint: Pubkey,
    pub collateral_amount: u64,
    pub debt_amount: u64,
    pub liquidation_threshold: u64,
    pub liquidation_bonus: u64,
    pub last_update_slot: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct OraclePrice {
    pub price: u64,
    pub confidence: u64,
    pub expo: i32,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct CascadeOpportunity {
    pub positions: Vec<LiquidationTarget>,
    pub total_profit: u64,
    pub required_capital: u64,
    pub execution_cost: u64,
    pub risk_score: u64,
    pub cascade_depth: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct LiquidationTarget {
    pub position: LeveragePosition,
    pub liquidation_amount: u64,
    pub expected_profit: u64,
    pub priority_score: u64,
}

impl Ord for LiquidationTarget {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority_score.cmp(&self.priority_score)
    }
}

impl PartialOrd for LiquidationTarget {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LiquidationTarget {
    fn eq(&self, other: &Self) -> bool {
        self.priority_score == other.priority_score
    }
}

impl Eq for LiquidationTarget {}

pub struct LeverageCascadeCalculator {
    oracle_prices: HashMap<Pubkey, OraclePrice>,
    positions: Vec<LeveragePosition>,
    liquidation_queue: BinaryHeap<LiquidationTarget>,
    cascade_cache: HashMap<u64, CascadeOpportunity>,
    current_slot: u64,
}

impl LeverageCascadeCalculator {
    pub fn new(current_slot: u64) -> Self {
        Self {
            oracle_prices: HashMap::with_capacity(256),
            positions: Vec::with_capacity(10_000),
            liquidation_queue: BinaryHeap::with_capacity(1_000),
            cascade_cache: HashMap::with_capacity(100),
            current_slot,
        }
    }

    pub fn update_oracle_price(&mut self, mint: Pubkey, price: OraclePrice) {
        if price.slot + 10 >= self.current_slot && price.confidence <= ORACLE_CONFIDENCE_THRESHOLD {
            self.oracle_prices.insert(mint, price);
        }
    }

    pub fn add_position(&mut self, position: LeveragePosition) {
        if position.collateral_amount > 0 && position.debt_amount > 0 {
            self.positions.push(position);
        }
    }

    pub fn calculate_health_factor(&self, position: &LeveragePosition) -> Option<u64> {
        let collateral_price = self.oracle_prices.get(&position.collateral_mint)?;
        let debt_price = self.oracle_prices.get(&position.debt_mint)?;

        let collateral_value = self.calculate_value(
            position.collateral_amount,
            collateral_price.price,
            collateral_price.expo,
        )?;

        let debt_value = self.calculate_value(
            position.debt_amount,
            debt_price.price,
            debt_price.expo,
        )?;

        if debt_value == 0 {
            return Some(u64::MAX);
        }

        let adjusted_collateral = collateral_value
            .checked_mul(position.liquidation_threshold)?
            .checked_div(BASIS_POINTS)?;

        adjusted_collateral.checked_mul(PRECISION_FACTOR as u64)?
            .checked_div(debt_value)
    }

    pub fn identify_liquidation_targets(&mut self) -> Vec<LiquidationTarget> {
        self.liquidation_queue.clear();

        for position in &self.positions {
            if let Some(health_factor) = self.calculate_health_factor(position) {
                if health_factor < PRECISION_FACTOR as u64 {
                    if let Some(target) = self.create_liquidation_target(position) {
                        self.liquidation_queue.push(target);
                    }
                }
            }
        }

        let mut targets = Vec::with_capacity(min(100, self.liquidation_queue.len()));
        while let Some(target) = self.liquidation_queue.pop() {
            targets.push(target);
            if targets.len() >= 100 {
                break;
            }
        }

        targets
    }

    pub fn calculate_cascade_opportunity(
        &mut self,
        available_capital: u64,
        max_gas_cost: u64,
    ) -> Option<CascadeOpportunity> {
        let cache_key = available_capital.wrapping_add(self.current_slot);
        if let Some(cached) = self.cascade_cache.get(&cache_key) {
            if cached.execution_cost <= max_gas_cost {
                return Some(cached.clone());
            }
        }

        let targets = self.identify_liquidation_targets();
        if targets.is_empty() {
            return None;
        }

        let mut selected_positions = Vec::with_capacity(MAX_CASCADE_DEPTH);
        let mut total_profit = 0u64;
        let mut required_capital = 0u64;
        let mut execution_cost = GAS_BUFFER_LAMPORTS;

        for target in targets {
            let position_capital = self.calculate_required_capital(&target)?;
            
            if required_capital.saturating_add(position_capital) > available_capital {
                continue;
            }

            let position_gas = self.estimate_liquidation_gas(&target);
            if execution_cost.saturating_add(position_gas) > max_gas_cost {
                break;
            }

            required_capital = required_capital.saturating_add(position_capital);
            execution_cost = execution_cost.saturating_add(position_gas);
            total_profit = total_profit.saturating_add(target.expected_profit);
            selected_positions.push(target);

            if selected_positions.len() >= MAX_CASCADE_DEPTH {
                break;
            }
        }

        if selected_positions.is_empty() || total_profit <= execution_cost {
            return None;
        }

        let opportunity = CascadeOpportunity {
            positions: selected_positions.clone(),
            total_profit,
            required_capital,
            execution_cost,
            risk_score: self.calculate_risk_score(&selected_positions),
            cascade_depth: selected_positions.len(),
        };

        self.cascade_cache.insert(cache_key, opportunity.clone());
        Some(opportunity)
    }

    pub fn optimize_liquidation_amount(&self, position: &LeveragePosition) -> Option<u64> {
        let health_factor = self.calculate_health_factor(position)?;
        if health_factor >= PRECISION_FACTOR as u64 {
            return None;
        }

        let collateral_price = self.oracle_prices.get(&position.collateral_mint)?;
        let debt_price = self.oracle_prices.get(&position.debt_mint)?;

        let max_liquidation_ratio = self.calculate_max_liquidation_ratio(health_factor);
        let max_liquidation_amount = position.debt_amount
            .checked_mul(max_liquidation_ratio)?
            .checked_div(BASIS_POINTS)?;

        let price_impact = self.estimate_price_impact(
            position.debt_mint,
            max_liquidation_amount,
            debt_price.price,
        );

        if price_impact > PRICE_IMPACT_THRESHOLD {
            let adjusted_amount = max_liquidation_amount
                .checked_mul(PRICE_IMPACT_THRESHOLD)?
                .checked_div(price_impact)?;
            Some(min(adjusted_amount, max_liquidation_amount))
        } else {
            Some(max_liquidation_amount)
        }
    }

    pub fn calculate_expected_profit(
        &self,
        position: &LeveragePosition,
        liquidation_amount: u64,
    ) -> Option<u64> {
        let collateral_price = self.oracle_prices.get(&position.collateral_mint)?;
        let debt_price = self.oracle_prices.get(&position.debt_mint)?;

        let debt_value = self.calculate_value(
            liquidation_amount,
            debt_price.price,
            debt_price.expo,
        )?;

        let collateral_ratio = liquidation_amount.checked_mul(PRECISION_FACTOR as u64)?
            .checked_div(position.debt_amount)?;

        let collateral_received = position.collateral_amount
            .checked_mul(collateral_ratio)?
            .checked_div(PRECISION_FACTOR as u64)?;

        let bonus_amount = collateral_received
            .checked_mul(position.liquidation_bonus)?
            .checked_div(BASIS_POINTS)?;

        let total_collateral = collateral_received.checked_add(bonus_amount)?;

        let collateral_value = self.calculate_value(
            total_collateral,
            collateral_price.price,
            collateral_price.expo,
        )?;

        collateral_value.checked_sub(debt_value)
    }

    pub fn validate_cascade_execution(&self, opportunity: &CascadeOpportunity) -> bool {
        if opportunity.positions.is_empty() || opportunity.cascade_depth > MAX_CASCADE_DEPTH {
            return false;
        }

        let mut simulated_capital = opportunity.required_capital;
        
        for target in &opportunity.positions {
            if let Some(health_factor) = self.calculate_health_factor(&target.position) {
                if health_factor >= PRECISION_FACTOR as u64 {
                    return false;
                }
            } else {
                return false;
            }

            if let Some(required) = self.calculate_required_capital(target) {
                if required > simulated_capital {
                    return false;
                }
                simulated_capital = simulated_capital.saturating_sub(required);
                simulated_capital = simulated_capital.saturating_add(target.expected_profit);
            } else {
                return false;
            }
        }

        opportunity.total_profit > opportunity.execution_cost.saturating_mul(2)
    }

    fn create_liquidation_target(&self, position: &LeveragePosition) -> Option<LiquidationTarget> {
        let liquidation_amount = self.optimize_liquidation_amount(position)?;
        let expected_profit = self.calculate_expected_profit(position, liquidation_amount)?;

        if expected_profit == 0 {
            return None;
        }

        let health_factor = self.calculate_health_factor(position)?;
        let urgency_score = PRECISION_FACTOR as u64 / max(1, health_factor);
        
        let profitability_score = expected_profit
            .checked_mul(BASIS_POINTS)?
            .checked_div(max(1, liquidation_amount))?;

        let priority_score = urgency_score.saturating_add(profitability_score);

        Some(LiquidationTarget {
            position: *position,
            liquidation_amount,
            expected_profit,
            priority_score,
        })
    }

    fn calculate_value(&self, amount: u64, price: u64, expo: i32) -> Option<u64> {
        let scaled_amount = amount as u128;
        let scaled_price = price as u128;

        let value = if expo < 0 {
            let divisor = 10u128.checked_pow(expo.abs() as u32)?;
            scaled_amount.checked_mul(scaled_price)?.checked_div(divisor)?
        } else {
            let multiplier = 10u128.checked_pow(expo as u32)?;
            scaled_amount.checked_mul(scaled_price)?.checked_mul(multiplier)?
        };

        if value > u64::MAX as u128 {
            None
        } else {
            Some(value as u64)
        }
    }

    fn calculate_max_liquidation_ratio(&self, health_factor: u64) -> u64 {
        if health_factor == 0 {
            return BASIS_POINTS;
        }

        let deficit_ratio = PRECISION_FACTOR as u64 / health_factor;
        min(BASIS_POINTS, deficit_ratio.saturating_mul(5000) / BASIS_POINTS)
    }

    fn estimate_price_impact(&self, mint: Pubkey, amount: u64, current_price: u64) -> u64 {
        let impact_coefficient = 100; // 1% per 100k tokens
        let normalized_amount = amount / 100_000;
        min(normalized_amount * impact_coefficient, PRICE_IMPACT_THRESHOLD)
    }

    fn calculate_required_capital(&self, target: &LiquidationTarget) -> Option<u64> {
        let debt_price = self.oracle_prices.get(&target.position.debt_mint)?;
        
        let capital = self.calculate_value(
            target.liquidation_amount,
                        debt_price.price,
            debt_price.expo,
        )?;
        
        let buffer = capital.checked_mul(105)?.checked_div(100)?;
        Some(buffer)
    }

    fn estimate_liquidation_gas(&self, target: &LiquidationTarget) -> u64 {
        let base_cost = 50_000;
        let per_account_cost = 10_000;
        let compute_units = 200_000;
        
        let accounts_involved = 8;
        let total_accounts_cost = per_account_cost.saturating_mul(accounts_involved);
        
        let complexity_multiplier = if target.liquidation_amount > 1_000_000_000 {
            2
        } else {
            1
        };
        
        base_cost
            .saturating_add(total_accounts_cost)
            .saturating_add(compute_units)
            .saturating_mul(complexity_multiplier)
    }

    fn calculate_risk_score(&self, positions: &[LiquidationTarget]) -> u64 {
        if positions.is_empty() {
            return u64::MAX;
        }

        let mut total_risk = 0u64;
        let mut concentration_map: HashMap<Pubkey, u64> = HashMap::new();

        for target in positions {
            let position_risk = self.calculate_position_risk(&target.position);
            total_risk = total_risk.saturating_add(position_risk);

            *concentration_map.entry(target.position.collateral_mint).or_insert(0) += 1;
            *concentration_map.entry(target.position.debt_mint).or_insert(0) += 1;
        }

        let max_concentration = concentration_map.values().max().copied().unwrap_or(0);
        let concentration_penalty = max_concentration.saturating_mul(1000);

        let avg_risk = total_risk.checked_div(positions.len() as u64).unwrap_or(u64::MAX);
        avg_risk.saturating_add(concentration_penalty)
    }

    fn calculate_position_risk(&self, position: &LeveragePosition) -> u64 {
        let leverage = position.debt_amount
            .checked_mul(BASIS_POINTS)
            .and_then(|d| d.checked_div(max(1, position.collateral_amount)))
            .unwrap_or(u64::MAX);

        let volatility_risk = self.estimate_volatility_risk(position);
        let liquidity_risk = self.estimate_liquidity_risk(position);
        
        leverage
            .saturating_add(volatility_risk)
            .saturating_add(liquidity_risk)
            .checked_div(3)
            .unwrap_or(u64::MAX)
    }

    fn estimate_volatility_risk(&self, position: &LeveragePosition) -> u64 {
        let collateral_volatility = self.get_asset_volatility(position.collateral_mint);
        let debt_volatility = self.get_asset_volatility(position.debt_mint);
        
        collateral_volatility.saturating_add(debt_volatility).checked_div(2).unwrap_or(0)
    }

    fn estimate_liquidity_risk(&self, position: &LeveragePosition) -> u64 {
        let collateral_liquidity = self.get_asset_liquidity_score(position.collateral_mint);
        let debt_liquidity = self.get_asset_liquidity_score(position.debt_mint);
        
        BASIS_POINTS
            .saturating_sub(min(collateral_liquidity, debt_liquidity))
            .checked_div(100)
            .unwrap_or(0)
    }

    fn get_asset_volatility(&self, mint: Pubkey) -> u64 {
        match mint.to_bytes()[0] {
            0x00..=0x3F => 200,  // Stablecoins
            0x40..=0x7F => 500,  // Major tokens
            0x80..=0xBF => 1000, // Mid-cap tokens
            _ => 2000,           // Small-cap tokens
        }
    }

    fn get_asset_liquidity_score(&self, mint: Pubkey) -> u64 {
        match mint.to_bytes()[0] {
            0x00..=0x3F => 9500,  // High liquidity
            0x40..=0x7F => 7500,  // Good liquidity
            0x80..=0xBF => 5000,  // Medium liquidity
            _ => 2500,            // Low liquidity
        }
    }

    pub fn update_slot(&mut self, slot: u64) {
        self.current_slot = slot;
        self.clean_stale_data();
    }

    fn clean_stale_data(&mut self) {
        let stale_threshold = self.current_slot.saturating_sub(150);
        
        self.oracle_prices.retain(|_, price| price.slot > stale_threshold);
        self.positions.retain(|pos| pos.last_update_slot > stale_threshold);
        
        if self.cascade_cache.len() > 100 {
            self.cascade_cache.clear();
        }
    }

    pub fn get_optimal_execution_order(&self, opportunity: &CascadeOpportunity) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..opportunity.positions.len()).collect();
        
        indices.sort_by(|&a, &b| {
            let pos_a = &opportunity.positions[a];
            let pos_b = &opportunity.positions[b];
            
            let profit_ratio_a = pos_a.expected_profit
                .checked_mul(BASIS_POINTS)
                .and_then(|p| p.checked_div(max(1, pos_a.liquidation_amount)))
                .unwrap_or(0);
                
            let profit_ratio_b = pos_b.expected_profit
                .checked_mul(BASIS_POINTS)
                .and_then(|p| p.checked_div(max(1, pos_b.liquidation_amount)))
                .unwrap_or(0);
            
            profit_ratio_b.cmp(&profit_ratio_a)
        });
        
        indices
    }

    pub fn simulate_cascade_impact(
        &self,
        opportunity: &CascadeOpportunity,
        market_conditions: &MarketConditions,
    ) -> CascadeSimulation {
        let mut cumulative_profit = 0u64;
        let mut cumulative_cost = opportunity.execution_cost;
        let mut success_probability = 100u64;

        for (idx, target) in opportunity.positions.iter().enumerate() {
            let slippage = self.estimate_execution_slippage(target, market_conditions);
            let adjusted_profit = target.expected_profit
                .saturating_sub(target.expected_profit.saturating_mul(slippage) / BASIS_POINTS);
            
            cumulative_profit = cumulative_profit.saturating_add(adjusted_profit);
            
            let position_success_rate = self.estimate_success_rate(target, market_conditions);
            success_probability = success_probability
                .saturating_mul(position_success_rate)
                .checked_div(100)
                .unwrap_or(0);
            
            if success_probability < 50 {
                break;
            }
        }

        CascadeSimulation {
            expected_profit: cumulative_profit,
            total_cost: cumulative_cost,
            success_probability,
            optimal_positions: opportunity.positions.len(),
        }
    }

    fn estimate_execution_slippage(
        &self,
        target: &LiquidationTarget,
        market_conditions: &MarketConditions,
    ) -> u64 {
        let base_slippage = 10; // 0.1%
        let size_impact = target.liquidation_amount
            .checked_div(market_conditions.avg_volume)
            .unwrap_or(0)
            .saturating_mul(100);
        
        let volatility_impact = market_conditions.volatility
            .checked_div(10)
            .unwrap_or(0);
        
        min(
            base_slippage.saturating_add(size_impact).saturating_add(volatility_impact),
            500 // Max 5% slippage
        )
    }

    fn estimate_success_rate(
        &self,
        target: &LiquidationTarget,
        market_conditions: &MarketConditions,
    ) -> u64 {
        let base_rate = 95u64;
        
        let competition_penalty = min(market_conditions.competition_level, 30);
        let network_penalty = market_conditions.network_congestion
            .checked_div(10)
            .unwrap_or(0);
        
        base_rate
            .saturating_sub(competition_penalty)
            .saturating_sub(network_penalty)
            .max(10)
    }

    pub fn calculate_dynamic_parameters(
        &self,
        market_volatility: u64,
        network_congestion: u64,
    ) -> DynamicParameters {
        let adjusted_threshold = if market_volatility > 1000 {
            ORACLE_CONFIDENCE_THRESHOLD.saturating_mul(2)
        } else {
            ORACLE_CONFIDENCE_THRESHOLD
        };

        let gas_multiplier = if network_congestion > 80 {
            150 // 1.5x
        } else if network_congestion > 50 {
            120 // 1.2x
        } else {
            100 // 1x
        };

        let max_positions = if market_volatility > 2000 {
            MAX_CASCADE_DEPTH / 2
        } else {
            MAX_CASCADE_DEPTH
        };

        DynamicParameters {
            confidence_threshold: adjusted_threshold,
            gas_multiplier,
            max_cascade_positions: max_positions,
            min_profit_threshold: GAS_BUFFER_LAMPORTS.saturating_mul(gas_multiplier as u64) / 100,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MarketConditions {
    pub volatility: u64,
    pub avg_volume: u64,
    pub competition_level: u64,
    pub network_congestion: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct CascadeSimulation {
    pub expected_profit: u64,
    pub total_cost: u64,
    pub success_probability: u64,
    pub optimal_positions: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct DynamicParameters {
    pub confidence_threshold: u64,
    pub gas_multiplier: u32,
    pub max_cascade_positions: usize,
    pub min_profit_threshold: u64,
}

impl Default for MarketConditions {
    fn default() -> Self {
        Self {
            volatility: 500,
            avg_volume: 1_000_000_000,
            competition_level: 50,
            network_congestion: 30,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_factor_calculation() {
        let mut calculator = LeverageCascadeCalculator::new(100);
        
        let position = LeveragePosition {
            owner: Pubkey::default(),
            collateral_mint: Pubkey::new_unique(),
            debt_mint: Pubkey::new_unique(),
            collateral_amount: 1_000_000_000,
            debt_amount: 500_000_000,
            liquidation_threshold: 8000,
            liquidation_bonus: 500,
            last_update_slot: 100,
        };

        calculator.update_oracle_price(
            position.collateral_mint,
            OraclePrice {
                price: 100_000_000,
                confidence: 50,
                expo: -8,
                slot: 100,
            },
        );

        calculator.update_oracle_price(
            position.debt_mint,
            OraclePrice {
                price: 100_000_000,
                confidence: 50,
                expo: -8,
                slot: 100,
            },
        );

        let health_factor = calculator.calculate_health_factor(&position);
        assert!(health_factor.is_some());
    }
}

