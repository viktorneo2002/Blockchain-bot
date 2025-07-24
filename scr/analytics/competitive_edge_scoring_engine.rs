use solana_sdk::{
    clock::Slot,
    pubkey::Pubkey,
    signature::Signature,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpportunityType {
    Arbitrage,
    Liquidation,
    SandwichAttack,
    JitLiquidity,
    FlashLoan,
}

#[derive(Debug, Clone)]
pub struct OpportunityMetrics {
    pub opportunity_type: OpportunityType,
    pub gross_profit_lamports: u64,
    pub priority_fee_lamports: u64,
    pub compute_units: u32,
    pub accounts_touched: Vec<Pubkey>,
    pub competing_bots: u32,
    pub market_volatility: f64,
    pub liquidity_depth: u64,
    pub slippage_tolerance: f64,
    pub execution_complexity: u8,
    pub time_sensitivity_ms: u64,
    pub bundle_size: usize,
}

#[derive(Debug, Clone)]
pub struct NetworkConditions {
    pub slot: Slot,
    pub recent_blockhash_age: u64,
    pub network_congestion: f64,
    pub average_priority_fee: u64,
    pub slot_success_rate: f64,
    pub leader_schedule: HashMap<Pubkey, Vec<Slot>>,
    pub rpc_latency_ms: u64,
}

#[derive(Debug, Clone)]
pub struct HistoricalPerformance {
    pub success_count: u64,
    pub failure_count: u64,
    pub total_profit: i64,
    pub average_priority_fee: u64,
    pub average_execution_time_ms: u64,
    pub revert_reasons: HashMap<String, u32>,
}

#[derive(Debug, Clone)]
pub struct CompetitorAnalysis {
    pub bot_pubkey: Pubkey,
    pub success_rate: f64,
    pub average_priority_fee: u64,
    pub typical_strategies: Vec<OpportunityType>,
    pub reaction_time_ms: u64,
    pub market_share: f64,
}

pub struct CompetitiveEdgeScoringEngine {
    historical_data: Arc<RwLock<HashMap<OpportunityType, HistoricalPerformance>>>,
    competitor_data: Arc<RwLock<HashMap<Pubkey, CompetitorAnalysis>>>,
    slot_performance: Arc<RwLock<VecDeque<(Slot, f64)>>>,
    scoring_weights: ScoringWeights,
    risk_parameters: RiskParameters,
}

#[derive(Debug, Clone)]
struct ScoringWeights {
    profit_weight: Decimal,
    success_probability_weight: Decimal,
    competition_weight: Decimal,
    timing_weight: Decimal,
    complexity_weight: Decimal,
    network_condition_weight: Decimal,
    historical_performance_weight: Decimal,
}

#[derive(Debug, Clone)]
struct RiskParameters {
    max_priority_fee_ratio: Decimal,
    min_profit_threshold: u64,
    max_compute_units: u32,
    max_accounts_per_tx: usize,
    volatility_penalty_factor: Decimal,
    congestion_penalty_factor: Decimal,
    failure_penalty_multiplier: Decimal,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            profit_weight: dec!(0.35),
            success_probability_weight: dec!(0.25),
            competition_weight: dec!(0.15),
            timing_weight: dec!(0.10),
            complexity_weight: dec!(0.05),
            network_condition_weight: dec!(0.05),
            historical_performance_weight: dec!(0.05),
        }
    }
}

impl Default for RiskParameters {
    fn default() -> Self {
        Self {
            max_priority_fee_ratio: dec!(0.30),
            min_profit_threshold: 100_000,
            max_compute_units: 1_400_000,
            max_accounts_per_tx: 64,
            volatility_penalty_factor: dec!(0.02),
            congestion_penalty_factor: dec!(0.03),
            failure_penalty_multiplier: dec!(2.5),
        }
    }
}

impl CompetitiveEdgeScoringEngine {
    pub fn new() -> Self {
        Self {
            historical_data: Arc::new(RwLock::new(HashMap::new())),
            competitor_data: Arc::new(RwLock::new(HashMap::new())),
            slot_performance: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            scoring_weights: ScoringWeights::default(),
            risk_parameters: RiskParameters::default(),
        }
    }

    pub fn score_opportunity(
        &self,
        metrics: &OpportunityMetrics,
        network: &NetworkConditions,
    ) -> Result<Decimal, &'static str> {
        if !self.validate_opportunity(metrics)? {
            return Ok(Decimal::ZERO);
        }

        let profit_score = self.calculate_profit_score(metrics, network)?;
        let success_probability = self.calculate_success_probability(metrics, network)?;
        let competition_score = self.calculate_competition_score(metrics)?;
        let timing_score = self.calculate_timing_score(metrics, network)?;
        let complexity_score = self.calculate_complexity_score(metrics)?;
        let network_score = self.calculate_network_score(network)?;
        let historical_score = self.calculate_historical_score(metrics)?;

        let weighted_score = profit_score * self.scoring_weights.profit_weight
            + success_probability * self.scoring_weights.success_probability_weight
            + competition_score * self.scoring_weights.competition_weight
            + timing_score * self.scoring_weights.timing_weight
            + complexity_score * self.scoring_weights.complexity_weight
            + network_score * self.scoring_weights.network_condition_weight
            + historical_score * self.scoring_weights.historical_performance_weight;

        let risk_adjusted_score = self.apply_risk_adjustments(weighted_score, metrics, network)?;
        
        Ok(risk_adjusted_score.min(dec!(100)).max(dec!(0)))
    }

    fn validate_opportunity(&self, metrics: &OpportunityMetrics) -> Result<bool, &'static str> {
        if metrics.compute_units > self.risk_parameters.max_compute_units {
            return Ok(false);
        }

        if metrics.accounts_touched.len() > self.risk_parameters.max_accounts_per_tx {
            return Ok(false);
        }

        let net_profit = metrics.gross_profit_lamports.saturating_sub(metrics.priority_fee_lamports);
        if net_profit < self.risk_parameters.min_profit_threshold {
            return Ok(false);
        }

        let fee_ratio = Decimal::from(metrics.priority_fee_lamports) / 
                       Decimal::from(metrics.gross_profit_lamports.max(1));
        if fee_ratio > self.risk_parameters.max_priority_fee_ratio {
            return Ok(false);
        }

        Ok(true)
    }

    fn calculate_profit_score(
        &self,
        metrics: &OpportunityMetrics,
        network: &NetworkConditions,
    ) -> Result<Decimal, &'static str> {
        let net_profit = metrics.gross_profit_lamports.saturating_sub(metrics.priority_fee_lamports);
        let base_score = (Decimal::from(net_profit) / dec!(1_000_000_000)).min(dec!(100));
        
        let market_adjusted_score = match metrics.opportunity_type {
            OpportunityType::Arbitrage => {
                let volatility_adjustment = dec!(1) - (Decimal::from_f64(metrics.market_volatility)
                    .unwrap_or(dec!(0)) * self.risk_parameters.volatility_penalty_factor);
                base_score * volatility_adjustment
            },
            OpportunityType::Liquidation => {
                let urgency_bonus = dec!(1) + (dec!(1000) / 
                    Decimal::from(metrics.time_sensitivity_ms.max(100)));
                base_score * urgency_bonus.min(dec!(1.5))
            },
            OpportunityType::SandwichAttack => {
                let slippage_penalty = dec!(1) - Decimal::from_f64(metrics.slippage_tolerance)
                    .unwrap_or(dec!(0));
                base_score * slippage_penalty
            },
            OpportunityType::JitLiquidity => {
                let liquidity_bonus = (Decimal::from(metrics.liquidity_depth) / 
                    dec!(1_000_000_000)).min(dec!(1.2));
                base_score * liquidity_bonus
            },
            OpportunityType::FlashLoan => {
                let complexity_penalty = dec!(1) - (Decimal::from(metrics.execution_complexity) / 
                    dec!(100));
                base_score * complexity_penalty
            },
        };

        Ok(market_adjusted_score)
    }

    fn calculate_success_probability(
        &self,
        metrics: &OpportunityMetrics,
        network: &NetworkConditions,
    ) -> Result<Decimal, &'static str> {
        let historical = self.historical_data.read().unwrap();
        let base_probability = if let Some(perf) = historical.get(&metrics.opportunity_type) {
            let total = perf.success_count + perf.failure_count;
            if total > 0 {
                Decimal::from(perf.success_count) / Decimal::from(total)
            } else {
                dec!(0.5)
            }
        } else {
            dec!(0.5)
        };

        let compute_factor = dec!(1) - (Decimal::from(metrics.compute_units) / 
            Decimal::from(self.risk_parameters.max_compute_units) * dec!(0.3));
        
        let network_factor = Decimal::from_f64(network.slot_success_rate)
            .unwrap_or(dec!(0.8));
        
        let competition_factor = dec!(1) / (dec!(1) + Decimal::from(metrics.competing_bots) * dec!(0.1));
        
        let timing_factor = if metrics.time_sensitivity_ms < 100 {
            dec!(0.7)
        } else if metrics.time_sensitivity_ms < 500 {
            dec!(0.85)
        } else {
            dec!(0.95)
        };

        let adjusted_probability = base_probability * compute_factor * 
            network_factor * competition_factor * timing_factor;

        Ok(adjusted_probability.min(dec!(1)).max(dec!(0)))
    }

    fn calculate_competition_score(&self, metrics: &OpportunityMetrics) -> Result<Decimal, &'static str> {
        let competitors = self.competitor_data.read().unwrap();
        
        let active_competitors = competitors.values()
            .filter(|c| c.typical_strategies.contains(&metrics.opportunity_type))
            .count();

        let competition_intensity = Decimal::from(active_competitors + metrics.competing_bots as usize);
        let base_score = dec!(100) / (dec!(1) + competition_intensity * dec!(0.2));

        let priority_fee_competitiveness = if metrics.priority_fee_lamports > 0 {
            let avg_competitor_fee = competitors.values()
                .map(|c| c.average_priority_fee)
                .sum::<u64>() / competitors.len().max(1) as u64;
            
            if metrics.priority_fee_lamports > avg_competitor_fee {
                dec!(1) + (Decimal::from(metrics.priority_fee_lamports - avg_competitor_fee) / 
                    Decimal::from(avg_competitor_fee.max(1)) * dec!(0.2)).min(dec!(0.5))
            } else {
                dec!(1) - (Decimal::from(avg_competitor_fee - metrics.priority_fee_lamports) / 
                    Decimal::from(avg_competitor_fee.max(1)) * dec!(0.3)).max(dec!(0.5))
            }
        } else {
            dec!(0.5)
        };

        Ok(base_score * priority_fee_competitiveness)
    }

    fn calculate_timing_score(
        &self,
        metrics: &OpportunityMetrics,
        network: &NetworkConditions,
    ) -> Result<Decimal, &'static str> {
        let latency_penalty = Decimal::from(network.rpc_latency_ms) / dec!(1000);
        let time_pressure = Decimal::from(metrics.time_sensitivity_ms) / dec!(10000);
        
        let blockhash_freshness = dec!(1) - (Decimal::from(network.recent_blockhash_age) / dec!(150));
        
        let execution_window = match metrics.opportunity_type {
            OpportunityType::Arbitrage => dec!(0.95),
            OpportunityType::SandwichAttack => dec!(0.85),
            OpportunityType::Liquidation => dec!(0.90),
            OpportunityType::JitLiquidity => dec!(0.80),
            OpportunityType::FlashLoan => dec!(0.88),
        };

        let timing_score = (execution_window - latency_penalty - time_pressure) * 
            blockhash_freshness * dec!(100);

        Ok(timing_score.max(dec!(0)))
    }

    fn calculate_complexity_score(&self, metrics: &OpportunityMetrics) -> Result<Decimal, &'static str> {
        let account_complexity = dec!(1) - (Decimal::from(metrics.accounts_touched.len()) / 
            Decimal::from(self.risk_parameters.max_accounts_per_tx) * dec!(0.4));
        
        let compute_complexity = dec!(1) - (Decimal::from(metrics.compute_units) / 
            Decimal::from(self.risk_parameters.max_compute_units) * dec!(0.3));
        
        let execution_complexity = dec!(1) - (Decimal::from(metrics.execution_complexity) / dec!(10) * dec!(0.3));
        
                let bundle_complexity = dec!(1) - (Decimal::from(metrics.bundle_size) / dec!(5) * dec!(0.2)).min(dec!(0.4));
        
        let complexity_score = (account_complexity * dec!(0.3) + 
            compute_complexity * dec!(0.3) + 
            execution_complexity * dec!(0.2) + 
            bundle_complexity * dec!(0.2)) * dec!(100);

        Ok(complexity_score.max(dec!(0)))
    }

    fn calculate_network_score(&self, network: &NetworkConditions) -> Result<Decimal, &'static str> {
        let congestion_factor = dec!(1) - Decimal::from_f64(network.network_congestion)
            .unwrap_or(dec!(0.5)) * self.risk_parameters.congestion_penalty_factor;
        
        let slot_performance = self.slot_performance.read().unwrap();
        let recent_performance = if slot_performance.len() >= 10 {
            let recent_sum: f64 = slot_performance.iter()
                .rev()
                .take(10)
                .map(|(_, perf)| perf)
                .sum();
            Decimal::from_f64(recent_sum / 10.0).unwrap_or(dec!(0.5))
        } else {
            dec!(0.5)
        };
        
        let fee_environment = if network.average_priority_fee > 0 {
            let fee_ratio = Decimal::from(network.average_priority_fee) / dec!(1_000_000);
            (dec!(1) - fee_ratio.min(dec!(0.5)))
        } else {
            dec!(1)
        };

        let network_score = congestion_factor * recent_performance * fee_environment * dec!(100);
        Ok(network_score.max(dec!(0)))
    }

    fn calculate_historical_score(&self, metrics: &OpportunityMetrics) -> Result<Decimal, &'static str> {
        let historical = self.historical_data.read().unwrap();
        
        if let Some(perf) = historical.get(&metrics.opportunity_type) {
            let total_attempts = perf.success_count + perf.failure_count;
            if total_attempts == 0 {
                return Ok(dec!(50));
            }

            let success_rate = Decimal::from(perf.success_count) / Decimal::from(total_attempts);
            let profitability = if perf.total_profit > 0 {
                (Decimal::from(perf.total_profit) / Decimal::from(total_attempts) / dec!(1_000_000)).min(dec!(1))
            } else {
                dec!(0)
            };
            
            let fee_efficiency = if perf.average_priority_fee > 0 && perf.total_profit > 0 {
                let fee_profit_ratio = Decimal::from(perf.average_priority_fee) / 
                    (Decimal::from(perf.total_profit) / Decimal::from(total_attempts).max(dec!(1)));
                (dec!(1) - fee_profit_ratio.min(dec!(1))).max(dec!(0))
            } else {
                dec!(0.5)
            };

            let historical_score = (success_rate * dec!(0.5) + 
                profitability * dec!(0.3) + 
                fee_efficiency * dec!(0.2)) * dec!(100);

            Ok(historical_score)
        } else {
            Ok(dec!(50))
        }
    }

    fn apply_risk_adjustments(
        &self,
        base_score: Decimal,
        metrics: &OpportunityMetrics,
        network: &NetworkConditions,
    ) -> Result<Decimal, &'static str> {
        let mut adjusted_score = base_score;

        let volatility_risk = Decimal::from_f64(metrics.market_volatility)
            .unwrap_or(dec!(0)) * self.risk_parameters.volatility_penalty_factor;
        adjusted_score *= (dec!(1) - volatility_risk).max(dec!(0.7));

        let congestion_risk = Decimal::from_f64(network.network_congestion)
            .unwrap_or(dec!(0)) * self.risk_parameters.congestion_penalty_factor;
        adjusted_score *= (dec!(1) - congestion_risk).max(dec!(0.8));

        if metrics.competing_bots > 5 {
            let competition_penalty = Decimal::from(metrics.competing_bots - 5) * dec!(0.02);
            adjusted_score *= (dec!(1) - competition_penalty.min(dec!(0.3)));
        }

        let historical = self.historical_data.read().unwrap();
        if let Some(perf) = historical.get(&metrics.opportunity_type) {
            if perf.failure_count > perf.success_count * 2 {
                let failure_ratio = Decimal::from(perf.failure_count) / 
                    Decimal::from(perf.success_count.max(1));
                let penalty = (failure_ratio - dec!(2)) * dec!(0.1);
                adjusted_score *= (dec!(1) - penalty.min(dec!(0.5)));
            }
        }

        if metrics.execution_complexity > 7 {
            let complexity_penalty = Decimal::from(metrics.execution_complexity - 7) * dec!(0.05);
            adjusted_score *= (dec!(1) - complexity_penalty.min(dec!(0.2)));
        }

        Ok(adjusted_score)
    }

    pub fn update_historical_performance(
        &self,
        opportunity_type: OpportunityType,
        success: bool,
        profit: i64,
        priority_fee: u64,
        execution_time_ms: u64,
        revert_reason: Option<String>,
    ) -> Result<(), &'static str> {
        let mut historical = self.historical_data.write().unwrap();
        let perf = historical.entry(opportunity_type).or_insert_with(|| HistoricalPerformance {
            success_count: 0,
            failure_count: 0,
            total_profit: 0,
            average_priority_fee: 0,
            average_execution_time_ms: 0,
            revert_reasons: HashMap::new(),
        });

        if success {
            perf.success_count += 1;
        } else {
            perf.failure_count += 1;
            if let Some(reason) = revert_reason {
                *perf.revert_reasons.entry(reason).or_insert(0) += 1;
            }
        }

        perf.total_profit += profit;
        
        let total_attempts = perf.success_count + perf.failure_count;
        perf.average_priority_fee = ((perf.average_priority_fee * (total_attempts - 1)) + priority_fee) / total_attempts;
        perf.average_execution_time_ms = ((perf.average_execution_time_ms * (total_attempts - 1)) + execution_time_ms) / total_attempts;

        Ok(())
    }

    pub fn update_competitor_analysis(
        &self,
        bot_pubkey: Pubkey,
        success_rate: f64,
        priority_fee: u64,
        strategy: OpportunityType,
        reaction_time_ms: u64,
    ) -> Result<(), &'static str> {
        let mut competitors = self.competitor_data.write().unwrap();
        let competitor = competitors.entry(bot_pubkey).or_insert_with(|| CompetitorAnalysis {
            bot_pubkey,
            success_rate: 0.0,
            average_priority_fee: 0,
            typical_strategies: Vec::new(),
            reaction_time_ms: 0,
            market_share: 0.0,
        });

        competitor.success_rate = (competitor.success_rate * 0.9) + (success_rate * 0.1);
        competitor.average_priority_fee = ((competitor.average_priority_fee as f64 * 0.9) + 
            (priority_fee as f64 * 0.1)) as u64;
        
        if !competitor.typical_strategies.contains(&strategy) {
            competitor.typical_strategies.push(strategy);
        }
        
        competitor.reaction_time_ms = ((competitor.reaction_time_ms as f64 * 0.9) + 
            (reaction_time_ms as f64 * 0.1)) as u64;

        let total_market_activity: f64 = competitors.values()
            .map(|c| c.success_rate)
            .sum();
        
        for comp in competitors.values_mut() {
            comp.market_share = comp.success_rate / total_market_activity.max(1.0);
        }

        Ok(())
    }

    pub fn update_slot_performance(&self, slot: Slot, performance: f64) -> Result<(), &'static str> {
        let mut slot_perf = self.slot_performance.write().unwrap();
        
        if slot_perf.len() >= 1000 {
            slot_perf.pop_front();
        }
        
        slot_perf.push_back((slot, performance));
        Ok(())
    }

    pub fn get_opportunity_ranking(
        &self,
        opportunities: Vec<OpportunityMetrics>,
        network: &NetworkConditions,
    ) -> Result<Vec<(usize, Decimal)>, &'static str> {
        let mut scored_opportunities: Vec<(usize, Decimal)> = opportunities
            .iter()
            .enumerate()
            .filter_map(|(idx, metrics)| {
                self.score_opportunity(metrics, network)
                    .ok()
                    .filter(|&score| score > dec!(0))
                    .map(|score| (idx, score))
            })
            .collect();

        scored_opportunities.sort_by(|a, b| b.1.cmp(&a.1));
        
        let top_opportunities: Vec<(usize, Decimal)> = scored_opportunities
            .into_iter()
            .take(10)
            .collect();

        Ok(top_opportunities)
    }

    pub fn should_execute_opportunity(
        &self,
        score: Decimal,
        metrics: &OpportunityMetrics,
        network: &NetworkConditions,
    ) -> bool {
        if score < dec!(30) {
            return false;
        }

        let net_profit = metrics.gross_profit_lamports.saturating_sub(metrics.priority_fee_lamports);
        if net_profit < self.risk_parameters.min_profit_threshold {
            return false;
        }

        if network.network_congestion > 0.85 && score < dec!(70) {
            return false;
        }

        if metrics.competing_bots > 10 && score < dec!(60) {
            return false;
        }

        let historical = self.historical_data.read().unwrap();
        if let Some(perf) = historical.get(&metrics.opportunity_type) {
            let total = perf.success_count + perf.failure_count;
            if total > 100 {
                let success_rate = perf.success_count as f64 / total as f64;
                if success_rate < 0.3 && score < dec!(80) {
                    return false;
                }
            }
        }

        true
    }

    pub fn adjust_weights(&mut self, performance_feedback: HashMap<OpportunityType, f64>) {
        for (opp_type, performance) in performance_feedback {
            let adjustment = Decimal::from_f64(performance).unwrap_or(dec!(1));
            
            if adjustment > dec!(1.2) {
                self.scoring_weights.profit_weight *= dec!(1.05);
                self.scoring_weights.success_probability_weight *= dec!(0.95);
            } else if adjustment < dec!(0.8) {
                self.scoring_weights.profit_weight *= dec!(0.95);
                self.scoring_weights.success_probability_weight *= dec!(1.05);
            }
        }

        let total_weight = self.scoring_weights.profit_weight +
            self.scoring_weights.success_probability_weight +
            self.scoring_weights.competition_weight +
            self.scoring_weights.timing_weight +
            self.scoring_weights.complexity_weight +
            self.scoring_weights.network_condition_weight +
            self.scoring_weights.historical_performance_weight;

        self.scoring_weights.profit_weight /= total_weight;
        self.scoring_weights.success_probability_weight /= total_weight;
        self.scoring_weights.competition_weight /= total_weight;
        self.scoring_weights.timing_weight /= total_weight;
        self.scoring_weights.complexity_weight /= total_weight;
        self.scoring_weights.network_condition_weight /= total_weight;
        self.scoring_weights.historical_performance_weight /= total_weight;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scoring_engine_initialization() {
        let engine = CompetitiveEdgeScoringEngine::new();
        assert!(engine.historical_data.read().unwrap().is_empty());
        assert!(engine.competitor_data.read().unwrap().is_empty());
    }
}

