use solana_sdk::{
    pubkey::Pubkey,
    clock::Clock,
    account::Account,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use serde::{Deserialize, Serialize};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use rand::Rng;

const MAX_POSITION_RATIO: Decimal = dec!(0.15);
const HEAT_DECAY_RATE: f64 = 0.95;
const HEAT_THRESHOLD_HIGH: f64 = 85.0;
const HEAT_THRESHOLD_CRITICAL: f64 = 95.0;
const COOLDOWN_MULTIPLIER: f64 = 1.5;
const MAX_LOSS_THRESHOLD: Decimal = dec!(0.02);
const WIN_RATE_THRESHOLD: f64 = 0.45;
const VOLUME_SPIKE_MULTIPLIER: f64 = 3.0;
const MAX_CONCURRENT_POSITIONS: usize = 8;
const HEAT_WINDOW_SECONDS: u64 = 300;
const POSITION_TIMEOUT_SECONDS: u64 = 180;
const REBALANCE_INTERVAL_SECONDS: u64 = 600;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionMetrics {
    pub entry_price: Decimal,
    pub current_price: Decimal,
    pub size: Decimal,
    pub entry_time: u64,
    pub last_update: u64,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub gas_spent: Decimal,
    pub slippage_cost: Decimal,
    pub heat_contribution: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenHeatProfile {
    pub token_mint: Pubkey,
    pub current_heat: f64,
    pub trade_count: u32,
    pub volume_24h: Decimal,
    pub avg_volume_baseline: Decimal,
    pub last_trade_time: u64,
    pub cooldown_until: Option<u64>,
    pub win_rate: f64,
    pub avg_profit_per_trade: Decimal,
    pub consecutive_losses: u32,
    pub risk_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioState {
    pub total_value: Decimal,
    pub active_positions: HashMap<Pubkey, PositionMetrics>,
    pub heat_profiles: HashMap<Pubkey, TokenHeatProfile>,
    pub global_heat: f64,
    pub total_realized_pnl: Decimal,
    pub total_gas_spent: Decimal,
    pub last_rebalance: u64,
    pub circuit_breaker_active: bool,
    pub max_drawdown: Decimal,
    pub current_drawdown: Decimal,
}

#[derive(Debug)]
pub struct HeatEvent {
    pub timestamp: u64,
    pub heat_value: f64,
    pub event_type: HeatEventType,
    pub token: Option<Pubkey>,
}

#[derive(Debug, Clone, Copy)]
pub enum HeatEventType {
    TradeExecuted,
    LargeSlippage,
    FailedTransaction,
    CompetitorDetected,
    VolumeSpike,
    ConsecutiveLoss,
    NetworkCongestion,
}

impl HeatEventType {
    fn heat_value(&self) -> f64 {
        match self {
            HeatEventType::TradeExecuted => 5.0,
            HeatEventType::LargeSlippage => 15.0,
            HeatEventType::FailedTransaction => 8.0,
            HeatEventType::CompetitorDetected => 20.0,
            HeatEventType::VolumeSpike => 12.0,
            HeatEventType::ConsecutiveLoss => 25.0,
            HeatEventType::NetworkCongestion => 10.0,
        }
    }
}

pub struct PortfolioHeatManager {
    state: Arc<RwLock<PortfolioState>>,
    heat_events: Arc<RwLock<VecDeque<HeatEvent>>>,
    risk_params: RiskParameters,
    start_time: Instant,
}

#[derive(Debug, Clone)]
pub struct RiskParameters {
    pub max_position_size: Decimal,
    pub max_portfolio_heat: f64,
    pub min_profit_threshold: Decimal,
    pub max_gas_per_trade: Decimal,
    pub position_timeout: Duration,
    pub heat_decay_interval: Duration,
    pub anti_pattern_threshold: f64,
}

impl Default for RiskParameters {
    fn default() -> Self {
        Self {
            max_position_size: dec!(10000),
            max_portfolio_heat: 90.0,
            min_profit_threshold: dec!(0.002),
            max_gas_per_trade: dec!(0.05),
            position_timeout: Duration::from_secs(POSITION_TIMEOUT_SECONDS),
            heat_decay_interval: Duration::from_secs(5),
            anti_pattern_threshold: 0.85,
        }
    }
}

impl PortfolioHeatManager {
    pub fn new(initial_capital: Decimal) -> Self {
        let now_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let state = PortfolioState {
            total_value: initial_capital,
            active_positions: HashMap::new(),
            heat_profiles: HashMap::new(),
            global_heat: 0.0,
            total_realized_pnl: Decimal::ZERO,
            total_gas_spent: Decimal::ZERO,
            last_rebalance: now_timestamp,
            circuit_breaker_active: false,
            max_drawdown: Decimal::ZERO,
            current_drawdown: Decimal::ZERO,
        };

        Self {
            state: Arc::new(RwLock::new(state)),
            heat_events: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            risk_params: RiskParameters::default(),
            start_time: Instant::now(),
        }
    }

    fn get_timestamp(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn can_open_position(
        &self,
        token_mint: &Pubkey,
        proposed_size: Decimal,
        current_price: Decimal,
    ) -> Result<bool, String> {
        let state = self.state.read().map_err(|e| format!("Lock error: {}", e))?;
        
        if state.circuit_breaker_active {
            return Ok(false);
        }

        if state.active_positions.len() >= MAX_CONCURRENT_POSITIONS {
            return Ok(false);
        }

        let position_value = proposed_size * current_price;
        if position_value > state.total_value * MAX_POSITION_RATIO {
            return Ok(false);
        }

        let current_time = self.get_timestamp();

        if let Some(heat_profile) = state.heat_profiles.get(token_mint) {
            if heat_profile.current_heat > HEAT_THRESHOLD_HIGH {
                return Ok(false);
            }

            if let Some(cooldown) = heat_profile.cooldown_until {
                if current_time < cooldown {
                    return Ok(false);
                }
            }

            if heat_profile.consecutive_losses > 3 {
                return Ok(false);
            }

            if heat_profile.win_rate < WIN_RATE_THRESHOLD && heat_profile.trade_count > 10 {
                return Ok(false);
            }
        }

        if state.global_heat > self.risk_params.max_portfolio_heat {
            return Ok(false);
        }

        Ok(true)
    }

    pub fn open_position(
        &self,
        token_mint: Pubkey,
        size: Decimal,
        entry_price: Decimal,
        gas_estimate: Decimal,
    ) -> Result<(), String> {
        let mut state = self.state.write().map_err(|e| format!("Lock error: {}", e))?;
        let current_time = self.get_timestamp();
        
        let position = PositionMetrics {
            entry_price,
            current_price: entry_price,
            size,
            entry_time: current_time,
            last_update: current_time,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            gas_spent: gas_estimate,
            slippage_cost: Decimal::ZERO,
            heat_contribution: 5.0,
        };

        state.active_positions.insert(token_mint, position);
        state.total_gas_spent += gas_estimate;

        let heat_profile = state.heat_profiles.entry(token_mint).or_insert_with(|| {
            TokenHeatProfile {
                token_mint,
                current_heat: 0.0,
                trade_count: 0,
                volume_24h: Decimal::ZERO,
                avg_volume_baseline: Decimal::ZERO,
                last_trade_time: current_time,
                cooldown_until: None,
                win_rate: 0.5,
                avg_profit_per_trade: Decimal::ZERO,
                consecutive_losses: 0,
                risk_score: 0.0,
            }
        });

        heat_profile.trade_count += 1;
        heat_profile.volume_24h += size * entry_price;
        heat_profile.last_trade_time = current_time;
        heat_profile.current_heat += HeatEventType::TradeExecuted.heat_value();

        drop(state);
        self.record_heat_event(HeatEventType::TradeExecuted, Some(token_mint));
        
        Ok(())
    }

    pub fn update_position(
        &self,
        token_mint: &Pubkey,
        current_price: Decimal,
        slippage: Option<Decimal>,
    ) -> Result<(), String> {
        let mut state = self.state.write().map_err(|e| format!("Lock error: {}", e))?;
        let current_time = self.get_timestamp();
        
        if let Some(position) = state.active_positions.get_mut(token_mint) {
            position.current_price = current_price;
            position.last_update = current_time;
            
            let price_diff = current_price - position.entry_price;
            position.unrealized_pnl = price_diff * position.size - position.gas_spent;
            
            if let Some(slip) = slippage {
                position.slippage_cost += slip;
                if slip > position.entry_price * dec!(0.02) {
                    drop(state);
                    self.record_heat_event(HeatEventType::LargeSlippage, Some(*token_mint));
                }
            }
            
            let position_age = current_time.saturating_sub(position.entry_time);
            if position_age > POSITION_TIMEOUT_SECONDS {
                position.heat_contribution *= 1.5;
            }
        }
        
        Ok(())
    }

    pub fn close_position(
        &self,
        token_mint: &Pubkey,
        exit_price: Decimal,
        gas_cost: Decimal,
    ) -> Result<Decimal, String> {
        let mut state = self.state.write().map_err(|e| format!("Lock error: {}", e))?;
        
        if let Some(position) = state.active_positions.remove(token_mint) {
            let gross_pnl = (exit_price - position.entry_price) * position.size;
            let total_costs = position.gas_spent + gas_cost + position.slippage_cost;
            let net_pnl = gross_pnl - total_costs;
            
            state.total_realized_pnl += net_pnl;
            state.total_gas_spent += gas_cost;
            
            if let Some(heat_profile) = state.heat_profiles.get_mut(token_mint) {
                if net_pnl > Decimal::ZERO {
                    heat_profile.consecutive_losses = 0;
                    heat_profile.win_rate = (heat_profile.win_rate * 
                        (heat_profile.trade_count - 1) as f64 + 1.0) / 
                        heat_profile.trade_count as f64;
                } else {
                    heat_profile.consecutive_losses += 1;
                    heat_profile.win_rate = (heat_profile.win_rate * 
                        (heat_profile.trade_count - 1) as f64) / 
                        heat_profile.trade_count as f64;
                    
                    if heat_profile.consecutive_losses > 2 {
                        let current_time = self.get_timestamp();
                        heat_profile.cooldown_until = Some(
                            current_time + (60 * heat_profile.consecutive_losses as u64).min(300)
                        );
                    }
                }
                
                heat_profile.avg_profit_per_trade = 
                    (heat_profile.avg_profit_per_trade * 
                    Decimal::from(heat_profile.trade_count - 1) + net_pnl) / 
                    Decimal::from(heat_profile.trade_count);
                
                self.update_risk_score(heat_profile);
            }
            
            self.update_drawdown(&mut state);
            
            Ok(net_pnl)
        } else {
            Err("Position not found".to_string())
        }
    }

    pub fn record_heat_event(&self, event_type: HeatEventType, token: Option<Pubkey>) {
        let heat_event = HeatEvent {
            timestamp: self.get_timestamp(),
            heat_value: event_type.heat_value(),
            event_type,
            token,
        };

        if let Ok(mut events) = self.heat_events.write() {
            events.push_back(heat_event);
            if events.len() > 1000 {
                events.pop_front();
            }
        }

        if let Ok(mut state) = self.state.write() {
            state.global_heat += event_type.heat_value();
            
            if let Some(token_mint) = token {
                if let Some(profile) = state.heat_profiles.get_mut(&token_mint) {
                    profile.current_heat += event_type.heat_value();
                }
            }
            
                        if state.global_heat > HEAT_THRESHOLD_CRITICAL {
                state.circuit_breaker_active = true;
            }
        }
    }

    pub fn decay_heat(&self) -> Result<(), String> {
        let mut state = self.state.write().map_err(|e| format!("Lock error: {}", e))?;
        let mut events = self.heat_events.write().map_err(|e| format!("Lock error: {}", e))?;
        
        let current_time = self.get_timestamp();
        let cutoff = current_time.saturating_sub(HEAT_WINDOW_SECONDS);
        
        events.retain(|event| event.timestamp > cutoff);
        
        state.global_heat *= HEAT_DECAY_RATE;
        if state.global_heat < HEAT_THRESHOLD_HIGH && state.circuit_breaker_active {
            state.circuit_breaker_active = false;
        }
        
        for (_, profile) in state.heat_profiles.iter_mut() {
            profile.current_heat *= HEAT_DECAY_RATE;
            
            if profile.current_heat < 10.0 && profile.cooldown_until.is_some() {
                if let Some(cooldown) = profile.cooldown_until {
                    if current_time > cooldown {
                        profile.cooldown_until = None;
                    }
                }
            }
            
            let time_since_trade = current_time.saturating_sub(profile.last_trade_time);
            if time_since_trade > 3600 {
                profile.volume_24h *= dec!(0.95);
            }
        }
        
        Ok(())
    }

    pub fn check_volume_spike(&self, token_mint: &Pubkey, current_volume: Decimal) -> bool {
        if let Ok(state) = self.state.read() {
            if let Some(profile) = state.heat_profiles.get(token_mint) {
                if profile.avg_volume_baseline > Decimal::ZERO {
                    let spike_ratio = current_volume / profile.avg_volume_baseline;
                    if spike_ratio > Decimal::from_f64_retain(VOLUME_SPIKE_MULTIPLIER).unwrap_or(dec!(3)) {
                        drop(state);
                        self.record_heat_event(HeatEventType::VolumeSpike, Some(*token_mint));
                        return true;
                    }
                }
            }
        }
        false
    }

    pub fn update_risk_score(&self, profile: &mut TokenHeatProfile) {
        let heat_factor = profile.current_heat / 100.0;
        let loss_factor = (profile.consecutive_losses as f64 * 0.15).min(0.6);
        let win_rate_factor = (WIN_RATE_THRESHOLD - profile.win_rate).max(0.0) * 2.0;
        let volume_stability = if profile.avg_volume_baseline > Decimal::ZERO {
            let variance = (profile.volume_24h - profile.avg_volume_baseline).abs() / profile.avg_volume_baseline;
            variance.to_f64().unwrap_or(0.0).min(1.0)
        } else {
            0.5
        };
        
        profile.risk_score = (heat_factor + loss_factor + win_rate_factor + volume_stability) / 4.0;
        profile.risk_score = profile.risk_score.clamp(0.0, 1.0);
    }

    pub fn update_drawdown(&self, state: &mut PortfolioState) {
        let current_total = state.total_value + state.total_realized_pnl;
        let peak_value = state.total_value + state.max_drawdown.abs();
        
        if current_total > peak_value {
            state.max_drawdown = Decimal::ZERO;
        } else {
            let drawdown = (peak_value - current_total) / peak_value;
            state.current_drawdown = drawdown;
            if drawdown > state.max_drawdown {
                state.max_drawdown = drawdown;
            }
        }
        
        if state.current_drawdown > MAX_LOSS_THRESHOLD {
            state.circuit_breaker_active = true;
        }
    }

    pub fn rebalance_portfolio(&self) -> Result<Vec<RebalanceAction>, String> {
        let mut state = self.state.write().map_err(|e| format!("Lock error: {}", e))?;
        let current_time = self.get_timestamp();
        
        if current_time.saturating_sub(state.last_rebalance) < REBALANCE_INTERVAL_SECONDS {
            return Ok(Vec::new());
        }
        
        state.last_rebalance = current_time;
        let mut actions = Vec::new();
        
        let total_position_value: Decimal = state.active_positions.values()
            .map(|p| p.size * p.current_price)
            .sum();
        
        for (token, position) in state.active_positions.iter() {
            let position_value = position.size * position.current_price;
            let position_ratio = position_value / state.total_value;
            
            if position_ratio > MAX_POSITION_RATIO {
                let target_value = state.total_value * MAX_POSITION_RATIO;
                let reduction_amount = (position_value - target_value) / position.current_price;
                actions.push(RebalanceAction::Reduce {
                    token: *token,
                    amount: reduction_amount,
                    reason: RebalanceReason::OverExposure,
                });
            }
            
            if position.unrealized_pnl < -(position_value * MAX_LOSS_THRESHOLD) {
                actions.push(RebalanceAction::Close {
                    token: *token,
                    reason: RebalanceReason::StopLoss,
                });
            }
            
            let age = current_time.saturating_sub(position.entry_time);
            if age > POSITION_TIMEOUT_SECONDS * 2 {
                actions.push(RebalanceAction::Close {
                    token: *token,
                    reason: RebalanceReason::Timeout,
                });
            }
        }
        
        let high_risk_tokens: Vec<Pubkey> = state.heat_profiles.iter()
            .filter(|(_, p)| p.risk_score > self.risk_params.anti_pattern_threshold)
            .map(|(k, _)| *k)
            .collect();
        
        for token in high_risk_tokens {
            if state.active_positions.contains_key(&token) {
                actions.push(RebalanceAction::Close {
                    token,
                    reason: RebalanceReason::HighRisk,
                });
            }
        }
        
        Ok(actions)
    }

    pub fn get_position_sizing(
        &self,
        token_mint: &Pubkey,
        opportunity_score: f64,
        market_volatility: f64,
    ) -> Result<Decimal, String> {
        let state = self.state.read().map_err(|e| format!("Lock error: {}", e))?;
        
        let base_size = state.total_value * dec!(0.05);
        let heat_multiplier = if let Some(profile) = state.heat_profiles.get(token_mint) {
            let heat_factor = 1.0 - (profile.current_heat / 100.0);
            let risk_factor = 1.0 - profile.risk_score;
            let win_rate_bonus = if profile.win_rate > 0.6 && profile.trade_count > 20 {
                1.2
            } else {
                1.0
            };
            heat_factor * risk_factor * win_rate_bonus
        } else {
            0.8
        };
        
        let volatility_adjustment = 1.0 / (1.0 + market_volatility * 2.0);
        let opportunity_multiplier = (opportunity_score * 1.5).min(2.0);
        let drawdown_penalty = 1.0 - state.current_drawdown.to_f64().unwrap_or(0.0);
        
        let final_multiplier = heat_multiplier * volatility_adjustment * 
            opportunity_multiplier * drawdown_penalty;
        
        let position_size = base_size * Decimal::from_f64_retain(final_multiplier)
            .unwrap_or(Decimal::ONE);
        
        Ok(position_size.min(self.risk_params.max_position_size))
    }

    pub fn should_exit_position(
        &self,
        token_mint: &Pubkey,
        current_market_conditions: &MarketConditions,
    ) -> Result<bool, String> {
        let state = self.state.read().map_err(|e| format!("Lock error: {}", e))?;
        
        if let Some(position) = state.active_positions.get(token_mint) {
            let pnl_ratio = position.unrealized_pnl / (position.size * position.entry_price);
            
            if pnl_ratio < -MAX_LOSS_THRESHOLD {
                return Ok(true);
            }
            
            let current_time = self.get_timestamp();
            let time_in_position = current_time.saturating_sub(position.entry_time);
            if time_in_position > POSITION_TIMEOUT_SECONDS {
                if pnl_ratio < self.risk_params.min_profit_threshold {
                    return Ok(true);
                }
            }
            
            if let Some(profile) = state.heat_profiles.get(token_mint) {
                if profile.current_heat > HEAT_THRESHOLD_CRITICAL {
                    return Ok(true);
                }
                
                if profile.risk_score > 0.9 {
                    return Ok(true);
                }
            }
            
            if current_market_conditions.network_congestion > 0.8 {
                if position.unrealized_pnl > Decimal::ZERO {
                    return Ok(true);
                }
            }
            
            if state.circuit_breaker_active {
                return Ok(true);
            }
        }
        
        Ok(false)
    }

    pub fn get_portfolio_metrics(&self) -> Result<PortfolioMetrics, String> {
        let state = self.state.read().map_err(|e| format!("Lock error: {}", e))?;
        
        let total_unrealized: Decimal = state.active_positions.values()
            .map(|p| p.unrealized_pnl)
            .sum();
        
        let position_count = state.active_positions.len();
        let avg_position_heat = if position_count > 0 {
            state.active_positions.values()
                .map(|p| p.heat_contribution)
                .sum::<f64>() / position_count as f64
        } else {
            0.0
        };
        
        let high_risk_positions = state.active_positions.keys()
            .filter(|token| {
                state.heat_profiles.get(token)
                    .map(|p| p.risk_score > 0.7)
                    .unwrap_or(false)
            })
            .count();
        
        Ok(PortfolioMetrics {
            total_value: state.total_value + state.total_realized_pnl + total_unrealized,
            realized_pnl: state.total_realized_pnl,
            unrealized_pnl: total_unrealized,
            active_positions: position_count,
            global_heat: state.global_heat,
            avg_position_heat,
            circuit_breaker_active: state.circuit_breaker_active,
            current_drawdown: state.current_drawdown,
            max_drawdown: state.max_drawdown,
            total_gas_spent: state.total_gas_spent,
            high_risk_positions,
        })
    }

    pub fn apply_anti_detection_delay(&self) -> Duration {
        let base_delay = Duration::from_millis(50);
        let mut rng = rand::thread_rng();
        let jitter = rng.gen_range(0..100);
        
        if let Ok(state) = self.state.read() {
            let heat_factor = (state.global_heat / 100.0 * 5.0) as u64;
            let activity_factor = (state.active_positions.len() as u64).saturating_sub(3) * 20;
            
            Duration::from_millis(base_delay.as_millis() as u64 + jitter + heat_factor + activity_factor)
        } else {
            base_delay + Duration::from_millis(jitter)
        }
    }

    pub fn calculate_optimal_gas_price(&self, base_gas_price: Decimal, priority: TradePriority) -> Decimal {
        let heat_multiplier = if let Ok(state) = self.state.read() {
            1.0 + (state.global_heat / 200.0)
        } else {
            1.0
        };
        
        let priority_multiplier = match priority {
            TradePriority::Low => 1.0,
            TradePriority::Normal => 1.15,
            TradePriority::High => 1.35,
            TradePriority::Critical => 1.6,
        };
        
        let gas_price = base_gas_price * Decimal::from_f64_retain(heat_multiplier * priority_multiplier)
            .unwrap_or(dec!(1.2));
        
        gas_price.min(self.risk_params.max_gas_per_trade)
    }

    pub fn generate_trade_id(&self) -> u64 {
        let mut rng = rand::thread_rng();
        let base_id = self.get_timestamp();
        let random_component: u64 = rng.gen_range(1000..9999);
        base_id.wrapping_mul(10000).wrapping_add(random_component)
    }
}

#[derive(Debug, Clone)]
pub enum RebalanceAction {
    Close { token: Pubkey, reason: RebalanceReason },
    Reduce { token: Pubkey, amount: Decimal, reason: RebalanceReason },
}

#[derive(Debug, Clone)]
pub enum RebalanceReason {
    OverExposure,
    StopLoss,
    Timeout,
    HighRisk,
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub network_congestion: f64,
    pub competitor_activity: f64,
    pub volatility_index: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PortfolioMetrics {
    pub total_value: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
        pub active_positions: usize,
    pub global_heat: f64,
    pub avg_position_heat: f64,
    pub circuit_breaker_active: bool,
    pub current_drawdown: Decimal,
    pub max_drawdown: Decimal,
    pub total_gas_spent: Decimal,
    pub high_risk_positions: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum TradePriority {
    Low,
    Normal,
    High,
    Critical,
}

impl PortfolioHeatManager {
    pub fn emergency_shutdown(&self) -> Result<Vec<Pubkey>, String> {
        let mut state = self.state.write().map_err(|e| format!("Lock error: {}", e))?;
        
        state.circuit_breaker_active = true;
        let positions_to_close: Vec<Pubkey> = state.active_positions.keys().cloned().collect();
        
        let current_time = self.get_timestamp();
        for (_, profile) in state.heat_profiles.iter_mut() {
            profile.cooldown_until = Some(current_time + 1800);
            profile.current_heat = 100.0;
        }
        
        state.global_heat = 100.0;
        
        Ok(positions_to_close)
    }

    pub fn reset_circuit_breaker(&self) -> Result<(), String> {
        let mut state = self.state.write().map_err(|e| format!("Lock error: {}", e))?;
        
        if state.active_positions.is_empty() && state.global_heat < 50.0 {
            state.circuit_breaker_active = false;
            Ok(())
        } else {
            Err("Cannot reset: positions still active or heat too high".to_string())
        }
    }

    pub fn get_trade_authorization(&self, token_mint: &Pubkey, trade_size: Decimal) -> Result<bool, String> {
        let state = self.state.read().map_err(|e| format!("Lock error: {}", e))?;
        
        if state.circuit_breaker_active {
            return Ok(false);
        }
        
        if let Some(profile) = state.heat_profiles.get(token_mint) {
            if profile.risk_score > 0.85 {
                return Ok(false);
            }
            
            if profile.current_heat > HEAT_THRESHOLD_HIGH {
                return Ok(false);
            }
            
            let current_time = self.get_timestamp();
            if let Some(cooldown) = profile.cooldown_until {
                if current_time < cooldown {
                    return Ok(false);
                }
            }
        }
        
        let position_value = trade_size * dec!(1); // Assuming price will be checked separately
        if position_value > self.risk_params.max_position_size {
            return Ok(false);
        }
        
        Ok(true)
    }

    pub fn periodic_maintenance(&self) -> Result<MaintenanceReport, String> {
        self.decay_heat()?;
        
        let mut positions_closed = 0;
        let mut positions_reduced = 0;
        let mut tokens_blacklisted = 0;
        
        let rebalance_actions = self.rebalance_portfolio()?;
        for action in &rebalance_actions {
            match action {
                RebalanceAction::Close { .. } => positions_closed += 1,
                RebalanceAction::Reduce { .. } => positions_reduced += 1,
            }
        }
        
        if let Ok(mut state) = self.state.write() {
            let current_time = self.get_timestamp();
            let tokens_to_blacklist: Vec<Pubkey> = state.heat_profiles.iter()
                .filter(|(_, p)| {
                    p.consecutive_losses > 5 || 
                    (p.win_rate < 0.3 && p.trade_count > 15) ||
                    p.risk_score > 0.95
                })
                .map(|(k, _)| *k)
                .collect();
            
            for token in &tokens_to_blacklist {
                if let Some(profile) = state.heat_profiles.get_mut(token) {
                    profile.cooldown_until = Some(current_time + 7200);
                    tokens_blacklisted += 1;
                }
            }
            
            let expired_positions: Vec<Pubkey> = state.active_positions.iter()
                .filter(|(_, p)| {
                    let age = current_time.saturating_sub(p.entry_time);
                    age > POSITION_TIMEOUT_SECONDS * 3
                })
                .map(|(k, _)| *k)
                .collect();
            
            for token in expired_positions {
                state.active_positions.remove(&token);
                positions_closed += 1;
            }
        }
        
        Ok(MaintenanceReport {
            positions_closed,
            positions_reduced,
            tokens_blacklisted,
            heat_decay_applied: true,
            timestamp: self.get_timestamp(),
        })
    }

    pub fn export_state_snapshot(&self) -> Result<PortfolioSnapshot, String> {
        let state = self.state.read().map_err(|e| format!("Lock error: {}", e))?;
        let events = self.heat_events.read().map_err(|e| format!("Lock error: {}", e))?;
        
        Ok(PortfolioSnapshot {
            timestamp: self.get_timestamp(),
            total_value: state.total_value,
            active_positions: state.active_positions.len(),
            global_heat: state.global_heat,
            circuit_breaker_active: state.circuit_breaker_active,
            total_realized_pnl: state.total_realized_pnl,
            current_drawdown: state.current_drawdown,
            recent_events_count: events.len(),
        })
    }

    pub fn validate_state_integrity(&self) -> Result<bool, String> {
        let state = self.state.read().map_err(|e| format!("Lock error: {}", e))?;
        
        let total_position_value: Decimal = state.active_positions.values()
            .map(|p| p.size * p.current_price)
            .sum();
        
        if total_position_value > state.total_value {
            return Ok(false);
        }
        
        for (token, position) in &state.active_positions {
            if position.size <= Decimal::ZERO || position.entry_price <= Decimal::ZERO {
                return Ok(false);
            }
            
            if position.gas_spent < Decimal::ZERO {
                return Ok(false);
            }
            
            if !state.heat_profiles.contains_key(token) {
                return Ok(false);
            }
        }
        
        if state.global_heat < 0.0 || state.global_heat > 100.0 {
            return Ok(false);
        }
        
        Ok(true)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PortfolioSnapshot {
    pub timestamp: u64,
    pub total_value: Decimal,
    pub active_positions: usize,
    pub global_heat: f64,
    pub circuit_breaker_active: bool,
    pub total_realized_pnl: Decimal,
    pub current_drawdown: Decimal,
    pub recent_events_count: usize,
}

#[derive(Debug, Clone)]
pub struct MaintenanceReport {
    pub positions_closed: usize,
    pub positions_reduced: usize,
    pub tokens_blacklisted: usize,
    pub heat_decay_applied: bool,
    pub timestamp: u64,
}

impl Default for PortfolioHeatManager {
    fn default() -> Self {
        Self::new(dec!(100000))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_portfolio_initialization() {
        let manager = PortfolioHeatManager::new(dec!(100000));
        let metrics = manager.get_portfolio_metrics().expect("Failed to get metrics");
        assert_eq!(metrics.total_value, dec!(100000));
        assert_eq!(metrics.active_positions, 0);
        assert_eq!(metrics.global_heat, 0.0);
    }

    #[test]
    fn test_position_constraints() {
        let manager = PortfolioHeatManager::new(dec!(100000));
        let token = Pubkey::new_unique();
        let can_open = manager.can_open_position(&token, dec!(20000), dec!(1))
            .expect("Failed to check position");
        assert!(!can_open);
    }

    #[test]
    fn test_heat_decay() {
        let manager = PortfolioHeatManager::new(dec!(100000));
        manager.record_heat_event(HeatEventType::TradeExecuted, None);
        let initial_metrics = manager.get_portfolio_metrics().expect("Failed to get metrics");
        let initial_heat = initial_metrics.global_heat;
        
        manager.decay_heat().expect("Failed to decay heat");
        let after_metrics = manager.get_portfolio_metrics().expect("Failed to get metrics");
        assert!(after_metrics.global_heat < initial_heat);
    }
}

