use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MartingaleError {
    #[error("No active position")]
    NoActivePosition,
    #[error("Position already active")]
    PositionAlreadyActive,
    #[error("Circuit breaker triggered")]
    CircuitBreakerActive,
    #[error("Insufficient liquidity: {0}")]
    InsufficientLiquidity(Decimal),
    #[error("Cooldown period active")]
    CooldownActive,
    #[error("Invalid position size: {0}")]
    InvalidPositionSize(Decimal),
    #[error("Time error: {0}")]
    TimeError(#[from] std::time::SystemTimeError),
}

pub type Result<T> = std::result::Result<T, MartingaleError>;

#[derive(Debug, Clone)]
pub struct MartingaleConfig {
    pub base_position_size: Decimal,
    pub max_position_size: Decimal,
    pub multiplier: Decimal,
    pub max_consecutive_losses: u32,
    pub profit_target_percent: Decimal,
    pub stop_loss_percent: Decimal,
    pub cooldown_period_ms: u64,
    pub volatility_adjustment: bool,
    pub kelly_fraction: Decimal,
    pub max_drawdown_percent: Decimal,
    pub risk_per_trade: Decimal,
    pub slippage_tolerance: Decimal,
    pub min_liquidity_threshold: Decimal,
    pub max_gas_price_lamports: u64,
    pub position_timeout_ms: u64,
    pub circuit_breaker_threshold: Decimal,
    pub ema_period: usize,
    pub volatility_window: usize,
    pub confidence_threshold: Decimal,
    pub trailing_stop_activation: Decimal,
    pub max_trade_history: usize,
}

impl Default for MartingaleConfig {
    fn default() -> Self {
        Self {
            base_position_size: dec!(0.001),
            max_position_size: dec!(0.05),
            multiplier: dec!(2.0),
            max_consecutive_losses: 5,
            profit_target_percent: dec!(0.015),
            stop_loss_percent: dec!(0.005),
            cooldown_period_ms: 100,
            volatility_adjustment: true,
            kelly_fraction: dec!(0.25),
            max_drawdown_percent: dec!(0.10),
            risk_per_trade: dec!(0.02),
            slippage_tolerance: dec!(0.003),
            min_liquidity_threshold: dec!(50000),
            max_gas_price_lamports: 50000,
            position_timeout_ms: 30000,
            circuit_breaker_threshold: dec!(0.05),
            ema_period: 20,
            volatility_window: 50,
            confidence_threshold: dec!(0.65),
            trailing_stop_activation: dec!(0.01),
            max_trade_history: 1000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradeMetrics {
    pub timestamp: u64,
    pub position_size: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Option<Decimal>,
    pub pnl: Decimal,
    pub is_win: bool,
    pub slippage: Decimal,
    pub gas_cost: Decimal,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct StrategyState {
    pub consecutive_losses: u32,
    pub consecutive_wins: u32,
    pub current_position_size: Decimal,
    pub total_pnl: Decimal,
    pub peak_balance: Decimal,
    pub current_drawdown: Decimal,
    pub last_trade_timestamp: u64,
    pub trade_history: VecDeque<TradeMetrics>,
    pub price_history: VecDeque<(u64, Decimal)>,
    pub volatility_cache: Option<(u64, Decimal)>,
    pub ema_value: Decimal,
    pub confidence_score: Decimal,
    pub active_position: Option<ActivePosition>,
    pub circuit_breaker_triggered: bool,
    pub total_gas_spent: Decimal,
    pub highest_price_since_entry: Decimal,
}

#[derive(Debug, Clone)]
pub struct ActivePosition {
    pub entry_price: Decimal,
    pub size: Decimal,
    pub timestamp: u64,
    pub stop_loss: Decimal,
    pub take_profit: Decimal,
    pub trailing_stop: Option<Decimal>,
}

pub struct MartingaleOptimizer {
    config: Arc<RwLock<MartingaleConfig>>,
    state: Arc<RwLock<StrategyState>>,
}

impl MartingaleOptimizer {
    pub fn new(config: MartingaleConfig) -> Self {
        let state = StrategyState {
            consecutive_losses: 0,
            consecutive_wins: 0,
            current_position_size: config.base_position_size,
            total_pnl: Decimal::ZERO,
            peak_balance: dec!(1.0),
            current_drawdown: Decimal::ZERO,
            last_trade_timestamp: 0,
            trade_history: VecDeque::with_capacity(config.max_trade_history),
            price_history: VecDeque::with_capacity(config.volatility_window * 2),
            volatility_cache: None,
            ema_value: Decimal::ZERO,
            confidence_score: dec!(0.5),
            active_position: None,
            circuit_breaker_triggered: false,
            total_gas_spent: Decimal::ZERO,
            highest_price_since_entry: Decimal::ZERO,
        };

        Self {
            config: Arc::new(RwLock::new(config)),
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn should_enter_position(
        &self,
        current_price: Decimal,
        liquidity: Decimal,
        network_congestion: f64,
    ) -> Result<(bool, Decimal)> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        if state.circuit_breaker_triggered {
            return Err(MartingaleError::CircuitBreakerActive);
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

        if now - state.last_trade_timestamp < config.cooldown_period_ms {
            return Err(MartingaleError::CooldownActive);
        }

        if liquidity < config.min_liquidity_threshold {
            return Err(MartingaleError::InsufficientLiquidity(liquidity));
        }

        if state.active_position.is_some() {
            return Err(MartingaleError::PositionAlreadyActive);
        }

        self.update_price_history(&mut state, now, current_price);
        self.invalidate_volatility_cache(&mut state, now);
        
        let volatility = self.calculate_volatility(&state, &config);
        self.update_ema(&mut state, current_price, config.ema_period);

        let position_size = self.calculate_optimal_position_size(
            &config,
            &state,
            volatility,
            network_congestion,
        );

        if position_size <= Decimal::ZERO || position_size > config.max_position_size {
            return Err(MartingaleError::InvalidPositionSize(position_size));
        }

        let confidence = self.calculate_confidence_score(
            &state,
            volatility,
            current_price,
        );

        state.confidence_score = confidence;

        if confidence < config.confidence_threshold {
            return Ok((false, Decimal::ZERO));
        }

        let drawdown_check = state.current_drawdown < config.max_drawdown_percent;
        let risk_check = position_size * current_price <= config.risk_per_trade;

        Ok((drawdown_check && risk_check, position_size))
    }

    pub async fn enter_position(
        &self,
        entry_price: Decimal,
        position_size: Decimal,
        actual_slippage: Decimal,
        gas_cost: u64,
    ) -> Result<()> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        if state.active_position.is_some() {
            return Err(MartingaleError::PositionAlreadyActive);
        }

        let adjusted_entry = entry_price * (Decimal::ONE + actual_slippage);
        let stop_loss = adjusted_entry * (Decimal::ONE - config.stop_loss_percent);
        let take_profit = adjusted_entry * (Decimal::ONE + config.profit_target_percent);

        state.active_position = Some(ActivePosition {
            entry_price: adjusted_entry,
            size: position_size,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            stop_loss,
            take_profit,
            trailing_stop: None,
        });

        state.current_position_size = position_size;
        state.total_gas_spent += Decimal::from(gas_cost) / dec!(1_000_000_000);
        state.highest_price_since_entry = adjusted_entry;

        Ok(())
    }

    pub async fn should_exit_position(
        &self,
        current_price: Decimal,
    ) -> Result<(bool, ExitReason)> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        let position = state.active_position.as_ref()
            .ok_or(MartingaleError::NoActivePosition)?;

        if current_price > state.highest_price_since_entry {
            state.highest_price_since_entry = current_price;
        }

        if current_price <= position.stop_loss {
            return Ok((true, ExitReason::StopLoss));
        }

        if current_price >= position.take_profit {
            return Ok((true, ExitReason::TakeProfit));
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        if now - position.timestamp > config.position_timeout_ms {
            return Ok((true, ExitReason::Timeout));
        }

        let profit_percent = (current_price - position.entry_price) / position.entry_price;
        if profit_percent > config.trailing_stop_activation {
            let trailing_stop = self.calculate_trailing_stop(
                &state.highest_price_since_entry,
                profit_percent,
            );
            
            if let Some(pos) = state.active_position.as_mut() {
                pos.trailing_stop = Some(trailing_stop);
            }
            
            if current_price <= trailing_stop {
                return Ok((true, ExitReason::TrailingStop));
            }
        }

        Ok((false, ExitReason::None))
    }

    pub async fn exit_position(
        &self,
        exit_price: Decimal,
        actual_slippage: Decimal,
        gas_cost: u64,
        execution_time_ms: u64,
    ) -> Result<Decimal> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        let position = state.active_position.take()
            .ok_or(MartingaleError::NoActivePosition)?;

        let adjusted_exit = exit_price * (Decimal::ONE - actual_slippage);
        let gross_pnl = (adjusted_exit - position.entry_price) * position.size;
        let gas_decimal = Decimal::from(gas_cost) / dec!(1_000_000_000);
        let net_pnl = gross_pnl - gas_decimal;

        let is_win = net_pnl > Decimal::ZERO;

        let trade = TradeMetrics {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            position_size: position.size,
            entry_price: position.entry_price,
            exit_price: Some(adjusted_exit),
            pnl: net_pnl,
            is_win,
            slippage: actual_slippage,
            gas_cost: gas_decimal,
            execution_time_ms,
        };

        self.record_trade(&mut state, trade, &config);
        self.update_position_sizing(&mut state, is_win, &config);
        self.update_drawdown(&mut state);
        self.check_circuit_breaker(&mut state, &config);

        state.highest_price_since_entry = Decimal::ZERO;

        Ok(net_pnl)
    }

    fn calculate_optimal_position_size(
        &self,
        config: &MartingaleConfig,
        state: &StrategyState,
        volatility: Decimal,
        network_congestion: f64,
    ) -> Decimal {
        let base_size = state.current_position_size;

        let stats = self.calculate_trade_statistics(state);
        
        if stats.avg_loss == Decimal::ZERO || stats.win_rate == Decimal::ZERO {
            return base_size * dec!(0.5);
        }

        let kelly_criterion = (stats.win_rate * stats.avg_win - (Decimal::ONE - stats.win_rate) * stats.avg_loss) 
            / stats.avg_win;
        let kelly_size = base_size * kelly_criterion.max(Decimal::ZERO) * config.kelly_fraction;

        let volatility_adjusted = if config.volatility_adjustment {
            kelly_size * (Decimal::ONE / (Decimal::ONE + volatility * dec!(10)))
        } else {
            kelly_size
        };

        let congestion_factor = Decimal::from_f64_retain(1.0 - network_congestion * 0.5)
            .unwrap_or(Decimal::ONE);
        
        let sharpe_adjustment = (stats.sharpe_ratio / dec!(2)).min(dec!(1.5)).max(dec!(0.5));
        
        (volatility_adjusted * congestion_factor * state.confidence_score * sharpe_adjustment)
            .min(config.max_position_size)
            .max(Decimal::ZERO)
    }

    fn calculate_volatility(&self, state: &StrategyState, config: &MartingaleConfig) -> Decimal {
        if let Some((cache_time, cached_vol)) = state.volatility_cache {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            if now - cache_time < 5000 {
                return cached_vol;
            }
        }

        if state.price_history.len() < 2 {
            return dec!(0.01);
        }

        let prices: Vec<Decimal> = state.price_history
            .iter()
            .rev()
            .take(config.volatility_window)
            .map(|(_, p)| *p)
            .collect();

        if prices.len() < 2 {
            return dec!(0.01);
        }

        let mut returns = Vec::with_capacity(prices.len() - 1);
        for i in 1..prices.len() {
            let ret = (prices[i - 1] - prices[i]) / prices[i];
            returns.push(ret);
        }

        let mean = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let variance = returns.iter()
            .map(|r| (*r - mean).powi(2))
            .sum::<Decimal>() / Decimal::from(returns.len());

        variance.sqrt()
    }

    fn update_ema(&self, state: &mut StrategyState, current_price: Decimal, period: usize) {
        if state.ema_value == Decimal::ZERO {
            state.ema_value = current_price;
        } else {
            let alpha = dec!(2) / (Decimal::from(period) + dec!(1));
            state.ema_value = current_price * alpha + state.ema_value * (dec!(1) - alpha);
        }
    }

    fn calculate_confidence_score(
        &self,
        state: &StrategyState,
        volatility: Decimal,
        current_price: Decimal,
    ) -> Decimal {
        let mut score = Decimal::ZERO;
        
        if state.ema_value != Decimal::ZERO {
            let trend_alignment = if current_price > state.ema_value {
                let strength = ((current_price - state.ema_value) / state.ema_value).min(dec!(0.05));
                dec!(0.3) + strength * dec!(2)
            } else {
                let weakness = ((state.ema_value - current_price) / state.ema_value).min(dec!(0.05));
                dec!(0.1) - weakness
            };
            score += trend_alignment.max(Decimal::ZERO);
        }

        let vol_score = ((dec!(0.02) - volatility) / dec!(0.02)).max(Decimal::ZERO) * dec!(0.25);
        score += vol_score;

        let stats = self.calculate_trade_statistics(state);
        score += stats.win_rate * dec!(0.2);

        if state.consecutive_wins > 0 {
            let win_momentum = (Decimal::from(state.consecutive_wins) / dec!(5)).min(dec!(1));
            score += win_momentum * dec!(0.15);
        }

        let drawdown_penalty = (state.current_drawdown * dec!(2)).min(dec!(0.3));
        score -= drawdown_penalty;

        if stats.profit_factor > dec!(1.5) {
            score += dec!(0.1);
        }

        score.max(Decimal::ZERO).min(Decimal::ONE)
    }

    fn calculate_trailing_stop(&self, highest_price: &Decimal, profit_percent: Decimal) -> Decimal {
        let trailing_percent = if profit_percent > dec!(0.05) {
            dec!(0.015)
        } else if profit_percent > dec!(0.03) {
            dec!(0.010)
        } else if profit_percent > dec!(0.02) {
            dec!(0.007)
        } else {
            dec!(0.005)
        };
        
        highest_price * (dec!(1) - trailing_percent)
    }

    fn update_price_history(&self, state: &mut StrategyState, timestamp: u64, price: Decimal) {
        state.price_history.push_back((timestamp, price));
        
        let cutoff_time = timestamp.saturating_sub(300000);
        while let Some((ts, _)) = state.price_history.front() {
            if *ts < cutoff_time {
                state.price_history.pop_front();
            } else {
                break;
            }
        }
    }

    fn invalidate_volatility_cache(&self, state: &mut StrategyState, timestamp: u64) {
        if let Some((cache_time, _)) = state.volatility_cache {
            if timestamp - cache_time > 5000 {
                state.volatility_cache = None;
            }
        }
    }

    fn calculate_trade_statistics(&self, state: &StrategyState) -> TradeStatistics {
        let recent_trades: Vec<&TradeMetrics> = state.trade_history
            .iter()
            .rev()
            .take(50)
            .collect();

        if recent_trades.is_empty() {
            return TradeStatistics::default();
        }

        let wins: Vec<&TradeMetrics> = recent_trades.iter()
            .filter(|t| t.is_win)
            .copied()
            .collect();

        let losses: Vec<&TradeMetrics> = recent_trades.iter()
            .filter(|t| !t.is_win)
            .copied()
            .collect();

        let win_rate = Decimal::from(wins.len()) / Decimal::from(recent_trades.len());
        
        let avg_win = if !wins.is_empty() {
            wins.iter().map(|t| t.pnl).sum::<Decimal>() / Decimal::from(wins.len())
        } else {
            dec!(0.01)
        };

        let avg_loss = if !losses.is_empty() {
            losses.iter().map(|t| t.pnl.abs()).sum::<Decimal>() / Decimal::from(losses.len())
        } else {
            dec!(0.01)
        };

        let gross_profit = wins.iter().map(|t| t.pnl).sum::<Decimal>();
        let gross_loss = losses.iter().map(|t| t.pnl.abs()).sum::<Decimal>();
        
        let profit_factor = if gross_loss > Decimal::ZERO {
            gross_profit / gross_loss
        } else if gross_profit > Decimal::ZERO {
            dec!(999)
        } else {
            dec!(1)
        };

        let returns: Vec<Decimal> = recent_trades.iter().map(|t| t.pnl).collect();
        let sharpe_ratio = self.calculate_sharpe_ratio(&returns);

        TradeStatistics {
            win_rate,
            avg_win,
            avg_loss,
            profit_factor,
            sharpe_ratio,
        }
    }

    fn calculate_sharpe_ratio(&self, returns: &[Decimal]) -> Decimal {
        if returns.len() < 2 {
            return Decimal::ZERO;
        }

        let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let variance = returns
            .iter()
            .map(|r| (*r - mean_return).powi(2))
            .sum::<Decimal>() / Decimal::from(returns.len());

        let std_dev = variance.sqrt();
        
        if std_dev > Decimal::ZERO {
            mean_return / std_dev * dec!(15.87)
        } else {
            Decimal::ZERO
        }
    }

    fn record_trade(&self, state: &mut StrategyState, trade: TradeMetrics, config: &MartingaleConfig) {
        state.total_pnl += trade.pnl;
        state.total_gas_spent += trade.gas_cost;
        state.last_trade_timestamp = trade.timestamp;
        
        state.trade_history.push_back(trade);
        if state.trade_history.len() > config.max_trade_history {
            state.trade_history.pop_front();
        }
    }

    fn update_position_sizing(&self, state: &mut StrategyState, is_win: bool, config: &MartingaleConfig) {
        if is_win {
            state.consecutive_wins += 1;
            state.consecutive_losses = 0;
            state.current_position_size = config.base_position_size;
        } else {
            state.consecutive_losses += 1;
            state.consecutive_wins = 0;
            
            if state.consecutive_losses < config.max_consecutive_losses {
                state.current_position_size = (state.current_position_size * config.multiplier)
                    .min(config.max_position_size);
            } else {
                state.current_position_size = config.base_position_size;
                state.consecutive_losses = 0;
            }
        }
    }

    fn update_drawdown(&self, state: &mut StrategyState) {
        let current_balance = dec!(1) + state.total_pnl;
        
        if current_balance > state.peak_balance {
            state.peak_balance = current_balance;
            state.current_drawdown = Decimal::ZERO;
        } else {
            state.current_drawdown = (state.peak_balance - current_balance) / state.peak_balance;
        }
    }

    fn check_circuit_breaker(&self, state: &mut StrategyState, config: &MartingaleConfig) {
        if state.current_drawdown >= config.circuit_breaker_threshold {
            state.circuit_breaker_triggered = true;
        }

        let recent_losses = state.trade_history
            .iter()
            .rev()
            .take(10)
            .filter(|t| !t.is_win)
            .count();

        if recent_losses >= 8 {
            state.circuit_breaker_triggered = true;
        }

        let recent_pnl: Decimal = state.trade_history
            .iter()
            .rev()
            .take(20)
            .map(|t| t.pnl)
            .sum();

        if recent_pnl < -config.circuit_breaker_threshold {
            state.circuit_breaker_triggered = true;
        }
    }

    pub async fn reset_circuit_breaker(&self) {
        let mut state = self.state.write().await;
        state.circuit_breaker_triggered = false;
        state.consecutive_losses = 0;
        state.current_position_size = self.config.read().await.base_position_size;
    }

    pub async fn get_performance_metrics(&self) -> PerformanceMetrics {
        let state = self.state.read().await;
        let stats = self.calculate_trade_statistics(&state);
        
        let total_trades = state.trade_history.len();
        let winning_trades = state.trade_history.iter().filter(|t| t.is_win).count();
        
        let (max_consecutive_wins, max_consecutive_losses) = self.calculate_max_streaks(&state);

        let avg_trade_duration = if !state.trade_history.is_empty() {
            state.trade_history.iter()
                .map(|t| t.execution_time_ms)
                .sum::<u64>() / state.trade_history.len() as u64
        } else {
            0
        };

        PerformanceMetrics {
            total_pnl: state.total_pnl,
            total_trades,
            winning_trades,
            win_rate: stats.win_rate,
            profit_factor: stats.profit_factor,
            sharpe_ratio: stats.sharpe_ratio,
            max_drawdown: state.peak_balance.checked_sub(dec!(1) + state.total_pnl)
                .unwrap_or(Decimal::ZERO) / state.peak_balance,
            current_drawdown: state.current_drawdown,
            avg_trade_duration_ms: avg_trade_duration,
            total_gas_spent: state.total_gas_spent,
            current_position_size: state.current_position_size,
            confidence_score: state.confidence_score,
            circuit_breaker_active: state.circuit_breaker_triggered,
            consecutive_losses: state.consecutive_losses,
            consecutive_wins: state.consecutive_wins,
            max_consecutive_wins,
            max_consecutive_losses,
            avg_win: stats.avg_win,
            avg_loss: stats.avg_loss,
        }
    }

    fn calculate_max_streaks(&self, state: &StrategyState) -> (u32, u32) {
        let mut max_wins = 0u32;
        let mut max_losses = 0u32;
        let mut current_wins = 0u32;
        let mut current_losses = 0u32;

        for trade in &state.trade_history {
            if trade.is_win {
                current_wins += 1;
                current_losses = 0;
                max_wins = max_wins.max(current_wins);
            } else {
                current_losses += 1;
                current_wins = 0;
                max_losses = max_losses.max(current_losses);
            }
        }

        (max_wins, max_losses)
    }

    pub async fn adjust_parameters_adaptive(&self) {
        let metrics = self.get_performance_metrics().await;
        let mut config = self.config.write().await;

        if metrics.total_trades < 20 {
            return;
        }

        if metrics.win_rate < dec!(0.4) {
            config.profit_target_percent = (config.profit_target_percent * dec!(0.95)).max(dec!(0.008));
            config.stop_loss_percent = (config.stop_loss_percent * dec!(1.05)).min(dec!(0.01));
        } else if metrics.win_rate > dec!(0.65) {
            config.profit_target_percent = (config.profit_target_percent * dec!(1.02)).min(dec!(0.025));
        }

        if metrics.sharpe_ratio < dec!(0.5) {
            config.kelly_fraction = (config.kelly_fraction * dec!(0.9)).max(dec!(0.1));
        } else if metrics.sharpe_ratio > dec!(2.0) {
            config.kelly_fraction = (config.kelly_fraction * dec!(1.1)).min(dec!(0.4));
        }

        if metrics.current_drawdown > dec!(0.08) {
            config.base_position_size = (config.base_position_size * dec!(0.8)).max(dec!(0.0005));
            config.max_position_size = (config.max_position_size * dec!(0.8)).max(dec!(0.01));
        }

        if metrics.consecutive_losses >= 4 {
            config.multiplier = (config.multiplier * dec!(0.95)).max(dec!(1.5));
        } else if metrics.consecutive_wins >= 5 && metrics.profit_factor > dec!(2) {
            config.multiplier = (config.multiplier * dec!(1.05)).min(dec!(2.5));
        }

        if metrics.avg_trade_duration_ms > 20000 {
            config.position_timeout_ms = (config.position_timeout_ms * 9 / 10).max(15000);
        }

        let volatility = {
            let state = self.state.read().await;
            self.calculate_volatility(&state, &config)
        };

        if volatility > dec!(0.03) {
            config.confidence_threshold = (config.confidence_threshold + dec!(0.05)).min(dec!(0.8));
        } else if volatility < dec!(0.01) {
            config.confidence_threshold = (config.confidence_threshold - dec!(0.05)).max(dec!(0.5));
        }
    }

    pub async fn validate_market_conditions(
        &self,
        bid_ask_spread: Decimal,
        volume_24h: Decimal,
        price_impact: Decimal,
    ) -> bool {
        let config = self.config.read().await;
        let state = self.state.read().await;

        if bid_ask_spread > config.slippage_tolerance * dec!(2) {
            return false;
        }

        if volume_24h < config.min_liquidity_threshold * dec!(10) {
            return false;
        }

        if price_impact > config.slippage_tolerance {
            return false;
        }

        let volatility = self.calculate_volatility(&state, &config);
        if volatility > dec!(0.05) {
            return false;
        }

        if state.circuit_breaker_triggered {
            return false;
        }

        true
    }

    pub async fn emergency_close_position(&self, current_price: Decimal) -> Result<bool> {
        let mut state = self.state.write().await;
        
        if let Some(position) = &state.active_position {
            let pnl_percent = (current_price - position.entry_price) / position.entry_price;
            
            if pnl_percent < dec!(-0.02) || state.circuit_breaker_triggered {
                state.active_position = None;
                state.current_position_size = self.config.read().await.base_position_size;
                state.consecutive_losses = 0;
                return Ok(true);
            }
        }
        
        Ok(false)
    }

    pub async fn get_position_recommendation(&self, market_data: &MarketData) -> PositionRecommendation {
        let config = self.config.read().await;
        let state = self.state.read().await;
        
        let volatility = self.calculate_volatility(&state, &config);
        let confidence = state.confidence_score;
        
        let risk_score = self.calculate_risk_score(&state, volatility, market_data);
        let opportunity_score = self.calculate_opportunity_score(&state, market_data, confidence);
        
        let recommended_size = if risk_score < dec!(0.3) && opportunity_score > dec!(0.7) {
            state.current_position_size * dec!(1.2)
        } else if risk_score > dec!(0.7) || opportunity_score < dec!(0.3) {
            state.current_position_size * dec!(0.5)
        } else {
            state.current_position_size
        };
        
        PositionRecommendation {
            action: if opportunity_score > confidence && risk_score < dec!(0.5) {
                RecommendedAction::Enter
            } else if state.active_position.is_some() && risk_score > dec!(0.8) {
                RecommendedAction::Exit
            } else {
                RecommendedAction::Hold
            },
            size: recommended_size.min(config.max_position_size),
            confidence: opportunity_score * (dec!(1) - risk_score),
            risk_level: risk_score,
        }
    }

    fn calculate_risk_score(
        &self,
        state: &StrategyState,
        volatility: Decimal,
        market_data: &MarketData,
    ) -> Decimal {
        let mut risk = Decimal::ZERO;
        
        risk += volatility * dec!(10);
        risk += state.current_drawdown * dec!(5);
        risk += Decimal::from(state.consecutive_losses) * dec!(0.1);
        
        if market_data.volume_24h < market_data.avg_volume_7d * dec!(0.5) {
            risk += dec!(0.2);
        }
        
        if market_data.bid_ask_spread > dec!(0.002) {
            risk += dec!(0.15);
        }
        
        risk.min(Decimal::ONE)
    }

    fn calculate_opportunity_score(
        &self,
        state: &StrategyState,
        market_data: &MarketData,
        confidence: Decimal,
    ) -> Decimal {
        let mut opportunity = confidence;
        
        if market_data.price < state.ema_value * dec!(0.98) {
            opportunity += dec!(0.2);
        }
        
        if market_data.rsi < dec!(30) {
            opportunity += dec!(0.15);
        } else if market_data.rsi > dec!(70) {
            opportunity -= dec!(0.15);
        }
        
        let volume_ratio = market_data.volume_24h / market_data.avg_volume_7d;
        if volume_ratio > dec!(1.5) {
            opportunity += dec!(0.1);
        }
        
        opportunity.max(Decimal::ZERO).min(Decimal::ONE)
    }
}

#[derive(Debug, Clone)]
pub struct TradeStatistics {
    pub win_rate: Decimal,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub profit_factor: Decimal,
    pub sharpe_ratio: Decimal,
}

impl Default for TradeStatistics {
    fn default() -> Self {
        Self {
            win_rate: dec!(0.5),
            avg_win: dec!(0.01),
            avg_loss: dec!(0.01),
            profit_factor: dec!(1),
            sharpe_ratio: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_pnl: Decimal,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub sharpe_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub current_drawdown: Decimal,
    pub avg_trade_duration_ms: u64,
    pub total_gas_spent: Decimal,
    pub current_position_size: Decimal,
    pub confidence_score: Decimal,
    pub circuit_breaker_active: bool,
    pub consecutive_losses: u32,
    pub consecutive_wins: u32,
    pub max_consecutive_wins: u32,
    pub max_consecutive_losses: u32,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExitReason {
    None,
    StopLoss,
    TakeProfit,
    TrailingStop,
    Timeout,
}

#[derive(Debug, Clone)]
pub struct MarketData {
    pub price: Decimal,
    pub volume_24h: Decimal,
    pub avg_volume_7d: Decimal,
    pub bid_ask_spread: Decimal,
    pub rsi: Decimal,
}

#[derive(Debug, Clone)]
pub struct PositionRecommendation {
    pub action: RecommendedAction,
    pub size: Decimal,
    pub confidence: Decimal,
    pub risk_level: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RecommendedAction {
    Enter,
    Exit,
    Hold,
}

impl MartingaleOptimizer {
    pub async fn health_check(&self) -> HealthStatus {
        let state = self.state.read().await;
        let config = self.config.read().await;
        
        let is_healthy = !state.circuit_breaker_triggered 
            && state.current_drawdown < config.max_drawdown_percent
            && state.total_pnl > -config.circuit_breaker_threshold;
        
        HealthStatus {
            is_healthy,
            circuit_breaker_active: state.circuit_breaker_triggered,
            current_drawdown: state.current_drawdown,
            total_pnl: state.total_pnl,
            last_trade_timestamp: state.last_trade_timestamp,
            active_position: state.active_position.is_some(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub circuit_breaker_active: bool,
    pub current_drawdown: Decimal,
    pub total_pnl: Decimal,
    pub last_trade_timestamp: u64,
    pub active_position: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_martingale_optimizer_initialization() {
        let config = MartingaleConfig::default();
        let optimizer = MartingaleOptimizer::new(config);
        
        let metrics = optimizer.get_performance_metrics().await;
        assert_eq!(metrics.total_pnl, Decimal::ZERO);
        assert_eq!(metrics.total_trades, 0);
        assert!(!metrics.circuit_breaker_active);
    }

    #[tokio::test]
    async fn test_position_entry_validation() {
        let config = MartingaleConfig::default();
        let optimizer = MartingaleOptimizer::new(config);
        
        let result = optimizer.should_enter_position(
            dec!(100),
            dec!(10000),
            0.1,
        ).await;
        
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let mut config = MartingaleConfig::default();
        config.circuit_breaker_threshold = dec!(0.05);
        let optimizer = MartingaleOptimizer::new(config);
        
        let health = optimizer.health_check().await;
        assert!(health.is_healthy);
        assert!(!health.circuit_breaker_active);
    }
}
