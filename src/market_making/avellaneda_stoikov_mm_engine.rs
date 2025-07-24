use anchor_lang::prelude::*;
use pyth_sdk_solana::Price;
use solana_program::clock::Clock;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

const MAX_PRICE_HISTORY: usize = 1000;
const VOLATILITY_WINDOW: usize = 100;
const GAMMA_DEFAULT: f64 = 0.1;
const KAPPA_DEFAULT: f64 = 1.5;
const ETA_DEFAULT: f64 = 0.01;
const MIN_SPREAD_BPS: f64 = 5.0;
const MAX_SPREAD_BPS: f64 = 200.0;
const INVENTORY_TARGET: f64 = 0.0;
const MAX_INVENTORY_RATIO: f64 = 0.8;
const TICK_SIZE: f64 = 0.00001;
const LOT_SIZE: f64 = 0.01;
const VOLATILITY_DECAY: f64 = 0.94;
const PRICE_IMPACT_COEFFICIENT: f64 = 0.0001;
const MIN_ORDER_SIZE: f64 = 0.1;
const MAX_ORDER_SIZE: f64 = 1000.0;
const RISK_AVERSION_MULTIPLIER: f64 = 2.0;
const TIME_HORIZON: f64 = 86400.0;
const VOLATILITY_FLOOR: f64 = 0.0001;
const VOLATILITY_CEILING: f64 = 0.5;
const SPREAD_BUFFER: f64 = 1.0001;
const INVENTORY_SKEW_FACTOR: f64 = 0.5;
const MAKER_FEE_BPS: f64 = -2.0;
const TAKER_FEE_BPS: f64 = 5.0;
const MIN_PROFIT_BPS: f64 = 3.0;
const ORACLE_CONFIDENCE_THRESHOLD: f64 = 0.02;
const MAX_PRICE_DEVIATION: f64 = 0.05;
const EWMA_ALPHA: f64 = 0.1;
const MICROSTRUCTURE_NOISE: f64 = 0.0001;

#[derive(Clone, Debug)]
pub struct ASMMEngine {
    pub symbol: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub gamma: f64,
    pub kappa: f64,
    pub eta: f64,
    pub sigma: f64,
    pub inventory: f64,
    pub target_inventory: f64,
    pub max_inventory: f64,
    pub min_inventory: f64,
    pub price_history: Arc<RwLock<VecDeque<PricePoint>>>,
    pub volatility_estimator: Arc<RwLock<VolatilityEstimator>>,
    pub risk_manager: Arc<RwLock<RiskManager>>,
    pub last_update: i64,
    pub time_to_close: f64,
    pub reservation_price: f64,
    pub optimal_spread: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub mid_price: f64,
    pub oracle_price: f64,
    pub confidence_interval: f64,
    pub order_size_bid: f64,
    pub order_size_ask: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub total_volume: f64,
    pub trade_count: u64,
    pub active: bool,
}

#[derive(Clone, Debug)]
pub struct PricePoint {
    pub timestamp: i64,
    pub price: f64,
    pub volume: f64,
    pub spread: f64,
}

#[derive(Clone, Debug)]
pub struct VolatilityEstimator {
    pub realized_vol: f64,
    pub garch_vol: f64,
    pub ewma_vol: f64,
    pub returns: VecDeque<f64>,
    pub squared_returns: VecDeque<f64>,
    pub alpha: f64,
    pub beta: f64,
    pub omega: f64,
}

#[derive(Clone, Debug)]
pub struct RiskManager {
    pub max_position_size: f64,
    pub max_order_size: f64,
    pub daily_loss_limit: f64,
    pub current_daily_pnl: f64,
    pub position_limits_breached: bool,
    pub trading_halted: bool,
    pub last_risk_check: i64,
}

impl ASMMEngine {
    pub fn new(
        symbol: String,
        base_decimals: u8,
        quote_decimals: u8,
        max_inventory: f64,
    ) -> Self {
        let volatility_estimator = Arc::new(RwLock::new(VolatilityEstimator {
            realized_vol: 0.01,
            garch_vol: 0.01,
            ewma_vol: 0.01,
            returns: VecDeque::with_capacity(VOLATILITY_WINDOW),
            squared_returns: VecDeque::with_capacity(VOLATILITY_WINDOW),
            alpha: 0.05,
            beta: 0.94,
            omega: 0.000001,
        }));

        let risk_manager = Arc::new(RwLock::new(RiskManager {
            max_position_size: max_inventory,
            max_order_size: max_inventory * 0.1,
            daily_loss_limit: max_inventory * 0.02,
            current_daily_pnl: 0.0,
            position_limits_breached: false,
            trading_halted: false,
            last_risk_check: 0,
        }));

        Self {
            symbol,
            base_decimals,
            quote_decimals,
            gamma: GAMMA_DEFAULT,
            kappa: KAPPA_DEFAULT,
            eta: ETA_DEFAULT,
            sigma: 0.01,
            inventory: 0.0,
            target_inventory: INVENTORY_TARGET,
            max_inventory,
            min_inventory: -max_inventory,
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_PRICE_HISTORY))),
            volatility_estimator,
            risk_manager,
            last_update: 0,
            time_to_close: TIME_HORIZON,
            reservation_price: 0.0,
            optimal_spread: 0.0,
            bid_price: 0.0,
            ask_price: 0.0,
            mid_price: 0.0,
            oracle_price: 0.0,
            confidence_interval: 0.0,
            order_size_bid: MIN_ORDER_SIZE,
            order_size_ask: MIN_ORDER_SIZE,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            total_volume: 0.0,
            trade_count: 0,
            active: true,
        }
    }

    pub async fn update_market_data(
        &mut self,
        oracle_price: f64,
        confidence: f64,
        timestamp: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if confidence > ORACLE_CONFIDENCE_THRESHOLD {
            return Err("Oracle confidence too low".into());
        }

        let price_deviation = (oracle_price - self.oracle_price).abs() / self.oracle_price.max(1.0);
        if self.oracle_price > 0.0 && price_deviation > MAX_PRICE_DEVIATION {
            return Err("Price deviation exceeds threshold".into());
        }

        self.oracle_price = oracle_price;
        self.confidence_interval = confidence;
        self.mid_price = oracle_price;

        let mut price_history = self.price_history.write().await;
        price_history.push_back(PricePoint {
            timestamp,
            price: oracle_price,
            volume: 0.0,
            spread: self.optimal_spread,
        });

        if price_history.len() > MAX_PRICE_HISTORY {
            price_history.pop_front();
        }

        self.update_volatility(oracle_price).await?;
        self.calculate_reservation_price(timestamp).await?;
        self.calculate_optimal_quotes(timestamp).await?;
        self.check_risk_limits().await?;

        self.last_update = timestamp;
        Ok(())
    }

    async fn update_volatility(&mut self, price: f64) -> Result<(), Box<dyn std::error::Error>> {
        let mut vol_estimator = self.volatility_estimator.write().await;
        let price_history = self.price_history.read().await;

        if price_history.len() < 2 {
            return Ok(());
        }

        let prev_price = price_history[price_history.len() - 2].price;
        let log_return = (price / prev_price).ln();

        vol_estimator.returns.push_back(log_return);
        vol_estimator.squared_returns.push_back(log_return * log_return);

        if vol_estimator.returns.len() > VOLATILITY_WINDOW {
            vol_estimator.returns.pop_front();
            vol_estimator.squared_returns.pop_front();
        }

        if vol_estimator.returns.len() >= 10 {
            let mean_return: f64 = vol_estimator.returns.iter().sum::<f64>() / vol_estimator.returns.len() as f64;
            let variance: f64 = vol_estimator.returns.iter()
                .map(|r| (r - mean_return).powi(2))
                .sum::<f64>() / (vol_estimator.returns.len() - 1) as f64;
            
            vol_estimator.realized_vol = variance.sqrt() * (86400.0_f64).sqrt();

            vol_estimator.ewma_vol = EWMA_ALPHA * log_return.abs() + (1.0 - EWMA_ALPHA) * vol_estimator.ewma_vol;

            let last_squared_return = log_return * log_return;
            vol_estimator.garch_vol = (vol_estimator.omega 
                + vol_estimator.alpha * last_squared_return 
                + vol_estimator.beta * vol_estimator.garch_vol.powi(2)).sqrt();

            self.sigma = (0.4 * vol_estimator.realized_vol 
                + 0.3 * vol_estimator.garch_vol 
                + 0.3 * vol_estimator.ewma_vol)
                .max(VOLATILITY_FLOOR)
                .min(VOLATILITY_CEILING);
        }

        Ok(())
    }

    async fn calculate_reservation_price(&mut self, timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
        let remaining_time = (self.time_to_close - (timestamp as f64 - self.last_update as f64)).max(1.0);
        let time_factor = remaining_time / TIME_HORIZON;
        
        let inventory_risk = self.inventory - self.target_inventory;
        let risk_adjustment = self.gamma * self.sigma.powi(2) * inventory_risk * time_factor;
        
        let microstructure_adjustment = MICROSTRUCTURE_NOISE * self.sigma * inventory_risk.signum();
        
        self.reservation_price = self.mid_price - risk_adjustment - microstructure_adjustment;
        
        Ok(())
    }

    async fn calculate_optimal_quotes(&mut self, timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
        let remaining_time = (self.time_to_close - (timestamp as f64 - self.last_update as f64)).max(1.0);
        let time_factor = (remaining_time / TIME_HORIZON).sqrt();
        
        let base_spread = 2.0 / self.gamma + self.sigma * (2.0 * time_factor * self.gamma.ln()).sqrt();
        
        let inventory_penalty = 2.0 * self.gamma * self.sigma.powi(2) * 
            (self.inventory - self.target_inventory).abs() * time_factor;
        
        self.optimal_spread = (base_spread + inventory_penalty)
            .max(MIN_SPREAD_BPS / 10000.0 * self.mid_price)
            .min(MAX_SPREAD_BPS / 10000.0 * self.mid_price);
        
        let inventory_skew = INVENTORY_SKEW_FACTOR * self.gamma * self.sigma.powi(2) * 
            (self.inventory - self.target_inventory) * time_factor;
        
        let half_spread = self.optimal_spread / 2.0;
        let tick_adjusted_half_spread = (half_spread / TICK_SIZE).round() * TICK_SIZE;
        
        self.bid_price = ((self.reservation_price - tick_adjusted_half_spread - inventory_skew) / TICK_SIZE).round() * TICK_SIZE;
        self.ask_price = ((self.reservation_price + tick_adjusted_half_spread - inventory_skew) / TICK_SIZE).round() * TICK_SIZE;
        
        let spread_with_fees = (TAKER_FEE_BPS - MAKER_FEE_BPS) / 10000.0 * self.mid_price;
        let min_profitable_spread = (MIN_PROFIT_BPS / 10000.0 + spread_with_fees) * SPREAD_BUFFER;
        
        if self.ask_price - self.bid_price < min_profitable_spread {
            let adjustment = (min_profitable_spread - (self.ask_price - self.bid_price)) / 2.0;
            self.bid_price -= adjustment;
            self.ask_price += adjustment;
        }
        
        self.calculate_order_sizes().await?;
        
        Ok(())
    }

    async fn calculate_order_sizes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let risk_manager = self.risk_manager.read().await;
        
        let base_size = self.max_inventory * 0.05;
        let vol_adjustment = (1.0 / (1.0 + self.sigma * 10.0)).max(0.2);
        let inventory_ratio = self.inventory / self.max_inventory;
        
                let bid_size_adjustment = 1.0 + INVENTORY_SKEW_FACTOR * inventory_ratio.max(-1.0).min(1.0);
        let ask_size_adjustment = 1.0 - INVENTORY_SKEW_FACTOR * inventory_ratio.max(-1.0).min(1.0);
        
        self.order_size_bid = ((base_size * vol_adjustment * bid_size_adjustment) / LOT_SIZE).round() * LOT_SIZE;
        self.order_size_ask = ((base_size * vol_adjustment * ask_size_adjustment) / LOT_SIZE).round() * LOT_SIZE;
        
        self.order_size_bid = self.order_size_bid
            .max(MIN_ORDER_SIZE)
            .min(risk_manager.max_order_size)
            .min(self.max_inventory - self.inventory);
            
        self.order_size_ask = self.order_size_ask
            .max(MIN_ORDER_SIZE)
            .min(risk_manager.max_order_size)
            .min(self.inventory - self.min_inventory);
        
        if self.inventory >= self.max_inventory * MAX_INVENTORY_RATIO {
            self.order_size_bid = 0.0;
        }
        
        if self.inventory <= self.min_inventory * MAX_INVENTORY_RATIO {
            self.order_size_ask = 0.0;
        }
        
        Ok(())
    }

    async fn check_risk_limits(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut risk_manager = self.risk_manager.write().await;
        
        if self.inventory.abs() >= self.max_inventory * MAX_INVENTORY_RATIO {
            risk_manager.position_limits_breached = true;
        } else {
            risk_manager.position_limits_breached = false;
        }
        
        if risk_manager.current_daily_pnl <= -risk_manager.daily_loss_limit {
            risk_manager.trading_halted = true;
            self.active = false;
            return Err("Daily loss limit reached".into());
        }
        
        let position_value = self.inventory.abs() * self.mid_price;
        let max_allowed_position = risk_manager.max_position_size * self.mid_price;
        
        if position_value > max_allowed_position {
            self.order_size_bid = 0.0;
            self.order_size_ask = 0.0;
            return Err("Position size limit exceeded".into());
        }
        
        risk_manager.last_risk_check = Clock::get()?.unix_timestamp;
        
        Ok(())
    }

    pub async fn execute_trade(
        &mut self,
        side: OrderSide,
        quantity: f64,
        price: f64,
        timestamp: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let adjusted_quantity = (quantity / LOT_SIZE).round() * LOT_SIZE;
        
        if adjusted_quantity < MIN_ORDER_SIZE {
            return Err("Order size below minimum".into());
        }
        
        match side {
            OrderSide::Buy => {
                if self.inventory + adjusted_quantity > self.max_inventory {
                    return Err("Trade would exceed max inventory".into());
                }
                self.inventory += adjusted_quantity;
                self.realized_pnl -= adjusted_quantity * price * (1.0 + MAKER_FEE_BPS / 10000.0);
            },
            OrderSide::Sell => {
                if self.inventory - adjusted_quantity < self.min_inventory {
                    return Err("Trade would exceed min inventory".into());
                }
                self.inventory -= adjusted_quantity;
                self.realized_pnl += adjusted_quantity * price * (1.0 - MAKER_FEE_BPS / 10000.0);
            }
        }
        
        self.total_volume += adjusted_quantity;
        self.trade_count += 1;
        
        self.update_unrealized_pnl();
        
        let mut risk_manager = self.risk_manager.write().await;
        risk_manager.current_daily_pnl = self.realized_pnl + self.unrealized_pnl;
        
        self.calculate_reservation_price(timestamp).await?;
        self.calculate_optimal_quotes(timestamp).await?;
        
        Ok(())
    }

    fn update_unrealized_pnl(&mut self) {
        self.unrealized_pnl = self.inventory * (self.mid_price - self.calculate_average_cost());
    }

    fn calculate_average_cost(&self) -> f64 {
        if self.inventory.abs() < f64::EPSILON {
            return self.mid_price;
        }
        
        let notional = self.realized_pnl + self.inventory * self.mid_price;
        notional / self.inventory
    }

    pub async fn get_quote(&self) -> Result<Quote, Box<dyn std::error::Error>> {
        if !self.active {
            return Err("Engine inactive".into());
        }
        
        let risk_manager = self.risk_manager.read().await;
        if risk_manager.trading_halted {
            return Err("Trading halted due to risk limits".into());
        }
        
        Ok(Quote {
            bid_price: self.bid_price,
            bid_size: self.order_size_bid,
            ask_price: self.ask_price,
            ask_size: self.order_size_ask,
            timestamp: self.last_update,
            spread: self.ask_price - self.bid_price,
            mid_price: self.mid_price,
        })
    }

    pub async fn update_parameters(
        &mut self,
        gamma: Option<f64>,
        kappa: Option<f64>,
        eta: Option<f64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(g) = gamma {
            if g <= 0.0 || g > 1.0 {
                return Err("Invalid gamma parameter".into());
            }
            self.gamma = g;
        }
        
        if let Some(k) = kappa {
            if k <= 0.0 || k > 10.0 {
                return Err("Invalid kappa parameter".into());
            }
            self.kappa = k;
        }
        
        if let Some(e) = eta {
            if e <= 0.0 || e > 1.0 {
                return Err("Invalid eta parameter".into());
            }
            self.eta = e;
        }
        
        self.calculate_optimal_quotes(Clock::get()?.unix_timestamp).await?;
        
        Ok(())
    }

    pub async fn reset_daily_metrics(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut risk_manager = self.risk_manager.write().await;
        risk_manager.current_daily_pnl = 0.0;
        risk_manager.trading_halted = false;
        
        self.realized_pnl = 0.0;
        self.trade_count = 0;
        self.total_volume = 0.0;
        
        Ok(())
    }

    pub fn get_inventory_ratio(&self) -> f64 {
        self.inventory / self.max_inventory
    }

    pub fn get_spread_bps(&self) -> f64 {
        if self.mid_price > 0.0 {
            (self.ask_price - self.bid_price) / self.mid_price * 10000.0
        } else {
            0.0
        }
    }

    pub async fn emergency_close_position(&mut self, market_price: f64) -> Result<f64, Box<dyn std::error::Error>> {
        if self.inventory.abs() < f64::EPSILON {
            return Ok(0.0);
        }
        
        let fee_multiplier = if self.inventory > 0.0 {
            1.0 - TAKER_FEE_BPS / 10000.0
        } else {
            1.0 + TAKER_FEE_BPS / 10000.0
        };
        
        let close_pnl = self.inventory.abs() * market_price * fee_multiplier;
        
        if self.inventory > 0.0 {
            self.realized_pnl += close_pnl;
        } else {
            self.realized_pnl -= close_pnl;
        }
        
        self.inventory = 0.0;
        self.unrealized_pnl = 0.0;
        self.active = false;
        
        Ok(close_pnl)
    }

    pub fn calculate_sharpe_ratio(&self) -> f64 {
        let price_history = futures::executor::block_on(self.price_history.read());
        
        if price_history.len() < 2 {
            return 0.0;
        }
        
        let returns: Vec<f64> = price_history.windows(2)
            .map(|w| (w[1].price / w[0].price).ln())
            .collect();
        
        if returns.is_empty() {
            return 0.0;
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        if variance > 0.0 {
            mean_return / variance.sqrt() * (365.0_f64).sqrt()
        } else {
            0.0
        }
    }

    pub fn get_risk_metrics(&self) -> RiskMetrics {
        RiskMetrics {
            inventory_ratio: self.get_inventory_ratio(),
            spread_bps: self.get_spread_bps(),
            volatility: self.sigma,
            sharpe_ratio: self.calculate_sharpe_ratio(),
            total_pnl: self.realized_pnl + self.unrealized_pnl,
            realized_pnl: self.realized_pnl,
            unrealized_pnl: self.unrealized_pnl,
            trade_count: self.trade_count,
            total_volume: self.total_volume,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug)]
pub struct Quote {
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
    pub timestamp: i64,
    pub spread: f64,
    pub mid_price: f64,
}

#[derive(Clone, Debug)]
pub struct RiskMetrics {
    pub inventory_ratio: f64,
    pub spread_bps: f64,
    pub volatility: f64,
    pub sharpe_ratio: f64,
    pub total_pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub trade_count: u64,
    pub total_volume: f64,
}

impl VolatilityEstimator {
    pub fn update_garch_params(&mut self, returns: &[f64]) {
        if returns.len() < 20 {
            return;
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        self.omega = variance * 0.01;
        self.alpha = 0.05;
        self.beta = 0.94;
        
        let persistence = self.alpha + self.beta;
        if persistence >= 1.0 {
            self.beta = 0.99 - self.alpha;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_initialization() {
        let engine = ASMMEngine::new(
            "SOL/USDC".to_string(),
            9,
            6,
            1000.0,
        );
        
        assert_eq!(engine.inventory, 0.0);
        assert_eq!(engine.gamma, GAMMA_DEFAULT);
        assert!(engine.active);
    }

    #[tokio::test]
    async fn test_quote_generation() {
        let mut engine = ASMMEngine::new(
            "SOL/USDC".to_string(),
            9,
            6,
            1000.0,
        );
        
        engine.update_market_data(100.0, 0.01, 1000).await.unwrap();
        
        let quote = engine.get_quote().await.unwrap();
        assert!(quote.bid_price < quote.ask_price);
        assert!(quote.bid_size > 0.0);
        assert!(quote.ask_size > 0.0);
    }
}

