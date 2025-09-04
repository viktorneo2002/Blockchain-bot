use anchor_client::{Client, Cluster, Program};
use anchor_lang::prelude::*;
use drift::controller::position::PositionDirection;
use drift::instructions::{OrderParams, OrderType, MarketType, OrderTriggerCondition, PostOnlyParam};
use drift::math::constants::{
    PRICE_PRECISION, PRICE_PRECISION_I64, QUOTE_PRECISION, QUOTE_PRECISION_I64,
    BASE_PRECISION, BASE_PRECISION_I64, FUNDING_RATE_PRECISION, FUNDING_RATE_PRECISION_I128,
    PEG_PRECISION, AMM_RESERVE_PRECISION, SPOT_BALANCE_PRECISION, SPOT_BALANCE_PRECISION_U64,
};
use drift::math::funding::calculate_funding_rate_long_short;
use drift::math::oracle::{oracle_price_data_to_price, OraclePriceData};
use drift::state::oracle::OracleSource;
use drift::state::perp_market::{PerpMarket, MarketStatus, AMM};
use drift::state::spot_market::{SpotMarket, SpotBalanceType};
use drift::state::state::State;
use drift::state::user::{User, UserStats, PerpPosition, SpotPosition};
use pyth_sdk_solana::state::PriceAccount;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::{interval, sleep, timeout};
use thiserror::Error;
use crate::execution::{ExecutionEngine, ExecutionError};
use std::{sync::Arc, time::Duration};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, pubkey::Pubkey};
use anchor_lang::prelude::Program;
use drift::state::user::User;
use crate::{
    execution::{ExecutionEngine, ExecutionError},
    tx_manager::TxManager,
    risk_manager::{RiskConfig, RiskManager},
    error::ArbitrageError,
};
use std::sync::Arc;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, pubkey::Pubkey};
use anchor_lang::prelude::Program;
use drift::state::user::User;
use crate::{
    execution::{ExecutionEngine, ExecutionError},
    tx_manager::TxManager,
    risk_manager::{RiskConfig, RiskManager},
    error::ArbitrageError,
};

// Fixed local utils (implemented inline)
mod utils {
    pub mod precision {
        pub fn decimal_to_precision(value: f64, precision: u64) -> u64 {
            (value * precision as f64) as u64
        }
    }
    
    pub mod market_metrics {
        use drift::state::perp_market::PerpMarket;
        
        pub struct PerpMarketMetrics {
            pub market: PerpMarket,
            pub mark_prices: Vec<u64>,
        }
        
        impl PerpMarketMetrics {
            pub fn new(market: PerpMarket, _window: usize) -> Self {
                Self {
                    market,
                    mark_prices: Vec::new(),
                }
            }
            
            pub fn push_mark(&mut self, price: u64) {
                self.mark_prices.push(price);
                if self.mark_prices.len() > 100 {
                    self.mark_prices.remove(0);
                }
            }
            
            pub fn std_dev_log_return(&self) -> f64 {
                if self.mark_prices.len() < 2 {
                    return 0.0;
                }
                
                let mut returns = Vec::new();
                for i in 1..self.mark_prices.len() {
                    let prev = self.mark_prices[i-1] as f64;
                    let current = self.mark_prices[i] as f64;
                    if prev > 0.0 {
                        returns.push((current / prev).ln());
                    }
                }
                
                if returns.is_empty() {
                    return 0.0;
                }
                
                let mean = returns.iter().sum::<f64>() / returns.len() as f64;
                let variance = returns.iter()
                    .map(|r| (r - mean).powi(2))
                    .sum::<f64>() / returns.len() as f64;
                
                variance.sqrt()
            }
            
            pub fn liquidity_depth(&self) -> f64 {
                // Simplified liquidity calculation
                let base_asset_reserve = self.market.amm.base_asset_reserve as f64;
                let quote_asset_reserve = self.market.amm.quote_asset_reserve as f64;
                (base_asset_reserve * quote_asset_reserve).sqrt() / PRICE_PRECISION as f64
            }
            
            pub fn total_notional_quote(&self, mark_price: u64) -> u128 {
                let oi = self.market.amm.base_asset_amount_long.abs() as u128 + 
                         self.market.amm.base_asset_amount_short.abs() as u128;
                (oi * mark_price as u128) / PRICE_PRECISION as u128
            }
        }
    }
}

mod core {
    pub mod funding_adjust {
        use drift::state::perp_market::PerpMarket;
        use crate::utils::market_metrics::PerpMarketMetrics;
        
        pub fn compute_adjusted_funding_rates(
            metrics: &PerpMarketMetrics,
            oracle_price: u64,
            mark_price: u64,
            current_ts: i64,
        ) -> Result<(i128, i128), Box<dyn std::error::Error>> {
            // Simplified implementation - use the same as original for now
            let market = &metrics.market;
            let (long_rate, short_rate) = drift::math::funding::calculate_funding_rate_long_short(
                market,
                oracle_price,
                mark_price,
                current_ts,
            )?;
            
            Ok((long_rate, short_rate))
        }
    }
    
    pub mod confidence {
        pub fn calculate_confidence_score(
            oracle_confidence: u64,
            oracle_price: u64,
            open_interest: u128,
            total_fee: i128,
        ) -> f64 {
            let confidence_ratio = oracle_confidence as f64 / oracle_price as f64;
            let oi_ratio = open_interest as f64 / 1_000_000_000.0; // Normalize
            let fee_ratio = total_fee.abs() as f64 / 1_000_000.0; // Normalize
            
            // Weighted average
            (confidence_ratio * 0.5 + oi_ratio * 0.3 + fee_ratio * 0.2).min(1.0).max(0.0)
        }
    }
}

// Use the inline modules
use crate::utils::market_metrics::PerpMarketMetrics;
use crate::core::funding_adjust::compute_adjusted_funding_rates;
use crate::core::confidence::calculate_confidence_score;
// ----------------------------------------------------

const DRIFT_PROGRAM_ID: &str = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const MIN_FUNDING_RATE_THRESHOLD_BPS: i128 = 10;
const MAX_POSITION_SIZE_USDC: u64 = 100_000;
const MAX_LEVERAGE: u32 = 5;
const SLIPPAGE_BPS: u64 = 30;
const MIN_PROFIT_THRESHOLD_USDC: u64 = 10;
const ORACLE_CONFIDENCE_THRESHOLD_BPS: u64 = 100;
const MAX_FUNDING_PAYMENT_PERIOD: i64 = 3600;
const PRIORITY_FEE_MICRO_LAMPORTS: u64 = 50_000;
const COMPUTE_UNITS: u32 = 400_000;
const ORACLE_STALENESS_SLOTS: u64 = 25;
const MAX_RETRY_ATTEMPTS: u32 = 3;
const POSITION_CHECK_INTERVAL_SECS: u64 = 60;
const MARKET_UPDATE_INTERVAL_MS: u64 = 500;
const ARB_CHECK_INTERVAL_MS: u64 = 100;
const FUNDING_RATE_BUFFER: u64 = 10 * PRICE_PRECISION / 10000; // 10 bps buffer
const STALE_ORDER_THRESHOLD_SECONDS: u64 = 300;

#[derive(Error, Debug)]
pub enum ArbitrageError {
    #[error("RPC error: {0}")]
    RpcError(#[from] solana_client::client_error::ClientError),
    #[error("Anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    #[error("Oracle error: {0}")]
    OracleError(String),
    #[error("Position error: {0}")]
    PositionError(String),
    #[error("Insufficient collateral: required {required}, available {available}")]
    InsufficientCollateral { required: u64, available: u64 },
    #[error("Market data stale: last update {0}ms ago")]
    StaleMarketData(u64),
    #[error("Transaction failed after {0} attempts")]
    TransactionFailed(u32),
    #[error("Risk limit exceeded: {0}")]
    RiskLimitExceeded(String),
    #[error("Math error: {0}")]
    MathError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}

type Result<T> = std::result::Result<T, ArbitrageError>;

#[derive(Clone)]
pub struct DriftPerpFundingArbitrageur {
    pub client: Arc<RpcClient>,
    pub drift_program: Program,
    pub execution_engine: ExecutionEngine,
    pub risk_manager: RiskManager,
    pub config: DriftPerpFundingArbitrageurConfig,
    pub market_cache: Arc<RwLock<HashMap<u16, PerpMarketData>>>,
    pub user_account: Arc<RwLock<Option<User>>>,
    pub last_update: Arc<RwLock<Instant>>,
    pub priority_fee: Arc<RwLock<u64>>,
    pub keypair: Arc<Keypair>,
}

#[derive(Clone, Debug)]
struct PerpMarketData {
    market: PerpMarket,
    funding_rate_long: i128,
    funding_rate_short: i128,
    mark_price: u64,
    oracle_price: u64,
    oracle_confidence: u64,
    open_interest: u128,
    last_update: Instant,
}

#[derive(Debug)]
struct ArbitrageOpportunity {
    market_index: u16,
    direction: PositionDirection,
    size: u64,
    expected_funding_payment: i128,
    entry_price: u64,
    funding_rate: i128,
    confidence_score: f64,
}

impl DriftPerpFundingArbitrageur {
    pub async fn new(
        client: Arc<RpcClient>,
        drift_program: Program,
        config: DriftPerpFundingArbitrageurConfig,
        starting_equity: i128,
    ) -> Result<Self> {
        let execution_engine = ExecutionEngine::new(
            TxManager::new(client.clone()),
            drift_program.clone(),
        );
        
        let risk_cfg = RiskConfig {
            max_leverage: 5.0,
            max_per_trade_pct: 0.5,
            max_total_exposure_pct: 10.0,
            session_max_drawdown_pct: 5.0,
            min_confidence: 0.7,
            stop_loss_usdc: 100_000, // $100
        };
        
        let risk_manager = RiskManager::new(risk_cfg, starting_equity);
        
        let keypair = Keypair::new();
        let cluster = Cluster::Custom("https://spl_gdn.mainnet.fandahao.com".to_string(), "https://spl_gdn.mainnet.fandahao.com".to_string());
        let payer = Arc::new(keypair);
        let anchor_client = Client::new_with_options(
            cluster,
            Arc::clone(&payer),
            CommitmentConfig::confirmed(),
        );
        
        let drift_program = anchor_client.program(
            Pubkey::from_str(DRIFT_PROGRAM_ID)
                .map_err(|_| ArbitrageError::AnchorError(anchor_lang::error::ErrorCode::InvalidProgramId.into()))?
        );
        
        Ok(Self {
            client,
            drift_program,
            execution_engine,
            risk_manager,
            config,
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            user_account: Arc::new(RwLock::new(None)),
            last_update: Arc::new(RwLock::new(Instant::now())),
            priority_fee: Arc::new(RwLock::new(PRIORITY_FEE_MICRO_LAMPORTS)),
            keypair: payer,
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.validate_config()?;
        self.initialize_user_account().await?;
        
        let mut update_interval = interval(Duration::from_millis(MARKET_UPDATE_INTERVAL_MS));
        let mut arb_interval = interval(Duration::from_millis(ARB_CHECK_INTERVAL_MS));
        
        loop {
            tokio::select! {
                _ = update_interval.tick() => {
                    if let Err(e) = self.update_market_data().await {
                        log::error!("Failed to update market data: {}", e);
                    }
                }
                _ = arb_interval.tick() => {
                    if let Err(e) = self.check_and_execute_arbitrage().await {
                        log::error!("Arbitrage execution failed: {}", e);
                    }
                }
            }
        }
    }

    async fn initialize_user_account(&self) -> Result<()> {
        let user_pubkey = self.get_user_account_pubkey()?;
        match self.client.get_account(&user_pubkey).await {
            Ok(account) => {
                let user = User::try_deserialize(&mut account.data.as_slice())
                    .map_err(|e| ArbitrageError::AnchorError(e.into()))?;
                *self.user_account.write().unwrap() = Some(user);
            }
            Err(_) => {
                log::warn!("User account not found, needs initialization");
            }
        }
        Ok(())
    }

    async fn update_market_data(&self) -> Result<()> {
        let state_pubkey = self.get_state_pubkey();
        let state_account = self.client.get_account(&state_pubkey).await?;
        let state = State::try_deserialize(&mut state_account.data.as_slice())
            .map_err(|e| ArbitrageError::AnchorError(e.into()))?;
        
        let current_slot = self.client.get_slot().await?;
        let current_ts = self.get_current_timestamp()?;
        let mut market_cache = self.market_cache.write().unwrap();
        
        for i in 0..state.number_of_markets {
            let market_pubkey = self.get_perp_market_pubkey(i);
            let market_account = self.client.get_account(&market_pubkey).await?;
            let market = PerpMarket::try_deserialize(&mut market_account.data.as_slice())
                .map_err(|e| ArbitrageError::AnchorError(e.into()))?;
            
            if market.status != MarketStatus::Active {
                continue;
            }
            
            let oracle_data = self.get_oracle_price_data(&market.amm.oracle, &market.amm.oracle_source, current_slot).await?;
            let mark_price = calculate_mark_price(&market.amm, oracle_data.price);
            let (funding_rate_long, funding_rate_short) = calculate_funding_rate_long_short(
                &market,
                oracle_data.price,
                mark_price,
                current_ts,
            ).map_err(|e| ArbitrageError::MathError(format!("Funding rate calculation failed: {:?}", e)))?;
            
            market_cache.insert(i, PerpMarketData {
                market: market.clone(),
                funding_rate_long,
                funding_rate_short,
                mark_price,
                oracle_price: oracle_data.price,
                oracle_confidence: oracle_data.confidence,
                open_interest: market.amm.base_asset_amount_long.abs() as u128 + 
                              market.amm.base_asset_amount_short.abs() as u128,
                last_update: Instant::now(),
            });
        }
        
        *self.last_update.write().unwrap() = Instant::now();
        Ok(())
    }

    async fn get_oracle_price_data(&self, oracle: &Pubkey, oracle_source: &OracleSource, current_slot: u64) -> Result<OraclePriceData> {
        let oracle_account = self.client.get_account(oracle).await?;
        
        match oracle_source {
            OracleSource::Pyth | OracleSource::PythStableCoin => {
                let price_account: PriceAccount = *pyth_sdk_solana::state::load(&oracle).map_err(|_| 
                    ArbitrageError::OracleError("Failed to load Pyth price account".to_string()))?;
                
                let price = price_account.agg.price as i64;
                let confidence = price_account.agg.conf as u64;
                let publish_time = price_account.timestamp;
                
                let current_time = self.get_current_timestamp()?;
                if current_time - publish_time > 30 {
                    return Err(ArbitrageError::OracleError("Pyth oracle price too stale".to_string()));
                }
                
                let scale = 10u64.pow(price_account.expo.unsigned_abs());
                Ok(OraclePriceData {
                    price: (price.unsigned_abs() * PRICE_PRECISION) / scale,
                    confidence: (confidence * PRICE_PRECISION) / scale,
                    delay: (current_time - publish_time) as u64,
                    has_sufficient_number_of_data_points: true,
                })
            }
            OracleSource::Switchboard => {
                if oracle_account.data.len() < 277 {
                    return Err(ArbitrageError::OracleError("Invalid Switchboard data".to_string()));
                }
                
                let result_bytes = &oracle_account.data[1..33];
                let mantissa = i128::from_le_bytes(result_bytes[..16].try_into().unwrap());
                let scale = u32::from_le_bytes(result_bytes[28..32].try_into().unwrap());
                
                let confidence_bytes = &oracle_account.data[33..65];
                let conf_mantissa = i128::from_le_bytes(confidence_bytes[..16].try_into().unwrap());
                let conf_scale = u32::from_le_bytes(confidence_bytes[28..32].try_into().unwrap());
                
                let slot = u64::from_le_bytes(oracle_account.data[269..277].try_into().unwrap());
                
                if current_slot.saturating_sub(slot) > ORACLE_STALENESS_SLOTS {
                    return Err(ArbitrageError::OracleError("Switchboard oracle too stale".to_string()));
                }
                
                let price = (mantissa.unsigned_abs() * PRICE_PRECISION as u128 / 10u128.pow(scale)) as u64;
                let confidence = (conf_mantissa.unsigned_abs() * PRICE_PRECISION as u128 / 10u128.pow(conf_scale)) as u64;
                
                Ok(OraclePriceData {
                    price,
                    confidence,
                    delay: current_slot.saturating_sub(slot),
                    has_sufficient_number_of_data_points: true,
                })
            }
            _ => Err(ArbitrageError::OracleError("Unsupported oracle source".to_string())),
        }
    }

    async fn check_and_execute_arbitrage(&self) -> Result<()> {
        let opportunities = self.find_arbitrage_opportunities()?;
        
        for opp in opportunities {
            if self.validate_opportunity(&opp).await? {
                match timeout(Duration::from_secs(30), self.execute_arbitrage(opp)).await {
                    Ok(Ok(_)) => log::info!("Successfully executed arbitrage"),
                    Ok(Err(e)) => log::error!("Failed to execute arbitrage: {}", e),
                    Err(_) => log::error!("Arbitrage execution timed out"),
                }
            }
        }
        
        Ok(())
    }

    fn find_arbitrage_opportunities(&self) -> Result<Vec<ArbitrageOpportunity>> {
        let market_cache = self.market_cache.read().unwrap();
        let mut opportunities = Vec::new();
        
        for (&market_index, market_data) in market_cache.iter() {
            if market_data.last_update.elapsed() > Duration::from_millis(1000) {
                continue;
            }
            
            let funding_rate_threshold = (MIN_FUNDING_RATE_THRESHOLD_BPS as i128 * FUNDING_RATE_PRECISION_I128) / 10000;
            
            if market_data.funding_rate_long.abs() > funding_rate_threshold {
                let mut metrics = PerpMarketMetrics::new(market_data.market.clone(), 64);
                metrics.push_mark(market_data.mark_price);

                let (funding_rate_long_adj, funding_rate_short_adj) = match compute_adjusted_funding_rates(
                    &metrics,
                    market_data.oracle_price,
                    market_data.mark_price,
                    self.get_current_timestamp()?,
                ) {
                    Ok((l,s)) => (l, s),
                    Err(e) => {
                        log::warn!("Funding adjust failed for market {}: {}", market_index, e);
                        continue;
                    }
                };

                let direction = if funding_rate_long_adj > 0 {
                    PositionDirection::Short
                } else {
                    PositionDirection::Long
                };

                let funding_rate_used = if direction == PositionDirection::Long { 
                    funding_rate_short_adj 
                } else { 
                    funding_rate_long_adj 
                };

                let max_base_size = self.calculate_max_position_size(&market_data.market, market_data.mark_price)?;
                let size = max_base_size.min((MAX_POSITION_SIZE_USDC as u128 * PRICE_PRECISION as u128 / market_data.mark_price as u128) as u64);

                let raw_funding_payment = calculate_funding_payment_in_quote_precision(funding_rate_used, size as i128)
                    .map_err(|e| ArbitrageError::MathError(format!("funding payment calc failed: {:?}", e)))?;

                let volatility_mult = (metrics.std_dev_log_return().powi(2) * 1.0 + 1.0).clamp(1.0, 3.0);
                let liquidity = metrics.liquidity_depth() as f64;
                let liquidity_mult = if liquidity < 1_000_000.0 { 1.15 + (1_000_000.0 - liquidity) / 10_000_000.0 } else { 1.0 };
                let fee_pool_ratio = metrics.market.amm.total_fee_minus_distributions.abs() as f64 / (metrics.total_notional_quote(market_data.mark_price) as f64 + 1.0);
                let fee_mult = if fee_pool_ratio < 0.01 { 0.8 } else { 1.0 };
                let funding_buffer_multiplier = 1.0 - (FUNDING_RATE_BUFFER as f64 / PRICE_PRECISION as f64);
                let funding_modifier = (volatility_mult * liquidity_mult * fee_mult * funding_buffer_multiplier).clamp(0.5, 3.0);

                let expected_funding_payment = ((raw_funding_payment as f64) * funding_modifier).round() as i128;

                let confidence_score = calculate_confidence_score(
                    market_data.oracle_confidence,
                    market_data.oracle_price,
                    market_data.open_interest,
                    metrics.market.amm.total_fee_minus_distributions,
                );

                if expected_funding_payment.abs() as u128 > (MIN_PROFIT_THRESHOLD_USDC as u128 * QUOTE_PRECISION as u128) && confidence_score > 0.7 {
                    opportunities.push(ArbitrageOpportunity {
                        market_index,
                        direction,
                        size,
                        expected_funding_payment,
                        entry_price: market_data.mark_price,
                        funding_rate: funding_rate_used,
                        confidence_score,
                    });
                }
            }
        }
        
        opportunities.sort_by(|a, b| {
            let a_score = (a.expected_funding_payment.abs() as f64) * a.confidence_score;
            let b_score = (b.expected_funding_payment.abs() as f64) * b.confidence_score;
            b_score.partial_cmp(&a_score).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        Ok(opportunities)
    }

    async fn validate_opportunity(&self, opp: &ArbitrageOpportunity) -> Result<bool> {
        let market_cache = self.market_cache.read().unwrap();
        let market_data = market_cache.get(&opp.market_index)
            .ok_or_else(|| ArbitrageError::PositionError("Market not found".to_string()))?;
        
        if market_data.last_update.elapsed() > Duration::from_millis(1000) {
            return Err(ArbitrageError::StaleMarketData(market_data.last_update.elapsed().as_millis() as u64));
        }
        
        let price_deviation_bps = ((market_data.oracle_price as i128 - market_data.mark_price as i128).abs() * 10000) 
            / market_data.oracle_price as i128;
        if price_deviation_bps > ORACLE_CONFIDENCE_THRESHOLD_BPS as i128 {
            return Ok(false);
        }
        
        let user_equity = self.get_user_equity().await?;
        let required_margin = (opp.size * market_data.mark_price / PRICE_PRECISION) / MAX_LEVERAGE as u64;
        
        if required_margin > user_equity * 8 / 10 {
            return Err(ArbitrageError::InsufficientCollateral {
                required: required_margin,
                available: user_equity * 8 / 10,
            });
        }
        
        let existing_position = self.get_position(opp.market_index).await?;
        if let Some(pos) = existing_position {
            if (pos.base_asset_amount > 0 && opp.direction == PositionDirection::Long) ||
               (pos.base_asset_amount < 0 && opp.direction == PositionDirection::Short) {
                return Ok(false);
            }
        }
        
        Ok(true)
    }

    async fn execute_arbitrage(&self, opp: ArbitrageOpportunity) -> Result<()> {
        // Check risk limits before proceeding
        if !self.risk_manager.can_trade(opp.confidence_score) {
            return Err(ArbitrageError::RiskLimitExceeded);
        }
        
        // Calculate adaptive position size
        let base_size = self.risk_manager.calc_position_size_base(
            opp.equity_quote,
            opp.mark_price,
            opp.volatility,
            opp.liquidity_score
        );
        
        let limit_price = match opp.direction {
            PositionDirection::Long => opp.entry_price * (10000 + SLIPPAGE_BPS) / 10000,
            PositionDirection::Short => opp.entry_price * (10000 - SLIPPAGE_BPS) / 10000,
        };
        
        let order_params = OrderParams {
            order_type: OrderType::Limit,
            market_type: MarketType::Perp,
            direction: opp.direction,
            user_order_id: 0,
            base_asset_amount: base_size,
            price: limit_price,
            market_index: opp.market_index,
            reduce_only: false,
            post_only: PostOnlyParam::None,
            immediate_or_cancel: false,
            trigger_price: Some(0),
            trigger_condition: OrderTriggerCondition::Above,
            oracle_price_offset: Some(0),
            auction_duration: Some(0),
            max_ts: Some(self.get_current_timestamp()? + 60),
            auction_start_price: Some(0),
            auction_end_price: Some(0),
        };
        
        let base_ix_accounts = vec![
            AccountMeta::new_readonly(self.get_state_pubkey(), false),
            AccountMeta::new(self.get_user_account_pubkey()?, false),
            AccountMeta::new(self.get_user_stats_pubkey()?, false),
            AccountMeta::new_readonly(self.keypair.pubkey(), true),
            AccountMeta::new_readonly(self.get_perp_market_pubkey(opp.market_index), false),
            AccountMeta::new_readonly(self.market_cache.read().unwrap().get(&opp.market_index).unwrap().market.amm.oracle, false),
        ];
        
        let sig = self.execution_engine
            .place_limit_with_fallbacks(
                base_ix_accounts,
                order_params,
                &self.keypair,
                &[],
            )
            .await
            .map_err(|e| ArbitrageError::ExecutionError(e.to_string()))?;

        // Schedule stale order cancellation
        tokio::time::sleep(Duration::from_secs(STALE_ORDER_THRESHOLD_SECONDS)).await;
        self.execution_engine
            .cancel_all_stale(vec![self.get_user_account_pubkey()?], &self.keypair, &[])
            .await
            .map_err(|e| ArbitrageError::ExecutionError(e.to_string()))?;

        log::info!(
            "Executed funding arbitrage: market={}, direction={:?}, size={}, expected_payment={}, confidence={:.2}, tx={}",
            opp.market_index,
            opp.direction,
            base_size,
            opp.expected_funding_payment,
            opp.confidence_score,
            sig
        );
        
        tokio::spawn({
            let self_clone = self.clone();
            let market_index = opp.market_index;
            let direction = opp.direction;
            async move {
                if let Err(e) = self_clone.monitor_position(market_index, direction).await {
                    log::error!("Position monitoring failed: {}", e);
                }
            }
        });
        
        // Update risk manager with new equity position
        self.risk_manager.update_equity(self.get_user_equity().await?);
        
        Ok(())
    }

    async fn monitor_position(&self, market_index: u16, direction: PositionDirection) -> Result<()> {
        let mut check_interval = interval(Duration::from_secs(POSITION_CHECK_INTERVAL_SECS));
        let start_time = Instant::now();
        
        loop {
            check_interval.tick().await;
            
            let position = self.get_position(market_index).await?;
            if position.is_none() || position.as_ref().unwrap().base_asset_amount == 0 {
                log::info!("Position closed for market {}", market_index);
                break;
            }
            
            let pos = position.unwrap();
            let market_cache = self.market_cache.read().unwrap();
            let market_data = market_cache.get(&market_index)
                .ok_or_else(|| ArbitrageError::PositionError("Market not found".to_string()))?;
            
            let current_funding_rate = if direction == PositionDirection::Short {
                market_data.funding_rate_long
            } else {
                market_data.funding_rate_short
            };
            let mark_price = market_data.mark_price;
            drop(market_cache);
            
            let unrealized_pnl = calculate_unrealized_pnl(&pos, mark_price);
            let funding_pnl = calculate_accrued_funding(&pos, current_funding_rate);
            let total_pnl = unrealized_pnl + funding_pnl;
            
            log::debug!(
                "Position monitor: market={}, pnl={}, funding_pnl={}, duration={}s",
                market_index,
                total_pnl,
                funding_pnl,
                start_time.elapsed().as_secs()
            );
            
            if start_time.elapsed() > Duration::from_secs(MAX_FUNDING_PAYMENT_PERIOD as u64) {
                log::info!("Max funding period reached, closing position");
                self.close_position(market_index).await?;
                break;
            }
            
            if (direction == PositionDirection::Short && current_funding_rate < 0) ||
               (direction == PositionDirection::Long && current_funding_rate > 0) {
                log::info!("Funding rate flipped, closing position");
                self.close_position(market_index).await?;
                break;
            }
            
            if total_pnl < -(MIN_PROFIT_THRESHOLD_USDC as i128 * QUOTE_PRECISION_I64 as i128 * 2) {
                log::warn!("Stop loss triggered, closing position");
                self.close_position(market_index).await?;
                break;
            }
        }
        
        Ok(())
    }

    async fn close_position(&self, market_index: u16) -> Result<()> {
        let position = self.get_position(market_index).await?
            .ok_or_else(|| ArbitrageError::PositionError("No position to close".to_string()))?;
        
        let direction = if position.base_asset_amount > 0 {
            PositionDirection::Short
        } else {
            PositionDirection::Long
        };
        
        let market_cache = self.market_cache.read().unwrap();
        let market_data = market_cache.get(&market_index)
            .ok_or_else(|| ArbitrageError::PositionError("Market not found".to_string()))?;
        let mark_price = market_data.mark_price;
        drop(market_cache);
        
        let limit_price = match direction {
            PositionDirection::Long => mark_price * (10000 + SLIPPAGE_BPS) / 10000,
            PositionDirection::Short => mark_price * (10000 - SLIPPAGE_BPS) / 10000,
        };
        
        let order_params = OrderParams {
            order_type: OrderType::Limit,
            market_type: MarketType::Perp,
            direction,
            user_order_id: 0,
            base_asset_amount: position.base_asset_amount.unsigned_abs() as u64,
            price: limit_price,
            market_index,
            reduce_only: true,
            post_only: PostOnlyParam::None,
            immediate_or_cancel: true,
            trigger_price: Some(0),
            trigger_condition: OrderTriggerCondition::Above,
            oracle_price_offset: Some(0),
            auction_duration: Some(0),
            max_ts: Some(self.get_current_timestamp()? + 30),
            auction_start_price: Some(0),
            auction_end_price: Some(0),
        };
        
        let base_ix_accounts = vec![
            AccountMeta::new_readonly(self.get_state_pubkey(), false),
            AccountMeta::new(self.get_user_account_pubkey()?, false),
            AccountMeta::new(self.get_user_stats_pubkey()?, false),
            AccountMeta::new_readonly(self.keypair.pubkey(), true),
            AccountMeta::new_readonly(self.get_perp_market_pubkey(opp.market_index), false),
            AccountMeta::new_readonly(self.market_cache.read().unwrap().get(&opp.market_index).unwrap().market.amm.oracle, false),
        ];
        
        let sig = self.execution_engine
            .place_limit_with_fallbacks(
                base_ix_accounts,
                order_params,
                &self.keypair,
                &[],
            )
            .await
            .map_err(|e| ArbitrageError::ExecutionError(e.to_string()))?;

        log::info!("Closed position: market={}, tx={}", market_index, sig);
        Ok(())
    }

    async fn get_position(&self, market_index: u16) -> Result<Option<PerpPosition>> {
        let user = self.get_user_account().await?;
        Ok(user.perp_positions.iter()
            .find(|p| p.market_index == market_index && p.base_asset_amount != 0)
            .cloned())
    }

    async fn get_user_account(&self) -> Result<User> {
        let user_pubkey = self.get_user_account_pubkey()?;
        let user_account = self.client.get_account(&user_pubkey).await?;
        User::try_deserialize(&mut user_account.data.as_slice())
            .map_err(|e| ArbitrageError::AnchorError(e.into()))
    }

    async fn get_user_equity(&self) -> Result<u64> {
        let user = self.get_user_account().await?;
        let mut total_collateral = 0i128;
        let mut total_liability = 0i128;
        
        for spot_position in user.spot_positions.iter() {
            if spot_position.scaled_balance == 0 {
                continue;
            }
            
            let spot_market = self.get_spot_market(spot_position.market_index).await?;
            let token_amount = get_token_amount(
                spot_position.scaled_balance,
                &spot_market,
                &spot_position.balance_type,
            )?;
            
            let oracle_data = self.get_oracle_price_data(
                &spot_market.oracle,
                &spot_market.oracle_source,
                self.client.get_slot().await?,
            ).await?;
            
            let value = (token_amount as i128 * oracle_data.price as i128) / PRICE_PRECISION_I64 as i128;
            
            match spot_position.balance_type {
                SpotBalanceType::Deposit => total_collateral += value,
                SpotBalanceType::Borrow => total_liability += value,
            }
        }
        
        for perp_position in user.perp_positions.iter() {
            if perp_position.base_asset_amount != 0 {
                let market_cache = self.market_cache.read().unwrap();
                if let Some(market_data) = market_cache.get(&perp_position.market_index) {
                    let pnl = calculate_unrealized_pnl(perp_position, market_data.mark_price);
                    if pnl > 0 {
                        total_collateral += pnl;
                    } else {
                        total_liability += pnl.abs();
                    }
                }
            }
        }
        
        Ok((total_collateral.saturating_sub(total_liability)).max(0) as u64)
    }

    async fn get_spot_market(&self, market_index: u16) -> Result<SpotMarket> {
        let spot_market_pubkey = self.get_spot_market_pubkey(market_index);
        let spot_market_account = self.client.get_account(&spot_market_pubkey).await?;
        SpotMarket::try_deserialize(&mut spot_market_account.data.as_slice())
            .map_err(|e| ArbitrageError::AnchorError(e.into()))
    }

    fn calculate_max_position_size(&self, market: &PerpMarket, mark_price: u64) -> Result<u64> {
        let max_open_interest = market.amm.max_open_interest;
        let current_oi_long = market.amm.base_asset_amount_long.unsigned_abs();
        let current_oi_short = market.amm.base_asset_amount_short.unsigned_abs();
        let current_oi = current_oi_long.max(current_oi_short);
        
        if current_oi >= max_open_interest {
            return Ok(0);
        }
        
        let available_oi = max_open_interest.saturating_sub(current_oi);
        let max_size_from_oi = available_oi / 10;
        let max_size_from_capital = (MAX_POSITION_SIZE_USDC * PRICE_PRECISION) / mark_price;
        
        Ok(max_size_from_oi.min(max_size_from_capital) as u64)
    }

    fn get_current_timestamp(&self) -> Result<i64> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ArbitrageError::PositionError("System time error".to_string()))?
            .as_secs() as i64)
    }

    fn get_state_pubkey(&self) -> Pubkey {
        Pubkey::find_program_address(
            &[b"drift_state"],
            &Pubkey::from_str(DRIFT_PROGRAM_ID).unwrap(),
        ).0
    }

    fn get_user_account_pubkey(&self) -> Result<Pubkey> {
        Ok(Pubkey::find_program_address(
            &[
                b"user",
                self.keypair.pubkey().as_ref(),
                &0u16.to_le_bytes(),
            ],
            &Pubkey::from_str(DRIFT_PROGRAM_ID).unwrap(),
        ).0)
    }

    fn get_user_stats_pubkey(&self) -> Result<Pubkey> {
        Ok(Pubkey::find_program_address(
            &[
                b"user_stats",
                self.keypair.pubkey().as_ref(),
            ],
            &Pubkey::from_str(DRIFT_PROGRAM_ID).unwrap(),
        ).0)
    }

    fn get_perp_market_pubkey(&self, market_index: u16) -> Pubkey {
        Pubkey::find_program_address(
            &[
                b"perp_market",
                &market_index.to_le_bytes(),
            ],
            &Pubkey::from_str(DRIFT_PROGRAM_ID).unwrap(),
        ).0
    }

    fn get_spot_market_pubkey(&self, market_index: u16) -> Pubkey {
        Pubkey::find_program_address(
            &[
                b"spot_market",
                &market_index.to_le_bytes(),
            ],
            &Pubkey::from_str(DRIFT_PROGRAM_ID).unwrap(),
        ).0
    }

    pub async fn set_priority_fee(&self, micro_lamports: u64) -> Result<()> {
        *self.priority_fee.write().unwrap() = micro_lamports;
        log::info!("Updated priority fee to {} micro-lamports", micro_lamports);
        Ok(())
    }

    pub async fn emergency_close_all_positions(&self) -> Result<()> {
        log::warn!("Emergency close initiated");
        let user = self.get_user_account().await?;
        
        for perp_position in user.perp_positions.iter() {
            if perp_position.base_asset_amount != 0 {
                match self.close_position(perp_position.market_index).await {
                    Ok(_) => log::info!("Closed position in market {}", perp_position.market_index),
                    Err(e) => log::error!("Failed to close position in market {}: {}", perp_position.market_index, e),
                }
            }
        }
        
        Ok(())
    }

    pub fn validate_config(&self) -> Result<()> {
        if MAX_POSITION_SIZE_USDC < MIN_PROFIT_THRESHOLD_USDC * 10 {
            return Err(ArbitrageError::RiskLimitExceeded(
                "Position size too small relative to profit threshold".to_string()
            ));
        }
        
        if MIN_FUNDING_RATE_THRESHOLD_BPS > 1000 {
            return Err(ArbitrageError::RiskLimitExceeded(
                "Funding rate threshold too high".to_string()
            ));
        }
        
        if SLIPPAGE_BPS > 100 {
            return Err(ArbitrageError::RiskLimitExceeded(
                "Slippage tolerance too high".to_string()
            ));
        }
        
        Ok(())
    }
}

fn calculate_mark_price(amm: &AMM, oracle_price: u64) -> u64 {
    let base_spread = amm.base_spread as u64;
    let net_base_amount = amm.base_asset_amount_with_amm;
    if net_base_amount > 0 {
        oracle_price + base_spread / 2
    } else if net_base_amount < 0 {
        oracle_price - base_spread / 2
    } else {
        oracle_price
    }
}

fn calculate_funding_payment_in_quote_precision(
    funding_rate: i128,
    base_asset_amount: i128,
) -> Result<i128, ArbitrageError> {
    let payment = base_asset_amount
        .checked_mul(funding_rate)
        .ok_or_else(|| ArbitrageError::MathError("Funding payment overflow".to_string()))?
        .checked_div(FUNDING_RATE_PRECISION_I128)
        .ok_or_else(|| ArbitrageError::MathError("Funding payment division error".to_string()))?;
    Ok(payment)
}

fn calculate_expected_funding_payment(
    base_amount: u64,
    funding_rate: i128,
    duration_seconds: i64,
    mark_price: u64,
) -> i128 {
    let base_i128 = base_amount as i128;
    let funding_rate_abs = funding_rate.abs();
    let mark_price_i128 = mark_price as i128;
    let duration_i128 = duration_seconds as i128;

    let numerator = base_i128
        .checked_mul(funding_rate_abs)
        .and_then(|v| v.checked_mul(mark_price_i128))
        .and_then(|v| v.checked_mul(duration_i128))
        .unwrap_or(0);

    let denominator = (PRICE_PRECISION as i128)
        .checked_mul(FUNDING_RATE_PRECISION_I128)
        .and_then(|v| v.checked_mul(3600))
        .unwrap_or(1);

    let raw_payment = numerator.checked_div(denominator).unwrap_or(0);

    if funding_rate < 0 {
        -raw_payment
    } else {
        raw_payment
    }
}

fn calculate_unrealized_pnl(position: &PerpPosition, mark_price: u64) -> i128 {
    let base_value = (position.base_asset_amount.abs() as i128 * mark_price as i128) / PRICE_PRECISION_I64 as i128;
    let entry_value = position.quote_asset_amount.abs() as i128;
    
    if position.base_asset_amount > 0 {
        base_value - entry_value
    } else {
        entry_value - base_value
    }
}

fn calculate_accrued_funding(position: &PerpPosition, current_funding_rate: i128) -> i128 {
    let funding_rate_delta = current_funding_rate.saturating_sub(position.last_cumulative_funding_rate);
    let funding_payment = (position.base_asset_amount as i128 * funding_rate_delta) / FUNDING_RATE_PRECISION_I128;
    -funding_payment
}

fn get_token_amount(
    scaled_balance: u64,
    spot_market: &SpotMarket,
    balance_type: &SpotBalanceType,
) -> Result<u64, ArbitrageError> {
    match balance_type {
        SpotBalanceType::Deposit => {
            let cumulative_interest = spot_market.cumulative_deposit_interest;
            let precision = SPOT_BALANCE_PRECISION_U64;
            Ok(((scaled_balance as u128 * cumulative_interest as u128) / precision as u128) as u64)
        }
        SpotBalanceType::Borrow => {
            let cumulative_interest = spot_market.cumulative_borrow_interest;
            let precision = SPOT_BALANCE_PRECISION_U64;
            Ok(((scaled_balance as u128 * cumulative_interest as u128) / precision as u128) as u64)
        }
    }
}

impl Clone for DriftPerpFundingArbitrageur {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            drift_program: self.drift_program.clone(),
            execution_engine: self.execution_engine.clone(),
            risk_manager: self.risk_manager.clone(),
            config: self.config.clone(),
            market_cache: Arc::clone(&self.market_cache),
            user_account: Arc::clone(&self.user_account),
            last_update: Arc::clone(&self.last_update),
            priority_fee: Arc::clone(&self.priority_fee),
            keypair: Arc::clone(&self.keypair),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_calculate_mark_price() {
        let mut amm = AMM::default();
        amm.base_spread = 20;
        amm.base_asset_amount_with_amm = 1000;
        
        let oracle_price = 50_000 * PRICE_PRECISION;
        let mark_price = calculate_mark_price(&amm, oracle_price);
        
        assert_eq!(mark_price, oracle_price + 10);
    }
    
    #[test]
    fn test_calculate_expected_funding_payment() {
        let base_amount = 10_000_000; // 0.01 BTC
        let funding_rate = 10 * FUNDING_RATE_PRECISION_I128 / 10000; // 0.1% (10 bps)
        let duration_seconds = 3600; // 1 hour
        let mark_price = 50_000 * PRICE_PRECISION;
        
        let payment = calculate_expected_funding_payment(
            base_amount,
            funding_rate,
            duration_seconds,
            mark_price,
        );
        
        assert!(payment < 0); // Long position pays funding
    }
    
    #[test]
    fn test_validate_config() {
        let keypair = Keypair::new();
        let execution_engine = ExecutionEngine::new();
        let arbitrageur = DriftPerpFundingArbitrageur {
            client: Arc::new(RpcClient::new("http://localhost:8899".to_string())),
            drift_program: Program::default(),
            execution_engine,
            risk_manager: RiskManager::new(RiskConfig::default(), 0),
            config: DriftPerpFundingArbitrageurConfig::default(),
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            user_account: Arc::new(RwLock::new(None)),
            last_update: Arc::new(RwLock::new(Instant::now())),
            priority_fee: Arc::new(RwLock::new(PRIORITY_FEE_MICRO_LAMPORTS)),
            keypair: Arc::new(keypair),
        };
        
        assert!(arbitrageur.validate_config().is_ok());
    }
}
