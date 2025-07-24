use anchor_client::{Client, Cluster, Program};
use anchor_lang::prelude::*;
use drift::controller::position::PositionDirection;
use drift::instructions::{OrderParams, OrderType, MarketType, OrderTriggerCondition, PostOnlyParam};
use drift::math::constants::{
    PRICE_PRECISION, PRICE_PRECISION_I64, QUOTE_PRECISION, QUOTE_PRECISION_I64,
    BASE_PRECISION, BASE_PRECISION_I64, FUNDING_RATE_PRECISION, FUNDING_RATE_PRECISION_I128,
    PEG_PRECISION, AMM_RESERVE_PRECISION, SPOT_BALANCE_PRECISION, SPOT_BALANCE_PRECISION_U64,
};
use crate::math::funding::{
    calculate_funding_payment_in_quote_precision, calculate_funding_rate_long_short,
};
use crate::math::constants::FUNDING_RATE_BUFFER;
use crate::math::safe_math::SafeMath;
use crate::state::perp_market::PerpMarket;
use crate::state::user::PerpPosition;
use crate::error::{DriftResult, ErrorCode};
use crate::drift_math::funding::{calculate_funding_payment};
use drift::math::funding::calculate_funding_rate_long_short;
use drift::math::oracle::{oracle_price_data_to_price, OraclePriceData};
use drift::state::oracle::OracleSource;
use drift::state::perp_market::{PerpMarket, MarketStatus, AMM};
use drift::state::spot_market::{SpotMarket, SpotBalanceType};
use drift::state::state::State;
use drift::state::user::{User, UserStats, PerpPosition, SpotPosition, MarketPosition};
use pyth_sdk_solana::state::PriceAccount;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
    signer::keypair::read_keypair_file,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::{interval, sleep, timeout};
use thiserror::Error;

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
}

type Result<T> = std::result::Result<T, ArbitrageError>;

#[derive(Clone)]
pub struct DriftPerpFundingArbitrageur {
    client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    drift_program: Program,
    market_cache: Arc<RwLock<HashMap<u16, PerpMarketData>>>,
    user_account: Arc<RwLock<Option<User>>>,
    last_update: Arc<RwLock<Instant>>,
    priority_fee: Arc<RwLock<u64>>,
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
    pub async fn new(rpc_url: &str, keypair: Keypair) -> Result<Self> {
        let client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        
        let cluster = Cluster::Custom(rpc_url.to_string(), rpc_url.to_string());
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
            keypair: payer,
            drift_program,
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            user_account: Arc::new(RwLock::new(None)),
            last_update: Arc::new(RwLock::new(Instant::now())),
            priority_fee: Arc::new(RwLock::new(PRIORITY_FEE_MICRO_LAMPORTS)),
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
                open_interest: market.amm.base_asset_amount_long + market.amm.base_asset_amount_short,
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
                let price_feed = pyth_sdk_solana::PriceFeed::try_deserialize(&oracle_account.data)
                    .map_err(|_| ArbitrageError::OracleError("Failed to deserialize Pyth oracle".to_string()))?;
                
                let price_account = price_feed.get_price_unchecked();
                let price = price_account.price as u64;
                let confidence = price_account.conf;
                let publish_time = price_account.publish_time;
                
                let current_time = self.get_current_timestamp()?;
                if current_time - publish_time > 30 {
                    return Err(ArbitrageError::OracleError("Pyth oracle price too stale".to_string()));
                }
                
                let scale = 10u64.pow(price_account.expo.unsigned_abs());
                Ok(OraclePriceData {
                    price: (price * PRICE_PRECISION) / scale,
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
                let direction = if market_data.funding_rate_long > 0 {
                    PositionDirection::Short
                } else {
                    PositionDirection::Long
                };
                
// --- ADVANCED FUNDING RATE CALCULATION (ASYMMETRIC, RISK-AWARE) ---

// 1️⃣ Volatility-aware skew penalty (Merton-Jump, time-adjusted)
let volatility_skew_boost = (market.std_dev_log_return * JUMP_INTENSITY).clamp(0.01, 0.15);

// 2️⃣ Long/Short open interest ratio (aka directional bias)
let long_oi = market.amm.base_asset_amount_long.max(1); // avoid div-zero
let short_oi = market.amm.base_asset_amount_short.max(1);
let oi_ratio = long_oi as f64 / short_oi as f64;
let directional_bias = (oi_ratio.ln()).clamp(-0.75, 0.75); // asymmetry control

// 3️⃣ Oracle deviation penalty
let oracle_diff_ratio = ((market.mark_price as f64 - market.oracle_price as f64).abs())
    / (market.oracle_price as f64).max(1.0);
let oracle_penalty = if oracle_diff_ratio > 0.01 {
    0.85 - (oracle_diff_ratio * 2.0).min(0.3)
} else {
    1.0
};

// 4️⃣ Entropy guard from spoofed funding (if spoof entropy high, reduce funding)
let spoof_entropy_ratio = (market.spoof_score as f64) / 100.0; // assume pre-computed
let entropy_penalty = if spoof_entropy_ratio > 0.25 {
    1.0 - spoof_entropy_ratio.min(0.5)
} else {
    1.0
};

// 5️⃣ Fee pool exhaustion compensation
let fee_pool_reserve_ratio = (market.fee_pool as f64) / (market.total_notional as f64 + 1.0);
let fee_pool_guard = if fee_pool_reserve_ratio < 0.01 {
    0.75
} else {
    1.0
};

// 6️⃣ Base rate from protocol
let (base_long_rate, base_short_rate, funding_settle_ts) =
    calculate_funding_rate_long_short(&mut market, raw_funding_rate)?;

// 7️⃣ Adjust long/short asymmetrically based on directional pressure
let funding_rate_long = (base_long_rate as f64)
    * (1.0 + directional_bias + volatility_skew_boost)
    * oracle_penalty
    * entropy_penalty
    * fee_pool_guard;

let funding_rate_short = (base_short_rate as f64)
    * (1.0 - directional_bias + volatility_skew_boost)
    * oracle_penalty
    * entropy_penalty
    * fee_pool_guard;

// 8️⃣ Final type cast and clamp
let funding_rate_long = funding_rate_long.round().clamp(-i64::MAX as f64, i64::MAX as f64) as i64;
let funding_rate_short = funding_rate_short.round().clamp(-i64::MAX as f64, i64::MAX as f64) as i64;

// 9️⃣ Return the values as tuple
let (_, _, funding_settle_ts) =
    calculate_funding_rate_long_short(&mut market, raw_funding_rate)?;
                
                let max_base_size = self.calculate_max_position_size(&market_data.market, market_data.mark_price)?;
                let size = max_base_size.min(MAX_POSITION_SIZE_USDC * PRICE_PRECISION / market_data.mark_price);
                
// --- ADVANCED FUNDING PAYMENT CALCULATION LOGIC ---

// Fetch expected funding rate baseline
let raw_funding_payment = calculate_funding_payment_in_quote_precision(funding_rate, size as i128)?;

// 1️⃣ Volatility Risk Adjustment (Merton-Jump-Aware)
let volatility_risk_multiplier = 1.0 + (market.std_dev_log_return.powi(2) * JUMP_INTENSITY * 0.5);

// 2️⃣ Liquidity Stress Penalty (shallow books = more skew = more cost)
let liquidity_stress = if market.liquidity < 1_000_000 {
    1.15 + (1_000_000.0 - market.liquidity as f64) / 10_000_000.0
} else {
    1.0
};

// 3️⃣ Oracle Divergence Guard (price manipulation or stale oracles)
let oracle_deviation = (market.mark_price as f64 - market.oracle_price as f64).abs()
    / (market.oracle_price as f64).max(1.0);
let oracle_penalty = if oracle_deviation > 0.01 {
    1.1 + oracle_deviation * 5.0
} else {
    1.0
};

// 4️⃣ Fee Pool Availability Modifier (if low, funding must be capped)
let fee_pool_capacity_ratio = (market.fee_pool as f64) / (market.total_notional as f64 + 1.0);
let fee_pool_cap = if fee_pool_capacity_ratio < 0.01 {
    0.8 // cap funding to avoid drain
} else {
    1.0
};

// 5️⃣ Funding Buffer (guard against over-accrual)
let funding_buffer_multiplier = 1.0 - FUNDING_RATE_BUFFER as f64 / PRICE_PRECISION as f64;

// 6️⃣ Combine all modifiers into a truth-based funding multiplier
let funding_modifier = volatility_risk_multiplier
    * liquidity_stress
    * oracle_penalty
    * funding_buffer_multiplier
    * fee_pool_cap;

// 7️⃣ Final expected funding payment in quote precision
let expected_funding_payment = ((raw_funding_payment as f64) * funding_modifier)
    .round() as i64;
                
                let confidence_score = calculate_confidence_score(
                    market_data.oracle_confidence,
                    market_data.oracle_price,
                    market_data.open_interest,
                    market_data.market.amm.total_fee_minus_distributions,
                );
                
                if expected_funding_payment.abs() as u64 > MIN_PROFIT_THRESHOLD_USDC * QUOTE_PRECISION && confidence_score > 0.7 {
                    opportunities.push(ArbitrageOpportunity {
                        market_index,
                        direction,
                        size,
                        expected_funding_payment,
                        entry_price: market_data.mark_price,
                        funding_rate,
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
        let limit_price = match opp.direction {
            PositionDirection::Long => opp.entry_price * (10000 + SLIPPAGE_BPS) / 10000,
            PositionDirection::Short => opp.entry_price * (10000 - SLIPPAGE_BPS) / 10000,
        };
        
        let order_params = OrderParams {
            order_type: OrderType::Limit,
            market_type: MarketType::Perp,
            direction: opp.direction,
            user_order_id: 0,
            base_asset_amount: opp.size,
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
        
        let tx = self.build_place_order_transaction(order_params).await?;
        let signature = self.send_transaction_with_retry(&tx).await?;
        
        log::info!(
            "Executed funding arbitrage: market={}, direction={:?}, size={}, expected_payment={}, confidence={:.2}, tx={}",
            opp.market_index,
            opp.direction,
            opp.size,
            opp.expected_funding_payment,
            opp.confidence_score,
            signature
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
        
        Ok(())
    }

    async fn build_place_order_transaction(&self, order_params: OrderParams) -> Result<Transaction> {
        let user_pubkey = self.get_user_account_pubkey()?;
        let user_stats_pubkey = self.get_user_stats_pubkey()?;
        let state_pubkey = self.get_state_pubkey();
        let market_pubkey = self.get_perp_market_pubkey(order_params.market_index);
        
        let market_cache = self.market_cache.read().unwrap();
        let market_data = market_cache.get(&order_params.market_index)
            .ok_or_else(|| ArbitrageError::PositionError("Market not found".to_string()))?;
        let oracle = market_data.market.amm.oracle;
        drop(market_cache);
        
        let accounts = drift::accounts::PlaceOrder {
            state: state_pubkey,
            user: user_pubkey,
            user_stats: user_stats_pubkey,
            authority: self.keypair.pubkey(),
        };
        
        let mut ix = self.drift_program
            .request()
            .accounts(accounts)
            .args(drift::instruction::PlaceOrder { order_params })
            .instructions()
            .map_err(|e| ArbitrageError::AnchorError(e))?;
        
        ix[0].accounts.push(AccountMeta::new_readonly(market_pubkey, false));
        ix[0].accounts.push(AccountMeta::new_readonly(oracle, false));
        
        let priority_fee = *self.priority_fee.read().unwrap();
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(priority_fee);
        
        let mut all_instructions = vec![compute_budget_ix, priority_fee_ix];
        all_instructions.extend(ix);
        
        let recent_blockhash = self.client.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &all_instructions,
            Some(&self.keypair.pubkey()),
            &[&*self.keypair],
            recent_blockhash,
        );
        
        Ok(tx)
    }

    async fn send_transaction_with_retry(&self, tx: &Transaction) -> Result<String> {
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
            max_retries: Some(0),
            ..Default::default()
        };
        
        for attempt in 0..MAX_RETRY_ATTEMPTS {
            match self.client.send_transaction_with_config(tx, config.clone()).await {
                Ok(signature) => {
                    match self.confirm_transaction(&signature).await {
                        Ok(_) => return Ok(signature.to_string()),
                        Err(e) => {
                            if attempt == MAX_RETRY_ATTEMPTS - 1 {
                                return Err(ArbitrageError::TransactionFailed(MAX_RETRY_ATTEMPTS));
                            }
                            log::warn!("Transaction confirmation failed, retrying: {}", e);
                        }
                    }
                }
                Err(e) => {
                    if attempt == MAX_RETRY_ATTEMPTS - 1 {
                        return Err(ArbitrageError::TransactionFailed(MAX_RETRY_ATTEMPTS));
                    }
                    log::warn!("Transaction send failed, retrying: {}", e);
                    sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                }
            }
        }
        
        Err(ArbitrageError::TransactionFailed(MAX_RETRY_ATTEMPTS))
    }

    async fn confirm_transaction(&self, signature: &solana_sdk::signature::Signature) -> Result<()> {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(30) {
            match self.client.get_signature_status(signature).await? {
                Some(Ok(_)) => return Ok(()),
                Some(Err(e)) => return Err(ArbitrageError::RpcError(
                    solana_client::client_error::ClientError::from(e)
                )),
                None => sleep(Duration::from_millis(500)).await,
            }
        }
        Err(ArbitrageError::TransactionFailed(1))
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
        
        let tx = self.build_place_order_transaction(order_params).await?;
        let signature = self.send_transaction_with_retry(&tx).await?;
        
        log::info!("Closed position: market={}, tx={}", market_index, signature);
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
        let current_oi_long = market.amm.base_asset_amount_long;
        let current_oi_short = market.amm.base_asset_amount_short;
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
// 1️⃣ Liquidity pressure adjustment: depth shrink = wider spread
let liquidity_ratio = (amm.reserve_liquidity as f64) / (amm.max_open_interest as f64).max(1.0);
let liquidity_penalty = if liquidity_ratio < 0.25 {
    1.5
} else if liquidity_ratio < 0.5 {
    1.25
} else {
    1.0
};

// 2️⃣ Volatility-aware base spread boost (jump-aware)
let volatility_boost = (amm.std_dev_log_return * 10000.0).round() as u64; // in quote precision
let volatility_spread = volatility_boost.clamp(10, 500); // safety guard

// 3️⃣ Oracle deviation shock guard
let oracle_diff = (amm.mark_price as i64 - oracle_price as i64).abs() as u64;
let oracle_penalty = (oracle_diff / 10).clamp(0, 250); // penalize stale/spoofed prices

// 4️⃣ Spread skew based on inventory imbalance
let oi_diff = amm.base_asset_amount_with_amm as i64;
let imbalance_spread_skew = (oi_diff.abs() as u64 * amm.base_spread / amm.max_open_interest.max(1))
    .clamp(0, amm.base_spread);

// 5️⃣ Combine all into final effective spread
let raw_spread = amm.base_spread
    + volatility_spread
    + oracle_penalty
    + imbalance_spread_skew;

let effective_spread = ((raw_spread as f64) * liquidity_penalty).round().clamp(10.0, 5000.0) as u64;

// 6️⃣ Final bid/ask calculation
let bid_price = oracle_price.saturating_sub(effective_spread / 2);
let ask_price = oracle_price.saturating_add(effective_spread / 2);
    
    let net_base_amount = amm.base_asset_amount_with_amm;
    if net_base_amount > 0 {
        ask_price
    } else if net_base_amount < 0 {
        bid_price
    } else {
        oracle_price
    }
}

pub fn calculate_real_funding_payment_max_precision(
    market: &mut PerpMarket,
    market_position: &PerpPosition,
    raw_funding_rate: i128,
) -> DriftResult<i64> {
    // Step 1: Calculate capped and asymmetric funding rate (protocol-level correction)
    let (funding_rate_long, funding_rate_short, _uncapped_pnl) =
        calculate_funding_rate_long_short(market, raw_funding_rate)?;

    // Step 2: Determine direction and choose appropriate funding rate
    let funding_rate_delta = if market_position.base_asset_amount > 0 {
        // Longs
        funding_rate_long.safe_sub(market_position.last_cumulative_funding_rate.cast()?)?
    } else {
        // Shorts
        funding_rate_short.safe_sub(market_position.last_cumulative_funding_rate.cast()?)?
    };

    if funding_rate_delta == 0 {
        return Ok(0);
    }

    // Step 3: Use full quote-precision version to account for decimal scaling
    let funding_pnl = calculate_funding_payment_in_quote_precision(
        funding_rate_delta,
        market_position.base_asset_amount.cast()?,
    )?;

    // Step 4: Cast to i64 (user-level PnL) safely
    Ok(funding_pnl.cast()?)
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

fn calculate_confidence_score(
    oracle_confidence: u64,
    oracle_price: u64,
    open_interest: u128,
    total_fee: i128,
) -> f64 {
// --- ADVANCED RELIABILITY CONFIDENCE MULTIPLIER ---

// 1️⃣ Oracle Confidence Normalization (nonlinear trust curve)
let oracle_conf_ratio = (oracle_confidence as f64 / oracle_price.max(1) as f64).min(0.5);
let conf_score = 1.0 - (oracle_conf_ratio.powf(0.7)).clamp(0.0, 0.3); // convex decay

// 2️⃣ Entropy Penalty (spoof score → exponential risk)
let spoof_score = market.spoof_score.unwrap_or(0).clamp(0, 100) as f64;
let entropy_penalty = (-spoof_score / 30.0).exp().clamp(0.6, 1.0); // sharper drop-off

// 3️⃣ Oracle Delay Penalty (latency = alpha decay)
let delay_penalty = (25.0 - market.oracle_delay.min(25) as f64) / 25.0; // 0.0–1.0 linear
let delay_score = delay_penalty.powf(1.5); // nonlinear trust decay

// 4️⃣ Volatility Risk Amplifier (add impact from jump regimes)
let vol_amp = (market.std_dev_log_return.powi(2) * 100.0).clamp(1.0, 8.0); // scale modifier

// 5️⃣ Signal Composite (weighted regime-aware)
let raw_score = (conf_score * 0.4 + oi_score * 0.3 + fee_score * 0.3)
    * entropy_penalty
    * delay_score;

// 6️⃣ Final Confidence Score
let reliability_score = (raw_score / vol_amp).clamp(0.0, 1.0);
    let oi_score = (open_interest as f64 / 1e15).min(1.0);
    let fee_score = (total_fee.abs() as f64 / 1e9).min(1.0);
    
    (confidence_ratio * 0.4 + oi_score * 0.4 + fee_score * 0.2).min(1.0).max(0.0)
}

fn get_token_amount(
    scaled_balance: u64,
    spot_market: &SpotMarket,
    balance_type: &SpotBalanceType,
) -> Result<u64> {
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
            keypair: Arc::clone(&self.keypair),
            drift_program: self.drift_program.clone(),
            market_cache: Arc::clone(&self.market_cache),
            user_account: Arc::clone(&self.user_account),
            last_update: Arc::clone(&self.last_update),
                        priority_fee: Arc::clone(&self.priority_fee),
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
        
        // Position value: 0.01 * 50,000 = $500
        // Funding payment: $500 * 0.001 * 1/24 = ~$0.021
        assert!(payment < 0); // Long position pays funding
        assert_eq!(payment.abs() / QUOTE_PRECISION_I64 as i128, 0); // ~$0.02
    }
    
    #[test]
    fn test_calculate_confidence_score() {
        let oracle_confidence = 50 * PRICE_PRECISION;
        let oracle_price = 50_000 * PRICE_PRECISION;
        let open_interest = 1_000_000 * BASE_PRECISION as u128;
        let total_fee = 1_000 * QUOTE_PRECISION_I64 as i128;
        
        let score = calculate_confidence_score(
            oracle_confidence,
            oracle_price,
            open_interest,
            total_fee,
        );
        
        assert!(score > 0.0 && score <= 1.0);
    }
    
    #[test]
    fn test_validate_config() {
        let keypair = Keypair::new();
        let arbitrageur = DriftPerpFundingArbitrageur {
            client: Arc::new(RpcClient::new("http://localhost:8899".to_string())),
            keypair: Arc::new(keypair),
            drift_program: unsafe { std::mem::zeroed() },
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            user_account: Arc::new(RwLock::new(None)),
            last_update: Arc::new(RwLock::new(Instant::now())),
            priority_fee: Arc::new(RwLock::new(PRIORITY_FEE_MICRO_LAMPORTS)),
        };
        
        assert!(arbitrageur.validate_config().is_ok());
    }
}

