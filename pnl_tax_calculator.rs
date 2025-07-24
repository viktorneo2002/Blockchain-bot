use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use chrono::{DateTime, Utc, Datelike};
use rust_decimal::prelude::*;
use rust_decimal::MathematicalOps;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use thiserror::Error;
use log::{info, error, warn};

const DECIMALS_USDC: u32 = 6;
const DECIMALS_SOL: u32 = 9;
const SHORT_TERM_DAYS: i64 = 365;
const WASH_SALE_DAYS: i64 = 30;
const MAX_PRECISION: u32 = 12;
const MAX_HISTORY_DAYS: i64 = 730;
const RATE_LIMIT_MS: u64 = 10;
const MAX_DISPOSALS: usize = 100000;
const SQRT_ITERATIONS: u64 = 50;

#[derive(Error, Debug)]
pub enum PnlError {
    #[error("Insufficient balance: need {needed}, have {available}")]
    InsufficientBalance { needed: Decimal, available: Decimal },
    #[error("Invalid decimal conversion")]
    DecimalConversion,
    #[error("Tax lot not found: {0}")]
    TaxLotNotFound(u64),
    #[error("Invalid tax method")]
    InvalidTaxMethod,
    #[error("Calculation overflow")]
    CalculationOverflow,
    #[error("Invalid quantity: {0}")]
    InvalidQuantity(Decimal),
    #[error("Rate limited")]
    RateLimited,
    #[error("State locked")]
    StateLocked,
    #[error("Invalid price: {0}")]
    InvalidPrice(Decimal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaxMethod {
    FIFO,
    LIFO,
    HIFO,
    SpecificIdentification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionType {
    Arbitrage,
    Sandwich,
    Liquidation,
    JitLiquidity,
    AtomicArb,
    Frontrun,
    Backrun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaxJurisdiction {
    US,
    UK,
    Germany,
    Singapore,
    Dubai,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaxLot {
    pub id: u64,
    pub mint: Pubkey,
    pub quantity: Decimal,
    pub cost_basis: Decimal,
    pub acquisition_date: DateTime<Utc>,
    pub acquisition_price: Decimal,
    pub transaction_type: TransactionType,
    pub signature: Signature,
    pub decimals: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Disposal {
    pub mint: Pubkey,
    pub quantity: Decimal,
    pub proceeds: Decimal,
    pub disposal_date: DateTime<Utc>,
    pub disposal_price: Decimal,
    pub lots_used: Vec<(u64, Decimal)>,
    pub realized_pnl: Decimal,
    pub short_term_gain: Decimal,
    pub long_term_gain: Decimal,
    pub fees: Decimal,
    pub audit_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlSnapshot {
    pub timestamp: DateTime<Utc>,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub roi_percentage: Decimal,
    pub sharpe_ratio: Decimal,
    pub win_rate: Decimal,
    pub avg_profit_per_trade: Decimal,
}

#[derive(Debug, Clone)]
struct StateTransaction {
    lots_backup: HashMap<Pubkey, VecDeque<TaxLot>>,
    disposals_count: usize,
}

pub struct PnlTaxCalculator {
    tax_lots: Arc<RwLock<HashMap<Pubkey, VecDeque<TaxLot>>>>,
    disposals: Arc<RwLock<Vec<Disposal>>>,
    tax_method: TaxMethod,
    jurisdiction: TaxJurisdiction,
    lot_counter: Arc<AtomicU64>,
    pnl_history: Arc<RwLock<BTreeMap<DateTime<Utc>, PnlSnapshot>>>,
    current_prices: Arc<RwLock<HashMap<Pubkey, Decimal>>>,
    basis_adjustments: Arc<RwLock<HashMap<u64, Decimal>>>,
    last_operation: Arc<Mutex<std::time::Instant>>,
    decimal_cache: Arc<RwLock<HashMap<Pubkey, u32>>>,
    state_lock: Arc<Mutex<()>>,
}

impl PnlTaxCalculator {
    pub fn new(tax_method: TaxMethod, jurisdiction: TaxJurisdiction) -> Self {
        Self {
            tax_lots: Arc::new(RwLock::new(HashMap::new())),
            disposals: Arc::new(RwLock::new(Vec::with_capacity(1000))),
            tax_method,
            jurisdiction,
            lot_counter: Arc::new(AtomicU64::new(0)),
            pnl_history: Arc::new(RwLock::new(BTreeMap::new())),
            current_prices: Arc::new(RwLock::new(HashMap::new())),
            basis_adjustments: Arc::new(RwLock::new(HashMap::new())),
            last_operation: Arc::new(Mutex::new(std::time::Instant::now())),
            decimal_cache: Arc::new(RwLock::new(HashMap::new())),
            state_lock: Arc::new(Mutex::new(())),
        }
    }

    fn rate_limit_check(&self) -> Result<(), PnlError> {
        let mut last = self.last_operation.lock().map_err(|_| PnlError::StateLocked)?;
        let now = std::time::Instant::now();
        if now.duration_since(*last).as_millis() < RATE_LIMIT_MS as u128 {
            return Err(PnlError::RateLimited);
        }
        *last = now;
        Ok(())
    }

    fn validate_quantity(&self, quantity: Decimal) -> Result<(), PnlError> {
        if quantity <= Decimal::ZERO || quantity.is_sign_negative() {
            return Err(PnlError::InvalidQuantity(quantity));
        }
        if quantity.scale() > MAX_PRECISION {
            return Err(PnlError::InvalidQuantity(quantity));
        }
        Ok(())
    }

    fn validate_price(&self, price: Decimal) -> Result<(), PnlError> {
        if price <= Decimal::ZERO || price.is_sign_negative() {
            return Err(PnlError::InvalidPrice(price));
        }
        Ok(())
    }

    fn normalize_amount(&self, amount: Decimal, decimals: u32) -> Decimal {
        let scale_factor = Decimal::from(10u64.pow(decimals));
        amount.round_dp_with_strategy(decimals, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
    }

    fn create_state_backup(&self) -> Result<StateTransaction, PnlError> {
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let disposals = self.disposals.read().map_err(|_| PnlError::StateLocked)?;
        
        Ok(StateTransaction {
            lots_backup: lots.clone(),
            disposals_count: disposals.len(),
        })
    }

    fn rollback_state(&self, backup: StateTransaction) -> Result<(), PnlError> {
        let mut lots = self.tax_lots.write().map_err(|_| PnlError::StateLocked)?;
        let mut disposals = self.disposals.write().map_err(|_| PnlError::StateLocked)?;
        
        *lots = backup.lots_backup;
        disposals.truncate(backup.disposals_count);
        
        Ok(())
    }

    pub fn record_acquisition(
        &self,
        mint: Pubkey,
        quantity: Decimal,
        cost: Decimal,
        transaction_type: TransactionType,
        signature: Signature,
        decimals: u32,
    ) -> Result<u64, PnlError> {
        self.rate_limit_check()?;
        self.validate_quantity(quantity)?;
        self.validate_price(cost)?;
        
        let _lock = self.state_lock.lock().map_err(|_| PnlError::StateLocked)?;
        
        let normalized_quantity = self.normalize_amount(quantity, decimals);
        let normalized_cost = self.normalize_amount(cost, DECIMALS_USDC);
        
        let price = normalized_cost.checked_div(normalized_quantity)
            .ok_or(PnlError::DecimalConversion)?;
        
        self.validate_price(price)?;
        
        let lot_id = self.lot_counter.fetch_add(1, Ordering::SeqCst) + 1;
        
        let tax_lot = TaxLot {
            id: lot_id,
            mint,
            quantity: normalized_quantity,
            cost_basis: normalized_cost,
            acquisition_date: Utc::now(),
            acquisition_price: price,
            transaction_type,
            signature,
            decimals,
        };
        
        let mut lots = self.tax_lots.write().map_err(|_| PnlError::StateLocked)?;
        lots.entry(mint).or_insert_with(VecDeque::new).push_back(tax_lot);
        
        self.decimal_cache.write().map_err(|_| PnlError::StateLocked)?
            .insert(mint, decimals);
        
        info!("Acquired {} {} at {} (lot #{})", normalized_quantity, mint, price, lot_id);
        
        Ok(lot_id)
    }

    pub fn record_disposal(
        &self,
        mint: Pubkey,
        quantity: Decimal,
        proceeds: Decimal,
        fees: Decimal,
    ) -> Result<Disposal, PnlError> {
        self.rate_limit_check()?;
        self.validate_quantity(quantity)?;
        self.validate_price(proceeds)?;
        
        let _lock = self.state_lock.lock().map_err(|_| PnlError::StateLocked)?;
        let backup = self.create_state_backup()?;
        
        let result = self.record_disposal_internal(mint, quantity, proceeds, fees);
        
        match result {
            Ok(disposal) => {
                self.prune_old_history()?;
                Ok(disposal)
            }
            Err(e) => {
                self.rollback_state(backup)?;
                Err(e)
            }
        }
    }

    fn record_disposal_internal(
        &self,
        mint: Pubkey,
        quantity: Decimal,
        proceeds: Decimal,
        fees: Decimal,
    ) -> Result<Disposal, PnlError> {
        let mut lots = self.tax_lots.write().map_err(|_| PnlError::StateLocked)?;
        let mint_lots = lots.get_mut(&mint).ok_or(PnlError::InsufficientBalance { 
            needed: quantity, 
            available: Decimal::ZERO 
        })?;
        
        let decimals = self.decimal_cache.read().map_err(|_| PnlError::StateLocked)?
            .get(&mint).copied().unwrap_or(DECIMALS_SOL);
        
        let normalized_quantity = self.normalize_amount(quantity, decimals);
        let normalized_proceeds = self.normalize_amount(proceeds, DECIMALS_USDC);
        let normalized_fees = self.normalize_amount(fees, DECIMALS_USDC);
        
        let available = mint_lots.iter().map(|l| l.quantity).sum();
        if normalized_quantity > available {
            return Err(PnlError::InsufficientBalance { 
                needed: normalized_quantity, 
                available 
            });
        }
        
        let mut remaining = normalized_quantity;
        let mut lots_used = Vec::new();
        let mut total_cost = Decimal::ZERO;
        let mut short_term_gain = Decimal::ZERO;
        let mut long_term_gain = Decimal::ZERO;
        let disposal_date = Utc::now();
        
        let ordered_indices = self.get_lot_order(mint_lots);
        
        for &idx in &ordered_indices {
            if remaining <= Decimal::ZERO {
                break;
            }
            
            let lot = &mut mint_lots[idx];
            let used_quantity = remaining.min(lot.quantity);
            let used_cost = lot.cost_basis
                .checked_mul(used_quantity)
                .and_then(|c| c.checked_div(lot.quantity))
                .ok_or(PnlError::CalculationOverflow)?;
            
            lots_used.push((lot.id, used_quantity));
            total_cost = total_cost.checked_add(used_cost)
                .ok_or(PnlError::CalculationOverflow)?;
            
            let proceeds_portion = normalized_proceeds
                .checked_mul(used_quantity)
                .and_then(|p| p.checked_div(normalized_quantity))
                .ok_or(PnlError::CalculationOverflow)?;
            
            let fees_portion = normalized_fees
                .checked_mul(used_quantity)
                .and_then(|f| f.checked_div(normalized_quantity))
                .ok_or(PnlError::CalculationOverflow)?;
            
                        let net_proceeds = proceeds_portion.checked_sub(fees_portion)
                .ok_or(PnlError::CalculationOverflow)?;
            
            let gain = net_proceeds.checked_sub(used_cost)
                .ok_or(PnlError::CalculationOverflow)?;
            
            let days_held = (disposal_date - lot.acquisition_date).num_days();
            
            if days_held < SHORT_TERM_DAYS {
                short_term_gain = short_term_gain.checked_add(gain)
                    .ok_or(PnlError::CalculationOverflow)?;
            } else {
                long_term_gain = long_term_gain.checked_add(gain)
                    .ok_or(PnlError::CalculationOverflow)?;
            }
            
            lot.quantity = lot.quantity.checked_sub(used_quantity)
                .ok_or(PnlError::CalculationOverflow)?;
            lot.cost_basis = lot.cost_basis.checked_sub(used_cost)
                .ok_or(PnlError::CalculationOverflow)?;
            remaining = remaining.checked_sub(used_quantity)
                .ok_or(PnlError::CalculationOverflow)?;
        }
        
        mint_lots.retain(|lot| lot.quantity > Decimal::ZERO);
        
        let net_proceeds = normalized_proceeds.checked_sub(normalized_fees)
            .ok_or(PnlError::CalculationOverflow)?;
        let realized_pnl = net_proceeds.checked_sub(total_cost)
            .ok_or(PnlError::CalculationOverflow)?;
        
        let disposal_price = normalized_proceeds.checked_div(normalized_quantity)
            .ok_or(PnlError::DecimalConversion)?;
        
        let audit_data = format!("{:?}:{:?}:{:?}:{:?}", mint, normalized_quantity, disposal_date, lots_used);
        let audit_hash = solana_sdk::hash::hash(audit_data.as_bytes()).to_bytes();
        
        let disposal = Disposal {
            mint,
            quantity: normalized_quantity,
            proceeds: normalized_proceeds,
            disposal_date,
            disposal_price,
            lots_used,
            realized_pnl,
            short_term_gain,
            long_term_gain,
            fees: normalized_fees,
            audit_hash,
        };
        
        let mut disposals = self.disposals.write().map_err(|_| PnlError::StateLocked)?;
        if disposals.len() >= MAX_DISPOSALS {
            let remove_count = disposals.len() / 10;
            disposals.drain(0..remove_count);
        }
        disposals.push(disposal.clone());
        drop(disposals);
        
        self.check_wash_sales(&mint, &disposal_date)?;
        
        info!("Disposed {} {} at {} (PnL: {})", normalized_quantity, mint, disposal_price, realized_pnl);
        
        Ok(disposal)
    }

    fn get_lot_order(&self, lots: &VecDeque<TaxLot>) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..lots.len()).collect();
        
        match self.tax_method {
            TaxMethod::FIFO => {},
            TaxMethod::LIFO => indices.reverse(),
            TaxMethod::HIFO => {
                indices.sort_by(|&a, &b| {
                    lots[b].acquisition_price
                        .partial_cmp(&lots[a].acquisition_price)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            },
            TaxMethod::SpecificIdentification => {},
        }
        
        indices
    }

    fn check_wash_sales(&self, mint: &Pubkey, disposal_date: &DateTime<Utc>) -> Result<(), PnlError> {
        if self.jurisdiction != TaxJurisdiction::US {
            return Ok(());
        }
        
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let mut adjustments = self.basis_adjustments.write().map_err(|_| PnlError::StateLocked)?;
        
        if let Some(mint_lots) = lots.get(mint) {
            for lot in mint_lots {
                let days_diff = (*disposal_date - lot.acquisition_date).num_days().abs();
                if days_diff <= WASH_SALE_DAYS {
                    let adjustment = Decimal::from_str("0.01").unwrap_or(Decimal::ZERO);
                    let current = adjustments.get(&lot.id).copied().unwrap_or(Decimal::ZERO);
                    adjustments.insert(lot.id, current.checked_add(adjustment)
                        .unwrap_or(current));
                }
            }
        }
        
        Ok(())
    }

    pub fn update_market_price(&self, mint: Pubkey, price: Decimal) -> Result<(), PnlError> {
        self.validate_price(price)?;
        self.current_prices.write().map_err(|_| PnlError::StateLocked)?
            .insert(mint, price);
        Ok(())
    }

    pub fn calculate_unrealized_pnl(&self, mint: &Pubkey) -> Result<Decimal, PnlError> {
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let prices = self.current_prices.read().map_err(|_| PnlError::StateLocked)?;
        
        let current_price = prices.get(mint).copied()
            .ok_or(PnlError::InvalidPrice(Decimal::ZERO))?;
        
        let mint_lots = match lots.get(mint) {
            Some(lots) => lots,
            None => return Ok(Decimal::ZERO),
        };
        
        let mut unrealized = Decimal::ZERO;
        for lot in mint_lots {
            let market_value = lot.quantity.checked_mul(current_price)
                .ok_or(PnlError::CalculationOverflow)?;
            
            let gain = market_value.checked_sub(lot.cost_basis)
                .ok_or(PnlError::CalculationOverflow)?;
            
            unrealized = unrealized.checked_add(gain)
                .ok_or(PnlError::CalculationOverflow)?;
        }
        
        Ok(unrealized)
    }

    pub fn calculate_total_pnl(&self) -> Result<PnlSnapshot, PnlError> {
        let disposals = self.disposals.read().map_err(|_| PnlError::StateLocked)?;
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let prices = self.current_prices.read().map_err(|_| PnlError::StateLocked)?;
        
        let mut realized_pnl = Decimal::ZERO;
        let mut total_trades = 0u64;
        let mut winning_trades = 0u64;
        
        for disposal in disposals.iter() {
            realized_pnl = realized_pnl.checked_add(disposal.realized_pnl)
                .ok_or(PnlError::CalculationOverflow)?;
            total_trades += 1;
            if disposal.realized_pnl > Decimal::ZERO {
                winning_trades += 1;
            }
        }
        
        let mut unrealized_pnl = Decimal::ZERO;
        let mut total_invested = Decimal::ZERO;
        
        for (mint, mint_lots) in lots.iter() {
            if let Some(&price) = prices.get(mint) {
                for lot in mint_lots {
                    let market_value = lot.quantity.checked_mul(price)
                        .ok_or(PnlError::CalculationOverflow)?;
                    
                    let unrealized = market_value.checked_sub(lot.cost_basis)
                        .ok_or(PnlError::CalculationOverflow)?;
                    
                    unrealized_pnl = unrealized_pnl.checked_add(unrealized)
                        .ok_or(PnlError::CalculationOverflow)?;
                    
                    total_invested = total_invested.checked_add(lot.cost_basis)
                        .ok_or(PnlError::CalculationOverflow)?;
                }
            }
        }
        
        let total_pnl = realized_pnl.checked_add(unrealized_pnl)
            .ok_or(PnlError::CalculationOverflow)?;
        
        let roi_percentage = if total_invested > Decimal::ZERO {
            total_pnl.checked_mul(Decimal::from(100))
                .and_then(|p| p.checked_div(total_invested))
                .unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        
        let win_rate = if total_trades > 0 {
            Decimal::from(winning_trades)
                .checked_mul(Decimal::from(100))
                .and_then(|w| w.checked_div(Decimal::from(total_trades)))
                .unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        
        let avg_profit = if total_trades > 0 {
            realized_pnl.checked_div(Decimal::from(total_trades))
                .unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        
        let sharpe = self.safe_calculate_sharpe(&disposals)?;
        
        let snapshot = PnlSnapshot {
            timestamp: Utc::now(),
            realized_pnl,
            unrealized_pnl,
            total_pnl,
            roi_percentage,
            sharpe_ratio: sharpe,
            win_rate,
            avg_profit_per_trade: avg_profit,
        };
        
        self.pnl_history.write().map_err(|_| PnlError::StateLocked)?
            .insert(snapshot.timestamp, snapshot.clone());
        
        Ok(snapshot)
    }

    fn safe_calculate_sharpe(&self, disposals: &[Disposal]) -> Result<Decimal, PnlError> {
        if disposals.len() < 30 {
            return Ok(Decimal::ZERO);
        }
        
        let mut daily_returns = HashMap::new();
        for disposal in disposals {
            let date = disposal.disposal_date.date();
            let entry = daily_returns.entry(date).or_insert((Decimal::ZERO, 0u32));
            entry.0 = entry.0.checked_add(disposal.realized_pnl)
                .ok_or(PnlError::CalculationOverflow)?;
            entry.1 += 1;
        }
        
        let returns: Vec<Decimal> = daily_returns.values()
            .map(|(pnl, _)| *pnl)
            .collect();
        
        if returns.len() < 20 {
            return Ok(Decimal::ZERO);
        }
        
        let mean = returns.iter()
            .try_fold(Decimal::ZERO, |acc, &r| acc.checked_add(r))
            .and_then(|sum| sum.checked_div(Decimal::from(returns.len())))
            .ok_or(PnlError::CalculationOverflow)?;
        
        let variance = returns.iter()
            .try_fold(Decimal::ZERO, |acc, &r| {
                let diff = r.checked_sub(mean)?;
                let squared = diff.checked_mul(diff)?;
                acc.checked_add(squared)
            })
            .and_then(|sum| sum.checked_div(Decimal::from(returns.len())))
            .ok_or(PnlError::CalculationOverflow)?;
        
        if variance <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        
        let std_dev = self.safe_sqrt(variance)?;
        let annual_factor = Decimal::from_str("15.87").unwrap_or(Decimal::from(252).sqrt().unwrap_or(Decimal::ONE));
        
        let risk_free = Decimal::from_str("0.045").unwrap_or(Decimal::ZERO);
        let daily_rf = risk_free.checked_div(Decimal::from(252))
            .unwrap_or(Decimal::ZERO);
        
        let excess = mean.checked_sub(daily_rf)
            .unwrap_or(mean);
        
        if std_dev > Decimal::ZERO {
            excess.checked_mul(annual_factor)
                .and_then(|e| e.checked_div(std_dev.checked_mul(annual_factor)
                    .unwrap_or(std_dev)))
                .ok_or(PnlError::CalculationOverflow)
        } else {
            Ok(Decimal::ZERO)
        }
    }

    fn safe_sqrt(&self, value: Decimal) -> Result<Decimal, PnlError> {
        if value < Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        if value == Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        
        let mut x = value;
        let two = Decimal::from(2);
        
        for _ in 0..SQRT_ITERATIONS {
            let next = x.checked_add(value.checked_div(x).unwrap_or(x))
                .and_then(|sum| sum.checked_div(two))
                .unwrap_or(x);
            
            if (next.checked_sub(x).unwrap_or(Decimal::ZERO)).abs() < Decimal::from_str("0.0000001").unwrap() {
                break;
            }
            x = next;
        }
        
        Ok(x)
    }

    pub fn calculate_tax_liability(&self, income: Decimal) -> Result<Decimal, PnlError> {
        if income <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        
                let brackets = match self.jurisdiction {
            TaxJurisdiction::US => vec![
                (Decimal::from(10275), Decimal::from_str("0.10").unwrap()),
                (Decimal::from(41775), Decimal::from_str("0.12").unwrap()),
                (Decimal::from(89075), Decimal::from_str("0.22").unwrap()),
                (Decimal::from(170050), Decimal::from_str("0.24").unwrap()),
                (Decimal::from(215950), Decimal::from_str("0.32").unwrap()),
                (Decimal::from(539900), Decimal::from_str("0.35").unwrap()),
                (Decimal::MAX, Decimal::from_str("0.37").unwrap()),
            ],
            TaxJurisdiction::UK => vec![
                (Decimal::from(12570), Decimal::ZERO),
                (Decimal::from(50270), Decimal::from_str("0.20").unwrap()),
                (Decimal::from(150000), Decimal::from_str("0.40").unwrap()),
                (Decimal::MAX, Decimal::from_str("0.45").unwrap()),
            ],
            TaxJurisdiction::Germany => vec![
                (Decimal::from(10908), Decimal::ZERO),
                (Decimal::from(62810), Decimal::from_str("0.14").unwrap()),
                (Decimal::from(277826), Decimal::from_str("0.42").unwrap()),
                (Decimal::MAX, Decimal::from_str("0.45").unwrap()),
            ],
            TaxJurisdiction::Singapore => vec![
                (Decimal::from(20000), Decimal::ZERO),
                (Decimal::from(40000), Decimal::from_str("0.02").unwrap()),
                (Decimal::from(80000), Decimal::from_str("0.07").unwrap()),
                (Decimal::from(120000), Decimal::from_str("0.115").unwrap()),
                (Decimal::from(160000), Decimal::from_str("0.15").unwrap()),
                (Decimal::from(200000), Decimal::from_str("0.18").unwrap()),
                (Decimal::from(240000), Decimal::from_str("0.19").unwrap()),
                (Decimal::from(280000), Decimal::from_str("0.195").unwrap()),
                (Decimal::from(320000), Decimal::from_str("0.20").unwrap()),
                (Decimal::from(500000), Decimal::from_str("0.22").unwrap()),
                (Decimal::from(1000000), Decimal::from_str("0.23").unwrap()),
                (Decimal::MAX, Decimal::from_str("0.24").unwrap()),
            ],
            TaxJurisdiction::Dubai => vec![
                (Decimal::MAX, Decimal::ZERO),
            ],
        };
        
        let mut tax = Decimal::ZERO;
        let mut prev_bracket = Decimal::ZERO;
        
        for (threshold, rate) in brackets {
            let taxable_in_bracket = income.min(threshold).checked_sub(prev_bracket)
                .unwrap_or(Decimal::ZERO);
            
            if taxable_in_bracket > Decimal::ZERO {
                let bracket_tax = taxable_in_bracket.checked_mul(rate)
                    .ok_or(PnlError::CalculationOverflow)?;
                tax = tax.checked_add(bracket_tax)
                    .ok_or(PnlError::CalculationOverflow)?;
            }
            
            if income <= threshold {
                break;
            }
            prev_bracket = threshold;
        }
        
        Ok(tax)
    }

    pub fn generate_tax_report(&self, year: i32) -> Result<TaxReport, PnlError> {
        let disposals = self.disposals.read().map_err(|_| PnlError::StateLocked)?;
        let year_start = chrono::NaiveDate::from_ymd_opt(year, 1, 1)
            .ok_or(PnlError::DecimalConversion)?
            .and_hms_opt(0, 0, 0)
            .ok_or(PnlError::DecimalConversion)?
            .and_utc();
        let year_end = chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)
            .ok_or(PnlError::DecimalConversion)?
            .and_hms_opt(0, 0, 0)
            .ok_or(PnlError::DecimalConversion)?
            .and_utc();
        
        let mut short_term_gains = Decimal::ZERO;
        let mut long_term_gains = Decimal::ZERO;
        let mut total_proceeds = Decimal::ZERO;
        let mut total_cost_basis = Decimal::ZERO;
        let mut total_fees = Decimal::ZERO;
        
        for disposal in disposals.iter() {
            if disposal.disposal_date >= year_start && disposal.disposal_date < year_end {
                short_term_gains = short_term_gains.checked_add(disposal.short_term_gain)
                    .ok_or(PnlError::CalculationOverflow)?;
                long_term_gains = long_term_gains.checked_add(disposal.long_term_gain)
                    .ok_or(PnlError::CalculationOverflow)?;
                total_proceeds = total_proceeds.checked_add(disposal.proceeds)
                    .ok_or(PnlError::CalculationOverflow)?;
                total_fees = total_fees.checked_add(disposal.fees)
                    .ok_or(PnlError::CalculationOverflow)?;
                
                let cost = disposal.proceeds.checked_sub(disposal.realized_pnl)
                    .unwrap_or(Decimal::ZERO);
                total_cost_basis = total_cost_basis.checked_add(cost)
                    .ok_or(PnlError::CalculationOverflow)?;
            }
        }
        
        let net_gains = short_term_gains.checked_add(long_term_gains)
            .ok_or(PnlError::CalculationOverflow)?;
        
        let short_tax = self.calculate_tax_liability(short_term_gains)?;
        let long_tax = if self.jurisdiction == TaxJurisdiction::US {
            long_term_gains.checked_mul(Decimal::from_str("0.15").unwrap())
                .ok_or(PnlError::CalculationOverflow)?
        } else {
            self.calculate_tax_liability(long_term_gains)?
        };
        
        let estimated_tax = short_tax.checked_add(long_tax)
            .ok_or(PnlError::CalculationOverflow)?;
        
        let effective_rate = if net_gains > Decimal::ZERO {
            estimated_tax.checked_div(net_gains)
                .unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        
        Ok(TaxReport {
            year,
            jurisdiction: self.jurisdiction,
            short_term_gains,
            long_term_gains,
            total_proceeds,
            total_cost_basis,
            total_fees,
            net_gains,
            estimated_tax,
            effective_rate,
        })
    }

    pub fn optimize_tax_harvest(&self, target_loss: Decimal) -> Result<Vec<HarvestRecommendation>, PnlError> {
        self.validate_price(target_loss)?;
        
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let prices = self.current_prices.read().map_err(|_| PnlError::StateLocked)?;
        let mut recommendations = Vec::new();
        let mut accumulated_loss = Decimal::ZERO;
        
        let mut candidates = Vec::new();
        
        for (mint, mint_lots) in lots.iter() {
            if let Some(&current_price) = prices.get(mint) {
                for lot in mint_lots {
                    let market_value = lot.quantity.checked_mul(current_price)
                        .ok_or(PnlError::CalculationOverflow)?;
                    
                    if market_value < lot.cost_basis {
                        let loss = lot.cost_basis.checked_sub(market_value)
                            .unwrap_or(Decimal::ZERO);
                        
                        let days_held = (Utc::now() - lot.acquisition_date).num_days();
                        let priority = if days_held < SHORT_TERM_DAYS {
                            loss.checked_mul(Decimal::from_str("1.5").unwrap())
                                .unwrap_or(loss)
                        } else {
                            loss
                        };
                        
                        candidates.push((*mint, lot.id, lot.quantity, loss, priority));
                    }
                }
            }
        }
        
        candidates.sort_by(|a, b| b.4.cmp(&a.4));
        
        for (mint, lot_id, quantity, loss, _) in candidates {
            if accumulated_loss >= target_loss {
                break;
            }
            
            let tax_benefit = self.calculate_tax_liability(loss)?;
            
            recommendations.push(HarvestRecommendation {
                mint,
                lot_id,
                quantity,
                expected_loss: loss,
                tax_benefit,
            });
            
            accumulated_loss = accumulated_loss.checked_add(loss)
                .ok_or(PnlError::CalculationOverflow)?;
        }
        
        Ok(recommendations)
    }

    fn prune_old_history(&self) -> Result<(), PnlError> {
        let mut history = self.pnl_history.write().map_err(|_| PnlError::StateLocked)?;
        let cutoff = Utc::now() - chrono::Duration::days(MAX_HISTORY_DAYS);
        
        let keys_to_remove: Vec<_> = history.range(..cutoff)
            .map(|(k, _)| *k)
            .collect();
        
        for key in keys_to_remove {
            history.remove(&key);
        }
        
        Ok(())
    }

    pub fn get_portfolio_summary(&self) -> Result<PortfolioSummary, PnlError> {
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let prices = self.current_prices.read().map_err(|_| PnlError::StateLocked)?;
        
        let mut positions = Vec::new();
        let mut total_value = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;
        
        for (mint, mint_lots) in lots.iter() {
            let mut position_quantity = Decimal::ZERO;
            let mut position_cost = Decimal::ZERO;
            
            for lot in mint_lots {
                position_quantity = position_quantity.checked_add(lot.quantity)
                    .ok_or(PnlError::CalculationOverflow)?;
                position_cost = position_cost.checked_add(lot.cost_basis)
                    .ok_or(PnlError::CalculationOverflow)?;
            }
            
            if position_quantity > Decimal::ZERO {
                let current_price = prices.get(mint).copied()
                    .unwrap_or(Decimal::ZERO);
                
                let market_value = position_quantity.checked_mul(current_price)
                    .ok_or(PnlError::CalculationOverflow)?;
                
                total_value = total_value.checked_add(market_value)
                    .ok_or(PnlError::CalculationOverflow)?;
                total_cost = total_cost.checked_add(position_cost)
                    .ok_or(PnlError::CalculationOverflow)?;
                
                positions.push(PositionSummary {
                    mint: *mint,
                    quantity: position_quantity,
                    cost_basis: position_cost,
                    market_value,
                    unrealized_pnl: market_value.checked_sub(position_cost)
                        .unwrap_or(Decimal::ZERO),
                });
            }
        }
        
        Ok(PortfolioSummary {
            positions,
            total_market_value: total_value,
            total_cost_basis: total_cost,
            total_unrealized_pnl: total_value.checked_sub(total_cost)
                .unwrap_or(Decimal::ZERO),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaxReport {
    pub year: i32,
    pub jurisdiction: TaxJurisdiction,
    pub short_term_gains: Decimal,
    pub long_term_gains: Decimal,
    pub total_proceeds: Decimal,
    pub total_cost_basis: Decimal,
    pub total_fees: Decimal,
    pub net_gains: Decimal,
    pub estimated_tax: Decimal,
    pub effective_rate: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarvestRecommendation {
    pub mint: Pubkey,
    pub lot_id: u64,
    pub quantity: Decimal,
    pub expected_loss: Decimal,
    pub tax_benefit: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSummary {
    pub mint: Pubkey,
    pub quantity: Decimal,
    pub cost_basis: Decimal,
    pub market_value: Decimal,
    pub unrealized_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSummary {
    pub positions: Vec<PositionSummary>,
    pub total_market_value: Decimal,
    pub total_cost_basis: Decimal,
    pub total_unrealized_pnl: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_pnl_calculation() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();
        
        let _ = calc.record_acquisition(
            mint,
            Decimal::from(100),
            Decimal::from(1000),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        ).expect("Acquisition should succeed");
        
        let disposal = calc.record_disposal(
            mint,
            Decimal::from(50),
            Decimal::from(600),
            Decimal::from(5),
        ).expect("Disposal should succeed");
        
        assert_eq!(disposal.realized_pn
        assert_eq!(disposal.realized_pnl, Decimal::from(95));
        assert!(disposal.quantity == Decimal::from(50));
    }
    
    #[test]
    fn test_wash_sale_detection() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();
        
        let _ = calc.record_acquisition(
            mint,
            Decimal::from(100),
            Decimal::from(1000),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        ).expect("First acquisition should succeed");
        
        let _ = calc.record_disposal(
            mint,
            Decimal::from(100),
            Decimal::from(900),
            Decimal::from(10),
        ).expect("Disposal should succeed");
        
        let _ = calc.record_acquisition(
            mint,
            Decimal::from(100),
            Decimal::from(950),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        ).expect("Second acquisition should succeed");
        
        let adjustments = calc.basis_adjustments.read().unwrap();
        assert!(!adjustments.is_empty());
    }
    
    #[test]
    fn test_tax_calculation() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        
        let tax = calc.calculate_tax_liability(Decimal::from(100000))
            .expect("Tax calculation should succeed");
        
        assert!(tax > Decimal::ZERO);
        assert!(tax < Decimal::from(100000));
    }
    
    #[test]
    fn test_rate_limiting() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();
        
        for i in 0..5 {
            let result = calc.record_acquisition(
                mint,
                Decimal::from(100),
                Decimal::from(1000),
                TransactionType::Arbitrage,
                Signature::default(),
                9,
            );
            
            if i > 0 {
                std::thread::sleep(std::time::Duration::from_millis(RATE_LIMIT_MS + 1));
            }
            
            assert!(result.is_ok() || matches!(result, Err(PnlError::RateLimited)));
        }
    }
    
    #[test]
    fn test_invalid_quantities() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();
        
        let result = calc.record_acquisition(
            mint,
            Decimal::from(-100),
            Decimal::from(1000),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        );
        
        assert!(matches!(result, Err(PnlError::InvalidQuantity(_))));
        
        let result2 = calc.record_acquisition(
            mint,
            Decimal::ZERO,
            Decimal::from(1000),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        );
        
        assert!(matches!(result2, Err(PnlError::InvalidQuantity(_))));
    }
    
    #[test]
    fn test_portfolio_summary() {
        let calc = PnlTaxCalculator::new(TaxMethod::HIFO, TaxJurisdiction::Singapore);
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();
        
        calc.record_acquisition(
            mint1,
            Decimal::from(100),
            Decimal::from(10000),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        ).expect("Acquisition 1 should succeed");
        
        calc.record_acquisition(
            mint2,
            Decimal::from(50),
            Decimal::from(5000),
            TransactionType::Sandwich,
            Signature::default(),
            6,
        ).expect("Acquisition 2 should succeed");
        
        calc.update_market_price(mint1, Decimal::from(110)).expect("Price update should succeed");
        calc.update_market_price(mint2, Decimal::from(95)).expect("Price update should succeed");
        
        let summary = calc.get_portfolio_summary().expect("Portfolio summary should succeed");
        
        assert_eq!(summary.positions.len(), 2);
        assert!(summary.total_unrealized_pnl != Decimal::ZERO);
    }
    
    #[test]
    fn test_performance_metrics() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();
        
        for i in 0..50 {
            std::thread::sleep(std::time::Duration::from_millis(15));
            
            calc.record_acquisition(
                mint,
                Decimal::from(10),
                Decimal::from(100 + i),
                TransactionType::Arbitrage,
                Signature::default(),
                9,
            ).expect("Acquisition should succeed");
            
            if i % 2 == 1 {
                calc.record_disposal(
                    mint,
                    Decimal::from(5),
                    Decimal::from(55 + i),
                    Decimal::from(1),
                ).expect("Disposal should succeed");
            }
        }
        
        let snapshot = calc.calculate_total_pnl().expect("PnL calculation should succeed");
        assert!(snapshot.win_rate > Decimal::ZERO);
        assert!(snapshot.sharpe_ratio != Decimal::ZERO);
    }
}

