use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::{Arc, RwLock};
use solana_sdk::pubkey::Pubkey;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use thiserror::Error;
use bincode;
use rocksdb::{DB, Options, WriteBatch};
use std::path::Path;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum TaxBasisError {
    #[error("Insufficient balance for disposal")]
    InsufficientBalance,
    #[error("Invalid disposal amount")]
    InvalidAmount,
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Lock poisoned")]
    LockPoisoned,
    #[error("Token not found")]
    TokenNotFound,
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AccountingMethod {
    FIFO,
    LIFO,
    SpecificIdentification(String),
    WeightedAverage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaxLot {
    pub lot_id: String,
    pub token_mint: Pubkey,
    pub acquisition_time: DateTime<Utc>,
    pub quantity: Decimal,
    pub cost_basis_per_unit: Decimal,
    pub total_cost_basis: Decimal,
    pub disposal_time: Option<DateTime<Utc>>,
    pub disposal_price: Option<Decimal>,
    pub realized_pnl: Option<Decimal>,
    pub tx_signature: String,
    pub is_disposed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPosition {
    pub token_mint: Pubkey,
    pub total_quantity: Decimal,
    pub average_cost_basis: Decimal,
    pub lots: VecDeque<TaxLot>,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub last_update: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaxReport {
    pub token_mint: Pubkey,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub realized_gains: Decimal,
    pub realized_losses: Decimal,
    pub net_realized: Decimal,
    pub short_term_gains: Decimal,
    pub long_term_gains: Decimal,
    pub wash_sale_adjustments: Decimal,
}

pub struct TaxBasisTracker {
    positions: Arc<RwLock<HashMap<Pubkey, TokenPosition>>>,
    accounting_method: AccountingMethod,
    db: Option<DB>,
    long_term_threshold: i64, // seconds
    wash_sale_window: i64,    // seconds
}
    tax_filing_status: String,
}

impl TaxBasisTracker {
    pub fn new(
        accounting_method: AccountingMethod,
        db_path: Option<&str>,
    ) -> Result<Self, TaxBasisError> {
        let db = if let Some(path) = db_path {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.set_max_open_files(10000);
            opts.set_keep_log_file_num(10);
            opts.set_max_total_wal_size(64 * 1024 * 1024);
            opts.set_write_buffer_size(32 * 1024 * 1024);
            opts.set_max_write_buffer_number(3);
            opts.set_target_file_size_base(64 * 1024 * 1024);
            opts.set_level_zero_file_num_compaction_trigger(10);
            opts.set_level_zero_slowdown_writes_trigger(20);
            opts.set_level_zero_stop_writes_trigger(40);
            opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
            
            Some(DB::open(&opts, path).map_err(|e| TaxBasisError::DatabaseError(e.to_string()))?)
        } else {
            None
        };

        Ok(Self {
            positions: Arc::new(RwLock::new(HashMap::new())),
            accounting_method,
            db,
            long_term_threshold: 365 * 24 * 3600, // 1 year in seconds
            wash_sale_window: 30 * 24 * 3600,     // 30 days in seconds
        })
    }

    pub fn record_acquisition(
        &self,
        token_mint: Pubkey,
        quantity: Decimal,
        cost_per_unit: Decimal,
        tx_signature: String,
    ) -> Result<String, TaxBasisError> {
        // Input validation
        if quantity <= Decimal::ZERO {
            return Err(TaxBasisError::InvalidInput("Quantity must be positive".to_string()));
        }
        
        if cost_per_unit < Decimal::ZERO {
            return Err(TaxBasisError::InvalidInput("Cost per unit must be non-negative".to_string()));
        }
        
        if tx_signature.is_empty() {
            return Err(TaxBasisError::InvalidInput("Transaction signature cannot be empty".to_string()));
        }
        
        let lot_id = format!("{}-{}", token_mint, Uuid::new_v4().to_string());
        let total_cost = quantity.checked_mul(cost_per_unit)
            .ok_or_else(|| TaxBasisError::InvalidInput("Cost calculation overflow".to_string()))?;
        
        let tax_lot = TaxLot {
            lot_id: lot_id.clone(),
            token_mint,
            acquisition_time: Utc::now(),
            quantity,
            cost_basis_per_unit: cost_per_unit,
            total_cost_basis: total_cost,
            disposal_time: None,
            disposal_price: None,
            realized_pnl: None,
            tx_signature,
            is_disposed: false,
        };

        let mut positions = self.positions.write()
            .map_err(|_| TaxBasisError::LockPoisoned)?;

        let position = positions.entry(token_mint).or_insert_with(|| TokenPosition {
            token_mint,
            total_quantity: Decimal::ZERO,
            average_cost_basis: Decimal::ZERO,
            lots: VecDeque::new(),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            last_update: Utc::now(),
        });

        position.total_quantity += quantity;
        position.lots.push_back(tax_lot.clone());
        position.average_cost_basis = self.calculate_weighted_average(&position.lots);
        position.last_update = Utc::now();

        if let Some(ref db) = self.db {
            self.persist_position(&token_mint, position)?;
            self.persist_lot(&lot_id, &tax_lot)?;
        }

        Ok(lot_id)
    }

    pub fn record_disposal(
        &self,
        token_mint: Pubkey,
        quantity: Decimal,
        price_per_unit: Decimal,
        tx_signature: String,
    ) -> Result<Vec<(String, Decimal)>, TaxBasisError> {
        // Input validation
        if quantity <= Decimal::ZERO {
            return Err(TaxBasisError::InvalidAmount);
        }
        
        if price_per_unit < Decimal::ZERO {
            return Err(TaxBasisError::InvalidInput("Price per unit must be non-negative".to_string()));
        }
        
        if tx_signature.is_empty() {
            return Err(TaxBasisError::InvalidInput("Transaction signature cannot be empty".to_string()));
        }

        let mut positions = self.positions.write()
            .map_err(|_| TaxBasisError::LockPoisoned)?;

        let position = positions.get_mut(&token_mint)
            .ok_or(TaxBasisError::TokenNotFound)?;

        if position.total_quantity < quantity {
            return Err(TaxBasisError::InsufficientBalance);
        }

        let lots_to_dispose = self.select_lots_for_disposal(
            &mut position.lots,
            quantity,
            &self.accounting_method,
        )?;

        let mut realized_pnl_details = Vec::new();
        let mut total_realized_pnl = Decimal::ZERO;
        let mut remaining_quantity = quantity;

        for (lot_idx, disposal_quantity) in lots_to_dispose {
            let lot = &mut position.lots[lot_idx];
            
            // Calculate values with overflow protection
            let disposal_value = disposal_quantity.checked_mul(price_per_unit)
                .ok_or_else(|| TaxBasisError::InvalidInput("Disposal value calculation overflow".to_string()))?;
            let cost_basis = disposal_quantity.checked_mul(lot.cost_basis_per_unit)
                .ok_or_else(|| TaxBasisError::InvalidInput("Cost basis calculation overflow".to_string()))?;
            let realized_pnl = disposal_value.checked_sub(cost_basis)
                .ok_or_else(|| TaxBasisError::InvalidInput("Realized PNL calculation overflow".to_string()))?;

            lot.quantity = lot.quantity.checked_sub(disposal_quantity)
                .ok_or_else(|| TaxBasisError::InvalidInput("Lot quantity subtraction underflow".to_string()))?;
            lot.total_cost_basis = lot.quantity.checked_mul(lot.cost_basis_per_unit)
                .ok_or_else(|| TaxBasisError::InvalidInput("Lot cost basis calculation overflow".to_string()))?;

            if lot.quantity == Decimal::ZERO {
                lot.is_disposed = true;
                lot.disposal_time = Some(Utc::now());
                lot.disposal_price = Some(price_per_unit);
                lot.realized_pnl = Some(realized_pnl);
            }

            realized_pnl_details.push((lot.lot_id.clone(), realized_pnl));
            total_realized_pnl += realized_pnl;
            remaining_quantity -= disposal_quantity;

            if let Some(ref db) = self.db {
                self.persist_lot(&lot.lot_id, lot)?;
            }
        }

        position.lots.retain(|lot| !lot.is_disposed);
        position.total_quantity = position.total_quantity.checked_sub(quantity)
            .ok_or_else(|| TaxBasisError::InvalidInput("Position quantity subtraction underflow".to_string()))?;
        position.realized_pnl = position.realized_pnl.checked_add(total_realized_pnl)
            .ok_or_else(|| TaxBasisError::InvalidInput("Position realized PNL addition overflow".to_string()))?;
        position.average_cost_basis = self.calculate_weighted_average(&position.lots);
        position.last_update = Utc::now();

        if let Some(ref db) = self.db {
            self.persist_position(&token_mint, position)?;
            self.record_disposal_transaction(
                &token_mint,
                quantity,
                price_per_unit,
                total_realized_pnl,
                &tx_signature,
            )?;
        }

        Ok(realized_pnl_details)
    }

    fn select_lots_for_disposal(
        &self,
        lots: &mut VecDeque<TaxLot>,
        quantity: Decimal,
        method: &AccountingMethod,
    ) -> Result<Vec<(usize, Decimal)>, TaxBasisError> {
        let mut disposals = Vec::new();
        let mut remaining = quantity;

        match method {
            AccountingMethod::FIFO => {
                for (idx, lot) in lots.iter().enumerate() {
                    if remaining <= Decimal::ZERO {
                        break;
                    }
                    if !lot.is_disposed {
                        let disposal_qty = remaining.min(lot.quantity);
                        disposals.push((idx, disposal_qty));
                        remaining -= disposal_qty;
                    }
                }
            },
            AccountingMethod::LIFO => {
                for (idx, lot) in lots.iter().enumerate().rev() {
                    if remaining <= Decimal::ZERO {
                        break;
                    }
                    if !lot.is_disposed {
                        let disposal_qty = remaining.min(lot.quantity);
                        disposals.push((idx, disposal_qty));
                        remaining -= disposal_qty;
                    }
                }
            },
            AccountingMethod::SpecificIdentification(lot_id) => {
                if let Some((idx, lot)) = lots.iter().enumerate()
                    .find(|(_, l)| l.lot_id == *lot_id && !l.is_disposed) {
                    let disposal_qty = remaining.min(lot.quantity);
                    disposals.push((idx, disposal_qty));
                    remaining -= disposal_qty;
                }
            },
            AccountingMethod::WeightedAverage => {
                let avg_cost = self.calculate_weighted_average(lots);
                for (idx, lot) in lots.iter_mut().enumerate() {
                    if remaining <= Decimal::ZERO {
                        break;
                    }
                    if !lot.is_disposed {
                        lot.cost_basis_per_unit = avg_cost;
                        let disposal_qty = remaining.min(lot.quantity);
                        disposals.push((idx, disposal_qty));
                        remaining -= disposal_qty;
                    }
                }
            },
        }

        if remaining > Decimal::ZERO {
            return Err(TaxBasisError::InsufficientBalance);
        }

        Ok(disposals)
    }

    fn calculate_weighted_average(&self, lots: &VecDeque<TaxLot>) -> Decimal {
        let mut total_cost = Decimal::ZERO;
        let mut total_quantity = Decimal::ZERO;

        for lot in lots.iter() {
            if !lot.is_disposed {
                total_cost = total_cost.checked_add(lot.total_cost_basis)
                    .unwrap_or_else(|_| Decimal::ZERO); // In case of overflow, we return ZERO
                total_quantity = total_quantity.checked_add(lot.quantity)
                    .unwrap_or_else(|_| Decimal::ZERO); // In case of overflow, we return ZERO
            }
        }

        if total_quantity > Decimal::ZERO {
            total_cost.checked_div(total_quantity)
                .unwrap_or_else(|_| Decimal::ZERO) // In case of division error, we return ZERO
        } else {
            Decimal::ZERO
        }
    }

    pub fn get_position(&self, token_mint: &Pubkey) -> Result<Option<TokenPosition>, TaxBasisError> {
        let positions = self.positions.read()
            .map_err(|_| TaxBasisError::LockPoisoned)?;
        Ok(positions.get(token_mint).cloned())
    }

    pub fn calculate_unrealized_pnl(
        &self,
        token_mint: &Pubkey,
        current_price: Decimal,
    ) -> Result<Decimal, TaxBasisError> {
        let mut positions = self.positions.write()
            .map_err(|_| TaxBasisError::LockPoisoned)?;

        if let Some(position) = positions.get_mut(token_mint) {
            let market_value = position.total_quantity.checked_mul(current_price)
                .ok_or_else(|| TaxBasisError::InvalidInput("Market value calculation overflow".to_string()))?;
            let cost_basis = position.lots.iter()
                .filter(|lot| !lot.is_disposed)
                .map(|lot| lot.total_cost_basis)
                .try_fold(Decimal::ZERO, |acc, cost| acc.checked_add(cost))
                .ok_or_else(|| TaxBasisError::InvalidInput("Cost basis calculation overflow".to_string()))?;
            
            position.unrealized_pnl = market_value.checked_sub(cost_basis)
                .ok_or_else(|| TaxBasisError::InvalidInput("Unrealized PNL calculation overflow".to_string()))?;
            position.last_update = Utc::now();
            
            if let Some(ref db) = self.db {
                self.persist_position(token_mint, position)?;
            }
            
            Ok(position.unrealized_pnl)
        } else {
            Ok(Decimal::ZERO)
        }
    }

    pub fn generate_tax_report(
        &self,
        token_mint: &Pubkey,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<TaxReport, TaxBasisError> {
        let positions = self.positions.read()
            .map_err(|_| TaxBasisError::LockPoisoned)?;

        let mut report = TaxReport {
            token_mint: *token_mint,
            period_start: start_date,
            period_end: end_date,
            realized_gains: Decimal::ZERO,
                        realized_losses: Decimal::ZERO,
            net_realized: Decimal::ZERO,
            short_term_gains: Decimal::ZERO,
            long_term_gains: Decimal::ZERO,
            wash_sale_adjustments: Decimal::ZERO,
        };

        if let Some(position) = positions.get(token_mint) {
            let long_term_cutoff = Utc::now().checked_sub_signed(chrono::Duration::seconds(self.long_term_threshold))
                .ok_or_else(|| TaxBasisError::InvalidInput("Invalid long term threshold".to_string()))?;
            
            for lot in &position.lots {
                if let Some(disposal_time) = lot.disposal_time {
                    if disposal_time >= start_date && disposal_time <= end_date {
                        if let Some(pnl) = lot.realized_pnl {
                            if pnl > Decimal::ZERO {
                                report.realized_gains += pnl;
                                if lot.acquisition_time < long_term_cutoff {
                                    report.long_term_gains += pnl;
                                } else {
                                    report.short_term_gains += pnl;
                                }
                            } else {
                                report.realized_losses += pnl.abs();
                            }
                        }
                    }
                }
            }
            
            report.net_realized = report.realized_gains - report.realized_losses;
            report.wash_sale_adjustments = self.calculate_wash_sales(position, start_date, end_date)?;
        }

        Ok(report)
    }

    fn calculate_wash_sales(
        &self,
        position: &TokenPosition,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<Decimal, TaxBasisError> {
        let mut wash_sale_adjustment = Decimal::ZERO;
        let wash_window = chrono::Duration::seconds(self.wash_sale_window);

        for (i, lot) in position.lots.iter().enumerate() {
            if let Some(disposal_time) = lot.disposal_time {
                if disposal_time >= start_date && disposal_time <= end_date {
                    if let Some(loss) = lot.realized_pnl {
                        if loss < Decimal::ZERO {
                            let wash_start = disposal_time.checked_sub_signed(wash_window)
                                .ok_or_else(|| TaxBasisError::InvalidInput("Invalid wash sale window start".to_string()))?;
                            let wash_end = disposal_time.checked_add_signed(wash_window)
                                .ok_or_else(|| TaxBasisError::InvalidInput("Invalid wash sale window end".to_string()))?;
                            
                            for (j, other_lot) in position.lots.iter().enumerate() {
                                if i != j && other_lot.acquisition_time >= wash_start 
                                    && other_lot.acquisition_time <= wash_end {
                                    wash_sale_adjustment += loss.abs();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(wash_sale_adjustment)
    }

    fn persist_position(&self, token_mint: &Pubkey, position: &TokenPosition) -> Result<(), TaxBasisError> {
        if let Some(ref db) = self.db {
            let key = format!("position:{}", token_mint);
            let value = bincode::serialize(position)
                .map_err(|e| TaxBasisError::SerializationError(e.to_string()))?;
            db.put(key.as_bytes(), &value)
                .map_err(|e| TaxBasisError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn persist_lot(&self, lot_id: &str, lot: &TaxLot) -> Result<(), TaxBasisError> {
        if let Some(ref db) = self.db {
            let key = format!("lot:{}", lot_id);
            let value = bincode::serialize(lot)
                .map_err(|e| TaxBasisError::SerializationError(e.to_string()))?;
            db.put(key.as_bytes(), &value)
                .map_err(|e| TaxBasisError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn record_disposal_transaction(
        &self,
        token_mint: &Pubkey,
        quantity: Decimal,
        price: Decimal,
        realized_pnl: Decimal,
        tx_signature: &str,
    ) -> Result<(), TaxBasisError> {
        if let Some(ref db) = self.db {
            let key = format!("disposal:{}:{}", token_mint, Utc::now().timestamp_nanos());
            let disposal_record = serde_json::json!({
                "token_mint": token_mint.to_string(),
                "quantity": quantity.to_string(),
                "price": price.to_string(),
                "realized_pnl": realized_pnl.to_string(),
                "tx_signature": tx_signature,
                "timestamp": Utc::now().to_rfc3339()
            });
            
            db.put(key.as_bytes(), disposal_record.to_string().as_bytes())
                .map_err(|e| TaxBasisError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    pub fn load_from_db(&self) -> Result<(), TaxBasisError> {
        if let Some(ref db) = self.db {
            let mut positions = self.positions.write()
                .map_err(|_| TaxBasisError::LockPoisoned)?;
            
            let iter = db.iterator(rocksdb::IteratorMode::Start);
            for (key, value) in iter {
                let key_str = String::from_utf8_lossy(&key);
                if key_str.starts_with("position:") {
                    let position: TokenPosition = bincode::deserialize(&value)
                        .map_err(|e| TaxBasisError::SerializationError(e.to_string()))?;
                    positions.insert(position.token_mint, position);
                }
            }
        }
        Ok(())
    }

    pub fn export_lots_csv(&self, token_mint: &Pubkey) -> Result<String, TaxBasisError> {
        let positions = self.positions.read()
            .map_err(|_| TaxBasisError::LockPoisoned)?;
        
        let mut csv_output = String::from(
            "lot_id,acquisition_time,quantity,cost_basis_per_unit,total_cost_basis,disposal_time,disposal_price,realized_pnl,tx_signature\n"
        );
        
        if let Some(position) = positions.get(token_mint) {
            for lot in &position.lots {
                csv_output.push_str(&format!(
                    "{},{},{},{},{},{},{},{},{}\n",
                    lot.lot_id,
                    lot.acquisition_time.to_rfc3339(),
                    lot.quantity,
                    lot.cost_basis_per_unit,
                    lot.total_cost_basis,
                    lot.disposal_time.map(|dt| dt.to_rfc3339()).unwrap_or_default(),
                    lot.disposal_price.map(|p| p.to_string()).unwrap_or_default(),
                    lot.realized_pnl.map(|p| p.to_string()).unwrap_or_default(),
                    lot.tx_signature
                ));
            }
        }
        
        Ok(csv_output)
    }

    pub fn get_total_realized_pnl(&self, token_mint: &Pubkey) -> Result<Decimal, TaxBasisError> {
        let positions = self.positions.read()
            .map_err(|_| TaxBasisError::LockPoisoned)?;
        
        Ok(positions.get(token_mint)
            .map(|p| p.realized_pnl)
            .unwrap_or(Decimal::ZERO))
    }

    pub fn get_all_positions(&self) -> Result<Vec<TokenPosition>, TaxBasisError> {
        let positions = self.positions.read()
            .map_err(|_| TaxBasisError::LockPoisoned)?;
        
        Ok(positions.values().cloned().collect())
    }

    pub fn optimize_tax_disposal(
        &self,
        token_mint: &Pubkey,
        target_amount: Decimal,
        minimize_gains: bool,
    ) -> Result<Vec<(String, Decimal)>, TaxBasisError> {
        let positions = self.positions.read()
            .map_err(|_| TaxBasisError::LockPoisoned)?;
        
        if let Some(position) = positions.get(token_mint) {
            let mut lot_candidates: Vec<_> = position.lots.iter()
                .filter(|lot| !lot.is_disposed)
                .collect();
            
            if minimize_gains {
                lot_candidates.sort_by(|a, b| b.cost_basis_per_unit.cmp(&a.cost_basis_per_unit));
            } else {
                lot_candidates.sort_by(|a, b| a.cost_basis_per_unit.cmp(&b.cost_basis_per_unit));
            }
            
            let mut selections = Vec::new();
            let mut remaining = target_amount;
            
            for lot in lot_candidates {
                if remaining <= Decimal::ZERO {
                    break;
                }
                
                let disposal_qty = remaining.min(lot.quantity);
                selections.push((lot.lot_id.clone(), disposal_qty));
                remaining -= disposal_qty;
            }
            
            if remaining > Decimal::ZERO {
                return Err(TaxBasisError::InsufficientBalance);
            }
            
            Ok(selections)
        } else {
            Err(TaxBasisError::TokenNotFound)
        }
    }

    pub fn compact_database(&self) -> Result<(), TaxBasisError> {
        if let Some(ref db) = self.db {
            db.compact_range(None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }

    pub fn get_cost_basis_for_quantity(
        &self,
        token_mint: &Pubkey,
        quantity: Decimal,
    ) -> Result<Decimal, TaxBasisError> {
        let positions = self.positions.read()
            .map_err(|_| TaxBasisError::LockPoisoned)?;
        
        if let Some(position) = positions.get(token_mint) {
            let mut total_cost = Decimal::ZERO;
            let mut remaining = quantity;
            
            match self.accounting_method {
                AccountingMethod::FIFO => {
                    for lot in &position.lots {
                        if remaining <= Decimal::ZERO {
                            break;
                        }
                        if !lot.is_disposed {
                            let use_qty = remaining.min(lot.quantity);
                            total_cost += use_qty * lot.cost_basis_per_unit;
                            remaining -= use_qty;
                        }
                    }
                },
                AccountingMethod::WeightedAverage => {
                    total_cost = quantity * position.average_cost_basis;
                },
                _ => {
                    for lot in &position.lots {
                        if remaining <= Decimal::ZERO {
                            break;
                        }
                        if !lot.is_disposed {
                            let use_qty = remaining.min(lot.quantity);
                            total_cost += use_qty * lot.cost_basis_per_unit;
                            remaining -= use_qty;
                        }
                    }
                }
            }
            
            if remaining > Decimal::ZERO {
                return Err(TaxBasisError::InsufficientBalance);
            }
            
            Ok(total_cost)
        } else {
            Err(TaxBasisError::TokenNotFound)
        }
    }

    pub fn flush_to_disk(&self) -> Result<(), TaxBasisError> {
        if let Some(ref db) = self.db {
            db.flush().map_err(|e| TaxBasisError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fifo_disposal() {
        let tracker = TaxBasisTracker::new(AccountingMethod::FIFO, None).unwrap();
        let token_mint = Pubkey::new_unique();
        
        tracker.record_acquisition(token_mint, dec!(100), dec!(10), "sig1".to_string()).unwrap();
        tracker.record_acquisition(token_mint, dec!(100), dec!(20), "sig2".to_string()).unwrap();
        
        let disposals = tracker.record_disposal(token_mint, dec!(150), dec!(25), "sig3".to_string()).unwrap();
        
        assert_eq!(disposals.len(), 2);
        assert_eq!(disposals[0].1, dec!(1500));
        assert_eq!(disposals[1].1, dec!(250));
    }
}
