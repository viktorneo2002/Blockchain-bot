use anyhow::{anyhow, Result};
use dashmap::DashMap;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
    instruction::AccountMeta,
};
use solana_account_decoder::{UiAccountEncoding, parse_token::parse_token};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
    str::FromStr,
};
use tokio::{
    sync::{broadcast, Mutex, RwLock},
    time::{interval, sleep, timeout},
};
use borsh::{BorshDeserialize, BorshSerialize};

const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PYTH_PROGRAM: &str = "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH";
const JUPITER_V6_PROGRAM: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

const PRECISION_SCALE: u32 = 9;
const MAX_HISTORY_SIZE: usize = 50000;
const METRICS_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
const PRICE_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(300);
const MAX_RETRY_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(100);
const POSITION_CHECK_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionPnL {
    pub signature: String,
    pub timestamp: u64,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub amount_in: Decimal,
    pub amount_out: Decimal,
    pub gas_fee: Decimal,
    pub priority_fee: Decimal,
    pub gross_profit: Decimal,
    pub net_profit: Decimal,
    pub profit_percentage: Decimal,
    pub execution_time_ms: u64,
    pub tx_type: TransactionType,
    pub status: TxStatus,
    pub slippage_cost: Decimal,
    pub mev_profit: Decimal,
    pub bundle_index: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TransactionType {
    Arbitrage,
    Liquidation,
    JitSandwich,
    FrontRun,
    BackRun,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum TxStatus {
    Pending,
    Success,
    Failed,
    Reverted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub total_transactions: u64,
    pub successful_transactions: u64,
    pub failed_transactions: u64,
    pub win_rate: Decimal,
    pub average_profit: Decimal,
    pub total_gross_profit: Decimal,
    pub total_net_profit: Decimal,
    pub total_gas_spent: Decimal,
    pub total_mev_profit: Decimal,
    pub largest_win: Decimal,
    pub largest_loss: Decimal,
    pub sharpe_ratio: Decimal,
    pub sortino_ratio: Decimal,
    pub profit_factor: Decimal,
    pub max_drawdown: Decimal,
    pub current_drawdown: Decimal,
    pub calmar_ratio: Decimal,
    pub current_streak: i32,
    pub best_streak: i32,
    pub worst_streak: i32,
    pub avg_execution_time_ms: u64,
    pub success_rate_by_type: HashMap<TransactionType, Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPosition {
    pub token: Pubkey,
    pub balance: Decimal,
    pub avg_entry_price: Decimal,
    pub current_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub last_update: u64,
    pub total_volume: Decimal,
    pub position_opened: u64,
}

#[derive(Debug, Clone)]
pub struct PriceData {
    pub price: Decimal,
    pub confidence: Decimal,
    pub slot: u64,
    pub timestamp: u64,
}

pub struct RealTimePnLDashboard {
    rpc_client: Arc<RpcClient>,
    transactions: Arc<DashMap<String, TransactionPnL>>,
    transaction_history: Arc<Mutex<VecDeque<TransactionPnL>>>,
    positions: Arc<DashMap<Pubkey, TokenPosition>>,
    metrics: Arc<RwLock<PerformanceMetrics>>,
    price_feeds: Arc<DashMap<Pubkey, PriceData>>,
    running: Arc<AtomicBool>,
    last_update: Arc<AtomicU64>,
    profit_targets: Arc<RwLock<ProfitTargets>>,
    risk_params: Arc<RwLock<RiskParameters>>,
    event_sender: Arc<broadcast::Sender<DashboardEvent>>,
    peak_balance: Arc<RwLock<Decimal>>,
    daily_returns: Arc<Mutex<VecDeque<Decimal>>>,
    oracle_accounts: Arc<HashMap<Pubkey, Pubkey>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfitTargets {
    pub daily_target: Decimal,
    pub weekly_target: Decimal,
    pub monthly_target: Decimal,
    pub stop_loss: Decimal,
    pub trailing_stop_percent: Decimal,
    pub take_profit_levels: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskParameters {
    pub max_position_size: Decimal,
    pub max_drawdown_percent: Decimal,
    pub max_gas_per_tx: Decimal,
    pub min_profit_threshold: Decimal,
    pub position_sizing_kelly: Decimal,
    pub max_correlation: Decimal,
    pub var_confidence: Decimal,
}

#[derive(Debug, Clone)]
pub enum DashboardEvent {
    ProfitTarget(Decimal),
    StopLoss(Decimal),
    RiskLimit(String),
    NewHighWaterMark(Decimal),
    PositionAlert(Pubkey, String),
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            total_transactions: 0,
            successful_transactions: 0,
            failed_transactions: 0,
            win_rate: Decimal::ZERO,
            average_profit: Decimal::ZERO,
            total_gross_profit: Decimal::ZERO,
            total_net_profit: Decimal::ZERO,
            total_gas_spent: Decimal::ZERO,
            total_mev_profit: Decimal::ZERO,
            largest_win: Decimal::ZERO,
            largest_loss: Decimal::ZERO,
            sharpe_ratio: Decimal::ZERO,
            sortino_ratio: Decimal::ZERO,
            profit_factor: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            current_drawdown: Decimal::ZERO,
            calmar_ratio: Decimal::ZERO,
            current_streak: 0,
            best_streak: 0,
            worst_streak: 0,
            avg_execution_time_ms: 0,
            success_rate_by_type: HashMap::new(),
        }
    }
}

impl RealTimePnLDashboard {
    pub async fn new(rpc_url: &str) -> Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        let (event_sender, _) = broadcast::channel(1000);
        
        let mut oracle_accounts = HashMap::new();
        oracle_accounts.insert(
            Pubkey::from_str(SOL_MINT)?,
            Pubkey::from_str("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG")?
        );
        oracle_accounts.insert(
            Pubkey::from_str(USDC_MINT)?,
            Pubkey::from_str("Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD")?
        );

        Ok(Self {
            rpc_client,
            transactions: Arc::new(DashMap::new()),
            transaction_history: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_HISTORY_SIZE))),
            positions: Arc::new(DashMap::new()),
            metrics: Arc::new(RwLock::new(PerformanceMetrics::default())),
            price_feeds: Arc::new(DashMap::new()),
            running: Arc::new(AtomicBool::new(false)),
            last_update: Arc::new(AtomicU64::new(0)),
            profit_targets: Arc::new(RwLock::new(ProfitTargets {
                daily_target: Decimal::from_str("100").unwrap(),
                weekly_target: Decimal::from_str("500").unwrap(),
                monthly_target: Decimal::from_str("2000").unwrap(),
                stop_loss: Decimal::from_str("-50").unwrap(),
                trailing_stop_percent: Decimal::from_str("0.05").unwrap(),
                take_profit_levels: vec![
                    (Decimal::from_str("50").unwrap(), Decimal::from_str("0.25").unwrap()),
                    (Decimal::from_str("100").unwrap(), Decimal::from_str("0.5").unwrap()),
                ],
            })),
            risk_params: Arc::new(RwLock::new(RiskParameters {
                max_position_size: Decimal::from_str("1000").unwrap(),
                max_drawdown_percent: Decimal::from_str("0.1").unwrap(),
                max_gas_per_tx: Decimal::from_str("0.1").unwrap(),
                min_profit_threshold: Decimal::from_str("0.001").unwrap(),
                position_sizing_kelly: Decimal::from_str("0.25").unwrap(),
                max_correlation: Decimal::from_str("0.7").unwrap(),
                var_confidence: Decimal::from_str("0.95").unwrap(),
            })),
            event_sender: Arc::new(event_sender),
            peak_balance: Arc::new(RwLock::new(Decimal::ZERO)),
            daily_returns: Arc::new(Mutex::new(VecDeque::with_capacity(365))),
            oracle_accounts: Arc::new(oracle_accounts),
        })
    }

    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(anyhow!("Dashboard already running"));
        }

        let metrics_handle = self.spawn_metrics_updater();
        let price_handle = self.spawn_price_updater();
        let cleanup_handle = self.spawn_cleanup_task();
        let monitor_handle = self.spawn_position_monitor();
        let risk_handle = self.spawn_risk_monitor();

        tokio::spawn(async move {
            let _ = tokio::join!(
                metrics_handle,
                price_handle,
                cleanup_handle,
                monitor_handle,
                risk_handle
            );
        });

        Ok(())
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub async fn record_transaction(&self, tx: TransactionPnL) -> Result<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(anyhow!("Dashboard not running"));
        }

        let signature = tx.signature.clone();
        
        let mut metrics = self.metrics.write().await;
        self.update_metrics(&mut metrics, &tx);
        drop(metrics);

        self.transactions.insert(signature.clone(), tx.clone());
        
        let mut history = self.transaction_history.lock().await;
        history.push_back(tx.clone());
        if history.len() > MAX_HISTORY_SIZE {
            history.pop_front();
        }
        drop(history);

        self.update_position(&tx).await?;
        self.check_risk_limits(&tx).await?;
        self.update_daily_returns(&tx).await?;
        
        self.last_update.store(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            Ordering::SeqCst
        );

        Ok(())
    }

    fn update_metrics(&self, metrics: &mut PerformanceMetrics, tx: &TransactionPnL) {
        metrics.total_transactions += 1;
        
        match tx.status {
            TxStatus::Success => {
                metrics.successful_transactions += 1;
                metrics.total_gross_profit = metrics.total_gross_profit + tx.gross_profit;
                metrics.total_net_profit = metrics.total_net_profit + tx.net_profit;
                metrics.total_mev_profit = metrics.total_mev_profit + tx.mev_profit;
                
                let type_count = metrics.success_rate_by_type
                                        .entry(tx.tx_type)
                    .or_insert(Decimal::ZERO);
                *type_count += Decimal::ONE;
                
                if tx.net_profit > Decimal::ZERO {
                    metrics.current_streak = metrics.current_streak.max(0) + 1;
                    metrics.best_streak = metrics.best_streak.max(metrics.current_streak);
                    
                    if tx.net_profit > metrics.largest_win {
                        metrics.largest_win = tx.net_profit;
                    }
                } else {
                    metrics.current_streak = metrics.current_streak.min(0) - 1;
                    metrics.worst_streak = metrics.worst_streak.min(metrics.current_streak);
                    
                    if tx.net_profit < metrics.largest_loss {
                        metrics.largest_loss = tx.net_profit;
                    }
                }
                
                let total_time = metrics.avg_execution_time_ms * (metrics.successful_transactions - 1) + tx.execution_time_ms;
                metrics.avg_execution_time_ms = total_time / metrics.successful_transactions;
            }
            TxStatus::Failed | TxStatus::Reverted => {
                metrics.failed_transactions += 1;
                metrics.current_streak = metrics.current_streak.min(0) - 1;
                metrics.worst_streak = metrics.worst_streak.min(metrics.current_streak);
                metrics.total_gas_spent = metrics.total_gas_spent + tx.gas_fee;
            }
            _ => {}
        }
        
        metrics.total_gas_spent = metrics.total_gas_spent + tx.gas_fee + tx.priority_fee;
        
        if metrics.total_transactions > 0 {
            metrics.win_rate = Decimal::from(metrics.successful_transactions) 
                / Decimal::from(metrics.total_transactions);
            
            if metrics.successful_transactions > 0 {
                metrics.average_profit = metrics.total_net_profit 
                    / Decimal::from(metrics.successful_transactions);
            }
        }
        
        self.calculate_advanced_metrics(metrics);
    }

    fn calculate_advanced_metrics(&self, metrics: &mut PerformanceMetrics) {
        if metrics.successful_transactions < 2 {
            return;
        }

        let total_wins = metrics.total_gross_profit.max(Decimal::ZERO);
        let total_losses = metrics.total_gross_profit.min(Decimal::ZERO).abs();
        
        if total_losses > Decimal::ZERO {
            metrics.profit_factor = total_wins / total_losses;
        } else if total_wins > Decimal::ZERO {
            metrics.profit_factor = Decimal::from(1000);
        }

        let history_fut = self.transaction_history.try_lock();
        if let Ok(history) = history_fut {
            let returns: Vec<Decimal> = history.iter()
                .filter(|tx| tx.status == TxStatus::Success)
                .map(|tx| tx.net_profit)
                .collect();
            
            if returns.len() > 20 {
                let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
                let variance = returns.iter()
                    .map(|r| (*r - mean_return).powu(2))
                    .sum::<Decimal>() / Decimal::from(returns.len() - 1);
                
                if variance > Decimal::ZERO {
                    let std_dev = variance.sqrt().unwrap_or(Decimal::ONE);
                    let risk_free_rate = Decimal::from_str("0.02").unwrap() / Decimal::from(365);
                    metrics.sharpe_ratio = (mean_return - risk_free_rate) / std_dev;
                    
                    let downside_returns: Vec<Decimal> = returns.iter()
                        .filter(|&&r| r < mean_return)
                        .copied()
                        .collect();
                    
                    if !downside_returns.is_empty() {
                        let downside_variance = downside_returns.iter()
                            .map(|r| (*r - mean_return).powu(2))
                            .sum::<Decimal>() / Decimal::from(downside_returns.len());
                        
                        if downside_variance > Decimal::ZERO {
                            let downside_dev = downside_variance.sqrt().unwrap_or(Decimal::ONE);
                            metrics.sortino_ratio = (mean_return - risk_free_rate) / downside_dev;
                        }
                    }
                }
                
                let mut cumulative_returns = vec![Decimal::ZERO];
                let mut peak = Decimal::ZERO;
                let mut max_dd = Decimal::ZERO;
                
                for (i, ret) in returns.iter().enumerate() {
                    let cum_ret = cumulative_returns[i] + ret;
                    cumulative_returns.push(cum_ret);
                    
                    if cum_ret > peak {
                        peak = cum_ret;
                    }
                    
                    let drawdown = if peak > Decimal::ZERO {
                        (peak - cum_ret) / peak
                    } else {
                        Decimal::ZERO
                    };
                    
                    if drawdown > max_dd {
                        max_dd = drawdown;
                    }
                }
                
                metrics.max_drawdown = max_dd;
                metrics.current_drawdown = if peak > Decimal::ZERO {
                    (peak - cumulative_returns.last().unwrap_or(&Decimal::ZERO)) / peak
                } else {
                    Decimal::ZERO
                };
                
                if metrics.max_drawdown > Decimal::ZERO && returns.len() >= 252 {
                    let annual_return = mean_return * Decimal::from(252);
                    metrics.calmar_ratio = annual_return / metrics.max_drawdown;
                }
            }
        }
    }

    async fn update_position(&self, tx: &TransactionPnL) -> Result<()> {
        if tx.status != TxStatus::Success {
            return Ok(());
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        
        let mut out_position = self.positions.entry(tx.token_out).or_insert_with(|| {
            TokenPosition {
                token: tx.token_out,
                balance: Decimal::ZERO,
                avg_entry_price: Decimal::ZERO,
                current_price: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                last_update: now,
                total_volume: Decimal::ZERO,
                position_opened: now,
            }
        });

        let old_balance = out_position.balance;
        let new_balance = old_balance + tx.amount_out;
        out_position.total_volume += tx.amount_out;
        
        if new_balance > Decimal::ZERO && tx.amount_in > Decimal::ZERO && tx.amount_out > Decimal::ZERO {
            let out_price = self.get_token_price(&tx.token_out).await?;
            let in_price = self.get_token_price(&tx.token_in).await?;
            let entry_price = (tx.amount_in * in_price) / tx.amount_out;
            
            if old_balance > Decimal::ZERO {
                out_position.avg_entry_price = 
                    (out_position.avg_entry_price * old_balance + entry_price * tx.amount_out) 
                    / new_balance;
            } else {
                out_position.avg_entry_price = entry_price;
                out_position.position_opened = now;
            }
        }
        
        out_position.balance = new_balance;
        out_position.current_price = self.get_token_price(&tx.token_out).await?;
        out_position.last_update = now;

        let mut in_position = self.positions.entry(tx.token_in).or_insert_with(|| {
            TokenPosition {
                token: tx.token_in,
                balance: Decimal::ZERO,
                avg_entry_price: Decimal::ZERO,
                current_price: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                last_update: now,
                total_volume: Decimal::ZERO,
                position_opened: now,
            }
        });

        let old_in_balance = in_position.balance;
        let new_in_balance = old_in_balance - tx.amount_in;
        in_position.total_volume += tx.amount_in;
        
        if old_in_balance > Decimal::ZERO && tx.amount_in > Decimal::ZERO {
            let ratio = tx.amount_in / old_in_balance;
            let realized = ratio * (in_position.current_price - in_position.avg_entry_price) * tx.amount_in;
            in_position.realized_pnl += realized;
        }
        
        in_position.balance = new_in_balance;
        in_position.current_price = self.get_token_price(&tx.token_in).await?;
        in_position.last_update = now;

        Ok(())
    }

    async fn check_risk_limits(&self, tx: &TransactionPnL) -> Result<()> {
        let metrics = self.metrics.read().await;
        let risk_params = self.risk_params.read().await;
        let profit_targets = self.profit_targets.read().await;

        if metrics.total_net_profit < profit_targets.stop_loss {
            let _ = self.event_sender.send(DashboardEvent::StopLoss(metrics.total_net_profit));
            return Err(anyhow!("Stop loss triggered: {}", metrics.total_net_profit));
        }

        if metrics.current_drawdown > risk_params.max_drawdown_percent {
            let _ = self.event_sender.send(DashboardEvent::RiskLimit(
                format!("Max drawdown exceeded: {}", metrics.current_drawdown)
            ));
        }

        if tx.gas_fee + tx.priority_fee > risk_params.max_gas_per_tx {
            return Err(anyhow!("Gas fee exceeded limit: {}", tx.gas_fee + tx.priority_fee));
        }

        let total_position_value = self.calculate_total_position_value().await?;
        if total_position_value > risk_params.max_position_size {
            let _ = self.event_sender.send(DashboardEvent::RiskLimit(
                format!("Position size limit exceeded: {}", total_position_value)
            ));
        }

        let mut peak = self.peak_balance.write().await;
        if metrics.total_net_profit > *peak {
            *peak = metrics.total_net_profit;
            let _ = self.event_sender.send(DashboardEvent::NewHighWaterMark(*peak));
        }

        Ok(())
    }

    async fn update_daily_returns(&self, tx: &TransactionPnL) -> Result<()> {
        if tx.status != TxStatus::Success {
            return Ok(());
        }

        let mut returns = self.daily_returns.lock().await;
        returns.push_back(tx.net_profit);
        
        if returns.len() > 365 {
            returns.pop_front();
        }
        
        Ok(())
    }

    async fn calculate_total_position_value(&self) -> Result<Decimal> {
        let mut total_value = Decimal::ZERO;
        
        for position in self.positions.iter() {
            let price = self.get_token_price(&position.token).await?;
            total_value += position.balance.abs() * price;
        }
        
        Ok(total_value)
    }

    async fn get_token_price(&self, token: &Pubkey) -> Result<Decimal> {
        if let Some(price_data) = self.price_feeds.get(token) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
            if now - price_data.timestamp < 1000 {
                return Ok(price_data.price);
            }
        }

        let price = self.fetch_pyth_price(token).await?;
        Ok(price)
    }

    async fn fetch_pyth_price(&self, token: &Pubkey) -> Result<Decimal> {
        let oracle_account = self.oracle_accounts.get(token)
            .ok_or_else(|| anyhow!("No oracle for token {}", token))?;

        let mut attempts = 0;
        loop {
            match self.fetch_pyth_price_internal(oracle_account).await {
                Ok(price_data) => {
                    self.price_feeds.insert(*token, price_data.clone());
                    return Ok(price_data.price);
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= MAX_RETRY_ATTEMPTS {
                        return Err(anyhow!("Failed to fetch price: {}", e));
                    }
                    sleep(RETRY_DELAY * attempts).await;
                }
            }
        }
    }

    async fn fetch_pyth_price_internal(&self, oracle: &Pubkey) -> Result<PriceData> {
        let account_data = self.rpc_client.get_account_data(oracle).await?;
        
        if account_data.len() < 48 {
            return Err(anyhow!("Invalid price account data"));
        }

        let price_i64 = i64::from_le_bytes(account_data[208..216].try_into()?);
        let expo_i32 = i32::from_le_bytes(account_data[224..228].try_into()?);
        let conf_u64 = u64::from_le_bytes(account_data[216..224].try_into()?);
        let slot = u64::from_le_bytes(account_data[240..248].try_into()?);
        
        let price = Decimal::from(price_i64) / Decimal::from(10_i64.pow((-expo_i32) as u32));
        let confidence = Decimal::from(conf_u64) / Decimal::from(10_i64.pow((-expo_i32) as u32));
        
        Ok(PriceData {
            price,
            confidence,
            slot,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
        })
    }

    fn spawn_metrics_updater(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let positions = self.positions.clone();
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
                        let mut interval = interval(METRICS_UPDATE_INTERVAL);
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                for mut position in positions.iter_mut() {
                    if position.balance > Decimal::ZERO && position.avg_entry_price > Decimal::ZERO {
                        position.unrealized_pnl = position.balance * 
                            (position.current_price - position.avg_entry_price);
                    }
                }
                
                let mut metrics_write = metrics.write().await;
                for (tx_type, count) in metrics_write.success_rate_by_type.iter_mut() {
                    if metrics_write.total_transactions > 0 {
                        *count = *count / Decimal::from(metrics_write.total_transactions);
                    }
                }
            }
        })
    }

    fn spawn_price_updater(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let positions = self.positions.clone();
        let rpc_client = self.rpc_client.clone();
        let oracle_accounts = self.oracle_accounts.clone();
        let price_feeds = self.price_feeds.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(PRICE_UPDATE_INTERVAL);
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                let active_tokens: Vec<Pubkey> = positions.iter()
                    .filter(|p| p.balance.abs() > Decimal::ZERO)
                    .map(|p| p.token)
                    .collect();
                
                for token in active_tokens {
                    if let Some(oracle) = oracle_accounts.get(&token) {
                        match Self::fetch_price_static(&rpc_client, oracle).await {
                            Ok(price_data) => {
                                price_feeds.insert(token, price_data);
                                if let Some(mut pos) = positions.get_mut(&token) {
                                    pos.current_price = price_data.price;
                                }
                            }
                            Err(_) => continue,
                        }
                    }
                }
            }
        })
    }

    async fn fetch_price_static(rpc_client: &RpcClient, oracle: &Pubkey) -> Result<PriceData> {
        let account_data = rpc_client.get_account_data(oracle).await?;
        
        if account_data.len() < 248 {
            return Err(anyhow!("Invalid price account data"));
        }

        let price_i64 = i64::from_le_bytes(account_data[208..216].try_into()?);
        let expo_i32 = i32::from_le_bytes(account_data[224..228].try_into()?);
        let conf_u64 = u64::from_le_bytes(account_data[216..224].try_into()?);
        let slot = u64::from_le_bytes(account_data[240..248].try_into()?);
        
        let price = Decimal::from(price_i64) / Decimal::from(10_i64.pow((-expo_i32) as u32));
        let confidence = Decimal::from(conf_u64) / Decimal::from(10_i64.pow((-expo_i32) as u32));
        
        Ok(PriceData {
            price,
            confidence,
            slot,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
        })
    }

    fn spawn_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let transactions = self.transactions.clone();
        let price_feeds = self.price_feeds.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(CLEANUP_INTERVAL);
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                let now = SystemTime::now().duration_since(UNIX_EPOCH)
                    .unwrap_or_default().as_millis() as u64;
                
                transactions.retain(|_, tx| {
                    now - tx.timestamp < 86400000
                });
                
                price_feeds.retain(|_, price_data| {
                    now - price_data.timestamp < 60000
                });
            }
        })
    }

    fn spawn_position_monitor(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let positions = self.positions.clone();
        let risk_params = self.risk_params.clone();
        let profit_targets = self.profit_targets.clone();
        let event_sender = self.event_sender.clone();
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(POSITION_CHECK_INTERVAL);
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                let params = risk_params.read().await;
                let targets = profit_targets.read().await;
                let current_metrics = metrics.read().await;
                let total_pnl = current_metrics.total_net_profit;
                drop(current_metrics);
                
                for mut position in positions.iter_mut() {
                    if position.balance <= Decimal::ZERO {
                        continue;
                    }
                    
                    let position_value = position.balance * position.current_price;
                    
                    if position_value > params.max_position_size {
                        let _ = event_sender.send(DashboardEvent::PositionAlert(
                            position.token,
                            format!("Position size {} exceeds limit", position_value)
                        ));
                    }
                    
                    let position_pnl_percent = if position.avg_entry_price > Decimal::ZERO {
                        (position.current_price - position.avg_entry_price) / position.avg_entry_price
                    } else {
                        Decimal::ZERO
                    };
                    
                    if position_pnl_percent < -params.max_drawdown_percent {
                        let _ = event_sender.send(DashboardEvent::PositionAlert(
                            position.token,
                            format!("Position drawdown {} exceeds limit", position_pnl_percent)
                        ));
                    }
                    
                    for (profit_level, reduction_percent) in &targets.take_profit_levels {
                        if position.unrealized_pnl > *profit_level {
                            let _ = event_sender.send(DashboardEvent::ProfitTarget(*profit_level));
                        }
                    }
                    
                    if total_pnl > Decimal::ZERO {
                        let trailing_stop = total_pnl * (Decimal::ONE - targets.trailing_stop_percent);
                        if position.unrealized_pnl < trailing_stop - total_pnl {
                            let _ = event_sender.send(DashboardEvent::PositionAlert(
                                position.token,
                                "Trailing stop triggered".to_string()
                            ));
                        }
                    }
                }
            }
        })
    }

    fn spawn_risk_monitor(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let metrics = self.metrics.clone();
        let daily_returns = self.daily_returns.clone();
        let risk_params = self.risk_params.clone();
        let event_sender = self.event_sender.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                let returns = daily_returns.lock().await;
                if returns.len() < 20 {
                    continue;
                }
                
                let params = risk_params.read().await;
                let var_confidence = params.var_confidence;
                
                let mut sorted_returns: Vec<Decimal> = returns.iter().copied().collect();
                sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                
                let var_index = ((Decimal::ONE - var_confidence) * Decimal::from(sorted_returns.len())).to_usize().unwrap_or(0);
                let value_at_risk = sorted_returns.get(var_index).copied().unwrap_or(Decimal::ZERO);
                
                if value_at_risk.abs() > params.max_position_size * params.max_drawdown_percent {
                    let _ = event_sender.send(DashboardEvent::RiskLimit(
                        format!("VaR {} exceeds risk limit", value_at_risk)
                    ));
                }
                
                drop(returns);
                
                let current_metrics = metrics.read().await;
                if current_metrics.current_drawdown > params.max_drawdown_percent {
                    let _ = event_sender.send(DashboardEvent::RiskLimit(
                        format!("Current drawdown {} exceeds limit", current_metrics.current_drawdown)
                    ));
                }
            }
        })
    }

    pub async fn get_current_metrics(&self) -> Result<PerformanceMetrics> {
        Ok(self.metrics.read().await.clone())
    }

    pub async fn get_recent_transactions(&self, limit: usize) -> Result<Vec<TransactionPnL>> {
        let history = self.transaction_history.lock().await;
        let transactions: Vec<TransactionPnL> = history
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect();
        Ok(transactions)
    }

    pub async fn get_positions(&self) -> Result<Vec<TokenPosition>> {
        let positions: Vec<TokenPosition> = self.positions
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|p| p.balance.abs() > Decimal::from_str("0.000001").unwrap())
            .collect();
        Ok(positions)
    }

    pub async fn get_profit_by_type(&self) -> Result<Vec<(TransactionType, Decimal)>> {
        let mut profit_map: HashMap<TransactionType, Decimal> = HashMap::new();
        
        for tx in self.transactions.iter() {
            if tx.status == TxStatus::Success {
                *profit_map.entry(tx.tx_type).or_insert(Decimal::ZERO) += tx.net_profit;
            }
        }
        
        Ok(profit_map.into_iter().collect())
    }

    pub async fn get_hourly_performance(&self, hours: usize) -> Result<Vec<(u64, Decimal)>> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let hour_ms = 3600000u64;
        let start_time = now.saturating_sub(hours as u64 * hour_ms);
        
        let mut hourly_profits: Vec<(u64, Decimal)> = Vec::new();
        
        for i in 0..hours {
            let hour_start = start_time + (i as u64 * hour_ms);
            let hour_end = hour_start + hour_ms;
            
            let hour_profit = self.transactions
                .iter()
                .filter(|tx| tx.timestamp >= hour_start && tx.timestamp < hour_end && tx.status == TxStatus::Success)
                .fold(Decimal::ZERO, |acc, tx| acc + tx.net_profit);
            
            hourly_profits.push((hour_start, hour_profit));
        }
        
        Ok(hourly_profits)
    }

    pub async fn calculate_kelly_position_size(
        &self,
        opportunity_profit: Decimal,
        success_probability: Decimal,
    ) -> Result<Decimal> {
        let metrics = self.metrics.read().await;
        let risk_params = self.risk_params.read().await;
        
        if metrics.total_transactions < 100 {
            return Ok(risk_params.max_position_size * Decimal::from_str("0.1")?);
        }
        
        let win_rate = metrics.win_rate.max(success_probability);
        let loss_rate = Decimal::ONE - win_rate;
        
        let avg_win = if metrics.largest_win > Decimal::ZERO {
            (metrics.largest_win + opportunity_profit) / Decimal::from(2)
        } else {
            opportunity_profit
        };
        
        let avg_loss = if metrics.largest_loss < Decimal::ZERO {
            metrics.largest_loss.abs()
        } else {
            avg_win * Decimal::from_str("0.5")?
        };
        
        if avg_loss == Decimal::ZERO || avg_win == Decimal::ZERO {
            return Ok(risk_params.max_position_size * Decimal::from_str("0.1")?);
        }
        
        let win_loss_ratio = avg_win / avg_loss;
        let kelly_fraction = (win_rate * win_loss_ratio - loss_rate) / win_loss_ratio;
        
        let adjusted_kelly = kelly_fraction * risk_params.position_sizing_kelly;
        let position_size = risk_params.max_position_size * 
            adjusted_kelly.min(Decimal::from_str("0.25")?).max(Decimal::ZERO);
        
        Ok(position_size)
    }

    pub async fn export_metrics_json(&self) -> Result<String> {
        let metrics = self.get_current_metrics().await?;
        let positions = self.get_positions().await?;
        let recent_txs = self.get_recent_transactions(100).await?;
        let hourly_perf = self.get_hourly_performance(24).await?;
        let profit_by_type = self.get_profit_by_type().await?;
        
        let export_data = serde_json::json!({
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            "metrics": metrics,
            "positions": positions,
            "recent_transactions": recent_txs,
            "hourly_performance": hourly_perf,
            "profit_by_type": profit_by_type,
            "last_update": self.last_update.load(Ordering::SeqCst),
            "is_running": self.running.load(Ordering::SeqCst),
        });
        
        Ok(serde_json::to_string_pretty(&export_data)?)
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<DashboardEvent> {
                self.event_sender.subscribe()
    }

    pub async fn reset_metrics(&self) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        *metrics = PerformanceMetrics::default();
        
        self.transactions.clear();
        self.transaction_history.lock().await.clear();
        self.positions.clear();
        self.daily_returns.lock().await.clear();
        
        let mut peak = self.peak_balance.write().await;
        *peak = Decimal::ZERO;
        
        self.last_update.store(0, Ordering::SeqCst);
        
        Ok(())
    }

    pub async fn get_risk_adjusted_metrics(&self) -> Result<RiskAdjustedMetrics> {
        let metrics = self.metrics.read().await;
        let returns = self.daily_returns.lock().await;
        
        if returns.is_empty() {
            return Ok(RiskAdjustedMetrics::default());
        }
        
        let returns_vec: Vec<Decimal> = returns.iter().copied().collect();
        let mean_return = returns_vec.iter().sum::<Decimal>() / Decimal::from(returns_vec.len());
        
        let mut sorted_returns = returns_vec.clone();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let percentile_5 = sorted_returns.get(returns_vec.len() / 20).copied().unwrap_or(Decimal::ZERO);
        let percentile_95 = sorted_returns.get(returns_vec.len() * 19 / 20).copied().unwrap_or(Decimal::ZERO);
        
        let downside_returns: Vec<Decimal> = returns_vec.iter()
            .filter(|&&r| r < Decimal::ZERO)
            .copied()
            .collect();
        
        let max_consecutive_losses = self.calculate_max_consecutive_losses(&returns_vec);
        let recovery_time = self.calculate_recovery_time(&returns_vec);
        
        Ok(RiskAdjustedMetrics {
            sharpe_ratio: metrics.sharpe_ratio,
            sortino_ratio: metrics.sortino_ratio,
            calmar_ratio: metrics.calmar_ratio,
            information_ratio: self.calculate_information_ratio(&returns_vec, mean_return),
            treynor_ratio: self.calculate_treynor_ratio(&metrics, mean_return),
            var_95: percentile_5.abs(),
            cvar_95: downside_returns.iter().filter(|&&r| r <= percentile_5).sum::<Decimal>() 
                / Decimal::from(downside_returns.len().max(1)),
            max_consecutive_losses,
            recovery_time,
            gain_to_pain_ratio: if metrics.max_drawdown > Decimal::ZERO {
                metrics.total_net_profit / metrics.max_drawdown
            } else {
                Decimal::ZERO
            },
        })
    }

    fn calculate_information_ratio(&self, returns: &[Decimal], benchmark_return: Decimal) -> Decimal {
        if returns.len() < 2 {
            return Decimal::ZERO;
        }
        
        let excess_returns: Vec<Decimal> = returns.iter()
            .map(|r| *r - benchmark_return)
            .collect();
        
        let mean_excess = excess_returns.iter().sum::<Decimal>() / Decimal::from(excess_returns.len());
        
        let tracking_error = excess_returns.iter()
            .map(|r| (*r - mean_excess).powu(2))
            .sum::<Decimal>() / Decimal::from(excess_returns.len() - 1);
        
        if tracking_error > Decimal::ZERO {
            mean_excess / tracking_error.sqrt().unwrap_or(Decimal::ONE)
        } else {
            Decimal::ZERO
        }
    }

    fn calculate_treynor_ratio(&self, metrics: &PerformanceMetrics, mean_return: Decimal) -> Decimal {
        let risk_free_rate = Decimal::from_str("0.02").unwrap() / Decimal::from(365);
        let excess_return = mean_return - risk_free_rate;
        
        if metrics.max_drawdown > Decimal::ZERO {
            excess_return / metrics.max_drawdown
        } else {
            Decimal::ZERO
        }
    }

    fn calculate_max_consecutive_losses(&self, returns: &[Decimal]) -> u32 {
        let mut max_losses = 0u32;
        let mut current_losses = 0u32;
        
        for ret in returns {
            if *ret < Decimal::ZERO {
                current_losses += 1;
                max_losses = max_losses.max(current_losses);
            } else {
                current_losses = 0;
            }
        }
        
        max_losses
    }

    fn calculate_recovery_time(&self, returns: &[Decimal]) -> u32 {
        let mut cumulative = Decimal::ZERO;
        let mut peak = Decimal::ZERO;
        let mut in_drawdown = false;
        let mut drawdown_start = 0;
        let mut max_recovery_time = 0u32;
        
        for (i, ret) in returns.iter().enumerate() {
            cumulative += ret;
            
            if cumulative > peak {
                peak = cumulative;
                if in_drawdown {
                    let recovery_time = (i - drawdown_start) as u32;
                    max_recovery_time = max_recovery_time.max(recovery_time);
                    in_drawdown = false;
                }
            } else if !in_drawdown {
                in_drawdown = true;
                drawdown_start = i;
            }
        }
        
        max_recovery_time
    }

    pub async fn get_correlation_matrix(&self) -> Result<HashMap<(TransactionType, TransactionType), Decimal>> {
        let mut correlations = HashMap::new();
        let types = vec![
            TransactionType::Arbitrage,
            TransactionType::Liquidation,
            TransactionType::JitSandwich,
            TransactionType::FrontRun,
            TransactionType::BackRun,
        ];
        
        for &type1 in &types {
            for &type2 in &types {
                let returns1 = self.get_returns_by_type(type1).await?;
                let returns2 = self.get_returns_by_type(type2).await?;
                
                if returns1.len() >= 10 && returns2.len() >= 10 {
                    let correlation = self.calculate_correlation(&returns1, &returns2);
                    correlations.insert((type1, type2), correlation);
                }
            }
        }
        
        Ok(correlations)
    }

    async fn get_returns_by_type(&self, tx_type: TransactionType) -> Result<Vec<Decimal>> {
        let returns: Vec<Decimal> = self.transactions
            .iter()
            .filter(|tx| tx.tx_type == tx_type && tx.status == TxStatus::Success)
            .map(|tx| tx.net_profit)
            .collect();
        Ok(returns)
    }

    fn calculate_correlation(&self, returns1: &[Decimal], returns2: &[Decimal]) -> Decimal {
        let n = returns1.len().min(returns2.len());
        if n < 2 {
            return Decimal::ZERO;
        }
        
        let mean1 = returns1[..n].iter().sum::<Decimal>() / Decimal::from(n);
        let mean2 = returns2[..n].iter().sum::<Decimal>() / Decimal::from(n);
        
        let mut covariance = Decimal::ZERO;
        let mut variance1 = Decimal::ZERO;
        let mut variance2 = Decimal::ZERO;
        
        for i in 0..n {
            let diff1 = returns1[i] - mean1;
            let diff2 = returns2[i] - mean2;
            
            covariance += diff1 * diff2;
            variance1 += diff1.powu(2);
            variance2 += diff2.powu(2);
        }
        
        if variance1 > Decimal::ZERO && variance2 > Decimal::ZERO {
            covariance / (variance1.sqrt().unwrap_or(Decimal::ONE) * variance2.sqrt().unwrap_or(Decimal::ONE))
        } else {
            Decimal::ZERO
        }
    }

    pub async fn update_risk_parameters(&self, new_params: RiskParameters) -> Result<()> {
        let mut params = self.risk_params.write().await;
        *params = new_params;
        Ok(())
    }

    pub async fn update_profit_targets(&self, new_targets: ProfitTargets) -> Result<()> {
        let mut targets = self.profit_targets.write().await;
        *targets = new_targets;
        Ok(())
    }

    pub async fn get_transaction(&self, signature: &str) -> Option<TransactionPnL> {
        self.transactions.get(signature).map(|tx| tx.clone())
    }

    pub async fn get_active_positions_count(&self) -> usize {
        self.positions.iter()
            .filter(|p| p.balance.abs() > Decimal::from_str("0.000001").unwrap())
            .count()
    }

    pub async fn get_total_volume_24h(&self) -> Result<Decimal> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let day_ago = now - 86400000;
        
        let volume = self.transactions
            .iter()
            .filter(|tx| tx.timestamp >= day_ago && tx.status == TxStatus::Success)
            .fold(Decimal::ZERO, |acc, tx| acc + tx.amount_in + tx.amount_out);
        
        Ok(volume)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RiskAdjustedMetrics {
    pub sharpe_ratio: Decimal,
    pub sortino_ratio: Decimal,
    pub calmar_ratio: Decimal,
    pub information_ratio: Decimal,
    pub treynor_ratio: Decimal,
    pub var_95: Decimal,
    pub cvar_95: Decimal,
    pub max_consecutive_losses: u32,
    pub recovery_time: u32,
    pub gain_to_pain_ratio: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_dashboard_lifecycle() {
        let dashboard = RealTimePnLDashboard::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        
        assert!(!dashboard.is_running());
        
        dashboard.start().await.unwrap();
        assert!(dashboard.is_running());
        
        dashboard.stop().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!dashboard.is_running());
    }

    #[tokio::test]
    async fn test_transaction_recording() {
        let dashboard = RealTimePnLDashboard::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        
        dashboard.start().await.unwrap();
        
        let tx = TransactionPnL {
            signature: "test_sig_1".to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            token_in: Pubkey::from_str(USDC_MINT).unwrap(),
            token_out: Pubkey::from_str(SOL_MINT).unwrap(),
            amount_in: Decimal::from_str("100").unwrap(),
            amount_out: Decimal::from_str("2.5").unwrap(),
            gas_fee: Decimal::from_str("0.005").unwrap(),
            priority_fee: Decimal::from_str("0.001").unwrap(),
            gross_profit: Decimal::from_str("5").unwrap(),
            net_profit: Decimal::from_str("4.994").unwrap(),
            profit_percentage: Decimal::from_str("0.05").unwrap(),
            execution_time_ms: 150,
            tx_type: TransactionType::Arbitrage,
            status: TxStatus::Success,
            slippage_cost: Decimal::from_str("0.1").unwrap(),
            mev_profit: Decimal::from_str("4.9").unwrap(),
            bundle_index: Some(0),
        };
        
        dashboard.record_transaction(tx).await.unwrap();
        
        let metrics = dashboard.get_current_metrics().await.unwrap();
        assert_eq!(metrics.total_transactions, 1);
        assert_eq!(metrics.successful_transactions, 1);
        
        dashboard.stop().await;
    }
}

