use anyhow::{Context, Result};
use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
    option_serializer::OptionSerializer,
};
use spl_token::instruction::TokenInstruction;
use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use statistical::{mean, standard_deviation};

const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const OPENBOOK_V2: &str = "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX";
const PHOENIX: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY";

const DETECTION_WINDOW_SECS: u64 = 900;
const MIN_TRADES_FOR_ANALYSIS: usize = 10;
const HIGH_WIN_RATE_THRESHOLD: f64 = 0.72;
const ANOMALY_STD_DEVS: f64 = 2.5;
const PROFIT_THRESHOLD_SOL: f64 = 50.0;
const VOLUME_SPIKE_MULTIPLIER: f64 = 3.5;
const MAX_TRACKED_TRADERS: usize = 10000;
const CACHE_TTL_SECS: u64 = 300;

#[derive(Debug, Clone)]
pub struct TraderMetrics {
    pub pubkey: Pubkey,
    pub total_trades: u64,
    pub winning_trades: u64,
    pub total_volume_sol: f64,
    pub total_profit_sol: f64,
    pub avg_trade_size_sol: f64,
    pub tokens_traded: HashMap<Pubkey, TokenMetrics>,
    pub recent_trades: VecDeque<TradeInfo>,
    pub first_seen: u64,
    pub last_updated: u64,
    pub anomaly_score: f64,
    pub reaction_times_ms: Vec<u64>,
    pub front_run_success_rate: f64,
    pub unique_tokens_count: u32,
}

#[derive(Debug, Clone)]
pub struct TokenMetrics {
    pub trades: u32,
    pub volume: f64,
    pub profit: f64,
    pub win_rate: f64,
    pub avg_holding_time_secs: u64,
}

#[derive(Debug, Clone)]
pub struct TradeInfo {
    pub signature: Signature,
    pub timestamp: u64,
    pub token_mint: Pubkey,
    pub amount_in_sol: f64,
    pub amount_out_sol: f64,
    pub profit_sol: f64,
    pub dex: DexType,
    pub is_buy: bool,
    pub slippage_bps: u16,
    pub gas_used: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DexType {
    Raydium,
    Orca,
    Jupiter,
    Openbook,
    Phoenix,
}

pub struct InformedTraderDetector {
    rpc_client: Arc<RpcClient>,
    trader_metrics: Arc<DashMap<Pubkey, TraderMetrics>>,
    token_price_cache: Arc<DashMap<Pubkey, (f64, u64)>>,
    informed_traders: Arc<RwLock<HashMap<Pubkey, InformedTraderProfile>>>,
    last_cleanup: Arc<AtomicU64>,
    market_metrics: Arc<RwLock<MarketMetrics>>,
}

#[derive(Debug, Clone)]
pub struct InformedTraderProfile {
    pub pubkey: Pubkey,
    pub confidence_score: f64,
    pub avg_profit_per_trade: f64,
    pub win_rate: f64,
    pub detection_timestamp: u64,
    pub trading_patterns: TradingPatterns,
    pub risk_score: f64,
}

#[derive(Debug, Clone)]
pub struct TradingPatterns {
    pub prefers_new_tokens: bool,
    pub avg_position_size_sol: f64,
    pub preferred_dexes: Vec<DexType>,
    pub active_hours: [bool; 24],
    pub uses_complex_routes: bool,
    pub avg_slippage_tolerance_bps: u16,
}

#[derive(Debug, Default)]
struct MarketMetrics {
    pub avg_win_rate: f64,
    pub avg_volume_per_trader: f64,
    pub total_traders_analyzed: u64,
    pub market_volatility_index: f64,
}

impl InformedTraderDetector {
    pub fn new(rpc_url: &str) -> Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
            rpc_url.to_string(),
            Duration::from_secs(30),
            CommitmentConfig::confirmed(),
        ));

        Ok(Self {
            rpc_client,
            trader_metrics: Arc::new(DashMap::new()),
            token_price_cache: Arc::new(DashMap::new()),
            informed_traders: Arc::new(RwLock::new(HashMap::new())),
            last_cleanup: Arc::new(AtomicU64::new(0)),
            market_metrics: Arc::new(RwLock::new(MarketMetrics::default())),
        })
    }

    pub async fn analyze_transaction(&self, signature: &Signature) -> Result<Option<Pubkey>> {
        let tx = self.rpc_client
            .get_transaction(signature, UiTransactionEncoding::JsonParsed)
            .context("Failed to fetch transaction")?;

        if let Some(meta) = &tx.transaction.meta {
            if meta.err.is_some() {
                return Ok(None);
            }

            let trader = self.extract_trader_from_tx(&tx)?;
            if let Some(trader_pubkey) = trader {
                let trade_info = self.parse_trade_info(&tx, trader_pubkey).await?;
                if let Some(trade) = trade_info {
                    self.update_trader_metrics(trader_pubkey, trade).await?;
                    return Ok(Some(trader_pubkey));
                }
            }
        }

        Ok(None)
    }

    fn extract_trader_from_tx(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> Result<Option<Pubkey>> {
        if let Some(transaction) = &tx.transaction.transaction {
            let message = transaction.decode()
                .context("Failed to decode transaction")?
                .message;

            if !message.account_keys.is_empty() {
                let fee_payer = message.account_keys[0];
                if self.is_likely_trader(&fee_payer, &message.account_keys) {
                    return Ok(Some(fee_payer));
                }
            }
        }
        Ok(None)
    }

    fn is_likely_trader(&self, pubkey: &Pubkey, account_keys: &[Pubkey]) -> bool {
        let system_programs = [
            "11111111111111111111111111111111",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
        ];

        for program in &system_programs {
            if pubkey.to_string() == *program {
                return false;
            }
        }

        let is_dex_interaction = account_keys.iter().any(|key| {
            let key_str = key.to_string();
            key_str == RAYDIUM_V4 || key_str == ORCA_WHIRLPOOL || 
            key_str == JUPITER_V6 || key_str == OPENBOOK_V2 || key_str == PHOENIX
        });

        is_dex_interaction && account_keys.len() > 3
    }

    async fn parse_trade_info(
        &self,
        tx: &EncodedConfirmedTransactionWithStatusMeta,
        trader: Pubkey,
    ) -> Result<Option<TradeInfo>> {
        let Some(meta) = &tx.transaction.meta else {
            return Ok(None);
        };

        let Some(block_time) = tx.block_time else {
            return Ok(None);
        };

        let pre_balances = &meta.pre_balances;
        let post_balances = &meta.post_balances;
        
        if pre_balances.is_empty() || post_balances.is_empty() {
            return Ok(None);
        }

        let sol_change = (post_balances[0] as f64 - pre_balances[0] as f64) / 1e9;
        let gas_used = meta.fee;

        let (dex_type, token_mint, is_buy, amount_sol) = self.extract_swap_details(tx)?;

        if let Some((dex, mint, buy, amount)) = dex_type.zip(token_mint).zip(is_buy).zip(amount_sol)
            .map(|(((a, b), c), d)| (a, b, c, d)) {
            
            let profit_sol = if buy {
                -amount - (gas_used as f64 / 1e9)
            } else {
                amount - (gas_used as f64 / 1e9)
            };

            let slippage_bps = self.calculate_slippage(tx);

            Ok(Some(TradeInfo {
                signature: *tx.transaction.transaction
                    .as_ref()
                    .and_then(|t| t.decode().ok())
                    .map(|t| t.signatures.first())
                    .flatten()
                    .unwrap_or(&Signature::default()),
                timestamp: block_time as u64,
                token_mint: mint,
                amount_in_sol: amount.abs(),
                amount_out_sol: 0.0,
                profit_sol,
                dex,
                is_buy: buy,
                slippage_bps,
                gas_used,
            }))
        } else {
            Ok(None)
        }
    }

    fn extract_swap_details(
        &self,
        tx: &EncodedConfirmedTransactionWithStatusMeta,
    ) -> Result<(Option<DexType>, Option<Pubkey>, Option<bool>, Option<f64>)> {
        let Some(transaction) = &tx.transaction.transaction else {
            return Ok((None, None, None, None));
        };

        let decoded = transaction.decode()?;
        let account_keys = &decoded.message.account_keys;

        let mut dex_type = None;
        let mut token_mint = None;
        let mut is_buy = None;
        let mut amount_sol = None;

        for (idx, key) in account_keys.iter().enumerate() {
            let key_str = key.to_string();
            match key_str.as_str() {
                RAYDIUM_V4 => dex_type = Some(DexType::Raydium),
                ORCA_WHIRLPOOL => dex_type = Some(DexType::Orca),
                JUPITER_V6 => dex_type = Some(DexType::Jupiter),
                OPENBOOK_V2 => dex_type = Some(DexType::Openbook),
                PHOENIX => dex_type = Some(DexType::Phoenix),
                _ => {}
            }
        }

        if let Some(meta) = &tx.transaction.meta {
            if let Some(inner) = &meta.inner_instructions {
                for inner_ix in inner {
                    for ix in &inner_ix.instructions {
                        if let Ok(parsed) = serde_json::from_value::<serde_json::Value>(ix.clone()) {
                            if let Some(program) = parsed.get("program").and_then(|p| p.as_str()) {
                                if program == "spl-token" {
                                    if let Some(parsed_ix) = parsed.get("parsed") {
                                        if let Some(ix_type) = parsed_ix.get("type").and_then(|t| t.as_str()) {
                                            if ix_type == "transfer" || ix_type == "transferChecked" {
                                                if let Some(info) = parsed_ix.get("info") {
                                                    if let Some(mint_str) = info.get("mint").and_then(|m| m.as_str()) {
                                                        token_mint = Pubkey::from_str(mint_str).ok();
                                                    }
                                                    if let Some(amount_str) = info.get("tokenAmount")
                                                        .and_then(|a| a.get("uiAmount"))
                                                        .and_then(|a| a.as_f64()) {
                                                        amount_sol = Some(amount_str);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

                        let pre_token_count = meta.pre_token_balances.as_ref().map(|b| b.len()).unwrap_or(0);
            let post_token_count = meta.post_token_balances.as_ref().map(|b| b.len()).unwrap_or(0);
            
            is_buy = Some(post_token_count > pre_token_count);
        }

        Ok((dex_type, token_mint, is_buy, amount_sol))
    }

    fn calculate_slippage(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> u16 {
        if let Some(meta) = &tx.transaction.meta {
            if let Some(logs) = &meta.log_messages {
                for log in logs {
                    if log.contains("slippage") {
                        if let Some(slippage_str) = log.split_whitespace()
                            .find(|s| s.parse::<f64>().is_ok()) {
                            if let Ok(slippage) = slippage_str.parse::<f64>() {
                                return (slippage * 100.0) as u16;
                            }
                        }
                    }
                }
            }
        }
        50 // Default 0.5% slippage
    }

    async fn update_trader_metrics(&self, trader: Pubkey, trade: TradeInfo) -> Result<()> {
        let mut entry = self.trader_metrics.entry(trader).or_insert_with(|| {
            TraderMetrics {
                pubkey: trader,
                total_trades: 0,
                winning_trades: 0,
                total_volume_sol: 0.0,
                total_profit_sol: 0.0,
                avg_trade_size_sol: 0.0,
                tokens_traded: HashMap::new(),
                recent_trades: VecDeque::with_capacity(100),
                first_seen: trade.timestamp,
                last_updated: trade.timestamp,
                anomaly_score: 0.0,
                reaction_times_ms: Vec::new(),
                front_run_success_rate: 0.0,
                unique_tokens_count: 0,
            }
        });

        let metrics = entry.value_mut();
        metrics.total_trades += 1;
        metrics.total_volume_sol += trade.amount_in_sol;
        metrics.total_profit_sol += trade.profit_sol;
        
        if trade.profit_sol > 0.0 {
            metrics.winning_trades += 1;
        }

        metrics.avg_trade_size_sol = metrics.total_volume_sol / metrics.total_trades as f64;
        
        if metrics.recent_trades.len() >= 100 {
            metrics.recent_trades.pop_front();
        }
        metrics.recent_trades.push_back(trade.clone());

        let token_entry = metrics.tokens_traded.entry(trade.token_mint).or_insert(TokenMetrics {
            trades: 0,
            volume: 0.0,
            profit: 0.0,
            win_rate: 0.0,
            avg_holding_time_secs: 0,
        });

        token_entry.trades += 1;
        token_entry.volume += trade.amount_in_sol;
        token_entry.profit += trade.profit_sol;
        token_entry.win_rate = if token_entry.profit > 0.0 {
            (token_entry.trades as f64 * token_entry.win_rate + 1.0) / (token_entry.trades as f64 + 1.0)
        } else {
            (token_entry.trades as f64 * token_entry.win_rate) / (token_entry.trades as f64 + 1.0)
        };

        metrics.unique_tokens_count = metrics.tokens_traded.len() as u32;
        metrics.last_updated = trade.timestamp;

        self.calculate_anomaly_score(metrics);
        self.update_reaction_times(metrics, &trade).await;
        
        drop(entry);

        if metrics.total_trades >= MIN_TRADES_FOR_ANALYSIS as u64 {
            self.evaluate_trader_sophistication(trader).await?;
        }

        self.cleanup_old_data().await;

        Ok(())
    }

    fn calculate_anomaly_score(&self, metrics: &mut TraderMetrics) {
        if metrics.total_trades < MIN_TRADES_FOR_ANALYSIS as u64 {
            return;
        }

        let win_rate = metrics.winning_trades as f64 / metrics.total_trades as f64;
        let avg_profit_per_trade = metrics.total_profit_sol / metrics.total_trades as f64;
        
        let trade_sizes: Vec<f64> = metrics.recent_trades.iter()
            .map(|t| t.amount_in_sol)
            .collect();

        if trade_sizes.len() >= 5 {
            let mean_size = mean(&trade_sizes);
            let std_dev = standard_deviation(&trade_sizes, Some(mean_size));
            
            let consistency_score = if std_dev > 0.0 {
                1.0 / (1.0 + std_dev / mean_size)
            } else {
                1.0
            };

            let win_rate_deviation = (win_rate - 0.5).abs() * 2.0;
            let profit_factor = (avg_profit_per_trade / 10.0).min(1.0).max(0.0);
            
            metrics.anomaly_score = (win_rate_deviation * 0.4 + 
                                    consistency_score * 0.3 + 
                                    profit_factor * 0.3) * 100.0;
        }
    }

    async fn update_reaction_times(&self, metrics: &mut TraderMetrics, trade: &TradeInfo) {
        if metrics.recent_trades.len() < 2 {
            return;
        }

        let prev_trade = &metrics.recent_trades[metrics.recent_trades.len() - 2];
        let time_diff_ms = (trade.timestamp - prev_trade.timestamp) * 1000;
        
        if time_diff_ms < 60000 {
            metrics.reaction_times_ms.push(time_diff_ms);
            if metrics.reaction_times_ms.len() > 50 {
                metrics.reaction_times_ms.remove(0);
            }
        }
    }

    async fn evaluate_trader_sophistication(&self, trader: Pubkey) -> Result<()> {
        let Some(entry) = self.trader_metrics.get(&trader) else {
            return Ok(());
        };
        let metrics = entry.value();

        let win_rate = metrics.winning_trades as f64 / metrics.total_trades as f64;
        let avg_profit = metrics.total_profit_sol / metrics.total_trades as f64;
        
        let mut confidence_score = 0.0;
        let mut indicators = 0;

        if win_rate > HIGH_WIN_RATE_THRESHOLD {
            confidence_score += 25.0;
            indicators += 1;
        }

        if metrics.anomaly_score > 75.0 {
            confidence_score += 20.0;
            indicators += 1;
        }

        if metrics.total_profit_sol > PROFIT_THRESHOLD_SOL {
            confidence_score += 15.0;
            indicators += 1;
        }

        let avg_reaction_time = if !metrics.reaction_times_ms.is_empty() {
            metrics.reaction_times_ms.iter().sum::<u64>() as f64 / 
            metrics.reaction_times_ms.len() as f64
        } else {
            f64::MAX
        };

        if avg_reaction_time < 500.0 {
            confidence_score += 20.0;
            indicators += 1;
        }

        let tokens_per_trade = metrics.unique_tokens_count as f64 / metrics.total_trades as f64;
        if tokens_per_trade < 0.3 {
            confidence_score += 10.0;
            indicators += 1;
        }

        let market_metrics = self.market_metrics.read().await;
        if win_rate > market_metrics.avg_win_rate * 1.5 && market_metrics.avg_win_rate > 0.0 {
            confidence_score += 10.0;
            indicators += 1;
        }
        drop(market_metrics);

        if indicators >= 3 && confidence_score >= 50.0 {
            let trading_patterns = self.analyze_trading_patterns(metrics);
            let risk_score = self.calculate_risk_score(metrics, &trading_patterns);

            let profile = InformedTraderProfile {
                pubkey: trader,
                confidence_score,
                avg_profit_per_trade: avg_profit,
                win_rate,
                detection_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                trading_patterns,
                risk_score,
            };

            let mut informed_traders = self.informed_traders.write().await;
            informed_traders.insert(trader, profile);
        }

        drop(entry);
        Ok(())
    }

    fn analyze_trading_patterns(&self, metrics: &TraderMetrics) -> TradingPatterns {
        let mut dex_usage: HashMap<DexType, u32> = HashMap::new();
        let mut active_hours = [false; 24];
        let mut total_slippage = 0u32;
        let mut position_sizes = Vec::new();

        for trade in &metrics.recent_trades {
            *dex_usage.entry(trade.dex).or_insert(0) += 1;
            
            let hour = (trade.timestamp / 3600) % 24;
            active_hours[hour as usize] = true;
            
            total_slippage += trade.slippage_bps as u32;
            position_sizes.push(trade.amount_in_sol);
        }

        let mut preferred_dexes: Vec<(DexType, u32)> = dex_usage.into_iter().collect();
        preferred_dexes.sort_by(|a, b| b.1.cmp(&a.1));
        let preferred_dexes = preferred_dexes.into_iter().map(|(dex, _)| dex).collect();

        let avg_position_size = if !position_sizes.is_empty() {
            position_sizes.iter().sum::<f64>() / position_sizes.len() as f64
        } else {
            0.0
        };

        let avg_slippage = if metrics.recent_trades.is_empty() {
            50
        } else {
            (total_slippage / metrics.recent_trades.len() as u32) as u16
        };

        let token_age_sum: u64 = metrics.tokens_traded.values()
            .map(|t| t.avg_holding_time_secs)
            .sum();
        let avg_token_age = if !metrics.tokens_traded.is_empty() {
            token_age_sum / metrics.tokens_traded.len() as u64
        } else {
            0
        };

        TradingPatterns {
            prefers_new_tokens: avg_token_age < 86400,
            avg_position_size_sol: avg_position_size,
            preferred_dexes,
            active_hours,
            uses_complex_routes: metrics.recent_trades.iter()
                .any(|t| t.dex == DexType::Jupiter),
            avg_slippage_tolerance_bps: avg_slippage,
        }
    }

    fn calculate_risk_score(&self, metrics: &TraderMetrics, patterns: &TradingPatterns) -> f64 {
        let mut risk_score = 0.0;

        if patterns.avg_position_size_sol > 100.0 {
            risk_score += 20.0;
        }

        let volatility = if metrics.recent_trades.len() >= 5 {
            let profits: Vec<f64> = metrics.recent_trades.iter()
                .map(|t| t.profit_sol)
                .collect();
            let mean_profit = mean(&profits);
            standard_deviation(&profits, Some(mean_profit)) / mean_profit.abs().max(1.0)
        } else {
            0.0
        };

        risk_score += (volatility * 30.0).min(30.0);

        if patterns.avg_slippage_tolerance_bps > 100 {
            risk_score += 15.0;
        }

        if patterns.prefers_new_tokens {
            risk_score += 25.0;
        }

        let concentration = metrics.tokens_traded.values()
            .map(|t| t.volume / metrics.total_volume_sol)
            .fold(0.0, |acc, ratio| acc + ratio * ratio);
        
        risk_score += (1.0 - concentration) * 10.0;

        risk_score.min(100.0)
    }

    async fn cleanup_old_data(&self) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let last_cleanup = self.last_cleanup.load(Ordering::Relaxed);

        if now - last_cleanup < 300 {
            return;
        }

        self.last_cleanup.store(now, Ordering::Relaxed);

        let cutoff_time = now - DETECTION_WINDOW_SECS;

        self.trader_metrics.retain(|_, metrics| {
            metrics.last_updated > cutoff_time || metrics.total_trades >= 50
        });

        self.token_price_cache.retain(|_, (_, timestamp)| {
            *timestamp > now - CACHE_TTL_SECS
        });

        let mut informed_traders = self.informed_traders.write().await;
        informed_traders.retain(|_, profile| {
            profile.detection_timestamp > now - 86400
        });

        self.update_market_metrics().await;
    }

    async fn update_market_metrics(&self) {
        let mut total_win_rate = 0.0;
        let mut total_volume = 0.0;
        let mut trader_count = 0u64;

        for entry in self.trader_metrics.iter() {
            let metrics = entry.value();
            if metrics.total_trades >= MIN_TRADES_FOR_ANALYSIS as u64 {
                let win_rate = metrics.winning_trades as f64 / metrics.total_trades as f64;
                total_win_rate += win_rate;
                total_volume += metrics.total_volume_sol;
                trader_count += 1;
            }
        }

        if trader_count > 0 {
            let mut market_metrics = self.market_metrics.write().await;
            market_metrics.avg_win_rate = total_win_rate / trader_count as f64;
            market_metrics.avg_volume_per_trader = total_volume / trader_count as f64;
                        market_metrics.total_traders_analyzed = trader_count;
            
            // Calculate market volatility based on profit variance
            let mut profit_variance = 0.0;
            let mut profit_samples = 0;
            
            for entry in self.trader_metrics.iter() {
                let metrics = entry.value();
                if metrics.total_trades >= 5 {
                    let avg_profit = metrics.total_profit_sol / metrics.total_trades as f64;
                    profit_variance += avg_profit.powi(2);
                    profit_samples += 1;
                }
            }
            
            if profit_samples > 0 {
                market_metrics.market_volatility_index = (profit_variance / profit_samples as f64).sqrt();
            }
        }
    }

    pub async fn get_informed_traders(&self) -> Vec<InformedTraderProfile> {
        let traders = self.informed_traders.read().await;
        let mut profiles: Vec<InformedTraderProfile> = traders.values().cloned().collect();
        profiles.sort_by(|a, b| b.confidence_score.partial_cmp(&a.confidence_score).unwrap_or(std::cmp::Ordering::Equal));
        profiles
    }

    pub async fn get_trader_profile(&self, trader: &Pubkey) -> Option<InformedTraderProfile> {
        let traders = self.informed_traders.read().await;
        traders.get(trader).cloned()
    }

    pub async fn is_informed_trader(&self, trader: &Pubkey) -> bool {
        let traders = self.informed_traders.read().await;
        traders.contains_key(trader)
    }

    pub async fn get_trader_metrics(&self, trader: &Pubkey) -> Option<TraderMetrics> {
        self.trader_metrics.get(trader).map(|entry| entry.value().clone())
    }

    pub async fn analyze_historical_trades(&self, trader: Pubkey, signatures: Vec<Signature>) -> Result<()> {
        for signature in signatures.iter().take(100) {
            match self.analyze_transaction(signature).await {
                Ok(_) => {},
                Err(e) => {
                    eprintln!("Error analyzing historical transaction {}: {}", signature, e);
                }
            }
        }
        Ok(())
    }

    pub async fn calculate_trader_pnl(&self, trader: &Pubkey, time_window_secs: u64) -> Option<f64> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let cutoff = now.saturating_sub(time_window_secs);
        
        self.trader_metrics.get(trader).map(|entry| {
            let metrics = entry.value();
            metrics.recent_trades.iter()
                .filter(|t| t.timestamp >= cutoff)
                .map(|t| t.profit_sol)
                .sum()
        })
    }

    pub async fn get_top_traders_by_profit(&self, limit: usize) -> Vec<(Pubkey, f64)> {
        let mut traders: Vec<(Pubkey, f64)> = self.trader_metrics.iter()
            .filter(|entry| entry.value().total_trades >= MIN_TRADES_FOR_ANALYSIS as u64)
            .map(|entry| (entry.key().clone(), entry.value().total_profit_sol))
            .collect();
            
        traders.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        traders.truncate(limit);
        traders
    }

    pub async fn get_top_traders_by_win_rate(&self, limit: usize) -> Vec<(Pubkey, f64)> {
        let mut traders: Vec<(Pubkey, f64)> = self.trader_metrics.iter()
            .filter(|entry| entry.value().total_trades >= MIN_TRADES_FOR_ANALYSIS as u64)
            .map(|entry| {
                let metrics = entry.value();
                let win_rate = metrics.winning_trades as f64 / metrics.total_trades as f64;
                (entry.key().clone(), win_rate)
            })
            .collect();
            
        traders.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        traders.truncate(limit);
        traders
    }

    pub async fn detect_anomalous_activity(&self, trader: &Pubkey) -> Option<AnomalyReport> {
        let entry = self.trader_metrics.get(trader)?;
        let metrics = entry.value();
        
        if metrics.total_trades < MIN_TRADES_FOR_ANALYSIS as u64 {
            return None;
        }

        let mut anomalies = Vec::new();
        
        // Check for sudden volume spikes
        if metrics.recent_trades.len() >= 10 {
            let recent_volumes: Vec<f64> = metrics.recent_trades.iter()
                .rev()
                .take(10)
                .map(|t| t.amount_in_sol)
                .collect();
            
            let avg_recent = mean(&recent_volumes);
            let current_trade = metrics.recent_trades.back()?;
            
            if current_trade.amount_in_sol > avg_recent * VOLUME_SPIKE_MULTIPLIER {
                anomalies.push(AnomalyType::VolumeSpikeDetected);
            }
        }
        
        // Check for unusual win streaks
        let recent_wins = metrics.recent_trades.iter()
            .rev()
            .take(10)
            .filter(|t| t.profit_sol > 0.0)
            .count();
            
        if recent_wins >= 8 {
            anomalies.push(AnomalyType::UnusualWinStreak);
        }
        
        // Check for rapid trading
        if metrics.reaction_times_ms.len() >= 5 {
            let avg_reaction = metrics.reaction_times_ms.iter().sum::<u64>() as f64 
                / metrics.reaction_times_ms.len() as f64;
            if avg_reaction < 200.0 {
                anomalies.push(AnomalyType::SubhumanReactionTime);
            }
        }

        if !anomalies.is_empty() {
            Some(AnomalyReport {
                trader: *trader,
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs(),
                anomalies,
                confidence: metrics.anomaly_score,
            })
        } else {
            None
        }
    }

    pub async fn get_market_metrics(&self) -> MarketMetrics {
        self.market_metrics.read().await.clone()
    }

    pub fn get_active_trader_count(&self) -> usize {
        self.trader_metrics.len()
    }

    pub async fn export_trader_data(&self, trader: &Pubkey) -> Option<TraderExportData> {
        let entry = self.trader_metrics.get(trader)?;
        let metrics = entry.value();
        let profile = self.get_trader_profile(trader).await;
        
        Some(TraderExportData {
            pubkey: *trader,
            metrics: metrics.clone(),
            profile,
            export_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs(),
        })
    }

    pub async fn batch_analyze_transactions(&self, signatures: &[Signature]) -> Result<Vec<Option<Pubkey>>> {
        let mut results = Vec::with_capacity(signatures.len());
        
        for chunk in signatures.chunks(10) {
            let futures: Vec<_> = chunk.iter()
                .map(|sig| self.analyze_transaction(sig))
                .collect();
            
            for future in futures {
                match future.await {
                    Ok(trader) => results.push(trader),
                    Err(_) => results.push(None),
                }
            }
            
            // Rate limiting
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        
        Ok(results)
    }

    pub fn calculate_trader_score(&self, metrics: &TraderMetrics) -> f64 {
        if metrics.total_trades < MIN_TRADES_FOR_ANALYSIS as u64 {
            return 0.0;
        }

        let win_rate = metrics.winning_trades as f64 / metrics.total_trades as f64;
        let avg_profit = metrics.total_profit_sol / metrics.total_trades as f64;
        let consistency = 1.0 / (1.0 + self.calculate_profit_variance(metrics));
        
        let volume_score = (metrics.total_volume_sol / 1000.0).min(1.0);
        let diversity_score = (metrics.unique_tokens_count as f64 / 50.0).min(1.0);
        
        let reaction_score = if !metrics.reaction_times_ms.is_empty() {
            let avg_reaction = metrics.reaction_times_ms.iter().sum::<u64>() as f64 
                / metrics.reaction_times_ms.len() as f64;
            1.0 / (1.0 + avg_reaction / 1000.0)
        } else {
            0.0
        };

        (win_rate * 0.3 + 
         (avg_profit / 10.0).min(1.0).max(0.0) * 0.25 + 
         consistency * 0.2 + 
         volume_score * 0.1 + 
         diversity_score * 0.1 + 
         reaction_score * 0.05) * 100.0
    }

    fn calculate_profit_variance(&self, metrics: &TraderMetrics) -> f64 {
        if metrics.recent_trades.len() < 2 {
            return 0.0;
        }

        let profits: Vec<f64> = metrics.recent_trades.iter()
            .map(|t| t.profit_sol)
            .collect();
        
        let mean_profit = mean(&profits);
        let variance = profits.iter()
            .map(|p| (p - mean_profit).powi(2))
            .sum::<f64>() / profits.len() as f64;
        
        variance.sqrt()
    }
}

#[derive(Debug, Clone)]
pub struct AnomalyReport {
    pub trader: Pubkey,
    pub timestamp: u64,
    pub anomalies: Vec<AnomalyType>,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub enum AnomalyType {
    VolumeSpikeDetected,
    UnusualWinStreak,
    SubhumanReactionTime,
}

#[derive(Debug, Clone)]
pub struct TraderExportData {
    pub pubkey: Pubkey,
    pub metrics: TraderMetrics,
    pub profile: Option<InformedTraderProfile>,
    pub export_timestamp: u64,
}

impl Default for InformedTraderDetector {
    fn default() -> Self {
        Self::new("https://api.mainnet-beta.solana.com").expect("Failed to create detector")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_detector_creation() {
        let detector = InformedTraderDetector::new("https://api.mainnet-beta.solana.com");
        assert!(detector.is_ok());
    }

    #[tokio::test]
    async fn test_anomaly_score_calculation() {
        let detector = InformedTraderDetector::default();
        let trader = Pubkey::new_unique();
        
        let trade = TradeInfo {
            signature: Signature::new_unique(),
            timestamp: 1000,
            token_mint: Pubkey::new_unique(),
            amount_in_sol: 10.0,
            amount_out_sol: 11.0,
            profit_sol: 1.0,
            dex: DexType::Raydium,
            is_buy: true,
            slippage_bps: 50,
            gas_used: 5000,
        };
        
        let _ = detector.update_trader_metrics(trader, trade).await;
        let metrics = detector.get_trader_metrics(&trader).await;
        assert!(metrics.is_some());
    }
}

