use std::collections::{HashMap, VecDeque, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta,
    UiTransactionEncoding,
    option_serializer::OptionSerializer,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};

const CAPITAL_HISTORY_SIZE: usize = 1000;
const POSITION_CACHE_SIZE: usize = 500;
const COMPETITOR_UPDATE_INTERVAL_MS: u64 = 100;
const CAPITAL_THRESHOLD_LAMPORTS: u64 = 1_000_000_000; // 1 SOL
const MAX_COMPETITORS: usize = 1000;
const TRANSACTION_BATCH_SIZE: usize = 50;
const METRICS_UPDATE_INTERVAL_MS: u64 = 5000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitorProfile {
    pub address: Pubkey,
    pub last_seen: u64,
    pub total_volume_24h: u64,
    pub win_rate: f64,
    pub avg_position_size: u64,
    pub preferred_tokens: Vec<Pubkey>,
    pub strategy_fingerprint: StrategyType,
    pub risk_score: f64,
    pub capital_efficiency: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum StrategyType {
    Arbitrage,
    FrontRunning,
    Sandwiching,
    Liquidation,
    JitLiquidity,
    AtomicArbitrage,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct CapitalSnapshot {
    pub timestamp: u64,
    pub sol_balance: u64,
    pub token_values: HashMap<Pubkey, u64>,
    pub total_value_lamports: u64,
    pub open_positions: u32,
    pub pending_txs: u32,
}

#[derive(Debug, Clone)]
pub struct PositionTracker {
    pub token_mint: Pubkey,
    pub entry_price: f64,
    pub size: u64,
    pub entry_time: u64,
    pub exit_time: Option<u64>,
    pub pnl: Option<f64>,
    pub strategy: StrategyType,
}

#[derive(Debug)]
pub struct CompetitorMetrics {
    pub total_tracked: usize,
    pub active_last_hour: usize,
    pub total_capital_tracked: u64,
    pub dominant_strategy: StrategyType,
    pub avg_transaction_size: u64,
    pub network_congestion_score: f64,
}

pub struct CompetitorCapitalTracker {
    rpc_client: Arc<RpcClient>,
    competitors: Arc<DashMap<Pubkey, CompetitorProfile>>,
    capital_history: Arc<DashMap<Pubkey, VecDeque<CapitalSnapshot>>>,
    position_cache: Arc<DashMap<Pubkey, VecDeque<PositionTracker>>>,
    hot_wallets: Arc<RwLock<HashSet<Pubkey>>>,
    metrics: Arc<TokioRwLock<CompetitorMetrics>>,
    update_channel: mpsc::Sender<CompetitorUpdate>,
    shutdown: Arc<tokio::sync::Notify>,
}

#[derive(Debug)]
pub enum CompetitorUpdate {
    NewCompetitor(Pubkey),
    CapitalChange(Pubkey, u64),
    PositionOpened(Pubkey, PositionTracker),
    PositionClosed(Pubkey, Pubkey, f64),
    StrategyDetected(Pubkey, StrategyType),
}

impl CompetitorCapitalTracker {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        initial_competitors: Vec<Pubkey>,
    ) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<CompetitorUpdate>(10000);
        
        let tracker = Self {
            rpc_client: rpc_client.clone(),
            competitors: Arc::new(DashMap::new()),
            capital_history: Arc::new(DashMap::new()),
            position_cache: Arc::new(DashMap::new()),
            hot_wallets: Arc::new(RwLock::new(HashSet::new())),
            metrics: Arc::new(TokioRwLock::new(CompetitorMetrics {
                total_tracked: 0,
                active_last_hour: 0,
                total_capital_tracked: 0,
                dominant_strategy: StrategyType::Unknown,
                avg_transaction_size: 0,
                network_congestion_score: 0.0,
            })),
            update_channel: tx,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        };

        for competitor in initial_competitors {
            tracker.add_competitor(competitor).await?;
        }

        let update_handler = tracker.clone();
        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                update_handler.handle_update(update).await;
            }
        });

        Ok(tracker)
    }

    pub async fn add_competitor(&self, address: Pubkey) -> Result<()> {
        if self.competitors.len() >= MAX_COMPETITORS {
            self.evict_inactive_competitor().await;
        }

        let profile = CompetitorProfile {
            address,
            last_seen: self.current_timestamp(),
            total_volume_24h: 0,
            win_rate: 0.0,
            avg_position_size: 0,
            preferred_tokens: Vec::new(),
            strategy_fingerprint: StrategyType::Unknown,
            risk_score: 0.5,
            capital_efficiency: 0.0,
        };

        self.competitors.insert(address, profile);
        self.capital_history.insert(address, VecDeque::with_capacity(CAPITAL_HISTORY_SIZE));
        self.position_cache.insert(address, VecDeque::with_capacity(POSITION_CACHE_SIZE));
        
        let _ = self.update_channel.send(CompetitorUpdate::NewCompetitor(address)).await;
        Ok(())
    }

    pub async fn track_capital_movements(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(COMPETITOR_UPDATE_INTERVAL_MS));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.update_all_competitors().await;
                }
                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }
    }

    async fn update_all_competitors(&self) {
        let competitors: Vec<Pubkey> = self.competitors.iter()
            .map(|entry| *entry.key())
            .collect();

        for chunk in competitors.chunks(TRANSACTION_BATCH_SIZE) {
            let futures: Vec<_> = chunk.iter()
                .map(|addr| self.update_competitor_capital(*addr))
                .collect();
            
            let _ = futures::future::join_all(futures).await;
        }

        self.update_metrics().await;
    }

    async fn update_competitor_capital(&self, address: Pubkey) -> Result<()> {
        let balance = self.rpc_client
            .get_balance(&address)
            .await
            .unwrap_or(0);

        let token_accounts = self.get_token_accounts(&address).await?;
        let mut token_values = HashMap::new();
        let mut total_value = balance;

        for (mint, amount) in token_accounts {
            let value = self.estimate_token_value(&mint, amount).await?;
            token_values.insert(mint, value);
            total_value += value;
        }

        let snapshot = CapitalSnapshot {
            timestamp: self.current_timestamp(),
            sol_balance: balance,
            token_values,
            total_value_lamports: total_value,
            open_positions: self.count_open_positions(&address),
            pending_txs: 0,
        };

        if let Some(mut history) = self.capital_history.get_mut(&address) {
            if history.len() >= CAPITAL_HISTORY_SIZE {
                history.pop_front();
            }
            history.push_back(snapshot.clone());
        }

        if total_value > CAPITAL_THRESHOLD_LAMPORTS {
            self.hot_wallets.write().unwrap().insert(address);
        }

        let _ = self.update_channel.send(CompetitorUpdate::CapitalChange(address, total_value)).await;
        
        Ok(())
    }

    async fn get_token_accounts(&self, address: &Pubkey) -> Result<Vec<(Pubkey, u64)>> {
        let accounts = self.rpc_client
            .get_token_accounts_by_owner(
                address,
                solana_client::rpc_request::TokenAccountsFilter::ProgramId(
                    spl_token::id()
                ),
            )
            .await?;

        let mut result = Vec::new();
        for account in accounts.value {
            if let Ok(parsed) = serde_json::from_value::<serde_json::Value>(account.account.data.parsed()) {
                if let Some(info) = parsed.get("info") {
                    if let (Some(mint), Some(amount)) = (
                        info.get("mint").and_then(|m| m.as_str()),
                        info.get("tokenAmount").and_then(|ta| ta.get("amount")).and_then(|a| a.as_str())
                    ) {
                        if let (Ok(mint_pubkey), Ok(amount_u64)) = (
                            mint.parse::<Pubkey>(),
                            amount.parse::<u64>()
                        ) {
                            result.push((mint_pubkey, amount_u64));
                        }
                    }
                }
            }
        }
        
        Ok(result)
    }

    async fn estimate_token_value(&self, mint: &Pubkey, amount: u64) -> Result<u64> {
        let price_lamports_per_token = match mint.to_string().as_str() {
            "So11111111111111111111111111111111111111112" => 1_000_000_000,
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => 150_000_000,
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" => 140_000_000,
            _ => 1_000_000,
        };
        
        Ok((amount as u128 * price_lamports_per_token as u128 / 1_000_000_000) as u64)
    }

    pub async fn analyze_competitor_strategy(&self, address: &Pubkey) -> Result<StrategyType> {
        let recent_txs = self.get_recent_transactions(address, 100).await?;
        
        let mut strategy_scores = HashMap::new();
        strategy_scores.insert(StrategyType::Arbitrage, 0.0);
        strategy_scores.insert(StrategyType::FrontRunning, 0.0);
        strategy_scores.insert(StrategyType::Sandwiching, 0.0);
        strategy_scores.insert(StrategyType::Liquidation, 0.0);
        strategy_scores.insert(StrategyType::JitLiquidity, 0.0);
        strategy_scores.insert(StrategyType::AtomicArbitrage, 0.0);

        for tx in recent_txs {
            if let Some(meta) = tx.transaction.meta {
                let pre_balances = meta.pre_balances;
                let post_balances = meta.post_balances;
                let log_messages = meta.log_messages.unwrap_or_default();
                
                if self.is_arbitrage_pattern(&pre_balances, &post_balances, &log_messages) {
                    *strategy_scores.get_mut(&StrategyType::Arbitrage).unwrap() += 1.0;
                }
                
                if self.is_sandwich_pattern(&tx) {
                    *strategy_scores.get_mut(&StrategyType::Sandwiching).unwrap() += 1.0;
                }
                
                if self.is_liquidation_pattern(&log_messages) {
                    *strategy_scores.get_mut(&StrategyType::Liquidation).unwrap() += 1.0;
                }
            }
        }

        let dominant_strategy = strategy_scores.iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(k, _)| *k)
            .unwrap_or(StrategyType::Unknown);

        if let Some(mut profile) = self.competitors.get_mut(address) {
            profile.strategy_fingerprint = dominant_strategy;
        }

        Ok(dominant_strategy)
    }

    async fn get_recent_transactions(
        &self, 
        address: &Pubkey, 
        limit: usize
    ) -> Result<Vec<EncodedConfirmedTransactionWithStatusMeta>> {
        let signatures = self.rpc_client
            .get_signatures_for_address_with_config(
                address,
                solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                    limit: Some(limit),
                    ..Default::default()
                }
            )
            .await?;

        let mut transactions = Vec::new();
        for sig_info in signatures.iter().take(limit) {
            if let Ok(tx) = self.rpc_client
                .get_transaction(
                    &sig_info.signature.parse::<Signature>()?,
                    UiTransactionEncoding::Json
                )
                .await
            {
                transactions.push(tx);
            }
        }

        Ok(transactions)
    }

        fn is_arbitrage_pattern(&self, pre: &[u64], post: &[u64], logs: &[String]) -> bool {
        if pre.len() < 3 || post.len() < 3 {
            return false;
        }

        let balance_changes: Vec<i64> = pre.iter().zip(post.iter())
            .map(|(p, q)| *q as i64 - *p as i64)
            .collect();

        let positive_changes = balance_changes.iter().filter(|&&x| x > 0).count();
        let negative_changes = balance_changes.iter().filter(|&&x| x < 0).count();

        let has_swap_logs = logs.iter().any(|log| 
            log.contains("Swap") || log.contains("Exchange") || log.contains("Route")
        );

        positive_changes >= 2 && negative_changes >= 2 && has_swap_logs
    }

    fn is_sandwich_pattern(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> bool {
        if let Some(meta) = &tx.transaction.meta {
            if let Some(inner_instructions) = &meta.inner_instructions {
                if inner_instructions.len() >= 3 {
                    let has_victim_pattern = meta.log_messages
                        .as_ref()
                        .map(|logs| logs.iter().any(|log| 
                            log.contains("Program log: Error") || 
                            log.contains("slippage")
                        ))
                        .unwrap_or(false);
                    
                    return inner_instructions.len() >= 3 && !has_victim_pattern;
                }
            }
        }
        false
    }

    fn is_liquidation_pattern(&self, logs: &[String]) -> bool {
        logs.iter().any(|log| 
            log.contains("Liquidate") || 
            log.contains("Repay") && log.contains("Seize") ||
            log.contains("liquidation")
        )
    }

    fn count_open_positions(&self, address: &Pubkey) -> u32 {
        self.position_cache
            .get(address)
            .map(|positions| 
                positions.iter()
                    .filter(|p| p.exit_time.is_none())
                    .count() as u32
            )
            .unwrap_or(0)
    }

    pub async fn track_position_entry(
        &self,
        competitor: Pubkey,
        token_mint: Pubkey,
        size: u64,
        entry_price: f64,
        strategy: StrategyType,
    ) -> Result<()> {
        let position = PositionTracker {
            token_mint,
            entry_price,
            size,
            entry_time: self.current_timestamp(),
            exit_time: None,
            pnl: None,
            strategy,
        };

        if let Some(mut cache) = self.position_cache.get_mut(&competitor) {
            if cache.len() >= POSITION_CACHE_SIZE {
                cache.pop_front();
            }
            cache.push_back(position.clone());
        }

        let _ = self.update_channel.send(
            CompetitorUpdate::PositionOpened(competitor, position)
        ).await;

        Ok(())
    }

    pub async fn track_position_exit(
        &self,
        competitor: Pubkey,
        token_mint: Pubkey,
        exit_price: f64,
    ) -> Result<()> {
        if let Some(mut cache) = self.position_cache.get_mut(&competitor) {
            if let Some(position) = cache.iter_mut()
                .rev()
                .find(|p| p.token_mint == token_mint && p.exit_time.is_none()) 
            {
                position.exit_time = Some(self.current_timestamp());
                let pnl = (exit_price - position.entry_price) / position.entry_price * 100.0;
                position.pnl = Some(pnl);

                let _ = self.update_channel.send(
                    CompetitorUpdate::PositionClosed(competitor, token_mint, pnl)
                ).await;

                self.update_competitor_stats(&competitor, pnl).await;
            }
        }

        Ok(())
    }

    async fn update_competitor_stats(&self, address: &Pubkey, pnl: f64) {
        if let Some(mut profile) = self.competitors.get_mut(address) {
            let positions = self.position_cache.get(address)
                .map(|cache| cache.clone())
                .unwrap_or_default();

            let completed_positions: Vec<&PositionTracker> = positions.iter()
                .filter(|p| p.pnl.is_some())
                .collect();

            if !completed_positions.is_empty() {
                let wins = completed_positions.iter()
                    .filter(|p| p.pnl.unwrap_or(0.0) > 0.0)
                    .count();
                
                profile.win_rate = wins as f64 / completed_positions.len() as f64;
                
                let total_size: u64 = positions.iter()
                    .map(|p| p.size)
                    .sum();
                profile.avg_position_size = total_size / positions.len().max(1) as u64;

                let capital_used = profile.avg_position_size * positions.len() as u64;
                let returns = completed_positions.iter()
                    .map(|p| p.pnl.unwrap_or(0.0))
                    .sum::<f64>();
                
                profile.capital_efficiency = if capital_used > 0 {
                    (returns / 100.0 * capital_used as f64) / capital_used as f64
                } else {
                    0.0
                };

                profile.risk_score = self.calculate_risk_score(&profile, &positions);
            }

            profile.last_seen = self.current_timestamp();
        }
    }

    fn calculate_risk_score(&self, profile: &CompetitorProfile, positions: &VecDeque<PositionTracker>) -> f64 {
        let mut risk_factors = Vec::new();

        let position_concentration = if !positions.is_empty() {
            let unique_tokens: HashSet<_> = positions.iter()
                .map(|p| p.token_mint)
                .collect();
            1.0 - (unique_tokens.len() as f64 / positions.len() as f64)
        } else {
            0.0
        };
        risk_factors.push(position_concentration * 0.3);

        let avg_position_ratio = profile.avg_position_size as f64 / CAPITAL_THRESHOLD_LAMPORTS as f64;
        risk_factors.push(avg_position_ratio.min(1.0) * 0.3);

        let loss_rate = 1.0 - profile.win_rate;
        risk_factors.push(loss_rate * 0.2);

        let strategy_risk = match profile.strategy_fingerprint {
            StrategyType::Liquidation => 0.8,
            StrategyType::FrontRunning => 0.7,
            StrategyType::Sandwiching => 0.6,
            StrategyType::Arbitrage => 0.4,
            StrategyType::JitLiquidity => 0.5,
            StrategyType::AtomicArbitrage => 0.3,
            StrategyType::Unknown => 0.5,
        };
        risk_factors.push(strategy_risk * 0.2);

        risk_factors.iter().sum::<f64>().min(1.0).max(0.0)
    }

    pub fn get_hot_competitors(&self) -> Vec<Pubkey> {
        self.hot_wallets.read().unwrap().iter().cloned().collect()
    }

    pub fn get_competitor_capital(&self, address: &Pubkey) -> Option<u64> {
        self.capital_history.get(address)
            .and_then(|history| history.back())
            .map(|snapshot| snapshot.total_value_lamports)
    }

    pub fn get_capital_velocity(&self, address: &Pubkey, window_ms: u64) -> f64 {
        if let Some(history) = self.capital_history.get(address) {
            let current_time = self.current_timestamp();
            let window_start = current_time.saturating_sub(window_ms);
            
            let relevant_snapshots: Vec<&CapitalSnapshot> = history.iter()
                .filter(|s| s.timestamp >= window_start)
                .collect();

            if relevant_snapshots.len() < 2 {
                return 0.0;
            }

            let capital_changes: f64 = relevant_snapshots.windows(2)
                .map(|w| (w[1].total_value_lamports as f64 - w[0].total_value_lamports as f64).abs())
                .sum();

            let time_span = (relevant_snapshots.last().unwrap().timestamp - 
                           relevant_snapshots.first().unwrap().timestamp) as f64;

            if time_span > 0.0 {
                capital_changes / time_span * 1000.0
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    pub async fn get_network_capital_distribution(&self) -> HashMap<StrategyType, u64> {
        let mut distribution = HashMap::new();
        
        for entry in self.competitors.iter() {
            let strategy = entry.value().strategy_fingerprint;
            let capital = self.get_competitor_capital(entry.key()).unwrap_or(0);
            *distribution.entry(strategy).or_insert(0) += capital;
        }

        distribution
    }

    pub fn get_competitor_profile(&self, address: &Pubkey) -> Option<CompetitorProfile> {
        self.competitors.get(address).map(|entry| entry.clone())
    }

    pub async fn predict_competitor_move(&self, address: &Pubkey) -> Option<(Pubkey, f64)> {
        let profile = self.get_competitor_profile(address)?;
        let recent_positions = self.position_cache.get(address)?;
        
        if recent_positions.len() < 5 {
            return None;
        }

        let token_frequency: HashMap<Pubkey, usize> = recent_positions.iter()
            .fold(HashMap::new(), |mut map, pos| {
                *map.entry(pos.token_mint).or_insert(0) += 1;
                map
            });

        let most_traded = token_frequency.iter()
            .max_by_key(|&(_, count)| count)
            .map(|(token, _)| *token)?;

        let confidence = token_frequency[&most_traded] as f64 / recent_positions.len() as f64;

        Some((most_traded, confidence))
    }

    async fn handle_update(&self, update: CompetitorUpdate) {
        match update {
            CompetitorUpdate::NewCompetitor(pubkey) => {
                log::info!("New competitor tracked: {}", pubkey);
            }
            CompetitorUpdate::CapitalChange(pubkey, amount) => {
                if amount > CAPITAL_THRESHOLD_LAMPORTS * 10 {
                    log::warn!("Large capital movement detected: {} moved {} SOL", 
                        pubkey, amount / 1_000_000_000);
                }
            }
            CompetitorUpdate::PositionOpened(pubkey, position) => {
                log::debug!("Position opened by {}: {:?}", pubkey, position);
            }
            CompetitorUpdate::PositionClosed(pubkey, token, pnl) => {
                if pnl.abs() > 50.0 {
                    log::info!("Significant PnL by {}: {}% on {}", pubkey, pnl, token);
                }
            }
            CompetitorUpdate::StrategyDetected(pubkey, strategy) => {
                log::info!("Strategy detected for {}: {:?}", pubkey, strategy);
            }
        }
    }

    async fn update_metrics(&self) {
        let mut metrics = self.metrics.write().await;
        
        metrics.total_tracked = self.competitors.len();
        
        let current_time = self.current_timestamp();
        let hour_ago = current_time - 3_600_000;
        
        metrics.active_last_hour = self.competitors.iter()
            .filter(|entry| entry.value().last_seen >= hour_ago)
            .count();

        metrics.total_capital_tracked = self.competitors.iter()
            .map(|entry| self.get_competitor_capital(entry.key()).unwrap_or(0))
            .sum();

        let strategy_counts: HashMap<StrategyType, usize> = self.competitors.iter()
            .fold(HashMap::new(), |mut map, entry| {
                *map.entry(entry.value().strategy_fingerprint).or_insert(0) += 1;
                map
            });

        metrics.dominant_strategy = strategy_counts.iter()
            .max_by_key(|&(_, count)| count)
            .map(|(strategy, _)| *strategy)
            .unwrap_or(StrategyType::Unknown);

        let total_positions: Vec<u64> = self.position_cache.iter()
            .flat_map(|entry| entry.value().iter().map(|p| p.size))
            .collect();

        metrics.avg_transaction_size = if !total_positions.is_empty() {
            total_positions.iter().sum::<u64>() / total_positions.len() as u64
        } else {
            0
        };

        metrics.network_congestion_score = self.calculate_congestion_score();
    }

    fn calculate_congestion_score(&self) -> f64 {
        let active_competitors = self.hot_wallets.read().unwrap().len();
        let base_score = (active_competitors as f64 / MAX_COMPETITORS as f64).min(1.0);
        
        let velocity_sum: f64 = self.hot_wallets.read().unwrap().iter()
            .map(|addr| self.get_capital_velocity(addr, 60_000))
            .sum();

        let velocity_score = (velocity_sum / 1000.0).min(1.0);
        
        (base_score * 0.6 + velocity_score * 0.4).min(1.0).max(0.0)
    }

    async fn evict_inactive_competitor(&self) {
        let current_time = self.current_timestamp();
        let eviction_threshold = current_time - 86_400_000; // 24 hours

        let to_evict: Vec<Pubkey> = self.competitors.iter()
            .filter(|entry| entry.value().last_seen < eviction_threshold)
            .map(|entry| *entry.key())
            .take(10)
            .collect();

        for pubkey in to_evict {
            self.competitors.remove(&pubkey);
                        self.capital_history.remove(&pubkey);
            self.position_cache.remove(&pubkey);
            self.hot_wallets.write().unwrap().remove(&pubkey);
        }
    }

    fn current_timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub async fn get_competitor_risk_report(&self, address: &Pubkey) -> Option<RiskReport> {
        let profile = self.get_competitor_profile(address)?;
        let capital = self.get_competitor_capital(address)?;
        let velocity = self.get_capital_velocity(address, 3_600_000);
        
        let positions = self.position_cache.get(address)
            .map(|cache| cache.clone())
            .unwrap_or_default();

        let open_positions = positions.iter()
            .filter(|p| p.exit_time.is_none())
            .count();

        let avg_pnl = positions.iter()
            .filter_map(|p| p.pnl)
            .fold(0.0, |acc, pnl| acc + pnl) / positions.len().max(1) as f64;

        let max_drawdown = positions.iter()
            .filter_map(|p| p.pnl)
            .filter(|&pnl| pnl < 0.0)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);

        Some(RiskReport {
            address: *address,
            risk_score: profile.risk_score,
            capital_at_risk: capital,
            open_positions: open_positions as u32,
            avg_pnl,
            max_drawdown,
            capital_velocity: velocity,
            strategy: profile.strategy_fingerprint,
            last_updated: self.current_timestamp(),
        })
    }

    pub fn get_top_competitors_by_capital(&self, limit: usize) -> Vec<(Pubkey, u64)> {
        let mut competitors_with_capital: Vec<(Pubkey, u64)> = self.competitors.iter()
            .filter_map(|entry| {
                let capital = self.get_competitor_capital(entry.key())?;
                Some((*entry.key(), capital))
            })
            .collect();

        competitors_with_capital.sort_by(|a, b| b.1.cmp(&a.1));
        competitors_with_capital.truncate(limit);
        competitors_with_capital
    }

    pub fn get_competitors_by_strategy(&self, strategy: StrategyType) -> Vec<Pubkey> {
        self.competitors.iter()
            .filter(|entry| entry.value().strategy_fingerprint == strategy)
            .map(|entry| *entry.key())
            .collect()
    }

    pub async fn detect_coordinated_activity(&self) -> Vec<CoordinatedGroup> {
        let mut groups = Vec::new();
        let time_window = 5000; // 5 seconds
        let current_time = self.current_timestamp();

        let recent_active: Vec<Pubkey> = self.competitors.iter()
            .filter(|entry| entry.value().last_seen >= current_time - time_window)
            .map(|entry| *entry.key())
            .collect();

        if recent_active.len() < 2 {
            return groups;
        }

        for i in 0..recent_active.len() {
            for j in i+1..recent_active.len() {
                let addr1 = &recent_active[i];
                let addr2 = &recent_active[j];

                if self.are_coordinated(addr1, addr2).await {
                    let mut group_members = vec![*addr1, *addr2];
                    
                    for k in j+1..recent_active.len() {
                        let addr3 = &recent_active[k];
                        if self.are_coordinated(addr1, addr3).await {
                            group_members.push(*addr3);
                        }
                    }

                    if group_members.len() >= 2 {
                        groups.push(CoordinatedGroup {
                            members: group_members,
                            detected_at: current_time,
                            confidence: 0.8,
                        });
                    }
                }
            }
        }

        groups
    }

    async fn are_coordinated(&self, addr1: &Pubkey, addr2: &Pubkey) -> bool {
        let positions1 = self.position_cache.get(addr1)
            .map(|cache| cache.clone())
            .unwrap_or_default();
        
        let positions2 = self.position_cache.get(addr2)
            .map(|cache| cache.clone())
            .unwrap_or_default();

        if positions1.is_empty() || positions2.is_empty() {
            return false;
        }

        let recent_positions1: Vec<&PositionTracker> = positions1.iter()
            .filter(|p| p.entry_time >= self.current_timestamp() - 300_000)
            .collect();

        let recent_positions2: Vec<&PositionTracker> = positions2.iter()
            .filter(|p| p.entry_time >= self.current_timestamp() - 300_000)
            .collect();

        let common_tokens: HashSet<Pubkey> = recent_positions1.iter()
            .map(|p| p.token_mint)
            .collect::<HashSet<_>>()
            .intersection(&recent_positions2.iter()
                .map(|p| p.token_mint)
                .collect())
            .cloned()
            .collect();

        let token_overlap = common_tokens.len() as f64 / 
            recent_positions1.len().min(recent_positions2.len()).max(1) as f64;

        let time_correlation = recent_positions1.iter()
            .any(|p1| recent_positions2.iter()
                .any(|p2| p1.token_mint == p2.token_mint && 
                    (p1.entry_time as i64 - p2.entry_time as i64).abs() < 5000));

        token_overlap > 0.7 && time_correlation
    }

    pub fn estimate_competitor_impact(&self, address: &Pubkey) -> f64 {
        let capital = self.get_competitor_capital(address).unwrap_or(0) as f64;
        let velocity = self.get_capital_velocity(address, 3_600_000);
        let profile = self.get_competitor_profile(address);

        let strategy_weight = match profile.as_ref().map(|p| p.strategy_fingerprint) {
            Some(StrategyType::FrontRunning) => 1.5,
            Some(StrategyType::Sandwiching) => 1.4,
            Some(StrategyType::Liquidation) => 1.3,
            Some(StrategyType::Arbitrage) => 1.1,
            Some(StrategyType::JitLiquidity) => 1.2,
            Some(StrategyType::AtomicArbitrage) => 1.0,
            _ => 1.0,
        };

        let win_rate = profile.as_ref().map(|p| p.win_rate).unwrap_or(0.5);
        let efficiency = profile.as_ref().map(|p| p.capital_efficiency).unwrap_or(0.0);

        let base_impact = (capital / CAPITAL_THRESHOLD_LAMPORTS as f64).min(10.0);
        let velocity_factor = (velocity / 100.0).min(2.0).max(0.5);
        let performance_factor = win_rate * 2.0 + efficiency;

        base_impact * velocity_factor * strategy_weight * performance_factor
    }

    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
    }
}

#[derive(Debug, Clone)]
pub struct RiskReport {
    pub address: Pubkey,
    pub risk_score: f64,
    pub capital_at_risk: u64,
    pub open_positions: u32,
    pub avg_pnl: f64,
    pub max_drawdown: f64,
    pub capital_velocity: f64,
    pub strategy: StrategyType,
    pub last_updated: u64,
}

#[derive(Debug, Clone)]
pub struct CoordinatedGroup {
    pub members: Vec<Pubkey>,
    pub detected_at: u64,
    pub confidence: f64,
}

impl Clone for CompetitorCapitalTracker {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            competitors: self.competitors.clone(),
            capital_history: self.capital_history.clone(),
            position_cache: self.position_cache.clone(),
            hot_wallets: self.hot_wallets.clone(),
            metrics: self.metrics.clone(),
            update_channel: self.update_channel.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    #[tokio::test]
    async fn test_competitor_tracking() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let tracker = CompetitorCapitalTracker::new(rpc_client, vec![]).await.unwrap();
        
        let test_pubkey = Pubkey::new_unique();
        tracker.add_competitor(test_pubkey).await.unwrap();
        
        assert!(tracker.get_competitor_profile(&test_pubkey).is_some());
    }

    #[tokio::test]
    async fn test_capital_velocity() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let tracker = CompetitorCapitalTracker::new(rpc_client, vec![]).await.unwrap();
        
        let test_pubkey = Pubkey::new_unique();
        tracker.add_competitor(test_pubkey).await.unwrap();
        
        let velocity = tracker.get_capital_velocity(&test_pubkey, 60_000);
        assert_eq!(velocity, 0.0);
    }
}

