use solana_sdk::{
    account::Account,
    clock::Clock,
    pubkey::Pubkey,
    signature::Signature,
};
use solana_client::rpc_client::RpcClient;
use spl_token::state::Mint;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use serde::{Deserialize, Serialize};
use anchor_lang::prelude::*;

const TOXICITY_THRESHOLD: f64 = 0.75;
const HISTORY_WINDOW: usize = 1000;
const FAILURE_RATE_THRESHOLD: f64 = 0.65;
const SLIPPAGE_SPIKE_THRESHOLD: f64 = 0.15;
const WHALE_THRESHOLD_PCT: f64 = 0.02;
const MEV_COMPETITION_THRESHOLD: u64 = 5;
const RUGPULL_LIQUIDITY_THRESHOLD: f64 = 0.90;
const WASH_TRADE_SIMILARITY_THRESHOLD: f64 = 0.95;
const ACCOUNT_AGE_THRESHOLD: i64 = 86400;
const MAX_CACHE_SIZE: usize = 10000;
const CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToxicityScore {
    pub score: f64,
    pub sandwich_risk: f64,
    pub failure_probability: f64,
    pub competition_level: f64,
    pub rugpull_risk: f64,
    pub wash_trade_probability: f64,
    pub whale_influence: f64,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
pub struct TransactionMetrics {
    pub signature: Signature,
    pub accounts: Vec<Pubkey>,
    pub program_id: Pubkey,
    pub amount: u64,
    pub slot: u64,
    pub timestamp: i64,
    pub success: bool,
    pub compute_units: u64,
    pub priority_fee: u64,
}

#[derive(Debug, Clone)]
pub struct AccountMetrics {
    pub pubkey: Pubkey,
    pub creation_slot: u64,
    pub transaction_count: u64,
    pub failure_count: u64,
    pub avg_priority_fee: u64,
    pub last_update: Instant,
    pub interaction_patterns: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct PoolMetrics {
    pub pool_address: Pubkey,
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub liquidity: u64,
    pub volume_24h: u64,
    pub price_impact: f64,
    pub historical_volatility: f64,
}

pub struct ToxicFlowClassifier {
    rpc_client: Arc<RpcClient>,
    transaction_history: Arc<RwLock<VecDeque<TransactionMetrics>>>,
    account_cache: Arc<RwLock<HashMap<Pubkey, AccountMetrics>>>,
    pool_cache: Arc<RwLock<HashMap<Pubkey, PoolMetrics>>>,
    toxicity_cache: Arc<RwLock<HashMap<String, ToxicityScore>>>,
}

impl ToxicFlowClassifier {
    pub fn new(rpc_url: &str) -> Self {
        Self {
            rpc_client: Arc::new(RpcClient::new(rpc_url.to_string())),
            transaction_history: Arc::new(RwLock::new(VecDeque::with_capacity(HISTORY_WINDOW))),
            account_cache: Arc::new(RwLock::new(HashMap::with_capacity(MAX_CACHE_SIZE))),
            pool_cache: Arc::new(RwLock::new(HashMap::with_capacity(1000))),
            toxicity_cache: Arc::new(RwLock::new(HashMap::with_capacity(MAX_CACHE_SIZE))),
        }
    }

    pub async fn classify_transaction(
        &self,
        tx_metrics: &TransactionMetrics,
        pool_address: Option<Pubkey>,
    ) -> Result<ToxicityScore, Box<dyn std::error::Error>> {
        let cache_key = format!("{:?}_{:?}", tx_metrics.signature, pool_address);
        
        if let Some(cached_score) = self.get_cached_score(&cache_key) {
            if cached_score.timestamp.elapsed() < CACHE_TTL {
                return Ok(cached_score);
            }
        }

        let sandwich_risk = self.calculate_sandwich_risk(tx_metrics, pool_address).await?;
        let failure_probability = self.calculate_failure_probability(&tx_metrics.accounts).await?;
        let competition_level = self.calculate_competition_level(tx_metrics).await?;
        let rugpull_risk = if let Some(pool) = pool_address {
            self.calculate_rugpull_risk(&pool).await?
        } else { 0.0 };
        let wash_trade_probability = self.calculate_wash_trade_probability(tx_metrics).await?;
        let whale_influence = self.calculate_whale_influence(tx_metrics, pool_address).await?;

        let weighted_score = sandwich_risk * 0.25 +
            failure_probability * 0.20 +
            competition_level * 0.15 +
            rugpull_risk * 0.20 +
            wash_trade_probability * 0.10 +
            whale_influence * 0.10;

        let toxicity_score = ToxicityScore {
            score: weighted_score.min(1.0).max(0.0),
            sandwich_risk,
            failure_probability,
            competition_level,
            rugpull_risk,
            wash_trade_probability,
            whale_influence,
            timestamp: Instant::now(),
        };

        self.cache_toxicity_score(cache_key, toxicity_score.clone());
        self.update_transaction_history(tx_metrics.clone());

        Ok(toxicity_score)
    }

    async fn calculate_sandwich_risk(
        &self,
        tx_metrics: &TransactionMetrics,
        pool_address: Option<Pubkey>,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let history = self.transaction_history.read().unwrap();
        let current_slot = tx_metrics.slot;
        
        let mut risk_score = 0.0;
        let mut pattern_matches = 0;
        
        for (i, historical_tx) in history.iter().enumerate() {
            if historical_tx.slot >= current_slot.saturating_sub(5) && 
               historical_tx.slot <= current_slot {
                
                if self.is_sandwich_pattern(tx_metrics, historical_tx, pool_address) {
                    pattern_matches += 1;
                    risk_score += 1.0 / (i + 1) as f64;
                }
            }
        }

        let high_priority_multiplier = if tx_metrics.priority_fee > 1000000 {
            1.5
        } else {
            1.0
        };

        let final_risk = (risk_score * high_priority_multiplier).min(1.0);
        Ok(final_risk)
    }

    fn is_sandwich_pattern(
        &self,
        current_tx: &TransactionMetrics,
        historical_tx: &TransactionMetrics,
        pool_address: Option<Pubkey>,
    ) -> bool {
        if pool_address.is_none() {
            return false;
        }

        let pool = pool_address.unwrap();
        let shares_pool = current_tx.accounts.contains(&pool) && 
                         historical_tx.accounts.contains(&pool);
        
        let slot_diff = current_tx.slot.abs_diff(historical_tx.slot);
        let is_close_slot = slot_diff <= 2;
        
        let similar_amount = (current_tx.amount as f64 / historical_tx.amount as f64).abs() > 0.8 &&
                            (current_tx.amount as f64 / historical_tx.amount as f64).abs() < 1.2;

        shares_pool && is_close_slot && similar_amount
    }

    async fn calculate_failure_probability(
        &self,
        accounts: &[Pubkey],
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mut total_failure_rate = 0.0;
        let mut account_count = 0;

        for account in accounts {
            if let Some(metrics) = self.get_account_metrics(account).await? {
                if metrics.transaction_count > 0 {
                    let failure_rate = metrics.failure_count as f64 / metrics.transaction_count as f64;
                    total_failure_rate += failure_rate;
                    account_count += 1;
                }
            }
        }

        if account_count == 0 {
            return Ok(0.5);
        }

        Ok((total_failure_rate / account_count as f64).min(1.0))
    }

    async fn calculate_competition_level(
        &self,
        tx_metrics: &TransactionMetrics,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let history = self.transaction_history.read().unwrap();
        let current_slot = tx_metrics.slot;
        
        let competing_txs = history.iter()
            .filter(|tx| {
                tx.slot >= current_slot.saturating_sub(10) &&
                tx.slot <= current_slot &&
                tx.program_id == tx_metrics.program_id &&
                tx.priority_fee > 500000
            })
            .count();

        let priority_percentile = self.calculate_priority_percentile(tx_metrics.priority_fee);
        let competition_score = (competing_txs as f64 / MEV_COMPETITION_THRESHOLD as f64).min(1.0);
        
        Ok((competition_score * 0.6 + priority_percentile * 0.4).min(1.0))
    }

    fn calculate_priority_percentile(&self, priority_fee: u64) -> f64 {
        let history = self.transaction_history.read().unwrap();
        let mut fees: Vec<u64> = history.iter()
            .map(|tx| tx.priority_fee)
            .collect();
        
        if fees.is_empty() {
            return 0.5;
        }

        fees.sort_unstable();
        let position = fees.binary_search(&priority_fee).unwrap_or_else(|i| i);
        position as f64 / fees.len() as f64
    }

    async fn calculate_rugpull_risk(
        &self,
        pool_address: &Pubkey,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let pool_metrics = self.get_pool_metrics(pool_address).await?;
        
        if pool_metrics.is_none() {
            return Ok(0.5);
        }

        let pool = pool_metrics.unwrap();
        let mut risk_factors = Vec::new();

        let liquidity_ratio = if pool.volume_24h > 0 {
            pool.liquidity as f64 / pool.volume_24h as f64
        } else {
            1.0
        };
        
        if liquidity_ratio < 0.1 {
            risk_factors.push(0.8);
        }

        if pool.historical_volatility > 0.5 {
            risk_factors.push(0.6);
        }

        let token_a_age = self.get_token_age(&pool.token_a).await?;
        let token_b_age = self.get_token_age(&pool.token_b).await?;
        
        if token_a_age < ACCOUNT_AGE_THRESHOLD || token_b_age < ACCOUNT_AGE_THRESHOLD {
            risk_factors.push(0.9);
        }

        if pool.liquidity < 100000 {
            risk_factors.push(0.7);
        }

        if risk_factors.is_empty() {
            return Ok(0.1);
        }

        let avg_risk = risk_factors.iter().sum::<f64>() / risk_factors.len() as f64;
        Ok(avg_risk.min(1.0))
    }

    async fn calculate_wash_trade_probability(
        &self,
        tx_metrics: &TransactionMetrics,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let history = self.transaction_history.read().unwrap();
        let mut pattern_score = 0.0;
        let mut pattern_count = 0;

        for historical_tx in history.iter() {
            if self.is_wash_trade_pattern(tx_metrics, historical_tx) {
                pattern_count += 1;
                let time_weight = 1.0 / (1.0 + (tx_metrics.slot - historical_tx.slot) as f64 / 100.0);
                pattern_score += time_weight;
            }
        }

        let normalized_score = if pattern_count > 0 {
            (pattern_score / pattern_count as f64).min(1.0)
        } else {
            0.0
        };

        Ok(normalized_score)
    }

    fn is_wash_trade_pattern(
        &self,
        tx1: &TransactionMetrics,
        tx2: &TransactionMetrics,
    ) -> bool {
        let amount_similarity = (tx1.amount as f64 - tx2.amount as f64).abs() / 
                               (tx1.amount as f64).max(1.0);
        
        let shared_accounts = tx1.accounts.iter()
            .filter(|acc| tx2.accounts.contains(acc))
            .count();
        
        let account_overlap = shared_accounts as f64 / tx1.accounts.len().min(tx2.accounts.len()) as f64;
        
        amount_similarity < (1.0 - WASH_TRADE_SIMILARITY_THRESHOLD) &&
        account_overlap > 0.5 &&
        tx1.program_id == tx2.program_id
    }

       async fn calculate_whale_influence(
        &self,
        tx_metrics: &TransactionMetrics,
        pool_address: Option<Pubkey>,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        if pool_address.is_none() {
            return Ok(0.0);
        }

        let pool = pool_address.unwrap();
        let pool_metrics = self.get_pool_metrics(&pool).await?;
        
        if pool_metrics.is_none() {
            return Ok(0.5);
        }

        let pool_data = pool_metrics.unwrap();
        let whale_threshold = (pool_data.liquidity as f64 * WHALE_THRESHOLD_PCT) as u64;
        
        let mut influence_score = 0.0;
        
        if tx_metrics.amount >= whale_threshold {
            let impact_ratio = tx_metrics.amount as f64 / pool_data.liquidity as f64;
            influence_score = impact_ratio * 2.0;
        }

        let price_impact_multiplier = if pool_data.price_impact > SLIPPAGE_SPIKE_THRESHOLD {
            1.5
        } else {
            1.0
        };

        let history = self.transaction_history.read().unwrap();
        let recent_whale_activity = history.iter()
            .filter(|tx| {
                tx.slot >= tx_metrics.slot.saturating_sub(50) &&
                tx.accounts.iter().any(|acc| tx_metrics.accounts.contains(acc)) &&
                tx.amount >= whale_threshold
            })
            .count();

        let activity_score = (recent_whale_activity as f64 / 10.0).min(1.0);
        
        let final_score = ((influence_score * price_impact_multiplier * 0.6) + 
                          (activity_score * 0.4)).min(1.0);
        
        Ok(final_score)
    }

    async fn get_account_metrics(
        &self,
        account: &Pubkey,
    ) -> Result<Option<AccountMetrics>, Box<dyn std::error::Error>> {
        let mut cache = self.account_cache.write().unwrap();
        
        if let Some(metrics) = cache.get(account) {
            if metrics.last_update.elapsed() < CACHE_TTL {
                return Ok(Some(metrics.clone()));
            }
        }

        let account_info = match self.rpc_client.get_account(account) {
            Ok(info) => info,
            Err(_) => return Ok(None),
        };

        let slot = self.rpc_client.get_slot()?;
        let creation_slot = self.estimate_creation_slot(&account_info, slot)?;
        
        let signatures = self.rpc_client.get_signatures_for_address(account)?;
        let transaction_count = signatures.len() as u64;
        
        let mut failure_count = 0u64;
        let mut total_priority_fee = 0u64;
        let mut interaction_patterns = Vec::new();
        
        for (idx, sig_info) in signatures.iter().take(100).enumerate() {
            if sig_info.err.is_some() {
                failure_count += 1;
            }
            
            if let Ok(tx) = self.rpc_client.get_transaction(&sig_info.signature, 
                solana_transaction_status::UiTransactionEncoding::Base64) {
                if let Some(meta) = tx.transaction.meta {
                    if let Some(fee) = meta.fee {
                        total_priority_fee += fee.saturating_sub(5000);
                    }
                }
            }
            
            if idx < 20 {
                interaction_patterns.push(sig_info.slot);
            }
        }

        let avg_priority_fee = if transaction_count > 0 {
            total_priority_fee / transaction_count.min(100)
        } else {
            0
        };

        let metrics = AccountMetrics {
            pubkey: *account,
            creation_slot,
            transaction_count,
            failure_count,
            avg_priority_fee,
            last_update: Instant::now(),
            interaction_patterns,
        };

        if cache.len() >= MAX_CACHE_SIZE {
            self.evict_oldest_cache_entries(&mut cache);
        }

        cache.insert(*account, metrics.clone());
        Ok(Some(metrics))
    }

    async fn get_pool_metrics(
        &self,
        pool_address: &Pubkey,
    ) -> Result<Option<PoolMetrics>, Box<dyn std::error::Error>> {
        let mut cache = self.pool_cache.write().unwrap();
        
        if let Some(metrics) = cache.get(pool_address) {
            return Ok(Some(metrics.clone()));
        }

        let pool_account = match self.rpc_client.get_account(pool_address) {
            Ok(account) => account,
            Err(_) => return Ok(None),
        };

        let pool_data = self.parse_pool_data(&pool_account)?;
        
        if let Some(parsed_pool) = pool_data {
            let volume_24h = self.calculate_24h_volume(pool_address).await?;
            let price_impact = self.calculate_price_impact(&parsed_pool)?;
            let historical_volatility = self.calculate_volatility(pool_address).await?;
            
            let metrics = PoolMetrics {
                pool_address: *pool_address,
                token_a: parsed_pool.0,
                token_b: parsed_pool.1,
                liquidity: parsed_pool.2,
                volume_24h,
                price_impact,
                historical_volatility,
            };

            cache.insert(*pool_address, metrics.clone());
            Ok(Some(metrics))
        } else {
            Ok(None)
        }
    }

    fn parse_pool_data(&self, account: &Account) -> Result<Option<(Pubkey, Pubkey, u64)>, Box<dyn std::error::Error>> {
        if account.data.len() < 128 {
            return Ok(None);
        }

        let token_a = Pubkey::new_from_array(
            account.data[8..40].try_into()
                .map_err(|_| "Invalid token A pubkey")?
        );
        let token_b = Pubkey::new_from_array(
            account.data[40..72].try_into()
                .map_err(|_| "Invalid token B pubkey")?
        );
        
        let liquidity_bytes: [u8; 8] = account.data[72..80].try_into()
            .map_err(|_| "Invalid liquidity bytes")?;
        let liquidity = u64::from_le_bytes(liquidity_bytes);

        Ok(Some((token_a, token_b, liquidity)))
    }

    async fn calculate_24h_volume(
        &self,
        pool_address: &Pubkey,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let current_slot = self.rpc_client.get_slot()?;
        let slots_per_day = 216000u64;
        let start_slot = current_slot.saturating_sub(slots_per_day);
        
        let signatures = self.rpc_client.get_signatures_for_address(pool_address)?;
        let mut volume = 0u64;
        
        for sig_info in signatures.iter() {
            if sig_info.slot < start_slot {
                break;
            }
            
            if sig_info.err.is_none() {
                if let Ok(tx) = self.rpc_client.get_transaction(&sig_info.signature,
                    solana_transaction_status::UiTransactionEncoding::Base64) {
                    if let Some(meta) = tx.transaction.meta {
                        if let Some(pre) = meta.pre_balances.get(0) {
                            if let Some(post) = meta.post_balances.get(0) {
                                volume += pre.abs_diff(*post);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(volume)
    }

    fn calculate_price_impact(&self, pool_data: &(Pubkey, Pubkey, u64)) -> Result<f64, Box<dyn std::error::Error>> {
        let liquidity = pool_data.2 as f64;
        let base_impact = 0.003;
        let liquidity_factor = 1000000.0 / liquidity.max(1.0);
        Ok((base_impact * liquidity_factor).min(1.0))
    }

    async fn calculate_volatility(
        &self,
        pool_address: &Pubkey,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let history = self.transaction_history.read().unwrap();
        let pool_txs: Vec<&TransactionMetrics> = history.iter()
            .filter(|tx| tx.accounts.contains(pool_address))
            .collect();
        
        if pool_txs.len() < 10 {
            return Ok(0.3);
        }

        let mut price_changes = Vec::new();
        for window in pool_txs.windows(2) {
            let change = (window[1].amount as f64 - window[0].amount as f64).abs() / 
                        window[0].amount as f64;
            price_changes.push(change);
        }

        let mean = price_changes.iter().sum::<f64>() / price_changes.len() as f64;
        let variance = price_changes.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / price_changes.len() as f64;
        
        Ok(variance.sqrt().min(1.0))
    }

    async fn get_token_age(&self, token: &Pubkey) -> Result<i64, Box<dyn std::error::Error>> {
        let account = match self.rpc_client.get_account(token) {
            Ok(acc) => acc,
            Err(_) => return Ok(0),
        };

        let current_slot = self.rpc_client.get_slot()?;
        let creation_slot = self.estimate_creation_slot(&account, current_slot)?;
        let slot_diff = current_slot.saturating_sub(creation_slot);
        let age_seconds = (slot_diff as f64 * 0.4) as i64;
        
        Ok(age_seconds)
    }

    fn estimate_creation_slot(&self, account: &Account, current_slot: u64) -> Result<u64, Box<dyn std::error::Error>> {
        let data_hash = self.hash_account_data(&account.data);
        let estimated_age = (account.lamports as f64 / 1000000.0).log2() * 100000.0;
        Ok(current_slot.saturating_sub(estimated_age as u64))
    }

    fn hash_account_data(&self, data: &[u8]) -> u64 {
        let mut hash = 0u64;
        for (i, byte) in data.iter().enumerate().take(32) {
            hash = hash.wrapping_add(*byte as u64);
            hash = hash.wrapping_mul(31);
            hash ^= i as u64;
        }
        hash
    }

    fn get_cached_score(&self, key: &str) -> Option<ToxicityScore> {
        let cache = self.toxicity_cache.read().unwrap();
        cache.get(key).cloned()
    }

    fn cache_toxicity_score(&self, key: String, score: ToxicityScore) {
        let mut cache = self.toxicity_cache.write().unwrap();
        
        if cache.len() >= MAX_CACHE_SIZE {
            let oldest_key = cache.iter()
                .min_by_key(|(_, v)| v.timestamp)
                .map(|(k, _)| k.clone());
            
            if let Some(k) = oldest_key {
                cache.remove(&k);
            }
        }
        
        cache.insert(key, score);
    }

    fn update_transaction_history(&self, tx: TransactionMetrics) {
        let mut history = self.transaction_history.write().unwrap();
        
        if history.len() >= HISTORY_WINDOW {
            history.pop_front();
        }
        
        history.push_back(tx);
    }

    fn evict_oldest_cache_entries(&self, cache: &mut HashMap<Pubkey, AccountMetrics>) {
        let mut entries: Vec<(Pubkey, Instant)> = cache.iter()
            .map(|(k, v)| (*k, v.last_update))
            .collect();
        
        entries.sort_by_key(|(_, time)| *time);
        
        for (key, _) in entries.iter().take(MAX_CACHE_SIZE / 10) {
            cache.remove(key);
        }
    }

    pub fn is_toxic(&self, score: &ToxicityScore) -> bool {
        score.score >= TOXICITY_THRESHOLD
    }

    pub fn get_risk_assessment(&self, score: &ToxicityScore) -> String {
        match score.score {
            s if s >= 0.9 => "EXTREME_RISK",
            s if s >= 0.75 => "HIGH_RISK", 
            s if s >= 0.5 => "MEDIUM_RISK",
            s if s >= 0.25 => "LOW_RISK",
            _ => "MINIMAL_RISK",
        }.to_string()
    }
    pub async fn cleanup_stale_data(&self) {
        let now = Instant::now();
        
        {
            let mut account_cache = self.account_cache.write().unwrap();
            account_cache.retain(|_, v| now.duration_since(v.last_update) < CACHE_TTL);
        }
        
        {
            let mut toxicity_cache = self.toxicity_cache.write().unwrap();
            toxicity_cache.retain(|_, v| now.duration_since(v.timestamp) < CACHE_TTL);
        }
    }

    pub fn get_classification_metrics(&self) -> ClassificationMetrics {
        let history = self.transaction_history.read().unwrap();
        let total_txs = history.len();
        let failed_txs = history.iter().filter(|tx| !tx.success).count();
        
        ClassificationMetrics {
            total_transactions: total_txs,
            failed_transactions: failed_txs,
            cache_size: self.toxicity_cache.read().unwrap().len(),
            account_cache_size: self.account_cache.read().unwrap().len(),
            pool_cache_size: self.pool_cache.read().unwrap().len(),
        }
    }

    pub async fn emergency_reset(&self) {
        self.transaction_history.write().unwrap().clear();
        self.account_cache.write().unwrap().clear();
        self.pool_cache.write().unwrap().clear();
        self.toxicity_cache.write().unwrap().clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationMetrics {
    pub total_transactions: usize,
    pub failed_transactions: usize,
    pub cache_size: usize,
    pub account_cache_size: usize,
    pub pool_cache_size: usize,
}

impl Default for ToxicityScore {
    fn default() -> Self {
        Self {
            score: 0.0,
            sandwich_risk: 0.0,
            failure_probability: 0.0,
            competition_level: 0.0,
            rugpull_risk: 0.0,
            wash_trade_probability: 0.0,
            whale_influence: 0.0,
            timestamp: Instant::now(),
        }
    }
}

