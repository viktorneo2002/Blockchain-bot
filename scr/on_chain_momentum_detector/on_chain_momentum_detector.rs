use anyhow::{Context, Result};
use dashmap::DashMap;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{RwLock, Semaphore},
    time::{interval, sleep},
};

const MOMENTUM_WINDOW_SIZE: usize = 120;
const VOLUME_SPIKE_THRESHOLD: f64 = 2.5;
const PRICE_MOMENTUM_THRESHOLD: f64 = 0.0015;
const LIQUIDITY_CHANGE_THRESHOLD: f64 = 0.05;
const MAX_CONCURRENT_CHECKS: usize = 50;
const RETRY_ATTEMPTS: usize = 3;
const RETRY_DELAY_MS: u64 = 100;

#[derive(Debug, Clone)]
pub struct MomentumSignal {
    pub token_mint: Pubkey,
    pub signal_type: SignalType,
    pub strength: f64,
    pub volume_24h: u64,
    pub price_change_rate: f64,
    pub liquidity_depth: u64,
    pub timestamp: u64,
    pub confidence: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SignalType {
    VolumeSpike,
    PriceMomentum,
    LiquidityShift,
    WhaleActivity,
    ArbitrageOpening,
}

#[derive(Debug, Clone)]
struct TokenMetrics {
    price_history: VecDeque<f64>,
    volume_history: VecDeque<u64>,
    liquidity_history: VecDeque<u64>,
    last_update: Instant,
    momentum_score: f64,
    volatility: f64,
}

impl Default for TokenMetrics {
    fn default() -> Self {
        Self {
            price_history: VecDeque::with_capacity(MOMENTUM_WINDOW_SIZE),
            volume_history: VecDeque::with_capacity(MOMENTUM_WINDOW_SIZE),
            liquidity_history: VecDeque::with_capacity(MOMENTUM_WINDOW_SIZE),
            last_update: Instant::now(),
            momentum_score: 0.0,
            volatility: 0.0,
        }
    }
}

pub struct OnChainMomentumDetector {
    rpc_client: Arc<RpcClient>,
    token_metrics: Arc<DashMap<Pubkey, TokenMetrics>>,
    active_signals: Arc<RwLock<Vec<MomentumSignal>>>,
    running: Arc<AtomicBool>,
    detection_semaphore: Arc<Semaphore>,
    last_slot_processed: Arc<AtomicU64>,
}

impl OnChainMomentumDetector {
    pub fn new(rpc_url: &str) -> Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        Ok(Self {
            rpc_client,
            token_metrics: Arc::new(DashMap::new()),
            active_signals: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
            detection_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_CHECKS)),
            last_slot_processed: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        
        let detector = self.clone();
        tokio::spawn(async move {
            if let Err(e) = detector.run_detection_loop().await {
                eprintln!("Momentum detection error: {:?}", e);
            }
        });

        Ok(())
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub async fn get_active_signals(&self) -> Vec<MomentumSignal> {
        self.active_signals.read().await.clone()
    }

    async fn run_detection_loop(&self) -> Result<()> {
        let mut interval = interval(Duration::from_millis(500));

        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;

            let current_slot = self.get_current_slot().await?;
            let last_processed = self.last_slot_processed.load(Ordering::SeqCst);

            if current_slot > last_processed {
                self.process_slot_range(last_processed + 1, current_slot).await?;
                self.last_slot_processed.store(current_slot, Ordering::SeqCst);
            }

            self.update_momentum_scores().await?;
            self.detect_signals().await?;
            self.cleanup_stale_signals().await;
        }

        Ok(())
    }

    async fn get_current_slot(&self) -> Result<u64> {
        for _ in 0..RETRY_ATTEMPTS {
            match self.rpc_client.get_slot().await {
                Ok(slot) => return Ok(slot),
                Err(_) => sleep(Duration::from_millis(RETRY_DELAY_MS)).await,
            }
        }
        anyhow::bail!("Failed to get current slot after retries")
    }

    async fn process_slot_range(&self, start_slot: u64, end_slot: u64) -> Result<()> {
        let slots_to_process = (start_slot..=end_slot).collect::<Vec<_>>();
        let chunks = slots_to_process.chunks(10);

        for chunk in chunks {
            let mut handles = vec![];

            for &slot in chunk {
                let rpc = self.rpc_client.clone();
                let metrics = self.token_metrics.clone();
                let sem = self.detection_semaphore.clone();

                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await.ok()?;
                    Self::process_slot_transactions(rpc, metrics, slot).await.ok()
                });

                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.await;
            }
        }

        Ok(())
    }

    async fn process_slot_transactions(
        rpc: Arc<RpcClient>,
        metrics: Arc<DashMap<Pubkey, TokenMetrics>>,
        slot: u64,
    ) -> Result<()> {
        let block = match rpc.get_block(slot).await {
            Ok(block) => block,
            Err(_) => return Ok(()),
        };

        for tx in block.transactions.iter() {
            if let Some(meta) = &tx.meta {
                if meta.err.is_none() {
                    Self::analyze_transaction(&tx.transaction, metrics.clone()).await?;
                }
            }
        }

        Ok(())
    }

    async fn analyze_transaction(
        tx: &solana_transaction_status::EncodedTransaction,
        metrics: Arc<DashMap<Pubkey, TokenMetrics>>,
    ) -> Result<()> {
        let decoded = match tx {
            solana_transaction_status::EncodedTransaction::Json(ui_tx) => ui_tx,
            _ => return Ok(()),
        };

        if let Some(msg) = &decoded.message {
            for instruction in &msg.instructions {
                if let Some(program_id) = Self::extract_program_id(instruction) {
                    if Self::is_dex_program(&program_id) {
                        Self::extract_token_metrics(instruction, metrics.clone()).await?;
                    }
                }
            }
        }

        Ok(())
    }

    fn extract_program_id(instruction: &solana_transaction_status::UiInstruction) -> Option<Pubkey> {
        match instruction {
            solana_transaction_status::UiInstruction::Compiled(compiled) => {
                compiled.program_id_index.try_into().ok()
            }
            _ => None,
        }
    }

    fn is_dex_program(program_id: &Pubkey) -> bool {
        let dex_programs = [
            "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", // Orca
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // Raydium
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", // Whirlpool
            "EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S", // Lifinity
            "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ", // Saber Stable
        ];

        dex_programs.iter().any(|&prog| {
            program_id == &prog.parse::<Pubkey>().unwrap_or_default()
        })
    }

    async fn extract_token_metrics(
        instruction: &solana_transaction_status::UiInstruction,
        metrics: Arc<DashMap<Pubkey, TokenMetrics>>,
    ) -> Result<()> {
        // Extract swap data from instruction
        if let solana_transaction_status::UiInstruction::Compiled(compiled) = instruction {
            if let Some(data) = &compiled.data {
                let decoded_data = bs58::decode(data).into_vec()?;
                
                if decoded_data.len() >= 32 {
                    let token_mint = Pubkey::new(&decoded_data[0..32]);
                    let amount = u64::from_le_bytes(decoded_data[32..40].try_into()?);
                    let price = f64::from_le_bytes(decoded_data[40..48].try_into()?);
                    
                    metrics.entry(token_mint).and_modify(|m| {
                        m.price_history.push_back(price);
                        if m.price_history.len() > MOMENTUM_WINDOW_SIZE {
                            m.price_history.pop_front();
                        }
                        
                        m.volume_history.push_back(amount);
                        if m.volume_history.len() > MOMENTUM_WINDOW_SIZE {
                            m.volume_history.pop_front();
                        }
                        
                        m.last_update = Instant::now();
                    }).or_insert_with(|| {
                        let mut m = TokenMetrics::default();
                        m.price_history.push_back(price);
                        m.volume_history.push_back(amount);
                        m
                    });
                }
            }
        }

        Ok(())
    }

    async fn update_momentum_scores(&self) -> Result<()> {
        for mut entry in self.token_metrics.iter_mut() {
            let metrics = entry.value_mut();
            
            if metrics.price_history.len() >= 10 {
                let prices: Vec<f64> = metrics.price_history.iter().copied().collect();
                metrics.momentum_score = Self::calculate_momentum(&prices);
                metrics.volatility = Self::calculate_volatility(&prices);
            }
        }

        Ok(())
    }

    fn calculate_momentum(prices: &[f64]) -> f64 {
        if prices.len() < 2 {
            return 0.0;
        }

        let n = prices.len();
        let recent_avg = prices[n/2..].iter().sum::<f64>() / (n/2) as f64;
        let older_avg = prices[..n/2].iter().sum::<f64>() / (n/2) as f64;

        if older_avg > 0.0 {
            (recent_avg - older_avg) / older_avg
        } else {
            0.0
        }
    }

    fn calculate_volatility(prices: &[f64]) -> f64 {
        if prices.len() < 2 {
            return 0.0;
        }

        let mean = prices.iter().sum::<f64>() / prices.len() as f64;
        let variance = prices.iter()
            .map(|p| (p - mean).powi(2))
            .sum::<f64>() / prices.len() as f64;

        variance.sqrt() / mean.max(f64::EPSILON)
    }

    async fn detect_signals(&self) -> Result<()> {
        let mut new_signals = Vec::new();

        for entry in self.token_metrics.iter() {
            let token_mint = *entry.key();
            let metrics = entry.value();

            if let Some(signal) = self.analyze_token_metrics(&token_mint, metrics).await? {
                new_signals.push(signal);
            }
        }

        if !new_signals.is_empty() {
            let mut signals = self.active_signals.write().await;
            signals.extend(new_signals);
            signals.sort_by(|a, b| b.strength.partial_cmp(&a.strength).unwrap());
            signals.truncate(100);
        }

        Ok(())
    }

    async fn analyze_token_metrics(
        &self,
        token_mint: &Pubkey,
        metrics: &TokenMetrics,
    ) -> Result<Option<MomentumSignal>> {
        if metrics.price_history.len() < 20 {
            return Ok(None);
        }

        let volume_spike = self.detect_volume_spike(metrics);
        let price_momentum = metrics.momentum_score.abs() > PRICE_MOMENTUM_THRESHOLD;
        let high_volatility = metrics.volatility > 0.02;

                if volume_spike || price_momentum || high_volatility {
            let signal_type = if volume_spike {
                SignalType::VolumeSpike
            } else if price_momentum {
                SignalType::PriceMomentum
            } else {
                SignalType::ArbitrageOpening
            };

            let strength = self.calculate_signal_strength(metrics, &signal_type);
            let confidence = self.calculate_confidence(metrics);

            if strength > 0.5 && confidence > 0.6 {
                let volume_24h = metrics.volume_history.iter().sum();
                let price_change_rate = metrics.momentum_score;
                let liquidity_depth = self.estimate_liquidity_depth(metrics);

                return Ok(Some(MomentumSignal {
                    token_mint: *token_mint,
                    signal_type,
                    strength,
                    volume_24h,
                    price_change_rate,
                    liquidity_depth,
                    timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                    confidence,
                }));
            }
        }

        Ok(None)
    }

    fn detect_volume_spike(&self, metrics: &TokenMetrics) -> bool {
        if metrics.volume_history.len() < 20 {
            return false;
        }

        let recent_volume: u64 = metrics.volume_history.iter().rev().take(5).sum();
        let avg_volume: u64 = metrics.volume_history.iter().sum::<u64>() / metrics.volume_history.len() as u64;

        recent_volume as f64 > avg_volume as f64 * VOLUME_SPIKE_THRESHOLD
    }

    fn calculate_signal_strength(&self, metrics: &TokenMetrics, signal_type: &SignalType) -> f64 {
        match signal_type {
            SignalType::VolumeSpike => {
                let volume_ratio = if metrics.volume_history.len() >= 5 {
                    let recent: u64 = metrics.volume_history.iter().rev().take(5).sum();
                    let total: u64 = metrics.volume_history.iter().sum();
                    let avg = total as f64 / metrics.volume_history.len() as f64;
                    (recent as f64 / 5.0) / avg
                } else {
                    1.0
                };
                (volume_ratio / 10.0).min(1.0)
            }
            SignalType::PriceMomentum => {
                (metrics.momentum_score.abs() * 50.0).min(1.0)
            }
            SignalType::LiquidityShift => {
                let liquidity_change = self.calculate_liquidity_change(metrics);
                (liquidity_change.abs() * 10.0).min(1.0)
            }
            SignalType::WhaleActivity => {
                let whale_score = self.detect_whale_activity(metrics);
                whale_score.min(1.0)
            }
            SignalType::ArbitrageOpening => {
                let arb_score = metrics.volatility * 25.0;
                arb_score.min(1.0)
            }
        }
    }

    fn calculate_confidence(&self, metrics: &TokenMetrics) -> f64 {
        let data_points = metrics.price_history.len() as f64;
        let recency_factor = 1.0 - (metrics.last_update.elapsed().as_secs() as f64 / 300.0).min(1.0);
        let consistency_factor = 1.0 - metrics.volatility.min(0.5) * 2.0;
        
        ((data_points / MOMENTUM_WINDOW_SIZE as f64) * 0.3 +
         recency_factor * 0.4 +
         consistency_factor * 0.3).min(1.0).max(0.0)
    }

    fn calculate_liquidity_change(&self, metrics: &TokenMetrics) -> f64 {
        if metrics.liquidity_history.len() < 10 {
            return 0.0;
        }

        let recent_liquidity: u64 = metrics.liquidity_history.iter().rev().take(5).sum();
        let older_liquidity: u64 = metrics.liquidity_history.iter().take(5).sum();

        if older_liquidity > 0 {
            (recent_liquidity as f64 - older_liquidity as f64) / older_liquidity as f64
        } else {
            0.0
        }
    }

    fn detect_whale_activity(&self, metrics: &TokenMetrics) -> f64 {
        if metrics.volume_history.is_empty() {
            return 0.0;
        }

        let max_volume = *metrics.volume_history.iter().max().unwrap_or(&0);
        let avg_volume: u64 = metrics.volume_history.iter().sum::<u64>() / metrics.volume_history.len() as u64;

        if avg_volume > 0 {
            (max_volume as f64 / avg_volume as f64 / 20.0).min(1.0)
        } else {
            0.0
        }
    }

    fn estimate_liquidity_depth(&self, metrics: &TokenMetrics) -> u64 {
        if metrics.liquidity_history.is_empty() {
            return metrics.volume_history.iter().sum::<u64>() / 10;
        }

        let avg_liquidity: u64 = metrics.liquidity_history.iter().sum::<u64>() / metrics.liquidity_history.len() as u64;
        let volume_factor = metrics.volume_history.iter().sum::<u64>() / metrics.volume_history.len().max(1) as u64;
        
        (avg_liquidity * 7 + volume_factor * 3) / 10
    }

    async fn cleanup_stale_signals(&self) {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut signals = self.active_signals.write().await;
        
        signals.retain(|signal| {
            current_time - signal.timestamp < 300 && signal.strength > 0.3
        });
    }

    pub async fn get_top_signals(&self, count: usize) -> Vec<MomentumSignal> {
        let signals = self.active_signals.read().await;
        signals.iter().take(count).cloned().collect()
    }

    pub async fn get_signals_by_type(&self, signal_type: SignalType) -> Vec<MomentumSignal> {
        let signals = self.active_signals.read().await;
        signals.iter()
            .filter(|s| s.signal_type == signal_type)
            .cloned()
            .collect()
    }

    pub async fn get_token_metrics(&self, token_mint: &Pubkey) -> Option<TokenMetrics> {
        self.token_metrics.get(token_mint).map(|entry| entry.clone())
    }

    pub async fn force_update_token(&self, token_mint: Pubkey) -> Result<()> {
        let permit = self.detection_semaphore.acquire().await?;
        
        let account_info = self.rpc_client
            .get_account(&token_mint)
            .await
            .context("Failed to fetch token account")?;

        if account_info.lamports > 0 {
            self.token_metrics.entry(token_mint).or_insert_with(TokenMetrics::default);
        }

        drop(permit);
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub async fn clear_signals(&self) {
        self.active_signals.write().await.clear();
    }

    pub async fn update_liquidity_data(&self, token_mint: Pubkey, liquidity: u64) {
        self.token_metrics.entry(token_mint).and_modify(|metrics| {
            metrics.liquidity_history.push_back(liquidity);
            if metrics.liquidity_history.len() > MOMENTUM_WINDOW_SIZE {
                metrics.liquidity_history.pop_front();
            }
            metrics.last_update = Instant::now();
        });
    }

    pub async fn inject_price_update(&self, token_mint: Pubkey, price: f64, volume: u64) {
        self.token_metrics.entry(token_mint).and_modify(|metrics| {
            metrics.price_history.push_back(price);
            if metrics.price_history.len() > MOMENTUM_WINDOW_SIZE {
                metrics.price_history.pop_front();
            }
            
            metrics.volume_history.push_back(volume);
            if metrics.volume_history.len() > MOMENTUM_WINDOW_SIZE {
                metrics.volume_history.pop_front();
            }
            
            metrics.last_update = Instant::now();
            
            if metrics.price_history.len() >= 10 {
                let prices: Vec<f64> = metrics.price_history.iter().copied().collect();
                metrics.momentum_score = Self::calculate_momentum(&prices);
                metrics.volatility = Self::calculate_volatility(&prices);
            }
        }).or_insert_with(|| {
            let mut m = TokenMetrics::default();
            m.price_history.push_back(price);
            m.volume_history.push_back(volume);
            m
        });
    }

    pub async fn get_momentum_score(&self, token_mint: &Pubkey) -> Option<f64> {
        self.token_metrics.get(token_mint).map(|entry| entry.momentum_score)
    }

    pub async fn get_volatility(&self, token_mint: &Pubkey) -> Option<f64> {
        self.token_metrics.get(token_mint).map(|entry| entry.volatility)
    }

    pub async fn prune_inactive_tokens(&self, inactive_threshold_secs: u64) {
        let now = Instant::now();
        let threshold = Duration::from_secs(inactive_threshold_secs);
        
        self.token_metrics.retain(|_, metrics| {
            now.duration_since(metrics.last_update) < threshold
        });
    }
}

impl Clone for OnChainMomentumDetector {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            token_metrics: self.token_metrics.clone(),
            active_signals: self.active_signals.clone(),
            running: self.running.clone(),
            detection_semaphore: self.detection_semaphore.clone(),
            last_slot_processed: self.last_slot_processed.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_momentum_calculation() {
        let prices = vec![100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0];
        let momentum = OnChainMomentumDetector::calculate_momentum(&prices);
        assert!(momentum > 0.0);
    }

    #[tokio::test]
    async fn test_volatility_calculation() {
        let prices = vec![100.0, 110.0, 90.0, 105.0, 95.0, 108.0, 92.0, 106.0];
        let volatility = OnChainMomentumDetector::calculate_volatility(&prices);
        assert!(volatility > 0.05);
    }

    #[tokio::test]
    async fn test_signal_detection() {
        let detector = OnChainMomentumDetector::new("https://api.mainnet-beta.solana.com")
            .expect("Failed to create detector");
        
        let token_mint = Pubkey::new_unique();
        
        for i in 0..30 {
            let price = 100.0 + (i as f64 * 0.5);
            let volume = 1000000 * (1 + i % 3);
            detector.inject_price_update(token_mint, price, volume).await;
        }
        
        let momentum = detector.get_momentum_score(&token_mint).await;
        assert!(momentum.is_some());
    }
}

