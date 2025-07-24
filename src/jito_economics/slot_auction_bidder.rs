use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
    system_instruction,
    hash::Hash,
    compute_budget::ComputeBudgetInstruction,
    packet::PACKET_DATA_SIZE,
};
use std::{
    sync::{Arc, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    collections::{HashMap, VecDeque, BTreeMap},
    str::FromStr,
};
use tokio::{
    sync::{RwLock as TokioRwLock, Semaphore},
    time::{interval, sleep},
};
use serde::{Serialize, Deserialize};
use reqwest::{Client, ClientBuilder, header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION}};
use base64::URL_SAFE;
use sha2::{Sha256, Digest};
use rand::{thread_rng, Rng};

// Jito Labs Mainnet Production Constants
const JITO_BLOCK_ENGINE_URL: &str = "https://mainnet.block-engine.jito.wtf";
const JITO_TIP_ADDRESSES: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT"
];

const MAX_BUNDLE_SIZE: usize = 5;
const BASE_TIP_LAMPORTS: u64 = 1_000_000;
const MIN_TIP_LAMPORTS: u64 = 100_000;
const MAX_TIP_LAMPORTS: u64 = 50_000_000;
const SLOT_DURATION_MS: u64 = 400;
const FUTURE_SLOT_BUFFER: u64 = 2;
const MAX_RETRIES: u8 = 2;
const RATE_LIMIT_PER_SECOND: usize = 5;
const PRIORITY_FEE_MICRO_LAMPORTS: u64 = 50_000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;

#[derive(Debug, Clone)]
pub struct SlotAuctionBidder {
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    keypair: Arc<Keypair>,
    auth_token: String,
    tip_history: Arc<RwLock<VecDeque<TipRecord>>>,
    active_bundles: Arc<TokioRwLock<HashMap<String, ActiveBundle>>>,
    current_slot: Arc<AtomicU64>,
    is_running: Arc<AtomicBool>,
    metrics: Arc<RwLock<BidderMetrics>>,
    competition_tracker: Arc<TokioRwLock<CompetitionData>>,
    rate_limiter: Arc<Semaphore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TipRecord {
    slot: Slot,
    tip_lamports: u64,
    bundle_id: String,
    timestamp: u64,
    landed: bool,
    priority_fee: u64,
    transactions_count: usize,
}

#[derive(Debug, Clone)]
struct ActiveBundle {
    jito_bundle_id: String,
    slot: Slot,
    tip_lamports: u64,
    transactions: Vec<Transaction>,
    submitted_at: Instant,
    retry_count: u8,
    expected_profit: u64,
}

#[derive(Debug, Default, Clone)]
struct BidderMetrics {
    bundles_submitted: u64,
    bundles_landed: u64,
    bundles_failed: u64,
    total_tips_paid: u64,
    total_profit_earned: u64,
    win_rate: f64,
    avg_tip: u64,
    p50_tip: u64,
    p90_tip: u64,
}

#[derive(Debug, Clone)]
struct CompetitionData {
    slot_tips: BTreeMap<Slot, Vec<u64>>,
    percentile_tips: HashMap<u8, u64>,
    hourly_multipliers: HashMap<u8, f64>,
    recent_success_rates: VecDeque<f64>,
}

#[derive(Serialize)]
struct JitoBundleRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Vec<Vec<String>>,
}

#[derive(Deserialize)]
struct JitoBundleResponse {
    jsonrpc: String,
    result: Option<JitoBundleResult>,
    error: Option<JitoError>,
    id: u64,
}

#[derive(Deserialize)]
struct JitoBundleResult {
    bundle_id: String,
}

#[derive(Deserialize)]
struct JitoError {
    code: i32,
    message: String,
    data: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct BundleStatusRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Vec<String>,
}

impl SlotAuctionBidder {
    pub fn new(
        rpc_url: &str,
        keypair: Keypair,
        auth_token: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", auth_token))?,
        );
        
        let http_client = Arc::new(
            ClientBuilder::new()
                .default_headers(headers)
                .timeout(Duration::from_secs(10))
                .build()?
        );

        Ok(Self {
            rpc_client,
            http_client,
            keypair: Arc::new(keypair),
            auth_token: auth_token.to_string(),
            tip_history: Arc::new(RwLock::new(VecDeque::with_capacity(10000))),
            active_bundles: Arc::new(TokioRwLock::new(HashMap::new())),
            current_slot: Arc::new(AtomicU64::new(0)),
            is_running: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(RwLock::new(BidderMetrics::default())),
            competition_tracker: Arc::new(TokioRwLock::new(CompetitionData {
                slot_tips: BTreeMap::new(),
                percentile_tips: HashMap::new(),
                hourly_multipliers: HashMap::new(),
                recent_success_rates: VecDeque::with_capacity(100),
            })),
            rate_limiter: Arc::new(Semaphore::new(RATE_LIMIT_PER_SECOND)),
        })
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.is_running.store(true, Ordering::SeqCst);
        
        let slot_monitor = self.clone();
        tokio::spawn(async move {
            slot_monitor.monitor_slots().await;
        });

        let competition_analyzer = self.clone();
        tokio::spawn(async move {
            competition_analyzer.analyze_competition().await;
        });

        let bundle_tracker = self.clone();
        tokio::spawn(async move {
            bundle_tracker.track_bundle_status().await;
        });

        Ok(())
    }

    async fn monitor_slots(&self) {
        let mut interval = interval(Duration::from_millis(100));
        
        while self.is_running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            if let Ok(slot) = self.rpc_client.get_slot() {
                self.current_slot.store(slot, Ordering::SeqCst);
            }
        }
    }

    pub async fn submit_bundle_opportunity(
        &self,
        transactions: Vec<Transaction>,
        expected_profit: u64,
        target_slot: Option<Slot>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        if transactions.is_empty() || transactions.len() >= MAX_BUNDLE_SIZE {
            return Err("Invalid bundle size".into());
        }

        let current_slot = self.current_slot.load(Ordering::SeqCst);
        let slot = target_slot.unwrap_or(current_slot + FUTURE_SLOT_BUFFER);
        
        let tip_amount = self.calculate_optimal_tip(slot, expected_profit).await;
        
        if !self.is_profitable(tip_amount, expected_profit) {
            return Err("Bundle not profitable after tip".into());
        }

        self.submit_bundle_to_jito(transactions, tip_amount, slot).await
    }

    async fn calculate_optimal_tip(&self, target_slot: Slot, expected_profit: u64) -> u64 {
        let competition = self.competition_tracker.read().await;
        let mut tip = BASE_TIP_LAMPORTS;
        
        // Historical pattern analysis
        let slot_pattern = target_slot % 1000;
        let similar_tips: Vec<u64> = competition.slot_tips
            .range(target_slot.saturating_sub(1000)..target_slot)
            .filter(|(s, _)| *s % 1000 == slot_pattern)
            .flat_map(|(_, tips)| tips.iter().cloned())
            .collect();
        
        if similar_tips.len() > 10 {
            tip = similar_tips.iter().sum::<u64>() / similar_tips.len() as u64;
        }
        
        // Percentile adjustment
        if let Some(&p75) = competition.percentile_tips.get(&75) {
            tip = tip.max(p75);
        }
        
        // Time-based adjustment
        let hour = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() / 3600) % 24;
        let multiplier = competition.hourly_multipliers.get(&(hour as u8))
            .copied()
            .unwrap_or_else(|| match hour {
                14..=22 => 1.3,
                6..=13 => 1.1,
                _ => 0.8,
            });
        
        tip = (tip as f64 * multiplier) as u64;
        
        // Win rate adjustment
        if !competition.recent_success_rates.is_empty() {
            let recent_win_rate = competition.recent_success_rates.iter()
                .sum::<f64>() / competition.recent_success_rates.len() as f64;
            
            if recent_win_rate < 0.05 {
                tip = (tip as f64 * 1.5) as u64;
            } else if recent_win_rate > 0.15 {
                tip = (tip as f64 * 0.9) as u64;
            }
        }
        
        tip.clamp(MIN_TIP_LAMPORTS, MAX_TIP_LAMPORTS.min(expected_profit / 2))
    }

    fn is_profitable(&self, tip: u64, expected_profit: u64) -> bool {
        expected_profit > tip && (expected_profit - tip) as f64 / expected_profit as f64 >= 0.3
    }

    async fn submit_bundle_to_jito(
        &self,
        mut transactions: Vec<Transaction>,
        tip_amount: u64,
        target_slot: Slot,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let _permit = self.rate_limiter.acquire().await?;
        
        // Create tip transaction
        let tip_account = self.select_tip_account(target_slot);
        let tip_ix = system_instruction::transfer(
            &self.keypair.pubkey(),
            &tip_account,
            tip_amount,
        );
        
        let priority_ix = ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICRO_LAMPORTS);
        let compute_ix = ComputeBudgetInstruction::set_compute_unit_limit(200_000);
        
        let mut tip_tx = Transaction::new_with_payer(
            &[compute_ix, priority_ix, tip_ix],
            Some(&self.keypair.pubkey()),
        );
        
        // Get fresh blockhash
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        
        // Sign tip transaction
        tip_tx.sign(&[self.keypair.as_ref()], recent_blockhash);
        
        // Prepare bundle transactions
        let mut bundle_txs = vec![tip_tx];
        
                // Re-sign all transactions with fresh blockhash
        for tx in transactions.iter_mut() {
            if tx.signatures.is_empty() || !tx.verify_with_signer(&self.keypair.pubkey()) {
                tx.partial_sign(&[self.keypair.as_ref()], recent_blockhash);
            }
        }
        
        bundle_txs.extend(transactions);
        
        if bundle_txs.len() > MAX_BUNDLE_SIZE {
            bundle_txs.truncate(MAX_BUNDLE_SIZE);
        }
        
        // Serialize for Jito
        let serialized_txs: Vec<String> = bundle_txs.iter()
            .map(|tx| base64::encode_config(bincode::serialize(tx).unwrap(), URL_SAFE))
            .collect();
        
        // Create Jito request
        let request = JitoBundleRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "sendBundle".to_string(),
            params: vec![serialized_txs],
        };
        
        // Submit to Jito
        let response = self.http_client
            .post(format!("{}/api/v1/bundles", JITO_BLOCK_ENGINE_URL))
            .json(&request)
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(format!("Jito request failed: {}", response.status()).into());
        }
        
        let jito_response: JitoBundleResponse = response.json().await?;
        
        if let Some(error) = jito_response.error {
            return Err(format!("Jito error: {} - {}", error.code, error.message).into());
        }
        
        let bundle_id = jito_response.result
            .ok_or("No bundle ID in response")?
            .bundle_id;
        
        // Track bundle
        let mut active_bundles = self.active_bundles.write().await;
        active_bundles.insert(bundle_id.clone(), ActiveBundle {
            jito_bundle_id: bundle_id.clone(),
            slot: target_slot,
            tip_lamports: tip_amount,
            transactions: bundle_txs,
            submitted_at: Instant::now(),
            retry_count: 0,
            expected_profit: 0,
        });
        
        // Record metrics
        self.record_submission(target_slot, tip_amount, &bundle_id);
        
        Ok(bundle_id)
    }

    fn select_tip_account(&self, slot: Slot) -> Pubkey {
        let index = (slot as usize) % JITO_TIP_ADDRESSES.len();
        Pubkey::from_str(JITO_TIP_ADDRESSES[index]).unwrap()
    }

    fn record_submission(&self, slot: Slot, tip: u64, bundle_id: &str) {
        let mut history = self.tip_history.write().unwrap();
        history.push_back(TipRecord {
            slot,
            tip_lamports: tip,
            bundle_id: bundle_id.to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            landed: false,
            priority_fee: PRIORITY_FEE_MICRO_LAMPORTS,
            transactions_count: 0,
        });
        
        if history.len() > 10000 {
            history.pop_front();
        }
        
        let mut metrics = self.metrics.write().unwrap();
        metrics.bundles_submitted += 1;
    }

    async fn analyze_competition(&self) {
        let mut interval = interval(Duration::from_secs(30));
        
        while self.is_running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            let mut competition = self.competition_tracker.write().await;
            let current_slot = self.current_slot.load(Ordering::SeqCst);
            
            // Cleanup old data
            competition.slot_tips.retain(|&slot, _| {
                slot > current_slot.saturating_sub(10000)
            });
            
            // Calculate percentiles
            let all_tips: Vec<u64> = competition.slot_tips
                .values()
                .flatten()
                .cloned()
                .collect();
            
            if all_tips.len() > 100 {
                let mut sorted = all_tips.clone();
                sorted.sort_unstable();
                
                let len = sorted.len();
                competition.percentile_tips.clear();
                competition.percentile_tips.insert(25, sorted[len / 4]);
                competition.percentile_tips.insert(50, sorted[len / 2]);
                competition.percentile_tips.insert(75, sorted[len * 3 / 4]);
                competition.percentile_tips.insert(90, sorted[len * 9 / 10]);
                competition.percentile_tips.insert(95, sorted[len * 95 / 100]);
            }
            
            // Update hourly patterns
            let history = self.tip_history.read().unwrap();
            let mut hourly_tips: HashMap<u8, Vec<u64>> = HashMap::new();
            
            for record in history.iter() {
                let hour = (record.timestamp / 3600) % 24;
                hourly_tips.entry(hour as u8)
                    .or_default()
                    .push(record.tip_lamports);
            }
            drop(history);
            
            let baseline = competition.percentile_tips.get(&50)
                .copied()
                .unwrap_or(BASE_TIP_LAMPORTS);
            
            for (hour, tips) in hourly_tips {
                if !tips.is_empty() {
                    let avg = tips.iter().sum::<u64>() / tips.len() as u64;
                    competition.hourly_multipliers.insert(hour, avg as f64 / baseline as f64);
                }
            }
            
            // Update metrics
            let mut metrics = self.metrics.write().unwrap();
            metrics.p50_tip = competition.percentile_tips.get(&50).copied().unwrap_or(0);
            metrics.p90_tip = competition.percentile_tips.get(&90).copied().unwrap_or(0);
        }
    }

    async fn track_bundle_status(&self) {
        let mut interval = interval(Duration::from_secs(2));
        
        while self.is_running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            let current_slot = self.current_slot.load(Ordering::SeqCst);
            let mut active = self.active_bundles.write().await;
            let mut completed = Vec::new();
            
            for (id, bundle) in active.iter() {
                if bundle.slot + 32 < current_slot || 
                   bundle.submitted_at.elapsed() > Duration::from_secs(30) {
                    let status = self.check_bundle_status(&bundle.jito_bundle_id).await;
                    completed.push((id.clone(), bundle.clone(), status));
                }
            }
            
            for (id, bundle, landed) in completed {
                active.remove(&id);
                self.update_metrics(bundle, landed).await;
            }
        }
    }

    async fn check_bundle_status(&self, bundle_id: &str) -> bool {
        let request = BundleStatusRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "getBundleStatuses".to_string(),
            params: vec![bundle_id.to_string()],
        };
        
        if let Ok(response) = self.http_client
            .post(format!("{}/api/v1/bundles", JITO_BLOCK_ENGINE_URL))
            .json(&request)
            .send()
            .await {
            if response.status().is_success() {
                if let Ok(text) = response.text().await {
                    return text.contains("\"landed\"");
                }
            }
        }
        
        false
    }

    async fn update_metrics(&self, bundle: ActiveBundle, landed: bool) {
        // Update tip history
        let mut history = self.tip_history.write().unwrap();
        if let Some(record) = history.iter_mut()
            .find(|r| r.bundle_id == bundle.jito_bundle_id) {
            record.landed = landed;
        }
        drop(history);
        
        // Update competition tracker
        let mut competition = self.competition_tracker.write().await;
        competition.slot_tips
            .entry(bundle.slot)
            .or_default()
            .push(bundle.tip_lamports);
        
        let total_submitted = self.metrics.read().unwrap().bundles_submitted;
        let win_rate = if landed { 1.0 } else { 0.0 };
        competition.recent_success_rates.push_back(win_rate);
        if competition.recent_success_rates.len() > 100 {
            competition.recent_success_rates.pop_front();
        }
        drop(competition);
        
        // Update metrics
        let mut metrics = self.metrics.write().unwrap();
        if landed {
            metrics.bundles_landed += 1;
            metrics.total_tips_paid += bundle.tip_lamports;
            metrics.total_profit_earned += bundle.expected_profit;
        } else {
            metrics.bundles_failed += 1;
        }
        
        if metrics.bundles_submitted > 0 {
            metrics.win_rate = metrics.bundles_landed as f64 / metrics.bundles_submitted as f64;
            metrics.avg_tip = metrics.total_tips_paid / metrics.bundles_landed.max(1);
        }
    }

    pub async fn retry_bundle(&self, bundle_id: &str) -> Result<String, Box<dyn std::error::Error>> {
        let mut active = self.active_bundles.write().await;
        
        if let Some(bundle) = active.get_mut(bundle_id) {
            if bundle.retry_count >= MAX_RETRIES {
                return Err("Max retries exceeded".into());
            }
            
            bundle.retry_count += 1;
            let new_tip = (bundle.tip_lamports as f64 * 1.5) as u64;
            let current_slot = self.current_slot.load(Ordering::SeqCst);
            let target_slot = current_slot + FUTURE_SLOT_BUFFER;
            
            let txs = bundle.transactions.clone();
            drop(active);
            
            self.submit_bundle_to_jito(txs, new_tip, target_slot).await
        } else {
            Err("Bundle not found".into())
        }
    }

    pub async fn get_metrics(&self) -> BidderMetrics {
        self.metrics.read().unwrap().clone()
    }

    pub async fn get_competition_stats(&self) -> HashMap<String, f64> {
        let competition = self.competition_tracker.read().await;
        let mut stats = HashMap::new();
        
        for (percentile, &value) in &competition.percentile_tips {
            stats.insert(format!("p{}_tip", percentile), value as f64);
        }
        
        let metrics = self.metrics.read().unwrap();
        stats.insert("win_rate".to_string(), metrics.win_rate);
        stats.insert("avg_tip".to_string(), metrics.avg_tip as f64);
        stats.insert("total_profit".to_string(), metrics.total_profit_earned as f64);
        
        stats
    }

    pub async fn shutdown(&self) {
        self.is_running.store(false, Ordering::SeqCst);
        
        let active = self.active_bundles.read().await;
        if !active.is_empty() {
            println!("Shutting down with {} active bundles", active.len());
        }
        
        sleep(Duration::from_millis(500)).await;
    }

    pub fn validate_bundle(&self, transactions: &[Transaction]) -> Result<(), Box<dyn std::error::Error>> {
        if transactions.is_empty() {
            return Err("Empty bundle".into());
        }
        
        if transactions.len() >= MAX_BUNDLE_SIZE {
            return Err(format!("Bundle exceeds max size of {}", MAX_BUNDLE_SIZE - 1).into());
        }
        
        let total_size: usize = transactions.iter()
            .map(|tx| bincode::serialize(tx).unwrap().len())
            .sum();
        
        if total_size > PACKET_DATA_SIZE * MAX_BUNDLE_SIZE {
            return Err("Bundle exceeds packet size limits".into());
        }
        
        for tx in transactions {
            if tx.signatures.is_empty() {
                return Err("Unsigned transaction in bundle".into());
            }
        }
        
        Ok(())
    }

    pub async fn estimate_tip_for_slot(&self, slot: Slot, expected_profit: u64) -> u64 {
        self.calculate_optimal_tip(slot, expected_profit).await
    }
}

impl Default for CompetitionData {
    fn default() -> Self {
        Self {
            slot_tips: BTreeMap::new(),
            percentile_tips: HashMap::new(),
            hourly_multipliers: HashMap::new(),
            recent_success_rates: VecDeque::with_capacity(100),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_initialization() {
        let keypair = Keypair::new();
        let bidder = SlotAuctionBidder::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "test_token",
        ).unwrap();
        
        assert!(!bidder.is_running.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_tip_calculation() {
        let keypair = Keypair::new();
        let bidder = SlotAuctionBidder::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "test_token",
        ).unwrap();
        
        let tip = bidder.calculate_optimal_tip(100000, 10_000_000).await;
        assert!(tip >= MIN_TIP_LAMPORTS);
        assert!(tip <= MAX_TIP_LAMPORTS);
    }

        #[tokio::test]
    async fn test_profitability() {
        let keypair = Keypair::new();
        let bidder = SlotAuctionBidder::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "test_token",
        ).unwrap();
        
        assert!(bidder.is_profitable(1_000_000, 10_000_000));
        assert!(!bidder.is_profitable(10_000_000, 10_000_000));
        assert!(!bidder.is_profitable(7_500_000, 10_000_000));
        assert!(bidder.is_profitable(5_000_000, 20_000_000));
    }

    #[tokio::test]
    async fn test_bundle_validation() {
        let keypair = Keypair::new();
        let bidder = SlotAuctionBidder::new(
            "https://api.mainnet-beta.solana.com",
            keypair.insecure_clone(),
            "test_token",
        ).unwrap();
        
        // Empty bundle
        assert!(bidder.validate_bundle(&[]).is_err());
        
        // Valid bundle
        let mut tx = Transaction::new_with_payer(
            &[system_instruction::transfer(
                &keypair.pubkey(),
                &Pubkey::new_unique(),
                1000,
            )],
            Some(&keypair.pubkey()),
        );
        tx.sign(&[&keypair], Hash::default());
        
        assert!(bidder.validate_bundle(&[tx.clone()]).is_ok());
        
        // Too many transactions
        let many_txs: Vec<Transaction> = (0..MAX_BUNDLE_SIZE).map(|_| tx.clone()).collect();
        assert!(bidder.validate_bundle(&many_txs).is_err());
    }

    #[tokio::test]
    async fn test_tip_account_selection() {
        let keypair = Keypair::new();
        let bidder = SlotAuctionBidder::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "test_token",
        ).unwrap();
        
        let account1 = bidder.select_tip_account(100);
        let account2 = bidder.select_tip_account(108);
        
        assert_ne!(account1, account2);
        
        // Verify it's a valid Jito tip account
        let account_str = account1.to_string();
        assert!(JITO_TIP_ADDRESSES.iter().any(|&addr| addr == account_str));
    }

    #[tokio::test]
    async fn test_metrics_tracking() {
        let keypair = Keypair::new();
        let bidder = SlotAuctionBidder::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "test_token",
        ).unwrap();
        
        let metrics = bidder.get_metrics().await;
        assert_eq!(metrics.bundles_submitted, 0);
        assert_eq!(metrics.bundles_landed, 0);
        assert_eq!(metrics.win_rate, 0.0);
        
        bidder.record_submission(100000, 1_000_000, "test_bundle_id");
        
        let metrics = bidder.get_metrics().await;
        assert_eq!(metrics.bundles_submitted, 1);
    }

    #[tokio::test]
    async fn test_competition_stats() {
        let keypair = Keypair::new();
        let bidder = SlotAuctionBidder::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "test_token",
        ).unwrap();
        
        let stats = bidder.get_competition_stats().await;
        assert!(stats.contains_key("win_rate"));
        assert!(stats.contains_key("avg_tip"));
        assert!(stats.contains_key("total_profit"));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let keypair = Keypair::new();
        let bidder = SlotAuctionBidder::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            "test_token",
        ).unwrap();
        
        bidder.start().await.unwrap();
        assert!(bidder.is_running.load(Ordering::SeqCst));
        
        bidder.shutdown().await;
        assert!(!bidder.is_running.load(Ordering::SeqCst));
    }
}

