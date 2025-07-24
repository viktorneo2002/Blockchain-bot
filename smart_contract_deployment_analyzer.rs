use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_program::{
    bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable,
    instruction::Instruction,
    program_pack::Pack,
    pubkey::Pubkey,
    system_program,
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    signature::Signature,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
};
use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep},
};

const MAX_DEPLOYMENT_CACHE: usize = 10000;
const DEPLOYMENT_SCAN_INTERVAL_MS: u64 = 100;
const RISK_SCORE_THRESHOLD: f64 = 0.75;
const PATTERN_WINDOW_SIZE: usize = 50;
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 50;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct DeploymentMetadata {
    pub program_id: Pubkey,
    pub deployer: Pubkey,
    pub deployment_slot: u64,
    pub deployment_timestamp: u64,
    pub data_len: usize,
    pub executable: bool,
    pub upgrade_authority: Option<Pubkey>,
    pub deployment_cost: u64,
    pub risk_score: f64,
    pub pattern_matches: Vec<PatternMatch>,
    pub interaction_count: AtomicU64,
    pub last_interaction_slot: AtomicU64,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct PatternMatch {
    pub pattern_type: PatternType,
    pub confidence: f64,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub enum PatternType {
    AmmDeployment,
    TokenMintDeployment,
    FlashLoanProtocol,
    ArbitrageBot,
    LiquidationProtocol,
    YieldFarming,
    Bridge,
    Oracle,
    Unknown,
}

pub struct SmartContractDeploymentAnalyzer {
    rpc_client: Arc<RpcClient>,
    deployment_cache: Arc<DashMap<Pubkey, Arc<DeploymentMetadata>>>,
    pattern_detector: Arc<PatternDetector>,
    risk_analyzer: Arc<RiskAnalyzer>,
    deployment_queue: Arc<Mutex<VecDeque<Pubkey>>>,
    is_running: Arc<AtomicBool>,
    last_scan_slot: Arc<AtomicU64>,
    metrics: Arc<AnalyzerMetrics>,
}

struct PatternDetector {
    amm_signatures: Vec<Vec<u8>>,
    token_signatures: Vec<Vec<u8>>,
    defi_signatures: Vec<Vec<u8>>,
    pattern_cache: RwLock<HashMap<Pubkey, Vec<PatternMatch>>>,
}

struct RiskAnalyzer {
    deployment_history: RwLock<VecDeque<(u64, Pubkey)>>,
    deployer_reputation: DashMap<Pubkey, f64>,
    anomaly_threshold: f64,
}

struct AnalyzerMetrics {
    total_deployments_analyzed: AtomicU64,
    high_risk_deployments: AtomicU64,
    pattern_matches: AtomicU64,
    analysis_latency_us: AtomicU64,
}

impl SmartContractDeploymentAnalyzer {
    pub async fn new(rpc_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let pattern_detector = Arc::new(PatternDetector::new());
        let risk_analyzer = Arc::new(RiskAnalyzer::new());

        Ok(Self {
            rpc_client,
            deployment_cache: Arc::new(DashMap::new()),
            pattern_detector,
            risk_analyzer,
            deployment_queue: Arc::new(Mutex::new(VecDeque::with_capacity(1000))),
            is_running: Arc::new(AtomicBool::new(false)),
            last_scan_slot: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(AnalyzerMetrics {
                total_deployments_analyzed: AtomicU64::new(0),
                high_risk_deployments: AtomicU64::new(0),
                pattern_matches: AtomicU64::new(0),
                analysis_latency_us: AtomicU64::new(0),
            }),
        })
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.is_running.store(true, Ordering::SeqCst);

        let scanner_handle = self.spawn_deployment_scanner();
        let analyzer_handle = self.spawn_deployment_analyzer();
        let cleaner_handle = self.spawn_cache_cleaner();

        tokio::try_join!(scanner_handle, analyzer_handle, cleaner_handle)?;
        Ok(())
    }

    pub async fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }

    fn spawn_deployment_scanner(&self) -> tokio::task::JoinHandle<()> {
        let rpc_client = Arc::clone(&self.rpc_client);
        let deployment_queue = Arc::clone(&self.deployment_queue);
        let is_running = Arc::clone(&self.is_running);
        let last_scan_slot = Arc::clone(&self.last_scan_slot);

        tokio::spawn(async move {
            let mut scan_interval = interval(Duration::from_millis(DEPLOYMENT_SCAN_INTERVAL_MS));

            while is_running.load(Ordering::SeqCst) {
                scan_interval.tick().await;

                if let Ok(slot) = rpc_client.get_slot().await {
                    let last_slot = last_scan_slot.load(Ordering::SeqCst);
                    
                    if slot > last_slot {
                        for scan_slot in last_slot + 1..=slot.min(last_slot + 10) {
                            if let Ok(block) = Self::get_block_with_retry(&rpc_client, scan_slot).await {
                                if let Some(transactions) = block.transactions {
                                    for tx in transactions {
                                        if let Some(meta) = &tx.meta {
                                            if meta.err.is_none() {
                                                Self::extract_deployments(&tx, &deployment_queue).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        last_scan_slot.store(slot, Ordering::SeqCst);
                    }
                }
            }
        })
    }

    fn spawn_deployment_analyzer(&self) -> tokio::task::JoinHandle<()> {
        let rpc_client = Arc::clone(&self.rpc_client);
        let deployment_cache = Arc::clone(&self.deployment_cache);
        let deployment_queue = Arc::clone(&self.deployment_queue);
        let pattern_detector = Arc::clone(&self.pattern_detector);
        let risk_analyzer = Arc::clone(&self.risk_analyzer);
        let is_running = Arc::clone(&self.is_running);
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            while is_running.load(Ordering::SeqCst) {
                let program_id = {
                    let mut queue = deployment_queue.lock().await;
                    queue.pop_front()
                };

                if let Some(program_id) = program_id {
                    let start = Instant::now();
                    
                    if let Ok(metadata) = Self::analyze_deployment(
                        &rpc_client,
                        &program_id,
                        &pattern_detector,
                        &risk_analyzer,
                    ).await {
                        deployment_cache.insert(program_id, Arc::new(metadata));
                        metrics.total_deployments_analyzed.fetch_add(1, Ordering::SeqCst);
                        
                        let latency = start.elapsed().as_micros() as u64;
                        metrics.analysis_latency_us.store(latency, Ordering::SeqCst);
                    }
                } else {
                    sleep(Duration::from_millis(10)).await;
                }
            }
        })
    }

    fn spawn_cache_cleaner(&self) -> tokio::task::JoinHandle<()> {
        let deployment_cache = Arc::clone(&self.deployment_cache);
        let is_running = Arc::clone(&self.is_running);

        tokio::spawn(async move {
            let mut cleanup_interval = interval(Duration::from_secs(60));

            while is_running.load(Ordering::SeqCst) {
                cleanup_interval.tick().await;

                if deployment_cache.len() > MAX_DEPLOYMENT_CACHE {
                    let mut entries: Vec<_> = deployment_cache.iter()
                        .map(|entry| (entry.key().clone(), entry.value().clone()))
                        .collect();
                    
                    entries.sort_by_key(|(_, metadata)| {
                        metadata.last_interaction_slot.load(Ordering::SeqCst)
                    });

                    let remove_count = deployment_cache.len() - MAX_DEPLOYMENT_CACHE * 9 / 10;
                    for (key, _) in entries.into_iter().take(remove_count) {
                        deployment_cache.remove(&key);
                    }
                }
            }
        })
    }

    async fn get_block_with_retry(
        client: &RpcClient,
        slot: u64,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, Box<dyn std::error::Error>> {
        let mut retries = 0;
        
        loop {
            match client.get_block_with_encoding(slot, UiTransactionEncoding::Base64).await {
                Ok(block) => return Ok(block),
                Err(e) if retries < MAX_RETRIES => {
                    retries += 1;
                    sleep(Duration::from_millis(RETRY_DELAY_MS * retries as u64)).await;
                }
                Err(e) => return Err(Box::new(e)),
            }
        }
    }

    async fn extract_deployments(
        tx: &EncodedConfirmedTransactionWithStatusMeta,
        queue: &Arc<Mutex<VecDeque<Pubkey>>>,
    ) {
        if let Some(message) = &tx.transaction.transaction.decode() {
            if let Some(message) = message.message() {
                for (idx, account_key) in message.account_keys.iter().enumerate() {
                    if let Some(meta) = &tx.meta {
                        if let Some(post_balances) = &meta.post_balances {
                            if let Some(pre_balances) = &meta.pre_balances {
                                if idx < post_balances.len() && idx < pre_balances.len() {
                                    let is_program_deployment = message.instructions.iter().any(|ix| {
                                        ix.program_id_index as usize == idx ||
                                        (ix.data.len() > 4 && Self::is_deployment_instruction(&ix.data))
                                    });

                                    if is_program_deployment {
                                        let mut queue_guard = queue.lock().await;
                                        if queue_guard.len() < 1000 {
                                            queue_guard.push_back(*account_key);
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

    fn is_deployment_instruction(data: &[u8]) -> bool {
        if data.len() < 4 {
            return false;
        }

        let instruction_discriminator = &data[0..4];
        
        instruction_discriminator == [0, 0, 0, 2] ||
        instruction_discriminator == [0, 0, 0, 3] ||
        instruction_discriminator == [1, 0, 0, 0]
    }

    async fn analyze_deployment(
        client: &RpcClient,
        program_id: &Pubkey,
        pattern_detector: &PatternDetector,
        risk_analyzer: &RiskAnalyzer,
    ) -> Result<DeploymentMetadata, Box<dyn std::error::Error>> {
        let account = client.get_account(program_id).await?;
        
        let deployer = Self::extract_deployer(&account);
        let upgrade_authority = Self::extract_upgrade_authority(&account);
        
        let pattern_matches = pattern_detector.detect_patterns(program_id, &account).await;
        let risk_score = risk_analyzer.calculate_risk_score(
            program_id,
            &deployer,
            &pattern_matches,
            &account,
        ).await;

        let deployment_slot = client.get_slot().await?;
                let deployment_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs();

        Ok(DeploymentMetadata {
            program_id: *program_id,
            deployer,
            deployment_slot,
            deployment_timestamp,
            data_len: account.data.len(),
            executable: account.executable,
            upgrade_authority,
            deployment_cost: account.lamports,
            risk_score,
            pattern_matches,
            interaction_count: AtomicU64::new(0),
            last_interaction_slot: AtomicU64::new(deployment_slot),
        })
    }

    fn extract_deployer(account: &Account) -> Pubkey {
        if account.owner == bpf_loader_upgradeable::id() && account.data.len() >= 45 {
            if account.data[0] == 1 {
                let mut deployer_bytes = [0u8; 32];
                deployer_bytes.copy_from_slice(&account.data[13..45]);
                Pubkey::new_from_array(deployer_bytes)
            } else {
                account.owner
            }
        } else {
            account.owner
        }
    }

    fn extract_upgrade_authority(account: &Account) -> Option<Pubkey> {
        if account.owner == bpf_loader_upgradeable::id() && account.data.len() >= 45 {
            if account.data[0] == 1 && account.data[12] == 1 {
                let mut authority_bytes = [0u8; 32];
                authority_bytes.copy_from_slice(&account.data[13..45]);
                Some(Pubkey::new_from_array(authority_bytes))
            } else {
                None
            }
        } else {
            None
        }
    }

    pub async fn get_deployment_metadata(&self, program_id: &Pubkey) -> Option<Arc<DeploymentMetadata>> {
        self.deployment_cache.get(program_id).map(|entry| entry.clone())
    }

    pub async fn get_high_risk_deployments(&self) -> Vec<Arc<DeploymentMetadata>> {
        self.deployment_cache
            .iter()
            .filter(|entry| entry.value().risk_score >= RISK_SCORE_THRESHOLD)
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub async fn get_deployments_by_pattern(&self, pattern_type: PatternType) -> Vec<Arc<DeploymentMetadata>> {
        self.deployment_cache
            .iter()
            .filter(|entry| {
                entry.value().pattern_matches.iter().any(|pm| pm.pattern_type == pattern_type)
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_metrics(&self) -> (u64, u64, u64, u64) {
        (
            self.metrics.total_deployments_analyzed.load(Ordering::SeqCst),
            self.metrics.high_risk_deployments.load(Ordering::SeqCst),
            self.metrics.pattern_matches.load(Ordering::SeqCst),
            self.metrics.analysis_latency_us.load(Ordering::SeqCst),
        )
    }
}

impl PatternDetector {
    fn new() -> Self {
        Self {
            amm_signatures: vec![
                vec![0x95, 0x2b, 0xb1, 0x3c],
                vec![0xf8, 0xc6, 0x9e, 0x91],
                vec![0x09, 0xbb, 0xd9, 0xf0],
            ],
            token_signatures: vec![
                vec![0x11, 0x11, 0x11, 0x11],
                vec![0x82, 0x16, 0x74, 0x62],
            ],
            defi_signatures: vec![
                vec![0x3d, 0x60, 0x2d, 0x80],
                vec![0x51, 0x7a, 0x28, 0x5d],
                vec![0xa8, 0xb3, 0x8e, 0x0b],
            ],
            pattern_cache: RwLock::new(HashMap::new()),
        }
    }

    async fn detect_patterns(&self, program_id: &Pubkey, account: &Account) -> Vec<PatternMatch> {
        let mut patterns = Vec::new();

        if let Some(cached) = self.pattern_cache.read().await.get(program_id) {
            return cached.clone();
        }

        let data_slice = if account.data.len() > 1000 {
            &account.data[..1000]
        } else {
            &account.data
        };

        for (idx, window) in data_slice.windows(4).enumerate() {
            for amm_sig in &self.amm_signatures {
                if window == amm_sig {
                    patterns.push(PatternMatch {
                        pattern_type: PatternType::AmmDeployment,
                        confidence: 0.85 + (idx as f64 / 1000.0) * 0.1,
                        metadata: HashMap::from([
                            ("signature_offset".to_string(), idx.to_string()),
                            ("signature".to_string(), hex::encode(amm_sig)),
                        ]),
                    });
                }
            }

            for token_sig in &self.token_signatures {
                if window == token_sig {
                    patterns.push(PatternMatch {
                        pattern_type: PatternType::TokenMintDeployment,
                        confidence: 0.80 + (idx as f64 / 1000.0) * 0.1,
                        metadata: HashMap::from([
                            ("signature_offset".to_string(), idx.to_string()),
                            ("signature".to_string(), hex::encode(token_sig)),
                        ]),
                    });
                }
            }

            for defi_sig in &self.defi_signatures {
                if window == defi_sig {
                    let pattern_type = match idx % 3 {
                        0 => PatternType::FlashLoanProtocol,
                        1 => PatternType::LiquidationProtocol,
                        _ => PatternType::YieldFarming,
                    };

                    patterns.push(PatternMatch {
                        pattern_type,
                        confidence: 0.75 + (idx as f64 / 1000.0) * 0.15,
                        metadata: HashMap::from([
                            ("signature_offset".to_string(), idx.to_string()),
                            ("signature".to_string(), hex::encode(defi_sig)),
                        ]),
                    });
                }
            }
        }

        if patterns.is_empty() && account.data.len() > 100 {
            patterns.push(PatternMatch {
                pattern_type: PatternType::Unknown,
                confidence: 0.5,
                metadata: HashMap::new(),
            });
        }

        self.pattern_cache.write().await.insert(*program_id, patterns.clone());
        patterns
    }
}

impl RiskAnalyzer {
    fn new() -> Self {
        Self {
            deployment_history: RwLock::new(VecDeque::with_capacity(PATTERN_WINDOW_SIZE)),
            deployer_reputation: DashMap::new(),
            anomaly_threshold: 0.3,
        }
    }

    async fn calculate_risk_score(
        &self,
        program_id: &Pubkey,
        deployer: &Pubkey,
        patterns: &[PatternMatch],
        account: &Account,
    ) -> f64 {
        let mut risk_score = 0.0;
        let mut weight_sum = 0.0;

        let pattern_risk = self.calculate_pattern_risk(patterns);
        risk_score += pattern_risk * 0.3;
        weight_sum += 0.3;

        let deployer_risk = self.calculate_deployer_risk(deployer).await;
        risk_score += deployer_risk * 0.25;
        weight_sum += 0.25;

        let size_risk = self.calculate_size_risk(account.data.len());
        risk_score += size_risk * 0.15;
        weight_sum += 0.15;

        let frequency_risk = self.calculate_frequency_risk(program_id).await;
        risk_score += frequency_risk * 0.2;
        weight_sum += 0.2;

        let anomaly_risk = self.calculate_anomaly_risk(account);
        risk_score += anomaly_risk * 0.1;
        weight_sum += 0.1;

        self.update_deployment_history(program_id).await;
        self.update_deployer_reputation(deployer, risk_score / weight_sum).await;

        (risk_score / weight_sum).min(1.0).max(0.0)
    }

    fn calculate_pattern_risk(&self, patterns: &[PatternMatch]) -> f64 {
        if patterns.is_empty() {
            return 0.5;
        }

        let mut total_risk = 0.0;
        for pattern in patterns {
            let base_risk = match pattern.pattern_type {
                PatternType::Unknown => 0.7,
                PatternType::FlashLoanProtocol => 0.8,
                PatternType::ArbitrageBot => 0.6,
                PatternType::LiquidationProtocol => 0.75,
                PatternType::AmmDeployment => 0.4,
                PatternType::TokenMintDeployment => 0.5,
                PatternType::YieldFarming => 0.45,
                PatternType::Bridge => 0.55,
                PatternType::Oracle => 0.3,
            };
            total_risk += base_risk * pattern.confidence;
        }

        total_risk / patterns.len() as f64
    }

    async fn calculate_deployer_risk(&self, deployer: &Pubkey) -> f64 {
        if let Some(reputation) = self.deployer_reputation.get(deployer) {
            1.0 - reputation.value()
        } else {
            0.6
        }
    }

    fn calculate_size_risk(&self, data_len: usize) -> f64 {
        match data_len {
            0..=10_000 => 0.2,
            10_001..=50_000 => 0.3,
            50_001..=100_000 => 0.4,
            100_001..=500_000 => 0.6,
            _ => 0.8,
        }
    }

    async fn calculate_frequency_risk(&self, program_id: &Pubkey) -> f64 {
        let history = self.deployment_history.read().await;
        let recent_deployments = history.iter()
            .filter(|(_, pid)| pid == program_id)
            .count();

        match recent_deployments {
            0 => 0.3,
            1..=2 => 0.5,
            3..=5 => 0.7,
            _ => 0.9,
        }
    }

    fn calculate_anomaly_risk(&self, account: &Account) -> f64 {
        let mut anomaly_score = 0.0;

        if account.executable && account.lamports < 1_000_000 {
            anomaly_score += 0.3;
        }

        if account.data.len() > 0 && account.data.iter().all(|&b| b == 0) {
            anomaly_score += 0.4;
        }

        let entropy = self.calculate_entropy(&account.data[..account.data.len().min(1000)]);
        if entropy < self.anomaly_threshold {
            anomaly_score += 0.3;
        }

        anomaly_score.min(1.0)
    }

    fn calculate_entropy(&self, data: &[u8]) -> f64 {
        let mut frequency = [0u64; 256];
        for &byte in data {
            frequency[byte as usize] += 1;
        }

        let len = data.len() as f64;
        let mut entropy = 0.0;

        for &count in &frequency {
            if count > 0 {
                let p = count as f64 / len;
                entropy -= p * p.log2();
            }
        }

        entropy / 8.0
    }

    async fn update_deployment_history(&self, program_id: &Pubkey) {
        let mut history = self.deployment_history.write().await;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        history.push_back((timestamp, *program_id));

        while history.len() > PATTERN_WINDOW_SIZE {
            history.pop_front();
        }
    }

    async fn update_deployer_reputation(&self, deployer: &Pubkey, risk_score: f64) {
        let new_reputation = 1.0 - risk_score;
        
        self.deployer_reputation
            .entry(*deployer)
            .and_modify(|rep| {
                *rep = (*rep * 0.7) + (new_reputation * 0.3);
            })
            .or_insert(new_reputation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_deployment_analyzer_creation() {
        let analyzer = SmartContractDeploymentAnalyzer::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        assert!(!analyzer.is_running.load(Ordering::SeqCst));
    }
}

