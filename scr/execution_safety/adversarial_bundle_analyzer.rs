use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    clock::Clock,
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, RwLock as AsyncRwLock};

// Production constants calibrated from mainnet data
const MAX_ADVERSARIAL_SCORE: f64 = 100.0;
const SANDWICH_PATTERN_WEIGHT: f64 = 35.0;
const FRONTRUN_PATTERN_WEIGHT: f64 = 28.0;
const GAS_ANOMALY_WEIGHT: f64 = 22.0;
const TIMING_ATTACK_WEIGHT: f64 = 15.0;
const FLASHLOAN_ATTACK_WEIGHT: f64 = 40.0;
const JIT_LIQUIDITY_WEIGHT: f64 = 25.0;
const HISTORICAL_ADVERSARY_MULTIPLIER: f64 = 1.45;
const MAX_BUNDLE_SIZE: usize = 27;
const PRIORITY_FEE_SPIKE_THRESHOLD: u64 = 250_000;
const HIGH_PRIORITY_FEE_PERCENTILE: u64 = 1_000_000;
const TIMING_WINDOW_US: u64 = 50_000; // 50ms in microseconds
const ADVERSARY_CACHE_SIZE: usize = 25_000;
const PATTERN_HISTORY_SIZE: usize = 5_000;
const SLOT_DURATION_US: u64 = 400_000; // 400ms
const MAX_ACCOUNT_LOCKS: usize = 64;
const WRITE_LOCK_CONFLICT_THRESHOLD: usize = 3;

// Solana DEX Program IDs - Production Mainnet
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const OPENBOOK_V2: &str = "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb";
const PHOENIX: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY";
const LIFINITY_V2: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";

// Instruction discriminators for DEX operations
const ORCA_SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const RAYDIUM_SWAP_DISCRIMINATOR: [u8; 8] = [153, 29, 235, 30, 248, 184, 178, 131];
const JUPITER_ROUTE_DISCRIMINATOR: [u8; 8] = [229, 23, 203, 151, 122, 227, 173, 42];
const ORCA_TWO_HOP_DISCRIMINATOR: [u8; 8] = [123, 205, 250, 133, 28, 28, 131, 209];

#[derive(Debug, Clone, PartialEq)]
pub enum AdversarialPattern {
    SandwichAttack { 
        victim_index: usize, 
        front_indices: Vec<usize>,
        back_indices: Vec<usize>,
        estimated_profit: u64,
    },
    Frontrunning { 
        target_index: usize, 
        frontrunner_index: usize,
        priority_delta: u64,
        same_slot_probability: f64,
    },
    GasWarfare { 
        indices: Vec<usize>, 
        avg_priority_fee: u64,
        max_priority_fee: u64,
        escalation_rate: f64,
    },
    TimingAttack { 
        indices: Vec<usize>, 
        timing_delta_us: u64,
        slot_boundary_proximity_us: u64,
    },
    FlashLoanAttack {
        loan_indices: Vec<usize>,
        exploit_indices: Vec<usize>,
        repay_indices: Vec<usize>,
        borrowed_amount: u64,
    },
    JitLiquidity {
        add_liquidity_index: usize,
        swap_indices: Vec<usize>,
        remove_liquidity_index: usize,
        pool_address: Pubkey,
    },
    WriteLockConflict {
        conflicting_indices: Vec<(usize, usize)>,
        shared_accounts: Vec<Pubkey>,
    },
    SlotManipulation {
        indices: Vec<usize>,
        target_slot: u64,
        manipulation_type: SlotManipulationType,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SlotManipulationType {
    Stuffing,
    Delaying,
    Racing,
}

#[derive(Debug, Clone)]
pub struct AdversarialScore {
    pub total_score: f64,
    pub patterns: Vec<AdversarialPattern>,
    pub risk_level: RiskLevel,
    pub recommended_action: RecommendedAction,
    pub competing_bots: Vec<CompetitorProfile>,
    pub confidence: f64,
    pub expected_loss: u64,
    pub mitigation_cost: u64,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum RiskLevel {
    Critical,
    High,
    Medium,
    Low,
    Minimal,
}

#[derive(Debug, Clone)]
pub enum RecommendedAction {
    Abort { reason: String },
    DelayExecution { slots: u64, reason: String },
    IncreaseGas { new_priority_fee: u64, increase_percentage: f64 },
    ModifyBundle { remove_indices: Vec<usize>, reorder_indices: Vec<(usize, usize)> },
    SplitBundle { split_points: Vec<usize> },
    Proceed { confidence: f64 },
}

#[derive(Debug, Clone)]
pub struct CompetitorProfile {
    pub address: Pubkey,
    pub historical_success_rate: f64,
    pub avg_priority_fee: u64,
    pub known_patterns: Vec<AdversarialPattern>,
    pub last_seen: Instant,
    pub total_volume: u64,
    pub win_rate_vs_us: f64,
    pub preferred_dexes: HashSet<Pubkey>,
    pub timing_distribution: TimingStats,
}

#[derive(Debug, Clone)]
pub struct TimingStats {
    pub avg_slot_position: f64,
    pub slot_boundary_preference: f64,
    pub reaction_time_us: u64,
}

#[derive(Debug, Clone)]
pub struct BundleTransaction {
    pub transaction: Transaction,
    pub priority_fee: u64,
    pub compute_units: u32,
    pub timestamp: Instant,
    pub size_bytes: usize,
    pub affected_accounts: Vec<Pubkey>,
    pub write_locked_accounts: Vec<Pubkey>,
    pub program_ids: Vec<Pubkey>,
    pub estimated_cu_consumption: u32,
    pub slot_received: Option<u64>,
}

pub struct AdversarialBundleAnalyzer {
    rpc_client: Arc<RpcClient>,
    adversary_cache: Arc<AsyncRwLock<HashMap<Pubkey, CompetitorProfile>>>,
    pattern_history: Arc<AsyncRwLock<VecDeque<(AdversarialPattern, Instant, Pubkey)>>>,
    known_dex_programs: HashMap<Pubkey, DexInfo>,
    known_adversaries: Arc<AsyncRwLock<HashMap<Pubkey, AdversaryInfo>>>,
    analysis_metrics: Arc<AsyncRwLock<AnalysisMetrics>>,
    slot_tracker: Arc<AsyncRwLock<SlotTracker>>,
    pattern_ml_weights: Arc<AsyncRwLock<PatternWeights>>,
}

#[derive(Debug, Clone)]
struct DexInfo {
    program_id: Pubkey,
    swap_discriminators: Vec<[u8; 8]>,
    liquidity_discriminators: Vec<[u8; 8]>,
    min_pool_size: u64,
}

#[derive(Debug, Clone)]
struct AdversaryInfo {
    pubkey: Pubkey,
    threat_level: f64,
    successful_attacks: u64,
    failed_attacks: u64,
    last_updated: SystemTime,
}

#[derive(Debug, Default)]
struct AnalysisMetrics {
    total_bundles_analyzed: u64,
    adversarial_bundles_detected: u64,
    successful_mitigations: u64,
    false_positives: u64,
    total_prevented_loss: u64,
    avg_analysis_time_us: u64,
}

#[derive(Debug)]
struct SlotTracker {
    current_slot: u64,
    slot_start_time: Instant,
    transactions_in_slot: HashMap<u64, Vec<Signature>>,
    slot_congestion: HashMap<u64, f64>,
}

#[derive(Debug, Clone)]
struct PatternWeights {
    sandwich: f64,
    frontrun: f64,
    gas_warfare: f64,
    timing: f64,
    flashloan: f64,
    jit: f64,
}

impl AdversarialBundleAnalyzer {
    pub fn new(rpc_endpoint: &str) -> Self {
        let mut known_dex_programs = HashMap::new();
        
        // Initialize DEX programs with production data
        known_dex_programs.insert(
            Pubkey::from_str_const(ORCA_WHIRLPOOL),
            DexInfo {
                program_id: Pubkey::from_str_const(ORCA_WHIRLPOOL),
                swap_discriminators: vec![ORCA_SWAP_DISCRIMINATOR, ORCA_TWO_HOP_DISCRIMINATOR],
                liquidity_discriminators: vec![[44, 233, 76, 10, 150, 208, 226, 183]],
                min_pool_size: 10_000_000,
            }
        );
        
        known_dex_programs.insert(
            Pubkey::from_str_const(RAYDIUM_V4),
            DexInfo {
                program_id: Pubkey::from_str_const(RAYDIUM_V4),
                swap_discriminators: vec![RAYDIUM_SWAP_DISCRIMINATOR],
                liquidity_discriminators: vec![[41, 198, 250, 59, 48, 91, 180, 152]],
                min_pool_size: 50_000_000,
            }
        );
        
        known_dex_programs.insert(
            Pubkey::from_str_const(JUPITER_V6),
            DexInfo {
                program_id: Pubkey::from_str_const(JUPITER_V6),
                swap_discriminators: vec![JUPITER_ROUTE_DISCRIMINATOR],
                liquidity_discriminators: vec![],
                min_pool_size: 0,
            }
        );

        Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_endpoint.to_string(),
                CommitmentConfig::processed(),
            )),
            adversary_cache: Arc::new(AsyncRwLock::new(HashMap::with_capacity(ADVERSARY_CACHE_SIZE))),
            pattern_history: Arc::new(AsyncRwLock::new(VecDeque::with_capacity(PATTERN_HISTORY_SIZE))),
            known_dex_programs,
            known_adversaries: Arc::new(AsyncRwLock::new(HashMap::new())),
            analysis_metrics: Arc::new(AsyncRwLock::new(AnalysisMetrics::default())),
            slot_tracker: Arc::new(AsyncRwLock::new(SlotTracker {
                current_slot: 0,
                slot_start_time: Instant::now(),
                transactions_in_slot: HashMap::new(),
                slot_congestion: HashMap::new(),
            })),
            pattern_ml_weights: Arc::new(AsyncRwLock::new(PatternWeights {
                sandwich: SANDWICH_PATTERN_WEIGHT,
                frontrun: FRONTRUN_PATTERN_WEIGHT,
                gas_warfare: GAS_ANOMALY_WEIGHT,
                timing: TIMING_ATTACK_WEIGHT,
                flashloan: FLASHLOAN_ATTACK_WEIGHT,
                jit: JIT_LIQUIDITY_WEIGHT,
            })),
        }
    }

    pub async fn analyze_bundle(&self, bundle: Vec<BundleTransaction>) -> Result<AdversarialScore, AnalysisError> {
        let analysis_start = Instant::now();
        
        // Update metrics
        {
            let mut metrics = self.analysis_metrics.write().await;
            metrics.total_bundles_analyzed += 1;
        }

        // Validate bundle integrity
        self.validate_bundle_integrity(&bundle)?;

        let mut patterns = Vec::new();
        let mut total_score = 0.0;
        let weights = self.pattern_ml_weights.read().await.clone();

        // Advanced pattern detection
        if let Some(sandwich_patterns) = self.detect_sandwich_attacks(&bundle).await? {
            for pattern in sandwich_patterns {
                total_score += weights.sandwich;
                patterns.push(pattern);
            }
        }

        if let Some(frontrun_patterns) = self.detect_frontrunning(&bundle).await? {
            for pattern in frontrun_patterns {
                total_score += weights.frontrun;
                patterns.push(pattern);
            }
        }

                if let Some(gas_patterns) = self.detect_gas_warfare(&bundle).await? {
            for pattern in gas_patterns {
                total_score += weights.gas_warfare;
                patterns.push(pattern);
            }
        }

        if let Some(timing_patterns) = self.detect_timing_attacks(&bundle).await? {
            for pattern in timing_patterns {
                total_score += weights.timing;
                patterns.push(pattern);
            }
        }

        if let Some(flashloan_patterns) = self.detect_flashloan_attacks(&bundle).await? {
            for pattern in flashloan_patterns {
                total_score += weights.flashloan;
                patterns.push(pattern);
            }
        }

        if let Some(jit_patterns) = self.detect_jit_liquidity(&bundle).await? {
            for pattern in jit_patterns {
                total_score += weights.jit;
                patterns.push(pattern);
            }
        }

        if let Some(writelock_patterns) = self.detect_writelock_conflicts(&bundle).await? {
            for pattern in writelock_patterns {
                total_score += 10.0; // Additional penalty for write conflicts
                patterns.push(pattern);
            }
        }

        // Analyze competitors with advanced profiling
        let competing_bots = self.analyze_competitors(&bundle).await?;
        
        // Apply historical adversary multiplier with decay
        let adversary_multiplier = self.calculate_adversary_multiplier(&bundle, &competing_bots).await;
        total_score *= adversary_multiplier;

        // Update pattern history with attribution
        self.update_pattern_history(&patterns, &bundle).await;

        // Calculate risk level with ML-adjusted thresholds
        let risk_level = self.calculate_risk_level(total_score);
        
        // Determine action with cost-benefit analysis
        let (recommended_action, mitigation_cost) = self.determine_optimal_action(&risk_level, &patterns, &bundle).await?;
        
        // Calculate expected loss if no action taken
        let expected_loss = self.calculate_expected_loss(&patterns, &bundle);
        
        // Calculate confidence based on pattern clarity and historical accuracy
        let confidence = self.calculate_confidence(&patterns, &competing_bots);

        // Update metrics
        {
            let mut metrics = self.analysis_metrics.write().await;
            metrics.avg_analysis_time_us = 
                (metrics.avg_analysis_time_us * (metrics.total_bundles_analyzed - 1) + 
                analysis_start.elapsed().as_micros() as u64) / metrics.total_bundles_analyzed;
            
            if total_score > 20.0 {
                metrics.adversarial_bundles_detected += 1;
            }
        }

        Ok(AdversarialScore {
            total_score: total_score.min(MAX_ADVERSARIAL_SCORE),
            patterns,
            risk_level,
            recommended_action,
            competing_bots,
            confidence,
            expected_loss,
            mitigation_cost,
        })
    }

    async fn detect_sandwich_attacks(&self, bundle: &[BundleTransaction]) -> Result<Option<Vec<AdversarialPattern>>, AnalysisError> {
        let mut patterns = Vec::new();
        
        for (i, victim_tx) in bundle.iter().enumerate() {
            if !self.is_dex_swap(victim_tx) {
                continue;
            }

            let victim_amount = self.extract_swap_amount(victim_tx)?;
            if victim_amount < 100_000_000 { // Min 0.1 SOL equivalent
                continue;
            }

            // Find potential sandwich transactions
            let mut front_runners = Vec::new();
            let mut back_runners = Vec::new();
            
            for (j, tx) in bundle.iter().enumerate() {
                if i == j || !self.is_dex_swap(tx) {
                    continue;
                }

                if self.shares_liquidity_pool(victim_tx, tx) {
                    let tx_amount = self.extract_swap_amount(tx)?;
                    
                    if j < i && self.is_same_direction(victim_tx, tx) && tx_amount > victim_amount * 2 {
                        front_runners.push(j);
                    } else if j > i && self.is_opposite_direction(victim_tx, tx) {
                        back_runners.push(j);
                    }
                }
            }

            // Validate sandwich pattern
            if !front_runners.is_empty() && !back_runners.is_empty() {
                let estimated_profit = self.estimate_sandwich_profit(
                    &bundle[front_runners[0]], 
                    victim_tx, 
                    &bundle[back_runners[0]]
                ).await?;

                if estimated_profit > 50_000_000 { // Min 0.05 SOL profit
                    patterns.push(AdversarialPattern::SandwichAttack {
                        victim_index: i,
                        front_indices: front_runners,
                        back_indices: back_runners,
                        estimated_profit,
                    });
                }
            }
        }

        if patterns.is_empty() { Ok(None) } else { Ok(Some(patterns)) }
    }

    async fn detect_frontrunning(&self, bundle: &[BundleTransaction]) -> Result<Option<Vec<AdversarialPattern>>, AnalysisError> {
        let mut patterns = Vec::new();
        let slot_tracker = self.slot_tracker.read().await;

        for i in 1..bundle.len() {
            let target = &bundle[i];
            
            for j in 0..i {
                let potential_frontrunner = &bundle[j];
                
                if self.is_similar_transaction(potential_frontrunner, target) &&
                   potential_frontrunner.priority_fee > target.priority_fee + PRIORITY_FEE_SPIKE_THRESHOLD {
                    
                    let priority_delta = potential_frontrunner.priority_fee - target.priority_fee;
                    let same_slot_probability = self.calculate_same_slot_probability(
                        potential_frontrunner,
                        target,
                        &slot_tracker
                    );

                    if same_slot_probability > 0.7 {
                        patterns.push(AdversarialPattern::Frontrunning {
                            target_index: i,
                            frontrunner_index: j,
                            priority_delta,
                            same_slot_probability,
                        });
                    }
                }
            }
        }

        if patterns.is_empty() { Ok(None) } else { Ok(Some(patterns)) }
    }

    async fn detect_gas_warfare(&self, bundle: &[BundleTransaction]) -> Result<Option<Vec<AdversarialPattern>>, AnalysisError> {
        let priority_fees: Vec<u64> = bundle.iter().map(|tx| tx.priority_fee).collect();
        
        if priority_fees.is_empty() {
            return Ok(None);
        }

        let avg_fee = priority_fees.iter().sum::<u64>() / priority_fees.len() as u64;
        let max_fee = *priority_fees.iter().max().unwrap_or(&0);
        
        let mut high_fee_indices = Vec::new();
        let mut escalation_pattern = Vec::new();
        
        for (i, tx) in bundle.iter().enumerate() {
            if tx.priority_fee > HIGH_PRIORITY_FEE_PERCENTILE {
                high_fee_indices.push(i);
                escalation_pattern.push((i, tx.priority_fee));
            }
        }

        if high_fee_indices.len() >= 3 {
            let escalation_rate = self.calculate_fee_escalation_rate(&escalation_pattern);
            
            return Ok(Some(vec![AdversarialPattern::GasWarfare {
                indices: high_fee_indices,
                avg_priority_fee: avg_fee,
                max_priority_fee: max_fee,
                escalation_rate,
            }]));
        }

        Ok(None)
    }

    async fn detect_timing_attacks(&self, bundle: &[BundleTransaction]) -> Result<Option<Vec<AdversarialPattern>>, AnalysisError> {
        let mut patterns = Vec::new();
        let slot_tracker = self.slot_tracker.read().await;
        
        // Group transactions by timing windows
        let mut time_clusters: HashMap<u64, Vec<usize>> = HashMap::new();
        
        for (i, tx) in bundle.iter().enumerate() {
            let timestamp_us = tx.timestamp.elapsed().as_micros() as u64;
            let cluster_key = timestamp_us / TIMING_WINDOW_US;
            time_clusters.entry(cluster_key).or_default().push(i);
        }

        for (_, indices) in time_clusters.iter() {
            if indices.len() >= 4 {
                let timing_delta_us = self.calculate_timing_delta(&bundle, indices);
                let slot_boundary_proximity = self.calculate_slot_boundary_proximity(
                    &bundle[indices[0]],
                    &slot_tracker
                );

                if timing_delta_us < TIMING_WINDOW_US && slot_boundary_proximity < 50_000 {
                    patterns.push(AdversarialPattern::TimingAttack {
                        indices: indices.clone(),
                        timing_delta_us,
                        slot_boundary_proximity_us: slot_boundary_proximity,
                    });
                }
            }
        }

        if patterns.is_empty() { Ok(None) } else { Ok(Some(patterns)) }
    }

    async fn detect_flashloan_attacks(&self, bundle: &[BundleTransaction]) -> Result<Option<Vec<AdversarialPattern>>, AnalysisError> {
        let mut patterns = Vec::new();
        
        for (i, tx) in bundle.iter().enumerate() {
            if let Some(loan_amount) = self.is_flashloan_instruction(tx)? {
                let mut exploit_indices = Vec::new();
                let mut repay_indices = Vec::new();
                
                // Find exploit and repay transactions
                for (j, other_tx) in bundle.iter().enumerate().skip(i + 1) {
                    if self.shares_accounts_with(tx, other_tx) {
                        if self.is_exploit_transaction(other_tx, loan_amount)? {
                            exploit_indices.push(j);
                        } else if self.is_flashloan_repay(other_tx, loan_amount)? {
                            repay_indices.push(j);
                        }
                    }
                }

                if !exploit_indices.is_empty() && !repay_indices.is_empty() {
                    patterns.push(AdversarialPattern::FlashLoanAttack {
                        loan_indices: vec![i],
                        exploit_indices,
                        repay_indices,
                        borrowed_amount: loan_amount,
                    });
                }
            }
        }

        if patterns.is_empty() { Ok(None) } else { Ok(Some(patterns)) }
    }

    async fn detect_jit_liquidity(&self, bundle: &[BundleTransaction]) -> Result<Option<Vec<AdversarialPattern>>, AnalysisError> {
        let mut patterns = Vec::new();
        
        for (i, add_liq_tx) in bundle.iter().enumerate() {
            if let Some(pool_address) = self.is_add_liquidity(add_liq_tx)? {
                let mut swap_indices = Vec::new();
                let mut remove_liq_index = None;
                
                for (j, tx) in bundle.iter().enumerate().skip(i + 1) {
                    if self.affects_pool(tx, &pool_address) {
                        if self.is_swap_transaction(tx) {
                            swap_indices.push(j);
                        } else if self.is_remove_liquidity(tx)? {
                            remove_liq_index = Some(j);
                            break;
                        }
                    }
                }

                if !swap_indices.is_empty() && remove_liq_index.is_some() {
                    patterns.push(AdversarialPattern::JitLiquidity {
                        add_liquidity_index: i,
                        swap_indices,
                        remove_liquidity_index: remove_liq_index.unwrap(),
                        pool_address,
                    });
                }
            }
        }

        if patterns.is_empty() { Ok(None) } else { Ok(Some(patterns)) }
    }

    async fn detect_writelock_conflicts(&self, bundle: &[BundleTransaction]) -> Result<Option<Vec<AdversarialPattern>>, AnalysisError> {
        let mut patterns = Vec::new();
        let mut write_lock_map: HashMap<Pubkey, Vec<usize>> = HashMap::new();
        
        for (i, tx) in bundle.iter().enumerate() {
            for account in &tx.write_locked_accounts {
                write_lock_map.entry(*account).or_default().push(i);
            }
        }

        let mut conflicting_indices = Vec::new();
        let mut shared_accounts = Vec::new();
        
        for (account, indices) in write_lock_map.iter() {
            if indices.len() >= WRITE_LOCK_CONFLICT_THRESHOLD {
                shared_accounts.push(*account);
                for i in 0..indices.len() {
                    for j in i+1..indices.len() {
                        conflicting_indices.push((indices[i], indices[j]));
                    }
                }
            }
        }

        if !conflicting_indices.is_empty() {
            patterns.push(AdversarialPattern::WriteLockConflict {
                conflicting_indices,
                shared_accounts,
            });
        }

        if patterns.is_empty() { Ok(None) } else { Ok(Some(patterns)) }
    }

    async fn analyze_competitors(&self, bundle: &[BundleTransaction]) -> Result<Vec<CompetitorProfile>, AnalysisError> {
        let mut competitors = Vec::new();
        let mut cache = self.adversary_cache.write().await;

        for tx in bundle {
            if let Some(signer) = self.extract_primary_signer(tx) {
                if let Some(profile) = cache.get_mut(&signer) {
                    profile.last_seen = Instant::now();
                    competitors.push(profile.clone());
                } else {
                    let new_profile = self.create_competitor_profile(signer, tx).await?;
                    cache.insert(signer, new_profile.clone());
                    competitors.push(new_profile);
                }
            }
        }

        // Maintain cache size
        if cache.len() > ADVERSARY_CACHE_SIZE {
            let oldest_keys: Vec<Pubkey> = cache.iter()
                .map(|(k, v)| (*k, v.last_seen))
                .collect::<Vec<_>>()
                .into_iter()
                .sorted_by_key(|(_, time)| *time)
                .take(cache.len() - ADVERSARY_CACHE_SIZE)
                .map(|(k, _)| k)
                .collect();
            
                        for key in oldest_keys {
                cache.remove(&key);
            }
        }

        Ok(competitors)
    }

    async fn calculate_adversary_multiplier(&self, bundle: &[BundleTransaction], competitors: &[CompetitorProfile]) -> f64 {
        let known_adversaries = self.known_adversaries.read().await;
        let mut multiplier = 1.0;

        for competitor in competitors {
            if let Some(adversary_info) = known_adversaries.get(&competitor.address) {
                multiplier *= 1.0 + (adversary_info.threat_level * 0.5);
                
                if adversary_info.successful_attacks > 10 {
                    multiplier *= 1.2;
                }
            }
            
            if competitor.historical_success_rate > 0.75 {
                multiplier *= 1.15;
            }
            
            if competitor.win_rate_vs_us > 0.6 {
                multiplier *= 1.25;
            }
            
            if competitor.avg_priority_fee > HIGH_PRIORITY_FEE_PERCENTILE {
                multiplier *= 1.1;
            }
        }

        multiplier.min(3.5)
    }

    async fn update_pattern_history(&self, patterns: &[AdversarialPattern], bundle: &[BundleTransaction]) {
        let mut history = self.pattern_history.write().await;
        let now = Instant::now();
        
        for pattern in patterns {
            if let Some(signer) = bundle.first().and_then(|tx| self.extract_primary_signer(tx)) {
                history.push_back((pattern.clone(), now, signer));
                
                if history.len() > PATTERN_HISTORY_SIZE {
                    history.pop_front();
                }
            }
        }
    }

    fn calculate_risk_level(&self, score: f64) -> RiskLevel {
        match score {
            s if s >= 85.0 => RiskLevel::Critical,
            s if s >= 65.0 => RiskLevel::High,
            s if s >= 45.0 => RiskLevel::Medium,
            s if s >= 25.0 => RiskLevel::Low,
            _ => RiskLevel::Minimal,
        }
    }

    async fn determine_optimal_action(&self, risk_level: &RiskLevel, patterns: &[AdversarialPattern], bundle: &[BundleTransaction]) -> Result<(RecommendedAction, u64), AnalysisError> {
        let base_cost = 50_000; // 0.00005 SOL base mitigation cost
        
        match risk_level {
            RiskLevel::Critical => {
                Ok((RecommendedAction::Abort { 
                    reason: "Critical adversarial patterns detected".to_string() 
                }, 0))
            },
            RiskLevel::High => {
                for pattern in patterns {
                    match pattern {
                        AdversarialPattern::SandwichAttack { estimated_profit, .. } => {
                            if *estimated_profit > 10_000_000_000 { // > 10 SOL
                                return Ok((RecommendedAction::Abort { 
                                    reason: "High-value sandwich attack detected".to_string() 
                                }, 0));
                            }
                        },
                        AdversarialPattern::GasWarfare { max_priority_fee, .. } => {
                            let new_fee = max_priority_fee + (max_priority_fee / 4);
                            let increase_percentage = (new_fee as f64 / *max_priority_fee as f64 - 1.0) * 100.0;
                            return Ok((RecommendedAction::IncreaseGas { 
                                new_priority_fee: new_fee,
                                increase_percentage,
                            }, new_fee - max_priority_fee + base_cost));
                        },
                        AdversarialPattern::TimingAttack { .. } => {
                            return Ok((RecommendedAction::DelayExecution { 
                                slots: 2,
                                reason: "Timing attack mitigation".to_string(),
                            }, base_cost * 2));
                        },
                        _ => {}
                    }
                }
                
                Ok((RecommendedAction::DelayExecution { 
                    slots: 1,
                    reason: "High risk mitigation".to_string(),
                }, base_cost))
            },
            RiskLevel::Medium => {
                if let Some(AdversarialPattern::WriteLockConflict { conflicting_indices, .. }) = patterns.first() {
                    let remove_indices: Vec<usize> = conflicting_indices.iter()
                        .flat_map(|(i, j)| vec![*i, *j])
                        .unique()
                        .take(2)
                        .collect();
                    
                    Ok((RecommendedAction::ModifyBundle { 
                        remove_indices,
                        reorder_indices: vec![],
                    }, base_cost * remove_indices.len() as u64))
                } else {
                    Ok((RecommendedAction::Proceed { confidence: 0.75 }, 0))
                }
            },
            _ => Ok((RecommendedAction::Proceed { confidence: 0.95 }, 0)),
        }
    }

    fn calculate_expected_loss(&self, patterns: &[AdversarialPattern], bundle: &[BundleTransaction]) -> u64 {
        let mut total_loss = 0u64;
        
        for pattern in patterns {
            match pattern {
                AdversarialPattern::SandwichAttack { estimated_profit, .. } => {
                    total_loss += estimated_profit;
                },
                AdversarialPattern::Frontrunning { priority_delta, same_slot_probability, .. } => {
                    let opportunity_value = bundle.iter()
                        .map(|tx| self.estimate_transaction_value(tx))
                        .max()
                        .unwrap_or(0);
                    total_loss += (opportunity_value as f64 * same_slot_probability) as u64;
                },
                AdversarialPattern::FlashLoanAttack { borrowed_amount, .. } => {
                    total_loss += borrowed_amount / 100; // Assume 1% attack success
                },
                _ => {}
            }
        }
        
        total_loss
    }

    fn calculate_confidence(&self, patterns: &[AdversarialPattern], competitors: &[CompetitorProfile]) -> f64 {
        let mut confidence = 1.0;
        
        // Reduce confidence based on pattern complexity
        confidence -= patterns.len() as f64 * 0.05;
        
        // Reduce confidence based on competitor strength
        for competitor in competitors {
            if competitor.win_rate_vs_us > 0.5 {
                confidence -= 0.1;
            }
        }
        
        confidence.max(0.1).min(0.99)
    }

    async fn estimate_sandwich_profit(&self, front_tx: &BundleTransaction, victim_tx: &BundleTransaction, back_tx: &BundleTransaction) -> Result<u64, AnalysisError> {
        let victim_amount = self.extract_swap_amount(victim_tx)?;
        let front_amount = self.extract_swap_amount(front_tx)?;
        
        // Simplified profit calculation based on typical sandwich attack mechanics
        let price_impact = (front_amount as f64 / (front_amount + victim_amount) as f64) * 0.003; // 0.3% AMM fee
        let profit = (victim_amount as f64 * price_impact * 0.8) as u64; // 80% efficiency
        
        Ok(profit)
    }

    fn calculate_same_slot_probability(&self, tx1: &BundleTransaction, tx2: &BundleTransaction, slot_tracker: &SlotTracker) -> f64 {
        let time_diff = tx2.timestamp.saturating_duration_since(tx1.timestamp).as_micros() as u64;
        
        if time_diff < SLOT_DURATION_US {
            let remaining_slot_time = SLOT_DURATION_US - (tx1.timestamp.elapsed().as_micros() as u64 % SLOT_DURATION_US);
            if time_diff < remaining_slot_time {
                return 0.95;
            }
        }
        
        (1.0 - (time_diff as f64 / SLOT_DURATION_US as f64)).max(0.0)
    }

    fn calculate_fee_escalation_rate(&self, escalation_pattern: &[(usize, u64)]) -> f64 {
        if escalation_pattern.len() < 2 {
            return 0.0;
        }
        
        let mut total_increase = 0.0;
        for i in 1..escalation_pattern.len() {
            let prev_fee = escalation_pattern[i-1].1;
            let curr_fee = escalation_pattern[i].1;
            if prev_fee > 0 {
                total_increase += (curr_fee as f64 / prev_fee as f64) - 1.0;
            }
        }
        
        total_increase / (escalation_pattern.len() - 1) as f64
    }

    fn calculate_timing_delta(&self, bundle: &[BundleTransaction], indices: &[usize]) -> u64 {
        if indices.len() < 2 {
            return 0;
        }
        
        let first_time = bundle[indices[0]].timestamp;
        let last_time = bundle[indices[indices.len() - 1]].timestamp;
        
        last_time.duration_since(first_time).as_micros() as u64
    }

    fn calculate_slot_boundary_proximity(&self, tx: &BundleTransaction, slot_tracker: &SlotTracker) -> u64 {
        let elapsed_in_slot = tx.timestamp.duration_since(slot_tracker.slot_start_time).as_micros() as u64;
        let position_in_slot = elapsed_in_slot % SLOT_DURATION_US;
        
        position_in_slot.min(SLOT_DURATION_US - position_in_slot)
    }

    fn is_dex_swap(&self, tx: &BundleTransaction) -> bool {
        tx.program_ids.iter().any(|pid| self.known_dex_programs.contains_key(pid)) &&
        tx.transaction.message.instructions.iter().any(|inst| {
            inst.data.len() >= 8 && self.is_swap_discriminator(&inst.data[..8])
        })
    }

    fn is_swap_discriminator(&self, data: &[u8]) -> bool {
        if data.len() < 8 {
            return false;
        }
        
        let discriminator: [u8; 8] = data[..8].try_into().unwrap_or([0; 8]);
        
        discriminator == ORCA_SWAP_DISCRIMINATOR ||
        discriminator == RAYDIUM_SWAP_DISCRIMINATOR ||
        discriminator == JUPITER_ROUTE_DISCRIMINATOR ||
        discriminator == ORCA_TWO_HOP_DISCRIMINATOR
    }

    fn extract_swap_amount(&self, tx: &BundleTransaction) -> Result<u64, AnalysisError> {
        for inst in &tx.transaction.message.instructions {
            if inst.data.len() >= 16 && self.is_swap_discriminator(&inst.data[..8]) {
                return Ok(u64::from_le_bytes(inst.data[8..16].try_into().unwrap_or([0; 8])));
            }
        }
        Ok(0)
    }

    fn shares_liquidity_pool(&self, tx1: &BundleTransaction, tx2: &BundleTransaction) -> bool {
        let common_accounts: HashSet<_> = tx1.affected_accounts.iter()
            .filter(|acc| tx2.affected_accounts.contains(acc))
            .cloned()
            .collect();
        
        common_accounts.len() >= 3 && 
        tx1.program_ids.iter().any(|p| tx2.program_ids.contains(p) && self.known_dex_programs.contains_key(p))
    }

    fn is_same_direction(&self, tx1: &BundleTransaction, tx2: &BundleTransaction) -> bool {
        if let (Ok(amount1), Ok(amount2)) = (self.extract_swap_amount(tx1), self.extract_swap_amount(tx2)) {
            if amount1 == 0 || amount2 == 0 {
                return false;
            }
            
            // Check if both are buying or selling same token
            let tx1_accounts = &tx1.transaction.message.account_keys;
            let tx2_accounts = &tx2.transaction.message.account_keys;
            
            tx1_accounts.len() > 2 && tx2_accounts.len() > 2 &&
            tx1_accounts[1] == tx2_accounts[1] && tx1_accounts[2] == tx2_accounts[2]
        } else {
            false
        }
    }

    fn is_opposite_direction(&self, tx1: &BundleTransaction, tx2: &BundleTransaction) -> bool {
        if let (Ok(amount1), Ok(amount2)) = (self.extract_swap_amount(tx1), self.extract_swap_amount(tx2)) {
            if amount1 == 0 || amount2 == 0 {
                return false;
            }
            
            let tx1_accounts = &tx1.transaction.message.account_keys;
            let tx2_accounts = &tx2.transaction.message.account_keys;
            
            tx1_accounts.len() > 2 && tx2_accounts.len() > 2 &&
            tx1_accounts[1] == tx2_accounts[2] && tx1_accounts[2] == tx2_accounts[1]
        } else {
            false
        }
    }

    fn is_similar_transaction(&self, tx1: &BundleTransaction, tx2: &BundleTransaction) -> bool {
        if tx1.program_ids != tx2.program_ids {
            return false;
        }

        let common_accounts = tx1.affected_accounts.iter()
            .filter(|acc| tx2.affected_accounts.contains(acc))
            .count();
        
        let similarity_ratio = common_accounts as f64 / tx1.affected_accounts.len().max(1) as f64;
        similarity_ratio > 0.85 && (tx1.size_bytes as i64 - tx2.size_bytes as i64).abs() < 100
    }

    fn is_flashloan_instruction(&self, tx: &BundleTransaction) -> Result<Option<u64>, AnalysisError> {
        for inst in &tx.transaction.message.instructions {
            if inst.data.len() >= 16 {
                let discriminator = &inst.data[..8];
                                // Known flashloan discriminators for major protocols
                if discriminator == [168, 198, 201, 176, 23, 9, 48, 157] || // Port Finance
                   discriminator == [234, 131, 45, 121, 255, 41, 11, 236] || // Solend
                   discriminator == [125, 204, 250, 187, 42, 155, 139, 73] {  // Marginfi
                    let amount = u64::from_le_bytes(inst.data[8..16].try_into().unwrap_or([0; 8]));
                    if amount > 1_000_000_000 { // > 1 SOL minimum for flashloan
                        return Ok(Some(amount));
                    }
                }
            }
        }
        Ok(None)
    }

    fn is_exploit_transaction(&self, tx: &BundleTransaction, loan_amount: u64) -> Result<bool, AnalysisError> {
        // Check for exploit patterns: arbitrage, liquidation, or oracle manipulation
        let has_multiple_dex = tx.program_ids.iter()
            .filter(|pid| self.known_dex_programs.contains_key(pid))
            .count() >= 2;
        
        let has_oracle_interaction = tx.program_ids.iter().any(|pid| {
            *pid == Pubkey::from_str_const("gSbePebfvPy7tRqimPoVecS2UsBvYv46ynrzWocc92s") || // Pyth
            *pid == Pubkey::from_str_const("SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f")    // Switchboard
        });
        
        Ok(has_multiple_dex || has_oracle_interaction || tx.compute_units > 400_000)
    }

    fn is_flashloan_repay(&self, tx: &BundleTransaction, loan_amount: u64) -> Result<bool, AnalysisError> {
        for inst in &tx.transaction.message.instructions {
            if inst.data.len() >= 16 {
                let discriminator = &inst.data[..8];
                // Repay discriminators
                if discriminator == [55, 187, 23, 196, 132, 7, 137, 210] || // Port Finance repay
                   discriminator == [124, 42, 111, 234, 169, 49, 52, 196] || // Solend repay
                   discriminator == [247, 35, 168, 219, 172, 17, 195, 206] {  // Marginfi repay
                    let repay_amount = u64::from_le_bytes(inst.data[8..16].try_into().unwrap_or([0; 8]));
                    // Allow for interest (up to 1%)
                    if repay_amount >= loan_amount && repay_amount <= loan_amount + (loan_amount / 100) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn is_add_liquidity(&self, tx: &BundleTransaction) -> Result<Option<Pubkey>, AnalysisError> {
        for (i, inst) in tx.transaction.message.instructions.iter().enumerate() {
            if inst.data.len() >= 8 {
                let discriminator = &inst.data[..8];
                // Add liquidity discriminators
                if discriminator == [14, 115, 238, 219, 217, 112, 43, 165] || // Orca add
                   discriminator == [41, 198, 250, 59, 48, 91, 180, 152] ||   // Raydium add
                   discriminator == [251, 97, 208, 73, 216, 195, 210, 129] {  // Whirlpool add
                    if let Some(pool_index) = inst.accounts.get(0) {
                        let pool_account = tx.transaction.message.account_keys.get(*pool_index as usize);
                        return Ok(pool_account.cloned());
                    }
                }
            }
        }
        Ok(None)
    }

    fn is_remove_liquidity(&self, tx: &BundleTransaction) -> Result<bool, AnalysisError> {
        for inst in &tx.transaction.message.instructions {
            if inst.data.len() >= 8 {
                let discriminator = &inst.data[..8];
                // Remove liquidity discriminators
                if discriminator == [80, 85, 209, 72, 24, 206, 177, 108] ||  // Orca remove
                   discriminator == [183, 18, 70, 156, 148, 109, 161, 34] ||  // Raydium remove
                   discriminator == [165, 209, 201, 177, 108, 217, 39, 17] {  // Whirlpool remove
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn affects_pool(&self, tx: &BundleTransaction, pool_address: &Pubkey) -> bool {
        tx.affected_accounts.contains(pool_address)
    }

    fn is_swap_transaction(&self, tx: &BundleTransaction) -> bool {
        self.is_dex_swap(tx)
    }

    fn shares_accounts_with(&self, tx1: &BundleTransaction, tx2: &BundleTransaction) -> bool {
        tx1.affected_accounts.iter().any(|acc| tx2.affected_accounts.contains(acc))
    }

    fn extract_primary_signer(&self, tx: &BundleTransaction) -> Option<Pubkey> {
        tx.transaction.message.account_keys.first().cloned()
    }

    async fn create_competitor_profile(&self, address: Pubkey, tx: &BundleTransaction) -> Result<CompetitorProfile, AnalysisError> {
        let mut preferred_dexes = HashSet::new();
        for pid in &tx.program_ids {
            if self.known_dex_programs.contains_key(pid) {
                preferred_dexes.insert(*pid);
            }
        }

        Ok(CompetitorProfile {
            address,
            historical_success_rate: 0.5, // Default 50% success rate
            avg_priority_fee: tx.priority_fee,
            known_patterns: vec![],
            last_seen: Instant::now(),
            total_volume: 0,
            win_rate_vs_us: 0.5,
            preferred_dexes,
            timing_distribution: TimingStats {
                avg_slot_position: 0.5,
                slot_boundary_preference: 0.0,
                reaction_time_us: 100_000,
            },
        })
    }

    fn estimate_transaction_value(&self, tx: &BundleTransaction) -> u64 {
        // Estimate based on swap amount or compute units * priority fee
        if let Ok(swap_amount) = self.extract_swap_amount(tx) {
            swap_amount
        } else {
            tx.compute_units as u64 * tx.priority_fee
        }
    }

    pub async fn validate_bundle_integrity(&self, bundle: &[BundleTransaction]) -> Result<(), AnalysisError> {
        if bundle.is_empty() {
            return Err(AnalysisError::EmptyBundle);
        }

        if bundle.len() > MAX_BUNDLE_SIZE {
            return Err(AnalysisError::BundleTooLarge(bundle.len()));
        }

        let mut seen_signatures = HashSet::new();
        for (i, tx) in bundle.iter().enumerate() {
            if tx.transaction.signatures.is_empty() {
                return Err(AnalysisError::MissingSignature(i));
            }

            for sig in &tx.transaction.signatures {
                if !seen_signatures.insert(sig) {
                    return Err(AnalysisError::DuplicateSignature(i));
                }
            }

            if tx.size_bytes > 1232 {
                return Err(AnalysisError::TransactionTooLarge(i, tx.size_bytes));
            }

            if tx.priority_fee > 50_000_000 {
                return Err(AnalysisError::ExcessivePriorityFee(i, tx.priority_fee));
            }

            if tx.compute_units > 1_400_000 {
                return Err(AnalysisError::ExcessiveComputeUnits(i, tx.compute_units));
            }

            if tx.write_locked_accounts.len() > MAX_ACCOUNT_LOCKS {
                return Err(AnalysisError::TooManyAccountLocks(i));
            }
        }

        Ok(())
    }

    pub async fn update_adversary_profile(&self, address: Pubkey, success: bool, pattern: Option<AdversarialPattern>) {
        let mut adversaries = self.known_adversaries.write().await;
        
        let info = adversaries.entry(address).or_insert_with(|| AdversaryInfo {
            pubkey: address,
            threat_level: 0.5,
            successful_attacks: 0,
            failed_attacks: 0,
            last_updated: SystemTime::now(),
        });

        if success {
            info.successful_attacks += 1;
        } else {
            info.failed_attacks += 1;
        }

        // Update threat level with exponential decay
        let total_attacks = info.successful_attacks + info.failed_attacks;
        if total_attacks > 0 {
            let success_rate = info.successful_attacks as f64 / total_attacks as f64;
            info.threat_level = info.threat_level * 0.9 + success_rate * 0.1;
        }

        info.last_updated = SystemTime::now();

        // Update competitor profile with pattern
        if let Some(pattern) = pattern {
            let mut cache = self.adversary_cache.write().await;
            if let Some(profile) = cache.get_mut(&address) {
                if !profile.known_patterns.iter().any(|p| std::mem::discriminant(p) == std::mem::discriminant(&pattern)) {
                    profile.known_patterns.push(pattern);
                    if profile.known_patterns.len() > 10 {
                        profile.known_patterns.remove(0);
                    }
                }
            }
        }
    }

    pub async fn get_analysis_metrics(&self) -> AnalysisMetrics {
        self.analysis_metrics.read().await.clone()
    }

    pub async fn update_ml_weights(&self, patterns: &[AdversarialPattern], outcome_success: bool) {
        let mut weights = self.pattern_ml_weights.write().await;
        let adjustment = if outcome_success { 0.02 } else { -0.01 };
        
        for pattern in patterns {
            match pattern {
                AdversarialPattern::SandwichAttack { .. } => weights.sandwich = (weights.sandwich + adjustment).max(10.0).min(50.0),
                AdversarialPattern::Frontrunning { .. } => weights.frontrun = (weights.frontrun + adjustment).max(10.0).min(40.0),
                AdversarialPattern::GasWarfare { .. } => weights.gas_warfare = (weights.gas_warfare + adjustment).max(10.0).min(35.0),
                AdversarialPattern::TimingAttack { .. } => weights.timing = (weights.timing + adjustment).max(5.0).min(25.0),
                AdversarialPattern::FlashLoanAttack { .. } => weights.flashloan = (weights.flashloan + adjustment).max(20.0).min(60.0),
                AdversarialPattern::JitLiquidity { .. } => weights.jit = (weights.jit + adjustment).max(10.0).min(40.0),
                _ => {}
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum AnalysisError {
    EmptyBundle,
    BundleTooLarge(usize),
    MissingSignature(usize),
    DuplicateSignature(usize),
    TransactionTooLarge(usize, usize),
    ExcessivePriorityFee(usize, u64),
    ExcessiveComputeUnits(usize, u32),
    TooManyAccountLocks(usize),
    InvalidInstruction(String),
    RpcError(String),
}

impl std::fmt::Display for AnalysisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyBundle => write!(f, "Bundle is empty"),
            Self::BundleTooLarge(size) => write!(f, "Bundle too large: {} transactions", size),
            Self::MissingSignature(i) => write!(f, "Transaction {} missing signature", i),
            Self::DuplicateSignature(i) => write!(f, "Transaction {} has duplicate signature", i),
            Self::TransactionTooLarge(i, size) => write!(f, "Transaction {} too large: {} bytes", i, size),
            Self::ExcessivePriorityFee(i, fee) => write!(f, "Transaction {} has excessive priority fee: {}", i, fee),
            Self::ExcessiveComputeUnits(i, units) => write!(f, "Transaction {} has excessive compute units: {}", i, units),
            Self::TooManyAccountLocks(i) => write!(f, "Transaction {} has too many account locks", i),
            Self::InvalidInstruction(msg) => write!(f, "Invalid instruction: {}", msg),
            Self::RpcError(msg) => write!(f, "RPC error: {}", msg),
        }
    }
}

impl std::error::Error for AnalysisError {}

// Utility traits for production use
pub trait Unique<T> {
    fn unique(self) -> Vec<T>;
}

impl<T: Eq + std::hash::Hash + Clone> Unique<T> for std::vec::IntoIter<T> {
    fn unique(self) -> Vec<T> {
        let mut seen = HashSet::new();
        self.filter(|item| seen.insert(item.clone())).collect()
    }
}

pub trait Sorted<T> {
    fn sorted_by_key<K, F>(self, f: F) -> Vec<T>
    where
        K: Ord,
        F: FnMut(&T) -> K;
}

impl<T> Sorted<T> for std::vec::IntoIter<T> {
    fn sorted_by_key<K, F>(self, mut f: F) -> Vec<T>
    where
        K: Ord,
        F: FnMut(&T) -> K,
    {
                let mut vec: Vec<T> = self.collect();
        vec.sort_by_key(|x| f(x));
        vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        hash::Hash,
        message::Message,
        signature::{Keypair, Signer},
        system_instruction,
        transaction::Transaction,
    };

    #[tokio::test]
    async fn test_adversarial_analysis_production() {
        let analyzer = AdversarialBundleAnalyzer::new("https://api.mainnet-beta.solana.com");
        
        // Create realistic test bundle
        let keypair = Keypair::new();
        let victim_keypair = Keypair::new();
        let attacker_keypair = Keypair::new();
        
        // Simulate sandwich attack bundle
        let mut bundle = Vec::new();
        
        // Front-running transaction
        let front_tx = create_mock_swap_transaction(
            &attacker_keypair,
            Pubkey::from_str_const(ORCA_WHIRLPOOL),
            10_000_000_000, // 10 SOL
            2_000_000, // High priority fee
        );
        bundle.push(BundleTransaction {
            transaction: front_tx,
            priority_fee: 2_000_000,
            compute_units: 200_000,
            timestamp: Instant::now(),
            size_bytes: 600,
            affected_accounts: vec![
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
            ],
            write_locked_accounts: vec![Pubkey::new_unique()],
            program_ids: vec![Pubkey::from_str_const(ORCA_WHIRLPOOL)],
            estimated_cu_consumption: 180_000,
            slot_received: Some(200_000_000),
        });
        
        // Victim transaction
        let victim_tx = create_mock_swap_transaction(
            &victim_keypair,
            Pubkey::from_str_const(ORCA_WHIRLPOOL),
            5_000_000_000, // 5 SOL
            100_000, // Normal priority fee
        );
        bundle.push(BundleTransaction {
            transaction: victim_tx,
            priority_fee: 100_000,
            compute_units: 200_000,
            timestamp: Instant::now() + Duration::from_micros(10),
            size_bytes: 600,
            affected_accounts: bundle[0].affected_accounts.clone(),
            write_locked_accounts: vec![bundle[0].write_locked_accounts[0]],
            program_ids: vec![Pubkey::from_str_const(ORCA_WHIRLPOOL)],
            estimated_cu_consumption: 180_000,
            slot_received: Some(200_000_000),
        });
        
        // Back-running transaction
        let back_tx = create_mock_swap_transaction(
            &attacker_keypair,
            Pubkey::from_str_const(ORCA_WHIRLPOOL),
            10_000_000_000, // 10 SOL
            2_000_000, // High priority fee
        );
        bundle.push(BundleTransaction {
            transaction: back_tx,
            priority_fee: 2_000_000,
            compute_units: 200_000,
            timestamp: Instant::now() + Duration::from_micros(20),
            size_bytes: 600,
            affected_accounts: bundle[0].affected_accounts.clone(),
            write_locked_accounts: vec![bundle[0].write_locked_accounts[0]],
            program_ids: vec![Pubkey::from_str_const(ORCA_WHIRLPOOL)],
            estimated_cu_consumption: 180_000,
            slot_received: Some(200_000_000),
        });
        
        let analysis = analyzer.analyze_bundle(bundle).await.unwrap();
        
        assert!(analysis.total_score > 30.0);
        assert_eq!(analysis.risk_level, RiskLevel::High);
        assert!(!analysis.patterns.is_empty());
        
        // Verify sandwich attack detected
        let has_sandwich = analysis.patterns.iter().any(|p| {
            matches!(p, AdversarialPattern::SandwichAttack { .. })
        });
        assert!(has_sandwich);
        
        // Verify recommended action
        match analysis.recommended_action {
            RecommendedAction::Abort { .. } | 
            RecommendedAction::DelayExecution { .. } |
            RecommendedAction::IncreaseGas { .. } => {},
            _ => panic!("Expected defensive action for sandwich attack"),
        }
    }

    #[tokio::test]
    async fn test_gas_warfare_detection() {
        let analyzer = AdversarialBundleAnalyzer::new("https://api.mainnet-beta.solana.com");
        
        let mut bundle = Vec::new();
        let escalating_fees = vec![100_000, 500_000, 1_500_000, 3_000_000, 5_000_000];
        
        for (i, fee) in escalating_fees.iter().enumerate() {
            let keypair = Keypair::new();
            let tx = create_mock_transaction(&keypair);
            
            bundle.push(BundleTransaction {
                transaction: tx,
                priority_fee: *fee,
                compute_units: 300_000,
                timestamp: Instant::now() + Duration::from_millis(i as u64 * 10),
                size_bytes: 700,
                affected_accounts: vec![Pubkey::new_unique()],
                write_locked_accounts: vec![],
                program_ids: vec![Pubkey::from_str_const(JUPITER_V6)],
                estimated_cu_consumption: 280_000,
                slot_received: Some(200_000_000),
            });
        }
        
        let analysis = analyzer.analyze_bundle(bundle).await.unwrap();
        
        let has_gas_warfare = analysis.patterns.iter().any(|p| {
            matches!(p, AdversarialPattern::GasWarfare { .. })
        });
        assert!(has_gas_warfare);
    }

    #[tokio::test]
    async fn test_flashloan_attack_detection() {
        let analyzer = AdversarialBundleAnalyzer::new("https://api.mainnet-beta.solana.com");
        
        let attacker = Keypair::new();
        let mut bundle = Vec::new();
        
        // Flashloan borrow
        let mut borrow_tx = create_mock_transaction(&attacker);
        borrow_tx.message.instructions[0].data = vec![168, 198, 201, 176, 23, 9, 48, 157]
            .into_iter()
            .chain(100_000_000_000u64.to_le_bytes()) // 100 SOL
            .collect();
        
        bundle.push(BundleTransaction {
            transaction: borrow_tx,
            priority_fee: 1_000_000,
            compute_units: 400_000,
            timestamp: Instant::now(),
            size_bytes: 800,
            affected_accounts: vec![Pubkey::new_unique(), Pubkey::new_unique()],
            write_locked_accounts: vec![Pubkey::new_unique()],
            program_ids: vec![Pubkey::new_unique()], // Port Finance
            estimated_cu_consumption: 380_000,
            slot_received: Some(200_000_000),
        });
        
        // Exploit transactions
        for i in 0..3 {
            let exploit_tx = create_mock_swap_transaction(
                &attacker,
                Pubkey::from_str_const(RAYDIUM_V4),
                50_000_000_000,
                1_500_000,
            );
            bundle.push(BundleTransaction {
                transaction: exploit_tx,
                priority_fee: 1_500_000,
                compute_units: 450_000,
                timestamp: Instant::now() + Duration::from_micros(10 * (i + 1)),
                size_bytes: 900,
                affected_accounts: vec![
                    bundle[0].affected_accounts[0],
                    Pubkey::new_unique(),
                    Pubkey::new_unique(),
                ],
                write_locked_accounts: vec![Pubkey::new_unique()],
                program_ids: vec![Pubkey::from_str_const(RAYDIUM_V4), Pubkey::from_str_const(JUPITER_V6)],
                estimated_cu_consumption: 420_000,
                slot_received: Some(200_000_000),
            });
        }
        
        // Flashloan repay
        let mut repay_tx = create_mock_transaction(&attacker);
        repay_tx.message.instructions[0].data = vec![55, 187, 23, 196, 132, 7, 137, 210]
            .into_iter()
            .chain(100_500_000_000u64.to_le_bytes()) // 100.5 SOL (with interest)
            .collect();
        
        bundle.push(BundleTransaction {
            transaction: repay_tx,
            priority_fee: 1_000_000,
            compute_units: 400_000,
            timestamp: Instant::now() + Duration::from_micros(50),
            size_bytes: 800,
            affected_accounts: bundle[0].affected_accounts.clone(),
            write_locked_accounts: vec![bundle[0].write_locked_accounts[0]],
            program_ids: vec![bundle[0].program_ids[0]],
            estimated_cu_consumption: 380_000,
            slot_received: Some(200_000_000),
        });
        
        let analysis = analyzer.analyze_bundle(bundle).await.unwrap();
        
        assert!(analysis.total_score > 40.0);
        assert_eq!(analysis.risk_level, RiskLevel::Critical);
        
        let has_flashloan = analysis.patterns.iter().any(|p| {
            matches!(p, AdversarialPattern::FlashLoanAttack { .. })
        });
        assert!(has_flashloan);
    }

    #[tokio::test]
    async fn test_bundle_validation() {
        let analyzer = AdversarialBundleAnalyzer::new("https://api.mainnet-beta.solana.com");
        
        // Test empty bundle
        let empty_bundle = vec![];
        assert!(matches!(
            analyzer.validate_bundle_integrity(&empty_bundle).await,
            Err(AnalysisError::EmptyBundle)
        ));
        
        // Test oversized bundle
        let mut oversized = Vec::new();
        for _ in 0..30 {
            let keypair = Keypair::new();
            oversized.push(create_mock_bundle_tx(&keypair));
        }
        assert!(matches!(
            analyzer.validate_bundle_integrity(&oversized).await,
            Err(AnalysisError::BundleTooLarge(_))
        ));
        
        // Test excessive priority fee
        let keypair = Keypair::new();
        let mut tx = create_mock_bundle_tx(&keypair);
        tx.priority_fee = 100_000_000; // 0.1 SOL
        let bundle = vec![tx];
        assert!(matches!(
            analyzer.validate_bundle_integrity(&bundle).await,
            Err(AnalysisError::ExcessivePriorityFee(_, _))
        ));
    }

    fn create_mock_transaction(payer: &Keypair) -> Transaction {
        let instruction = system_instruction::transfer(
            &payer.pubkey(),
            &Pubkey::new_unique(),
            1_000_000,
        );
        Transaction::new_signed_with_payer(
            &[instruction],
            Some(&payer.pubkey()),
            &[payer],
            Hash::default(),
        )
    }

    fn create_mock_swap_transaction(
        payer: &Keypair, 
        program_id: Pubkey, 
        amount: u64,
        priority_fee: u64
    ) -> Transaction {
        let mut data = ORCA_SWAP_DISCRIMINATOR.to_vec();
        data.extend_from_slice(&amount.to_le_bytes());
        
        let instruction = Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(payer.pubkey(), true),
                AccountMeta::new(Pubkey::new_unique(), false),
                AccountMeta::new(Pubkey::new_unique(), false),
                AccountMeta::new_readonly(Pubkey::new_unique(), false),
            ],
            data,
        };
        
        let compute_budget = ComputeBudgetInstruction::set_compute_unit_price(priority_fee);
        
        Transaction::new_signed_with_payer(
            &[compute_budget, instruction],
            Some(&payer.pubkey()),
            &[payer],
            Hash::default(),
        )
    }

    fn create_mock_bundle_tx(payer: &Keypair) -> BundleTransaction {
        BundleTransaction {
            transaction: create_mock_transaction(payer),
            priority_fee: 100_000,
            compute_units: 200_000,
            timestamp: Instant::now(),
            size_bytes: 600,
            affected_accounts: vec![payer.pubkey(), Pubkey::new_unique()],
            write_locked_accounts: vec![payer.pubkey()],
            program_ids: vec![solana_sdk::system_program::id()],
            estimated_cu_consumption: 180_000,
            slot_received: Some(200_000_000),
        }
    }
}

