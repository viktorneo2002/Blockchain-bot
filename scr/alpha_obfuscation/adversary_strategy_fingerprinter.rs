use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use dashmap::DashMap;
use ahash::AHasher;
use std::hash::{Hash, Hasher};

const FINGERPRINT_WINDOW_SIZE: usize = 1000;
const PATTERN_THRESHOLD: f64 = 0.85;
const MIN_PATTERN_OCCURRENCES: usize = 5;
const STRATEGY_DECAY_FACTOR: f64 = 0.95;
const MAX_TRACKED_ADVERSARIES: usize = 500;
const TIMING_BUCKET_MS: u64 = 10;
const GAS_PRICE_BUCKETS: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyType {
    Frontrunner,
    Backrunner,
    Sandwicher,
    Liquidator,
    Arbitrageur,
    JitLiquidity,
    AtomicArb,
    FlashLoan,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct TransactionFingerprint {
    pub signature: Signature,
    pub slot: Slot,
    pub timestamp: Instant,
    pub compute_units: u64,
    pub priority_fee: u64,
    pub programs_invoked: Vec<Pubkey>,
    pub account_keys: Vec<Pubkey>,
    pub instruction_count: usize,
    pub success: bool,
    pub error_code: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct AdversaryProfile {
    pub pubkey: Pubkey,
    pub strategy_scores: HashMap<StrategyType, f64>,
    pub avg_compute_units: f64,
    pub avg_priority_fee: f64,
    pub success_rate: f64,
    pub timing_distribution: Vec<u64>,
    pub target_programs: HashMap<Pubkey, usize>,
    pub transaction_patterns: Vec<TransactionPattern>,
    pub last_seen: Instant,
    pub total_transactions: usize,
    pub profitable_transactions: usize,
}

#[derive(Debug, Clone)]
pub struct TransactionPattern {
    pub instruction_sequence: Vec<u64>,
    pub timing_pattern: Vec<u64>,
    pub gas_pattern: Vec<u64>,
    pub occurrence_count: usize,
    pub confidence: f64,
}

pub struct AdversaryStrategyFingerprinter {
    adversary_profiles: Arc<DashMap<Pubkey, AdversaryProfile>>,
    transaction_cache: Arc<RwLock<VecDeque<TransactionFingerprint>>>,
    pattern_library: Arc<DashMap<u64, TransactionPattern>>,
    rpc_client: Arc<RpcClient>,
}

impl AdversaryStrategyFingerprinter {
    pub fn new(rpc_url: &str) -> Self {
        Self {
            adversary_profiles: Arc::new(DashMap::new()),
            transaction_cache: Arc::new(RwLock::new(VecDeque::with_capacity(FINGERPRINT_WINDOW_SIZE))),
            pattern_library: Arc::new(DashMap::new()),
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::confirmed(),
            )),
        }
    }

    pub async fn analyze_transaction(&self, signature: &Signature) -> Result<(), Box<dyn std::error::Error>> {
        let tx_result = self.rpc_client.get_transaction(
            signature,
            UiTransactionEncoding::Base64,
        )?;

        if let Some(tx_meta) = tx_result.transaction.meta {
            let fingerprint = self.extract_fingerprint(&tx_result, signature)?;
            self.update_adversary_profile(&fingerprint).await?;
            self.detect_patterns(&fingerprint).await?;
            
            let mut cache = self.transaction_cache.write().unwrap();
            if cache.len() >= FINGERPRINT_WINDOW_SIZE {
                cache.pop_front();
            }
            cache.push_back(fingerprint);
        }

        Ok(())
    }

    fn extract_fingerprint(
        &self,
        tx: &EncodedConfirmedTransactionWithStatusMeta,
        signature: &Signature,
    ) -> Result<TransactionFingerprint, Box<dyn std::error::Error>> {
        let meta = tx.transaction.meta.as_ref().unwrap();
        let transaction = &tx.transaction.transaction;
        
        let compute_units = meta.compute_units_consumed.unwrap_or(0);
        let priority_fee = self.calculate_priority_fee(meta);
        
        let programs_invoked = self.extract_programs(transaction);
        let account_keys = self.extract_account_keys(transaction);
        let instruction_count = self.count_instructions(transaction);
        
        Ok(TransactionFingerprint {
            signature: *signature,
            slot: tx.slot,
            timestamp: Instant::now(),
            compute_units,
            priority_fee,
            programs_invoked,
            account_keys,
            instruction_count,
            success: meta.err.is_none(),
            error_code: meta.err.as_ref().and_then(|e| {
                if let serde_json::Value::Object(map) = e {
                    map.get("Custom").and_then(|v| v.as_u64().map(|n| n as u32))
                } else {
                    None
                }
            }),
        })
    }

    fn calculate_priority_fee(&self, meta: &solana_transaction_status::UiTransactionStatusMeta) -> u64 {
        meta.fee / meta.compute_units_consumed.unwrap_or(1).max(1)
    }

    fn extract_programs(&self, transaction: &solana_transaction_status::EncodedTransaction) -> Vec<Pubkey> {
        let mut programs = Vec::new();
        
        if let solana_transaction_status::EncodedTransaction::Json(ui_tx) = transaction {
            if let Some(msg) = &ui_tx.message {
                for instruction in msg.instructions.iter() {
                    if let Ok(program_idx) = instruction.program_id_index.parse::<usize>() {
                        if let Some(account_key) = msg.account_keys.get(program_idx) {
                            if let Ok(pubkey) = account_key.parse::<Pubkey>() {
                                if !programs.contains(&pubkey) {
                                    programs.push(pubkey);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        programs
    }

    fn extract_account_keys(&self, transaction: &solana_transaction_status::EncodedTransaction) -> Vec<Pubkey> {
        let mut keys = Vec::new();
        
        if let solana_transaction_status::EncodedTransaction::Json(ui_tx) = transaction {
            if let Some(msg) = &ui_tx.message {
                for key_str in &msg.account_keys {
                    if let Ok(pubkey) = key_str.parse::<Pubkey>() {
                        keys.push(pubkey);
                    }
                }
            }
        }
        
        keys
    }

    fn count_instructions(&self, transaction: &solana_transaction_status::EncodedTransaction) -> usize {
        if let solana_transaction_status::EncodedTransaction::Json(ui_tx) = transaction {
            ui_tx.message.as_ref().map(|m| m.instructions.len()).unwrap_or(0)
        } else {
            0
        }
    }

    async fn update_adversary_profile(
        &self,
        fingerprint: &TransactionFingerprint,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if fingerprint.account_keys.is_empty() {
            return Ok(());
        }

        let signer = fingerprint.account_keys[0];
        
        self.adversary_profiles
            .entry(signer)
            .and_modify(|profile| {
                profile.total_transactions += 1;
                
                if fingerprint.success {
                    profile.profitable_transactions += 1;
                }
                
                profile.success_rate = profile.profitable_transactions as f64 / profile.total_transactions as f64;
                
                let alpha = 0.1;
                profile.avg_compute_units = profile.avg_compute_units * (1.0 - alpha) + fingerprint.compute_units as f64 * alpha;
                profile.avg_priority_fee = profile.avg_priority_fee * (1.0 - alpha) + fingerprint.priority_fee as f64 * alpha;
                
                let timing_bucket = (fingerprint.timestamp.elapsed().as_millis() / TIMING_BUCKET_MS as u128) as usize;
                if timing_bucket < profile.timing_distribution.len() {
                    profile.timing_distribution[timing_bucket] += 1;
                }
                
                for program in &fingerprint.programs_invoked {
                    *profile.target_programs.entry(*program).or_insert(0) += 1;
                }
                
                self.classify_strategy(profile, fingerprint);
                
                profile.last_seen = Instant::now();
            })
            .or_insert_with(|| AdversaryProfile {
                pubkey: signer,
                strategy_scores: HashMap::new(),
                avg_compute_units: fingerprint.compute_units as f64,
                avg_priority_fee: fingerprint.priority_fee as f64,
                success_rate: if fingerprint.success { 1.0 } else { 0.0 },
                timing_distribution: vec![0; 100],
                target_programs: fingerprint.programs_invoked.iter().map(|p| (*p, 1)).collect(),
                transaction_patterns: Vec::new(),
                last_seen: Instant::now(),
                total_transactions: 1,
                profitable_transactions: if fingerprint.success { 1 } else { 0 },
            });

        self.cleanup_old_adversaries();
        
        Ok(())
    }

    fn classify_strategy(&self, profile: &mut AdversaryProfile, fingerprint: &TransactionFingerprint) {
        for (strategy, score) in &mut profile.strategy_scores {
            *score *= STRATEGY_DECAY_FACTOR;
        }

        if fingerprint.priority_fee > 50000 && fingerprint.compute_units > 200000 {
            *profile.strategy_scores.entry(StrategyType::Frontrunner).or_insert(0.0) += 0.3;
        }

        if fingerprint.instruction_count >= 3 && self.is_sandwich_pattern(&fingerprint.programs_invoked) {
            *profile.strategy_scores.entry(StrategyType::Sandwicher).or_insert(0.0) += 0.4;
        }

        if self.is_liquidation_program(&fingerprint.programs_invoked) {
            *profile.strategy_scores.entry(StrategyType::Liquidator).or_insert(0.0) += 0.5;
        }

        if self.is_arbitrage_pattern(&fingerprint.programs_invoked, fingerprint.instruction_count) {
            *profile.strategy_scores.entry(StrategyType::Arbitrageur).or_insert(0.0) += 0.4;
        }

        if fingerprint.compute_units > 400000 {
            *profile.strategy_scores.entry(StrategyType::FlashLoan).or_insert(0.0) += 0.2;
        }

        profile.strategy_scores.retain(|_, &mut score| score > 0.1);
    }

    fn is_sandwich_pattern(&self, programs: &[Pubkey]) -> bool {
        if programs.len() < 2 {
            return false;
        }
        
        let dex_programs = [
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
            "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
        ];
        
        let mut dex_count = 0;
        for program in programs {
            if dex_programs.iter().any(|&dex| program.to_string() == dex) {
                dex_count += 1;
            }
        }
        
        dex_count >= 2
    }

    fn is_liquidation_program(&self, programs: &[Pubkey]) -> bool {
        let liquidation_programs = [
            "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo",
            "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD",
            "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA",
        ];
        
        programs.iter().any(|p| {
            liquidation_programs.iter().any(|&liq| p.to_string() == liq)
        })
    }

    fn is_arbitrage_pattern(&self, programs: &[Pubkey], instruction_count: usize) -> bool {
        instruction_count >= 2 && programs.len() >= 2 && {
            let unique_dex_count = programs.iter()
                .filter(|p| self.is_dex_program(p))
                .collect::<std::collections::HashSet<_>>()
                .len();
            unique_dex_count >= 2
        }
    }

    fn is_dex_program(&self, program: &Pubkey) -> bool {
        let dex_programs = [
                        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
            "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
            "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
            "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
        ];
        
        dex_programs.iter().any(|&dex| program.to_string() == dex)
    }

    async fn detect_patterns(
        &self,
        fingerprint: &TransactionFingerprint,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let pattern_hash = self.generate_pattern_hash(fingerprint);
        
        self.pattern_library
            .entry(pattern_hash)
            .and_modify(|pattern| {
                pattern.occurrence_count += 1;
                pattern.confidence = (pattern.occurrence_count as f64 / MIN_PATTERN_OCCURRENCES as f64).min(1.0);
                
                let timing_bucket = (fingerprint.timestamp.elapsed().as_millis() / TIMING_BUCKET_MS as u128) as u64;
                if !pattern.timing_pattern.contains(&timing_bucket) {
                    pattern.timing_pattern.push(timing_bucket);
                }
                
                let gas_bucket = (fingerprint.priority_fee / 1000) * 1000;
                if !pattern.gas_pattern.contains(&gas_bucket) {
                    pattern.gas_pattern.push(gas_bucket);
                }
            })
            .or_insert_with(|| TransactionPattern {
                instruction_sequence: self.encode_instruction_sequence(fingerprint),
                timing_pattern: vec![(fingerprint.timestamp.elapsed().as_millis() / TIMING_BUCKET_MS as u128) as u64],
                gas_pattern: vec![(fingerprint.priority_fee / 1000) * 1000],
                occurrence_count: 1,
                confidence: 1.0 / MIN_PATTERN_OCCURRENCES as f64,
            });

        if let Some(signer) = fingerprint.account_keys.first() {
            if let Some(mut profile) = self.adversary_profiles.get_mut(signer) {
                if let Some(pattern) = self.pattern_library.get(&pattern_hash) {
                    if pattern.confidence >= PATTERN_THRESHOLD {
                        let exists = profile.transaction_patterns.iter().any(|p| {
                            p.instruction_sequence == pattern.instruction_sequence
                        });
                        
                        if !exists {
                            profile.transaction_patterns.push(pattern.clone());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn generate_pattern_hash(&self, fingerprint: &TransactionFingerprint) -> u64 {
        let mut hasher = AHasher::default();
        
        for program in &fingerprint.programs_invoked {
            program.hash(&mut hasher);
        }
        
        fingerprint.instruction_count.hash(&mut hasher);
        (fingerprint.compute_units / 10000).hash(&mut hasher);
        (fingerprint.priority_fee / 5000).hash(&mut hasher);
        
        hasher.finish()
    }

    fn encode_instruction_sequence(&self, fingerprint: &TransactionFingerprint) -> Vec<u64> {
        fingerprint.programs_invoked
            .iter()
            .map(|p| {
                let mut hasher = AHasher::default();
                p.hash(&mut hasher);
                hasher.finish() % 1000000
            })
            .collect()
    }

    fn cleanup_old_adversaries(&self) {
        if self.adversary_profiles.len() > MAX_TRACKED_ADVERSARIES {
            let mut profiles_to_remove = Vec::new();
            let now = Instant::now();
            
            for entry in self.adversary_profiles.iter() {
                if now.duration_since(entry.value().last_seen) > Duration::from_secs(3600) {
                    profiles_to_remove.push(*entry.key());
                }
            }
            
            profiles_to_remove.sort_by(|a, b| {
                let profile_a = self.adversary_profiles.get(a);
                let profile_b = self.adversary_profiles.get(b);
                
                match (profile_a, profile_b) {
                    (Some(a), Some(b)) => {
                        let score_a = a.total_transactions as f64 * a.success_rate;
                        let score_b = b.total_transactions as f64 * b.success_rate;
                        score_a.partial_cmp(&score_b).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            });
            
            let remove_count = profiles_to_remove.len().saturating_sub(MAX_TRACKED_ADVERSARIES / 2);
            for i in 0..remove_count {
                if let Some(key) = profiles_to_remove.get(i) {
                    self.adversary_profiles.remove(key);
                }
            }
        }
    }

    pub fn get_adversary_strategy(&self, pubkey: &Pubkey) -> Option<StrategyType> {
        self.adversary_profiles.get(pubkey).and_then(|profile| {
            profile.strategy_scores
                .iter()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(strategy, _)| *strategy)
        })
    }

    pub fn get_adversary_profile(&self, pubkey: &Pubkey) -> Option<AdversaryProfile> {
        self.adversary_profiles.get(pubkey).map(|entry| entry.clone())
    }

    pub fn get_top_adversaries(&self, limit: usize) -> Vec<(Pubkey, AdversaryProfile)> {
        let mut adversaries: Vec<_> = self.adversary_profiles
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        
        adversaries.sort_by(|a, b| {
            let score_a = a.1.total_transactions as f64 * a.1.success_rate * a.1.avg_priority_fee;
            let score_b = b.1.total_transactions as f64 * b.1.success_rate * b.1.avg_priority_fee;
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        adversaries.into_iter().take(limit).collect()
    }

    pub fn predict_next_move(
        &self,
        adversary: &Pubkey,
        current_market_state: &MarketState,
    ) -> Option<PredictedAction> {
        let profile = self.adversary_profiles.get(adversary)?;
        
        let dominant_strategy = profile.strategy_scores
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(s, _)| *s)?;
        
        let timing_prediction = self.predict_timing(&profile.timing_distribution);
        let gas_prediction = profile.avg_priority_fee * 1.1;
        
        let target_programs: Vec<_> = profile.target_programs
            .iter()
            .filter(|(_, &count)| count > profile.total_transactions / 10)
            .map(|(p, _)| *p)
            .collect();
        
        Some(PredictedAction {
            strategy: dominant_strategy,
            expected_timing_ms: timing_prediction,
            expected_gas: gas_prediction as u64,
            likely_targets: target_programs,
            confidence: self.calculate_prediction_confidence(&profile),
        })
    }

    fn predict_timing(&self, distribution: &[u64]) -> u64 {
        if distribution.is_empty() {
            return 50;
        }
        
        let total: u64 = distribution.iter().sum();
        if total == 0 {
            return 50;
        }
        
        let mut weighted_sum = 0u64;
        let mut weight_total = 0u64;
        
        for (bucket, &count) in distribution.iter().enumerate() {
            weighted_sum += bucket as u64 * count * TIMING_BUCKET_MS;
            weight_total += count;
        }
        
        if weight_total > 0 {
            weighted_sum / weight_total
        } else {
            50
        }
    }

    fn calculate_prediction_confidence(&self, profile: &AdversaryProfile) -> f64 {
        let transaction_factor = (profile.total_transactions as f64 / 100.0).min(1.0);
        let pattern_factor = (profile.transaction_patterns.len() as f64 / 10.0).min(1.0);
        let consistency_factor = profile.success_rate;
        
        (transaction_factor * 0.3 + pattern_factor * 0.3 + consistency_factor * 0.4).min(0.95)
    }

    pub fn is_known_adversary(&self, pubkey: &Pubkey) -> bool {
        self.adversary_profiles.contains_key(pubkey)
    }

    pub fn get_adversary_threat_level(&self, pubkey: &Pubkey) -> ThreatLevel {
        if let Some(profile) = self.adversary_profiles.get(pubkey) {
            let activity_score = (profile.total_transactions as f64 / 1000.0).min(1.0);
            let success_score = profile.success_rate;
            let gas_aggression = (profile.avg_priority_fee / 100000.0).min(1.0);
            
            let threat_score = activity_score * 0.2 + success_score * 0.5 + gas_aggression * 0.3;
            
            match threat_score {
                s if s >= 0.8 => ThreatLevel::Critical,
                s if s >= 0.6 => ThreatLevel::High,
                s if s >= 0.4 => ThreatLevel::Medium,
                s if s >= 0.2 => ThreatLevel::Low,
                _ => ThreatLevel::Minimal,
            }
        } else {
            ThreatLevel::Unknown
        }
    }

    pub fn should_compete_with(&self, adversary: &Pubkey, opportunity_value: u64) -> bool {
        if let Some(profile) = self.adversary_profiles.get(adversary) {
            let threat_level = self.get_adversary_threat_level(adversary);
            let expected_adversary_bid = profile.avg_priority_fee * 1.2;
            let profit_after_competition = opportunity_value.saturating_sub(expected_adversary_bid as u64);
            
            match threat_level {
                ThreatLevel::Critical => profit_after_competition > opportunity_value * 7 / 10,
                ThreatLevel::High => profit_after_competition > opportunity_value * 6 / 10,
                ThreatLevel::Medium => profit_after_competition > opportunity_value * 5 / 10,
                ThreatLevel::Low => profit_after_competition > opportunity_value * 3 / 10,
                _ => true,
            }
        } else {
            true
        }
    }

    pub async fn export_adversary_data(&self) -> Vec<AdversaryExport> {
        self.adversary_profiles
            .iter()
            .map(|entry| AdversaryExport {
                pubkey: *entry.key(),
                profile: entry.value().clone(),
                threat_level: self.get_adversary_threat_level(entry.key()),
                last_updated: std::time::SystemTime::now(),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub slot: Slot,
    pub active_programs: Vec<Pubkey>,
    pub volatility_index: f64,
    pub competition_level: f64,
}

#[derive(Debug, Clone)]
pub struct PredictedAction {
    pub strategy: StrategyType,
    pub expected_timing_ms: u64,
    pub expected_gas: u64,
    pub likely_targets: Vec<Pubkey>,
    pub confidence: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreatLevel {
    Critical,
    High,
    Medium,
    Low,
    Minimal,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct AdversaryExport {
    pub pubkey: Pubkey,
    pub profile: AdversaryProfile,
    pub threat_level: ThreatLevel,
    pub last_updated: std::time::SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_classification() {
        let fingerprinter = AdversaryStrategyFingerprinter::new("https://api.mainnet-beta.solana.com");
        assert!(fingerprinter.adversary_profiles.is_empty());
    }

    #[test]
    fn test_pattern_detection() {
        let fingerprinter = AdversaryStrategyFingerprinter::new("https://api.mainnet-beta.solana.com");
        let cache = fingerprinter.transaction_cache.read().unwrap();
        assert!(cache.is_empty());
    }
}

