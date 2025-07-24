use solana_sdk::{
    account::Account,
    clock::Clock,
    instruction::CompiledInstruction,
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
};
use solana_program::{
    program_pack::Pack,
    sysvar,
};
use spl_token::state::Mint;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use dashmap::DashMap;
use ahash::AHasher;
use std::hash::{Hash, Hasher};

const MAX_HISTORY_SIZE: usize = 10000;
const DECAY_FACTOR: f64 = 0.95;
const BASE_TOXICITY_THRESHOLD: f64 = 0.7;
const ACCOUNT_CONTENTION_WEIGHT: f64 = 0.25;
const FAILURE_RATE_WEIGHT: f64 = 0.35;
const COMPLEXITY_WEIGHT: f64 = 0.2;
const COMPETITION_WEIGHT: f64 = 0.2;
const TIME_DECAY_SECONDS: u64 = 300;
const MIN_SAMPLE_SIZE: usize = 5;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const HIGH_FAILURE_THRESHOLD: f64 = 0.6;
const CRITICAL_ACCOUNTS_PENALTY: f64 = 0.15;

#[derive(Debug, Clone)]
pub struct FlowMetrics {
    pub success_count: u64,
    pub failure_count: u64,
    pub last_seen: Instant,
    pub avg_compute_units: u32,
    pub avg_latency_ms: u64,
    pub competition_score: f64,
    pub slippage_events: u64,
}

impl Default for FlowMetrics {
    fn default() -> Self {
        Self {
            success_count: 0,
            failure_count: 0,
            last_seen: Instant::now(),
            avg_compute_units: 0,
            avg_latency_ms: 0,
            competition_score: 0.0,
            slippage_events: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AccountMetrics {
    pub access_count: u64,
    pub contention_score: f64,
    pub last_update: Instant,
    pub write_lock_frequency: f64,
    pub concurrent_accessors: Vec<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct ToxicityScore {
    pub total_score: f64,
    pub failure_component: f64,
    pub contention_component: f64,
    pub complexity_component: f64,
    pub competition_component: f64,
    pub recommendation: FlowRecommendation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FlowRecommendation {
    Execute,
    ExecuteWithCaution,
    Skip,
    Blacklist,
}

pub struct FlowToxicityScorer {
    flow_history: Arc<DashMap<u64, FlowMetrics>>,
    account_metrics: Arc<DashMap<Pubkey, AccountMetrics>>,
    program_complexity: Arc<DashMap<Pubkey, f64>>,
    recent_failures: Arc<RwLock<VecDeque<(u64, Instant)>>>,
    critical_accounts: Arc<RwLock<HashMap<Pubkey, f64>>>,
    competition_tracker: Arc<DashMap<Pubkey, Vec<Instant>>>,
}

impl FlowToxicityScorer {
    pub fn new() -> Self {
        let mut critical_accounts = HashMap::new();
        // Critical Solana programs and accounts
        critical_accounts.insert(spl_token::ID, 0.1);
        critical_accounts.insert(spl_associated_token_account::ID, 0.08);
        critical_accounts.insert(sysvar::clock::ID, 0.05);
        critical_accounts.insert(sysvar::rent::ID, 0.05);
        
        // Known DEX programs
        critical_accounts.insert(
            Pubkey::from_str("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP").unwrap(), // Orca
            0.12
        );
        critical_accounts.insert(
            Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(), // Raydium V4
            0.12
        );
        critical_accounts.insert(
            Pubkey::from_str("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK").unwrap(), // Raydium CLMM
            0.12
        );

        Self {
            flow_history: Arc::new(DashMap::with_capacity(MAX_HISTORY_SIZE)),
            account_metrics: Arc::new(DashMap::with_capacity(10000)),
            program_complexity: Arc::new(DashMap::new()),
            recent_failures: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            critical_accounts: Arc::new(RwLock::new(critical_accounts)),
            competition_tracker: Arc::new(DashMap::new()),
        }
    }

    pub fn score_transaction(&self, tx: &Transaction, accounts: &[Account]) -> ToxicityScore {
        let flow_hash = self.compute_flow_hash(tx);
        
        let failure_component = self.calculate_failure_rate(flow_hash);
        let contention_component = self.calculate_account_contention(tx, accounts);
        let complexity_component = self.calculate_complexity(tx);
        let competition_component = self.calculate_competition_score(tx);
        
        let total_score = (failure_component * FAILURE_RATE_WEIGHT)
            + (contention_component * ACCOUNT_CONTENTION_WEIGHT)
            + (complexity_component * COMPLEXITY_WEIGHT)
            + (competition_component * COMPETITION_WEIGHT);
        
        let recommendation = self.determine_recommendation(total_score, &tx);
        
        ToxicityScore {
            total_score,
            failure_component,
            contention_component,
            complexity_component,
            competition_component,
            recommendation,
        }
    }

    fn compute_flow_hash(&self, tx: &Transaction) -> u64 {
        let mut hasher = AHasher::default();
        
        for instruction in &tx.message.instructions {
            instruction.program_id_index.hash(&mut hasher);
            instruction.data.len().hash(&mut hasher);
            
            for &account_index in &instruction.accounts {
                if let Some(pubkey) = tx.message.account_keys.get(account_index as usize) {
                    pubkey.hash(&mut hasher);
                }
            }
        }
        
        hasher.finish()
    }

    fn calculate_failure_rate(&self, flow_hash: u64) -> f64 {
        if let Some(metrics) = self.flow_history.get(&flow_hash) {
            let total = metrics.success_count + metrics.failure_count;
            if total < MIN_SAMPLE_SIZE as u64 {
                return 0.5; // Neutral score for insufficient data
            }
            
            let raw_failure_rate = metrics.failure_count as f64 / total as f64;
            let time_decay = self.apply_time_decay(metrics.last_seen);
            
            raw_failure_rate * time_decay
        } else {
            0.5 // Neutral score for new flows
        }
    }

    fn calculate_account_contention(&self, tx: &Transaction, accounts: &[Account]) -> f64 {
        let mut total_contention = 0.0;
        let mut write_accounts = Vec::new();
        
        for (i, instruction) in tx.message.instructions.iter().enumerate() {
            for (j, &account_index) in instruction.accounts.iter().enumerate() {
                if let Some(pubkey) = tx.message.account_keys.get(account_index as usize) {
                    let is_writable = tx.message.is_writable(account_index as usize, true);
                    
                    if is_writable {
                        write_accounts.push(*pubkey);
                    }
                    
                    if let Some(metrics) = self.account_metrics.get(pubkey) {
                        let mut contention = metrics.contention_score;
                        
                        if is_writable {
                            contention *= 1.5;
                        }
                        
                        if self.is_critical_account(pubkey) {
                            contention += CRITICAL_ACCOUNTS_PENALTY;
                        }
                        
                        total_contention += contention;
                    }
                }
            }
        }
        
        // Check for multiple writes to same account
        write_accounts.sort();
        let mut prev = None;
        for account in &write_accounts {
            if Some(account) == prev {
                total_contention += 0.2; // Penalty for multiple writes
            }
            prev = Some(account);
        }
        
        (total_contention / tx.message.account_keys.len() as f64).min(1.0)
    }

    fn calculate_complexity(&self, tx: &Transaction) -> f64 {
        let mut complexity = 0.0;
        
        // Instruction count complexity
        let instruction_count = tx.message.instructions.len();
        complexity += (instruction_count as f64 / 10.0).min(0.3);
        
        // Cross-program invocations
        let unique_programs = tx.message.instructions
            .iter()
            .map(|ix| ix.program_id_index)
            .collect::<std::collections::HashSet<_>>()
            .len();
        complexity += (unique_programs as f64 / 5.0).min(0.3);
        
        // Data size complexity
        let total_data_size: usize = tx.message.instructions
            .iter()
            .map(|ix| ix.data.len())
            .sum();
        complexity += (total_data_size as f64 / 1000.0).min(0.2);
        
        // Account complexity
        let account_count = tx.message.account_keys.len();
        complexity += (account_count as f64 / 20.0).min(0.2);
        
        complexity.min(1.0)
    }

    fn calculate_competition_score(&self, tx: &Transaction) -> f64 {
        let mut competition_score = 0.0;
        let now = Instant::now();
        
        for pubkey in &tx.message.account_keys {
            if let Some(mut tracker) = self.competition_tracker.get_mut(pubkey) {
                // Remove old entries
                tracker.retain(|&t| now.duration_since(t).as_secs() < TIME_DECAY_SECONDS);
                
                let recent_access_count = tracker.len();
                if recent_access_count > 0 {
                    competition_score += (recent_access_count as f64 / 10.0).min(1.0);
                }
            }
        }
        
        (competition_score / tx.message.account_keys.len() as f64).min(1.0)
    }

    fn determine_recommendation(&self, score: f64, tx: &Transaction) -> FlowRecommendation {
        // Check recent failure rate
        let recent_failure_rate = self.get_recent_failure_rate();
        
        if score > 0.9 || recent_failure_rate > HIGH_FAILURE_THRESHOLD {
            FlowRecommendation::Blacklist
        } else if score > BASE_TOXICITY_THRESHOLD {
            FlowRecommendation::Skip
        } else if score > 0.5 {
            FlowRecommendation::ExecuteWithCaution
        } else {
            FlowRecommendation::Execute
        }
    }

    pub fn update_flow_result(
        &self,
        tx: &Transaction,
        success: bool,
        compute_units: u32,
        latency_ms: u64,
    ) {
        let flow_hash = self.compute_flow_hash(tx);
        let now = Instant::now();
        
        self.flow_history.entry(flow_hash)
            .and_modify(|metrics| {
                if success {
                    metrics.success_count += 1;
                } else {
                    metrics.failure_count += 1;
                    if let Ok(mut failures) = self.recent_failures.write() {
                        failures.push_back((flow_hash, now));
                        if failures.len() > 1000 {
                            failures.pop_front();
                        }
                    }
                }
                
                metrics.last_seen = now;
                metrics.avg_compute_units = 
                    (metrics.avg_compute_units + compute_units) / 2;
                metrics.avg_latency_ms = 
                    (metrics.avg_latency_ms + latency_ms) / 2;
            })
            .or_insert_with(|| {
                let mut metrics = FlowMetrics::default();
                if success {
                    metrics.success_count = 1;
                } else {
                    metrics.failure_count = 1;
                }
                metrics.avg_compute_units = compute_units;
                metrics.avg_latency_ms = latency_ms;
                metrics
            });
        
        // Update account metrics
        for pubkey in &tx.message.account_keys {
            self.update_account_metrics(pubkey);
        }
        
        // Cleanup old entries
        if self.flow_history.len() > MAX_HISTORY_SIZE {
            self.cleanup_old_entries();
        }
    }

    fn update_account_metrics(&self, pubkey: &Pubkey) {
        let now = Instant::now();
        
        self.account_metrics.entry(*pubkey)
            .and_modify(|metrics| {
                metrics.access_count += 1;
                metrics.last_update = now;
                
                // Update contention score based on access frequency
                let time_since_last = now.duration_since(metrics.last_update).as_millis() as f64;
                if time_since_last < 100.0 {
                                    metrics.contention_score = (metrics.contention_score * 0.9 + 0.1).min(1.0);
                } else {
                    metrics.contention_score = (metrics.contention_score * DECAY_FACTOR).max(0.0);
                }
            })
            .or_insert_with(|| AccountMetrics {
                access_count: 1,
                contention_score: 0.1,
                last_update: now,
                write_lock_frequency: 0.0,
                concurrent_accessors: Vec::new(),
            });
        
        // Track competition
        self.competition_tracker.entry(*pubkey)
            .and_modify(|tracker| {
                tracker.push(now);
                if tracker.len() > 100 {
                    tracker.remove(0);
                }
            })
            .or_insert_with(|| vec![now]);
    }

    fn apply_time_decay(&self, last_seen: Instant) -> f64 {
        let elapsed = last_seen.elapsed().as_secs();
        let decay_periods = elapsed / TIME_DECAY_SECONDS;
        DECAY_FACTOR.powf(decay_periods as f64)
    }

    fn is_critical_account(&self, pubkey: &Pubkey) -> bool {
        if let Ok(critical) = self.critical_accounts.read() {
            critical.contains_key(pubkey)
        } else {
            false
        }
    }

    fn get_recent_failure_rate(&self) -> f64 {
        if let Ok(failures) = self.recent_failures.read() {
            let now = Instant::now();
            let recent_count = failures.iter()
                .filter(|(_, time)| now.duration_since(*time).as_secs() < 60)
                .count();
            
            (recent_count as f64 / 100.0).min(1.0)
        } else {
            0.0
        }
    }

    fn cleanup_old_entries(&self) {
        let now = Instant::now();
        let cutoff_duration = Duration::from_secs(TIME_DECAY_SECONDS * 2);
        
        // Clean flow history
        let old_flows: Vec<u64> = self.flow_history
            .iter()
            .filter(|entry| now.duration_since(entry.value().last_seen) > cutoff_duration)
            .map(|entry| *entry.key())
            .collect();
        
        for flow_hash in old_flows {
            self.flow_history.remove(&flow_hash);
        }
        
        // Clean account metrics
        let old_accounts: Vec<Pubkey> = self.account_metrics
            .iter()
            .filter(|entry| now.duration_since(entry.value().last_update) > cutoff_duration)
            .map(|entry| *entry.key())
            .collect();
        
        for pubkey in old_accounts {
            self.account_metrics.remove(&pubkey);
        }
    }

    pub fn get_flow_metrics(&self, tx: &Transaction) -> Option<FlowMetrics> {
        let flow_hash = self.compute_flow_hash(tx);
        self.flow_history.get(&flow_hash).map(|entry| entry.clone())
    }

    pub fn should_execute(&self, score: &ToxicityScore) -> bool {
        matches!(
            score.recommendation,
            FlowRecommendation::Execute | FlowRecommendation::ExecuteWithCaution
        )
    }

    pub fn get_recommended_priority_fee(&self, score: &ToxicityScore, base_fee: u64) -> u64 {
        match score.recommendation {
            FlowRecommendation::Execute => base_fee,
            FlowRecommendation::ExecuteWithCaution => (base_fee as f64 * 1.5) as u64,
            FlowRecommendation::Skip | FlowRecommendation::Blacklist => 0,
        }
    }

    pub fn analyze_bundle(&self, transactions: &[Transaction], accounts: &[Vec<Account>]) -> Vec<ToxicityScore> {
        let mut scores = Vec::with_capacity(transactions.len());
        let mut cumulative_toxicity = 0.0;
        
        for (i, tx) in transactions.iter().enumerate() {
            let mut score = self.score_transaction(tx, &accounts[i]);
            
            // Adjust score based on bundle position and cumulative toxicity
            if i > 0 {
                score.total_score = (score.total_score + cumulative_toxicity * 0.1).min(1.0);
            }
            
            cumulative_toxicity = (cumulative_toxicity + score.total_score) / 2.0;
            scores.push(score);
        }
        
        scores
    }

    pub fn update_slippage_event(&self, tx: &Transaction) {
        let flow_hash = self.compute_flow_hash(tx);
        
        self.flow_history.entry(flow_hash)
            .and_modify(|metrics| {
                metrics.slippage_events += 1;
            });
    }

    pub fn add_critical_account(&self, pubkey: Pubkey, weight: f64) {
        if let Ok(mut critical) = self.critical_accounts.write() {
            critical.insert(pubkey, weight.min(1.0).max(0.0));
        }
    }

    pub fn get_account_contention_score(&self, pubkey: &Pubkey) -> f64 {
        self.account_metrics
            .get(pubkey)
            .map(|metrics| metrics.contention_score)
            .unwrap_or(0.0)
    }

    pub fn predict_success_probability(&self, tx: &Transaction) -> f64 {
        let score = self.score_transaction(tx, &[]);
        
        // Sigmoid function to convert toxicity to success probability
        let exp_score = (-5.0 * (score.total_score - 0.5)).exp();
        exp_score / (1.0 + exp_score)
    }

    pub fn get_optimal_timing(&self, tx: &Transaction) -> Option<Duration> {
        let flow_hash = self.compute_flow_hash(tx);
        
        if let Some(metrics) = self.flow_history.get(&flow_hash) {
            if metrics.success_count + metrics.failure_count >= MIN_SAMPLE_SIZE as u64 {
                // Calculate optimal delay based on competition and contention
                let competition = self.calculate_competition_score(tx);
                let contention = self.calculate_account_contention(tx, &[]);
                
                let delay_ms = (competition * 50.0 + contention * 30.0) as u64;
                return Some(Duration::from_millis(delay_ms));
            }
        }
        
        None
    }

    pub fn estimate_compute_units(&self, tx: &Transaction) -> u32 {
        let flow_hash = self.compute_flow_hash(tx);
        
        if let Some(metrics) = self.flow_history.get(&flow_hash) {
            if metrics.avg_compute_units > 0 {
                return (metrics.avg_compute_units as f64 * 1.2) as u32;
            }
        }
        
        // Estimate based on transaction complexity
        let base_units = 5000;
        let per_instruction = 2000;
        let per_account = 500;
        
        let estimated = base_units 
            + (tx.message.instructions.len() as u32 * per_instruction)
            + (tx.message.account_keys.len() as u32 * per_account);
        
        estimated.min(MAX_COMPUTE_UNITS)
    }

    pub fn reset_account_metrics(&self, pubkey: &Pubkey) {
        self.account_metrics.remove(pubkey);
        self.competition_tracker.remove(pubkey);
    }

    pub fn get_health_metrics(&self) -> HealthMetrics {
        let total_flows = self.flow_history.len();
        let mut total_success = 0;
        let mut total_failure = 0;
        
        for entry in self.flow_history.iter() {
            total_success += entry.value().success_count;
            total_failure += entry.value().failure_count;
        }
        
        let success_rate = if total_success + total_failure > 0 {
            total_success as f64 / (total_success + total_failure) as f64
        } else {
            0.0
        };
        
        HealthMetrics {
            total_flows,
            success_rate,
            active_accounts: self.account_metrics.len(),
            recent_failure_rate: self.get_recent_failure_rate(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealthMetrics {
    pub total_flows: usize,
    pub success_rate: f64,
    pub active_accounts: usize,
    pub recent_failure_rate: f64,
}

impl Default for FlowToxicityScorer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        instruction::Instruction,
        message::Message,
        transaction::Transaction,
        signature::Keypair,
        signer::Signer,
        system_instruction,
    };

    #[test]
    fn test_toxicity_scorer_creation() {
        let scorer = FlowToxicityScorer::new();
        assert!(scorer.flow_history.is_empty());
        assert!(scorer.account_metrics.is_empty());
    }

    #[test]
    fn test_transaction_scoring() {
        let scorer = FlowToxicityScorer::new();
        let keypair = Keypair::new();
        
        let instruction = system_instruction::transfer(
            &keypair.pubkey(),
            &Pubkey::new_unique(),
            1000,
        );
        
        let message = Message::new(&[instruction], Some(&keypair.pubkey()));
        let transaction = Transaction::new_unsigned(message);
        
        let score = scorer.score_transaction(&transaction, &[]);
        assert!(score.total_score >= 0.0 && score.total_score <= 1.0);
        assert!(matches!(score.recommendation, FlowRecommendation::Execute | FlowRecommendation::ExecuteWithCaution));
    }

    #[test]
    fn test_flow_update() {
        let scorer = FlowToxicityScorer::new();
        let keypair = Keypair::new();
        
        let instruction = system_instruction::transfer(
            &keypair.pubkey(),
            &Pubkey::new_unique(),
            1000,
        );
        
        let message = Message::new(&[instruction], Some(&keypair.pubkey()));
        let transaction = Transaction::new_unsigned(message);
        
        scorer.update_flow_result(&transaction, true, 10000, 50);
        
        let metrics = scorer.get_flow_metrics(&transaction);
        assert!(metrics.is_some());
        assert_eq!(metrics.unwrap().success_count, 1);
    }
}


