use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use solana_sdk::signature::Signature;
use solana_sdk::pubkey::Pubkey;
use dashmap::DashMap;
use anyhow::{Result, anyhow};
use statrs::statistics::Statistics;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleStatus {
    Pending,
    Landed,
    Failed,
    Expired,
    Rejected,
    Simulated,
    DroppedByValidator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FailureReason {
    InsufficientTip,
    SimulationFailure,
    AccountLocked,
    SlotExpired,
    NetworkCongestion,
    ValidatorRejection,
    CompetitorOutbid,
    TipBelowMinimum,
    EpochBoundary,
    ValidatorRotation,
    BlockhashExpired,
    ComputeBudgetExceeded,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleSubmission {
    pub bundle_id: String,
    pub timestamp: u64,
    pub slot: u64,
    pub tip_lamports: u64,
    pub validator: Pubkey,
    pub status: BundleStatus,
    pub failure_reason: Option<FailureReason>,
    pub landing_slot: Option<u64>,
    pub gas_used: Option<u64>,
    pub priority_fee: u64,
    pub num_transactions: usize,
    pub competitor_tip: Option<u64>,
    pub submission_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindowStats {
    pub total_submissions: u64,
    pub successful_bundles: u64,
    pub failed_bundles: u64,
    pub expired_bundles: u64,
    pub rejected_bundles: u64,
    pub total_tips_paid: u64,
    pub average_tip: f64,
    pub success_rate: f64,
    pub average_landing_time_ms: f64,
    pub median_tip: u64,
    pub p95_tip: u64,
    pub min_successful_tip: u64,
    pub max_successful_tip: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorStats {
    pub validator: Pubkey,
    pub total_submissions: u64,
    pub successful_bundles: u64,
    pub success_rate: f64,
    pub average_tip: f64,
    pub last_submission: u64,
    pub average_landing_time_ms: f64,
    pub rejection_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureAnalysis {
    pub reason: FailureReason,
    pub count: u64,
    pub percentage: f64,
    pub average_tip_on_failure: f64,
    pub time_window_hours: u64,
}

// Production-grade constants based on mainnet research
const SOLANA_SLOT_TIME_MS: u64 = 420; // Updated from 400ms to actual Solana timing
const JITO_MIN_TIP_LAMPORTS: u64 = 1000; // Jito enforced minimum
const MAX_HISTORY_ENTRIES: usize = 50_000; // Reduced for memory efficiency
const OUTLIER_THRESHOLD_SIGMA: f64 = 3.0; // Statistical outlier detection
const MIN_STATISTICAL_SAMPLES: usize = 30; // Minimum for statistical significance
const VALIDATOR_ROTATION_SLOTS: u64 = 4; // Solana validator rotation pattern
const NETWORK_CONGESTION_LOOKBACK_SLOTS: u64 = 50; // Real-time congestion detection

pub struct BundleSuccessRateCalculator {
    submissions: Arc<DashMap<String, BundleSubmission>>,
    time_series_data: Arc<RwLock<VecDeque<BundleSubmission>>>,
    validator_stats: Arc<DashMap<Pubkey, ValidatorStats>>,
    failure_analysis: Arc<DashMap<FailureReason, FailureAnalysis>>,
    // Indexed lookups for performance
    slot_index: Arc<RwLock<BTreeMap<u64, Vec<String>>>>,
    validator_index: Arc<RwLock<BTreeMap<Pubkey, Vec<String>>>>,
    // Performance metrics
    total_submissions: Arc<AtomicU64>,
    successful_submissions: Arc<AtomicU64>,
    // Configuration
    max_history_duration: Duration,
    update_interval: Duration,
    last_cleanup: Arc<RwLock<Instant>>,
    // Statistical tracking
    tip_statistics: Arc<RwLock<TipStatistics>>,
}

#[derive(Debug, Clone, Default)]
struct TipStatistics {
    recent_tips: VecDeque<u64>,
    mean: f64,
    std_dev: f64,
    median: u64,
    p95: u64,
    outlier_count: u64,
    last_update: Instant,
}

impl TipStatistics {
    fn update(&mut self, tips: &[u64]) -> Result<()> {
        if tips.is_empty() {
            return Ok(());
        }
        
        self.recent_tips.clear();
        self.recent_tips.extend(tips.iter().copied());
        
        // Keep only recent data for performance
        while self.recent_tips.len() > 1000 {
            self.recent_tips.pop_front();
        }
        
        let data: Vec<f64> = tips.iter().map(|&x| x as f64).collect();
        
        if data.len() >= MIN_STATISTICAL_SAMPLES {
            self.mean = data.mean();
            self.std_dev = data.std_dev();
            
            let mut sorted_tips = tips.to_vec();
            sorted_tips.sort_unstable();
            
            self.median = sorted_tips[sorted_tips.len() / 2];
            self.p95 = sorted_tips[(sorted_tips.len() as f64 * 0.95) as usize];
            
            // Count outliers (beyond 3 sigma)
            self.outlier_count = tips.iter()
                .filter(|&&tip| {
                    let z_score = (tip as f64 - self.mean).abs() / self.std_dev;
                    z_score > OUTLIER_THRESHOLD_SIGMA
                })
                .count() as u64;
        }
        
        self.last_update = Instant::now();
        Ok(())
    }
    
    fn is_outlier(&self, tip: u64) -> bool {
        if self.std_dev == 0.0 {
            return false;
        }
        let z_score = (tip as f64 - self.mean).abs() / self.std_dev;
        z_score > OUTLIER_THRESHOLD_SIGMA
    }
}

impl BundleSuccessRateCalculator {
    pub fn new() -> Result<Self> {
        Ok(Self {
            submissions: Arc::new(DashMap::new()),
            time_series_data: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORY_ENTRIES))),
            validator_stats: Arc::new(DashMap::new()),
            failure_analysis: Arc::new(DashMap::new()),
            slot_index: Arc::new(RwLock::new(BTreeMap::new())),
            validator_index: Arc::new(RwLock::new(BTreeMap::new())),
            total_submissions: Arc::new(AtomicU64::new(0)),
            successful_submissions: Arc::new(AtomicU64::new(0)),
            max_history_duration: Duration::from_secs(3600 * 24), // 24 hours
            update_interval: Duration::from_secs(60), // 1 minute
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
            tip_statistics: Arc::new(RwLock::new(TipStatistics::default())),
        })
    }

    pub fn record_submission(&self, mut submission: BundleSubmission) -> Result<()> {
        // Validate submission data
        self.validate_submission(&submission)?;
        
        // Enforce Jito minimum tip
        if submission.tip_lamports < JITO_MIN_TIP_LAMPORTS {
            submission.status = BundleStatus::Failed;
            submission.failure_reason = Some(FailureReason::TipBelowMinimum);
        }
        
        let bundle_id = submission.bundle_id.clone();
        let validator = submission.validator;
        
        // Update atomic counters
        self.total_submissions.fetch_add(1, Ordering::Relaxed);
        if submission.status == BundleStatus::Landed {
            self.successful_submissions.fetch_add(1, Ordering::Relaxed);
        }
        
        self.submissions.insert(bundle_id.clone(), submission.clone());
        
        // Safe RwLock handling
        {
            let mut time_series = self.time_series_data.write()
                .map_err(|e| anyhow!("Failed to acquire time_series lock: {}", e))?;
            time_series.push_back(submission.clone());
            
            // Maintain size limit with proper memory management
            while time_series.len() > MAX_HISTORY_ENTRIES {
                if let Some(old_entry) = time_series.pop_front() {
                    self.remove_from_indices(&old_entry.bundle_id, old_entry.slot, old_entry.validator)?;
                }
            }
        }
        
        // Update indices for fast lookups
        self.update_indices(&bundle_id, submission.slot, validator)?;
        self.update_validator_stats(validator, &submission)?;
        
        if submission.status == BundleStatus::Failed {
            if let Some(reason) = submission.failure_reason {
                self.update_failure_analysis(reason, &submission)?;
            }
        }
        
        // Update tip statistics
        self.update_tip_statistics(&submission)?;
        
        // Non-blocking cleanup
        if let Ok(last_cleanup) = self.last_cleanup.try_read() {
            if last_cleanup.elapsed() > self.update_interval {
                drop(last_cleanup);
                if let Err(e) = self.cleanup_old_data() {
                    // Log error but don't fail the entire operation
                    eprintln!("Warning: Failed to cleanup old data: {}", e);
                }
            }
        }
        
        Ok(())
    }

    pub fn update_bundle_status(
        &self,
        bundle_id: &str,
        status: BundleStatus,
        failure_reason: Option<FailureReason>,
        landing_slot: Option<u64>,
        competitor_tip: Option<u64>,
    ) -> Result<()> {
        if let Some(mut submission) = self.submissions.get_mut(bundle_id) {
            let old_status = submission.status;
            
            submission.status = status;
            submission.failure_reason = failure_reason;
            submission.landing_slot = landing_slot;
            submission.competitor_tip = competitor_tip;
            
            // Update success counter atomically
            if old_status != BundleStatus::Landed && status == BundleStatus::Landed {
                self.successful_submissions.fetch_add(1, Ordering::Relaxed);
            } else if old_status == BundleStatus::Landed && status != BundleStatus::Landed {
                self.successful_submissions.fetch_sub(1, Ordering::Relaxed);
            }
            
            let updated_submission = submission.clone();
            drop(submission);
            
            self.update_validator_stats(updated_submission.validator, &updated_submission)?;
            
            if status == BundleStatus::Failed {
                if let Some(reason) = failure_reason {
                    self.update_failure_analysis(reason, &updated_submission)?;
                }
            }
            
            // Update tip statistics
            self.update_tip_statistics(&updated_submission)?;
        }
        Ok(())
    }

    pub fn get_stats_for_time_window(&self, window: Duration) -> Result<TimeWindowStats> {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time error: {}", e))?
            .as_secs()
            .saturating_sub(window.as_secs());
        
        let time_series = self.time_series_data.read()
            .map_err(|e| anyhow!("Failed to acquire time_series read lock: {}", e))?;
        let recent_submissions: Vec<&BundleSubmission> = time_series
            .iter()
            .filter(|s| s.timestamp >= cutoff_time)
            .collect();
        
        if recent_submissions.is_empty() {
            return Ok(TimeWindowStats {
                total_submissions: 0,
                successful_bundles: 0,
                failed_bundles: 0,
                expired_bundles: 0,
                rejected_bundles: 0,
                total_tips_paid: 0,
                average_tip: 0.0,
                success_rate: 0.0,
                average_landing_time_ms: 0.0,
                median_tip: 0,
                p95_tip: 0,
                min_successful_tip: 0,
                max_successful_tip: 0,
            });
        }
        
        let total_submissions = recent_submissions.len() as u64;
        let successful_bundles = recent_submissions
            .iter()
            .filter(|s| s.status == BundleStatus::Landed)
            .count() as u64;
        let failed_bundles = recent_submissions
            .iter()
            .filter(|s| s.status == BundleStatus::Failed)
            .count() as u64;
        let expired_bundles = recent_submissions
            .iter()
            .filter(|s| s.status == BundleStatus::Expired)
            .count() as u64;
        let rejected_bundles = recent_submissions
            .iter()
            .filter(|s| s.status == BundleStatus::Rejected)
            .count() as u64;
        
        let successful_tips: Vec<u64> = recent_submissions
            .iter()
            .filter(|s| s.status == BundleStatus::Landed)
            .map(|s| s.tip_lamports)
            .collect();
        
        let total_tips_paid: u64 = successful_tips.iter().sum();
        let average_tip = if successful_bundles > 0 {
            total_tips_paid as f64 / successful_bundles as f64
        } else {
            0.0
        };
        
        let success_rate = if total_submissions > 0 {
            successful_bundles as f64 / total_submissions as f64
        } else {
            0.0
        };
        
        // Fixed: Use correct Solana slot timing (420ms, not 400ms)
        let landing_times: Vec<u64> = recent_submissions
            .iter()
            .filter(|s| s.status == BundleStatus::Landed && s.landing_slot.is_some())
            .map(|s| (s.landing_slot.unwrap() - s.slot) * SOLANA_SLOT_TIME_MS)
            .collect();
        
        let average_landing_time_ms = if !landing_times.is_empty() {
            landing_times.iter().sum::<u64>() as f64 / landing_times.len() as f64
        } else {
            0.0
        };
        
        let (median_tip, p95_tip) = self.calculate_percentiles_safe(&successful_tips)?;
        let min_successful_tip = successful_tips.iter().min().copied().unwrap_or(0);
        let max_successful_tip = successful_tips.iter().max().copied().unwrap_or(0);
        
        Ok(TimeWindowStats {
            total_submissions,
            successful_bundles,
            failed_bundles,
            expired_bundles,
            rejected_bundles,
            total_tips_paid,
            average_tip,
            success_rate,
            average_landing_time_ms,
            median_tip,
            p95_tip,
            min_successful_tip,
            max_successful_tip,
        })
    }

    pub fn get_validator_performance(&self) -> Vec<ValidatorStats> {
        self.validator_stats
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn get_failure_analysis(&self, window: Duration) -> Vec<FailureAnalysis> {
        let mut analysis: Vec<FailureAnalysis> = self.failure_analysis
            .iter()
            .map(|entry| {
                let mut fa = entry.value().clone();
                fa.time_window_hours = window.as_secs() / 3600;
                fa
            })
            .collect();
        
        analysis.sort_by(|a, b| b.count.cmp(&a.count));
        analysis
    }

    pub fn get_optimal_tip_estimate(&self, validator: &Pubkey, confidence_level: f64) -> Result<u64> {
        if !(0.0..=1.0).contains(&confidence_level) {
            return Err(anyhow!("Confidence level must be between 0.0 and 1.0"));
        }
        
        let stats = self.get_stats_for_time_window(Duration::from_secs(3600))?;
        let validator_stats = self.validator_stats.get(validator);
        
        // Enhanced tip calculation with validator-specific adjustments
        let base_tip = if let Some(vs) = validator_stats {
            if vs.total_submissions >= MIN_STATISTICAL_SAMPLES as u64 {
                // Use validator-specific stats if we have enough data
                (vs.average_tip * 1.1) as u64
            } else {
                // Fall back to network average for new validators
                stats.average_tip as u64
            }
        } else {
            stats.average_tip as u64
        };
        
        // Account for validator rotation and slot position
        let slot_adjustment = self.calculate_slot_position_adjustment(validator)?;
        let confidence_multiplier = 1.0 + (confidence_level - 0.5).max(0.0) * 2.0;
        let network_congestion_factor = self.calculate_network_congestion_factor()?;
        
        let optimal_tip = (base_tip as f64 
            * confidence_multiplier 
            * network_congestion_factor 
            * slot_adjustment) as u64;
        
        // Enforce Jito minimum and reasonable bounds
        let bounded_tip = optimal_tip
            .max(JITO_MIN_TIP_LAMPORTS)
            .max(stats.median_tip)
            .min(stats.p95_tip * 3); // Allow higher multiplier for extreme confidence
        
        Ok(bounded_tip)
    }

    pub fn should_increase_tip(&self, bundle_id: &str, current_slot: u64) -> Result<bool> {
        if let Some(submission) = self.submissions.get(bundle_id) {
            let slots_passed = current_slot.saturating_sub(submission.slot);
            let recent_stats = self.get_stats_for_time_window(Duration::from_secs(300))?;
            
            // Conservative approach - increase tip if bundle is pending for more than 3 slots
            // and our tip is below recent median
            if slots_passed > 3 && submission.status == BundleStatus::Pending {
                return Ok(submission.tip_lamports < recent_stats.median_tip);
            }
        }
        Ok(false)
    }

    pub fn get_competitor_analysis(&self, window: Duration) -> Result<HashMap<String, f64>> {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time error: {}", e))?
            .as_secs()
            .saturating_sub(window.as_secs());
        
        let time_series = self.time_series_data.read()
            .map_err(|e| anyhow!("Failed to acquire time_series read lock: {}", e))?;
        let failed_with_competitor: Vec<&BundleSubmission> = time_series
            .iter()
            .filter(|s| {
                s.timestamp >= cutoff_time
                    && s.status == BundleStatus::Failed
                    && s.failure_reason == Some(FailureReason::CompetitorOutbid)
                    && s.competitor_tip.is_some()
            })
            .collect();
        
        let mut analysis = HashMap::new();
        
        if !failed_with_competitor.is_empty() {
            let our_tips: Vec<u64> = failed_with_competitor.iter().map(|s| s.tip_lamports).collect();
            let competitor_tips: Vec<u64> = failed_with_competitor
                .iter()
                .filter_map(|s| s.competitor_tip)
                .collect();
            
            let avg_our_tip = our_tips.iter().sum::<u64>() as f64 / our_tips.len() as f64;
            let avg_competitor_tip = competitor_tips.iter().sum::<u64>() as f64 / competitor_tips.len() as f64;
            
            analysis.insert("avg_outbid_percentage".to_string(), (avg_competitor_tip - avg_our_tip) / avg_our_tip * 100.0);
            analysis.insert("outbid_frequency".to_string(), failed_with_competitor.len() as f64);
            analysis.insert("recommended_tip_increase".to_string(), (avg_competitor_tip / avg_our_tip - 1.0).max(0.05) * 100.0);
        }
        
        Ok(analysis)
    }

        fn update_validator_stats(&self, validator: Pubkey, submission: &BundleSubmission) -> Result<()> {
        let mut entry = self.validator_stats.entry(validator).or_insert_with(|| {
            ValidatorStats {
                validator,
                total_submissions: 0,
                successful_bundles: 0,
                success_rate: 0.0,
                average_tip: 0.0,
                last_submission: submission.timestamp,
                average_landing_time_ms: 0.0,
                rejection_rate: 0.0,
            }
        });
        
        entry.total_submissions += 1;
        entry.last_submission = submission.timestamp;
        
        if submission.status == BundleStatus::Landed {
            entry.successful_bundles += 1;
            
            // Update average tip with exponential moving average
            let alpha = 0.1;
            entry.average_tip = entry.average_tip * (1.0 - alpha) + submission.tip_lamports as f64 * alpha;
            
            // Update landing time if available (fixed timing calculation)
            if let Some(landing_slot) = submission.landing_slot {
                let landing_time_ms = (landing_slot - submission.slot) * SOLANA_SLOT_TIME_MS;
                entry.average_landing_time_ms = entry.average_landing_time_ms * (1.0 - alpha) + landing_time_ms as f64 * alpha;
            }
        }
        
        entry.success_rate = entry.successful_bundles as f64 / entry.total_submissions as f64;
        
        if submission.status == BundleStatus::Rejected {
            let rejected_count = self.count_rejected_for_validator(&validator)?;
            entry.rejection_rate = rejected_count as f64 / entry.total_submissions as f64;
        }
        
        Ok(())
    }

    fn update_failure_analysis(&self, reason: FailureReason, submission: &BundleSubmission) -> Result<()> {
        let mut entry = self.failure_analysis.entry(reason).or_insert_with(|| {
            FailureAnalysis {
                reason,
                count: 0,
                percentage: 0.0,
                average_tip_on_failure: 0.0,
                time_window_hours: 24,
            }
        });
        
        entry.count += 1;
        
        // Update average tip on failure with exponential moving average
        let alpha = 0.1;
        entry.average_tip_on_failure = entry.average_tip_on_failure * (1.0 - alpha) + submission.tip_lamports as f64 * alpha;
        
        // Update percentage
        let total_failures = self.failure_analysis.iter().map(|e| e.count).sum::<u64>();
        if total_failures > 0 {
            entry.percentage = entry.count as f64 / total_failures as f64 * 100.0;
        }
        
        Ok(())
    }

    // Safe percentile calculation with outlier detection
    fn calculate_percentiles_safe(&self, tips: &[u64]) -> Result<(u64, u64)> {
        if tips.is_empty() {
            return Ok((0, 0));
        }
        
        if tips.len() < MIN_STATISTICAL_SAMPLES {
            // Not enough data for reliable statistics
            let mut sorted_tips = tips.to_vec();
            sorted_tips.sort_unstable();
            let median = sorted_tips[sorted_tips.len() / 2];
            return Ok((median, median));
        }
        
        // Remove outliers for more robust percentiles
        let filtered_tips = self.remove_outliers(tips)?;
        
        if filtered_tips.is_empty() {
            return Ok((0, 0));
        }
        
        let mut sorted_tips = filtered_tips;
        sorted_tips.sort_unstable();
        
        let median_index = sorted_tips.len() / 2;
        let median = if sorted_tips.len() % 2 == 0 && median_index > 0 {
            (sorted_tips[median_index - 1] + sorted_tips[median_index]) / 2
        } else {
            sorted_tips[median_index]
        };
        
        let p95_index = ((sorted_tips.len() as f64 * 0.95) as usize).min(sorted_tips.len() - 1);
        let p95 = sorted_tips[p95_index];
        
        Ok((median, p95))
    }
    
    fn remove_outliers(&self, tips: &[u64]) -> Result<Vec<u64>> {
        if tips.len() < MIN_STATISTICAL_SAMPLES {
            return Ok(tips.to_vec());
        }
        
        let data: Vec<f64> = tips.iter().map(|&x| x as f64).collect();
        let mean = data.iter().sum::<f64>() / data.len() as f64;
        let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / data.len() as f64;
        let std_dev = variance.sqrt();
        
        if std_dev == 0.0 {
            return Ok(tips.to_vec());
        }
        
        let filtered: Vec<u64> = tips.iter()
            .filter(|&&tip| {
                let z_score = (tip as f64 - mean).abs() / std_dev;
                z_score <= OUTLIER_THRESHOLD_SIGMA
            })
            .copied()
            .collect();
        
        // Ensure we don't filter out too much data
        if filtered.len() < tips.len() / 2 {
            Ok(tips.to_vec())
        } else {
            Ok(filtered)
        }
    }

    fn calculate_network_congestion_factor(&self) -> Result<f64> {
        let recent_stats = self.get_stats_for_time_window(Duration::from_secs(300))?; // 5 minutes
        let hourly_stats = self.get_stats_for_time_window(Duration::from_secs(3600))?; // 1 hour
        
        if recent_stats.total_submissions < MIN_STATISTICAL_SAMPLES as u64 || 
           hourly_stats.total_submissions < MIN_STATISTICAL_SAMPLES as u64 {
            return Ok(1.0);
        }
        
        let recent_failure_rate = 1.0 - recent_stats.success_rate;
        let hourly_failure_rate = 1.0 - hourly_stats.success_rate;
        
        // Multi-factor congestion analysis
        let failure_rate_ratio = recent_failure_rate / hourly_failure_rate.max(0.01);
        let tip_inflation = if hourly_stats.average_tip > 0.0 {
            recent_stats.average_tip / hourly_stats.average_tip
        } else {
            1.0
        };
        
        // Consider submission volume as congestion indicator
        let volume_factor = if hourly_stats.total_submissions > 0 {
            (recent_stats.total_submissions as f64 / 5.0) / // 5-minute window
            (hourly_stats.total_submissions as f64 / 60.0)   // Per-minute from hourly
        } else {
            1.0
        };
        
        // Weighted congestion score
        let congestion_score = (failure_rate_ratio * 0.4) + 
                              (tip_inflation * 0.4) + 
                              (volume_factor * 0.2);
        
        // Conservative bounds based on mainnet observations
        Ok(congestion_score.max(0.7).min(3.0))
    }

    fn count_rejected_for_validator(&self, validator: &Pubkey) -> Result<u64> {
        let time_series = self.time_series_data.read()
            .map_err(|e| anyhow!("Failed to acquire time_series read lock: {}", e))?;
        
        Ok(time_series
            .iter()
            .filter(|s| s.validator == *validator && s.status == BundleStatus::Rejected)
            .count() as u64)
    }
    
    // Critical helper functions for mainnet operation
    fn validate_submission(&self, submission: &BundleSubmission) -> Result<()> {
        if submission.bundle_id.is_empty() {
            return Err(anyhow!("Bundle ID cannot be empty"));
        }
        
        if submission.bundle_id.len() > 64 {
            return Err(anyhow!("Bundle ID too long (max 64 characters)"));
        }
        
        if submission.slot == 0 {
            return Err(anyhow!("Invalid slot number"));
        }
        
        if submission.num_transactions == 0 {
            return Err(anyhow!("Bundle must contain at least one transaction"));
        }
        
        if submission.num_transactions > 5 {
            return Err(anyhow!("Bundle cannot contain more than 5 transactions (Jito limit)"));
        }
        
        // Validate timestamp is reasonable (within last 24 hours to next hour)
        let now = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time error: {}", e))?.as_secs();
        
        if submission.timestamp > now + 3600 {
            return Err(anyhow!("Submission timestamp is too far in the future"));
        }
        
        if now.saturating_sub(submission.timestamp) > 86400 {
            return Err(anyhow!("Submission timestamp is too old"));
        }
        
        Ok(())
    }
    
    fn update_indices(&self, bundle_id: &str, slot: u64, validator: Pubkey) -> Result<()> {
        // Update slot index
        {
            let mut slot_index = self.slot_index.write()
                .map_err(|e| anyhow!("Failed to acquire slot_index write lock: {}", e))?;
            slot_index.entry(slot).or_default().push(bundle_id.to_string());
        }
        
        // Update validator index
        {
            let mut validator_index = self.validator_index.write()
                .map_err(|e| anyhow!("Failed to acquire validator_index write lock: {}", e))?;
            validator_index.entry(validator).or_default().push(bundle_id.to_string());
        }
        
        Ok(())
    }
    
    fn remove_from_indices(&self, bundle_id: &str, slot: u64, validator: Pubkey) -> Result<()> {
        // Remove from slot index
        {
            let mut slot_index = self.slot_index.write()
                .map_err(|e| anyhow!("Failed to acquire slot_index write lock: {}", e))?;
            if let Some(bundle_list) = slot_index.get_mut(&slot) {
                bundle_list.retain(|id| id != bundle_id);
                if bundle_list.is_empty() {
                    slot_index.remove(&slot);
                }
            }
        }
        
        // Remove from validator index
        {
            let mut validator_index = self.validator_index.write()
                .map_err(|e| anyhow!("Failed to acquire validator_index write lock: {}", e))?;
            if let Some(bundle_list) = validator_index.get_mut(&validator) {
                bundle_list.retain(|id| id != bundle_id);
                if bundle_list.is_empty() {
                    validator_index.remove(&validator);
                }
            }
        }
        
        Ok(())
    }
    
    fn update_tip_statistics(&self, submission: &BundleSubmission) -> Result<()> {
        if submission.status == BundleStatus::Landed {
            let tips = vec![submission.tip_lamports];
            let mut tip_stats = self.tip_statistics.write()
                .map_err(|e| anyhow!("Failed to acquire tip_statistics write lock: {}", e))?;
            tip_stats.update(&tips)?;
        }
        Ok(())
    }
    
    // Solana-specific slot position adjustment based on validator rotation
    fn calculate_slot_position_adjustment(&self, validator: &Pubkey) -> Result<f64> {
        // Get current network slot (would need to be provided externally in real implementation)
        // For now, use approximate calculation based on validator performance
        
        if let Some(validator_stats) = self.validator_stats.get(validator) {
            // Validators with better performance get slight boost during their rotation
            let performance_factor = validator_stats.success_rate;
            
            // Account for validator rotation pattern (every 4 slots)
            let rotation_boost = if performance_factor > 0.8 { 1.05 } else { 1.0 };
            
            Ok(rotation_boost)
        } else {
            // New validator - neutral adjustment
            Ok(1.0)
        }
    }

    fn cleanup_old_data(&self) -> Result<()> {
        let mut last_cleanup = self.last_cleanup.write()
            .map_err(|e| anyhow!("Failed to acquire last_cleanup write lock: {}", e))?;
        
        if last_cleanup.elapsed() < self.update_interval {
            return Ok(());
        }
        
        *last_cleanup = Instant::now();
        drop(last_cleanup);
        
        // Cleanup submissions older than retention period
        let retention_period = Duration::from_secs(86400); // 24 hours
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time error during cleanup: {}", e))?
            .as_secs().saturating_sub(retention_period.as_secs());
        
        self.submissions.retain(|_, submission| {
            submission.timestamp > cutoff_time
        });
        
        // Cleanup validator stats for inactive validators
        let inactive_threshold = Duration::from_secs(7200); // 2 hours
        let inactive_cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time error during validator cleanup: {}", e))?
            .as_secs().saturating_sub(inactive_threshold.as_secs());
        
        self.validator_stats.retain(|_, stats| {
            stats.last_submission > inactive_cutoff
        });
        
        // Cleanup failure analysis entries with very low counts
        self.failure_analysis.retain(|_, analysis| {
            analysis.count >= 5
        });
        
        Ok(())
    }

    pub fn get_slot_success_pattern(&self, window: Duration) -> Result<HashMap<u64, f64>> {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time error: {}", e))?
            .as_secs()
            .saturating_sub(window.as_secs());
        
        let time_series = self.time_series_data.read()
            .map_err(|e| anyhow!("Failed to acquire time_series read lock: {}", e))?;
        let mut slot_stats: HashMap<u64, (u64, u64)> = HashMap::new();
        
        for submission in time_series.iter().filter(|s| s.timestamp >= cutoff_time) {
            let slot_mod = submission.slot % 4;
            let (total, successful) = slot_stats.entry(slot_mod).or_insert((0, 0));
            *total += 1;
            if submission.status == BundleStatus::Landed {
                *successful += 1;
            }
        }
        
        Ok(slot_stats
            .into_iter()
            .map(|(slot, (total, successful))| {
                let success_rate = if total > 0 {
                    successful as f64 / total as f64
                } else {
                    0.0
                };
                (slot, success_rate)
            })
            .collect())
    }

    pub fn get_tip_optimization_metrics(&self) -> Result<TipOptimizationMetrics> {
        let stats_5m = self.get_stats_for_time_window(Duration::from_secs(300))?;
        let stats_1h = self.get_stats_for_time_window(Duration::from_secs(3600))?;
        let stats_24h = self.get_stats_for_time_window(Duration::from_secs(86400))?;
        
        let tip_volatility = self.calculate_tip_volatility()?;
        let optimal_tip_range = self.calculate_optimal_tip_range()?;
        
        Ok(TipOptimizationMetrics {
            current_median_tip: stats_5m.median_tip,
            hourly_median_tip: stats_1h.median_tip,
            daily_median_tip: stats_24h.median_tip,
            tip_volatility,
            optimal_tip_range,
            success_rate_by_tip_range: self.calculate_success_rate_by_tip_range()?,
            recommended_base_tip: self.calculate_recommended_base_tip()?,
            congestion_multiplier: self.calculate_network_congestion_factor()?,
        })
    }

    fn calculate_tip_volatility(&self) -> Result<f64> {
        let time_series = self.time_series_data.read()
            .map_err(|e| anyhow!("Failed to acquire time_series read lock: {}", e))?;
        let recent_tips: Vec<f64> = time_series
            .iter()
            .rev()
            .take(100)
            .filter(|s| s.status == BundleStatus::Landed)
            .map(|s| s.tip_lamports as f64)
            .collect();
        
        if recent_tips.len() < 2 {
            return Ok(0.0);
        }
        
        let mean = recent_tips.iter().sum::<f64>() / recent_tips.len() as f64;
        let variance = recent_tips.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / recent_tips.len() as f64;
        let std_dev = variance.sqrt();
        
        if mean == 0.0 {
            return Ok(0.0);
        }
        
        Ok(std_dev / mean)
    }

    fn calculate_optimal_tip_range(&self) -> Result<(u64, u64)> {
        let stats = self.get_stats_for_time_window(Duration::from_secs(3600))?;
        let lower_bound = (stats.median_tip as f64 * 0.9) as u64;
        let upper_bound = (stats.p95_tip as f64 * 1.1) as u64;
        
        Ok((lower_bound.max(JITO_MIN_TIP_LAMPORTS), upper_bound.min(1_000_000_000)))
    }

    fn calculate_success_rate_by_tip_range(&self) -> Result<HashMap<String, f64>> {
        let time_series = self.time_series_data.read()
            .map_err(|e| anyhow!("Failed to acquire time_series read lock: {}", e))?;
        let mut ranges: HashMap<String, (u64, u64)> = HashMap::new();
        
        for submission in time_series.iter() {
            let range_key = match submission.tip_lamports {
                0..=10_000 => "0-10k",
                10_001..=50_000 => "10k-50k",
                50_001..=100_000 => "50k-100k",
                100_001..=500_000 => "100k-500k",
                500_001..=1_000_000 => "500k-1M",
                _ => "1M+",
            };
            
            let (total, successful) = ranges.entry(range_key.to_string()).or_insert((0, 0));
            *total += 1;
            if submission.status == BundleStatus::Landed {
                *successful += 1;
            }
        }
        
        Ok(ranges
            .into_iter()
            .map(|(range, (total, successful))| {
                let success_rate = if total > 0 {
                    successful as f64 / total as f64
                } else {
                    0.0
                };
                (range, success_rate)
            })
            .collect())
    }

    fn calculate_recommended_base_tip(&self) -> Result<u64> {
        let stats = self.get_stats_for_time_window(Duration::from_secs(1800))?; // 30 minutes
        let congestion = self.calculate_network_congestion_factor()?;
        
        let base = if stats.success_rate > 0.8 {
            stats.median_tip
        } else if stats.success_rate > 0.6 {
            ((stats.median_tip + stats.p95_tip) / 2) as u64
        } else {
            stats.p95_tip
        };
        
        let recommended = (base as f64 * congestion) as u64;
        
        // Ensure minimum tip and reasonable bounds
        Ok(recommended.max(JITO_MIN_TIP_LAMPORTS).min(10_000_000)) // Max 10M lamports
    }

    pub fn export_metrics(&self) -> Result<BundleMetricsExport> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("System time error: {}", e))?
            .as_secs();
            
        Ok(BundleMetricsExport {
            timestamp,
            stats_5m: self.get_stats_for_time_window(Duration::from_secs(300))?,
            stats_1h: self.get_stats_for_time_window(Duration::from_secs(3600))?,
            stats_24h: self.get_stats_for_time_window(Duration::from_secs(86400))?,
            validator_performance: self.get_validator_performance(),
            failure_analysis: self.get_failure_analysis(Duration::from_secs(3600)),
            tip_optimization: self.get_tip_optimization_metrics()?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TipOptimizationMetrics {
    pub current_median_tip: u64,
    pub hourly_median_tip: u64,
    pub daily_median_tip: u64,
    pub tip_volatility: f64,
    pub optimal_tip_range: (u64, u64),
    pub success_rate_by_tip_range: HashMap<String, f64>,
    pub recommended_base_tip: u64,
    pub congestion_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleMetricsExport {
    pub timestamp: u64,
    pub stats_5m: TimeWindowStats,
    pub stats_1h: TimeWindowStats,
    pub stats_24h: TimeWindowStats,
    pub validator_performance: Vec<ValidatorStats>,
    pub failure_analysis: Vec<FailureAnalysis>,
    pub tip_optimization: TipOptimizationMetrics,
}

impl Default for BundleSuccessRateCalculator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundle_success_rate_calculator() {
        let calculator = BundleSuccessRateCalculator::new();
        let validator = Pubkey::new_unique();
        
                let submission = BundleSubmission {
            bundle_id: "test_bundle_1".to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            slot: 100,
            tip_lamports: 50_000,
            validator,
            status: BundleStatus::Pending,
            failure_reason: None,
            landing_slot: None,
            gas_used: Some(100_000),
            priority_fee: 1000,
            num_transactions: 2,
            competitor_tip: None,
            submission_latency_ms: 50,
        };
        
        calculator.record_submission(submission.clone()).unwrap();
        
        // Update to landed
        calculator.update_bundle_status(
            "test_bundle_1",
            BundleStatus::Landed,
            None,
            Some(102),
            None,
        ).unwrap();
        
        let stats = calculator.get_stats_for_time_window(Duration::from_secs(300)).unwrap();
        assert_eq!(stats.total_submissions, 1);
        assert_eq!(stats.successful_bundles, 1);
        assert_eq!(stats.success_rate, 1.0);
    }

    #[test]
    fn test_failure_analysis() {
        let calculator = BundleSuccessRateCalculator::new();
        let validator = Pubkey::new_unique();
        
        // Record multiple failed submissions
        for i in 0..5 {
            let submission = BundleSubmission {
                bundle_id: format!("failed_bundle_{}", i),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                slot: 200 + i as u64,
                tip_lamports: 30_000,
                validator,
                status: BundleStatus::Failed,
                failure_reason: Some(FailureReason::InsufficientTip),
                landing_slot: None,
                gas_used: Some(100_000),
                priority_fee: 1000,
                num_transactions: 2,
                competitor_tip: Some(40_000),
                submission_latency_ms: 50,
            };
            calculator.record_submission(submission);
        }
        
        let failure_analysis = calculator.get_failure_analysis(Duration::from_secs(3600));
        assert!(!failure_analysis.is_empty());
        assert_eq!(failure_analysis[0].reason, FailureReason::InsufficientTip);
        assert_eq!(failure_analysis[0].count, 5);
    }

    #[test]
    fn test_validator_performance() {
        let calculator = BundleSuccessRateCalculator::new();
        let validator1 = Pubkey::new_unique();
        let validator2 = Pubkey::new_unique();
        
        // Record submissions for different validators
        for i in 0..10 {
            let validator = if i % 2 == 0 { validator1 } else { validator2 };
            let status = if i < 7 { BundleStatus::Landed } else { BundleStatus::Failed };
            
            let submission = BundleSubmission {
                bundle_id: format!("bundle_{}", i),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                slot: 300 + i as u64,
                tip_lamports: 50_000 + (i as u64 * 1000),
                validator,
                status,
                failure_reason: if status == BundleStatus::Failed {
                    Some(FailureReason::NetworkCongestion)
                } else {
                    None
                },
                landing_slot: if status == BundleStatus::Landed {
                    Some(302 + i as u64)
                } else {
                    None
                },
                gas_used: Some(100_000),
                priority_fee: 1000,
                num_transactions: 2,
                competitor_tip: None,
                submission_latency_ms: 50,
            };
            calculator.record_submission(submission);
        }
        
        let validator_stats = calculator.get_validator_performance();
        assert_eq!(validator_stats.len(), 2);
    }

    #[test]
    fn test_tip_optimization() {
        let calculator = BundleSuccessRateCalculator::new();
        let validator = Pubkey::new_unique();
        
        // Simulate various tip amounts and outcomes
        let tip_scenarios = vec![
            (10_000, BundleStatus::Failed, Some(FailureReason::InsufficientTip)),
            (50_000, BundleStatus::Landed, None),
            (30_000, BundleStatus::Failed, Some(FailureReason::CompetitorOutbid)),
            (70_000, BundleStatus::Landed, None),
            (40_000, BundleStatus::Failed, Some(FailureReason::InsufficientTip)),
            (60_000, BundleStatus::Landed, None),
            (80_000, BundleStatus::Landed, None),
            (20_000, BundleStatus::Failed, Some(FailureReason::InsufficientTip)),
            (90_000, BundleStatus::Landed, None),
            (55_000, BundleStatus::Landed, None),
        ];
        
        for (i, (tip, status, failure_reason)) in tip_scenarios.iter().enumerate() {
            let submission = BundleSubmission {
                bundle_id: format!("tip_test_{}", i),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - (10 - i) as u64,
                slot: 400 + i as u64,
                tip_lamports: *tip,
                validator,
                status: *status,
                failure_reason: *failure_reason,
                landing_slot: if *status == BundleStatus::Landed {
                    Some(402 + i as u64)
                } else {
                    None
                },
                gas_used: Some(100_000),
                priority_fee: 1000,
                num_transactions: 2,
                competitor_tip: if *failure_reason == Some(FailureReason::CompetitorOutbid) {
                    Some(tip + 20_000)
                } else {
                    None
                },
                submission_latency_ms: 50,
            };
            calculator.record_submission(submission);
        }
        
        let optimal_tip = calculator.get_optimal_tip_estimate(&validator, 0.8).unwrap();
        assert!(optimal_tip >= 50_000 && optimal_tip <= 100_000);
        
        let tip_metrics = calculator.get_tip_optimization_metrics().unwrap();
        assert!(tip_metrics.tip_volatility > 0.0);
        assert!(tip_metrics.recommended_base_tip > 0);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;
        use std::sync::Arc;
        
        let calculator = Arc::new(BundleSuccessRateCalculator::new());
        let mut handles = vec![];
        
        for thread_id in 0..4 {
            let calc_clone = Arc::clone(&calculator);
            let handle = thread::spawn(move || {
                let validator = Pubkey::new_unique();
                for i in 0..25 {
                    let submission = BundleSubmission {
                        bundle_id: format!("thread_{}_bundle_{}", thread_id, i),
                        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                        slot: 500 + (thread_id * 100 + i) as u64,
                        tip_lamports: 50_000 + (i as u64 * 1000),
                        validator,
                        status: if i % 3 == 0 { BundleStatus::Failed } else { BundleStatus::Landed },
                        failure_reason: if i % 3 == 0 { Some(FailureReason::NetworkCongestion) } else { None },
                        landing_slot: if i % 3 != 0 { Some(502 + (thread_id * 100 + i) as u64) } else { None },
                        gas_used: Some(100_000),
                        priority_fee: 1000,
                        num_transactions: 2,
                        competitor_tip: None,
                        submission_latency_ms: 50,
                    };
                    calc_clone.record_submission(submission).unwrap();
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let stats = calculator.get_stats_for_time_window(Duration::from_secs(3600)).unwrap();
        assert_eq!(stats.total_submissions, 100);
    }
}

// Performance monitoring trait implementation
pub trait BundleMetricsProvider {
    fn get_current_success_rate(&self) -> f64;
    fn get_average_tip(&self) -> u64;
    fn get_network_congestion(&self) -> f64;
    fn should_submit_bundle(&self, tip: u64) -> bool;
}

impl BundleMetricsProvider for BundleSuccessRateCalculator {
    fn get_current_success_rate(&self) -> f64 {
        self.get_stats_for_time_window(Duration::from_secs(300))
            .map(|stats| stats.success_rate)
            .unwrap_or(0.0)
    }
    
    fn get_average_tip(&self) -> u64 {
        self.get_stats_for_time_window(Duration::from_secs(300))
            .map(|stats| stats.average_tip as u64)
            .unwrap_or(JITO_MIN_TIP_LAMPORTS)
    }
    
    fn get_network_congestion(&self) -> f64 {
        self.calculate_network_congestion_factor().unwrap_or(1.0)
    }
    
    fn should_submit_bundle(&self, tip: u64) -> bool {
        let stats = match self.get_stats_for_time_window(Duration::from_secs(300)) {
            Ok(stats) => stats,
            Err(_) => return tip >= JITO_MIN_TIP_LAMPORTS,
        };
        
        let congestion = self.calculate_network_congestion_factor().unwrap_or(1.0);
        
        // Don't submit if tip is too low during high congestion
        if congestion > 1.5 && tip < stats.median_tip {
            return false;
        }
        
        // Always submit if tip is above p95
        if tip >= stats.p95_tip {
            return true;
        }
        
        // Submit if success rate is good and tip is reasonable
        stats.success_rate > 0.6 && tip >= stats.median_tip
    }
}

// Real-time alert conditions
pub struct AlertConditions {
    pub low_success_rate_threshold: f64,
    pub high_failure_rate_threshold: f64,
    pub tip_spike_threshold: f64,
    pub validator_rejection_threshold: f64,
}

impl Default for AlertConditions {
    fn default() -> Self {
        Self {
            low_success_rate_threshold: 0.5,
            high_failure_rate_threshold: 0.7,
            tip_spike_threshold: 2.0,
            validator_rejection_threshold: 0.3,
        }
    }
}

impl BundleSuccessRateCalculator {
    pub fn check_alert_conditions(&self, conditions: &AlertConditions) -> Vec<String> {
        let mut alerts = Vec::new();
        
        let stats_5m = match self.get_stats_for_time_window(Duration::from_secs(300)) {
            Ok(stats) => stats,
            Err(_) => return alerts,
        };
        
        let stats_1h = match self.get_stats_for_time_window(Duration::from_secs(3600)) {
            Ok(stats) => stats,
            Err(_) => return alerts,
        };
        
        if stats_5m.success_rate < conditions.low_success_rate_threshold {
            alerts.push(format!(
                "Low success rate: {:.2}% in last 5 minutes",
                stats_5m.success_rate * 100.0
            ));
        }
        
        let failure_rate = 1.0 - stats_5m.success_rate;
        if failure_rate > conditions.high_failure_rate_threshold {
            alerts.push(format!(
                "High failure rate: {:.2}% in last 5 minutes",
                failure_rate * 100.0
            ));
        }
        
        if stats_1h.average_tip > 0.0 && stats_5m.average_tip / stats_1h.average_tip > conditions.tip_spike_threshold {
            alerts.push(format!(
                "Tip spike detected: {:.2}x increase from hourly average",
                stats_5m.average_tip / stats_1h.average_tip
            ));
        }
        
        for validator_stat in self.get_validator_performance() {
            if validator_stat.rejection_rate > conditions.validator_rejection_threshold {
                let validator_short = validator_stat.validator.to_string();
                let validator_display = if validator_short.len() >= 8 {
                    &validator_short[..8]
                } else {
                    &validator_short
                };
                alerts.push(format!(
                    "High rejection rate for validator {}: {:.2}%",
                    validator_display,
                    validator_stat.rejection_rate * 100.0
                ));
            }
        }
        
        alerts
    }
}

