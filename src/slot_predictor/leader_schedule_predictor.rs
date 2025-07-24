use anyhow::{anyhow, Result};
use dashmap::DashMap;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcBlockConfig, RpcGetVoteAccountsConfig},
};
use solana_sdk::{
    clock::{Epoch, Slot, DEFAULT_SLOTS_PER_EPOCH},
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep},
};
use tracing::{debug, error, info, warn};

const SCHEDULE_CACHE_TTL: Duration = Duration::from_secs(300);
const MAX_CACHED_EPOCHS: usize = 3;
const PREFETCH_THRESHOLD_SLOTS: u64 = 432;
const RPC_RETRY_ATTEMPTS: u32 = 5;
const RPC_RETRY_DELAY_MS: u64 = 100;
const LEADER_SCHEDULE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const VOTE_ACCOUNT_REFRESH_INTERVAL: Duration = Duration::from_secs(120);
const MIN_STAKE_LAMPORTS: u64 = 1_000_000_000;

#[derive(Debug, Clone)]
pub struct LeaderInfo {
    pub pubkey: Pubkey,
    pub slot: Slot,
    pub epoch: Epoch,
    pub stake: u64,
    pub commission: u8,
    pub last_vote: Option<Slot>,
}

#[derive(Debug, Clone)]
struct EpochSchedule {
    pub epoch: Epoch,
    pub start_slot: Slot,
    pub end_slot: Slot,
    pub leader_schedule: Vec<(Slot, Pubkey)>,
    pub slot_leaders: HashMap<Slot, Pubkey>,
    pub leader_slots: HashMap<Pubkey, Vec<Slot>>,
    pub cached_at: Instant,
}

#[derive(Debug, Clone)]
struct ValidatorInfo {
    pub pubkey: Pubkey,
    pub stake: u64,
    pub commission: u8,
    pub last_vote: Option<Slot>,
    pub vote_account: Pubkey,
}

pub struct LeaderSchedulePredictor {
    rpc_client: Arc<RpcClient>,
    epoch_schedules: Arc<DashMap<Epoch, EpochSchedule>>,
    validator_info: Arc<RwLock<HashMap<Pubkey, ValidatorInfo>>>,
    current_slot: Arc<AtomicU64>,
    current_epoch: Arc<AtomicU64>,
    slots_per_epoch: u64,
    first_normal_epoch: u64,
    first_normal_slot: u64,
    leader_schedule_cache: Arc<Mutex<BTreeMap<Slot, LeaderInfo>>>,
    update_lock: Arc<Mutex<()>>,
}

impl LeaderSchedulePredictor {
    pub async fn new(rpc_client: Arc<RpcClient>) -> Result<Self> {
        let epoch_info = rpc_client.get_epoch_info().await?;
        let genesis_config = rpc_client.get_genesis_hash().await?;
        
        let slots_per_epoch = epoch_info.slots_per_epoch;
        let first_normal_epoch = 0;
        let first_normal_slot = 0;

        let predictor = Self {
            rpc_client,
            epoch_schedules: Arc::new(DashMap::new()),
            validator_info: Arc::new(RwLock::new(HashMap::new())),
            current_slot: Arc::new(AtomicU64::new(epoch_info.absolute_slot)),
            current_epoch: Arc::new(AtomicU64::new(epoch_info.epoch)),
            slots_per_epoch,
            first_normal_epoch,
            first_normal_slot,
            leader_schedule_cache: Arc::new(Mutex::new(BTreeMap::new())),
            update_lock: Arc::new(Mutex::new(())),
        };

        predictor.initialize().await?;
        Ok(predictor)
    }

    async fn initialize(&self) -> Result<()> {
        self.update_validator_info().await?;
        
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);
        self.fetch_and_cache_epoch_schedule(current_epoch).await?;
        
        if current_epoch > 0 {
            let _ = self.fetch_and_cache_epoch_schedule(current_epoch.saturating_sub(1)).await;
        }
        
        let next_epoch = current_epoch + 1;
        let _ = self.fetch_and_cache_epoch_schedule(next_epoch).await;
        
        Ok(())
    }

    pub async fn start_background_updater(self: Arc<Self>) {
        let schedule_updater = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(LEADER_SCHEDULE_REFRESH_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(e) = schedule_updater.update_schedules().await {
                    error!("Failed to update leader schedules: {}", e);
                }
            }
        });

        let validator_updater = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(VOTE_ACCOUNT_REFRESH_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(e) = validator_updater.update_validator_info().await {
                    error!("Failed to update validator info: {}", e);
                }
            }
        });

        let slot_updater = self.clone();
        tokio::spawn(async move {
            loop {
                match slot_updater.update_current_slot().await {
                    Ok(_) => sleep(Duration::from_millis(400)).await,
                    Err(e) => {
                        error!("Failed to update current slot: {}", e);
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    async fn update_current_slot(&self) -> Result<()> {
        let slot = self.rpc_client.get_slot().await?;
        let epoch = self.slot_to_epoch(slot);
        
        self.current_slot.store(slot, AtomicOrdering::Release);
        self.current_epoch.store(epoch, AtomicOrdering::Release);
        
        Ok(())
    }

    async fn update_schedules(&self) -> Result<()> {
        let _lock = self.update_lock.lock().await;
        
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        
        self.cleanup_old_epochs(current_epoch);
        
        if !self.epoch_schedules.contains_key(&current_epoch) {
            self.fetch_and_cache_epoch_schedule(current_epoch).await?;
        }
        
        let next_epoch = current_epoch + 1;
        let epoch_end_slot = self.epoch_to_slot(next_epoch);
        
        if epoch_end_slot.saturating_sub(current_slot) <= PREFETCH_THRESHOLD_SLOTS {
            if !self.epoch_schedules.contains_key(&next_epoch) {
                self.fetch_and_cache_epoch_schedule(next_epoch).await?;
            }
        }
        
        Ok(())
    }

    async fn fetch_and_cache_epoch_schedule(&self, epoch: Epoch) -> Result<EpochSchedule> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match self.fetch_epoch_schedule_internal(epoch).await {
                Ok(schedule) => {
                    self.epoch_schedules.insert(epoch, schedule.clone());
                    info!("Cached leader schedule for epoch {}", epoch);
                    return Ok(schedule);
                }
                Err(e) if attempts < RPC_RETRY_ATTEMPTS => {
                    warn!("Failed to fetch epoch schedule (attempt {}): {}", attempts, e);
                    sleep(Duration::from_millis(RPC_RETRY_DELAY_MS * attempts as u64)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn fetch_epoch_schedule_internal(&self, epoch: Epoch) -> Result<EpochSchedule> {
        let schedule_data = self.rpc_client
            .get_leader_schedule_with_commitment(
                Some(self.epoch_to_slot(epoch)),
                CommitmentConfig::finalized(),
            )
            .await?
            .ok_or_else(|| anyhow!("No leader schedule found for epoch {}", epoch))?;

        let start_slot = self.epoch_to_slot(epoch);
        let end_slot = self.epoch_to_slot(epoch + 1).saturating_sub(1);
        
        let mut leader_schedule = Vec::new();
        let mut slot_leaders = HashMap::new();
        let mut leader_slots: HashMap<Pubkey, Vec<Slot>> = HashMap::new();

        for (leader_str, slots) in schedule_data {
            let leader = Pubkey::from_str(&leader_str)?;
            for &slot_index in &slots {
                let absolute_slot = start_slot + slot_index as u64;
                leader_schedule.push((absolute_slot, leader));
                slot_leaders.insert(absolute_slot, leader);
                leader_slots.entry(leader)
                    .or_insert_with(Vec::new)
                    .push(absolute_slot);
            }
        }

        leader_schedule.sort_by_key(|(slot, _)| *slot);

        Ok(EpochSchedule {
            epoch,
            start_slot,
            end_slot,
            leader_schedule,
            slot_leaders,
            leader_slots,
            cached_at: Instant::now(),
        })
    }

    async fn update_validator_info(&self) -> Result<()> {
        let vote_accounts = self.rpc_client
            .get_vote_accounts_with_commitment(CommitmentConfig::finalized())
            .await?;

        let mut validator_map = HashMap::new();

        for vote_account in vote_accounts.current.into_iter().chain(vote_accounts.delinquent) {
            if vote_account.activated_stake < MIN_STAKE_LAMPORTS {
                continue;
            }

            let vote_pubkey = Pubkey::from_str(&vote_account.vote_pubkey)?;
            let node_pubkey = Pubkey::from_str(&vote_account.node_pubkey)?;

            validator_map.insert(
                node_pubkey,
                ValidatorInfo {
                    pubkey: node_pubkey,
                    stake: vote_account.activated_stake,
                    commission: vote_account.commission,
                    last_vote: vote_account.last_vote,
                    vote_account: vote_pubkey,
                },
            );
        }

        *self.validator_info.write().await = validator_map;
        Ok(())
    }

    pub async fn get_leader_at_slot(&self, slot: Slot) -> Result<LeaderInfo> {
        let epoch = self.slot_to_epoch(slot);
        
        if let Some(schedule) = self.epoch_schedules.get(&epoch) {
            if let Some(&leader) = schedule.slot_leaders.get(&slot) {
                let validators = self.validator_info.read().await;
                let validator_info = validators.get(&leader);
                
                return Ok(LeaderInfo {
                    pubkey: leader,
                    slot,
                    epoch,
                    stake: validator_info.map(|v| v.stake).unwrap_or(0),
                    commission: validator_info.map(|v| v.commission).unwrap_or(100),
                    last_vote: validator_info.and_then(|v| v.last_vote),
                });
            }
        }

        self.fetch_and_cache_epoch_schedule(epoch).await?;
        
        if let Some(schedule) = self.epoch_schedules.get(&epoch) {
            if let Some(&leader) = schedule.slot_leaders.get(&slot) {
                let validators = self.validator_info.read().await;
                let validator_info = validators.get(&leader);
                
                return Ok(LeaderInfo {
                    pubkey: leader,
                    slot,
                    epoch,
                    stake: validator_info.map(|v| v.stake).unwrap_or(0),
                    commission: validator_info.map(|v| v.commission).unwrap_or(100),
                    last_vote: validator_info.and_then(|v| v.last_vote),
                });
            }
        }

        Err(anyhow!("Could not determine leader for slot {}", slot))
    }

    pub async fn get_upcoming_leaders(&self, num_slots: u64) -> Result<Vec<LeaderInfo>> {
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let mut leaders = Vec::with_capacity(num_slots as usize);

        let mut cache = self.leader_schedule_cache.lock().await;
        
        for offset in 0..num_slots {
            let slot = current_slot + offset;
            
            if let Some(leader_info) = cache.get(&slot) {
                leaders.push(leader_info.clone());
            } else {
                let leader_info = self.get_leader_at_slot(slot).await?;
                cache.insert(slot, leader_info.clone());
                leaders.push(leader_info);
            }
        }

        let cutoff_slot = current_slot.saturating_sub(100);
        cache.retain(|&slot, _| slot >= cutoff_slot);

        Ok(leaders)
    }

    pub async fn get_leader_slots_in_range(
        &self,
        leader: &Pubkey,
        start_slot: Slot,
        end_slot: Slot,
    ) -> Result<Vec<Slot>> {
        let start_epoch = self.slot_to_epoch(start_slot);
        let end_epoch = self.slot_to_epoch(end_slot);
                let mut leader_slots = Vec::new();

        for epoch in start_epoch..=end_epoch {
            if !self.epoch_schedules.contains_key(&epoch) {
                self.fetch_and_cache_epoch_schedule(epoch).await?;
            }

            if let Some(schedule) = self.epoch_schedules.get(&epoch) {
                if let Some(slots) = schedule.leader_slots.get(leader) {
                    for &slot in slots {
                        if slot >= start_slot && slot <= end_slot {
                            leader_slots.push(slot);
                        }
                    }
                }
            }
        }

        leader_slots.sort_unstable();
        Ok(leader_slots)
    }

    pub async fn get_next_leader_slot(&self, leader: &Pubkey) -> Result<Option<Slot>> {
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);

        for epoch in current_epoch..=current_epoch + 2 {
            if !self.epoch_schedules.contains_key(&epoch) {
                self.fetch_and_cache_epoch_schedule(epoch).await?;
            }

            if let Some(schedule) = self.epoch_schedules.get(&epoch) {
                if let Some(slots) = schedule.leader_slots.get(leader) {
                    for &slot in slots {
                        if slot > current_slot {
                            return Ok(Some(slot));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    pub async fn get_leader_performance_stats(&self, leader: &Pubkey) -> Result<LeaderStats> {
        let validators = self.validator_info.read().await;
        let validator_info = validators.get(leader)
            .ok_or_else(|| anyhow!("Validator info not found for {}", leader))?;

        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);
        
        let mut total_slots = 0u64;
        let mut upcoming_slots = 0u64;
        let mut recent_slots = Vec::new();

        for epoch in current_epoch.saturating_sub(1)..=current_epoch + 1 {
            if let Some(schedule) = self.epoch_schedules.get(&epoch) {
                if let Some(slots) = schedule.leader_slots.get(leader) {
                    total_slots += slots.len() as u64;
                    
                    for &slot in slots {
                        match slot.cmp(&current_slot) {
                            Ordering::Greater => upcoming_slots += 1,
                            Ordering::Equal | Ordering::Less => {
                                if current_slot.saturating_sub(slot) <= 1000 {
                                    recent_slots.push(slot);
                                }
                            }
                        }
                    }
                }
            }
        }

        let vote_lag = validator_info.last_vote
            .map(|last_vote| current_slot.saturating_sub(last_vote))
            .unwrap_or(u64::MAX);

        Ok(LeaderStats {
            pubkey: *leader,
            stake: validator_info.stake,
            commission: validator_info.commission,
            last_vote: validator_info.last_vote,
            vote_lag,
            total_slots,
            upcoming_slots,
            recent_slots,
            performance_score: calculate_performance_score(validator_info.stake, vote_lag, validator_info.commission),
        })
    }

    pub async fn predict_optimal_leaders(&self, num_slots: u64) -> Result<Vec<OptimalLeader>> {
        let upcoming_leaders = self.get_upcoming_leaders(num_slots).await?;
        let validators = self.validator_info.read().await;
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        
        let mut optimal_leaders = Vec::new();
        let mut seen_leaders = HashSet::new();

        for leader_info in upcoming_leaders {
            if !seen_leaders.insert(leader_info.pubkey) {
                continue;
            }

            let validator_info = validators.get(&leader_info.pubkey);
            let vote_lag = validator_info
                .and_then(|v| v.last_vote)
                .map(|last_vote| current_slot.saturating_sub(last_vote))
                .unwrap_or(u64::MAX);

            let reliability_score = calculate_reliability_score(
                leader_info.stake,
                vote_lag,
                leader_info.commission,
            );

            let mev_score = calculate_mev_score(
                leader_info.stake,
                leader_info.commission,
                vote_lag,
            );

            optimal_leaders.push(OptimalLeader {
                pubkey: leader_info.pubkey,
                next_slot: leader_info.slot,
                stake: leader_info.stake,
                commission: leader_info.commission,
                vote_lag,
                reliability_score,
                mev_score,
                combined_score: (reliability_score * 0.6 + mev_score * 0.4),
            });
        }

        optimal_leaders.sort_by(|a, b| {
            b.combined_score.partial_cmp(&a.combined_score)
                .unwrap_or(Ordering::Equal)
        });

        Ok(optimal_leaders)
    }

    pub async fn get_epoch_transition_info(&self) -> Result<EpochTransitionInfo> {
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        let current_epoch = self.current_epoch.load(AtomicOrdering::Acquire);
        let next_epoch = current_epoch + 1;
        let next_epoch_start = self.epoch_to_slot(next_epoch);
        let slots_until_transition = next_epoch_start.saturating_sub(current_slot);

        let current_schedule = self.epoch_schedules.get(&current_epoch)
            .ok_or_else(|| anyhow!("Current epoch schedule not found"))?;
        
        let mut next_schedule = None;
        if slots_until_transition <= PREFETCH_THRESHOLD_SLOTS {
            if let Some(schedule) = self.epoch_schedules.get(&next_epoch) {
                next_schedule = Some(schedule.clone());
            } else {
                let schedule = self.fetch_and_cache_epoch_schedule(next_epoch).await?;
                next_schedule = Some(schedule);
            }
        }

        Ok(EpochTransitionInfo {
            current_epoch,
            next_epoch,
            current_slot,
            next_epoch_start_slot: next_epoch_start,
            slots_until_transition,
            current_epoch_leaders: current_schedule.leader_schedule.len(),
            next_epoch_leaders: next_schedule.as_ref().map(|s| s.leader_schedule.len()),
            transition_ready: next_schedule.is_some(),
        })
    }

    fn slot_to_epoch(&self, slot: Slot) -> Epoch {
        if slot < self.first_normal_slot {
            slot / DEFAULT_SLOTS_PER_EPOCH
        } else {
            ((slot - self.first_normal_slot) / self.slots_per_epoch) + self.first_normal_epoch
        }
    }

    fn epoch_to_slot(&self, epoch: Epoch) -> Slot {
        if epoch <= self.first_normal_epoch {
            epoch * DEFAULT_SLOTS_PER_EPOCH
        } else {
            (epoch - self.first_normal_epoch) * self.slots_per_epoch + self.first_normal_slot
        }
    }

    fn cleanup_old_epochs(&self, current_epoch: Epoch) {
        let min_epoch = current_epoch.saturating_sub(1);
        let mut epochs_to_remove = Vec::new();

        for entry in self.epoch_schedules.iter() {
            if *entry.key() < min_epoch {
                epochs_to_remove.push(*entry.key());
            }
        }

        for epoch in epochs_to_remove {
            self.epoch_schedules.remove(&epoch);
            debug!("Removed old epoch schedule: {}", epoch);
        }

        if self.epoch_schedules.len() > MAX_CACHED_EPOCHS {
            let mut cached_epochs: Vec<_> = self.epoch_schedules.iter()
                .map(|entry| *entry.key())
                .collect();
            cached_epochs.sort_unstable();

            let excess = self.epoch_schedules.len().saturating_sub(MAX_CACHED_EPOCHS);
            for i in 0..excess {
                if let Some(&epoch) = cached_epochs.get(i) {
                    if epoch != current_epoch && epoch != current_epoch + 1 {
                        self.epoch_schedules.remove(&epoch);
                    }
                }
            }
        }
    }

    pub async fn is_leader_reliable(&self, leader: &Pubkey, threshold: f64) -> Result<bool> {
        let stats = self.get_leader_performance_stats(leader).await?;
        Ok(stats.performance_score >= threshold)
    }

    pub async fn get_current_leader(&self) -> Result<LeaderInfo> {
        let current_slot = self.current_slot.load(AtomicOrdering::Acquire);
        self.get_leader_at_slot(current_slot).await
    }

    pub fn get_cached_epochs(&self) -> Vec<Epoch> {
        self.epoch_schedules.iter()
            .map(|entry| *entry.key())
            .collect()
    }

    pub async fn force_refresh_epoch(&self, epoch: Epoch) -> Result<()> {
        self.fetch_and_cache_epoch_schedule(epoch).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LeaderStats {
    pub pubkey: Pubkey,
    pub stake: u64,
    pub commission: u8,
    pub last_vote: Option<Slot>,
    pub vote_lag: u64,
    pub total_slots: u64,
    pub upcoming_slots: u64,
    pub recent_slots: Vec<Slot>,
    pub performance_score: f64,
}

#[derive(Debug, Clone)]
pub struct OptimalLeader {
    pub pubkey: Pubkey,
    pub next_slot: Slot,
    pub stake: u64,
    pub commission: u8,
    pub vote_lag: u64,
    pub reliability_score: f64,
    pub mev_score: f64,
    pub combined_score: f64,
}

#[derive(Debug, Clone)]
pub struct EpochTransitionInfo {
    pub current_epoch: Epoch,
    pub next_epoch: Epoch,
    pub current_slot: Slot,
    pub next_epoch_start_slot: Slot,
    pub slots_until_transition: u64,
    pub current_epoch_leaders: usize,
    pub next_epoch_leaders: Option<usize>,
    pub transition_ready: bool,
}

fn calculate_performance_score(stake: u64, vote_lag: u64, commission: u8) -> f64 {
    let stake_score = (stake as f64 / 1_000_000_000_000.0).min(100.0) / 100.0;
    let lag_score = (1.0 / (1.0 + (vote_lag as f64 / 150.0))).max(0.0);
    let commission_score = 1.0 - (commission as f64 / 100.0);
    
    stake_score * 0.4 + lag_score * 0.5 + commission_score * 0.1
}

fn calculate_reliability_score(stake: u64, vote_lag: u64, commission: u8) -> f64 {
    let base_score = calculate_performance_score(stake, vote_lag, commission);
    let lag_penalty = if vote_lag > 150 { 0.5 } else { 1.0 };
    base_score * lag_penalty
}

fn calculate_mev_score(stake: u64, commission: u8, vote_lag: u64) -> f64 {
    let stake_normalized = (stake as f64 / 1_000_000_000_000.0).min(100.0) / 100.0;
    let commission_factor = 1.0 - (commission as f64 / 100.0);
    let lag_factor = if vote_lag < 50 { 1.0 } else if vote_lag < 150 { 0.8 } else { 0.5 };
    
        (stake_normalized * 0.3 + commission_factor * 0.4 + lag_factor * 0.3).max(0.0).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_epoch_calculations() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let predictor = LeaderSchedulePredictor::new(rpc_client).await.unwrap();
        
        let slot = 432000;
        let epoch = predictor.slot_to_epoch(slot);
        let epoch_start = predictor.epoch_to_slot(epoch);
        
        assert!(epoch_start <= slot);
        assert!(predictor.epoch_to_slot(epoch + 1) > slot);
    }

    #[test]
    fn test_performance_calculations() {
        let stake = 10_000_000_000_000;
        let vote_lag = 50;
        let commission = 5;
        
        let perf_score = calculate_performance_score(stake, vote_lag, commission);
        assert!(perf_score > 0.0 && perf_score <= 1.0);
        
        let reliability_score = calculate_reliability_score(stake, vote_lag, commission);
        assert!(reliability_score > 0.0 && reliability_score <= 1.0);
        
        let mev_score = calculate_mev_score(stake, commission, vote_lag);
        assert!(mev_score > 0.0 && mev_score <= 1.0);
    }
}

pub struct LeaderSchedulePredictorBuilder {
    rpc_endpoint: String,
    commitment: CommitmentConfig,
}

impl LeaderSchedulePredictorBuilder {
    pub fn new() -> Self {
        Self {
            rpc_endpoint: "https://api.mainnet-beta.solana.com".to_string(),
            commitment: CommitmentConfig::confirmed(),
        }
    }

    pub fn with_rpc_endpoint(mut self, endpoint: String) -> Self {
        self.rpc_endpoint = endpoint;
        self
    }

    pub fn with_commitment(mut self, commitment: CommitmentConfig) -> Self {
        self.commitment = commitment;
        self
    }

    pub async fn build(self) -> Result<Arc<LeaderSchedulePredictor>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            self.rpc_endpoint,
            self.commitment,
        ));
        
        let predictor = Arc::new(LeaderSchedulePredictor::new(rpc_client).await?);
        predictor.clone().start_background_updater().await;
        
        Ok(predictor)
    }
}

impl Default for LeaderSchedulePredictorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

