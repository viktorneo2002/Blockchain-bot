use std::collections::{HashMap, BTreeMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    clock::Slot,
    account::Account,
    hash::Hash,
};
use rayon::prelude::*;
use ahash::AHashMap;
use parking_lot::Mutex;
use serde::{Serialize, Deserialize};

const MAX_TREE_DEPTH: usize = 12;
const MAX_BRANCHES_PER_NODE: usize = 8;
const PRUNING_THRESHOLD_MS: u64 = 2000;
const MIN_PROBABILITY_THRESHOLD: f64 = 0.001;
const DECAY_FACTOR: f64 = 0.95;
const MAX_CACHED_STATES: usize = 50000;
const PARALLEL_THRESHOLD: usize = 100;
const CONFIDENCE_BOOST_FACTOR: f64 = 1.15;
const STATE_TRANSITION_WINDOW: usize = 32;
const MAX_PENDING_TRANSITIONS: usize = 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountState {
    pub lamports: u64,
    pub owner: Pubkey,
    pub data_hash: Hash,
    pub executable: bool,
    pub rent_epoch: u64,
    pub last_update_slot: Slot,
}

#[derive(Clone, Debug)]
pub struct StateTransition {
    pub from_state: AccountState,
    pub to_state: AccountState,
    pub transaction_signature: Signature,
    pub slot: Slot,
    pub timestamp: Instant,
    pub gas_used: u64,
    pub priority_fee: u64,
}

#[derive(Clone, Debug)]
pub struct ProbabilisticNode {
    pub state: AccountState,
    pub probability: f64,
    pub confidence: f64,
    pub depth: usize,
    pub last_update: Instant,
    pub children: Vec<Arc<RwLock<ProbabilisticNode>>>,
    pub parent_signature: Option<Signature>,
    pub mev_opportunity_score: f64,
    pub transition_count: u64,
    pub success_rate: f64,
}

#[derive(Debug)]
pub struct TransitionProbability {
    pub from_pattern: u64,
    pub to_pattern: u64,
    pub probability: f64,
    pub sample_count: u64,
    pub avg_profit: i64,
    pub volatility: f64,
}

pub struct ProbabilisticStateTree {
    roots: Arc<RwLock<HashMap<Pubkey, Arc<RwLock<ProbabilisticNode>>>>>,
    transition_history: Arc<Mutex<AHashMap<(Pubkey, u64), Vec<StateTransition>>>>,
    probability_cache: Arc<RwLock<BTreeMap<(u64, u64), TransitionProbability>>>,
    pending_transitions: Arc<Mutex<VecDeque<(Pubkey, StateTransition)>>>,
    state_cache: Arc<RwLock<AHashMap<Hash, AccountState>>>,
    pruning_queue: Arc<Mutex<VecDeque<(Instant, Pubkey, Signature)>>>,
    global_confidence_factor: Arc<RwLock<f64>>,
    slot_history: Arc<RwLock<VecDeque<(Slot, u64)>>>,
}

impl ProbabilisticStateTree {
    pub fn new() -> Self {
        Self {
            roots: Arc::new(RwLock::new(HashMap::new())),
            transition_history: Arc::new(Mutex::new(AHashMap::with_capacity(MAX_CACHED_STATES))),
            probability_cache: Arc::new(RwLock::new(BTreeMap::new())),
            pending_transitions: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_PENDING_TRANSITIONS))),
            state_cache: Arc::new(RwLock::new(AHashMap::with_capacity(MAX_CACHED_STATES))),
            pruning_queue: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_CACHED_STATES))),
            global_confidence_factor: Arc::new(RwLock::new(1.0)),
            slot_history: Arc::new(RwLock::new(VecDeque::with_capacity(STATE_TRANSITION_WINDOW))),
        }
    }

    pub fn insert_state(&self, pubkey: Pubkey, state: AccountState, signature: Signature) -> Result<(), &'static str> {
        let node = Arc::new(RwLock::new(ProbabilisticNode {
            state: state.clone(),
            probability: 1.0,
            confidence: 1.0,
            depth: 0,
            last_update: Instant::now(),
            children: Vec::with_capacity(MAX_BRANCHES_PER_NODE),
            parent_signature: Some(signature),
            mev_opportunity_score: 0.0,
            transition_count: 0,
            success_rate: 1.0,
        }));

        {
            let mut roots = self.roots.write().map_err(|_| "Failed to acquire write lock")?;
            roots.insert(pubkey, node.clone());
        }

        {
            let mut cache = self.state_cache.write().map_err(|_| "Failed to acquire cache lock")?;
            if cache.len() >= MAX_CACHED_STATES {
                cache.clear();
            }
            cache.insert(state.data_hash, state);
        }

        self.schedule_pruning(pubkey, signature);
        Ok(())
    }

    pub fn predict_transitions(&self, pubkey: &Pubkey, depth: usize) -> Vec<(AccountState, f64, f64)> {
        let roots = match self.roots.read() {
            Ok(r) => r,
            Err(_) => return vec![],
        };

        let root = match roots.get(pubkey) {
            Some(r) => r.clone(),
            None => return vec![],
        };

        let mut predictions = Vec::new();
        self.traverse_probabilistic_paths(&root, depth, &mut predictions);

        predictions.sort_by(|a, b| {
            let score_a = a.1 * a.2;
            let score_b = b.1 * b.2;
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        predictions.truncate(MAX_BRANCHES_PER_NODE);
        predictions
    }

    fn traverse_probabilistic_paths(
        &self,
        node: &Arc<RwLock<ProbabilisticNode>>,
        remaining_depth: usize,
        predictions: &mut Vec<(AccountState, f64, f64)>,
    ) {
        if remaining_depth == 0 {
            return;
        }

        let node_read = match node.read() {
            Ok(n) => n,
            Err(_) => return,
        };

        let pattern = self.compute_state_pattern(&node_read.state);
        let transitions = self.get_probable_transitions(pattern);

        for (to_pattern, prob) in transitions {
            if prob.probability < MIN_PROBABILITY_THRESHOLD {
                continue;
            }

            let predicted_state = self.apply_pattern_transition(&node_read.state, to_pattern, &prob);
            let confidence = self.calculate_confidence(&node_read, &prob);
            let mev_score = self.calculate_mev_opportunity(&node_read.state, &predicted_state, confidence);

            predictions.push((predicted_state.clone(), prob.probability * node_read.probability, confidence));

            if node_read.children.len() < MAX_BRANCHES_PER_NODE && remaining_depth > 1 {
                let child_node = Arc::new(RwLock::new(ProbabilisticNode {
                    state: predicted_state,
                    probability: prob.probability * node_read.probability * DECAY_FACTOR,
                    confidence: confidence * node_read.confidence,
                    depth: node_read.depth + 1,
                    last_update: Instant::now(),
                    children: Vec::new(),
                    parent_signature: node_read.parent_signature,
                    mev_opportunity_score: mev_score,
                    transition_count: node_read.transition_count + 1,
                    success_rate: prob.sample_count as f64 / (prob.sample_count + 1) as f64,
                }));

                drop(node_read);
                if let Ok(mut node_write) = node.write() {
                    if node_write.children.len() < MAX_BRANCHES_PER_NODE {
                        node_write.children.push(child_node.clone());
                        self.traverse_probabilistic_paths(&child_node, remaining_depth - 1, predictions);
                    }
                }
            }
        }
    }

    pub fn update_transition(&self, pubkey: Pubkey, transition: StateTransition) -> Result<(), &'static str> {
        let from_pattern = self.compute_state_pattern(&transition.from_state);
        let to_pattern = self.compute_state_pattern(&transition.to_state);

        {
            let mut history = self.transition_history.lock();
            let key = (pubkey, from_pattern);
            let transitions = history.entry(key).or_insert_with(Vec::new);
            if transitions.len() >= STATE_TRANSITION_WINDOW {
                transitions.remove(0);
            }
            transitions.push(transition.clone());
        }

        {
            let mut cache = self.probability_cache.write().map_err(|_| "Failed to acquire probability cache lock")?;
            let key = (from_pattern, to_pattern);
            
            let profit = transition.to_state.lamports as i64 - transition.from_state.lamports as i64 - transition.priority_fee as i64;
            
            match cache.get_mut(&key) {
                Some(prob) => {
                    prob.sample_count += 1;
                    let alpha = 1.0 / (prob.sample_count as f64);
                    prob.probability = (1.0 - alpha) * prob.probability + alpha;
                    prob.avg_profit = ((prob.avg_profit as f64 * (prob.sample_count - 1) as f64 + profit as f64) / prob.sample_count as f64) as i64;
                    
                    let variance = (profit - prob.avg_profit).abs() as f64;
                    prob.volatility = (1.0 - alpha) * prob.volatility + alpha * variance;
                }
                None => {
                    cache.insert(key, TransitionProbability {
                        from_pattern,
                        to_pattern,
                        probability: 1.0,
                        sample_count: 1,
                        avg_profit: profit,
                        volatility: 0.0,
                    });
                }
            }
        }

        self.update_confidence_factor(&transition);
        self.update_slot_history(transition.slot, transition.gas_used);

        Ok(())
    }

    fn compute_state_pattern(&self, state: &AccountState) -> u64 {
        let mut pattern = 0u64;
        pattern |= (state.lamports.min(u64::MAX >> 16) & 0xFFFF) << 48;
        pattern |= ((state.owner.to_bytes()[0] as u64) & 0xFF) << 40;
        pattern |= ((state.data_hash.to_bytes()[0] as u64) & 0xFF) << 32;
        pattern |= (state.executable as u64) << 31;
        pattern |= (state.rent_epoch.min(0x7FFFFFFF) & 0x7FFFFFFF);
        pattern
    }

    fn get_probable_transitions(&self, from_pattern: u64) -> Vec<(u64, TransitionProbability)> {
        let cache = match self.probability_cache.read() {
            Ok(c) => c,
            Err(_) => return vec![],
        };

        cache.range((from_pattern, 0)..(from_pattern + 1, 0))
            .map(|((_, to), prob)| (*to, prob.clone()))
            .filter(|(_, prob)| prob.probability >= MIN_PROBABILITY_THRESHOLD)
            .collect()
    }

    fn apply_pattern_transition(&self, current: &AccountState, to_pattern: u64, prob: &TransitionProbability) -> AccountState {
        let mut new_state = current.clone();
        
        let lamports_delta = ((to_pattern >> 48) & 0xFFFF) as u64;
        let scale = prob.avg_profit.abs() as f64 / 10000.0;
        new_state.lamports = if prob.avg_profit >= 0 {
            current.lamports.saturating_add((lamports_delta as f64 * scale) as u64)
        } else {
            current.lamports.saturating_sub((lamports_delta as f64 * scale) as u64)
        };
        
        new_state.last_update_slot = current.last_update_slot.saturating_add(1);
        new_state
    }

    fn calculate_confidence(&self, node: &ProbabilisticNode, prob: &TransitionProbability) -> f64 {
        let global_conf = *self.global_confidence_factor.read().unwrap_or(&1.0);
        let sample_confidence = (prob.sample_count as f64).ln() / 10.0;
        let volatility_penalty = (-prob.volatility / 10000.0).exp();
        let recency_factor = (-node.last_update.elapsed().as_secs_f64() / 60.0).exp();
        let success_factor = node.success_rate.powf(0.5);
        
        (global_conf * sample_confidence * volatility_penalty * recency_factor * success_factor)
            .min(1.0)
            .max(0.0)
    }

    fn calculate_mev_opportunity(&self, from: &AccountState, to: &AccountState, confidence: f64) -> f64 {
        let lamport_diff = to.lamports as i64 - from.lamports as i64;
        if lamport_diff <= 0 {
            return 0.0;
        }

                let slot_delta = to.last_update_slot.saturating_sub(from.last_update_slot);
        let time_factor = (-(slot_delta as f64) / 32.0).exp();
        
        let base_score = (lamport_diff as f64 / 1_000_000.0) * confidence * time_factor;
        let volatility_bonus = if let Ok(cache) = self.probability_cache.read() {
            cache.values()
                .filter(|p| p.volatility > 5000.0)
                .map(|p| p.volatility / 100000.0)
                .sum::<f64>()
                .min(2.0)
        } else {
            1.0
        };
        
        base_score * (1.0 + volatility_bonus) * CONFIDENCE_BOOST_FACTOR
    }

    fn update_confidence_factor(&self, transition: &StateTransition) {
        let profit = transition.to_state.lamports as i64 - transition.from_state.lamports as i64;
        let efficiency = profit as f64 / (transition.gas_used as f64 + 1.0);
        
        if let Ok(mut factor) = self.global_confidence_factor.write() {
            let alpha = 0.1;
            let boost = if efficiency > 0.0 { 1.02 } else { 0.98 };
            *factor = (*factor * (1.0 - alpha) + boost * alpha).min(2.0).max(0.5);
        }
    }

    fn update_slot_history(&self, slot: Slot, gas_used: u64) {
        if let Ok(mut history) = self.slot_history.write() {
            if history.len() >= STATE_TRANSITION_WINDOW {
                history.pop_front();
            }
            history.push_back((slot, gas_used));
        }
    }

    fn schedule_pruning(&self, pubkey: Pubkey, signature: Signature) {
        let timestamp = Instant::now() + Duration::from_millis(PRUNING_THRESHOLD_MS);
        if let Ok(mut queue) = self.pruning_queue.try_lock() {
            queue.push_back((timestamp, pubkey, signature));
        }
    }

    pub fn prune_stale_branches(&self) -> Result<usize, &'static str> {
        let now = Instant::now();
        let mut pruned_count = 0;
        
        let mut to_prune = Vec::new();
        {
            let mut queue = self.pruning_queue.lock();
            while let Some((timestamp, pubkey, signature)) = queue.front() {
                if *timestamp > now {
                    break;
                }
                to_prune.push((*timestamp, *pubkey, *signature));
                queue.pop_front();
            }
        }
        
        for (_, pubkey, signature) in to_prune {
            if self.prune_branch(&pubkey, &signature)? {
                pruned_count += 1;
            }
        }
        
        self.cleanup_caches()?;
        Ok(pruned_count)
    }

    fn prune_branch(&self, pubkey: &Pubkey, signature: &Signature) -> Result<bool, &'static str> {
        let mut roots = self.roots.write().map_err(|_| "Failed to acquire roots lock")?;
        
        if let Some(root) = roots.get(pubkey) {
            let should_prune = {
                let node = root.read().map_err(|_| "Failed to read node")?;
                node.probability < MIN_PROBABILITY_THRESHOLD || 
                node.last_update.elapsed() > Duration::from_millis(PRUNING_THRESHOLD_MS * 2)
            };
            
            if should_prune {
                roots.remove(pubkey);
                return Ok(true);
            }
            
            let root_clone = root.clone();
            drop(roots);
            self.prune_children(&root_clone, 0)?;
        }
        
        Ok(false)
    }

    fn prune_children(&self, node: &Arc<RwLock<ProbabilisticNode>>, depth: usize) -> Result<(), &'static str> {
        if depth >= MAX_TREE_DEPTH {
            return Ok(());
        }
        
        let mut node_write = node.write().map_err(|_| "Failed to write node")?;
        let now = Instant::now();
        
        node_write.children.retain(|child| {
            if let Ok(child_read) = child.read() {
                child_read.probability >= MIN_PROBABILITY_THRESHOLD &&
                child_read.last_update.elapsed() < Duration::from_millis(PRUNING_THRESHOLD_MS)
            } else {
                false
            }
        });
        
        let children_clone: Vec<_> = node_write.children.iter().cloned().collect();
        drop(node_write);
        
        if children_clone.len() > PARALLEL_THRESHOLD {
            children_clone.par_iter().for_each(|child| {
                let _ = self.prune_children(child, depth + 1);
            });
        } else {
            for child in &children_clone {
                self.prune_children(child, depth + 1)?;
            }
        }
        
        Ok(())
    }

    fn cleanup_caches(&self) -> Result<(), &'static str> {
        {
            let mut history = self.transition_history.lock();
            if history.len() > MAX_CACHED_STATES {
                let to_remove: Vec<_> = history.keys()
                    .take(history.len() / 4)
                    .cloned()
                    .collect();
                for key in to_remove {
                    history.remove(&key);
                }
            }
        }
        
        {
            let mut cache = self.probability_cache.write().map_err(|_| "Failed to acquire cache lock")?;
            if cache.len() > MAX_CACHED_STATES / 2 {
                let threshold = MIN_PROBABILITY_THRESHOLD * 10.0;
                cache.retain(|_, prob| prob.probability >= threshold && prob.sample_count > 5);
            }
        }
        
        {
            let mut state_cache = self.state_cache.write().map_err(|_| "Failed to acquire state cache lock")?;
            if state_cache.len() > MAX_CACHED_STATES {
                state_cache.clear();
            }
        }
        
        Ok(())
    }

    pub fn get_best_opportunities(&self, min_score: f64) -> Vec<(Pubkey, Vec<AccountState>, f64)> {
        let roots = match self.roots.read() {
            Ok(r) => r,
            Err(_) => return vec![],
        };
        
        let mut opportunities = Vec::new();
        
        for (pubkey, root) in roots.iter() {
            let mut path = Vec::new();
            let score = self.find_best_path(root, &mut path, min_score);
            if score >= min_score && !path.is_empty() {
                opportunities.push((*pubkey, path, score));
            }
        }
        
        opportunities.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        opportunities.truncate(10);
        opportunities
    }

    fn find_best_path(&self, node: &Arc<RwLock<ProbabilisticNode>>, path: &mut Vec<AccountState>, min_score: f64) -> f64 {
        let node_read = match node.read() {
            Ok(n) => n,
            Err(_) => return 0.0,
        };
        
        if node_read.mev_opportunity_score < min_score {
            return 0.0;
        }
        
        path.push(node_read.state.clone());
        
        if node_read.children.is_empty() {
            return node_read.mev_opportunity_score;
        }
        
        let mut best_score = node_read.mev_opportunity_score;
        let mut best_child_path = Vec::new();
        
        for child in &node_read.children {
            let mut child_path = Vec::new();
            let child_score = self.find_best_path(child, &mut child_path, min_score);
            if child_score > best_score {
                best_score = child_score;
                best_child_path = child_path;
            }
        }
        
        path.extend(best_child_path);
        best_score
    }

    pub fn process_pending_transitions(&self) -> Result<usize, &'static str> {
        let mut processed = 0;
        let mut pending = self.pending_transitions.lock();
        
        while let Some((pubkey, transition)) = pending.pop_front() {
            self.update_transition(pubkey, transition)?;
            processed += 1;
            
            if processed >= MAX_PENDING_TRANSITIONS / 4 {
                break;
            }
        }
        
        Ok(processed)
    }

    pub fn add_pending_transition(&self, pubkey: Pubkey, transition: StateTransition) -> Result<(), &'static str> {
        let mut pending = self.pending_transitions.lock();
        if pending.len() >= MAX_PENDING_TRANSITIONS {
            pending.pop_front();
        }
        pending.push_back((pubkey, transition));
        Ok(())
    }

    pub fn get_confidence_metrics(&self) -> (f64, f64, usize) {
        let global_confidence = *self.global_confidence_factor.read().unwrap_or(&1.0);
        
        let avg_gas = if let Ok(history) = self.slot_history.read() {
            if history.is_empty() {
                0.0
            } else {
                history.iter().map(|(_, gas)| *gas as f64).sum::<f64>() / history.len() as f64
            }
        } else {
            0.0
        };
        
        let active_branches = if let Ok(roots) = self.roots.read() {
            roots.values()
                .map(|root| {
                    if let Ok(node) = root.read() {
                        self.count_active_branches(&node)
                    } else {
                        0
                    }
                })
                .sum()
        } else {
            0
        };
        
        (global_confidence, avg_gas, active_branches)
    }

    fn count_active_branches(&self, node: &ProbabilisticNode) -> usize {
        if node.probability < MIN_PROBABILITY_THRESHOLD {
            return 0;
        }
        
        1 + node.children.iter()
            .filter_map(|child| child.read().ok())
            .map(|child| self.count_active_branches(&child))
            .sum::<usize>()
    }

    pub fn optimize_memory(&self) -> Result<(), &'static str> {
        self.cleanup_caches()?;
        self.prune_stale_branches()?;
        
        let mut roots = self.roots.write().map_err(|_| "Failed to acquire roots lock")?;
        roots.shrink_to_fit();
        
        Ok(())
    }
}

impl Default for ProbabilisticStateTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn test_state_pattern_computation() {
        let tree = ProbabilisticStateTree::new();
        let state = AccountState {
            lamports: 1000000,
            owner: Pubkey::new_unique(),
            data_hash: Hash::new_unique(),
            executable: false,
            rent_epoch: 100,
            last_update_slot: 1000,
        };
        
        let pattern = tree.compute_state_pattern(&state);
        assert!(pattern > 0);
    }

    #[test]
    fn test_confidence_calculation() {
        let tree = ProbabilisticStateTree::new();
        let node = ProbabilisticNode {
            state: AccountState {
                lamports: 1000000,
                owner: Pubkey::new_unique(),
                data_hash: Hash::new_unique(),
                executable: false,
                rent_epoch: 100,
                last_update_slot: 1000,
            },
            probability: 0.8,
            confidence: 0.9,
            depth: 1,
            last_update: Instant::now(),
            children: vec![],
            parent_signature: None,
            mev_opportunity_score: 0.5,
            transition_count: 10,
            success_rate: 0.85,
        };
        
        let prob = TransitionProbability {
            from_pattern: 1000,
            to_pattern: 2000,
            probability: 0.7,
            sample_count: 50,
            avg_profit: 10000,
            volatility: 1000.0,
        };
        
        let confidence = tree.calculate_confidence(&node, &prob);
        assert!(confidence > 0.0 && confidence <= 1.0);
    }
}

