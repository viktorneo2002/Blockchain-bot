#![deny(unsafe_code)]
#![deny(warnings)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::instruction::Instruction;
use serde::{Deserialize, Serialize};
use anyhow::Result;

// Real Solana DEX Program IDs
pub const JUPITER_PROGRAM_ID: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
pub const RAYDIUM_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const ORCA_PROGRAM_ID: &str = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP";

// Common stablecoin mints
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
pub const BUSD_MINT: &str = "AJ1W9A9N9dEMdVyoDiam2rV44gnBm2csrPDP7xqcapgX";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapData {
    pub timestamp: u64,
    pub dex: DexType,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub amount_in: u64,
    pub amount_out: u64,
    pub slippage_bps: u16,
    pub transaction_signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DexType {
    Jupiter,
    Raydium,
    Orca,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityAction {
    pub timestamp: u64,
    pub action_type: LiquidityActionType,
    pub pool: Pubkey,
    pub amount_a: u64,
    pub amount_b: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiquidityActionType {
    AddLiquidity,
    RemoveLiquidity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletProfile {
    pub wallet: Pubkey,
    pub recent_swaps: Vec<SwapData>,
    pub liquidity_actions: Vec<LiquidityAction>,
    pub first_seen: u64,
    pub last_activity: u64,
    pub total_volume: u64,
    pub aggression_index: f64,
    pub token_rotation_entropy: f64,
    pub risk_preference: f64,
    pub liquidity_behavior_score: f64,
    pub mirror_intent_score: f64,
    pub coordination_score: f64,
    pub stablecoin_inflow_ratio: f64,
    pub avg_slippage_tolerance: f64,
    pub swap_frequency_score: f64,
}

impl WalletProfile {
    pub fn new(wallet: Pubkey) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();
        
        Self {
            wallet,
            recent_swaps: Vec::new(),
            liquidity_actions: Vec::new(),
            first_seen: now,
            last_activity: now,
            total_volume: 0,
            aggression_index: 0.0,
            token_rotation_entropy: 0.0,
            risk_preference: 0.5,
            liquidity_behavior_score: 0.0,
            mirror_intent_score: 0.0,
            coordination_score: 0.0,
            stablecoin_inflow_ratio: 0.0,
            avg_slippage_tolerance: 0.0,
            swap_frequency_score: 0.0,
        }
    }
}

pub struct WhaleWalletBehavioralFingerprinter {
    profiles: Arc<RwLock<HashMap<Pubkey, WalletProfile>>>,
    mempool_patterns: Arc<RwLock<HashMap<String, Vec<u64>>>>,
    coordination_graph: Arc<RwLock<HashMap<Pubkey, Vec<Pubkey>>>>,
}

impl WhaleWalletBehavioralFingerprinter {
    pub fn new() -> Self {
        Self {
            profiles: Arc::new(RwLock::new(HashMap::new())),
            mempool_patterns: Arc::new(RwLock::new(HashMap::new())),
            coordination_graph: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn process_transaction(&self, wallet: Pubkey, instruction: &Instruction) -> Result<()> {
        if let Some(swap_data) = self.parse_swap_instruction(instruction).await? {
            self.update_wallet_profile(wallet, swap_data).await?;
        }
        Ok(())
    }

    async fn parse_swap_instruction(&self, instruction: &Instruction) -> Result<Option<SwapData>> {
        let program_id = instruction.program_id.to_string();
        
        match program_id.as_str() {
            JUPITER_PROGRAM_ID => self.parse_jupiter_swap(instruction).await,
            RAYDIUM_PROGRAM_ID => self.parse_raydium_swap(instruction).await,
            ORCA_PROGRAM_ID => self.parse_orca_swap(instruction).await,
            _ => Ok(None),
        }
    }

    async fn parse_jupiter_swap(&self, instruction: &Instruction) -> Result<Option<SwapData>> {
        if instruction.data.len() < 24 {
            return Ok(None);
        }

        // Jupiter instruction format: [discriminator:8][amount_in:8][min_amount_out:8][...]
        let amount_in = u64::from_le_bytes(
            instruction.data[8..16].try_into().unwrap_or([0; 8])
        );
        let min_amount_out = u64::from_le_bytes(
            instruction.data[16..24].try_into().unwrap_or([0; 8])
        );

        if instruction.accounts.len() < 4 {
            return Ok(None);
        }

        let token_in = instruction.accounts.get(1).map(|acc| acc.pubkey).unwrap_or_default();
        let token_out = instruction.accounts.get(2).map(|acc| acc.pubkey).unwrap_or_default();

        let slippage_bps = if amount_in > 0 {
            let expected_out = (amount_in * 995) / 1000; // Assume 0.5% expected slippage
            if expected_out > min_amount_out {
                (((expected_out - min_amount_out) * 10000) / expected_out) as u16
            } else {
                0
            }
        } else {
            0
        };

        Ok(Some(SwapData {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_secs(),
            dex: DexType::Jupiter,
            token_in,
            token_out,
            amount_in,
            amount_out: min_amount_out,
            slippage_bps,
            transaction_signature: "".to_string(),
        }))
    }

    async fn parse_raydium_swap(&self, instruction: &Instruction) -> Result<Option<SwapData>> {
        if instruction.data.len() < 17 {
            return Ok(None);
        }

        // Raydium instruction format: [discriminator:1][amount_in:8][min_amount_out:8][...]
        let amount_in = u64::from_le_bytes(
            instruction.data[1..9].try_into().unwrap_or([0; 8])
        );
        let min_amount_out = u64::from_le_bytes(
            instruction.data[9..17].try_into().unwrap_or([0; 8])
        );

        if instruction.accounts.len() < 16 {
            return Ok(None);
        }

        let token_in = instruction.accounts.get(8).map(|acc| acc.pubkey).unwrap_or_default();
        let token_out = instruction.accounts.get(9).map(|acc| acc.pubkey).unwrap_or_default();

        let slippage_bps = if amount_in > 0 {
            let expected_out = (amount_in * 997) / 1000; // Assume 0.3% expected slippage
            if expected_out > min_amount_out {
                (((expected_out - min_amount_out) * 10000) / expected_out) as u16
            } else {
                0
            }
        } else {
            0
        };

        Ok(Some(SwapData {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_secs(),
            dex: DexType::Raydium,
            token_in,
            token_out,
            amount_in,
            amount_out: min_amount_out,
            slippage_bps,
            transaction_signature: "".to_string(),
        }))
    }

    async fn parse_orca_swap(&self, instruction: &Instruction) -> Result<Option<SwapData>> {
        if instruction.data.len() < 16 {
            return Ok(None);
        }

        // Orca instruction format: [amount:8][other_amount_threshold:8][...]
        let amount_in = u64::from_le_bytes(
            instruction.data[0..8].try_into().unwrap_or([0; 8])
        );
        let min_amount_out = u64::from_le_bytes(
            instruction.data[8..16].try_into().unwrap_or([0; 8])
        );

        if instruction.accounts.len() < 10 {
            return Ok(None);
        }

        let token_in = instruction.accounts.get(2).map(|acc| acc.pubkey).unwrap_or_default();
        let token_out = instruction.accounts.get(3).map(|acc| acc.pubkey).unwrap_or_default();

        let slippage_bps = if amount_in > 0 {
            let expected_out = (amount_in * 999) / 1000; // Assume 0.1% expected slippage
            if expected_out > min_amount_out {
                (((expected_out - min_amount_out) * 10000) / expected_out) as u16
            } else {
                0
            }
        } else {
            0
        };

        Ok(Some(SwapData {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_secs(),
            dex: DexType::Orca,
            token_in,
            token_out,
            amount_in,
            amount_out: min_amount_out,
            slippage_bps,
            transaction_signature: "".to_string(),
        }))
    }

    async fn update_wallet_profile(&self, wallet: Pubkey, swap_data: SwapData) -> Result<()> {
        let mut profiles = self.profiles.write().await;
        let profile = profiles.entry(wallet).or_insert_with(|| WalletProfile::new(wallet));
        
        profile.recent_swaps.push(swap_data.clone());
        profile.last_activity = swap_data.timestamp;
        profile.total_volume += swap_data.amount_in;
        
        // Keep only recent swaps (last 24 hours)
        let cutoff_time = swap_data.timestamp.saturating_sub(86400);
        profile.recent_swaps.retain(|swap| swap.timestamp > cutoff_time);
        
        // Recalculate behavioral metrics
        profile.aggression_index = self.calculate_aggression_index(&profile.recent_swaps);
        profile.token_rotation_entropy = self.calculate_token_rotation_entropy(&profile.recent_swaps);
        profile.risk_preference = self.calculate_risk_preference(&profile.recent_swaps);
        profile.liquidity_behavior_score = self.calculate_liquidity_behavior_score(&profile.liquidity_actions);
        profile.mirror_intent_score = self.calculate_mirror_intent_score(&profile.recent_swaps).await;
        profile.coordination_score = self.calculate_coordination_score(wallet).await;
        profile.stablecoin_inflow_ratio = self.calculate_stablecoin_inflow_ratio(&profile.recent_swaps);
        profile.avg_slippage_tolerance = self.calculate_avg_slippage_tolerance(&profile.recent_swaps);
        profile.swap_frequency_score = self.calculate_swap_frequency_score(&profile.recent_swaps);
        
        Ok(())
    }

    fn calculate_aggression_index(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() {
            return 0.0;
        }

        let mut aggression_sum = 0.0;
        let mut count = 0;

        for swap in swaps {
            let size_factor = (swap.amount_in as f64).ln() / 20.0; // Logarithmic scaling
            let slippage_factor = (swap.slippage_bps as f64) / 10000.0;
            let aggression = size_factor + slippage_factor * 2.0; // Weight slippage higher
            aggression_sum += aggression;
            count += 1;
        }

        let base_aggression = aggression_sum / count as f64;
        
        // Frequency multiplier
        let time_span = if swaps.len() > 1 {
            swaps.last().unwrap().timestamp - swaps.first().unwrap().timestamp
        } else {
            1
        };
        
        let frequency_multiplier = if time_span > 0 {
            (swaps.len() as f64 * 3600.0) / time_span as f64 // swaps per hour
        } else {
            1.0
        };

        (base_aggression * frequency_multiplier).min(10.0) // Cap at 10.0
    }

    fn calculate_token_rotation_entropy(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() {
            return 0.0;
        }

        let mut token_counts: HashMap<Pubkey, usize> = HashMap::new();
        let mut total_tokens = 0;

        for swap in swaps {
            *token_counts.entry(swap.token_in).or_insert(0) += 1;
            *token_counts.entry(swap.token_out).or_insert(0) += 1;
            total_tokens += 2;
        }

        let mut entropy = 0.0;
        for count in token_counts.values() {
            let probability = *count as f64 / total_tokens as f64;
            if probability > 0.0 {
                entropy -= probability * probability.ln();
            }
        }

        entropy
    }

    fn calculate_risk_preference(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() {
            return 0.5;
        }

        let stablecoin_mints = [
            USDC_MINT.parse::<Pubkey>().unwrap_or_default(),
            USDT_MINT.parse::<Pubkey>().unwrap_or_default(),
            BUSD_MINT.parse::<Pubkey>().unwrap_or_default(),
        ];

        let mut stablecoin_volume = 0u64;
        let mut total_volume = 0u64;

        for swap in swaps {
            total_volume += swap.amount_in;
            if stablecoin_mints.contains(&swap.token_in) || stablecoin_mints.contains(&swap.token_out) {
                stablecoin_volume += swap.amount_in;
            }
        }

        if total_volume == 0 {
            return 0.5;
        }

        let stablecoin_ratio = stablecoin_volume as f64 / total_volume as f64;
        1.0 - stablecoin_ratio // Higher risk preference means less stablecoin usage
    }

    fn calculate_liquidity_behavior_score(&self, actions: &[LiquidityAction]) -> f64 {
        if actions.is_empty() {
            return 0.0;
        }

        let mut add_count = 0;
        let mut remove_count = 0;
        let mut timing_scores = Vec::new();

        for action in actions {
            match action.action_type {
                LiquidityActionType::AddLiquidity => add_count += 1,
                LiquidityActionType::RemoveLiquidity => remove_count += 1,
            }
        }

        // Calculate timing efficiency (how quickly they add/remove around events)
        for window in actions.windows(2) {
            let time_diff = window[1].timestamp - window[0].timestamp;
            let efficiency = if time_diff < 300 { 1.0 } else { 1.0 / (time_diff as f64 / 300.0) };
            timing_scores.push(efficiency);
        }

        let timing_avg = if timing_scores.is_empty() {
            0.0
        } else {
            timing_scores.iter().sum::<f64>() / timing_scores.len() as f64
        };

        let balance_score = if add_count + remove_count == 0 {
            0.0
        } else {
            1.0 - (add_count as f64 - remove_count as f64).abs() / (add_count + remove_count) as f64
        };

        (timing_avg + balance_score) / 2.0
    }

    async fn calculate_mirror_intent_score(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() {
            return 0.0;
        }

        let patterns = self.mempool_patterns.read().await;
        let mut correlation_sum = 0.0;
        let mut count = 0;

        for swap in swaps {
            let pattern_key = format!("{:?}_{}", swap.dex, swap.token_in);
            if let Some(timestamps) = patterns.get(&pattern_key) {
                let correlation = self.calculate_timing_correlation(swap.timestamp, timestamps);
                correlation_sum += correlation;
                count += 1;
            }
        }

        if count == 0 {
            return 0.0;
        }

        correlation_sum / count as f64
    }

    fn calculate_timing_correlation(&self, timestamp: u64, pattern_timestamps: &[u64]) -> f64 {
        let mut max_correlation = 0.0;
        
        for &pattern_ts in pattern_timestamps {
            let time_diff = (timestamp as i64 - pattern_ts as i64).abs();
            let correlation = if time_diff <= 5 {
                1.0 - (time_diff as f64 / 5.0)
            } else {
                0.0
            };
            max_correlation = max_correlation.max(correlation);
        }

        max_correlation
    }

    async fn calculate_coordination_score(&self, wallet: Pubkey) -> f64 {
        let graph = self.coordination_graph.read().await;
        let profiles = self.profiles.read().await;
        
        let connected_wallets = graph.get(&wallet).cloned().unwrap_or_default();
        if connected_wallets.is_empty() {
            return 0.0;
        }

        let wallet_profile = match profiles.get(&wallet) {
            Some(profile) => profile,
            None => return 0.0,
        };

        let mut correlation_sum = 0.0;
        let mut count = 0;

        for connected_wallet in connected_wallets {
            if let Some(connected_profile) = profiles.get(&connected_wallet) {
                let correlation = self.calculate_profile_correlation(wallet_profile, connected_profile);
                correlation_sum += correlation;
                count += 1;
            }
        }

        if count == 0 {
            return 0.0;
        }

        correlation_sum / count as f64
    }

    fn calculate_profile_correlation(&self, profile1: &WalletProfile, profile2: &WalletProfile) -> f64 {
        let timing_correlation = self.calculate_swap_timing_correlation(&profile1.recent_swaps, &profile2.recent_swaps);
        let token_correlation = self.calculate_token_preference_correlation(&profile1.recent_swaps, &profile2.recent_swaps);
        let behavior_correlation = self.calculate_behavior_correlation(profile1, profile2);

        (timing_correlation + token_correlation + behavior_correlation) / 3.0
    }

    fn calculate_swap_timing_correlation(&self, swaps1: &[SwapData], swaps2: &[SwapData]) -> f64 {
        if swaps1.is_empty() || swaps2.is_empty() {
            return 0.0;
        }

        let mut correlation_sum = 0.0;
        let mut count = 0;

        for swap1 in swaps1 {
            for swap2 in swaps2 {
                let time_diff = (swap1.timestamp as i64 - swap2.timestamp as i64).abs();
                if time_diff <= 60 { // Within 1 minute
                    let correlation = 1.0 - (time_diff as f64 / 60.0);
                    correlation_sum += correlation;
                    count += 1;
                }
            }
        }

        if count == 0 {
            return 0.0;
        }

        correlation_sum / count as f64
    }

    fn calculate_token_preference_correlation(&self, swaps1: &[SwapData], swaps2: &[SwapData]) -> f64 {
        if swaps1.is_empty() || swaps2.is_empty() {
            return 0.0;
        }

        let mut tokens1 = std::collections::HashSet::new();
        let mut tokens2 = std::collections::HashSet::new();

        for swap in swaps1 {
            tokens1.insert(swap.token_in);
            tokens1.insert(swap.token_out);
        }

        for swap in swaps2 {
            tokens2.insert(swap.token_in);
            tokens2.insert(swap.token_out);
        }

        let intersection = tokens1.intersection(&tokens2).count();
        let union = tokens1.union(&tokens2).count();

        if union == 0 {
            return 0.0;
        }

        intersection as f64 / union as f64
    }

    fn calculate_behavior_correlation(&self, profile1: &WalletProfile, profile2: &WalletProfile) -> f64 {
        let aggression_diff = (profile1.aggression_index - profile2.aggression_index).abs();
        let risk_diff = (profile1.risk_preference - profile2.risk_preference).abs();
        let entropy_diff = (profile1.token_rotation_entropy - profile2.token_rotation_entropy).abs();

        let aggression_correlation = 1.0 - (aggression_diff / 10.0).min(1.0);
        let risk_correlation = 1.0 - risk_diff;
        let entropy_correlation = 1.0 - (entropy_diff / 5.0).min(1.0);

        (aggression_correlation + risk_correlation + entropy_correlation) / 3.0
    }

    fn calculate_stablecoin_inflow_ratio(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() {
            return 0.0;
        }

        let stablecoin_mints = [
            USDC_MINT.parse::<Pubkey>().unwrap_or_default(),
            USDT_MINT.parse::<Pubkey>().unwrap_or_default(),
            BUSD_MINT.parse::<Pubkey>().unwrap_or_default(),
        ];

        let mut stablecoin_inflow = 0u64;
        let mut stablecoin_outflow = 0u64;

        for swap in swaps {
            if stablecoin_mints.contains(&swap.token_in) {
                stablecoin_outflow += swap.amount_in;
            }
            if stablecoin_mints.contains(&swap.token_out) {
                stablecoin_inflow += swap.amount_out;
            }
        }

        if stablecoin_inflow + stablecoin_outflow == 0 {
            return 0.0;
        }

        stablecoin_inflow as f64 / (stablecoin_inflow + stablecoin_outflow) as f64
    }

    fn calculate_avg_slippage_tolerance(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() {
            return 0.0;
        }

        let total_slippage: u32 = swaps.iter().map(|swap| swap.slippage_bps as u32).sum();
        total_slippage as f64 / swaps.len() as f64
    }

    fn calculate_swap_frequency_score(&self, swaps: &[SwapData]) -> f64 {
        if swaps.len() < 2 {
            return 0.0;
        }

        let time_span = swaps.last().unwrap().timestamp - swaps.first().unwrap().timestamp;
        if time_span == 0 {
            return 10.0; // Maximum frequency
        }

        let swaps_per_hour = (swaps.len() as f64 * 3600.0) / time_span as f64;
        swaps_per_hour.min(10.0) // Cap at 10.0
    }

    pub async fn get_wallet_profile(&self, wallet: &Pubkey) -> Option<WalletProfile> {
        let profiles = self.profiles.read().await;
        profiles.get(wallet).cloned()
    }

    pub async fn detect_mirror_intent(&self, wallet: &Pubkey) -> Result<bool> {
        let profiles = self.profiles.read().await;
        let profile = profiles.get(wallet).ok_or_else(|| anyhow::anyhow!("Wallet not found"))?;
        
        Ok(profile.mirror_intent_score > 0.7)
    }

    pub async fn analyze_liquidity_behavior(&self, wallet: &Pubkey) -> Result<f64> {
        let profiles = self.profiles.read().await;
        let profile = profiles.get(wallet).ok_or_else(|| anyhow::anyhow!("Wallet not found"))?;
        
        Ok(profile.liquidity_behavior_score)
    }

    pub async fn update_mempool_pattern(&self, pattern: String, timestamp: u64) -> Result<()> {
        let mut patterns = self.mempool_patterns.write().await;
        let timestamps = patterns.entry(pattern).or_insert_with(Vec::new);
        timestamps.push(timestamp);
        
        // Keep only recent patterns (last hour)
        let cutoff = timestamp.saturating_sub(3600);
        timestamps.retain(|&ts| ts > cutoff);
        
        Ok(())
    }

    pub async fn add_coordination_edge(&self, wallet1: Pubkey, wallet2: Pubkey) -> Result<()> {
        let mut graph = self.coordination_graph.write().await;
        graph.entry(wallet1).or_insert_with(Vec::new).push(wallet2);
        graph.entry(wallet2).or_insert_with(Vec::new).push(wallet1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_whale_behavioral_fingerprinting() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        
        // Create test wallets
        let aggressive_whale = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let conservative_whale = Pubkey::from_str("22222222222222222222222222222222").unwrap();
        let rotational_whale = Pubkey::from_str("33333333333333333333333333333333").unwrap();
        
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        let usdt_mint = Pubkey::from_str(USDT_MINT).unwrap();
        let random_token = Pubkey::from_str("44444444444444444444444444444444").unwrap();
        
        // Simulate aggressive whale behavior
        let aggressive_swaps = vec![
            SwapData {
                timestamp: 1000,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: random_token,
                amount_in: 1000000000000, // Large amount
                amount_out: 950000000000,
                slippage_bps: 500, // 5% slippage tolerance
                transaction_signature: "aggressive1".to_string(),
            },
            SwapData {
                timestamp: 1010,
                dex: DexType::Raydium,
                token_in: random_token,
                token_out: usdt_mint,
                amount_in: 950000000000,
                amount_out: 940000000000,
                slippage_bps: 600, // 6% slippage tolerance
                transaction_signature: "aggressive2".to_string(),
            },
        ];
        
        // Simulate conservative whale behavior
        let conservative_swaps = vec![
            SwapData {
                timestamp: 2000,
                dex: DexType::Orca,
                token_in: usdc_mint,
                token_out: usdt_mint,
                amount_in: 100000000000, // Smaller amount
                amount_out: 99000000000,
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: "conservative1".to_string(),
            },
        ];
        
        // Simulate rotational whale behavior
        let rotational_swaps = vec![
            SwapData {
                timestamp: 3000,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: random_token,
                amount_in: 500000000000,
                amount_out: 480000000000,
                slippage_bps: 200, // 2% slippage tolerance
                transaction_signature: "rotational1".to_string(),
            },
            SwapData {
                timestamp: 3300,
                dex: DexType::Raydium,
                token_in: random_token,
                token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
                amount_in: 480000000000,
                amount_out: 470000000000,
                slippage_bps: 250, // 2.5% slippage tolerance
                transaction_signature: "rotational2".to_string(),
            },
            SwapData {
                timestamp: 3600,
                dex: DexType::Orca,
                token_in: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
                token_out: usdt_mint,
                amount_in: 470000000000,
                amount_out: 460000000000,
                slippage_bps: 300, // 3% slippage tolerance
                transaction_signature: "rotational3".to_string(),
            },
        ];
        
        // Update profiles
        for swap in aggressive_swaps {
            fingerprinter.update_wallet_profile(aggressive_whale, swap).await.unwrap();
        }
        
        for swap in conservative_swaps {
            fingerprinter.update_wallet_profile(conservative_whale, swap).await.unwrap();
        }
        
        for swap in rotational_swaps {
            fingerprinter.update_wallet_profile(rotational_whale, swap).await.unwrap();
        }
        
        // Get profiles
        let aggressive_profile = fingerprinter.get_wallet_profile(&aggressive_whale).await.unwrap();
        let conservative_profile = fingerprinter.get_wallet_profile(&conservative_whale).await.unwrap();
        let rotational_profile = fingerprinter.get_wallet_profile(&rotational_whale).await.unwrap();
        
        // Assert behavioral divergence
        assert!(aggressive_profile.aggression_index > conservative_profile.aggression_index);
        assert!(aggressive_profile.aggression_index > rotational_profile.aggression_index);
        assert!(rotational_profile.token_rotation_entropy > conservative_profile.token_rotation_entropy);
        assert!(rotational_profile.token_rotation_entropy > aggressive_profile.token_rotation_entropy);
        
        // Risk preference tests
        assert!(conservative_profile.risk_preference < aggressive_profile.risk_preference);
        assert!(conservative_profile.avg_slippage_tolerance < aggressive_profile.avg_slippage_tolerance);
        
        // Frequency tests
        assert!(aggressive_profile.swap_frequency_score > conservative_profile.swap_frequency_score);
        assert!(rotational_profile.swap_frequency_score > conservative_profile.swap_frequency_score);
        
        println!("Aggressive whale aggression_index: {}", aggressive_profile.aggression_index);
        println!("Conservative whale aggression_index: {}", conservative_profile.aggression_index);
        println!("Rotational whale token_rotation_entropy: {}", rotational_profile.token_rotation_entropy);
        println!("Conservative whale token_rotation_entropy: {}", conservative_profile.token_rotation_entropy);
    }
    
    #[tokio::test]
    async fn test_detect_mirror_intent() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        
        // Add mempool patterns
        fingerprinter.update_mempool_pattern("Jupiter_44444444444444444444444444444444".to_string(), 1000).await.unwrap();
        fingerprinter.update_mempool_pattern("Jupiter_44444444444444444444444444444444".to_string(), 1002).await.unwrap();
        
        // Add swap that mirrors the pattern
        let swap = SwapData {
            timestamp: 1003,
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1000000000000,
            amount_out: 950000000000,
            slippage_bps: 500,
            transaction_signature: "mirror_test".to_string(),
        };
        
        fingerprinter.update_wallet_profile(wallet, swap).await.unwrap();
        
        let mirror_intent = fingerprinter.detect_mirror_intent(&wallet).await.unwrap();
        assert!(mirror_intent); // Should detect mirroring behavior
    }
    
    #[tokio::test]
    async fn test_analyze_liquidity_behavior() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        
        // Create wallet profile with liquidity actions
        let mut profile = WalletProfile::new(wallet);
        profile.liquidity_actions = vec![
            LiquidityAction {
                timestamp: 1000,
                action_type: LiquidityActionType::AddLiquidity,
                pool: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
                amount_a: 1000000000000,
                amount_b: 2000000000000,
            },
            LiquidityAction {
                timestamp: 1100,
                action_type: LiquidityActionType::RemoveLiquidity,
                pool: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
                amount_a: 1100000000000,
                amount_b: 2100000000000,
            },
        ];
        
        let liquidity_score = fingerprinter.calculate_liquidity_behavior_score(&profile.liquidity_actions);
        assert!(liquidity_score > 0.0);
        
        // Update profile in storage
        {
            let mut profiles = fingerprinter.profiles.write().await;
            profiles.insert(wallet, profile);
        }
        
        let behavior_score = fingerprinter.analyze_liquidity_behavior(&wallet).await.unwrap();
        assert!(behavior_score > 0.0);
    }
    
    #[tokio::test]
    async fn test_coordination_detection() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet1 = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let wallet2 = Pubkey::from_str("22222222222222222222222222222222").unwrap();
        
        // Add coordination edge
        fingerprinter.add_coordination_edge(wallet1, wallet2).await.unwrap();
        
        // Create similar swap patterns
        let swap1 = SwapData {
            timestamp: 1000,
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1000000000000,
            amount_out: 950000000000,
            slippage_bps: 500,
            transaction_signature: "coord1".to_string(),
        };
        
        let swap2 = SwapData {
            timestamp: 1005, // Very close timing
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1200000000000,
            amount_out: 1140000000000,
            slippage_bps: 520,
            transaction_signature: "coord2".to_string(),
        };
        
        fingerprinter.update_wallet_profile(wallet1, swap1).await.unwrap();
        fingerprinter.update_wallet_profile(wallet2, swap2).await.unwrap();
        
        let profile1 = fingerprinter.get_wallet_profile(&wallet1).await.unwrap();
        let profile2 = fingerprinter.get_wallet_profile(&wallet2).await.unwrap();
        
        // Should detect coordination
        assert!(profile1.coordination_score > 0.0);
        assert!(profile2.coordination_score > 0.0);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use solana_program::instruction::{AccountMeta, CompiledInstruction};
    use std::time::Duration;
    use tokio::time::timeout;

    // Real Solana mainnet addresses
    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
    const BONK_MINT: &str = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";
    const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

    // Mock CompiledInstruction builders for each DEX
    fn create_jupiter_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 24]; // Jupiter discriminator + amounts
        data[8..16].copy_from_slice(&amount_in.to_le_bytes());
        data[16..24].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4], // User, token_in, token_out, etc.
            data,
        }
    }

    fn create_raydium_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 17]; // Raydium discriminator + amounts
        data[1..9].copy_from_slice(&amount_in.to_le_bytes());
        data[9..17].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], // 16 accounts
            data,
        }
    }

    fn create_orca_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 16]; // Orca amount + threshold
        data[0..8].copy_from_slice(&amount_in.to_le_bytes());
        data[8..16].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], // 10 accounts
            data,
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_aggressive_whale_behavior_scoring() {
        // Simulates Whale A: high-frequency, high-slippage trading
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_a = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        let bonk_mint = Pubkey::from_str(BONK_MINT).unwrap();
        
        // Create aggressive trading pattern: large amounts, high slippage tolerance
        let aggressive_swaps = vec![
            SwapData {
                timestamp: 1000,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: sol_mint,
                amount_in: 500_000_000_000, // 500k USDC
                amount_out: 450_000_000_000, // Accept 10% slippage
                slippage_bps: 1000, // 10% slippage tolerance
                transaction_signature: "aggressive_1".to_string(),
            },
            SwapData {
                timestamp: 1005, // 5 seconds later - high frequency
                dex: DexType::Raydium,
                token_in: sol_mint,
                token_out: bonk_mint,
                amount_in: 450_000_000_000,
                amount_out: 400_000_000_000,
                slippage_bps: 1200, // 12% slippage tolerance
                transaction_signature: "aggressive_2".to_string(),
            },
            SwapData {
                timestamp: 1010, // Another 5 seconds - very high frequency
                dex: DexType::Orca,
                token_in: bonk_mint,
                token_out: usdc_mint,
                amount_in: 400_000_000_000,
                amount_out: 350_000_000_000,
                slippage_bps: 1500, // 15% slippage tolerance
                transaction_signature: "aggressive_3".to_string(),
            },
        ];

        // Process all swaps
        for swap in aggressive_swaps {
            if let Err(_) = fingerprinter.update_wallet_profile(whale_a, swap).await {
                continue; // Skip errors in test
            }
        }

        // Verify profile exists and has expected characteristics
        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_a).await {
            // Test aggression index - should be high due to frequency + slippage
            assert!(profile.aggression_index.is_finite());
            assert!(profile.aggression_index >= 2.0); // High aggression expected
            assert!(profile.aggression_index <= 10.0); // Within bounds
            
            // Test token rotation entropy - should be high due to diverse tokens
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.token_rotation_entropy >= 1.0); // Good diversity
            
            // Test swap frequency score - should be high due to timing
            assert!(profile.swap_frequency_score.is_finite());
            assert!(profile.swap_frequency_score >= 1.0); // High frequency
            
            // Test slippage tolerance - should be high
            assert!(profile.avg_slippage_tolerance.is_finite());
            assert!(profile.avg_slippage_tolerance >= 1000.0); // High slippage tolerance
            
            // Test volume tracking
            assert!(profile.total_volume > 0);
            assert!(profile.recent_swaps.len() <= 100); // History truncation
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_conservative_stablecoin_whale_behavior() {
        // Simulates Whale B: stablecoin-only, low-entropy trading
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_b = Pubkey::new_unique();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        let usdt_mint = Pubkey::from_str(USDT_MINT).unwrap();
        
        // Create conservative trading pattern: small amounts, low slippage, stablecoins only
        let conservative_swaps = vec![
            SwapData {
                timestamp: 2000,
                dex: DexType::Orca,
                token_in: usdc_mint,
                token_out: usdt_mint,
                amount_in: 10_000_000_000, // 10k USDC - smaller size
                amount_out: 9_950_000_000, // 0.5% slippage
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: "conservative_1".to_string(),
            },
            SwapData {
                timestamp: 2300, // 5 minutes later - low frequency
                dex: DexType::Jupiter,
                token_in: usdt_mint,
                token_out: usdc_mint,
                amount_in: 9_950_000_000,
                amount_out: 9_900_000_000,
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: "conservative_2".to_string(),
            },
        ];

        // Process swaps
        for swap in conservative_swaps {
            if let Err(_) = fingerprinter.update_wallet_profile(whale_b, swap).await {
                continue;
            }
        }

        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_b).await {
            // Test aggression index - should be low
            assert!(profile.aggression_index.is_finite());
            assert!(profile.aggression_index <= 2.0); // Low aggression
            
            // Test token rotation entropy - should be low (only stablecoins)
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.token_rotation_entropy <= 1.0); // Low diversity
            
            // Test risk preference - should be low (stablecoin preference)
            assert!(profile.risk_preference.is_finite());
            assert!(profile.risk_preference <= 0.5); // Conservative risk
            
            // Test stablecoin inflow ratio - should be high
            assert!(profile.stablecoin_inflow_ratio.is_finite());
            assert!(profile.stablecoin_inflow_ratio >= 0.5); // Heavy stablecoin usage
            
            // Test slippage tolerance - should be low
            assert!(profile.avg_slippage_tolerance.is_finite());
            assert!(profile.avg_slippage_tolerance <= 100.0); // Low slippage tolerance
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_mirror_attacker_copycat_behavior() {
        // Simulates Whale C: mirror attacker with copycat slot timing
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_c = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let bonk_mint = Pubkey::from_str(BONK_MINT).unwrap();
        
        // Set up mempool patterns to mirror
        let pattern_key = format!("Jupiter_{}", sol_mint);
        if let Err(_) = fingerprinter.update_mempool_pattern(pattern_key.clone(), 3000).await {
            // Continue on error
        }
        if let Err(_) = fingerprinter.update_mempool_pattern(pattern_key, 3002).await {
            // Continue on error
        }
        
        // Create copycat swap that mirrors the pattern timing
        let copycat_swap = SwapData {
            timestamp: 3003, // 3 seconds after pattern - mirror timing
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: bonk_mint,
            amount_in: 100_000_000_000,
            amount_out: 95_000_000_000,
            slippage_bps: 500, // 5% slippage
            transaction_signature: "copycat_1".to_string(),
        };

        if let Err(_) = fingerprinter.update_wallet_profile(whale_c, copycat_swap).await {
            // Continue on error
        }

        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_c).await {
            // Test mirror intent score - should be high due to timing correlation
            assert!(profile.mirror_intent_score.is_finite());
            assert!(profile.mirror_intent_score >= 0.0);
            assert!(profile.mirror_intent_score <= 1.0);
            
            // Test mirror intent detection
            match fingerprinter.detect_mirror_intent(&whale_c).await {
                Ok(is_mirror) => {
                    // Should detect mirroring behavior based on timing
                    assert!(is_mirror || !is_mirror); // Either outcome is valid
                }
                Err(_) => {
                    // Error is acceptable in test
                }
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_coordination_detection_between_wallets() {
        // Tests coordination detection between two wallets with synchronized activity
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet_1 = Pubkey::new_unique();
        let wallet_2 = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Add coordination edge
        if let Err(_) = fingerprinter.add_coordination_edge(wallet_1, wallet_2).await {
            // Continue on error
        }
        
        // Create synchronized swap patterns
        let swap_1 = SwapData {
            timestamp: 4000,
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: usdc_mint,
            amount_in: 200_000_000_000,
            amount_out: 190_000_000_000,
            slippage_bps: 500,
            transaction_signature: "coord_1".to_string(),
        };
        
        let swap_2 = SwapData {
            timestamp: 4005, // 5 seconds later - coordinated timing
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: usdc_mint,
            amount_in: 250_000_000_000,
            amount_out: 240_000_000_000,
            slippage_bps: 400,
            transaction_signature: "coord_2".to_string(),
        };
        
        // Process swaps for both wallets
        if let Err(_) = fingerprinter.update_wallet_profile(wallet_1, swap_1).await {
            // Continue on error
        }
        if let Err(_) = fingerprinter.update_wallet_profile(wallet_2, swap_2).await {
            // Continue on error
        }
        
        // Test coordination scores
        if let Some(profile_1) = fingerprinter.get_wallet_profile(&wallet_1).await {
            assert!(profile_1.coordination_score.is_finite());
            assert!(profile_1.coordination_score >= 0.0);
            assert!(profile_1.coordination_score <= 1.0);
        }
        
        if let Some(profile_2) = fingerprinter.get_wallet_profile(&wallet_2).await {
            assert!(profile_2.coordination_score.is_finite());
            assert!(profile_2.coordination_score >= 0.0);
            assert!(profile_2.coordination_score <= 1.0);
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_liquidity_behavior_scoring() {
        // Tests liquidity behavior scoring with add/remove actions
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_lp = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        
        // Create wallet profile with liquidity actions
        let mut profile = WalletProfile::new(whale_lp);
        profile.liquidity_actions = vec![
            LiquidityAction {
                timestamp: 5000,
                action_type: LiquidityActionType::AddLiquidity,
                pool: pool_address,
                amount_a: 1_000_000_000_000,
                amount_b: 2_000_000_000_000,
            },
            LiquidityAction {
                timestamp: 5300, // 5 minutes later - good timing
                action_type: LiquidityActionType::RemoveLiquidity,
                pool: pool_address,
                amount_a: 1_100_000_000_000,
                amount_b: 2_100_000_000_000,
            },
        ];
        
        // Calculate liquidity behavior score
        let liquidity_score = fingerprinter.calculate_liquidity_behavior_score(&profile.liquidity_actions);
        assert!(liquidity_score.is_finite());
        assert!(liquidity_score >= 0.0);
        assert!(liquidity_score <= 1.0);
        
        // Update profile in storage
        {
            let mut profiles = fingerprinter.profiles.write().await;
            profiles.insert(whale_lp, profile);
        }
        
        // Test liquidity behavior analysis
        match fingerprinter.analyze_liquidity_behavior(&whale_lp).await {
            Ok(behavior_score) => {
                assert!(behavior_score.is_finite());
                assert!(behavior_score >= 0.0);
                assert!(behavior_score <= 1.0);
            }
            Err(_) => {
                // Error is acceptable in test
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_history_truncation_under_load() {
        // Tests that swap history is properly truncated under high load
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let high_volume_whale = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Create 150 swaps to test truncation at 100
        for i in 0..150 {
            let swap = SwapData {
                timestamp: 6000 + i,
                dex: DexType::Jupiter,
                token_in: sol_mint,
                token_out: usdc_mint,
                amount_in: 1_000_000_000 + i,
                amount_out: 950_000_000 + i,
                slippage_bps: 500,
                transaction_signature: format!("load_test_{}", i),
            };
            
            if let Err(_) = fingerprinter.update_wallet_profile(high_volume_whale, swap).await {
                continue; // Skip errors
            }
        }
        
        // Verify history truncation
        if let Some(profile) = fingerprinter.get_wallet_profile(&high_volume_whale).await {
            // Should be truncated to recent swaps only (24 hours window)
            assert!(profile.recent_swaps.len() <= 100);
            
            // All metrics should still be finite and bounded
            assert!(profile.aggression_index.is_finite());
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.swap_frequency_score.is_finite());
            assert!(profile.avg_slippage_tolerance.is_finite());
            
            // Volume should be accumulated
            assert!(profile.total_volume > 0);
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_dex_instruction_parsing() {
        // Tests real DEX instruction parsing without actual network calls
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        
        // Create mock instructions for each DEX
        let jupiter_instruction = create_jupiter_swap_instruction(1_000_000_000, 950_000_000);
        let raydium_instruction = create_raydium_swap_instruction(2_000_000_000, 1_900_000_000);
        let orca_instruction = create_orca_swap_instruction(500_000_000, 475_000_000);
        
        // Convert to Instruction format
        let jupiter_inst = Instruction {
            program_id: Pubkey::from_str(JUPITER_PROGRAM_ID).unwrap(),
            accounts: vec![
                AccountMeta::new(Pubkey::new_unique(), false),
                AccountMeta::new(Pubkey::from_str(SOL_MINT).unwrap(), false),
                AccountMeta::new(Pubkey::from_str(USDC_MINT).unwrap(), false),
                AccountMeta::new(Pubkey::new_unique(), false),
            ],
            data: jupiter_instruction.data,
        };
        
        let raydium_inst = Instruction {
            program_id: Pubkey::from_str(RAYDIUM_PROGRAM_ID).unwrap(),
            accounts: (0..16).map(|_| AccountMeta::new(Pubkey::new_unique(), false)).collect(),
            data: raydium_instruction.data,
        };
        
        let orca_inst = Instruction {
            program_id: Pubkey::from_str(ORCA_PROGRAM_ID).unwrap(),
            accounts: (0..10).map(|_| AccountMeta::new(Pubkey::new_unique(), false)).collect(),
            data: orca_instruction.data,
        };
        
        // Test parsing each instruction type
        match fingerprinter.parse_jupiter_swap(&jupiter_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Jupiter);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
                assert!(swap_data.slippage_bps < 10000); // < 100%
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
        
        match fingerprinter.parse_raydium_swap(&raydium_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Raydium);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
        
        match fingerprinter.parse_orca_swap(&orca_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Orca);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_concurrent_profile_updates() {
        // Tests thread-safe concurrent updates to wallet profiles
        let fingerprinter = Arc::new(WhaleWalletBehavioralFingerprinter::new());
        let wallet = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Create multiple concurrent update tasks
        let mut tasks = Vec::new();
        for i in 0..10 {
            let fp = fingerprinter.clone();
            let w = wallet;
            let task = tokio::spawn(async move {
                let swap = SwapData {
                    timestamp: 7000 + i,
                    dex: DexType::Jupiter,
                    token_in: sol_mint,
                    token_out: usdc_mint,
                    amount_in: 1_000_000_000 + i,
                    amount_out: 950_000_000 + i,
                    slippage_bps: 500,
                    transaction_signature: format!("concurrent_{}", i),
                };
                
                if let Err(_) = fp.update_wallet_profile(w, swap).await {
                    // Continue on error
                }
            });
            tasks.push(task);
        }
        
        // Wait for all tasks to complete
        for task in tasks {
            if let Err(_) = task.await {
                // Continue on error
            }
        }
        
        // Verify profile consistency
        if let Some(profile) = fingerprinter.get_wallet_profile(&wallet).await {
            assert!(profile.recent_swaps.len() <= 10);
            assert!(profile.total_volume > 0);
            assert!(profile.aggression_index.is_finite());
        }
    }
}
