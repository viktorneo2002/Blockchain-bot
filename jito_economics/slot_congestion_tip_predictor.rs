        let stability_factor = 1.0 - congestion.volatility;
        let trend_clarity = congestion.trend.abs();
        
        let performance_factor = {
            let tracker = self.success_tracker.read().unwrap();
            tracker.current_performance
        };

        (history_factor * 0.3 + stability_factor * 0.3 + trend_clarity * 0.2 + performance_factor * 0.2)
            .clamp(0.1, 0.95)
    }

    fn estimate_success_rate(&self, tip: u64, congestion: &CongestionMetrics) -> f64 {
        let tracker = self.success_tracker.read().unwrap();
        
        let bucket_size = 1_000_000;
        let bucket_key = (tip / bucket_size) * bucket_size;
        
        if let Some(&(successes, attempts)) = tracker.tip_buckets.get(&bucket_key) {
            if attempts > 10 {
                let base_rate = successes as f64 / attempts as f64;
                let congestion_penalty = congestion.congestion_score * 0.2;
                let competition_penalty = congestion.competitive_pressure * 0.15;
                
                return (base_rate - congestion_penalty - competition_penalty).clamp(0.1, 0.9);
            }
        }

        let base_success_rate = 0.6;
        let tip_factor = (tip as f64 / 10_000_000.0).min(1.0) * 0.3;
        let congestion_penalty = congestion.congestion_score * 0.25;
        
        (base_success_rate + tip_factor - congestion_penalty).clamp(0.1, 0.9)
    }

    fn calculate_risk_adjusted_tip(&self, base_tip: u64, confidence: f64, success_rate: f64) -> u64 {
        let risk_tolerance = self.get_dynamic_risk_tolerance();
        let expected_value = base_tip as f64 * success_rate;
        let risk_premium = base_tip as f64 * (1.0 - success_rate) * (1.0 - confidence) * risk_tolerance;
        
        (expected_value + risk_premium).round().clamp(MIN_TIP_LAMPORTS as f64, MAX_TIP_LAMPORTS as f64) as u64
    }

    fn get_dynamic_risk_tolerance(&self) -> f64 {
        let tracker = self.success_tracker.read().unwrap();
        let recent_performance = tracker.current_performance;
        
        if recent_performance > 0.7 {
            0.3
        } else if recent_performance > 0.5 {
            0.5
        } else {
            0.7
        }
    }

    async fn get_slot_leader(&self, slot: Slot) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let leader_schedule = self.rpc_client.get_leader_schedule(Some(slot))?;
        
        if let Some(schedule) = leader_schedule {
            for (pubkey, slots) in schedule {
                if slots.contains(&(slot as usize)) {
                    return Ok(pubkey);
                }
            }
        }
        
        self.rpc_client.get_slot_leader(CommitmentConfig::confirmed())
            .map_err(|e| e.into())
    }

    fn get_leader_adjustment(&self, leader: &Pubkey) -> f64 {
        let stats = self.leader_stats.read().unwrap();
        
        if let Some(leader_stat) = stats.get(leader) {
            if leader_stat.last_updated.elapsed() < Duration::from_hours(1) {
                let congestion_factor = leader_stat.avg_congestion;
                let success_factor = leader_stat.tip_success_rates.values()
                    .fold(0.0, |acc, &rate| acc + rate) / leader_stat.tip_success_rates.len().max(1) as f64;
                
                return 0.7 + congestion_factor * 0.2 + success_factor * 0.1;
            }
        }
        
        1.0
    }

    fn get_time_based_adjustment(&self) -> f64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let hour = ((now / 3600) % 24) as u8;
        let patterns = self.congestion_patterns.read().unwrap();
        
        if let Some(hourly_pattern) = patterns.get(&hour) {
            let current_index = ((now % 3600) / (3600 / PRIORITY_LEVELS as u64)) as usize;
            if current_index < hourly_pattern.len() {
                return hourly_pattern[current_index];
            }
        }
        
        1.0
    }

    fn get_cached_prediction(&self, slot: Slot) -> Option<TipPrediction> {
        let cache = self.prediction_cache.read().unwrap();
        cache.get(&slot).cloned()
    }

    fn cache_prediction(&self, slot: Slot, prediction: TipPrediction) {
        let mut cache = self.prediction_cache.write().unwrap();
        cache.insert(slot, prediction);
        
        if cache.len() > 1000 {
            let min_slot = *cache.keys().min().unwrap();
            cache.remove(&min_slot);
        }
    }

    pub async fn update_slot_metrics(&self, slot: Slot) -> Result<(), Box<dyn std::error::Error>> {
        let block = self.rpc_client.get_block_with_config(
            slot,
            solana_client::rpc_config::RpcBlockConfig {
                encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
                transaction_details: Some(solana_transaction_status::TransactionDetails::Full),
                rewards: Some(false),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        )?;

        let timestamp = block.block_time.unwrap_or_else(|| {
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
        });

        let mut transaction_count = 0u64;
        let mut compute_units_consumed = 0u64;
        let mut total_fee = 0u64;
        let mut successful_tips = Vec::new();
        let mut failed_tips = Vec::new();
        let mut priority_fees = Vec::new();

        if let Some(transactions) = block.transactions {
            transaction_count = transactions.len() as u64;
            
            for tx in transactions {
                if let Some(meta) = tx.meta {
                    total_fee += meta.fee;
                    
                    if let Some(compute_units) = meta.compute_units_consumed {
                        compute_units_consumed += compute_units;
                    }
                    
                    if let Some(inner_instructions) = meta.inner_instructions {
                        for inner in inner_instructions {
                            for instruction in inner.instructions {
                                if self.is_tip_instruction(&instruction) {
                                    let tip_amount = self.extract_tip_amount(&instruction);
                                    if meta.err.is_none() {
                                        successful_tips.push(tip_amount);
                                    } else {
                                        failed_tips.push(tip_amount);
                                    }
                                }
                            }
                        }
                    }
                    
                    if let Some(pre_balances) = meta.pre_balances {
                        if let Some(post_balances) = meta.post_balances {
                            let priority_fee = self.calculate_priority_fee(&pre_balances, &post_balances, meta.fee);
                            if priority_fee > 0 {
                                priority_fees.push(priority_fee);
                            }
                        }
                    }
                }
            }
        }

        let block_time_ms = block.block_height
            .map(|_| SLOT_DURATION_MS)
            .unwrap_or(SLOT_DURATION_MS);

        let leader = self.get_slot_leader(slot).await?;

        let metrics = SlotMetrics {
            slot,
            timestamp,
            transaction_count,
            compute_units_consumed,
            total_fee,
            successful_tips,
            failed_tips,
            block_time_ms,
            leader,
            priority_fees,
        };

        self.store_slot_metrics(metrics);
        self.update_leader_stats(slot, &leader).await?;
        self.update_congestion_patterns(slot);
        
        Ok(())
    }

    fn is_tip_instruction(&self, instruction: &solana_transaction_status::UiInnerInstruction) -> bool {
        instruction.program_id_index == 11
    }

    fn extract_tip_amount(&self, instruction: &solana_transaction_status::UiInnerInstruction) -> u64 {
        if let Ok(data) = bs58::decode(&instruction.data).into_vec() {
            if data.len() >= 8 {
                return u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]));
            }
        }
        0
    }

    fn calculate_priority_fee(&self, pre_balances: &[u64], post_balances: &[u64], total_fee: u64) -> u64 {
        if pre_balances.len() != post_balances.len() || pre_balances.is_empty() {
            return 0;
        }
        
        let fee_payer_diff = pre_balances[0].saturating_sub(post_balances[0]);
        fee_payer_diff.saturating_sub(5000)
    }

    fn store_slot_metrics(&self, metrics: SlotMetrics) {
        let mut history = self.slot_history.write().unwrap();
        
        if history.len() >= MAX_HISTORY_SLOTS {
            history.pop_front();
        }
        
        history.push_back(metrics);
    }

    async fn update_leader_stats(&self, slot: Slot, leader: &Pubkey) -> Result<(), Box<dyn std::error::Error>> {
        let mut stats = self.leader_stats.write().unwrap();
        let history = self.slot_history.read().unwrap();
        
        let leader_slots: Vec<&SlotMetrics> = history.iter()
            .filter(|s| &s.leader == leader)
            .collect();
        
        if leader_slots.is_empty() {
            return Ok(());
        }
        
        let avg_congestion = leader_slots.iter()
            .map(|s| self.calculate_slot_congestion(s))
            .sum::<f64>() / leader_slots.len() as f64;
        
        let mut tip_success_rates = HashMap::new();
        for bucket in (0..=50).map(|i| i * 1_000_000) {
            let mut successes = 0;
            let mut attempts = 0;
            
            for slot_metrics in &leader_slots {
                for &tip in &slot_metrics.successful_tips {
                    if tip >= bucket && tip < bucket + 1_000_000 {
                        successes += 1;
                        attempts += 1;
                    }
                }
                for &tip in &slot_metrics.failed_tips {
                    if tip >= bucket && tip < bucket + 1_000_000 {
                        attempts += 1;
                    }
                }
            }
            
            if attempts > 0 {
                tip_success_rates.insert(bucket, successes as f64 / attempts as f64);
            }
        }
        
        stats.insert(*leader, LeaderStats {
            total_slots: leader_slots.len() as u64,
            avg_congestion,
            tip_success_rates,
            last_updated: Instant::now(),
        });
        
        Ok(())
    }

    fn update_congestion_patterns(&self, slot: Slot) {
        let history = self.slot_history.read().unwrap();
        let mut patterns = self.congestion_patterns.write().unwrap();
        
        if let Some(slot_metrics) = history.iter().find(|s| s.slot == slot) {
            let hour = ((slot_metrics.timestamp / 3600) % 24) as u8;
            let sub_hour_index = ((slot_metrics.timestamp % 3600) / (3600 / PRIORITY_LEVELS as i64)) as usize;
            
            let congestion = self.calculate_slot_congestion(slot_metrics);
            
            if let Some(hourly_pattern) = patterns.get_mut(&hour) {
                if sub_hour_index < hourly_pattern.len() {
                    hourly_pattern[sub_hour_index] = hourly_pattern[sub_hour_index] * 0.9 + congestion * 0.1;
                }
            }
        }
    }

    pub fn record_tip_result(&self, tip_amount: u64, success: bool) {
        let mut tracker = self.success_tracker.write().unwrap();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        
        tracker.recent_attempts.push_back((tip_amount, success, timestamp));
        if tracker.recent_attempts.len() > ADAPTIVE_WINDOW {
            tracker.recent_attempts.pop_front();
        }
        
        let bucket_key = (tip_amount / 1_000_000) * 1_000_000;
        let entry = tracker.tip_buckets.entry(bucket_key).or_insert((0, 0));
        entry.1 += 1;
        if success {
            entry.0 += 1;
        }
        
        let recent_successes = tracker.recent_attempts.iter()
            .filter(|(_, success, _)| *success)
            .count() as f64;
        tracker.current_performance = recent_successes / tracker.recent_attempts.len().max(1) as f64;
        
        self.update_adaptive_parameters();
    }

    fn update_adaptive_parameters(&self) {
        let tracker = self.success_tracker.read().unwrap();
        let mut params = self.adaptive_params.write().unwrap();
        
        let performance_delta = tracker.current_performance - 0.6;
        let adjustment = performance_delta * params.learning_rate;
        
        if tracker.current_performance < 0.5 {
            params.base_multiplier = (params.base_multiplier * (1.0 + adjustment * 2.0)).clamp(0.5, 2.0);
            params.volatility_weight = (params.volatility_weight * 1.1).min(0.5);
            params.competition_weight = (params.competition_weight * 1.15).min(0.5);
        } else if tracker.current_performance > 0.7 {
            params.base_multiplier = (params.base_multiplier * (1.0 + adjustment)).clamp(0.5, 2.0);
            params.volatility_weight = (params.volatility_weight * 0.95).max(0.1);
            params.competition_weight = (params.competition_weight * 0.9).max(0.1);
        }
        
        params.trend_weight = (params.trend_weight + adjustment * 0.5).clamp(0.1, 0.4);
        params.burst_weight = (params.burst_weight + adjustment * 0.3).clamp(0.1, 0.4);
    }

    pub async fn get_optimal_tip_for_transaction(
        &self,
        priority_level: PriorityLevel,
        value_at_risk: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let current_slot = self.rpc_client.get_slot(CommitmentConfig::processed())?;
        let target_slot = current_slot + 2;
        
        let prediction = self.predict_tip(target_slot).await?;
        
        let tip = match priority_level {
            PriorityLevel::Low => prediction.conservative_tip,
            PriorityLevel::Medium => prediction.base_tip,
            PriorityLevel::High => prediction.risk_adjusted_tip,
            PriorityLevel::Critical => prediction.aggressive_tip,
            PriorityLevel::Maximum => {
                let value_based_tip = (value_at_risk as f64 * 0.001).round() as u64;
                prediction.aggressive_tip.max(value_based_tip).min(MAX_TIP_LAMPORTS)
            }
        };
        
        Ok(self.apply_competitive_adjustment(tip, &prediction))
    }

    fn apply_competitive_adjustment(&self, base_tip: u64, prediction: &TipPrediction) -> u64 {
        let history = self.slot_history.read().unwrap();
        
        if let Some(last_slot) = history.back() {
            let recent_max_tip = last_slot.successful_tips.iter()
                .chain(last_slot.priority_fees.iter())
                .max()
                .copied()
                .unwrap_or(0);
            
            if recent_max_tip > base_tip && prediction.confidence < 0.7 {
                let adjustment_factor = 1.0 + (1.0 - prediction.confidence) * 0.2;
                return ((base_tip as f64 * adjustment_factor).round() as u64).min(MAX_TIP_LAMPORTS);
            }
        }
        
        base_tip
    }

    pub async fn start_monitoring(&self, update_interval: Duration) {
        let predictor = Arc::new(self);
        let predictor_clone = predictor.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(update_interval);
            let mut last_processed_slot = 0;
            
            loop {
                interval.tick().await;
                
                if let Ok(current_slot) = predictor_clone.rpc_client.get_slot(CommitmentConfig::confirmed()) {
                    for slot in (last_processed_slot + 1)..=current_slot {
                        if let Err(e) = predictor_clone.update_slot_metrics(slot).await {
                            eprintln!("Failed to update slot {} metrics: {}", slot, e);
                        }
                    }
                    last_processed_slot = current_slot;
                    
                    predictor_clone.cleanup_old_data();
                }
            }
        });
    }

    fn cleanup_old_data(&self) {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let cutoff_time = current_time - 3600;
        
        let mut tracker = self.success_tracker.write().unwrap();
        tracker.recent_attempts.retain(|(_, _, timestamp)| *timestamp > cutoff_time);
        
        let mut cache = self.prediction_cache.write().unwrap();
        if cache.len() > 500 {
            let to_remove: Vec<Slot> = cache.keys()
                .take(cache.len() - 500)
                .copied()
                .collect();
            for slot in to_remove {
                cache.remove(&slot);
            }
        }
        
        let mut leader_stats = self.leader_stats.write().unwrap();
        leader_stats.retain(|_, stats| stats.last_updated.elapsed() < Duration::from_hours(6));
    }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let tracker = self.success_tracker.read().unwrap();
        let history = self.slot_history.read().unwrap();
        let params = self.adaptive_params.read().unwrap();
        
        let total_attempts = tracker.recent_attempts.len();
        let successful_attempts = tracker.recent_attempts.iter()
            .filter(|(_, success, _)| *success)
            .count();
        
        let avg_tip = if total_attempts > 0 {
            tracker.recent_attempts.iter()
                .map(|(tip, _, _)| *tip as f64)
                .sum::<f64>() / total_attempts as f64
        } else {
            0.0
        };
        
        let recent_congestion = if history.len() >= CONGESTION_WINDOW {
            history.iter()
                .rev()
                .take(CONGESTION_WINDOW)
                .map(|slot| self.calculate_slot_congestion(slot))
                .sum::<f64>() / CONGESTION_WINDOW as f64
        } else {
            0.5
        };
        
        PerformanceMetrics {
            success_rate: tracker.current_performance,
            average_tip: avg_tip.round() as u64,
            total_attempts: total_attempts as u64,
            successful_attempts: successful_attempts as u64,
            recent_congestion_level: recent_congestion,
            adaptive_multiplier: params.base_multiplier,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PriorityLevel {
    Low,
    Medium,
    High,
    Critical,
    Maximum,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub success_rate: f64,
    pub average_tip: u64,
    pub total_attempts: u64,
    pub successful_attempts: u64,
    pub recent_congestion_level: f64,
    pub adaptive_multiplier: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tip_prediction() {
        let predictor = SlotCongestionTipPredictor::new("https://api.mainnet-beta.solana.com".to_string());
        
        let result = predictor.predict_tip(123456789).await;
        assert!(result.is_ok());
        
        let prediction = result.unwrap();
        assert!(prediction.base_tip >= MIN_TIP_LAMPORTS);
        assert!(prediction.base_tip <= MAX_TIP_LAMPORTS);
        assert!(prediction.conservative_tip <= prediction.base_tip);
        assert!(prediction.aggressive_tip >= prediction.base_tip);
        assert!(prediction.confidence >= 0.0 && prediction.confidence <= 1.0);
    }

    #[test]
    fn test_congestion_calculation() {
        let predictor = SlotCongestionTipPredictor::new("https://api.mainnet-beta.solana.com".to_string());
        
        let slot_metrics = SlotMetrics {
            slot: 123456789,
            timestamp: 1700000000,
            transaction_count: 2500,
            compute_units_consumed: 24_000_000,
            total_fee: 500_000_000,
            successful_tips: vec![100_000, 200_000, 300_000],
            failed_tips: vec![50_000, 150_000],
            block_time_ms: 400,
            leader: Pubkey::default(),
            priority_fees: vec![10_000, 20_000, 30_000],
        };
        
        let congestion = predictor.calculate_slot_congestion(&slot_metrics);
        assert!(congestion >= 0.0 && congestion <= 1.0);
    }
}
