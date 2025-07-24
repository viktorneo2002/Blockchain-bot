use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::VecDeque;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

const SPEED_OF_LIGHT_MS_PER_KM: f64 = 0.00333564;
const FIBER_OPTIC_REFRACTIVE_INDEX: f64 = 1.467;
const NETWORK_OVERHEAD_FACTOR: f64 = 1.4;
const SLOT_DURATION_MS: u64 = 400;
const SLOTS_PER_EPOCH: u64 = 432000;
const LEADER_SCHEDULE_SLOT_OFFSET: u64 = 4;
const MAX_LATENCY_SAMPLES: usize = 1000;
const OUTLIER_PERCENTILE: f64 = 0.95;
const JITTER_BUFFER_MS: u64 = 5;
const PACKET_PROCESSING_OVERHEAD_US: u64 = 50;
const SOLANA_TRANSACTION_SIZE_BYTES: usize = 1232;
const NETWORK_MTU: usize = 1500;
const TCP_HEADER_SIZE: usize = 20;
const IP_HEADER_SIZE: usize = 20;
const ETHERNET_HEADER_SIZE: usize = 14;
const QUIC_OVERHEAD: usize = 28;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeographicLocation {
    pub latitude: f64,
    pub longitude: f64,
    pub datacenter_id: String,
    pub region: String,
}

#[derive(Debug, Clone)]
pub struct LatencySample {
    pub timestamp: Instant,
    pub rtt_us: u64,
    pub jitter_us: u64,
    pub packet_loss: f64,
}

#[derive(Debug, Clone)]
pub struct NetworkPath {
    pub source: GeographicLocation,
    pub destination: GeographicLocation,
    pub hop_count: u32,
    pub congestion_factor: f64,
}

#[derive(Debug)]
pub struct SpeedOfLightCalculator {
    latency_samples: Arc<RwLock<VecDeque<LatencySample>>>,
    current_slot: Arc<AtomicU64>,
    slot_start_time: Arc<AtomicU64>,
    leader_schedule_cache: Arc<RwLock<Vec<(u64, String)>>>,
    validator_locations: Arc<RwLock<Vec<(String, GeographicLocation)>>>,
    p99_latency_us: Arc<AtomicU64>,
    p95_latency_us: Arc<AtomicU64>,
    median_latency_us: Arc<AtomicU64>,
}

impl SpeedOfLightCalculator {
    pub fn new() -> Self {
        Self {
            latency_samples: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_LATENCY_SAMPLES))),
            current_slot: Arc::new(AtomicU64::new(0)),
            slot_start_time: Arc::new(AtomicU64::new(0)),
            leader_schedule_cache: Arc::new(RwLock::new(Vec::new())),
            validator_locations: Arc::new(RwLock::new(Vec::new())),
            p99_latency_us: Arc::new(AtomicU64::new(0)),
            p95_latency_us: Arc::new(AtomicU64::new(0)),
            median_latency_us: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn calculate_geographic_distance(&self, loc1: &GeographicLocation, loc2: &GeographicLocation) -> f64 {
        const EARTH_RADIUS_KM: f64 = 6371.0;
        
        let lat1_rad = loc1.latitude.to_radians();
        let lat2_rad = loc2.latitude.to_radians();
        let delta_lat = (loc2.latitude - loc1.latitude).to_radians();
        let delta_lon = (loc2.longitude - loc1.longitude).to_radians();

        let a = (delta_lat / 2.0).sin().powi(2) +
                lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        EARTH_RADIUS_KM * c
    }

    pub fn calculate_fiber_latency_us(&self, distance_km: f64, network_path: &NetworkPath) -> u64 {
        let speed_in_fiber = SPEED_OF_LIGHT_MS_PER_KM * FIBER_OPTIC_REFRACTIVE_INDEX;
        let base_latency_ms = distance_km * speed_in_fiber;
        let routing_overhead = 1.0 + (network_path.hop_count as f64 * 0.02);
        let congestion_multiplier = 1.0 + network_path.congestion_factor;
        
        let total_latency_ms = base_latency_ms * NETWORK_OVERHEAD_FACTOR * routing_overhead * congestion_multiplier;
        let total_latency_us = (total_latency_ms * 1000.0) as u64;
        
        total_latency_us + (network_path.hop_count as u64 * PACKET_PROCESSING_OVERHEAD_US)
    }

    pub fn update_latency_sample(&self, sample: LatencySample) {
        let mut samples = self.latency_samples.write();
        
        if samples.len() >= MAX_LATENCY_SAMPLES {
            samples.pop_front();
        }
        
        samples.push_back(sample);
        drop(samples);
        
        self.recalculate_latency_percentiles();
    }

    fn recalculate_latency_percentiles(&self) {
        let samples = self.latency_samples.read();
        if samples.is_empty() {
            return;
        }

        let mut latencies: Vec<u64> = samples.iter().map(|s| s.rtt_us).collect();
        latencies.sort_unstable();

        let len = latencies.len();
        let median_idx = len / 2;
        let p95_idx = ((len as f64) * 0.95) as usize;
        let p99_idx = ((len as f64) * 0.99) as usize;

        self.median_latency_us.store(latencies[median_idx], Ordering::Relaxed);
        self.p95_latency_us.store(latencies[p95_idx.min(len - 1)], Ordering::Relaxed);
        self.p99_latency_us.store(latencies[p99_idx.min(len - 1)], Ordering::Relaxed);
    }

    pub fn calculate_optimal_submission_time(&self, target_slot: u64, transaction_size: usize) -> u64 {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let slot_start = self.slot_start_time.load(Ordering::Relaxed);
        
        if target_slot <= current_slot {
            return 0;
        }

        let slots_ahead = target_slot - current_slot;
        let slot_start_offset = slots_ahead * SLOT_DURATION_MS;
        
        let p95_latency_ms = self.p95_latency_us.load(Ordering::Relaxed) / 1000;
        let transmission_time_us = self.calculate_transmission_time_us(transaction_size);
        let transmission_time_ms = transmission_time_us / 1000;
        
        let leader_rotation_buffer = LEADER_SCHEDULE_SLOT_OFFSET * SLOT_DURATION_MS;
        let safety_margin_ms = JITTER_BUFFER_MS + (p95_latency_ms / 10);
        
        let optimal_offset = slot_start_offset
            .saturating_sub(p95_latency_ms)
            .saturating_sub(transmission_time_ms)
            .saturating_sub(safety_margin_ms)
            .saturating_sub(leader_rotation_buffer);

        slot_start + optimal_offset
    }

    pub fn calculate_transmission_time_us(&self, payload_size: usize) -> u64 {
        let total_packet_size = payload_size + QUIC_OVERHEAD + IP_HEADER_SIZE + ETHERNET_HEADER_SIZE;
        let num_packets = (total_packet_size + NETWORK_MTU - 1) / NETWORK_MTU;
        
        const GIGABIT_BANDWIDTH: u64 = 1_000_000_000;
        const BITS_PER_BYTE: u64 = 8;
        
        let transmission_time_ns = (total_packet_size as u64 * BITS_PER_BYTE * 1_000_000_000) / GIGABIT_BANDWIDTH;
        let packet_overhead_us = num_packets as u64 * PACKET_PROCESSING_OVERHEAD_US;
        
        (transmission_time_ns / 1000) + packet_overhead_us
    }

    pub fn predict_slot_timing(&self, slot: u64) -> (u64, u64) {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let current_slot_start = self.slot_start_time.load(Ordering::Relaxed);
        
        if slot < current_slot {
            return (0, 0);
        }
        
        let slot_diff = slot - current_slot;
        let predicted_start = current_slot_start + (slot_diff * SLOT_DURATION_MS);
        let predicted_end = predicted_start + SLOT_DURATION_MS;
        
        (predicted_start, predicted_end)
    }

    pub fn calculate_leader_distance(&self, leader_pubkey: &str) -> Option<f64> {
        let locations = self.validator_locations.read();
        
        let my_location = locations.iter()
            .find(|(id, _)| id == "self")
            .map(|(_, loc)| loc)?;
            
        let leader_location = locations.iter()
            .find(|(id, _)| id == leader_pubkey)
            .map(|(_, loc)| loc)?;
            
        Some(self.calculate_geographic_distance(my_location, leader_location))
    }

    pub fn estimate_propagation_delay(&self, leader_pubkey: &str) -> u64 {
        let distance_km = match self.calculate_leader_distance(leader_pubkey) {
            Some(d) => d,
            None => {
                return self.p95_latency_us.load(Ordering::Relaxed);
            }
        };

        let default_path = NetworkPath {
            source: GeographicLocation {
                latitude: 0.0,
                longitude: 0.0,
                datacenter_id: "default".to_string(),
                region: "default".to_string(),
            },
            destination: GeographicLocation {
                latitude: 0.0,
                longitude: 0.0,
                datacenter_id: "default".to_string(),
                region: "default".to_string(),
            },
            hop_count: ((distance_km / 1000.0).max(1.0) * 3.0) as u32,
            congestion_factor: 0.1,
        };

        self.calculate_fiber_latency_us(distance_km, &default_path)
    }

    pub fn update_slot_timing(&self, slot: u64, timestamp_ms: u64) {
        self.current_slot.store(slot, Ordering::Relaxed);
        self.slot_start_time.store(timestamp_ms, Ordering::Relaxed);
    }

    pub fn calculate_slot_progress(&self) -> f64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        let slot_start = self.slot_start_time.load(Ordering::Relaxed);
        let elapsed = now.saturating_sub(slot_start);
        
        (elapsed as f64 / SLOT_DURATION_MS as f64).min(1.0)
    }

    pub fn get_slot_remaining_ms(&self) -> u64 {
        let progress = self.calculate_slot_progress();
        let remaining = (1.0 - progress) * SLOT_DURATION_MS as f64;
        remaining as u64
    }

    pub fn should_submit_now(&self, target_slot: u64, priority_multiplier: f64) -> bool {
        let optimal_time = self.calculate_optimal_submission_time(target_slot, SOLANA_TRANSACTION_SIZE_BYTES);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        let adjusted_time = (optimal_time as f64 / priority_multiplier) as u64;
        
        now >= adjusted_time
    }

    pub fn calculate_retry_delay(&self, attempt: u32) -> Duration {
        let base_delay_us = self.median_latency_us.load(Ordering::Relaxed);
        let exponential_factor = 1.5_f64.powi(attempt as i32);
        let jittered_delay = base_delay_us as f64 * exponential_factor;
        
        Duration::from_micros(jittered_delay as u64)
    }

    pub fn estimate_confirmation_time(&self, leader_pubkey: &str) -> u64 {
        let propagation_delay = self.estimate_propagation_delay(leader_pubkey);
        let processing_time = SLOT_DURATION_MS * 1000 / 4;
        let confirmation_slots = 32;
        let confirmation_time = confirmation_slots * SLOT_DURATION_MS * 1000;
        
        propagation_delay + processing_time + confirmation_time
    }

    pub fn get_network_conditions(&self) -> NetworkConditions {
        let samples = self.latency_samples.read();
        let recent_samples: Vec<&LatencySample> = samples.iter()
            .rev()
            .take(100)
            .collect();
            
                    return NetworkConditions {
                avg_latency_us: 0,
                jitter_us: 0,
                packet_loss: 0.0,
                congestion_level: CongestionLevel::Low,
            };
        }
        
        let avg_latency_us = recent_samples.iter()
            .map(|s| s.rtt_us)
            .sum::<u64>() / recent_samples.len() as u64;
            
        let avg_jitter_us = recent_samples.iter()
            .map(|s| s.jitter_us)
            .sum::<u64>() / recent_samples.len() as u64;
            
        let avg_packet_loss = recent_samples.iter()
            .map(|s| s.packet_loss)
            .sum::<f64>() / recent_samples.len() as f64;
            
        let congestion_level = match avg_jitter_us {
            0..=1000 => CongestionLevel::Low,
            1001..=5000 => CongestionLevel::Medium,
            5001..=10000 => CongestionLevel::High,
            _ => CongestionLevel::Critical,
        };
        
        NetworkConditions {
            avg_latency_us,
            jitter_us: avg_jitter_us,
            packet_loss: avg_packet_loss,
            congestion_level,
        }
    }

    pub fn calculate_adaptive_timeout(&self, base_timeout_ms: u64) -> Duration {
        let conditions = self.get_network_conditions();
        let multiplier = match conditions.congestion_level {
            CongestionLevel::Low => 1.0,
            CongestionLevel::Medium => 1.5,
            CongestionLevel::High => 2.0,
            CongestionLevel::Critical => 3.0,
        };
        
        let adaptive_timeout_ms = (base_timeout_ms as f64 * multiplier) as u64;
        Duration::from_millis(adaptive_timeout_ms)
    }

    pub fn predict_next_leader_slots(&self, num_slots: usize) -> Vec<(u64, String)> {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let schedule = self.leader_schedule_cache.read();
        
        schedule.iter()
            .filter(|(slot, _)| *slot > current_slot)
            .take(num_slots)
            .cloned()
            .collect()
    }

    pub fn update_leader_schedule(&self, schedule: Vec<(u64, String)>) {
        let mut cache = self.leader_schedule_cache.write();
        *cache = schedule;
    }

    pub fn update_validator_location(&self, validator_id: String, location: GeographicLocation) {
        let mut locations = self.validator_locations.write();
        locations.retain(|(id, _)| id != &validator_id);
        locations.push((validator_id, location));
    }

    pub fn calculate_slot_boundary_risk(&self, submission_time_ms: u64) -> f64 {
        let slot_progress = self.calculate_slot_progress();
        let time_to_boundary = self.get_slot_remaining_ms();
        let network_latency_ms = self.p95_latency_us.load(Ordering::Relaxed) / 1000;
        
        if time_to_boundary < network_latency_ms {
            return 1.0;
        }
        
        let safety_margin = time_to_boundary.saturating_sub(network_latency_ms);
        let risk_factor = 1.0 - (safety_margin as f64 / SLOT_DURATION_MS as f64);
        
        risk_factor.clamp(0.0, 1.0)
    }

    pub fn estimate_block_production_time(&self, validator_pubkey: &str) -> u64 {
        let base_production_time_us = 150_000;
        let network_delay = self.estimate_propagation_delay(validator_pubkey);
        
        base_production_time_us + network_delay
    }

    pub fn calculate_mev_submission_window(&self, target_slot: u64, mev_type: MevType) -> SubmissionWindow {
        let (slot_start, slot_end) = self.predict_slot_timing(target_slot);
        let network_latency_ms = self.p95_latency_us.load(Ordering::Relaxed) / 1000;
        
        let priority_offset = match mev_type {
            MevType::Arbitrage => 0,
            MevType::Liquidation => network_latency_ms / 4,
            MevType::Sandwich => network_latency_ms / 2,
            MevType::JitLiquidity => network_latency_ms,
        };
        
        let window_start = slot_start + priority_offset;
        let window_end = slot_end.saturating_sub(network_latency_ms * 2);
        
        SubmissionWindow {
            start_ms: window_start,
            end_ms: window_end,
            optimal_ms: window_start + ((window_end - window_start) / 3),
            risk_threshold: 0.7,
        }
    }

    pub fn get_latency_stats(&self) -> LatencyStats {
        LatencyStats {
            p50_us: self.median_latency_us.load(Ordering::Relaxed),
            p95_us: self.p95_latency_us.load(Ordering::Relaxed),
            p99_us: self.p99_latency_us.load(Ordering::Relaxed),
            samples_count: self.latency_samples.read().len(),
        }
    }

    pub fn calculate_competitive_advantage(&self, competitor_latency_us: u64) -> f64 {
        let my_latency = self.median_latency_us.load(Ordering::Relaxed);
        
        if competitor_latency_us == 0 || my_latency >= competitor_latency_us {
            return 0.0;
        }
        
        let advantage_us = competitor_latency_us - my_latency;
        let advantage_ratio = advantage_us as f64 / competitor_latency_us as f64;
        
        advantage_ratio.clamp(0.0, 1.0)
    }

    pub fn estimate_mempool_propagation_time(&self) -> u64 {
        let base_propagation_us = 50_000;
        let network_conditions = self.get_network_conditions();
        
        let congestion_multiplier = match network_conditions.congestion_level {
            CongestionLevel::Low => 1.0,
            CongestionLevel::Medium => 1.3,
            CongestionLevel::High => 1.7,
            CongestionLevel::Critical => 2.5,
        };
        
        (base_propagation_us as f64 * congestion_multiplier) as u64
    }

    pub fn calculate_batch_submission_delay(&self, batch_size: usize) -> Duration {
        let per_tx_overhead_us = 100;
        let network_overhead_us = self.calculate_transmission_time_us(batch_size * SOLANA_TRANSACTION_SIZE_BYTES);
        let total_delay_us = (batch_size as u64 * per_tx_overhead_us) + network_overhead_us;
        
        Duration::from_micros(total_delay_us)
    }

    pub fn should_use_backup_rpc(&self) -> bool {
        let conditions = self.get_network_conditions();
        
        conditions.congestion_level == CongestionLevel::Critical ||
        conditions.packet_loss > 0.05 ||
        conditions.jitter_us > 10000
    }

    pub fn estimate_state_sync_time(&self, state_size_bytes: usize) -> u64 {
        let bandwidth_mbps = 1000.0;
        let bandwidth_bytes_per_ms = (bandwidth_mbps * 1_000_000.0) / (8.0 * 1000.0);
        let base_sync_time_ms = (state_size_bytes as f64 / bandwidth_bytes_per_ms) as u64;
        let network_latency_ms = self.median_latency_us.load(Ordering::Relaxed) / 1000;
        
        base_sync_time_ms + network_latency_ms
    }

    pub fn calculate_failover_delay(&self, primary_region: &str, backup_region: &str) -> u64 {
        let locations = self.validator_locations.read();
        
        let primary_loc = locations.iter()
            .find(|(_, loc)| loc.region == primary_region)
            .map(|(_, loc)| loc);
            
        let backup_loc = locations.iter()
            .find(|(_, loc)| loc.region == backup_region)
            .map(|(_, loc)| loc);
            
        match (primary_loc, backup_loc) {
            (Some(p), Some(b)) => {
                let distance = self.calculate_geographic_distance(p, b);
                let path = NetworkPath {
                    source: p.clone(),
                    destination: b.clone(),
                    hop_count: 5,
                    congestion_factor: 0.2,
                };
                self.calculate_fiber_latency_us(distance, &path)
            },
            _ => 50_000,
        }
    }

    pub fn optimize_parallel_submission_count(&self, total_transactions: usize) -> usize {
        let network_conditions = self.get_network_conditions();
        let base_parallel_count = match network_conditions.congestion_level {
            CongestionLevel::Low => 5,
            CongestionLevel::Medium => 3,
            CongestionLevel::High => 2,
            CongestionLevel::Critical => 1,
        };
        
        base_parallel_count.min(total_transactions)
    }

    pub fn calculate_quic_stream_priority(&self, transaction_value: u64) -> u8 {
        const MAX_PRIORITY: u8 = 255;
        const VALUE_THRESHOLD_HIGH: u64 = 1_000_000_000_000;
        const VALUE_THRESHOLD_MEDIUM: u64 = 100_000_000_000;
        
        match transaction_value {
            v if v >= VALUE_THRESHOLD_HIGH => MAX_PRIORITY,
            v if v >= VALUE_THRESHOLD_MEDIUM => 200,
            _ => 100,
        }
    }

    pub fn estimate_gossip_propagation(&self, hop_count: u32) -> u64 {
        let per_hop_delay_us = 5_000;
        let base_delay_us = self.median_latency_us.load(Ordering::Relaxed);
        
        base_delay_us + (hop_count as u64 * per_hop_delay_us)
    }

    pub fn calculate_dynamic_priority_fee(&self, base_fee: u64, urgency_factor: f64) -> u64 {
        let network_conditions = self.get_network_conditions();
        let congestion_multiplier = match network_conditions.congestion_level {
            CongestionLevel::Low => 1.0,
            CongestionLevel::Medium => 1.5,
            CongestionLevel::High => 2.5,
            CongestionLevel::Critical => 4.0,
        };
        
        let slot_progress = self.calculate_slot_progress();
        let timing_multiplier = if slot_progress > 0.8 { 2.0 } else { 1.0 };
        
        (base_fee as f64 * congestion_multiplier * timing_multiplier * urgency_factor) as u64
    }

    pub fn is_optimal_submission_time(&self) -> bool {
        let slot_progress = self.calculate_slot_progress();
        let network_conditions = self.get_network_conditions();
        
        match network_conditions.congestion_level {
            CongestionLevel::Low => slot_progress >= 0.1 && slot_progress <= 0.7,
            CongestionLevel::Medium => slot_progress >= 0.15 && slot_progress <= 0.6,
            CongestionLevel::High => slot_progress >= 0.2 && slot_progress <= 0.5,
            CongestionLevel::Critical => slot_progress >= 0.25 && slot_progress <= 0.4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CongestionLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
pub struct NetworkConditions {
    pub avg_latency_us: u64,
    pub jitter_us: u64,
    pub packet_loss: f64,
    pub congestion_level: CongestionLevel,
}

#[derive(Debug, Clone, Copy)]
pub enum MevType {
    Arbitrage,
    Liquidation,
    Sandwich,
    JitLiquidity,
}

#[derive(Debug, Clone)]
pub struct SubmissionWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    pub optimal_ms: u64,
    pub risk_threshold: f64,
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub samples_count: usize,
}

impl Default for SpeedOfLightCalculator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geographic_distance_calculation() {
        let calc = SpeedOfLightCalculator::new();
        
        let loc1 = GeographicLocation {
            latitude: 40.7128,
            longitude: -74.0060,
            datacenter_id: "nyc".to_string(),
            region: "us-east".to_string(),
        };
        
        let loc2 = GeographicLocation {
            latitude: 51.5074,
            longitude: -0.1278,
            datacenter_id: "london".to_string(),
            region: "eu-west".to_string(),
        };
        
        let distance = calc.calculate_geographic_distance(&loc1, &loc2);
        assert!(distance > 5500.0 && distance < 5600.0);
    }

    #[test]
    fn test_latency_percentile_calculation() {
        let calc = SpeedOfLightCalculator::new();
        
        for i in 1..=100 {
            let sample = LatencySample {
                timestamp: Instant::now(),
                rtt_us: i * 1000,
                jitter_us: i * 100,
                packet_loss: 0.0,
            };
                        calc.update_latency_sample(sample);
        }
        
        assert_eq!(calc.median_latency_us.load(Ordering::Relaxed), 50000);
        assert_eq!(calc.p95_latency_us.load(Ordering::Relaxed), 95000);
        assert_eq!(calc.p99_latency_us.load(Ordering::Relaxed), 99000);
    }

    #[test]
    fn test_slot_timing_prediction() {
        let calc = SpeedOfLightCalculator::new();
        calc.update_slot_timing(1000, 1_000_000);
        
        let (start, end) = calc.predict_slot_timing(1010);
        assert_eq!(start, 1_000_000 + (10 * SLOT_DURATION_MS));
        assert_eq!(end, start + SLOT_DURATION_MS);
    }

    #[test]
    fn test_optimal_submission_calculation() {
        let calc = SpeedOfLightCalculator::new();
        calc.update_slot_timing(1000, 1_000_000);
        calc.p95_latency_us.store(50_000, Ordering::Relaxed);
        
        let optimal_time = calc.calculate_optimal_submission_time(1005, 1232);
        assert!(optimal_time > 1_000_000);
        assert!(optimal_time < 1_000_000 + (5 * SLOT_DURATION_MS));
    }

    #[test]
    fn test_network_condition_detection() {
        let calc = SpeedOfLightCalculator::new();
        
        for i in 0..50 {
            let sample = LatencySample {
                timestamp: Instant::now(),
                rtt_us: 10_000,
                jitter_us: if i < 25 { 500 } else { 8000 },
                packet_loss: 0.01,
            };
            calc.update_latency_sample(sample);
        }
        
        let conditions = calc.get_network_conditions();
        assert!(conditions.jitter_us > 2000);
        assert!(matches!(conditions.congestion_level, CongestionLevel::Medium | CongestionLevel::High));
    }

    #[test]
    fn test_competitive_advantage_calculation() {
        let calc = SpeedOfLightCalculator::new();
        calc.median_latency_us.store(10_000, Ordering::Relaxed);
        
        let advantage = calc.calculate_competitive_advantage(20_000);
        assert!((advantage - 0.5).abs() < 0.01);
        
        let no_advantage = calc.calculate_competitive_advantage(5_000);
        assert_eq!(no_advantage, 0.0);
    }

    #[test]
    fn test_batch_submission_delay() {
        let calc = SpeedOfLightCalculator::new();
        
        let delay_10 = calc.calculate_batch_submission_delay(10);
        let delay_100 = calc.calculate_batch_submission_delay(100);
        
        assert!(delay_100.as_micros() > delay_10.as_micros());
        assert!(delay_10.as_micros() >= 1000);
    }

    #[test]
    fn test_priority_fee_calculation() {
        let calc = SpeedOfLightCalculator::new();
        
        for i in 0..10 {
            let sample = LatencySample {
                timestamp: Instant::now(),
                rtt_us: 50_000,
                jitter_us: 15_000,
                packet_loss: 0.0,
            };
            calc.update_latency_sample(sample);
        }
        
        let base_fee = 1000;
        let priority_fee = calc.calculate_dynamic_priority_fee(base_fee, 1.5);
        assert!(priority_fee >= base_fee * 3);
    }

    #[test]
    fn test_submission_window_calculation() {
        let calc = SpeedOfLightCalculator::new();
        calc.update_slot_timing(1000, 1_000_000);
        calc.p95_latency_us.store(30_000, Ordering::Relaxed);
        
        let window = calc.calculate_mev_submission_window(1005, MevType::Arbitrage);
        assert!(window.start_ms < window.optimal_ms);
        assert!(window.optimal_ms < window.end_ms);
        assert!(window.end_ms - window.start_ms > 100);
    }

    #[test]
    fn test_failover_calculation() {
        let calc = SpeedOfLightCalculator::new();
        
        calc.update_validator_location(
            "validator1".to_string(),
            GeographicLocation {
                latitude: 40.7128,
                longitude: -74.0060,
                datacenter_id: "nyc".to_string(),
                region: "us-east".to_string(),
            }
        );
        
        calc.update_validator_location(
            "validator2".to_string(),
            GeographicLocation {
                latitude: 37.7749,
                longitude: -122.4194,
                datacenter_id: "sf".to_string(),
                region: "us-west".to_string(),
            }
        );
        
        let failover_delay = calc.calculate_failover_delay("us-east", "us-west");
        assert!(failover_delay > 10_000);
        assert!(failover_delay < 100_000);
    }

    #[test]
    fn test_slot_boundary_risk() {
        let calc = SpeedOfLightCalculator::new();
        calc.update_slot_timing(1000, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 - 350);
        calc.p95_latency_us.store(60_000, Ordering::Relaxed);
        
        let risk = calc.calculate_slot_boundary_risk(0);
        assert!(risk > 0.5);
    }

    #[test]
    fn test_parallel_submission_optimization() {
        let calc = SpeedOfLightCalculator::new();
        
        for _ in 0..20 {
            let sample = LatencySample {
                timestamp: Instant::now(),
                rtt_us: 20_000,
                jitter_us: 2_000,
                packet_loss: 0.0,
            };
            calc.update_latency_sample(sample);
        }
        
        let parallel_count = calc.optimize_parallel_submission_count(10);
        assert!(parallel_count > 0 && parallel_count <= 5);
    }

    #[test]
    fn test_quic_priority_calculation() {
        let calc = SpeedOfLightCalculator::new();
        
        let high_priority = calc.calculate_quic_stream_priority(10_000_000_000_000);
        let medium_priority = calc.calculate_quic_stream_priority(500_000_000_000);
        let low_priority = calc.calculate_quic_stream_priority(1_000_000);
        
        assert_eq!(high_priority, 255);
        assert_eq!(medium_priority, 200);
        assert_eq!(low_priority, 100);
    }
}

