use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::clock::Slot;
use arrayvec::ArrayVec;
use smallvec::SmallVec;
use ahash::AHashMap;
use std::time::{Duration, Instant};
use crossbeam::channel::{bounded, Sender, Receiver};
use rayon::prelude::*;

const TICK_BUFFER_SIZE: usize = 8192;
const QUANTUM_THRESHOLD_BPS: u64 = 3;
const MIN_PROFIT_LAMPORTS: u64 = 1_000_000;
const MAX_SLIPPAGE_BPS: u64 = 50;
const TICK_WINDOW_MS: u64 = 400;
const VOLATILITY_LOOKBACK: usize = 128;
const MICROSTRUCTURE_DEPTH: usize = 32;
const GAS_BUFFER_LAMPORTS: u64 = 5_000_000;
const SLOT_DURATION_MS: u64 = 400;
const MAX_LATENCY_MS: u64 = 50;

#[repr(align(64))]
#[derive(Debug, Clone, Copy)]
pub struct TickData {
    pub timestamp: u64,
    pub slot: Slot,
    pub bid_price: u64,
    pub ask_price: u64,
    pub bid_size: u64,
    pub ask_size: u64,
    pub last_trade_price: u64,
    pub volume_24h: u64,
    pub liquidity_depth: u64,
    pub market_pubkey: Pubkey,
}

#[repr(align(64))]
#[derive(Debug, Clone, Copy)]
pub struct QuantumSignal {
    pub timestamp: u64,
    pub signal_strength: f64,
    pub profit_estimate: u64,
    pub confidence: f64,
    pub entry_price: u64,
    pub exit_price: u64,
    pub size: u64,
    pub gas_estimate: u64,
    pub priority_score: f64,
    pub market_pair: (Pubkey, Pubkey),
}

#[derive(Debug)]
pub struct MicrostructureProfile {
    pub spread_volatility: f64,
    pub order_flow_imbalance: f64,
    pub price_impact_coefficient: f64,
    pub liquidity_resilience: f64,
    pub adverse_selection_cost: f64,
    pub inventory_risk: f64,
}

pub struct TickBuffer {
    data: Box<[TickData; TICK_BUFFER_SIZE]>,
    write_pos: AtomicU64,
    read_pos: AtomicU64,
}

impl TickBuffer {
    fn new() -> Self {
        Self {
            data: Box::new([TickData {
                timestamp: 0,
                slot: 0,
                bid_price: 0,
                ask_price: 0,
                bid_size: 0,
                ask_size: 0,
                last_trade_price: 0,
                volume_24h: 0,
                liquidity_depth: 0,
                market_pubkey: Pubkey::default(),
            }; TICK_BUFFER_SIZE]),
            write_pos: AtomicU64::new(0),
            read_pos: AtomicU64::new(0),
        }
    }

    fn push(&self, tick: TickData) -> bool {
        let write = self.write_pos.load(Ordering::Acquire);
        let read = self.read_pos.load(Ordering::Acquire);
        
        if write.wrapping_sub(read) >= TICK_BUFFER_SIZE as u64 {
            return false;
        }
        
        let index = (write % TICK_BUFFER_SIZE as u64) as usize;
        unsafe {
            std::ptr::write_volatile(&self.data[index] as *const _ as *mut _, tick);
        }
        
        self.write_pos.store(write.wrapping_add(1), Ordering::Release);
        true
    }

    fn read_batch(&self, out: &mut SmallVec<[TickData; 64]>) {
        let read = self.read_pos.load(Ordering::Acquire);
        let write = self.write_pos.load(Ordering::Acquire);
        let available = write.saturating_sub(read).min(64) as usize;
        
        for i in 0..available {
            let index = ((read + i as u64) % TICK_BUFFER_SIZE as u64) as usize;
            let tick = unsafe {
                std::ptr::read_volatile(&self.data[index] as *const _)
            };
            out.push(tick);
        }
        
        if available > 0 {
            self.read_pos.store(read + available as u64, Ordering::Release);
        }
    }
}

pub struct VolatilityEstimator {
    returns: ArrayVec<f64, VOLATILITY_LOOKBACK>,
    ewma_variance: f64,
    lambda: f64,
}

impl VolatilityEstimator {
    fn new() -> Self {
        Self {
            returns: ArrayVec::new(),
            ewma_variance: 0.0,
            lambda: 0.94,
        }
    }

    fn update(&mut self, price: f64, prev_price: f64) {
        if prev_price > 0.0 {
            let ret = (price / prev_price).ln();
            
            if self.returns.len() == VOLATILITY_LOOKBACK {
                self.returns.remove(0);
            }
            self.returns.push(ret);
            
            self.ewma_variance = self.lambda * self.ewma_variance + 
                (1.0 - self.lambda) * ret * ret;
        }
    }

    fn get_volatility(&self) -> f64 {
        self.ewma_variance.sqrt()
    }

    fn get_volatility_percentile(&self, value: f64) -> f64 {
        if self.returns.is_empty() {
            return 0.5;
        }
        
        let mut sorted = self.returns.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let pos = sorted.binary_search_by(|&x| {
            x.partial_cmp(&value).unwrap_or(std::cmp::Ordering::Equal)
        }).unwrap_or_else(|x| x);
        
        pos as f64 / sorted.len() as f64
    }
}

pub struct OrderFlowAnalyzer {
    bid_volume_window: ArrayVec<u64, 64>,
    ask_volume_window: ArrayVec<u64, 64>,
    trade_flow: ArrayVec<i64, 128>,
    microstructure: MicrostructureProfile,
}

impl OrderFlowAnalyzer {
    fn new() -> Self {
        Self {
            bid_volume_window: ArrayVec::new(),
            ask_volume_window: ArrayVec::new(),
            trade_flow: ArrayVec::new(),
            microstructure: MicrostructureProfile {
                spread_volatility: 0.0,
                order_flow_imbalance: 0.0,
                price_impact_coefficient: 0.0,
                liquidity_resilience: 0.0,
                adverse_selection_cost: 0.0,
                inventory_risk: 0.0,
            },
        }
    }

    fn update(&mut self, tick: &TickData) {
        if self.bid_volume_window.len() >= 64 {
            self.bid_volume_window.remove(0);
            self.ask_volume_window.remove(0);
        }
        
        self.bid_volume_window.push(tick.bid_size);
        self.ask_volume_window.push(tick.ask_size);
        
        let bid_sum: u64 = self.bid_volume_window.iter().sum();
        let ask_sum: u64 = self.ask_volume_window.iter().sum();
        let total_volume = bid_sum + ask_sum;
        
        if total_volume > 0 {
            self.microstructure.order_flow_imbalance = 
                (bid_sum as f64 - ask_sum as f64) / total_volume as f64;
        }
        
        let spread = tick.ask_price.saturating_sub(tick.bid_price);
        let mid_price = (tick.bid_price + tick.ask_price) / 2;
        
        if mid_price > 0 {
            let spread_bps = (spread as f64 / mid_price as f64) * 10000.0;
            self.microstructure.spread_volatility = spread_bps;
            
            self.microstructure.liquidity_resilience = 
                (tick.liquidity_depth as f64).ln() / (spread_bps + 1.0);
            
            self.microstructure.price_impact_coefficient = 
                spread_bps / (tick.liquidity_depth as f64 / 1e9).sqrt();
        }
        
        self.microstructure.adverse_selection_cost = 
            self.microstructure.spread_volatility * 0.3 + 
            self.microstructure.order_flow_imbalance.abs() * 0.7;
        
        self.microstructure.inventory_risk = 
            (self.microstructure.order_flow_imbalance.abs() * 
             self.microstructure.spread_volatility).sqrt();
    }

    fn get_directional_pressure(&self) -> f64 {
        self.microstructure.order_flow_imbalance
    }

    fn get_execution_cost(&self, size: u64) -> f64 {
        let normalized_size = size as f64 / 1e9;
        
        self.microstructure.spread_volatility * 0.5 +
        self.microstructure.price_impact_coefficient * normalized_size.sqrt() +
        self.microstructure.adverse_selection_cost * 0.1
    }
}

pub struct QuantumDetector {
    volatility_estimators: AHashMap<Pubkey, VolatilityEstimator>,
    order_flow_analyzers: AHashMap<Pubkey, OrderFlowAnalyzer>,
    correlation_matrix: DashMap<(Pubkey, Pubkey), f64>,
    regime_detector: RegimeDetector,
}

impl QuantumDetector {
    fn new() -> Self {
        Self {
            volatility_estimators: AHashMap::new(),
            order_flow_analyzers: AHashMap::new(),
            correlation_matrix: DashMap::new(),
            regime_detector: RegimeDetector::new(),
        }
    }

    fn analyze_tick(&mut self, tick: &TickData) -> Option<QuantumSignal> {
        let volatility = self.volatility_estimators
            .entry(tick.market_pubkey)
            .or_insert_with(VolatilityEstimator::new);
        
        let mid_price = (tick.bid_price + tick.ask_price) / 2;
        let prev_price = tick.last_trade_price;
        
        if prev_price > 0 {
            volatility.update(mid_price as f64, prev_price as f64);
        }
        
        let order_flow = self.order_flow_analyzers
            .entry(tick.market_pubkey)
            .or_insert_with(OrderFlowAnalyzer::new);
        
        order_flow.update(tick);
        
        let vol = volatility.get_volatility();
        let pressure = order_flow.get_directional_pressure();
        let regime = self.regime_detector.current_regime();
        
        if self.detect_quantum_opportunity(tick, vol, pressure, regime) {
            Some(self.generate_signal(tick, vol, pressure, order_flow))
        } else {
            None
        }
    }

    fn detect_quantum_opportunity(&self, tick: &TickData, volatility: f64, 
                                   pressure: f64, regime: MarketRegime) -> bool {
        let spread = tick.ask_price.saturating_sub(tick.bid_price);
        let mid_price = (tick.bid_price + tick.ask_price) / 2;
        
        if mid_price == 0 {
            return false;
        }
        
        let spread_bps = (spread as f64 / mid_price as f64) * 10000.0;
        
        let volatility_threshold = match regime {
            MarketRegime::HighVolatility => 0.002,
            MarketRegime::Normal => 0.001,
            MarketRegime::LowVolatility => 0.0005,
        };
        
        let pressure_threshold = 0.15;
        let min_spread_bps = QUANTUM_THRESHOLD_BPS as f64;
        
        volatility > volatility_threshold &&
        pressure.abs() > pressure_threshold &&
        spread_bps > min_spread_bps &&
        tick.liquidity_depth > MIN_PROFIT_LAMPORTS * 10
    }

    fn generate_signal(&self, tick: &TickData, volatility: f64, 
                       pressure: f64, order_flow: &OrderFlowAnalyzer) -> QuantumSignal {
        let mid_price = (tick.bid_price + tick.ask_price) / 2;
        let spread = tick.ask_price - tick.bid_price;
        
        let size = self.calculate_optimal_size(tick, volatility);
        let execution_cost = order_flow.get_execution_cost(size);
        
        let (entry_price, exit_price) = if pressure > 0.0 {
            (
                tick.ask_price + (spread as f64 * 0.1) as u64,
                tick.bid_price + (spread as f64 * 0.9) as u64
            )
        } else {
            (
                tick.bid_price - (spread as f64 * 0.1) as u64,
                tick.ask_price - (spread as f64 * 0.9) as u64
            )
        };
        
                let gross_profit = exit_price.saturating_sub(entry_price) * size / 1_000_000_000;
        let gas_estimate = GAS_BUFFER_LAMPORTS + (size / 1_000_000);
        let net_profit = gross_profit.saturating_sub(gas_estimate);
        
        let confidence = self.calculate_confidence(volatility, pressure, execution_cost);
        let priority_score = self.calculate_priority_score(net_profit, confidence, volatility);
        
        QuantumSignal {
            timestamp: tick.timestamp,
            signal_strength: pressure.abs() * volatility * 1000.0,
            profit_estimate: net_profit,
            confidence,
            entry_price,
            exit_price,
            size,
            gas_estimate,
            priority_score,
            market_pair: (tick.market_pubkey, tick.market_pubkey),
        }
    }

    fn calculate_optimal_size(&self, tick: &TickData, volatility: f64) -> u64 {
        let base_size = tick.liquidity_depth / 20;
        let volatility_adjustment = (1.0 - volatility.min(0.05) * 20.0).max(0.1);
        let spread_adjustment = {
            let spread = tick.ask_price - tick.bid_price;
            let mid_price = (tick.bid_price + tick.ask_price) / 2;
            let spread_bps = (spread as f64 / mid_price as f64) * 10000.0;
            (100.0 / spread_bps).min(2.0).max(0.5)
        };
        
        (base_size as f64 * volatility_adjustment * spread_adjustment) as u64
    }

    fn calculate_confidence(&self, volatility: f64, pressure: f64, execution_cost: f64) -> f64 {
        let vol_confidence = (-volatility * 50.0).exp();
        let pressure_confidence = (pressure.abs() * 2.0).tanh();
        let cost_confidence = (-execution_cost / 10.0).exp();
        
        (vol_confidence * 0.3 + pressure_confidence * 0.5 + cost_confidence * 0.2)
            .min(0.99)
            .max(0.01)
    }

    fn calculate_priority_score(&self, profit: u64, confidence: f64, volatility: f64) -> f64 {
        let profit_score = (profit as f64 / 1_000_000.0).ln().max(0.0);
        let urgency_score = volatility * 100.0;
        let risk_adjusted_score = profit_score * confidence;
        
        (risk_adjusted_score * 0.7 + urgency_score * 0.3) * 100.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum MarketRegime {
    HighVolatility,
    Normal,
    LowVolatility,
}

struct RegimeDetector {
    volatility_history: ArrayVec<f64, 256>,
    current_regime: MarketRegime,
    regime_change_threshold: f64,
}

impl RegimeDetector {
    fn new() -> Self {
        Self {
            volatility_history: ArrayVec::new(),
            current_regime: MarketRegime::Normal,
            regime_change_threshold: 0.15,
        }
    }

    fn update(&mut self, volatility: f64) {
        if self.volatility_history.len() >= 256 {
            self.volatility_history.remove(0);
        }
        self.volatility_history.push(volatility);
        
        if self.volatility_history.len() >= 50 {
            let recent_avg = self.volatility_history[self.volatility_history.len()-50..]
                .iter()
                .sum::<f64>() / 50.0;
            
            let long_avg = self.volatility_history.iter().sum::<f64>() / 
                self.volatility_history.len() as f64;
            
            let ratio = recent_avg / long_avg.max(0.0001);
            
            self.current_regime = if ratio > 1.0 + self.regime_change_threshold {
                MarketRegime::HighVolatility
            } else if ratio < 1.0 - self.regime_change_threshold {
                MarketRegime::LowVolatility
            } else {
                MarketRegime::Normal
            };
        }
    }

    fn current_regime(&self) -> MarketRegime {
        self.current_regime
    }
}

pub struct CrossMarketAnalyzer {
    pair_correlations: DashMap<(Pubkey, Pubkey), CorrelationTracker>,
    arbitrage_detector: ArbitrageDetector,
}

impl CrossMarketAnalyzer {
    fn new() -> Self {
        Self {
            pair_correlations: DashMap::new(),
            arbitrage_detector: ArbitrageDetector::new(),
        }
    }

    fn update_correlation(&self, market1: Pubkey, price1: f64, market2: Pubkey, price2: f64) {
        let key = if market1 < market2 {
            (market1, market2)
        } else {
            (market2, market1)
        };
        
        self.pair_correlations
            .entry(key)
            .or_insert_with(CorrelationTracker::new)
            .update(price1, price2);
    }

    fn detect_arbitrage(&mut self, ticks: &[TickData]) -> Vec<QuantumSignal> {
        self.arbitrage_detector.analyze_opportunities(ticks, &self.pair_correlations)
    }
}

struct CorrelationTracker {
    price_pairs: ArrayVec<(f64, f64), 128>,
    correlation: f64,
    cointegration_score: f64,
}

impl CorrelationTracker {
    fn new() -> Self {
        Self {
            price_pairs: ArrayVec::new(),
            correlation: 0.0,
            cointegration_score: 0.0,
        }
    }

    fn update(&mut self, price1: f64, price2: f64) {
        if self.price_pairs.len() >= 128 {
            self.price_pairs.remove(0);
        }
        self.price_pairs.push((price1, price2));
        
        if self.price_pairs.len() >= 30 {
            self.calculate_correlation();
            self.calculate_cointegration();
        }
    }

    fn calculate_correlation(&mut self) {
        let n = self.price_pairs.len() as f64;
        let (sum_x, sum_y, sum_xy, sum_x2, sum_y2) = self.price_pairs.iter()
            .fold((0.0, 0.0, 0.0, 0.0, 0.0), |(sx, sy, sxy, sx2, sy2), &(x, y)| {
                (sx + x, sy + y, sxy + x * y, sx2 + x * x, sy2 + y * y)
            });
        
        let num = n * sum_xy - sum_x * sum_y;
        let den = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)).sqrt();
        
        self.correlation = if den > 0.0 { num / den } else { 0.0 };
    }

    fn calculate_cointegration(&mut self) {
        let returns: ArrayVec<(f64, f64), 127> = self.price_pairs
            .windows(2)
            .map(|w| {
                let r1 = (w[1].0 / w[0].0).ln();
                let r2 = (w[1].1 / w[0].1).ln();
                (r1, r2)
            })
            .collect();
        
        if returns.len() < 20 {
            return;
        }
        
        let mean_diff = returns.iter()
            .map(|(r1, r2)| r1 - r2)
            .sum::<f64>() / returns.len() as f64;
        
        let std_diff = (returns.iter()
            .map(|(r1, r2)| {
                let diff = r1 - r2 - mean_diff;
                diff * diff
            })
            .sum::<f64>() / returns.len() as f64).sqrt();
        
        self.cointegration_score = 1.0 / (1.0 + std_diff * 100.0);
    }
}

struct ArbitrageDetector {
    min_profit_threshold: u64,
    max_position_size: u64,
}

impl ArbitrageDetector {
    fn new() -> Self {
        Self {
            min_profit_threshold: MIN_PROFIT_LAMPORTS,
            max_position_size: 100_000_000_000,
        }
    }

    fn analyze_opportunities(&self, ticks: &[TickData], 
                           correlations: &DashMap<(Pubkey, Pubkey), CorrelationTracker>) -> Vec<QuantumSignal> {
        let mut signals = Vec::new();
        
        for i in 0..ticks.len() {
            for j in i+1..ticks.len() {
                let tick1 = &ticks[i];
                let tick2 = &ticks[j];
                
                if let Some(signal) = self.check_pair_arbitrage(tick1, tick2, correlations) {
                    signals.push(signal);
                }
            }
        }
        
        signals.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score)
            .unwrap_or(std::cmp::Ordering::Equal));
        
        signals.truncate(10);
        signals
    }

    fn check_pair_arbitrage(&self, tick1: &TickData, tick2: &TickData,
                           correlations: &DashMap<(Pubkey, Pubkey), CorrelationTracker>) -> Option<QuantumSignal> {
        let key = if tick1.market_pubkey < tick2.market_pubkey {
            (tick1.market_pubkey, tick2.market_pubkey)
        } else {
            (tick2.market_pubkey, tick1.market_pubkey)
        };
        
        let tracker = correlations.get(&key)?;
        
        if tracker.correlation.abs() < 0.7 || tracker.cointegration_score < 0.6 {
            return None;
        }
        
        let mid1 = (tick1.bid_price + tick1.ask_price) / 2;
        let mid2 = (tick2.bid_price + tick2.ask_price) / 2;
        
        let spread1 = tick1.ask_price - tick1.bid_price;
        let spread2 = tick2.ask_price - tick2.bid_price;
        
        let total_spread_cost = spread1 + spread2;
        let size = (tick1.liquidity_depth.min(tick2.liquidity_depth) / 10)
            .min(self.max_position_size);
        
        let price_diff = mid1.abs_diff(mid2);
        
        if price_diff > total_spread_cost + self.min_profit_threshold {
            let (entry_market, exit_market, entry_price, exit_price) = if mid1 > mid2 {
                (tick2.market_pubkey, tick1.market_pubkey, tick2.ask_price, tick1.bid_price)
            } else {
                (tick1.market_pubkey, tick2.market_pubkey, tick1.ask_price, tick2.bid_price)
            };
            
            let gross_profit = exit_price.saturating_sub(entry_price) * size / 1_000_000_000;
            let gas_estimate = GAS_BUFFER_LAMPORTS * 2;
            let net_profit = gross_profit.saturating_sub(gas_estimate);
            
            if net_profit > self.min_profit_threshold {
                return Some(QuantumSignal {
                    timestamp: tick1.timestamp.max(tick2.timestamp),
                    signal_strength: tracker.correlation * tracker.cointegration_score * 100.0,
                    profit_estimate: net_profit,
                    confidence: tracker.cointegration_score,
                    entry_price,
                    exit_price,
                    size,
                    gas_estimate,
                    priority_score: (net_profit as f64 / 1_000_000.0) * tracker.cointegration_score,
                    market_pair: (entry_market, exit_market),
                });
            }
        }
        
        None
    }
}

pub struct TickDataQuantumAnalyzer {
    tick_buffers: DashMap<Pubkey, Arc<TickBuffer>>,
    quantum_detector: Arc<RwLock<QuantumDetector>>,
    cross_market_analyzer: Arc<RwLock<CrossMarketAnalyzer>>,
    signal_tx: Sender<QuantumSignal>,
    signal_rx: Receiver<QuantumSignal>,
    running: Arc<AtomicBool>,
    last_analysis_time: Arc<RwLock<Instant>>,
}

impl TickDataQuantumAnalyzer {
    pub fn new() -> Self {
        let (signal_tx, signal_rx) = bounded(1024);
        
        Self {
            tick_buffers: DashMap::new(),
            quantum_detector: Arc::new(RwLock::new(QuantumDetector::new())),
            cross_market_analyzer: Arc::new(RwLock::new(CrossMarketAnalyzer::new())),
            signal_tx,
            signal_rx,
            running: Arc::new(AtomicBool::new(true)),
            last_analysis_time: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub fn ingest_tick(&self, tick: TickData) -> Result<(), &'static str> {
        if !self.running.load(Ordering::Acquire) {
            return Err("Analyzer is stopped");
        }
        
        let buffer = self.tick_buffers
            .entry(tick.market_pubkey)
            .or_insert_with(|| Arc::new(TickBuffer::new()));
        
        if !buffer.push(tick) {
            return Err("Buffer full");
        }
        
        Ok(())
    }

    pub fn process_ticks(&self) -> Vec<QuantumSignal> {
        let mut all_signals = Vec::new();
        let mut tick_batch = SmallVec::<[TickData; 64]>::new();
        let mut cross_market_ticks = Vec::new();
        
                for buffer_ref in self.tick_buffers.iter() {
            let market_pubkey = *buffer_ref.key();
            let buffer = buffer_ref.value();
            
            tick_batch.clear();
            buffer.read_batch(&mut tick_batch);
            
            if tick_batch.is_empty() {
                continue;
            }
            
            let detector_signals: Vec<QuantumSignal> = tick_batch
                .par_iter()
                .filter_map(|tick| {
                    let mut detector = self.quantum_detector.write();
                    detector.analyze_tick(tick)
                })
                .collect();
            
            all_signals.extend(detector_signals);
            
            if let Some(latest_tick) = tick_batch.last() {
                cross_market_ticks.push(*latest_tick);
            }
            
            for tick in &tick_batch {
                for other_tick in &cross_market_ticks {
                    if tick.market_pubkey != other_tick.market_pubkey {
                        let mid1 = (tick.bid_price + tick.ask_price) as f64 / 2.0;
                        let mid2 = (other_tick.bid_price + other_tick.ask_price) as f64 / 2.0;
                        
                        self.cross_market_analyzer.read().update_correlation(
                            tick.market_pubkey,
                            mid1,
                            other_tick.market_pubkey,
                            mid2
                        );
                    }
                }
            }
        }
        
        if cross_market_ticks.len() >= 2 {
            let mut analyzer = self.cross_market_analyzer.write();
            let arb_signals = analyzer.detect_arbitrage(&cross_market_ticks);
            all_signals.extend(arb_signals);
        }
        
        all_signals.sort_by(|a, b| {
            b.priority_score.partial_cmp(&a.priority_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        
        let now = Instant::now();
        let mut last_time = self.last_analysis_time.write();
        *last_time = now;
        
        all_signals.into_iter()
            .filter(|signal| {
                signal.profit_estimate > MIN_PROFIT_LAMPORTS &&
                signal.confidence > 0.6 &&
                signal.gas_estimate < signal.profit_estimate / 2
            })
            .take(20)
            .collect()
    }

    pub fn get_signals(&self, max_count: usize) -> Vec<QuantumSignal> {
        let mut signals = Vec::new();
        
        while signals.len() < max_count {
            match self.signal_rx.try_recv() {
                Ok(signal) => signals.push(signal),
                Err(_) => break,
            }
        }
        
        signals
    }

    pub fn run_analysis_loop(&self) {
        let running = self.running.clone();
        let tick_buffers = self.tick_buffers.clone();
        let quantum_detector = self.quantum_detector.clone();
        let cross_market_analyzer = self.cross_market_analyzer.clone();
        let signal_tx = self.signal_tx.clone();
        
        std::thread::spawn(move || {
            while running.load(Ordering::Acquire) {
                let start = Instant::now();
                
                let signals = {
                    let analyzer = TickDataQuantumAnalyzer {
                        tick_buffers: tick_buffers.clone(),
                        quantum_detector: quantum_detector.clone(),
                        cross_market_analyzer: cross_market_analyzer.clone(),
                        signal_tx: signal_tx.clone(),
                        signal_rx: bounded(1).1,
                        running: running.clone(),
                        last_analysis_time: Arc::new(RwLock::new(Instant::now())),
                    };
                    
                    analyzer.process_ticks()
                };
                
                for signal in signals {
                    let _ = signal_tx.try_send(signal);
                }
                
                let elapsed = start.elapsed();
                if elapsed < Duration::from_millis(TICK_WINDOW_MS / 10) {
                    std::thread::sleep(Duration::from_millis(TICK_WINDOW_MS / 10) - elapsed);
                }
            }
        });
    }

    pub fn analyze_market_depth(&self, market: Pubkey) -> Option<MicrostructureProfile> {
        let buffer = self.tick_buffers.get(&market)?;
        let mut tick_batch = SmallVec::<[TickData; 64]>::new();
        buffer.read_batch(&mut tick_batch);
        
        if tick_batch.is_empty() {
            return None;
        }
        
        let detector = self.quantum_detector.read();
        let analyzer = detector.order_flow_analyzers.get(&market)?;
        
        Some(MicrostructureProfile {
            spread_volatility: analyzer.microstructure.spread_volatility,
            order_flow_imbalance: analyzer.microstructure.order_flow_imbalance,
            price_impact_coefficient: analyzer.microstructure.price_impact_coefficient,
            liquidity_resilience: analyzer.microstructure.liquidity_resilience,
            adverse_selection_cost: analyzer.microstructure.adverse_selection_cost,
            inventory_risk: analyzer.microstructure.inventory_risk,
        })
    }

    pub fn get_market_volatility(&self, market: Pubkey) -> Option<f64> {
        let detector = self.quantum_detector.read();
        detector.volatility_estimators.get(&market).map(|v| v.get_volatility())
    }

    pub fn get_correlation(&self, market1: Pubkey, market2: Pubkey) -> Option<f64> {
        let key = if market1 < market2 {
            (market1, market2)
        } else {
            (market2, market1)
        };
        
        self.cross_market_analyzer.read()
            .pair_correlations
            .get(&key)
            .map(|tracker| tracker.correlation)
    }

    pub fn adjust_risk_parameters(&self, max_position_size: u64, min_profit: u64) {
        let mut analyzer = self.cross_market_analyzer.write();
        analyzer.arbitrage_detector.max_position_size = max_position_size;
        analyzer.arbitrage_detector.min_profit_threshold = min_profit;
    }

    pub fn get_stats(&self) -> AnalyzerStats {
        let total_markets = self.tick_buffers.len();
        let detector = self.quantum_detector.read();
        let total_volatility_trackers = detector.volatility_estimators.len();
        let total_correlations = self.cross_market_analyzer.read().pair_correlations.len();
        
        AnalyzerStats {
            active_markets: total_markets,
            volatility_trackers: total_volatility_trackers,
            correlation_pairs: total_correlations,
            last_analysis: *self.last_analysis_time.read(),
        }
    }

    pub fn optimize_for_latency(&self) {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus::get())
            .build_global()
            .unwrap_or_else(|_| {});
    }

    pub fn validate_signal(&self, signal: &QuantumSignal) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let signal_age_ms = now_ms.saturating_sub(signal.timestamp);
        
        if signal_age_ms > MAX_LATENCY_MS {
            return false;
        }
        
        if signal.profit_estimate < MIN_PROFIT_LAMPORTS {
            return false;
        }
        
        if signal.gas_estimate > signal.profit_estimate / 2 {
            return false;
        }
        
        if signal.confidence < 0.6 {
            return false;
        }
        
        let slippage_estimate = (signal.exit_price as f64 - signal.entry_price as f64) * 
            (MAX_SLIPPAGE_BPS as f64 / 10000.0);
        
        let adjusted_profit = signal.profit_estimate.saturating_sub(slippage_estimate as u64);
        
        adjusted_profit > MIN_PROFIT_LAMPORTS
    }

    pub fn emergency_stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn restart(&self) {
        self.running.store(true, Ordering::Release);
        self.run_analysis_loop();
    }

    pub fn clear_buffers(&self) {
        for buffer_ref in self.tick_buffers.iter() {
            let buffer = buffer_ref.value();
            buffer.write_pos.store(0, Ordering::Release);
            buffer.read_pos.store(0, Ordering::Release);
        }
    }

    pub fn prune_old_data(&self, max_age_ms: u64) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let markets_to_remove: Vec<Pubkey> = self.tick_buffers
            .iter()
            .filter_map(|entry| {
                let mut tick_batch = SmallVec::<[TickData; 1]>::new();
                entry.value().read_batch(&mut tick_batch);
                
                if let Some(tick) = tick_batch.first() {
                    if now_ms.saturating_sub(tick.timestamp) > max_age_ms {
                        return Some(*entry.key());
                    }
                }
                None
            })
            .collect();
        
        for market in markets_to_remove {
            self.tick_buffers.remove(&market);
        }
    }
}

#[derive(Debug)]
pub struct AnalyzerStats {
    pub active_markets: usize,
    pub volatility_trackers: usize,
    pub correlation_pairs: usize,
    pub last_analysis: Instant,
}

impl Default for TickDataQuantumAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tick_buffer() {
        let buffer = TickBuffer::new();
        let tick = TickData {
            timestamp: 1000,
            slot: 100,
            bid_price: 99,
            ask_price: 101,
            bid_size: 1000,
            ask_size: 1000,
            last_trade_price: 100,
            volume_24h: 1000000,
            liquidity_depth: 10000000,
            market_pubkey: Pubkey::new_unique(),
        };
        
        assert!(buffer.push(tick));
        
        let mut batch = SmallVec::new();
        buffer.read_batch(&mut batch);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_volatility_estimator() {
        let mut estimator = VolatilityEstimator::new();
        estimator.update(100.0, 99.0);
        estimator.update(101.0, 100.0);
        estimator.update(99.0, 101.0);
        
        let vol = estimator.get_volatility();
        assert!(vol > 0.0);
    }

    #[test]
    fn test_signal_validation() {
        let analyzer = TickDataQuantumAnalyzer::new();
        let signal = QuantumSignal {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            signal_strength: 10.0,
            profit_estimate: 2_000_000,
            confidence: 0.8,
            entry_price: 100,
            exit_price: 102,
            size: 1_000_000_000,
            gas_estimate: 500_000,
            priority_score: 50.0,
            market_pair: (Pubkey::new_unique(), Pubkey::new_unique()),
        };
        
        assert!(analyzer.validate_signal(&signal));
    }
}


