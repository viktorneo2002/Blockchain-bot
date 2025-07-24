use std::sync::{Arc, RwLock, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Mutex};
use solana_sdk::{
    pubkey::Pubkey,
    signature::Keypair,
    transaction::Transaction,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use dashmap::DashMap;
use parking_lot::RwLock as ParkingRwLock;
use crossbeam::channel;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyMetrics {
    pub total_trades: u64,
    pub successful_trades: u64,
    pub failed_trades: u64,
    pub total_profit: i64,
    pub win_rate: f64,
    pub avg_execution_time_us: u64,
    pub last_update: u64,
    pub strategy_score: f64,
}

#[derive(Debug, Clone)]
pub struct StrategyContext {
    pub rpc_client: Arc<RpcClient>,
    pub wallet: Arc<Keypair>,
    pub program_id: Pubkey,
    pub pool_address: Pubkey,
    pub slippage_bps: u16,
    pub priority_fee: u64,
}

#[async_trait::async_trait]
pub trait TradingStrategy: Send + Sync {
    fn strategy_id(&self) -> &str;
    fn version(&self) -> u32;
    
    async fn analyze_opportunity(
        &self,
        context: &StrategyContext,
        market_data: &MarketSnapshot,
    ) -> Result<Option<TradeSignal>, Box<dyn std::error::Error>>;
    
    async fn build_transaction(
        &self,
        context: &StrategyContext,
        signal: &TradeSignal,
    ) -> Result<Transaction, Box<dyn std::error::Error>>;
    
    fn update_metrics(&self, metrics: &mut StrategyMetrics, execution_result: &ExecutionResult);
    
    fn should_exit(&self, current_metrics: &StrategyMetrics) -> bool {
        current_metrics.win_rate < 0.45 || current_metrics.strategy_score < 0.3
    }
    
    fn priority_score(&self, metrics: &StrategyMetrics) -> f64 {
        let recency_weight = 1.0 / (1.0 + (Instant::now().elapsed().as_secs() as f64 / 3600.0));
        metrics.strategy_score * recency_weight * (1.0 + metrics.win_rate)
    }
}

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub timestamp: u64,
    pub best_bid: u64,
    pub best_ask: u64,
    pub mid_price: u64,
    pub volume_24h: u64,
    pub liquidity_depth: u64,
    pub volatility: f64,
}

#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub action: TradeAction,
    pub amount: u64,
    pub expected_profit: i64,
    pub confidence: f64,
    pub max_slippage_bps: u16,
    pub urgency: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TradeAction {
    Buy,
    Sell,
    Arbitrage { path: Vec<Pubkey> },
    LiquidityProvision { range: (u64, u64) },
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub success: bool,
    pub profit: i64,
    pub execution_time_us: u64,
    pub gas_used: u64,
    pub error: Option<String>,
}

pub struct StrategyInstance {
    strategy: Arc<dyn TradingStrategy>,
    metrics: Arc<ParkingRwLock<StrategyMetrics>>,
    active: AtomicBool,
    last_execution: AtomicU64,
    execution_count: AtomicU64,
}

pub struct RuntimeStrategyInjector {
    strategies: Arc<DashMap<String, Arc<StrategyInstance>>>,
    active_strategy: Arc<ParkingRwLock<Option<String>>>,
    context: Arc<StrategyContext>,
    metrics_aggregator: Arc<MetricsAggregator>,
    strategy_selector: Arc<StrategySelector>,
    command_channel: mpsc::Receiver<InjectorCommand>,
    shutdown: Arc<AtomicBool>,
}

struct MetricsAggregator {
    history: Arc<Mutex<VecDeque<(String, StrategyMetrics, u64)>>>,
    performance_threshold: f64,
}

struct StrategySelector {
    selection_mode: SelectionMode,
    rotation_interval: Duration,
    last_rotation: Arc<Mutex<Instant>>,
}

#[derive(Clone)]
enum SelectionMode {
    HighestScore,
    RoundRobin,
    Adaptive,
    Tournament,
}

enum InjectorCommand {
    LoadStrategy {
        strategy: Box<dyn TradingStrategy>,
        response: oneshot::Sender<Result<(), String>>,
    },
    UnloadStrategy {
        strategy_id: String,
        response: oneshot::Sender<Result<(), String>>,
    },
    SwitchStrategy {
        strategy_id: String,
        response: oneshot::Sender<Result<(), String>>,
    },
    UpdateMetrics {
        strategy_id: String,
        result: ExecutionResult,
    },
    GetActiveStrategy {
        response: oneshot::Sender<Option<String>>,
    },
}

impl RuntimeStrategyInjector {
    pub fn new(
        context: StrategyContext,
        command_sender: mpsc::Sender<InjectorCommand>,
    ) -> (Self, mpsc::Receiver<InjectorCommand>) {
        let (tx, rx) = mpsc::channel(1000);
        
        let injector = Self {
            strategies: Arc::new(DashMap::new()),
            active_strategy: Arc::new(ParkingRwLock::new(None)),
            context: Arc::new(context),
            metrics_aggregator: Arc::new(MetricsAggregator {
                history: Arc::new(Mutex::new(VecDeque::with_capacity(10000))),
                performance_threshold: 0.65,
            }),
            strategy_selector: Arc::new(StrategySelector {
                selection_mode: SelectionMode::Adaptive,
                rotation_interval: Duration::from_secs(300),
                last_rotation: Arc::new(Mutex::new(Instant::now())),
            }),
            command_channel: rx,
            shutdown: Arc::new(AtomicBool::new(false)),
        };
        
        (injector, rx)
    }
    
    pub async fn run(mut self) {
        let mut rotation_ticker = tokio::time::interval(Duration::from_secs(60));
        
        loop {
            tokio::select! {
                Some(command) = self.command_channel.recv() => {
                    self.handle_command(command).await;
                }
                _ = rotation_ticker.tick() => {
                    self.check_strategy_rotation().await;
                }
                _ = tokio::signal::ctrl_c() => {
                    self.shutdown.store(true, Ordering::Relaxed);
                    break;
                }
            }
            
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
        }
    }
    
    async fn handle_command(&self, command: InjectorCommand) {
        match command {
            InjectorCommand::LoadStrategy { strategy, response } => {
                let result = self.load_strategy(strategy).await;
                let _ = response.send(result);
            }
            InjectorCommand::UnloadStrategy { strategy_id, response } => {
                let result = self.unload_strategy(&strategy_id).await;
                let _ = response.send(result);
            }
            InjectorCommand::SwitchStrategy { strategy_id, response } => {
                let result = self.switch_strategy(&strategy_id).await;
                let _ = response.send(result);
            }
            InjectorCommand::UpdateMetrics { strategy_id, result } => {
                self.update_strategy_metrics(&strategy_id, result).await;
            }
            InjectorCommand::GetActiveStrategy { response } => {
                let active = self.active_strategy.read().clone();
                let _ = response.send(active);
            }
        }
    }
    
    async fn load_strategy(
        &self,
        strategy: Box<dyn TradingStrategy>,
    ) -> Result<(), String> {
        let strategy_id = strategy.strategy_id().to_string();
        
        if self.strategies.contains_key(&strategy_id) {
            return Err("Strategy already loaded".to_string());
        }
        
        let instance = Arc::new(StrategyInstance {
            strategy: Arc::from(strategy),
            metrics: Arc::new(ParkingRwLock::new(StrategyMetrics {
                total_trades: 0,
                successful_trades: 0,
                failed_trades: 0,
                total_profit: 0,
                win_rate: 0.0,
                avg_execution_time_us: 0,
                last_update: 0,
                strategy_score: 0.5,
            })),
            active: AtomicBool::new(true),
            last_execution: AtomicU64::new(0),
            execution_count: AtomicU64::new(0),
        });
        
        self.strategies.insert(strategy_id.clone(), instance);
        
        if self.active_strategy.read().is_none() {
            *self.active_strategy.write() = Some(strategy_id);
        }
        
        Ok(())
    }
    
    async fn unload_strategy(&self, strategy_id: &str) -> Result<(), String> {
        if let Some((_, instance)) = self.strategies.remove(strategy_id) {
            instance.active.store(false, Ordering::Relaxed);
            
            let mut active = self.active_strategy.write();
            if active.as_ref() == Some(&strategy_id.to_string()) {
                *active = None;
                drop(active);
                self.select_next_strategy().await;
            }
            
            Ok(())
        } else {
            Err("Strategy not found".to_string())
        }
    }
    
    async fn switch_strategy(&self, strategy_id: &str) -> Result<(), String> {
        if !self.strategies.contains_key(strategy_id) {
            return Err("Strategy not found".to_string());
        }
        
        let mut active = self.active_strategy.write();
        
        if let Some(current_id) = &*active {
            if let Some(current) = self.strategies.get(current_id) {
                current.last_execution.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64,
                    Ordering::Relaxed,
                );
            }
        }
        
        *active = Some(strategy_id.to_string());
        Ok(())
    }
    
    async fn update_strategy_metrics(&self, strategy_id: &str, result: ExecutionResult) {
        if let Some(instance) = self.strategies.get(strategy_id) {
            let mut metrics = instance.metrics.write();
            
            instance.strategy.update_metrics(&mut metrics, &result);
            
            metrics.total_trades += 1;
            if result.success {
                metrics.successful_trades += 1;
            } else {
                metrics.failed_trades += 1;
            }
            
            metrics.total_profit += result.profit;
            metrics.win_rate = metrics.successful_trades as f64 / metrics.total_trades as f64;
            
            let count = instance.execution_count.fetch_add(1, Ordering::Relaxed) + 1;
            metrics.avg_execution_time_us = 
                (metrics.avg_execution_time_us * (count - 1) + result.execution_time_us) / count;
            
            metrics.last_update = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            self.calculate_strategy_score(&mut metrics);
            
            let history_entry = (strategy_id.to_string(), metrics.clone(), metrics.last_update);
            let mut history = self.metrics_aggregator.history.lock().await;
            history.push_back(history_entry);
            if history.len() > 10000 {
                history.pop_front();
            }
        }
    }
    
    fn calculate_strategy_score(&self, metrics: &mut StrategyMetrics) {
        let profit_factor = if metrics.total_trades > 0 {
            (metrics.total_profit as f64 / metrics.total_trades as f64).max(0.0)
        } else {
            0.0
        };
        
        let consistency_factor = 1.0 - (metrics.failed_trades as f64 / metrics.total_trades.max(1) as f64);
        let efficiency_factor = 1.0 / (1.0 + (metrics.avg_execution_time_us as f64 / 1_000_000.0));
        
        metrics.strategy_score = (
            metrics.win_rate * 0.4 +
            consistency_factor * 0.3 +
            efficiency_factor * 0.2 +
            (profit_factor / 1000.0).min(1.0) * 0.1
        ).min(1.0).max(0.0);
    }
    
    async fn check_strategy_rotation(&self) {
        let should_rotate = {
            let last_rotation = self.strategy_selector.last_rotation.lock().await;
            last_rotation.elapsed() >= self.strategy_selector.rotation_interval
        };
        
        if should_rotate {
            self.select_next_strategy().await;
            *self.strategy_selector.last_rotation.lock().await = Instant::now();
        }
        
    async fn check_underperforming_strategies(&self) {
        let strategies_to_remove: Vec<String> = self.strategies.iter()
            .filter_map(|entry| {
                let (id, instance) = entry.pair();
                let metrics = instance.metrics.read();
                
                if instance.strategy.should_exit(&metrics) || 
                   metrics.strategy_score < 0.25 ||
                   (metrics.total_trades > 100 && metrics.win_rate < 0.4) {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        
        for strategy_id in strategies_to_remove {
            let _ = self.unload_strategy(&strategy_id).await;
        }
    }
    
    async fn select_next_strategy(&self) {
        match self.strategy_selector.selection_mode {
            SelectionMode::HighestScore => self.select_highest_score_strategy().await,
            SelectionMode::RoundRobin => self.select_round_robin_strategy().await,
            SelectionMode::Adaptive => self.select_adaptive_strategy().await,
            SelectionMode::Tournament => self.select_tournament_strategy().await,
        }
    }
    
    async fn select_highest_score_strategy(&self) {
        let best_strategy = self.strategies.iter()
            .filter(|entry| entry.value().active.load(Ordering::Relaxed))
            .max_by(|a, b| {
                let a_metrics = a.value().metrics.read();
                let b_metrics = b.value().metrics.read();
                
                let a_score = a.value().strategy.priority_score(&a_metrics);
                let b_score = b.value().strategy.priority_score(&b_metrics);
                
                a_score.partial_cmp(&b_score).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|entry| entry.key().clone());
        
        if let Some(strategy_id) = best_strategy {
            *self.active_strategy.write() = Some(strategy_id);
        }
    }
    
    async fn select_round_robin_strategy(&self) {
        let current = self.active_strategy.read().clone();
        let strategies: Vec<String> = self.strategies.iter()
            .filter(|entry| entry.value().active.load(Ordering::Relaxed))
            .map(|entry| entry.key().clone())
            .collect();
        
        if strategies.is_empty() {
            return;
        }
        
        let next_idx = if let Some(current_id) = current {
            strategies.iter()
                .position(|id| id == &current_id)
                .map(|idx| (idx + 1) % strategies.len())
                .unwrap_or(0)
        } else {
            0
        };
        
        *self.active_strategy.write() = Some(strategies[next_idx].clone());
    }
    
    async fn select_adaptive_strategy(&self) {
        let history = self.metrics_aggregator.history.lock().await;
        let recent_window = 100;
        
        let mut strategy_performance: HashMap<String, (f64, u32)> = HashMap::new();
        
        for (strategy_id, metrics, _) in history.iter().rev().take(recent_window) {
            let entry = strategy_performance.entry(strategy_id.clone()).or_insert((0.0, 0));
            entry.0 += metrics.strategy_score;
            entry.1 += 1;
        }
        
        let best_strategy = strategy_performance.into_iter()
            .filter(|(id, _)| {
                self.strategies.get(id)
                    .map(|s| s.active.load(Ordering::Relaxed))
                    .unwrap_or(false)
            })
            .max_by(|(_, (score_a, count_a)), (_, (score_b, count_b))| {
                let avg_a = score_a / (*count_a as f64);
                let avg_b = score_b / (*count_b as f64);
                avg_a.partial_cmp(&avg_b).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(id, _)| id);
        
        if let Some(strategy_id) = best_strategy {
            *self.active_strategy.write() = Some(strategy_id);
        } else {
            self.select_highest_score_strategy().await;
        }
    }
    
    async fn select_tournament_strategy(&self) {
        let active_strategies: Vec<(String, Arc<StrategyInstance>)> = self.strategies.iter()
            .filter(|entry| entry.value().active.load(Ordering::Relaxed))
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        
        if active_strategies.len() < 2 {
            self.select_highest_score_strategy().await;
            return;
        }
        
        let tournament_size = 3.min(active_strategies.len());
        let mut rng = rand::thread_rng();
        use rand::seq::SliceRandom;
        
        let mut tournament_pool = active_strategies.clone();
        tournament_pool.shuffle(&mut rng);
        tournament_pool.truncate(tournament_size);
        
        let winner = tournament_pool.into_iter()
            .max_by(|(_, a), (_, b)| {
                let a_metrics = a.metrics.read();
                let b_metrics = b.metrics.read();
                
                let a_score = a.strategy.priority_score(&a_metrics);
                let b_score = b.strategy.priority_score(&b_metrics);
                
                a_score.partial_cmp(&b_score).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(id, _)| id);
        
        if let Some(strategy_id) = winner {
            *self.active_strategy.write() = Some(strategy_id);
        }
    }
    
    pub async fn get_active_strategy_instance(&self) -> Option<Arc<StrategyInstance>> {
        let active = self.active_strategy.read().clone()?;
        self.strategies.get(&active).map(|entry| entry.clone())
    }
    
    pub async fn execute_strategy(
        &self,
        market_data: &MarketSnapshot,
    ) -> Result<Option<(TradeSignal, String)>, Box<dyn std::error::Error>> {
        let active = self.active_strategy.read().clone();
        
        let strategy_id = active.ok_or("No active strategy")?;
        let instance = self.strategies.get(&strategy_id)
            .ok_or("Active strategy not found")?;
        
        if !instance.active.load(Ordering::Relaxed) {
            return Ok(None);
        }
        
        let start_time = Instant::now();
        
        match instance.strategy.analyze_opportunity(&self.context, market_data).await? {
            Some(signal) if signal.confidence > 0.7 => {
                instance.last_execution.store(
                    start_time.elapsed().as_micros() as u64,
                    Ordering::Relaxed,
                );
                Ok(Some((signal, strategy_id)))
            }
            _ => Ok(None),
        }
    }
    
    pub async fn build_transaction_for_signal(
        &self,
        strategy_id: &str,
        signal: &TradeSignal,
    ) -> Result<Transaction, Box<dyn std::error::Error>> {
        let instance = self.strategies.get(strategy_id)
            .ok_or("Strategy not found")?;
        
        instance.strategy.build_transaction(&self.context, signal).await
    }
    
    pub fn report_execution_result(&self, strategy_id: String, result: ExecutionResult) {
        let injector = self.clone();
        tokio::spawn(async move {
            injector.update_strategy_metrics(&strategy_id, result).await;
        });
    }
    
    pub async fn get_all_strategies(&self) -> Vec<(String, StrategyMetrics)> {
        self.strategies.iter()
            .map(|entry| {
                let metrics = entry.value().metrics.read().clone();
                (entry.key().clone(), metrics)
            })
            .collect()
    }
    
    pub async fn get_strategy_metrics(&self, strategy_id: &str) -> Option<StrategyMetrics> {
        self.strategies.get(strategy_id)
            .map(|entry| entry.metrics.read().clone())
    }
    
    pub fn is_strategy_active(&self, strategy_id: &str) -> bool {
        self.strategies.get(strategy_id)
            .map(|entry| entry.active.load(Ordering::Relaxed))
            .unwrap_or(false)
    }
    
    pub async fn force_strategy_rotation(&self) {
        self.select_next_strategy().await;
        *self.strategy_selector.last_rotation.lock().await = Instant::now();
    }
    
    pub fn set_selection_mode(&self, mode: SelectionMode) {
        unsafe {
            let selector = &self.strategy_selector as *const StrategySelector as *mut StrategySelector;
            (*selector).selection_mode = mode;
        }
    }
}

impl Clone for RuntimeStrategyInjector {
    fn clone(&self) -> Self {
        panic!("RuntimeStrategyInjector should not be cloned directly");
    }
}

pub struct InjectorHandle {
    command_sender: mpsc::Sender<InjectorCommand>,
}

impl InjectorHandle {
    pub fn new(sender: mpsc::Sender<InjectorCommand>) -> Self {
        Self { command_sender: sender }
    }
    
    pub async fn load_strategy(
        &self,
        strategy: Box<dyn TradingStrategy>,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.command_sender.send(InjectorCommand::LoadStrategy {
            strategy,
            response: tx,
        }).await.map_err(|_| "Failed to send command")?;
        
        rx.await.map_err(|_| "Failed to receive response")?
    }
    
    pub async fn unload_strategy(&self, strategy_id: String) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.command_sender.send(InjectorCommand::UnloadStrategy {
            strategy_id,
            response: tx,
        }).await.map_err(|_| "Failed to send command")?;
        
        rx.await.map_err(|_| "Failed to receive response")?
    }
    
    pub async fn switch_strategy(&self, strategy_id: String) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.command_sender.send(InjectorCommand::SwitchStrategy {
            strategy_id,
            response: tx,
        }).await.map_err(|_| "Failed to send command")?;
        
        rx.await.map_err(|_| "Failed to receive response")?
    }
    
    pub async fn update_metrics(&self, strategy_id: String, result: ExecutionResult) {
        let _ = self.command_sender.send(InjectorCommand::UpdateMetrics {
            strategy_id,
            result,
        }).await;
    }
    
    pub async fn get_active_strategy(&self) -> Option<String> {
        let (tx, rx) = oneshot::channel();
        self.command_sender.send(InjectorCommand::GetActiveStrategy {
            response: tx,
        }).await.ok()?;
        
        rx.await.ok()?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    struct MockStrategy {
        id: String,
        version: u32,
    }
    
    #[async_trait::async_trait]
    impl TradingStrategy for MockStrategy {
        fn strategy_id(&self) -> &str {
            &self.id
        }
        
        fn version(&self) -> u32 {
            self.version
        }
        
        async fn analyze_opportunity(
            &self,
            _context: &StrategyContext,
            _market_data: &MarketSnapshot,
        ) -> Result<Option<TradeSignal>, Box<dyn std::error::Error>> {
            Ok(Some(TradeSignal {
                action: TradeAction::Buy,
                amount: 1000,
                expected_profit: 100,
                confidence: 0.8,
                max_slippage_bps: 50,
                urgency: 5,
            }))
        }
        
        async fn build_transaction(
            &self,
            _context: &StrategyContext,
            _signal: &TradeSignal,
        ) -> Result<Transaction, Box<dyn std::error::Error>> {
            Ok(Transaction::default())
        }
        
        fn update_metrics(&self, _metrics: &mut StrategyMetrics, _result: &ExecutionResult) {}
    }
}
