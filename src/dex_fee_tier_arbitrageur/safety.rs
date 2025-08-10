use anyhow::{anyhow, Result};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{error, warn, info};

/// Safety checks and circuit breakers
pub struct SafetyMonitor {
    state: Arc<RwLock<SafetyState>>,
    config: SafetyConfig,
}

#[derive(Debug, Clone)]
pub struct SafetyConfig {
    /// Maximum loss allowed in a rolling window (lamports)
    pub max_loss_per_window: u128,
    
    /// Time window for loss tracking (seconds)
    pub loss_window_seconds: u64,
    
    /// Maximum consecutive failed transactions
    pub max_consecutive_failures: u32,
    
    /// Maximum gas spent without profit (lamports)
    pub max_gas_without_profit: u128,
    
    /// Minimum time between transactions (milliseconds)
    pub min_tx_interval_ms: u64,
    
    /// Maximum transactions per minute
    pub max_tx_per_minute: u32,
    
    /// Maximum slippage allowed (basis points)
    pub max_slippage_bps: u64,
    
    /// Minimum pool liquidity required (lamports)
    pub min_pool_liquidity: u128,
    
    /// Maximum position size (lamports)
    pub max_position_size: u128,
    
    /// Enable circuit breaker
    pub circuit_breaker_enabled: bool,
    
    /// Auto-recovery after cooldown period
    pub auto_recovery_enabled: bool,
    
    /// Cooldown period after circuit break (seconds)
    pub cooldown_period_seconds: u64,
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            max_loss_per_window: 1_000_000_000, // 1 SOL
            loss_window_seconds: 3600, // 1 hour
            max_consecutive_failures: 5,
            max_gas_without_profit: 100_000_000, // 0.1 SOL
            min_tx_interval_ms: 100,
            max_tx_per_minute: 100,
            max_slippage_bps: 100, // 1%
            min_pool_liquidity: 1_000_000_000, // 1 SOL worth
            max_position_size: 10_000_000_000, // 10 SOL
            circuit_breaker_enabled: true,
            auto_recovery_enabled: true,
            cooldown_period_seconds: 300, // 5 minutes
        }
    }
}

#[derive(Debug, Clone)]
struct SafetyState {
    /// Circuit breaker tripped
    circuit_broken: bool,
    
    /// Time when circuit was broken
    circuit_broken_at: Option<u64>,
    
    /// Consecutive failures
    consecutive_failures: u32,
    
    /// Total gas spent without profit
    gas_without_profit: u128,
    
    /// Loss tracking
    loss_events: Vec<LossEvent>,
    
    /// Transaction timestamps for rate limiting
    tx_timestamps: Vec<u64>,
    
    /// Last transaction timestamp
    last_tx_timestamp: u64,
}

#[derive(Debug, Clone)]
struct LossEvent {
    amount: u128,
    timestamp: u64,
}

impl SafetyMonitor {
    pub fn new(config: SafetyConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(SafetyState {
                circuit_broken: false,
                circuit_broken_at: None,
                consecutive_failures: 0,
                gas_without_profit: 0,
                loss_events: Vec::new(),
                tx_timestamps: Vec::new(),
                last_tx_timestamp: 0,
            })),
            config,
        }
    }
    
    /// Check if it's safe to execute a transaction
    pub async fn check_safe_to_execute(
        &self,
        amount: u128,
        expected_profit: i128,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        
        // Check circuit breaker
        if state.circuit_broken {
            if self.config.auto_recovery_enabled {
                if let Some(broken_at) = state.circuit_broken_at {
                    let now = Self::current_timestamp();
                    if now - broken_at > self.config.cooldown_period_seconds {
                        info!("Circuit breaker auto-recovery after cooldown");
                        state.circuit_broken = false;
                        state.circuit_broken_at = None;
                        state.consecutive_failures = 0;
                    } else {
                        return Err(anyhow!("Circuit breaker is active, cooldown remaining: {} seconds", 
                            self.config.cooldown_period_seconds - (now - broken_at)));
                    }
                } else {
                    return Err(anyhow!("Circuit breaker is active"));
                }
            } else {
                return Err(anyhow!("Circuit breaker is active (manual reset required)"));
            }
        }
        
        // Check position size
        if amount > self.config.max_position_size {
            return Err(anyhow!(
                "Position size {} exceeds maximum {}",
                amount,
                self.config.max_position_size
            ));
        }
        
        // Check rate limiting
        let now_ms = Self::current_timestamp_ms();
        
        // Minimum interval between transactions
        if now_ms - state.last_tx_timestamp < self.config.min_tx_interval_ms {
            return Err(anyhow!("Too soon since last transaction"));
        }
        
        // Maximum transactions per minute
        let one_minute_ago = now_ms.saturating_sub(60_000);
        state.tx_timestamps.retain(|&ts| ts > one_minute_ago);
        
        if state.tx_timestamps.len() >= self.config.max_tx_per_minute as usize {
            return Err(anyhow!(
                "Rate limit exceeded: {} transactions in last minute",
                state.tx_timestamps.len()
            ));
        }
        
        // Check rolling loss window
        let now = Self::current_timestamp();
        let window_start = now.saturating_sub(self.config.loss_window_seconds);
        
        // Clean old loss events
        state.loss_events.retain(|event| event.timestamp > window_start);
        
        // Calculate total loss in window
        let total_loss: u128 = state.loss_events.iter().map(|e| e.amount).sum();
        
        if total_loss > self.config.max_loss_per_window {
            self.trip_circuit_breaker(&mut state, format!(
                "Maximum loss exceeded: {} lamports in {} seconds",
                total_loss,
                self.config.loss_window_seconds
            )).await;
            return Err(anyhow!("Maximum loss per window exceeded"));
        }
        
        // Check gas without profit
        if state.gas_without_profit > self.config.max_gas_without_profit {
            self.trip_circuit_breaker(&mut state, format!(
                "Maximum gas without profit exceeded: {} lamports",
                state.gas_without_profit
            )).await;
            return Err(anyhow!("Maximum gas spent without profit"));
        }
        
        // Update transaction timestamp
        state.tx_timestamps.push(now_ms);
        state.last_tx_timestamp = now_ms;
        
        Ok(())
    }
    
    /// Record a successful transaction
    pub async fn record_success(&self, profit: i128, gas_cost: u128) {
        let mut state = self.state.write().await;
        
        // Reset consecutive failures
        state.consecutive_failures = 0;
        
        // Update gas tracking
        if profit > 0 {
            // Reset gas without profit on profitable trade
            state.gas_without_profit = 0;
        } else {
            state.gas_without_profit = state.gas_without_profit.saturating_add(gas_cost);
        }
        
        info!("Transaction successful: profit={} gas={}", profit, gas_cost);
    }
    
    /// Record a failed transaction
    pub async fn record_failure(&self, loss: u128, gas_cost: u128, reason: String) {
        let mut state = self.state.write().await;
        
        // Increment consecutive failures
        state.consecutive_failures += 1;
        
        // Add gas to unprofitable spending
        state.gas_without_profit = state.gas_without_profit.saturating_add(gas_cost);
        
        // Record loss if any
        if loss > 0 {
            state.loss_events.push(LossEvent {
                amount: loss,
                timestamp: Self::current_timestamp(),
            });
        }
        
        warn!(
            "Transaction failed: loss={} gas={} consecutive_failures={} reason={}",
            loss, gas_cost, state.consecutive_failures, reason
        );
        
        // Check if we should trip circuit breaker
        if state.consecutive_failures >= self.config.max_consecutive_failures {
            self.trip_circuit_breaker(&mut state, format!(
                "Maximum consecutive failures reached: {}",
                state.consecutive_failures
            )).await;
        }
    }
    
    /// Check pool safety
    pub async fn check_pool_safety(
        &self,
        liquidity: u128,
        slippage_bps: u64,
    ) -> Result<()> {
        // Check minimum liquidity
        if liquidity < self.config.min_pool_liquidity {
            return Err(anyhow!(
                "Pool liquidity {} below minimum {}",
                liquidity,
                self.config.min_pool_liquidity
            ));
        }
        
        // Check maximum slippage
        if slippage_bps > self.config.max_slippage_bps {
            return Err(anyhow!(
                "Slippage {} bps exceeds maximum {} bps",
                slippage_bps,
                self.config.max_slippage_bps
            ));
        }
        
        Ok(())
    }
    
    /// Trip the circuit breaker
    async fn trip_circuit_breaker(&self, state: &mut SafetyState, reason: String) {
        if !self.config.circuit_breaker_enabled {
            return;
        }
        
        error!("CIRCUIT BREAKER TRIPPED: {}", reason);
        
        state.circuit_broken = true;
        state.circuit_broken_at = Some(Self::current_timestamp());
        
        // Reset counters
        state.consecutive_failures = 0;
        state.gas_without_profit = 0;
    }
    
    /// Reset circuit breaker manually
    pub async fn reset_circuit_breaker(&self) {
        let mut state = self.state.write().await;
        
        info!("Circuit breaker manually reset");
        
        state.circuit_broken = false;
        state.circuit_broken_at = None;
        state.consecutive_failures = 0;
        state.gas_without_profit = 0;
        state.loss_events.clear();
    }
    
    /// Get current safety status
    pub async fn get_status(&self) -> SafetyStatus {
        let state = self.state.read().await;
        
        let now = Self::current_timestamp();
        let window_start = now.saturating_sub(self.config.loss_window_seconds);
        
        // Calculate metrics
        let recent_losses: u128 = state.loss_events
            .iter()
            .filter(|e| e.timestamp > window_start)
            .map(|e| e.amount)
            .sum();
        
        let one_minute_ago = Self::current_timestamp_ms().saturating_sub(60_000);
        let recent_tx_count = state.tx_timestamps
            .iter()
            .filter(|&&ts| ts > one_minute_ago)
            .count() as u32;
        
        SafetyStatus {
            circuit_broken: state.circuit_broken,
            consecutive_failures: state.consecutive_failures,
            gas_without_profit: state.gas_without_profit,
            loss_in_window: recent_losses,
            tx_per_minute: recent_tx_count,
            last_tx_timestamp: state.last_tx_timestamp,
        }
    }
    
    /// Get current timestamp in seconds
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
    
    /// Get current timestamp in milliseconds
    fn current_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Safety status snapshot
#[derive(Debug, Clone)]
pub struct SafetyStatus {
    pub circuit_broken: bool,
    pub consecutive_failures: u32,
    pub gas_without_profit: u128,
    pub loss_in_window: u128,
    pub tx_per_minute: u32,
    pub last_tx_timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_circuit_breaker() {
        let config = SafetyConfig {
            max_consecutive_failures: 3,
            circuit_breaker_enabled: true,
            auto_recovery_enabled: false,
            ..Default::default()
        };
        
        let monitor = SafetyMonitor::new(config);
        
        // Should be safe initially
        assert!(monitor.check_safe_to_execute(1000, 100).await.is_ok());
        
        // Record failures
        monitor.record_failure(0, 1000, "test".to_string()).await;
        monitor.record_failure(0, 1000, "test".to_string()).await;
        monitor.record_failure(0, 1000, "test".to_string()).await;
        
        // Circuit should be broken now
        assert!(monitor.check_safe_to_execute(1000, 100).await.is_err());
        
        // Manual reset
        monitor.reset_circuit_breaker().await;
        assert!(monitor.check_safe_to_execute(1000, 100).await.is_ok());
    }
    
    #[tokio::test]
    async fn test_rate_limiting() {
        let config = SafetyConfig {
            max_tx_per_minute: 2,
            min_tx_interval_ms: 10,
            ..Default::default()
        };
        
        let monitor = SafetyMonitor::new(config);
        
        // First two should succeed
        assert!(monitor.check_safe_to_execute(1000, 100).await.is_ok());
        tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
        assert!(monitor.check_safe_to_execute(1000, 100).await.is_ok());
        
        // Third should fail (rate limit)
        tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
        assert!(monitor.check_safe_to_execute(1000, 100).await.is_err());
    }
    
    #[tokio::test]
    async fn test_loss_tracking() {
        let config = SafetyConfig {
            max_loss_per_window: 10000,
            loss_window_seconds: 60,
            ..Default::default()
        };
        
        let monitor = SafetyMonitor::new(config);
        
        // Record some losses
        monitor.record_failure(5000, 100, "test".to_string()).await;
        monitor.record_failure(3000, 100, "test".to_string()).await;
        
        // Should still be safe (8000 < 10000)
        assert!(monitor.check_safe_to_execute(1000, 100).await.is_ok());
        
        // One more loss should trip it
        monitor.record_failure(3000, 100, "test".to_string()).await;
        
        // Should be unsafe now (11000 > 10000)
        assert!(monitor.check_safe_to_execute(1000, 100).await.is_err());
    }
}
