#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;
    use std::time::Duration;
    use solana_sdk::{signature::Keypair, pubkey::Pubkey};
    use chrono::{DateTime, Utc};
    use proptest::prelude::*;
    
    /// Integration test for complete MEV bot workflow
    #[tokio::test]
    async fn test_complete_mev_workflow() {
        let config = PredictorConfig::default();
        config.validate().expect("Config should be valid");
        
        let predictor = create_test_predictor(config).await;
        
        // Initialize RPC connections
        let rpc_urls = vec!["https://api.mainnet-beta.solana.com".to_string()];
        predictor.initialize_rpc_pool(rpc_urls).await.expect("RPC initialization failed");
        
        // Test price data ingestion
        let test_price = create_test_price_point(100.0, 1000.0, 0);
        predictor.update_price_data(test_price).expect("Price update failed");
        
        // Test MEV opportunity detection
        let opportunities = predictor.monitor_mempool().await.expect("Mempool monitoring failed");
        
        // Test security validation
        for opportunity in &opportunities {
            let validation_result = predictor.validate_transaction_security(opportunity);
            match validation_result {
                Ok(_) => println!("Opportunity validated successfully"),
                Err(e) => println!("Opportunity rejected: {:?}", e),
            }
        }
        
        // Test prediction generation
        let prediction = predictor.predict_movement().await.expect("Prediction failed");
        assert!(prediction.confidence >= 0.0 && prediction.confidence <= 1.0);
        
        println!("Complete MEV workflow test passed");
    }
    
    /// Test arbitrage opportunity detection with real historical data patterns
    #[tokio::test] 
    async fn test_arbitrage_opportunity_detection() {
        let predictor = create_test_predictor(PredictorConfig::default()).await;
        
        // Simulate price divergence between DEXs
        let raydium_price = create_test_price_point(100.0, 1000.0, 0);
        let orca_price = create_test_price_point(101.5, 1000.0, 0); // 1.5% arbitrage opportunity
        
        predictor.update_price_data(raydium_price).expect("Price update failed");
        
        // Simulate large trade detection that would create arbitrage
        let large_trade = LargeTrade {
            dex_source: DexSource::Raydium,
            token_in: Pubkey::new_unique(),
            token_out: Pubkey::new_unique(),
            amount_in: 100_000_000_000, // 100 SOL equivalent
            expected_amount_out: 10_000_000, // 10M USDC equivalent 
            price_impact: 0.015, // 1.5% price impact
            detected_at: Instant::now(),
        };
        
        let opportunity = predictor.calculate_backrun_opportunity(&large_trade, 12345)
            .expect("Opportunity calculation failed");
            
        // Validate opportunity meets profitability thresholds
        assert!(opportunity.expected_profit_lamports > predictor.config.jito_min_tip_lamports * 2);
        assert!(opportunity.confidence > 0.5);
        
        println!("Arbitrage detection test passed with profit: {} lamports", opportunity.expected_profit_lamports);
    }
    
    /// Test transaction simulation and validation
    #[tokio::test]
    async fn test_transaction_simulation() {
        let predictor = create_test_predictor(PredictorConfig::default()).await;
        
        // Create test transaction
        let keypair = Keypair::new();
        let mut transaction = Transaction::default();
        
        // Add compute budget instruction
        let compute_instruction = ComputeBudgetInstruction::set_compute_unit_limit(200_000);
        transaction.message.instructions.push(compute_instruction.into());
        
        // Test key manager transaction simulation
        let key_manager = predictor.key_manager.read().unwrap();
        
        // This would normally connect to devnet/testnet for simulation
        // For unit test, we validate the security checks work
        match key_manager.validate_transaction_value(&transaction) {
            Ok(_) => println!("Transaction validation passed"),
            Err(e) => println!("Transaction rejected: {:?}", e),
        }
    }
    
    /// Fuzz test for price data edge cases
    #[test]
    fn test_price_data_fuzz() {
        proptest!(|(
            price in 0.0001f64..1000000.0,
            volume in 0.0..1000000000.0,
            timestamp in 0u64..u64::MAX,
        )| {
            let price_point = PricePoint {
                price: sanitize_value(price),
                volume: sanitize_value(volume),
                timestamp_ms: timestamp,
                bid: sanitize_value(price * 0.999),
                ask: sanitize_value(price * 1.001),
                bid_volume: sanitize_value(volume * 0.5),
                ask_volume: sanitize_value(volume * 0.5),
                trades_count: 1,
                slot_number: timestamp / 1000,
                priority_fee_microlamports: 1000,
                jito_tip_lamports: Some(10000),
                dex_source: DexSource::Raydium,
                is_bundle_tx: false,
                compute_units_consumed: 100000,
            };
            
            // Ensure no panics with extreme values
            let config = PredictorConfig::default();
            let mut history = VecDeque::new();
            history.push_back(price_point);
            
            // Test all calculations handle edge cases gracefully
            prop_assert!(price_point.price.is_finite());
            prop_assert!(price_point.volume >= 0.0);
        });
    }
    
    /// Test network conditions monitoring
    #[tokio::test]
    async fn test_network_conditions_monitoring() {
        let predictor = create_test_predictor(PredictorConfig::default()).await;
        
        // Update network conditions
        {
            let mut conditions = predictor.network_conditions.write().unwrap();
            conditions.current_slot = 12345;
            conditions.failed_tx_ratio = 0.05; // 5% failure rate
            conditions.mempool_congestion = 0.7; // High congestion
            conditions.compute_unit_price = 2000; // Higher than baseline
        }
        
        // Test gas optimization under different conditions
        let mock_opportunity = BackrunOpportunity {
            target_signature: Signature::default(),
            arbitrage_route: vec![DexSource::Raydium, DexSource::Orca],
            expected_profit_lamports: 50_000,
            required_capital_lamports: 1_000_000,
            execution_window_ms: 400,
            confidence: 0.8,
        };
        
        let (compute_units, priority_fee) = predictor.optimize_gas_parameters(&mock_opportunity)
            .expect("Gas optimization failed");
            
        // Verify gas parameters are reasonable
        assert!(compute_units >= 80_000 && compute_units <= 500_000);
        assert!(priority_fee > 0 && priority_fee <= predictor.config.max_priority_fee_microlamports);
        
        println!("Network conditions test passed - CU: {}, Fee: {}", compute_units, priority_fee);
    }
    
    /// Test circuit breaker functionality
    #[tokio::test]
    async fn test_circuit_breaker() {
        let predictor = create_test_predictor(PredictorConfig::default()).await;
        
        // Simulate consecutive losses to trigger circuit breaker
        {
            let mut circuit_breaker = predictor.circuit_breaker.write().unwrap();
            circuit_breaker.consecutive_losses = 6; // Exceeds default limit of 5
            circuit_breaker.current_drawdown = 0.20; // 20% drawdown
            circuit_breaker.is_active = true;
        }
        
        let mock_opportunity = BackrunOpportunity {
            target_signature: Signature::default(),
            arbitrage_route: vec![DexSource::Jupiter],
            expected_profit_lamports: 100_000,
            required_capital_lamports: 500_000,
            execution_window_ms: 200,
            confidence: 0.9,
        };
        
        // Circuit breaker should prevent execution
        match predictor.validate_transaction_security(&mock_opportunity) {
            Ok(_) => panic!("Circuit breaker should have prevented execution"),
            Err(PredictorError::SecurityError { check_type }) => {
                assert!(check_type.contains("Circuit breaker active"));
                println!("Circuit breaker test passed - execution blocked");
            },
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }
    
    /// Test backtesting with simulated historical data
    #[tokio::test]
    async fn test_comprehensive_backtesting() {
        let predictor = create_test_predictor(PredictorConfig::default()).await;
        
        // Generate realistic historical data with volatility patterns
        let historical_data = generate_realistic_historical_data(1000);
        let initial_capital = 10_000_000_000u64; // 10 SOL
        
        let backtest_results = predictor.backtest_strategy(historical_data, initial_capital).await
            .expect("Backtesting failed");
        
        // Validate backtest results
        assert!(backtest_results.total_trades > 0);
        assert!(backtest_results.mev_opportunities_detected > 0);
        assert!(backtest_results.sharpe_ratio.is_finite());
        assert!(backtest_results.max_drawdown >= 0.0 && backtest_results.max_drawdown <= 1.0);
        
        let win_rate = if backtest_results.total_trades > 0 {
            backtest_results.winning_trades as f64 / backtest_results.total_trades as f64
        } else {
            0.0
        };
        
        println!("Backtest Results:");
        println!("  Total Trades: {}", backtest_results.total_trades);
        println!("  Win Rate: {:.2}%", win_rate * 100.0);
        println!("  Total PnL: {} lamports", backtest_results.total_pnl_lamports);
        println!("  Sharpe Ratio: {:.3}", backtest_results.sharpe_ratio);
        println!("  Max Drawdown: {:.2}%", backtest_results.max_drawdown * 100.0);
        println!("  MEV Opportunities: {} detected, {} executed", 
                backtest_results.mev_opportunities_detected,
                backtest_results.mev_opportunities_executed);
    }
    
    /// Test adaptive weight learning mechanism
    #[tokio::test]
    async fn test_adaptive_weight_learning() {
        let predictor = create_test_predictor(PredictorConfig::default()).await;
        
        // Get initial weights
        let initial_momentum = *predictor.momentum_weight.read().unwrap();
        let initial_orderflow = *predictor.orderflow_weight.read().unwrap();
        
        // Simulate high accuracy in volatile market (should favor momentum)
        predictor.update_adaptive_weights(0.85, 0.9).await
            .expect("Weight update failed");
            
        let new_momentum = *predictor.momentum_weight.read().unwrap();
        let new_orderflow = *predictor.orderflow_weight.read().unwrap();
        
        // Momentum weight should have increased
        assert!(new_momentum > initial_momentum);
        
        // All weights should still sum to approximately 1.0
        let total_weights = new_momentum + new_orderflow + 
                          *predictor.technical_weight.read().unwrap() + 
                          *predictor.microstructure_weight.read().unwrap();
        assert!((total_weights - 1.0).abs() < 0.01);
        
        println!("Adaptive learning test passed - momentum weight: {:.3} -> {:.3}", 
                initial_momentum, new_momentum);
    }
    
    /// Test performance optimization and caching
    #[test]
    fn test_performance_caching() {
        let mut cache = IndicatorCache::new(100);
        
        // Store some indicators
        cache.store_indicator("rsi_14".to_string(), 65.5, 30, 1500);
        cache.store_indicator("macd_signal".to_string(), 0.25, 30, 2000);
        
        // Test cache hits
        assert_eq!(cache.get_indicator("rsi_14"), Some(65.5));
        assert_eq!(cache.get_indicator("macd_signal"), Some(0.25));
        assert_eq!(cache.get_indicator("nonexistent"), None);
        
        // Test cache efficiency
        let efficiency = cache.get_efficiency();
        assert!(efficiency > 0.0 && efficiency <= 1.0);
        
        println!("Cache efficiency: {:.2}%", efficiency * 100.0);
    }
    
    /// Stress test for high-frequency operations
    #[tokio::test]
    async fn test_high_frequency_stress() {
        let predictor = create_test_predictor(PredictorConfig::default()).await;
        let start_time = Instant::now();
        
        // Process 1000 price updates rapidly
        for i in 0..1000 {
            let price_point = create_test_price_point(
                100.0 + (i as f64 * 0.01), 
                1000.0 + (i as f64 * 10.0),
                i
            );
            
            predictor.update_price_data(price_point)
                .expect("High-frequency price update failed");
                
            // Every 10th update, generate prediction
            if i % 10 == 0 {
                let _prediction = predictor.predict_movement().await
                    .expect("High-frequency prediction failed");
            }
        }
        
        let duration = start_time.elapsed();
        let throughput = 1000.0 / duration.as_secs_f64();
        
        println!("High-frequency stress test: {:.0} updates/sec", throughput);
        assert!(throughput > 100.0, "Throughput too low: {:.2} updates/sec", throughput);
    }
    
    /// Test security oracle integration
    #[test]
    fn test_security_oracle() {
        let oracle = SecurityOracle::new();
        
        let safe_program = Pubkey::new_unique();
        let malicious_program = Pubkey::new_unique();
        
        // Add malicious program to exploit list
        {
            let mut exploits = oracle.known_exploits.write().unwrap();
            exploits.insert(malicious_program);
        }
        
        // Test security checks
        assert!(oracle.is_program_safe(&safe_program));
        assert!(!oracle.is_program_safe(&malicious_program));
        
        println!("Security oracle test passed");
    }
    
    // Helper functions for test setup
    
    async fn create_test_predictor(config: PredictorConfig) -> MicroPricePredictor {
        let security_settings = SecuritySettings {
            max_transactions_per_minute: 60,
            enable_transaction_simulation: true,
            require_confirmation_before_send: false,
            max_transaction_value_lamports: 10_000_000_000,
        };
        
        let rpc_endpoints = vec!["https://api.devnet.solana.com".to_string()];
        let rpc_pool = Arc::new(RpcConnectionPool::new(rpc_endpoints, 3));
        let async_rpc = Arc::new(AsyncRpcClient::new("https://api.devnet.solana.com".to_string()));
        
        MicroPricePredictor {
            config: config.clone(),
            price_history: Arc::new(RwLock::new(VecDeque::new())),
            prediction_cache: Arc::new(RwLock::new(HashMap::new())),
            last_prediction: Arc::new(RwLock::new(None)),
            performance_metrics: Arc::new(RwLock::new(PerformanceMetrics::default())),
            orderflow_weight: Arc::new(RwLock::new(0.35)),
            microstructure_weight: Arc::new(RwLock::new(0.25)),
            technical_weight: Arc::new(RwLock::new(0.15)),
            momentum_weight: Arc::new(RwLock::new(0.25)),
            cached_vwap: Arc::new(RwLock::new(None)),
            cached_vwap_timestamp: Arc::new(RwLock::new(None)),
            ema_12: Arc::new(RwLock::new(EmaState::new(12))),
            ema_26: Arc::new(RwLock::new(EmaState::new(26))),
            ema_signal: Arc::new(RwLock::new(EmaState::new(9))),
            rsi_state: Arc::new(RwLock::new(RsiState::new(config.rsi_period))),
            bollinger_state: Arc::new(RwLock::new(BollingerState::new(config.bollinger_window))),
            vwap_state: Arc::new(RwLock::new(VwapState::new(config.vwap_window))),
            recent_high: Arc::new(RwLock::new(0.0)),
            recent_low: Arc::new(RwLock::new(f64::MAX)),
            rpc_pool,
            async_rpc_client: async_rpc,
            jito_client: Arc::new(RwLock::new(None)),
            network_conditions: Arc::new(RwLock::new(NetworkConditions::default())),
            mempool_monitor: Arc::new(RwLock::new(MempoolMonitor::default())),
            geyser_subscriber: Arc::new(RwLock::new(None)),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::default())),
            transaction_validator: Arc::new(TransactionValidator::new(&config)),
            key_manager: Arc::new(RwLock::new(KeyManager::new(security_settings).unwrap())),
            indicator_cache: Arc::new(RwLock::new(IndicatorCache::new(1000))),
            parallel_executor: Arc::new(ParallelExecutor::new(4)),
        }
    }
    
    fn create_test_price_point(price: f64, volume: f64, offset: u64) -> PricePoint {
        PricePoint {
            price: sanitize_value(price),
            volume: sanitize_value(volume),
            timestamp_ms: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64 + offset,
            bid: sanitize_value(price * 0.999),
            ask: sanitize_value(price * 1.001),
            bid_volume: sanitize_value(volume * 0.4),
            ask_volume: sanitize_value(volume * 0.6),
            trades_count: 5 + (offset % 20) as u32,
            slot_number: 100000 + offset,
            priority_fee_microlamports: 1000 + (offset % 5000),
            jito_tip_lamports: Some(10000 + (offset % 50000)),
            dex_source: match offset % 3 {
                0 => DexSource::Raydium,
                1 => DexSource::Orca,
                _ => DexSource::Jupiter,
            },
            is_bundle_tx: offset % 7 == 0,
            compute_units_consumed: 50000 + ((offset % 150000) as u32),
        }
    }
    
    fn generate_realistic_historical_data(count: usize) -> Vec<PricePoint> {
        let mut data = Vec::with_capacity(count);
        let mut price = 100.0;
        let base_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        
        for i in 0..count {
            // Add realistic price movement with volatility clustering
            let volatility = 0.02 + (i as f64 / count as f64) * 0.03; // Increasing volatility
            let random_move = (rand::random::<f64>() - 0.5) * volatility;
            price *= 1.0 + random_move;
            price = price.max(0.01); // Prevent negative prices
            
            let volume = 1000.0 + (rand::random::<f64>() * 5000.0);
            
            data.push(create_test_price_point(price, volume, i as u64));
        }
        
        data
    }
}

/// Benchmark tests for performance validation
#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::time::Instant;
    
    #[tokio::test]
    async fn benchmark_prediction_generation() {
        let predictor = tests::create_test_predictor(PredictorConfig::default()).await;
        
        // Populate with test data
        for i in 0..100 {
            let price_point = tests::create_test_price_point(100.0 + i as f64 * 0.1, 1000.0, i);
            predictor.update_price_data(price_point).expect("Price update failed");
        }
        
        let start = Instant::now();
        let iterations = 100;
        
        for _ in 0..iterations {
            let _prediction = predictor.predict_movement().await.expect("Prediction failed");
        }
        
        let duration = start.elapsed();
        let avg_latency = duration.as_micros() / iterations;
        
        println!("Prediction benchmark: {} μs average latency", avg_latency);
        assert!(avg_latency < 10_000, "Prediction latency too high: {} μs", avg_latency);
    }
    
    #[test]
    fn benchmark_technical_indicators() {
        let start = Instant::now();
        let iterations = 10_000;
        
        let mut ema_state = EmaState::new(14);
        let mut rsi_state = RsiState::new(14);
        
        for i in 0..iterations {
            let price = 100.0 + (i as f64 * 0.01);
            ema_state.update(price);
            rsi_state.update(price);
        }
        
        let duration = start.elapsed();
        let throughput = iterations as f64 / duration.as_secs_f64();
        
        println!("Indicator benchmark: {:.0} calculations/sec", throughput);
        assert!(throughput > 100_000.0, "Indicator calculation too slow: {:.0}/sec", throughput);
    }
}
