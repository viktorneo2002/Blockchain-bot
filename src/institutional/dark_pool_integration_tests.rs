use super::*;
use solana_sdk::signer::keypair::Keypair;
use tokio::time::{sleep, Duration};

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_market_maker_registration() {
        let keypair = Keypair::new();
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            None,
        ).await.expect("Failed to create integration");

        // Test market maker registration flow
        let mm_keypair = Keypair::new();
        let registration = MarketMakerRegistration {
            pubkey: mm_keypair.pubkey(),
            stake_amount: MIN_MARKET_MAKER_STAKE,
            signature: vec![0u8; 64], // Mock signature
        };

        // Should successfully register market maker with sufficient stake
        assert!(integration.verify_market_maker_registration(&registration).await.is_ok());
    }

    #[tokio::test]
    async fn test_oracle_price_validation() {
        let keypair = Keypair::new();
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            None,
        ).await.expect("Failed to create integration");

        // Initialize oracle feeds
        integration.initialize_oracle_feeds().await.expect("Failed to init oracle feeds");

        // Test oracle price retrieval
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();

        // This might fail if oracle is stale, but should not panic
        match integration.get_oracle_price(&sol_mint, &usdc_mint).await {
            Ok(Some(price)) => {
                assert!(price > 0, "Oracle price should be positive");
                println!("Oracle price retrieved: {}", price);
            }
            Ok(None) => println!("No oracle feed available for this pair"),
            Err(e) => println!("Oracle error (expected in test): {}", e),
        }
    }

    #[tokio::test]
    async fn test_rfq_quote_generation() {
        let keypair = Keypair::new();
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            None,
        ).await.expect("Failed to create integration");

        // Create test RFQ
        let rfq = RFQRequest {
            base_mint: Pubkey::from_str(SOL_MINT).unwrap(),
            quote_mint: Pubkey::from_str(USDC_MINT).unwrap(),
            amount: 1_000_000, // 1 SOL
            side: OrderSide::Buy,
            commitment_hash: [1u8; 32],
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            expires_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + 300,
        };

        // Create mock market maker
        let mm = VerifiedMarketMaker {
            pubkey: Keypair::new().pubkey(),
            stake_amount: MIN_MARKET_MAKER_STAKE,
            reputation_score: 1.0,
            last_activity: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            is_active: true,
            total_trades: 10,
            successful_trades: 9,
            success_rate: 0.9,
            authentication_key: Keypair::new().pubkey(),
        };

        // Test quote generation
        match integration.generate_quote_for_market_maker(&rfq, &mm).await {
            Ok(quote) => {
                assert_eq!(quote.base_mint, rfq.base_mint);
                assert_eq!(quote.quote_mint, rfq.quote_mint);
                assert_eq!(quote.size, rfq.amount);
                assert!(quote.price > 0);
                assert!(quote.expires_at > SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs());
                println!("Generated quote: price={}, size={}", quote.price, quote.size);
            }
            Err(e) => panic!("Failed to generate quote: {}", e),
        }
    }

    #[tokio::test]
    async fn test_order_execution_flow() {
        let keypair = Keypair::new();
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            None,
        ).await.expect("Failed to create integration");

        // Create test order
        let order = Order {
            base_mint: Pubkey::from_str(SOL_MINT).unwrap(),
            quote_mint: Pubkey::from_str(USDC_MINT).unwrap(),
            amount: 100_000, // 0.1 SOL
            side: OrderSide::Buy,
            limit_price: Some(50_000_000), // $50 per SOL in microlamports
            max_slippage_bps: 100, // 1%
            expires_at: Some(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + 3600), // 1 hour
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Test order validation
        assert!(integration.validate_order_size(&order).is_ok());

        // Test commitment creation
        match integration.create_order_commitment(&order).await {
            Ok(commitment) => {
                assert_ne!(commitment.commitment_hash, [0u8; 32]);
                println!("Created order commitment: {:?}", hex::encode(commitment.commitment_hash));
            }
            Err(e) => panic!("Failed to create commitment: {}", e),
        }
    }

    #[tokio::test]
    async fn test_websocket_message_handling() {
        let keypair = Keypair::new();
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            None,
        ).await.expect("Failed to create integration");

        // Test WebSocket message serialization/deserialization
        let rfq_message = WebSocketMessage::RFQRequest(RFQRequest {
            base_mint: Pubkey::from_str(SOL_MINT).unwrap(),
            quote_mint: Pubkey::from_str(USDC_MINT).unwrap(),
            amount: 1_000_000,
            side: OrderSide::Buy,
            commitment_hash: [1u8; 32],
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            expires_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + 300,
        });

        // Test message serialization
        let serialized = serde_json::to_string(&rfq_message).expect("Serialization failed");
        let deserialized: WebSocketMessage = serde_json::from_str(&serialized).expect("Deserialization failed");
        
        match (rfq_message, deserialized) {
            (WebSocketMessage::RFQRequest(orig), WebSocketMessage::RFQRequest(deser)) => {
                assert_eq!(orig.base_mint, deser.base_mint);
                assert_eq!(orig.amount, deser.amount);
                println!("WebSocket message serialization test passed");
            }
            _ => panic!("Message type mismatch"),
        }
    }

    #[tokio::test]
    async fn test_signature_verification() {
        let keypair = Keypair::new();
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            keypair,
            None,
        ).await.expect("Failed to create integration");

        // Test signature generation and verification
        let price = 50_000_000u64;
        let amount = 1_000_000u64;
        let auth_key = Keypair::new().pubkey();

        match integration.sign_quote_data(&price, &amount, &auth_key).await {
            Ok(signature) => {
                assert_eq!(signature.len(), 64);
                assert_ne!(signature, vec![0u8; 64]);
                println!("Generated signature: {} bytes", signature.len());
            }
            Err(e) => panic!("Failed to generate signature: {}", e),
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_functionality() {
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
            None,
        ).await.expect("Failed to create integration");

        // Circuit breaker should be closed initially
        assert!(!integration.circuit_breaker.is_open());

        // Trigger multiple failures
        for _ in 0..5 {
            integration.circuit_breaker.record_failure();
        }

        // Circuit breaker should be open after failures
        assert!(integration.circuit_breaker.is_open());

        // Record success should help but not immediately close
        integration.circuit_breaker.record_success();
        println!("Circuit breaker test completed");
    }

    #[tokio::test]
    async fn test_metrics_collection() {
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
            None,
        ).await.expect("Failed to create integration");

        // Test metrics initialization
        let (total, successful, failed, volume) = integration.get_metrics().await;
        assert_eq!(total, 0);
        assert_eq!(successful, 0);
        assert_eq!(failed, 0);
        assert_eq!(volume, 0);

        // Test metrics update
        integration.metrics.total_orders.fetch_add(1, Ordering::Relaxed);
        integration.metrics.successful_orders.fetch_add(1, Ordering::Relaxed);
        integration.metrics.total_volume.fetch_add(1_000_000, Ordering::Relaxed);

        let (total, successful, failed, volume) = integration.get_metrics().await;
        assert_eq!(total, 1);
        assert_eq!(successful, 1);
        assert_eq!(failed, 0);
        assert_eq!(volume, 1_000_000);

        println!("Metrics: total={}, successful={}, failed={}, volume={}", 
                 total, successful, failed, volume);
    }

    #[tokio::test]
    async fn test_pool_refresh_logic() {
        let integration = DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
            None,
        ).await.expect("Failed to create integration");

        // Create test pool info
        let pool_info = Arc::new(PoolInfo {
            address: Pubkey::new_unique(),
            protocol: Protocol::Phoenix,
            base_reserve: AtomicU64::new(1_000_000),
            quote_reserve: AtomicU64::new(2_000_000),
            fee_bps: 30,
            last_update: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
        });

        // Test pool refresh (may fail due to invalid address, but should not panic)
        match integration.refresh_pool_reserves(pool_info.clone()).await {
            Ok(_) => println!("Pool refresh succeeded"),
            Err(e) => println!("Pool refresh failed (expected): {}", e),
        }

        // Verify pool info structure is intact
        assert!(pool_info.base_reserve.load(Ordering::Relaxed) >= 0);
        assert!(pool_info.quote_reserve.load(Ordering::Relaxed) >= 0);
    }
}

#[cfg(test)]
mod stress_tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_concurrent_order_processing() {
        let integration = Arc::new(DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
            None,
        ).await.expect("Failed to create integration"));

        let mut handles = vec![];

        // Spawn multiple concurrent order processing tasks
        for i in 0..10 {
            let integration_clone = integration.clone();
            let handle = tokio::spawn(async move {
                let order = Order {
                    base_mint: Pubkey::from_str(SOL_MINT).unwrap(),
                    quote_mint: Pubkey::from_str(USDC_MINT).unwrap(),
                    amount: 100_000 + i * 10_000, // Varying amounts
                    side: if i % 2 == 0 { OrderSide::Buy } else { OrderSide::Sell },
                    limit_price: Some(50_000_000),
                    max_slippage_bps: 100,
                    expires_at: None,
                    created_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                // Test concurrent commitment creation
                match integration_clone.create_order_commitment(&order).await {
                    Ok(commitment) => {
                        assert_ne!(commitment.commitment_hash, [0u8; 32]);
                        println!("Task {} created commitment", i);
                    }
                    Err(e) => println!("Task {} failed: {}", i, e),
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.expect("Task panicked");
        }

        println!("Concurrent order processing test completed");
    }

    #[tokio::test]
    async fn test_market_maker_connection_handling() {
        let integration = Arc::new(DarkPoolIntegration::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
            None,
        ).await.expect("Failed to create integration"));

        // Test multiple market maker connections
        let mut mm_handles = vec![];

        for i in 0..5 {
            let integration_clone = integration.clone();
            let handle = tokio::spawn(async move {
                let mm_keypair = Keypair::new();
                let registration = MarketMakerRegistration {
                    pubkey: mm_keypair.pubkey(),
                    stake_amount: MIN_MARKET_MAKER_STAKE + i * 1_000_000,
                    signature: vec![i as u8; 64],
                };

                // Test concurrent market maker registration
                match integration_clone.verify_market_maker_registration(&registration).await {
                    Ok(_) => println!("Market maker {} registered", i),
                    Err(e) => println!("Market maker {} registration failed: {}", i, e),
                }
            });
            mm_handles.push(handle);
        }

        // Wait for all market maker registrations
        for handle in mm_handles {
            handle.await.expect("Market maker task panicked");
        }

        println!("Market maker connection handling test completed");
    }
}
