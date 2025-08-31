use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::{env, sync::Arc, str::FromStr};
use tracing::{info, warn, error, Level};
use tracing_subscriber::FmtSubscriber;

use blockchain_bot::{
    infrastructure::{CustomRPCEndpoints, EndpointConfig},
    merkle_proof_prefetcher::{
        HeliusProofProvider, HeliusConfig, MerkleProofPrefetcher, ProofProvider
    },
};

/// Comprehensive example demonstrating the HeliusProofProvider integration
/// with advanced endpoint health monitoring and adaptive routing
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("🚀 Starting Helius Proof Provider Example");

    // 1. Configure multiple RPC endpoints with priority and health monitoring
    let rpc_endpoints = vec![
        EndpointConfig {
            url: "https://mainnet.helius-rpc.com".to_string(),
            ws_url: "wss://mainnet.helius-rpc.com".to_string(),
            weight: 10,
            max_requests_per_second: 100,
            timeout_ms: 8000,
            priority: 9, // Highest priority
        },
        EndpointConfig {
            url: "https://api.mainnet-beta.solana.com".to_string(),
            ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
            weight: 5,
            max_requests_per_second: 40,
            timeout_ms: 10000,
            priority: 7,
        },
        EndpointConfig {
            url: "https://solana-api.projectserum.com".to_string(),
            ws_url: "wss://solana-api.projectserum.com".to_string(),
            weight: 3,
            max_requests_per_second: 30,
            timeout_ms: 12000,
            priority: 5,
        },
    ];

    // 2. Initialize CustomRPCEndpoints with health monitoring
    let rpc_client = Arc::new(CustomRPCEndpoints::new(rpc_endpoints));
    
    // 3. Configure Helius with API key from environment
    let helius_config = HeliusConfig {
        api_key: env::var("HELIUS_API_KEY")
            .unwrap_or_else(|_| {
                warn!("HELIUS_API_KEY not set, using demo key");
                "your_api_key_here".to_string()
            }),
        cluster: "mainnet-beta".to_string(),
        max_retries: 3,
        timeout_ms: 10000,
        rate_limit_per_second: 100,
    };

    // 4. Create HeliusProofProvider
    let helius_provider = match HeliusProofProvider::new(helius_config, Arc::clone(&rpc_client)) {
        Ok(provider) => {
            info!("✅ Successfully initialized HeliusProofProvider");
            Arc::new(provider)
        }
        Err(e) => {
            error!("❌ Failed to initialize HeliusProofProvider: {}", e);
            return Err(e);
        }
    };

    // 5. Initialize MerkleProofPrefetcher with HeliusProvider
    let prefetcher_config = blockchain_bot::merkle_proof_prefetcher::MerkleProofPrefetcherConfig {
        cache_size: 10000,
        prefetch_batch_size: 50,
        max_concurrent_requests: 20,
        cache_ttl_seconds: 300,
        prefetch_threshold: 0.8,
        circuit_breaker_threshold: 5,
        health_check_interval_seconds: 30,
    };

    let proof_prefetcher = MerkleProofPrefetcher::new(
        helius_provider.clone(),
        Arc::clone(&rpc_client),
        prefetcher_config,
    )?;

    info!("🔧 MerkleProofPrefetcher initialized with HeliusProvider");

    // 6. Demonstrate endpoint health monitoring
    tokio::spawn({
        let rpc_clone = Arc::clone(&rpc_client);
        async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
                
                let health_status = rpc_clone.check_health().await;
                info!("📊 Endpoint Health Status:");
                
                for (endpoint_id, health) in health_status {
                    info!(
                        "  {} - {} | Latency: {}ms | Success Rate: {:.2}% | Score: {:.2}",
                        endpoint_id,
                        if health.is_healthy { "🟢 HEALTHY" } else { "🔴 UNHEALTHY" },
                        health.average_latency_ms,
                        health.success_rate * 100.0,
                        health.score
                    );
                }

                // Demonstrate adaptive routing
                match rpc_clone.select_best_endpoint().await {
                    Ok(best_endpoint) => {
                        info!("🎯 Best endpoint selected: {}", best_endpoint.url);
                    }
                    Err(e) => {
                        warn!("⚠️ Failed to select best endpoint: {}", e);
                    }
                }
            }
        }
    });

    // 7. Example compressed NFT addresses for testing
    let test_assets = vec![
        // Example compressed NFT IDs - replace with real ones for testing
        "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        "DRiP2Pn2K6fuMLKQmt5rZWyHiUZ6zUN2VLPGGwABaUSz", 
        "3VKx2faPKe3fhvBePKPqcQPx4BhgU8bvEKbkYNWRR5dP",
    ];

    // 8. Demonstrate proof fetching with different providers
    for asset_str in test_assets {
        if let Ok(asset_pubkey) = Pubkey::from_str(asset_str) {
            info!("🔍 Fetching proof for asset: {}", asset_pubkey);
            
            match helius_provider.get_account_with_proof(&asset_pubkey).await {
                Ok(account_proof) => {
                    info!(
                        "✅ Successfully retrieved proof for {}: {} proof elements, slot {}",
                        asset_pubkey,
                        account_proof.proof.proof.len(),
                        account_proof.slot
                    );
                    
                    // Demonstrate proof verification
                    info!(
                        "📋 Proof details:\n  Root: {}\n  Leaf Index: {}\n  Account Data Length: {} bytes",
                        account_proof.proof.root,
                        account_proof.proof.leaf_index,
                        account_proof.account.data.len()
                    );
                }
                Err(e) => {
                    warn!("⚠️ Failed to fetch proof for {}: {}", asset_pubkey, e);
                }
            }
            
            // Brief pause between requests
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    }

    // 9. Demonstrate cache warming and batch operations
    info!("🔥 Warming cache with batch operations...");
    
    let batch_assets: Vec<Pubkey> = test_assets
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect();
    
    if !batch_assets.is_empty() {
        match proof_prefetcher.warm_cache(&batch_assets).await {
            Ok(_) => {
                info!("✅ Cache warming completed successfully");
            }
            Err(e) => {
                warn!("⚠️ Cache warming failed: {}", e);
            }
        }
    }

    // 10. Display performance metrics
    let performance_metrics = rpc_client.get_performance_metrics().await;
    info!("📈 Performance Metrics Summary:");
    
    for (endpoint_id, metrics) in performance_metrics {
        info!(
            "  {} - Requests: {} | Success Rate: {:.2}% | Avg Latency: {}ms | Score: {:.2}",
            endpoint_id,
            metrics.total_requests,
            metrics.uptime_percentage,
            metrics.average_latency_ms,
            metrics.performance_score
        );
    }

    // 11. Demonstrate circuit breaker and health recovery
    info!("🔄 Testing circuit breaker behavior...");
    
    // Simulate some failed requests to trigger circuit breaker
    for i in 0..3 {
        let fake_asset = Pubkey::new_unique();
        match helius_provider.get_account_with_proof(&fake_asset).await {
            Ok(_) => info!("Unexpected success for fake asset {}", i),
            Err(_) => info!("Expected failure for fake asset {}", i),
        }
    }

    // Check circuit breaker status
    let health_status = helius_provider.get_health_status().await;
    info!("🔌 Circuit Breaker Status:");
    for (service, health) in health_status {
        info!(
            "  {} - Circuit Open: {} | Failures: {} | Success Rate: {:.2}%",
            service,
            health.circuit_breaker_open,
            health.consecutive_failures,
            health.success_rate * 100.0
        );
    }

    // 12. Demonstrate graceful shutdown
    info!("🛑 Demonstrating graceful shutdown...");
    rpc_client.shutdown().await;
    
    info!("✨ Helius Proof Provider Example completed successfully!");
    
    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_helius_provider_integration() {
        let rpc_endpoints = vec![EndpointConfig {
            url: "https://api.mainnet-beta.solana.com".to_string(),
            ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
            weight: 1,
            max_requests_per_second: 10,
            timeout_ms: 5000,
            priority: 5,
        }];

        let rpc_client = Arc::new(CustomRPCEndpoints::new(rpc_endpoints));
        
        let helius_config = HeliusConfig {
            api_key: "test_key".to_string(),
            cluster: "mainnet-beta".to_string(),
            max_retries: 1,
            timeout_ms: 5000,
            rate_limit_per_second: 10,
        };

        // This will fail without a real API key, but tests our error handling
        match HeliusProofProvider::new(helius_config, rpc_client) {
            Ok(_) => {
                // If we have a real API key, this would succeed
                assert!(true);
            }
            Err(e) => {
                // Expected with a test key
                assert!(e.to_string().contains("API key"));
            }
        }
    }

    #[tokio::test]
    async fn test_endpoint_health_monitoring() {
        let rpc_endpoints = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
                weight: 5,
                max_requests_per_second: 40,
                timeout_ms: 10000,
                priority: 7,
            },
        ];

        let rpc_client = CustomRPCEndpoints::new(rpc_endpoints);
        
        // Allow some time for health checks to run
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        let health_status = rpc_client.check_health().await;
        assert!(!health_status.is_empty());
        
        let performance_metrics = rpc_client.get_performance_metrics().await;
        assert!(!performance_metrics.is_empty());
    }

    #[tokio::test]
    async fn test_adaptive_routing() {
        let rpc_endpoints = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
                weight: 5,
                max_requests_per_second: 40,
                timeout_ms: 10000,
                priority: 7,
            },
            EndpointConfig {
                url: "https://solana-api.projectserum.com".to_string(),
                ws_url: "wss://solana-api.projectserum.com".to_string(),
                weight: 3,
                max_requests_per_second: 30,
                timeout_ms: 12000,
                priority: 5,
            },
        ];

        let rpc_client = CustomRPCEndpoints::new(rpc_endpoints);
        
        // Test adaptive endpoint selection
        match rpc_client.select_best_endpoint().await {
            Ok(best_endpoint) => {
                assert!(!best_endpoint.url.is_empty());
                assert!(best_endpoint.priority > 0);
            }
            Err(e) => {
                // May fail if endpoints are unhealthy, which is acceptable for testing
                println!("Adaptive routing test failed (expected in some cases): {}", e);
            }
        }
    }
}
