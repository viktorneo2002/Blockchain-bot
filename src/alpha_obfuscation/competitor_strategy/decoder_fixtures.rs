use super::*;
use solana_sdk::{signature::Signature, pubkey::Pubkey};

#[test]
fn test_raydium_clmm_route() {
    let tx = load_fixture("raydium_clmm");
    let decoder = CompetitorStrategyDecoder::new(Arc::new(RpcClient::new_mock()));
    
    let analysis = decoder.analyze_signature(tx.signature, tx.slot).await.unwrap();
    assert_eq!(analysis.strategy_type, Some(StrategyType::Arbitrage));
    assert!(analysis.pnl_lamports > 0);
}

#[test]
fn test_jupiter_multi_leg() {
    let tx = load_fixture("jupiter_multi_leg");
    let decoder = CompetitorStrategyDecoder::new(Arc::new(RpcClient::new_mock()));
    
    let analysis = decoder.analyze_signature(tx.signature, tx.slot).await.unwrap();
    assert_eq!(analysis.strategy_type, Some(StrategyType::Arbitrage));
    assert!(analysis.pnl_lamports > 0);
}

// Helper function to load test fixtures
fn load_fixture(name: &str) -> VersionedTransaction {
    let path = format!("tests/fixtures/{}.json", name);
    let data = std::fs::read_to_string(path).unwrap();
    serde_json::from_str(&data).unwrap()
}
