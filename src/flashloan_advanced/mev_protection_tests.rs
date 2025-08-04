#[cfg(test)]
mod tests {
    use super::mev_protection::*;
    
    #[test]
    fn test_congestion_ema_update() {
        let mut ema = CongestionEMA::new(0.3);
        ema.update(0.5);
        assert_eq!(ema.value, 0.5);
        
        ema.update(0.8);
        // EMA calculation: 0.3 * 0.8 + 0.7 * 0.5 = 0.24 + 0.35 = 0.59
        assert_eq!(ema.value, 0.59);
    }
    
    #[test]
    fn test_adaptive_jito_tip_calculation() {
        let mut ema = CongestionEMA::new(0.3);
        let tip = calculate_adaptive_jito_tip(128, 1000, 5000000, &mut ema);
        assert!(tip > 0);
    }
    
    #[test]
    fn test_adaptive_priority_fee_calculation() {
        let fee = calculate_adaptive_priority_fee(50.0, 1000.0, 100000);
        assert!(fee >= 100000 && fee <= 5000000);
    }
    
    #[test]
    fn test_raydium_quote_fallback() {
        let raydium_pool = RaydiumPool;
        let orca_router = OrcaRouter;
        
        // Test successful Raydium quote
        let quote = get_quote_with_fallback(&raydium_pool, &orca_router, 1000, &Pubkey::default(), &Pubkey::default()).unwrap();
        assert_eq!(quote, 950); // 5% slippage
        
        // Test Raydium failure with Orca fallback
        let quote = get_quote_with_fallback(&raydium_pool, &orca_router, 1000000001, &Pubkey::default(), &Pubkey::default()).unwrap();
        assert_eq!(quote, 980499999); // 2% slippage on 1000000001
    }
    
    #[test]
    fn test_build_compressed_bundle_message() {
        let transactions = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]];
        let metadata = vec![9, 10, 11, 12];
        
        let result = build_compressed_bundle_message(&transactions, &metadata);
        assert!(result.is_ok());
        
        let bundle_data = result.unwrap();
        assert!(!bundle_data.is_empty());
    }
}
