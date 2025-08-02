use crate::analytics::kalman_filter_spread_predictor::KalmanFilterSpreadPredictor;
use crate::ornstein_uhlenbeck_mean_reverter::ornstein_uhlenbeck_mean_reverter::OrnsteinUhlenbeckStrategy;
use crate::flashloan_mathematical_supremacy::ito_process_flashloan_modeler::{ITOFlashLoanModel, FlashLoanOpportunity, MarketConditions, SwapRoute};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use anyhow::Result;

pub struct StrategyIntegrator {
    kalman_filter: Arc<KalmanFilterSpreadPredictor>,
    ou_strategy: Arc<OrnsteinUhlenbeckStrategy>,
}

impl StrategyIntegrator {
    pub fn new(
        kalman_filter: Arc<KalmanFilterSpreadPredictor>,
        ou_strategy: Arc<OrnsteinUhlenbeckStrategy>,
    ) -> Self {
        Self {
            kalman_filter,
            ou_strategy,
        }
    }

    pub async fn evaluate_opportunity(
        &self,
        model: &ITOFlashLoanModel,
        market_conditions: &MarketConditions,
        loan_amount: u64,
    ) -> Result<Option<FlashLoanOpportunity>> {
        // Get spread prediction from Kalman filter
        let spread_prediction = self.kalman_filter.predict_spread(
            &model.token_mint,
            1000, // 1 second horizon
        ).await?;

        // Generate signal from OU strategy
        let signal = self.ou_strategy.generate_signal(market_conditions);

        // Check if both strategies agree on execution
        if self.should_execute(&spread_prediction, &signal, market_conditions) {
            let execution_params = self.ou_strategy.calculate_execution_params(&signal, loan_amount as f64);
            
            // Calculate expected profit
            let expected_profit = self.calculate_expected_profit(
                &spread_prediction,
                &execution_params,
                market_conditions,
            )?;

            // Calculate risk score
            let risk_score = self.calculate_risk_score(
                &spread_prediction,
                &signal,
                market_conditions,
            )?;

            Ok(Some(FlashLoanOpportunity {
                token_mint: model.token_mint,
                loan_amount,
                expected_profit,
                risk_score,
                confidence: signal.confidence.min(spread_prediction.model_confidence),
                execution_params: vec![], // To be filled by pathfinding
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs(),
            }))
        } else {
            Ok(None)
        }
    }

    fn should_execute(
        &self,
        spread_prediction: &crate::analytics::kalman_filter_spread_predictor::SpreadPrediction,
        signal: &crate::ornstein_uhlenbeck_mean_reverter::ornstein_uhlenbeck_mean_reverter::Signal,
        market_conditions: &MarketConditions,
    ) -> bool {
        // Both strategies need to have high confidence
        if signal.confidence < 0.8 || spread_prediction.model_confidence < 0.8 {
            return false;
        }

        // Check if OU strategy signals execution
        if matches!(signal.strength, crate::ornstein_uhlenbeck_mean_reverter::ornstein_uhlenbeck_mean_reverter::SignalStrength::None) {
            return false;
        }

        // Check if Kalman filter predicts favorable spread movement
        let spread_improvement = spread_prediction.predicted_spread - market_conditions.spread;
        if spread_improvement.abs() < 0.001 {
            return false;
        }

        // Check market conditions
        if market_conditions.volatility > 0.5 {
            return false; // Too volatile
        }

        if market_conditions.liquidity_score < 0.3 {
            return false; // Not enough liquidity
        }

        true
    }

    fn calculate_expected_profit(
        &self,
        spread_prediction: &crate::analytics::kalman_filter_spread_predictor::SpreadPrediction,
        execution_params: &crate::ornstein_uhlenbeck_mean_reverter::ornstein_uhlenbeck_mean_reverter::ExecutionParams,
        market_conditions: &MarketConditions,
    ) -> Result<u64> {
        let spread_improvement = spread_prediction.predicted_spread - market_conditions.spread;
        let position_size = execution_params.amount_usd;
        
        // Calculate expected profit from spread improvement
        let expected_profit = spread_improvement * position_size;
        
        // Adjust for market conditions
        let volatility_adjustment = 1.0 - market_conditions.volatility;
        let liquidity_adjustment = market_conditions.liquidity_score;
        
        let adjusted_profit = expected_profit * volatility_adjustment * liquidity_adjustment;
        
        Ok(adjusted_profit as u64)
    }

    fn calculate_risk_score(
        &self,
        spread_prediction: &crate::analytics::kalman_filter_spread_predictor::SpreadPrediction,
        signal: &crate::ornstein_uhlenbeck_mean_reverter::ornstein_uhlenbeck_mean_reverter::Signal,
        market_conditions: &MarketConditions,
    ) -> Result<f64> {
        // Combine risk factors from both strategies
        let kalman_risk = 1.0 - spread_prediction.model_confidence;
        let ou_risk = 1.0 - signal.confidence;
        
        // Market condition risks
        let volatility_risk = market_conditions.volatility;
        let liquidity_risk = 1.0 - market_conditions.liquidity_score;
        
        // Weighted combination
        let risk_score = 0.3 * kalman_risk + 
                        0.3 * ou_risk + 
                        0.2 * volatility_risk + 
                        0.2 * liquidity_risk;
        
        Ok(risk_score.clamp(0.0, 1.0))
    }
}
