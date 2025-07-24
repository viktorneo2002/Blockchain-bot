use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Keypair,
    commitment_config::CommitmentConfig,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use rayon::prelude::*;
use nalgebra::{DMatrix, DVector};
use statrs::distribution::{ContinuousCDF, Normal};

const CORRELATION_WINDOW: usize = 120;
const BREAKDOWN_THRESHOLD: f64 = 2.5;
const MIN_OBSERVATIONS: usize = 30;
const DECAY_FACTOR: f64 = 0.94;
const RISK_MULTIPLIER: f64 = 1.5;
const MAX_CORRELATION_AGE_MS: u64 = 5000;
const CORRELATION_UPDATE_INTERVAL_MS: u64 = 100;
const Z_SCORE_THRESHOLD: f64 = 3.0;
const EIGENVALUE_THRESHOLD: f64 = 0.001;
const MAX_PAIRS_TRACKED: usize = 50;
const CORRELATION_CACHE_SIZE: usize = 1000;

#[derive(Debug, Clone)]
pub struct PricePoint {
    pub price: f64,
    pub volume: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct CorrelationPair {
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub correlation: f64,
    pub rolling_correlation: VecDeque<f64>,
    pub mean: f64,
    pub std_dev: f64,
    pub last_update: Instant,
    pub breakdown_score: f64,
    pub regime: CorrelationRegime,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CorrelationRegime {
    Stable,
    Weakening,
    Breaking,
    Broken,
    Recovering,
}

#[derive(Debug, Clone)]
pub struct RiskAdjustment {
    pub position_multiplier: f64,
    pub stop_loss_adjustment: f64,
    pub take_profit_adjustment: f64,
    pub max_exposure: f64,
}

pub struct CorrelationBreakdownProtector {
    correlations: Arc<RwLock<HashMap<(Pubkey, Pubkey), CorrelationPair>>>,
    price_history: Arc<RwLock<HashMap<Pubkey, VecDeque<PricePoint>>>>,
    risk_adjustments: Arc<TokioRwLock<HashMap<(Pubkey, Pubkey), RiskAdjustment>>>,
    correlation_matrix: Arc<RwLock<DMatrix<f64>>>,
    token_indices: Arc<RwLock<HashMap<Pubkey, usize>>>,
    eigenvalues: Arc<RwLock<DVector<f64>>>,
    update_sender: mpsc::Sender<CorrelationUpdate>,
    shutdown: Arc<RwLock<bool>>,
}

#[derive(Debug)]
pub struct CorrelationUpdate {
    pub pair: (Pubkey, Pubkey),
    pub action: UpdateAction,
}

#[derive(Debug)]
pub enum UpdateAction {
    PriceUpdate(Pubkey, PricePoint),
    RecalculateCorrelation,
    AdjustRisk,
}

impl CorrelationBreakdownProtector {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        let (tx, mut rx) = mpsc::channel::<CorrelationUpdate>(10000);
        
        let protector = Self {
            correlations: Arc::new(RwLock::new(HashMap::with_capacity(CORRELATION_CACHE_SIZE))),
            price_history: Arc::new(RwLock::new(HashMap::with_capacity(MAX_PAIRS_TRACKED * 2))),
            risk_adjustments: Arc::new(TokioRwLock::new(HashMap::with_capacity(CORRELATION_CACHE_SIZE))),
            correlation_matrix: Arc::new(RwLock::new(DMatrix::zeros(0, 0))),
            token_indices: Arc::new(RwLock::new(HashMap::with_capacity(MAX_PAIRS_TRACKED * 2))),
            eigenvalues: Arc::new(RwLock::new(DVector::zeros(0))),
            update_sender: tx,
            shutdown: Arc::new(RwLock::new(false)),
        };

        let correlations = protector.correlations.clone();
        let price_history = protector.price_history.clone();
        let risk_adjustments = protector.risk_adjustments.clone();
        let correlation_matrix = protector.correlation_matrix.clone();
        let token_indices = protector.token_indices.clone();
        let eigenvalues = protector.eigenvalues.clone();
        let shutdown = protector.shutdown.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(CORRELATION_UPDATE_INTERVAL_MS));
            
            while !*shutdown.read().unwrap() {
                tokio::select! {
                    Some(update) = rx.recv() => {
                        match update.action {
                            UpdateAction::PriceUpdate(token, price_point) => {
                                Self::update_price_history(&price_history, token, price_point);
                            }
                            UpdateAction::RecalculateCorrelation => {
                                Self::recalculate_all_correlations(
                                    &correlations,
                                    &price_history,
                                    &correlation_matrix,
                                    &token_indices,
                                    &eigenvalues,
                                );
                            }
                            UpdateAction::AdjustRisk => {
                                Self::adjust_all_risks(&correlations, &risk_adjustments).await;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        Self::detect_breakdowns(&correlations, &risk_adjustments).await;
                        Self::clean_stale_data(&price_history, &correlations);
                    }
                }
            }
        });

        protector
    }

    pub async fn update_price(&self, token: Pubkey, price: f64, volume: f64) -> Result<(), Box<dyn std::error::Error>> {
        let price_point = PricePoint {
            price,
            volume,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,
        };

        self.update_sender.send(CorrelationUpdate {
            pair: (token, token),
            action: UpdateAction::PriceUpdate(token, price_point),
        }).await?;

        if self.should_recalculate() {
            self.update_sender.send(CorrelationUpdate {
                pair: (token, token),
                action: UpdateAction::RecalculateCorrelation,
            }).await?;
        }

        Ok(())
    }

    pub async fn get_risk_adjustment(&self, token_a: Pubkey, token_b: Pubkey) -> RiskAdjustment {
        let key = if token_a < token_b { (token_a, token_b) } else { (token_b, token_a) };
        
        self.risk_adjustments.read().await
            .get(&key)
            .cloned()
            .unwrap_or(RiskAdjustment {
                position_multiplier: 1.0,
                stop_loss_adjustment: 1.0,
                take_profit_adjustment: 1.0,
                max_exposure: 1.0,
            })
    }

    pub fn get_correlation(&self, token_a: Pubkey, token_b: Pubkey) -> Option<f64> {
        let key = if token_a < token_b { (token_a, token_b) } else { (token_b, token_a) };
        self.correlations.read().unwrap()
            .get(&key)
            .map(|cp| cp.correlation)
    }

    pub fn get_breakdown_score(&self, token_a: Pubkey, token_b: Pubkey) -> Option<f64> {
        let key = if token_a < token_b { (token_a, token_b) } else { (token_b, token_a) };
        self.correlations.read().unwrap()
            .get(&key)
            .map(|cp| cp.breakdown_score)
    }

    fn update_price_history(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<PricePoint>>>>,
        token: Pubkey,
        price_point: PricePoint,
    ) {
        let mut history = price_history.write().unwrap();
        let token_history = history.entry(token).or_insert_with(|| VecDeque::with_capacity(CORRELATION_WINDOW));
        
        token_history.push_back(price_point);
        
        while token_history.len() > CORRELATION_WINDOW {
            token_history.pop_front();
        }
    }

    fn recalculate_all_correlations(
        correlations: &Arc<RwLock<HashMap<(Pubkey, Pubkey), CorrelationPair>>>,
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<PricePoint>>>>,
        correlation_matrix: &Arc<RwLock<DMatrix<f64>>>,
        token_indices: &Arc<RwLock<HashMap<Pubkey, usize>>>,
        eigenvalues: &Arc<RwLock<DVector<f64>>>,
    ) {
        let history = price_history.read().unwrap();
        let tokens: Vec<Pubkey> = history.keys().cloned().collect();
        
        if tokens.len() < 2 {
            return;
        }

        let mut indices = token_indices.write().unwrap();
        indices.clear();
        for (i, token) in tokens.iter().enumerate() {
            indices.insert(*token, i);
        }
        drop(indices);

        let n = tokens.len();
        let mut matrix = DMatrix::zeros(n, n);

        tokens.par_iter().enumerate().for_each(|(i, token_a)| {
            if let Some(history_a) = history.get(token_a) {
                if history_a.len() >= MIN_OBSERVATIONS {
                    let returns_a = Self::calculate_returns(history_a);
                    
                    for (j, token_b) in tokens.iter().enumerate().skip(i) {
                        if let Some(history_b) = history.get(token_b) {
                            if history_b.len() >= MIN_OBSERVATIONS {
                                let returns_b = Self::calculate_returns(history_b);
                                
                                if let Some(correlation) = Self::calculate_correlation(&returns_a, &returns_b) {
                                    matrix[(i, j)] = correlation;
                                    matrix[(j, i)] = correlation;
                                    
                                    if i != j {
                                        let key = if token_a < token_b { (*token_a, *token_b) } else { (*token_b, *token_a) };
                                        Self::update_correlation_pair(correlations, key, correlation, &returns_a, &returns_b);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        *correlation_matrix.write().unwrap() = matrix.clone();

        if let Ok(eigen) = matrix.symmetric_eigen() {
            *eigenvalues.write().unwrap() = eigen.eigenvalues;
        }
    }

    fn calculate_returns(history: &VecDeque<PricePoint>) -> Vec<f64> {
        if history.len() < 2 {
            return Vec::new();
        }

        history.windows(2)
            .map(|window| {
                let prev = &window[0];
                let curr = &window[1];
                ((curr.price - prev.price) / prev.price).ln()
            })
            .collect()
    }

    fn calculate_correlation(returns_a: &[f64], returns_b: &[f64]) -> Option<f64> {
        if returns_a.len() != returns_b.len() || returns_a.is_empty() {
            return None;
        }

        let n = returns_a.len() as f64;
        let mean_a = returns_a.iter().sum::<f64>() / n;
        let mean_b = returns_b.iter().sum::<f64>() / n;

        let mut cov = 0.0;
        let mut var_a = 0.0;
        let mut var_b = 0.0;

        for i in 0..returns_a.len() {
            let dev_a = returns_a[i] - mean_a;
            let dev_b = returns_b[i] - mean_b;
            cov += dev_a * dev_b;
            var_a += dev_a * dev_a;
            var_b += dev_b * dev_b;
        }

        let std_a = (var_a / n).sqrt();
        let std_b = (var_b / n).sqrt();

        if std_a > 0.0 && std_b > 0.0 {
            Some((cov / n) / (std_a * std_b))
        } else {
            None
        }
    }

    fn update_correlation_pair(
        correlations: &Arc<RwLock<HashMap<(Pubkey, Pubkey), CorrelationPair>>>,
        key: (Pubkey, Pubkey),
        new_correlation: f64,
        returns_a: &[f64],
        returns_b: &[f64],
    ) {
        let mut corr_map = correlations.write().unwrap();
        
        let pair = corr_map.entry(key).or_insert_with(|| CorrelationPair {
            token_a: key.0,
            token_b: key.1,
            correlation: new_correlation,
            rolling_correlation: VecDeque::with_capacity(CORRELATION_WINDOW),
            mean: 0.0,
            std_dev: 0.0,
            last_update: Instant::now(),
            breakdown_score: 0.0,
            regime: CorrelationRegime::Stable,
        });

                pair.rolling_correlation.push_back(new_correlation);
        
        while pair.rolling_correlation.len() > CORRELATION_WINDOW {
            pair.rolling_correlation.pop_front();
        }
        
        if pair.rolling_correlation.len() >= MIN_OBSERVATIONS {
            let corr_vec: Vec<f64> = pair.rolling_correlation.iter().cloned().collect();
            pair.mean = corr_vec.iter().sum::<f64>() / corr_vec.len() as f64;
            
            let variance = corr_vec.iter()
                .map(|&x| (x - pair.mean).powi(2))
                .sum::<f64>() / corr_vec.len() as f64;
            pair.std_dev = variance.sqrt();
            
            let z_score = if pair.std_dev > 0.0 {
                (new_correlation - pair.mean).abs() / pair.std_dev
            } else {
                0.0
            };
            
            pair.breakdown_score = Self::calculate_breakdown_score(
                &pair.rolling_correlation,
                new_correlation,
                pair.mean,
                pair.std_dev,
            );
            
            pair.regime = Self::determine_regime(pair.breakdown_score, z_score, &pair.regime);
        }
        
        pair.correlation = new_correlation;
        pair.last_update = Instant::now();
    }

    fn calculate_breakdown_score(
        rolling_correlation: &VecDeque<f64>,
        current_correlation: f64,
        mean: f64,
        std_dev: f64,
    ) -> f64 {
        if rolling_correlation.len() < MIN_OBSERVATIONS || std_dev == 0.0 {
            return 0.0;
        }

        let mut score = 0.0;
        
        // Z-score component
        let z_score = (current_correlation - mean).abs() / std_dev;
        score += z_score * 0.3;
        
        // Trend component
        let recent_window = rolling_correlation.len() / 4;
        let recent_mean = rolling_correlation.iter()
            .rev()
            .take(recent_window)
            .sum::<f64>() / recent_window as f64;
        let trend_deviation = (recent_mean - mean).abs() / std_dev.max(0.001);
        score += trend_deviation * 0.25;
        
        // Volatility component
        let recent_volatility = Self::calculate_volatility(
            &rolling_correlation.iter().rev().take(recent_window).cloned().collect::<Vec<_>>()
        );
        let historical_volatility = Self::calculate_volatility(
            &rolling_correlation.iter().cloned().collect::<Vec<_>>()
        );
        let volatility_ratio = (recent_volatility / historical_volatility.max(0.001)).min(5.0);
        score += volatility_ratio * 0.25;
        
        // Directional change component
        let direction_changes = Self::count_direction_changes(rolling_correlation);
        let normalized_changes = (direction_changes as f64 / rolling_correlation.len() as f64).min(1.0);
        score += normalized_changes * 0.2;
        
        score.min(10.0)
    }

    fn calculate_volatility(values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        variance.sqrt()
    }

    fn count_direction_changes(values: &VecDeque<f64>) -> usize {
        if values.len() < 3 {
            return 0;
        }
        
        values.windows(3)
            .filter(|w| {
                let trend1 = w[1] - w[0];
                let trend2 = w[2] - w[1];
                trend1.signum() != trend2.signum()
            })
            .count()
    }

    fn determine_regime(
        breakdown_score: f64,
        z_score: f64,
        current_regime: &CorrelationRegime,
    ) -> CorrelationRegime {
        match current_regime {
            CorrelationRegime::Stable => {
                if breakdown_score > BREAKDOWN_THRESHOLD {
                    CorrelationRegime::Weakening
                } else {
                    CorrelationRegime::Stable
                }
            }
            CorrelationRegime::Weakening => {
                if breakdown_score > BREAKDOWN_THRESHOLD * 1.5 {
                    CorrelationRegime::Breaking
                } else if breakdown_score < BREAKDOWN_THRESHOLD * 0.7 {
                    CorrelationRegime::Stable
                } else {
                    CorrelationRegime::Weakening
                }
            }
            CorrelationRegime::Breaking => {
                if z_score > Z_SCORE_THRESHOLD {
                    CorrelationRegime::Broken
                } else if breakdown_score < BREAKDOWN_THRESHOLD {
                    CorrelationRegime::Recovering
                } else {
                    CorrelationRegime::Breaking
                }
            }
            CorrelationRegime::Broken => {
                if breakdown_score < BREAKDOWN_THRESHOLD * 0.8 {
                    CorrelationRegime::Recovering
                } else {
                    CorrelationRegime::Broken
                }
            }
            CorrelationRegime::Recovering => {
                if breakdown_score < BREAKDOWN_THRESHOLD * 0.5 {
                    CorrelationRegime::Stable
                } else if breakdown_score > BREAKDOWN_THRESHOLD * 1.2 {
                    CorrelationRegime::Breaking
                } else {
                    CorrelationRegime::Recovering
                }
            }
        }
    }

    async fn detect_breakdowns(
        correlations: &Arc<RwLock<HashMap<(Pubkey, Pubkey), CorrelationPair>>>,
        risk_adjustments: &Arc<TokioRwLock<HashMap<(Pubkey, Pubkey), RiskAdjustment>>>,
    ) {
        let corr_map = correlations.read().unwrap();
        let mut adjustments = risk_adjustments.write().await;
        
        for (key, pair) in corr_map.iter() {
            let adjustment = Self::calculate_risk_adjustment(pair);
            adjustments.insert(*key, adjustment);
        }
    }

    fn calculate_risk_adjustment(pair: &CorrelationPair) -> RiskAdjustment {
        let base_adjustment = match pair.regime {
            CorrelationRegime::Stable => RiskAdjustment {
                position_multiplier: 1.0,
                stop_loss_adjustment: 1.0,
                take_profit_adjustment: 1.0,
                max_exposure: 1.0,
            },
            CorrelationRegime::Weakening => RiskAdjustment {
                position_multiplier: 0.8,
                stop_loss_adjustment: 0.9,
                take_profit_adjustment: 1.1,
                max_exposure: 0.8,
            },
            CorrelationRegime::Breaking => RiskAdjustment {
                position_multiplier: 0.5,
                stop_loss_adjustment: 0.7,
                take_profit_adjustment: 1.3,
                max_exposure: 0.5,
            },
            CorrelationRegime::Broken => RiskAdjustment {
                position_multiplier: 0.2,
                stop_loss_adjustment: 0.5,
                take_profit_adjustment: 1.5,
                max_exposure: 0.2,
            },
            CorrelationRegime::Recovering => RiskAdjustment {
                position_multiplier: 0.6,
                stop_loss_adjustment: 0.8,
                take_profit_adjustment: 1.2,
                max_exposure: 0.6,
            },
        };
        
        let score_factor = (1.0 - (pair.breakdown_score / 10.0).min(1.0)).max(0.1);
        
        RiskAdjustment {
            position_multiplier: (base_adjustment.position_multiplier * score_factor).max(0.1),
            stop_loss_adjustment: (base_adjustment.stop_loss_adjustment * (2.0 - score_factor)).min(2.0),
            take_profit_adjustment: base_adjustment.take_profit_adjustment,
            max_exposure: (base_adjustment.max_exposure * score_factor).max(0.1),
        }
    }

    async fn adjust_all_risks(
        correlations: &Arc<RwLock<HashMap<(Pubkey, Pubkey), CorrelationPair>>>,
        risk_adjustments: &Arc<TokioRwLock<HashMap<(Pubkey, Pubkey), RiskAdjustment>>>,
    ) {
        let corr_map = correlations.read().unwrap();
        let mut adjustments = risk_adjustments.write().await;
        
        for (key, pair) in corr_map.iter() {
            let adjustment = Self::calculate_risk_adjustment(pair);
            adjustments.insert(*key, adjustment);
        }
    }

    fn clean_stale_data(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<PricePoint>>>>,
        correlations: &Arc<RwLock<HashMap<(Pubkey, Pubkey), CorrelationPair>>>,
    ) {
        let now = Instant::now();
        let max_age = Duration::from_millis(MAX_CORRELATION_AGE_MS);
        
        let mut corr_map = correlations.write().unwrap();
        corr_map.retain(|_, pair| now.duration_since(pair.last_update) < max_age);
        
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let mut history = price_history.write().unwrap();
        for (_, prices) in history.iter_mut() {
            prices.retain(|p| current_time - p.timestamp < MAX_CORRELATION_AGE_MS);
        }
        
        history.retain(|_, prices| !prices.is_empty());
    }

    fn should_recalculate(&self) -> bool {
        let history = self.price_history.read().unwrap();
        
        if history.len() < 2 {
            return false;
        }
        
        let total_points: usize = history.values().map(|v| v.len()).sum();
        total_points % 10 == 0
    }

    pub async fn get_portfolio_risk(&self) -> f64 {
        let eigenvalues = self.eigenvalues.read().unwrap();
        
        if eigenvalues.is_empty() {
            return 1.0;
        }
        
        let total_variance: f64 = eigenvalues.iter().sum();
        let significant_eigenvalues: Vec<f64> = eigenvalues.iter()
            .filter(|&&ev| ev > EIGENVALUE_THRESHOLD)
            .cloned()
            .collect();
        
        if significant_eigenvalues.is_empty() || total_variance == 0.0 {
            return 1.0;
        }
        
        let concentration = significant_eigenvalues[0] / total_variance;
        let effective_number = 1.0 / significant_eigenvalues.iter()
            .map(|&ev| (ev / total_variance).powi(2))
            .sum::<f64>();
        
        let diversification_ratio = effective_number / significant_eigenvalues.len() as f64;
        
        (1.0 - diversification_ratio).max(0.0) * RISK_MULTIPLIER
    }

    pub fn get_regime_distribution(&self) -> HashMap<CorrelationRegime, usize> {
        let correlations = self.correlations.read().unwrap();
        let mut distribution = HashMap::new();
        
        for pair in correlations.values() {
            *distribution.entry(pair.regime.clone()).or_insert(0) += 1;
        }
        
        distribution
    }

    pub async fn emergency_risk_reduction(&self) {
        let mut adjustments = self.risk_adjustments.write().await;
        
        for (_, adjustment) in adjustments.iter_mut() {
            adjustment.position_multiplier *= 0.3;
            adjustment.stop_loss_adjustment *= 0.5;
            adjustment.take_profit_adjustment *= 1.5;
            adjustment.max_exposure *= 0.2;
        }
    }

    pub fn shutdown(&self) {
        *self.shutdown.write().unwrap() = true;
    }
}

impl Drop for CorrelationBreakdownProtector {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    #[tokio::test]
    async fn test_correlation_calculation() {
        let rpc = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let protector = CorrelationBreakdownProtector::new(rpc);
        
        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        
        for i in 0..50 {
            let price_a = 100.0 + (i as f64).sin() * 10.0;
            let price_b = 200.0 + (i as f64).cos() * 20.0;
            
            protector.update_price(token_a, price_a, 1000.0).await.unwrap();
            protector.update_price(token_b, price_b, 2000.0).await.unwrap();
        }
        
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        let correlation = protector.get_correlation(token_a, token_b);
        assert!(correlation.is_some());
    }
}

