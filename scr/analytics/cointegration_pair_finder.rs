use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use rayon::prelude::*;
use ndarray::{Array1, Array2};
use nalgebra::{DMatrix, DVector};
use statistical::{mean, standard_deviation};
use ordered_float::OrderedFloat;

const PRICE_HISTORY_WINDOW: usize = 500;
const MIN_OBSERVATIONS: usize = 100;
const COINTEGRATION_THRESHOLD: f64 = 0.05;
const UPDATE_INTERVAL_MS: u64 = 100;
const MAX_PAIRS_TRACKED: usize = 1000;
const ADF_CRITICAL_VALUE_5PCT: f64 = -2.86;
const JOHANSEN_CRITICAL_VALUE_5PCT: f64 = 15.41;
const MIN_CORRELATION: f64 = 0.7;
const MAX_HALF_LIFE: f64 = 24.0;
const Z_SCORE_ENTRY_THRESHOLD: f64 = 2.0;
const PRICE_PRECISION: u64 = 1_000_000;

#[derive(Clone, Debug)]
pub struct PricePoint {
    pub timestamp: u64,
    pub price: f64,
    pub volume: f64,
    pub liquidity: f64,
}

#[derive(Clone, Debug)]
pub struct TokenPair {
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub pool_address: Pubkey,
    pub fee_tier: u16,
}

#[derive(Clone, Debug)]
pub struct CointegrationStats {
    pub adf_statistic: f64,
    pub p_value: f64,
    pub half_life: f64,
    pub mean_reversion_speed: f64,
    pub correlation: f64,
    pub hedge_ratio: f64,
    pub spread_mean: f64,
    pub spread_std: f64,
    pub current_z_score: f64,
    pub johansen_stat: f64,
    pub last_updated: Instant,
}

pub struct CointegrationPairFinder {
    price_history: Arc<RwLock<HashMap<Pubkey, VecDeque<PricePoint>>>>,
    cointegrated_pairs: Arc<RwLock<HashMap<(Pubkey, Pubkey), CointegrationStats>>>,
    token_metadata: Arc<RwLock<HashMap<Pubkey, TokenMetadata>>>,
    rpc_client: Arc<RpcClient>,
    last_update: Arc<RwLock<Instant>>,
}

#[derive(Clone, Debug)]
struct TokenMetadata {
    pub decimals: u8,
    pub symbol: String,
    pub market_cap: f64,
    pub daily_volume: f64,
}

impl CointegrationPairFinder {
    pub fn new(rpc_endpoint: &str) -> Self {
        Self {
            price_history: Arc::new(RwLock::new(HashMap::new())),
            cointegrated_pairs: Arc::new(RwLock::new(HashMap::new())),
            token_metadata: Arc::new(RwLock::new(HashMap::new())),
            rpc_client: Arc::new(RpcClient::new_with_timeout(
                rpc_endpoint.to_string(),
                Duration::from_secs(10),
            )),
            last_update: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub fn update_price(&self, token: Pubkey, price_point: PricePoint) -> Result<(), Box<dyn std::error::Error>> {
        let mut history = self.price_history.write().unwrap();
        let token_history = history.entry(token).or_insert_with(|| VecDeque::with_capacity(PRICE_HISTORY_WINDOW));
        
        if token_history.len() >= PRICE_HISTORY_WINDOW {
            token_history.pop_front();
        }
        
        token_history.push_back(price_point);
        
        if history.len() > MAX_PAIRS_TRACKED * 2 {
            self.cleanup_stale_data(&mut history);
        }
        
        Ok(())
    }

    pub fn find_cointegrated_pairs(&self) -> Vec<(TokenPair, CointegrationStats)> {
        let history = self.price_history.read().unwrap();
        let tokens: Vec<Pubkey> = history.keys().cloned().collect();
        
        let mut results = Vec::new();
        let pairs: Vec<(usize, usize)> = (0..tokens.len())
            .flat_map(|i| (i+1..tokens.len()).map(move |j| (i, j)))
            .collect();

        let cointegrated: Vec<_> = pairs
            .par_iter()
            .filter_map(|(i, j)| {
                let token_a = &tokens[*i];
                let token_b = &tokens[*j];
                
                if let (Some(prices_a), Some(prices_b)) = (history.get(token_a), history.get(token_b)) {
                    if prices_a.len() >= MIN_OBSERVATIONS && prices_b.len() >= MIN_OBSERVATIONS {
                        if let Some(stats) = self.test_cointegration(prices_a, prices_b) {
                            if self.is_valid_pair(&stats) {
                                return Some(((*token_a, *token_b), stats));
                            }
                        }
                    }
                }
                None
            })
            .collect();

        let mut cointegrated_pairs = self.cointegrated_pairs.write().unwrap();
        for ((token_a, token_b), stats) in cointegrated {
            cointegrated_pairs.insert((token_a, token_b), stats.clone());
            
            let pair = TokenPair {
                token_a,
                token_b,
                pool_address: self.derive_pool_address(token_a, token_b),
                fee_tier: 30,
            };
            
            results.push((pair, stats));
        }

        results.sort_by_key(|(_, stats)| OrderedFloat(-stats.mean_reversion_speed));
        results.truncate(50);
        
        results
    }

    fn test_cointegration(&self, prices_a: &VecDeque<PricePoint>, prices_b: &VecDeque<PricePoint>) -> Option<CointegrationStats> {
        let min_len = prices_a.len().min(prices_b.len());
        if min_len < MIN_OBSERVATIONS {
            return None;
        }

        let series_a: Vec<f64> = prices_a.iter().take(min_len).map(|p| p.price.ln()).collect();
        let series_b: Vec<f64> = prices_b.iter().take(min_len).map(|p| p.price.ln()).collect();

        let correlation = self.calculate_correlation(&series_a, &series_b);
        if correlation.abs() < MIN_CORRELATION {
            return None;
        }

        let hedge_ratio = self.calculate_hedge_ratio(&series_a, &series_b)?;
        let spread = self.calculate_spread(&series_a, &series_b, hedge_ratio);
        
        let adf_result = self.augmented_dickey_fuller(&spread)?;
        if adf_result.statistic > ADF_CRITICAL_VALUE_5PCT {
            return None;
        }

        let johansen_stat = self.johansen_test(&series_a, &series_b)?;
        if johansen_stat < JOHANSEN_CRITICAL_VALUE_5PCT {
            return None;
        }

        let half_life = self.calculate_half_life(&spread)?;
        if half_life > MAX_HALF_LIFE {
            return None;
        }

        let spread_mean = mean(&spread);
        let spread_std = standard_deviation(&spread, Some(spread_mean));
        let current_spread = spread.last().copied().unwrap_or(spread_mean);
        let current_z_score = (current_spread - spread_mean) / spread_std;

        Some(CointegrationStats {
            adf_statistic: adf_result.statistic,
            p_value: adf_result.p_value,
            half_life,
            mean_reversion_speed: 0.693 / half_life,
            correlation,
            hedge_ratio,
            spread_mean,
            spread_std,
            current_z_score,
            johansen_stat,
            last_updated: Instant::now(),
        })
    }

    fn calculate_correlation(&self, series_a: &[f64], series_b: &[f64]) -> f64 {
        let n = series_a.len() as f64;
        let mean_a = series_a.iter().sum::<f64>() / n;
        let mean_b = series_b.iter().sum::<f64>() / n;

        let covariance: f64 = series_a.iter()
            .zip(series_b.iter())
            .map(|(a, b)| (a - mean_a) * (b - mean_b))
            .sum::<f64>() / n;

        let std_a = (series_a.iter().map(|a| (a - mean_a).powi(2)).sum::<f64>() / n).sqrt();
        let std_b = (series_b.iter().map(|b| (b - mean_b).powi(2)).sum::<f64>() / n).sqrt();

        covariance / (std_a * std_b)
    }

    fn calculate_hedge_ratio(&self, series_a: &[f64], series_b: &[f64]) -> Option<f64> {
        let n = series_a.len();
        let x = DMatrix::from_fn(n, 2, |i, j| {
            if j == 0 { 1.0 } else { series_b[i] }
        });
        let y = DVector::from_fn(n, |i, _| series_a[i]);

        let xt_x = &x.transpose() * &x;
        let xt_x_inv = xt_x.try_inverse()?;
        let xt_y = &x.transpose() * &y;
        let beta = xt_x_inv * xt_y;

        Some(beta[(1, 0)])
    }

    fn calculate_spread(&self, series_a: &[f64], series_b: &[f64], hedge_ratio: f64) -> Vec<f64> {
        series_a.iter()
            .zip(series_b.iter())
            .map(|(a, b)| a - hedge_ratio * b)
            .collect()
    }

    fn augmented_dickey_fuller(&self, series: &[f64]) -> Option<AdfResult> {
        let n = series.len();
        if n < 3 {
            return None;
        }

        let mut diffs: Vec<f64> = Vec::with_capacity(n - 1);
        for i in 1..n {
            diffs.push(series[i] - series[i-1]);
        }

        let lagged: Vec<f64> = series[..n-1].to_vec();
        
        let x = DMatrix::from_fn(n - 1, 2, |i, j| {
            if j == 0 { 1.0 } else { lagged[i] }
        });
        let y = DVector::from_slice(&diffs);

        let xt_x = &x.transpose() * &x;
        let xt_x_inv = xt_x.try_inverse()?;
        let xt_y = &x.transpose() * &y;
        let beta = xt_x_inv * xt_y;

        let residuals = &y - &x * &beta;
        let rss = residuals.dot(&residuals);
        let se = (rss / (n as f64 - 3.0)).sqrt();
        
        let var_beta = se * se * xt_x_inv[(1, 1)];
        let t_stat = beta[(1, 0)] / var_beta.sqrt();

        let p_value = if t_stat < -3.5 { 0.001 }
        else if t_stat < -3.0 { 0.01 }
        else if t_stat < -2.5 { 0.05 }
        else { 0.1 };

        Some(AdfResult {
            statistic: t_stat,
            p_value,
        })
    }

    fn johansen_test(&self, series_a: &[f64], series_b: &[f64]) -> Option<f64> {
        let n = series_a.len();
        let data = DMatrix::from_fn(n, 2, |i, j| {
            if j == 0 { series_a[i] } else { series_b[i] }
        });

        let diffs = DMatrix::from_fn(n - 1, 2, |i, j| {
            data[(i + 1, j)] - data[(i, j)]
        });

        let lagged = data.rows(0, n - 1);
        
        let cov_matrix = diffs.transpose() * &diffs / (n as f64 - 1.0);
        let eigenvalues = cov_matrix.symmetric_eigenvalues();
        
        let trace_stat = -((n - 1) as f64) * eigenvalues.iter()
            .map(|&lambda| (1.0 - lambda).ln())
            .sum::<f64>();

        Some(trace_stat)
    }

    fn calculate_half_life(&self, spread: &[f64]) -> Option<f64> {
        let n = spread.len();
        let lagged: Vec<f64> = spread[..n-1].to_vec();
        let changes: Vec<f64> = (1..n).map(|i| spread[i] - spread[i-1]).collect();

        let x = DMatrix::from_fn(n - 1, 2, |i, j| {
            if j == 0 { 1.0 } else { lagged[i] }
        });
        let y = DVector::from_slice(&changes);

                let xt_x = &x.transpose() * &x;
        let xt_x_inv = xt_x.try_inverse()?;
        let xt_y = &x.transpose() * &y;
        let beta = xt_x_inv * xt_y;

        let lambda = beta[(1, 0)];
        if lambda >= 0.0 || lambda <= -1.0 {
            return None;
        }

        Some((-lambda.ln()).abs())
    }

    fn is_valid_pair(&self, stats: &CointegrationStats) -> bool {
        stats.adf_statistic < ADF_CRITICAL_VALUE_5PCT &&
        stats.p_value < COINTEGRATION_THRESHOLD &&
        stats.half_life > 0.1 && 
        stats.half_life < MAX_HALF_LIFE &&
        stats.correlation.abs() > MIN_CORRELATION &&
        stats.johansen_stat > JOHANSEN_CRITICAL_VALUE_5PCT &&
        stats.spread_std > 0.0 &&
        stats.hedge_ratio.is_finite() &&
        stats.hedge_ratio.abs() > 0.1 &&
        stats.hedge_ratio.abs() < 10.0
    }

    fn derive_pool_address(&self, token_a: Pubkey, token_b: Pubkey) -> Pubkey {
        let (mint_a, mint_b) = if token_a < token_b {
            (token_a, token_b)
        } else {
            (token_b, token_a)
        };

        let seeds = &[
            b"pool",
            mint_a.as_ref(),
            mint_b.as_ref(),
            &30u16.to_le_bytes(),
        ];

        let (pool_address, _) = Pubkey::find_program_address(
            seeds,
            &Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
        );

        pool_address
    }

    fn cleanup_stale_data(&self, history: &mut HashMap<Pubkey, VecDeque<PricePoint>>) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let stale_threshold = current_time - 3600;

        history.retain(|_, price_history| {
            if let Some(last_point) = price_history.back() {
                last_point.timestamp > stale_threshold && price_history.len() >= MIN_OBSERVATIONS / 2
            } else {
                false
            }
        });
    }

    pub fn get_trading_signals(&self) -> Vec<TradingSignal> {
        let pairs = self.cointegrated_pairs.read().unwrap();
        let mut signals = Vec::new();

        for ((token_a, token_b), stats) in pairs.iter() {
            if stats.last_updated.elapsed() > Duration::from_secs(60) {
                continue;
            }

            let signal_strength = self.calculate_signal_strength(stats);
            
            if stats.current_z_score.abs() > Z_SCORE_ENTRY_THRESHOLD {
                let signal = TradingSignal {
                    token_a: *token_a,
                    token_b: *token_b,
                    action: if stats.current_z_score > 0.0 {
                        SignalAction::ShortSpread
                    } else {
                        SignalAction::LongSpread
                    },
                    hedge_ratio: stats.hedge_ratio,
                    confidence: signal_strength,
                    expected_return: self.calculate_expected_return(stats),
                    time_to_reversion: stats.half_life,
                    risk_score: self.calculate_risk_score(stats),
                };

                if signal.confidence > 0.7 && signal.risk_score < 0.3 {
                    signals.push(signal);
                }
            }
        }

        signals.sort_by_key(|s| OrderedFloat(-s.expected_return));
        signals.truncate(10);
        
        signals
    }

    fn calculate_signal_strength(&self, stats: &CointegrationStats) -> f64 {
        let z_score_factor = (-stats.current_z_score.abs() / 3.0).exp();
        let half_life_factor = (-stats.half_life / 12.0).exp();
        let correlation_factor = stats.correlation.abs();
        let adf_factor = (ADF_CRITICAL_VALUE_5PCT / stats.adf_statistic).min(1.0);

        let strength = 0.3 * z_score_factor + 
                      0.25 * half_life_factor + 
                      0.25 * correlation_factor + 
                      0.2 * adf_factor;

        strength.min(1.0).max(0.0)
    }

    fn calculate_expected_return(&self, stats: &CointegrationStats) -> f64 {
        let z_score_abs = stats.current_z_score.abs();
        let expected_move = (z_score_abs - 1.0).max(0.0) * stats.spread_std;
        let time_factor = (-stats.half_life / 24.0).exp();
        
        expected_move * time_factor * 0.8
    }

    fn calculate_risk_score(&self, stats: &CointegrationStats) -> f64 {
        let z_score_risk = (stats.current_z_score.abs() / 4.0).min(1.0);
        let volatility_risk = (stats.spread_std / 0.1).min(1.0);
        let time_risk = (stats.half_life / MAX_HALF_LIFE).min(1.0);
        let hedge_ratio_risk = ((stats.hedge_ratio.abs() - 1.0).abs() / 3.0).min(1.0);

        (0.3 * z_score_risk + 
         0.3 * volatility_risk + 
         0.2 * time_risk + 
         0.2 * hedge_ratio_risk).min(1.0).max(0.0)
    }

    pub fn update_metadata(&self, token: Pubkey, metadata: TokenMetadata) {
        let mut meta_map = self.token_metadata.write().unwrap();
        meta_map.insert(token, metadata);
    }

    pub fn get_pair_statistics(&self, token_a: Pubkey, token_b: Pubkey) -> Option<CointegrationStats> {
        let pairs = self.cointegrated_pairs.read().unwrap();
        pairs.get(&(token_a, token_b))
            .or_else(|| pairs.get(&(token_b, token_a)))
            .cloned()
    }

    pub fn get_top_pairs(&self, limit: usize) -> Vec<((Pubkey, Pubkey), CointegrationStats)> {
        let pairs = self.cointegrated_pairs.read().unwrap();
        let mut sorted_pairs: Vec<_> = pairs.iter()
            .filter(|(_, stats)| self.is_valid_pair(stats))
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        sorted_pairs.sort_by(|a, b| {
            let score_a = a.1.mean_reversion_speed / a.1.half_life;
            let score_b = b.1.mean_reversion_speed / b.1.half_life;
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        sorted_pairs.truncate(limit);
        sorted_pairs
    }

    pub fn monitor_pair_health(&self) -> HashMap<(Pubkey, Pubkey), PairHealth> {
        let pairs = self.cointegrated_pairs.read().unwrap();
        let mut health_map = HashMap::new();

        for ((token_a, token_b), stats) in pairs.iter() {
            let health = PairHealth {
                is_cointegrated: stats.adf_statistic < ADF_CRITICAL_VALUE_5PCT,
                last_update: stats.last_updated,
                z_score_trend: self.calculate_z_score_trend(*token_a, *token_b),
                volume_trend: self.calculate_volume_trend(*token_a, *token_b),
                stability_score: self.calculate_stability_score(stats),
            };

            health_map.insert((*token_a, *token_b), health);
        }

        health_map
    }

    fn calculate_z_score_trend(&self, token_a: Pubkey, token_b: Pubkey) -> f64 {
        let history = self.price_history.read().unwrap();
        
        if let (Some(prices_a), Some(prices_b)) = (history.get(&token_a), history.get(&token_b)) {
            if prices_a.len() >= 10 && prices_b.len() >= 10 {
                let recent_spreads: Vec<f64> = prices_a.iter()
                    .rev()
                    .take(10)
                    .zip(prices_b.iter().rev().take(10))
                    .map(|(a, b)| a.price.ln() - b.price.ln())
                    .collect();

                if recent_spreads.len() >= 2 {
                    let first_half_mean = mean(&recent_spreads[..recent_spreads.len()/2]);
                    let second_half_mean = mean(&recent_spreads[recent_spreads.len()/2..]);
                    return (second_half_mean - first_half_mean) / first_half_mean.abs().max(0.01);
                }
            }
        }
        
        0.0
    }

    fn calculate_volume_trend(&self, token_a: Pubkey, token_b: Pubkey) -> f64 {
        let history = self.price_history.read().unwrap();
        
        if let (Some(prices_a), Some(prices_b)) = (history.get(&token_a), history.get(&token_b)) {
            let recent_volume_a: f64 = prices_a.iter().rev().take(10).map(|p| p.volume).sum();
            let recent_volume_b: f64 = prices_b.iter().rev().take(10).map(|p| p.volume).sum();
            
            let older_volume_a: f64 = prices_a.iter().rev().skip(10).take(10).map(|p| p.volume).sum();
            let older_volume_b: f64 = prices_b.iter().rev().skip(10).take(10).map(|p| p.volume).sum();
            
            if older_volume_a > 0.0 && older_volume_b > 0.0 {
                let recent_total = recent_volume_a + recent_volume_b;
                let older_total = older_volume_a + older_volume_b;
                return (recent_total - older_total) / older_total;
            }
        }
        
        0.0
    }

    fn calculate_stability_score(&self, stats: &CointegrationStats) -> f64 {
        let adf_score = (ADF_CRITICAL_VALUE_5PCT / stats.adf_statistic.min(-0.1)).min(1.0);
        let half_life_score = (1.0 - stats.half_life / MAX_HALF_LIFE).max(0.0);
        let spread_score = (1.0 - stats.spread_std / 0.2).max(0.0);
        let correlation_score = stats.correlation.abs();

        (adf_score * 0.3 + half_life_score * 0.3 + spread_score * 0.2 + correlation_score * 0.2).min(1.0)
    }

    pub fn should_update(&self) -> bool {
        let last_update = self.last_update.read().unwrap();
        last_update.elapsed() > Duration::from_millis(UPDATE_INTERVAL_MS)
    }

    pub fn mark_updated(&self) {
        let mut last_update = self.last_update.write().unwrap();
        *last_update = Instant::now();
    }
}

#[derive(Clone, Debug)]
pub struct TradingSignal {
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub action: SignalAction,
    pub hedge_ratio: f64,
    pub confidence: f64,
    pub expected_return: f64,
    pub time_to_reversion: f64,
    pub risk_score: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SignalAction {
    LongSpread,
    ShortSpread,
}

#[derive(Clone, Debug)]
pub struct PairHealth {
    pub is_cointegrated: bool,
    pub last_update: Instant,
    pub z_score_trend: f64,
    pub volume_trend: f64,
    pub stability_score: f64,
}

#[derive(Clone, Debug)]
struct AdfResult {
    statistic: f64,
    p_value: f64,
}

use std::str::FromStr;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cointegration_detection() {
        let finder = CointegrationPairFinder::new("https://api.mainnet-beta.solana.com");
        assert!(finder.should_update());
    }
}

