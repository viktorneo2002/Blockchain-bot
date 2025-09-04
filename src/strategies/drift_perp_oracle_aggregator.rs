use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use solana_client::{
    nonblocking::{rpc_client::RpcClient, pubsub_client::PubsubClient},
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
};
use solana_account_decoder::UiAccountEncoding;
use solana_sdk::pubkey::Pubkey;
use drift::state::oracle::OracleSource;
use tokio::sync::RwLock;
use solana_account_decoder::parse_account_data;
use pyth_sdk_solana::state::load_price_account;
use switchboard_solana::AggregatorAccountData;

#[derive(Clone, Debug)]
pub struct SafeOraclePrice {
    pub price: u64,
    pub conf: u64,
    pub slot_delay: u64,
    pub ts_delay: u64,
    pub source: OracleSource,
    pub received_at: Instant,
}

pub struct OracleAggregator {
    rpc: Arc<RpcClient>,
    cache: Arc<RwLock<HashMap<Pubkey, SafeOraclePrice>>>,
    max_slot_staleness: u64,
    max_ts_staleness_secs: u64,
    max_conf_bps: u64,
    max_oracle_vs_mark_bps: u64,
}

impl OracleAggregator {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        Self {
            rpc,
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_slot_staleness: 20,
            max_ts_staleness_secs: 20,
            max_conf_bps: 80,
            max_oracle_vs_mark_bps: 100,
        }
    }

    pub async fn get_safe_price(
        &self,
        market_oracle: &Pubkey,
        oracle_source: &OracleSource,
        current_slot: u64,
        mark_hint: Option<u64>,
        now_ts: i64,
    ) -> Result<SafeOraclePrice, ArbitrageError> {
        let mut candidates = Vec::new();

        // Primary source
        if let Ok(p) = self.fetch_one(market_oracle, oracle_source, current_slot, now_ts).await {
            candidates.push(p);
        }

        // Optional secondary source (example: Switchboard fallback if Pyth primary)
        if let OracleSource::Pyth | OracleSource::PythStableCoin = oracle_source {
            if let Ok(p) = self.fetch_switchboard_fallback(market_oracle, current_slot, now_ts).await {
                candidates.push(p);
            }
        }

        // Select best candidate by lowest conf as % of price
        let mut best = None;
        for mut c in candidates {
            if !self.pass_staleness(&c, current_slot, now_ts) {
                continue;
            }

            if !self.pass_confidence(&c) {
                continue;
            }

            if let Some(mark) = mark_hint {
                if self.price_deviation_bps(c.price, mark) > self.max_oracle_vs_mark_bps {
                    continue;
                }
            }

            c.received_at = Instant::now();
            best = Some(c);
            break;
        }

        best.ok_or_else(|| ArbitrageError::OracleError("No safe oracle price".to_string()))
    }

    fn pass_staleness(&self, p: &SafeOraclePrice, current_slot: u64, now_ts: i64) -> bool {
        p.slot_delay <= self.max_slot_staleness && 
        p.ts_delay <= self.max_ts_staleness_secs
    }

    fn pass_confidence(&self, p: &SafeOraclePrice) -> bool {
        if p.price == 0 {
            return false;
        }
        
        let conf_bps = (p.conf as u128)
            .saturating_mul(10_000)
            .saturating_div(p.price as u128) as u64;
            
        conf_bps <= self.max_conf_bps
    }

    fn price_deviation_bps(&self, a: u64, b: u64) -> u64 {
        if a == 0 || b == 0 {
            return u64::MAX;
        }
        
        let diff = (a as i128 - b as i128).abs() as u128;
        (diff.saturating_mul(10_000) / (a.max(b) as u128)) as u64
    }

    async fn fetch_one(
        &self,
        oracle: &Pubkey,
        oracle_source: &OracleSource,
        current_slot: u64,
        now_ts: i64,
    ) -> Result<SafeOraclePrice, ArbitrageError> {
        let account = self.rpc
            .get_account_with_commitment(oracle, CommitmentConfig::confirmed())
            .await?
            .value
            .ok_or_else(|| ArbitrageError::OracleError("Oracle account not found".into()))?;

        match oracle_source {
            OracleSource::Pyth | OracleSource::PythStableCoin => {
                let price_account = load_price_account(&account.data)
                    .map_err(|e| ArbitrageError::OracleError(format!("Pyth error: {}", e)))?;
                
                Ok(SafeOraclePrice {
                    price: price_account.agg.price as u64,
                    conf: price_account.agg.conf as u64,
                    slot_delay: current_slot.saturating_sub(price_account.valid_slot),
                    ts_delay: now_ts.saturating_sub(price_account.timestamp) as u64,
                    source: oracle_source.clone(),
                    received_at: Instant::now(),
                })
            }
            OracleSource::Switchboard => {
                let agg_data = AggregatorAccountData::new_from_bytes(&account.data)
                    .map_err(|e| ArbitrageError::OracleError(format!("Switchboard error: {}", e)))?;
                
                Ok(SafeOraclePrice {
                    price: agg_data.get_result()?.as_u64(),
                    conf: agg_data.get_current_round()?.std_deviation.as_u64(),
                    slot_delay: current_slot.saturating_sub(agg_data.latest_confirmed_round.round_open_slot),
                    ts_delay: now_ts.saturating_sub(agg_data.latest_confirmed_round.round_open_timestamp) as u64,
                    source: oracle_source.clone(),
                    received_at: Instant::now(),
                })
            }
            _ => Err(ArbitrageError::OracleError("Unsupported oracle source".into())),
        }
    }

    async fn fetch_switchboard_fallback(
        &self,
        _oracle: &Pubkey,
        _current_slot: u64,
        _now_ts: i64,
    ) -> Result<SafeOraclePrice, ArbitrageError> {
        // TODO: Implement Switchboard fallback logic
        Err(ArbitrageError::OracleError("No SB fallback configured".into()))
    }
}
