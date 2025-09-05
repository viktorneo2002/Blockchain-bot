use serde::Deserialize;
use std::path::Path;
use crate::TransactionAnalysis;

#[derive(Debug, Deserialize)]
pub struct LabeledTx {
    pub signature: String,
    pub expected_strategy_contains: String,
    pub expected_min_usd: f64,
}

#[derive(Default, Debug)]
pub struct BacktestStats {
    pub total: usize,
    pub matched: usize,
    pub pnl_pass: usize,
}

pub async fn run_backtest<P: AsRef<Path>, F>(
    fixture_dir: P,
    list_file: P,
    mut analyze: F,
) -> anyhow::Result<BacktestStats>
where
    F: FnMut(&str) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<TransactionAnalysis>> + Send>>,
{
    let list_str = std::fs::read_to_string(list_file)?;
    let labels: Vec<LabeledTx> = serde_json::from_str(&list_str)?;
    let mut stats = BacktestStats::default();

    for lbl in labels {
        stats.total += 1;
        let sig = lbl.signature.clone();
        let res = analyze(&sig).await;
        match res {
            Ok(ta) => {
                let matched = format!("{:?}", ta.strategy).contains(&lbl.expected_strategy_contains);
                if matched { stats.matched += 1; }
                if ta.pnl_usd >= lbl.expected_min_usd { stats.pnl_pass += 1; }
            }
            Err(_e) => { /* count as miss */ }
        }
    }
    Ok(stats)
}
