use std::collections::{HashMap, HashSet};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::TransactionStatusMeta;
use crate::{OracleBook, DecoderError};

#[derive(Debug, Clone, Default)]
pub struct PerMintDelta {
    pub mint: Pubkey,
    pub delta_raw: i128,     // token units pre-decimals
    pub decimals: u8,
}

#[derive(Debug, Clone, Default)]
pub struct PnlBreakdown {
    pub lamports_delta: i64,
    pub per_mint: Vec<PerMintDelta>,
    pub sol_total: f64,
    pub usd_total: f64,
    pub confidence: f32,
}

pub async fn compute_pnl_with_oracles(
    owners: &HashSet<Pubkey>,
    meta: &TransactionStatusMeta,
    oracle: &dyn OracleBook,
    slot: u64,
) -> Result<PnlBreakdown, DecoderError> {
    let mut out = PnlBreakdown::default();

    // 1) Lamports: best-effort net gain among owner-linked accounts (coarse fallback)
    if meta.pre_balances.len() == meta.post_balances.len() {
        let mut max_gain = 0i64;
        for (pre, post) in meta.pre_balances.iter().zip(meta.post_balances.iter()) {
            let d = (*post as i64 - *pre as i64);
            if d > max_gain { max_gain = d; }
        }
        out.lamports_delta = max_gain;
    }

    // 2) Token deltas (stable path): use meta pre/post token balances
    // UiTokenAmount includes mint, owner, decimals; RPC normalizes this.
    if let (Some(pre_t), Some(post_t)) = (&meta.pre_token_balances, &meta.post_token_balances) {
        // index by (owner, mint, account_index)
        let mut pre_map: HashMap<(String, String, u16), (&str, u8)> = HashMap::new();
        for t in pre_t {
            let owner = t.owner.as_deref().unwrap_or_default().to_string();
            let mint = t.mint.clone();
            let idx = t.account_index;
            let dec = t.ui_token_amount.decimals as u8;
            pre_map.insert((owner, mint, idx), (t.ui_token_amount.amount.as_str(), dec));
        }

        let mut mint_accum: HashMap<Pubkey, (i128, u8)> = HashMap::new();
        for t in post_t {
            let owner = t.owner.as_deref().unwrap_or_default().to_string();
            let mint_str = t.mint.clone();
            let idx = t.account_index;
            let dec = t.ui_token_amount.decimals as u8;
            let post_raw = t.ui_token_amount.amount.parse::<i128>().unwrap_or(0);
            let pre_raw = pre_map.get(&(owner.clone(), mint_str.clone(), idx))
                .map(|(a, _)| a.parse::<i128>().unwrap_or(0)).unwrap_or(0);
            let delta = post_raw - pre_raw;

            // Only aggregate if owner is in the competitor cluster (string->Pubkey parse)
            if let (Ok(mint), true) = (mint_str.parse::<Pubkey>(), owner_in_cluster(&owner, owners)) {
                let e = mint_accum.entry(mint).or_insert((0, dec));
                e.0 += delta;
                e.1 = dec;
            }
        }

        out.per_mint = mint_accum.into_iter()
            .map(|(mint, (delta_raw, decimals))| PerMintDelta { mint, delta_raw, decimals })
            .collect();
    }

    // 3) Oracle conversions: sum per mint into SOL and USD
    // Your OracleBook should accept raw units and handle decimal scaling internally or we do here.
    let mut sol_total = out.lamports_delta as f64 / 1_000_000_000.0;
    let mut qual = oracle.quality();

    for p in &out.per_mint {
        // Normalize to base units (e.g., 10^decimals)
        let units = p.delta_raw;
        // Convert to SOL, then USD
        let sol_contrib = oracle.to_sol(&p.mint, units, slot).await.map_err(|e| DecoderError::Oracle(e))?;
        sol_total += sol_contrib;
    }
    let usd_total = oracle.to_usd(&Pubkey::default(), (sol_total * 1_000_000_000.0) as i128, slot) // optionally implement SOL mint route
        .await
        .unwrap_or(sol_total * 150.0);

    out.sol_total = sol_total;
    out.usd_total = usd_total;
    out.confidence = qual;

    Ok(out)
}

fn owner_in_cluster(owner: &str, cluster: &HashSet<Pubkey>) -> bool {
    if let Ok(pk) = owner.parse::<Pubkey>() {
        cluster.contains(&pk)
    } else {
        false
    }
}
