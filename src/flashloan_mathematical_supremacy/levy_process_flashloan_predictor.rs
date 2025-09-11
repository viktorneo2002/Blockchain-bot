// p90 μ-lamports/CU from cluster; optional but improves first-try inclusion
async fn cluster_p90_fee_micros(rpc: &AsyncRpcClient) -> Option<u64> {
    use solana_client::rpc_response::RpcPrioritizationFee;
    let fees = rpc.get_recent_prioritization_fees(None).await.ok()?;
    if fees.is_empty() { return None; }
    let mut v: Vec<u64> = fees
        .into_iter()
        .map(|f: RpcPrioritizationFee| f.prioritization_fee as u64)
        .collect();
    v.sort_unstable();
    let idx = (v.len().saturating_sub(1) * 90) / 100;
    v.get(idx).cloned()
}

// === [IMPROVED: PIECE ϜR0] Core types, helpers, and slippage-safe min_out ===
#[derive(Clone, Debug)]
pub struct Quote {
    pub out: u64,              // best estimate of tokens out (exact-in)
    pub pool_fee_bps: u64,     // venue fee in bps
    pub route_name: &'static str,
    pub cu_estimate: u32,      // expected compute units
    pub impact_bps: u64,       // micro price impact proxy
}

#[inline]
fn bps_mul(amount: u64, bps: u64) -> u64 {
    ((amount as u128)
        .saturating_mul(bps.min(1_000_000) as u128)
        .saturating_div(10_000u128)) as u64
}

#[inline]
fn clamp_sub(a: u64, b: u64) -> u64 { a.saturating_sub(b) }

#[inline]
fn safe_min_out(quoted_out: u64, slippage_bps: u64) -> u64 {
    let slip = bps_mul(quoted_out, slippage_bps.min(9_999));
    clamp_sub(quoted_out, slip).max(1)
}

// host-router <-> on-chain ARB program identifiers
#[derive(BorshSerialize, BorshDeserialize, Clone, Copy, Debug)]
pub enum RouteKind { Phoenix, OpenBookV2, RaydiumClmm, OrcaWhirlpool }

// payload your ARB program expects (exact-in)
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
pub struct RouteSwapData {
    pub kind: RouteKind,
    pub in_mint: Pubkey,
    pub out_mint: Pubkey,
    pub qty_in: u64,
    pub min_out: u64,
    pub pool_fee_bps: u64,
    pub flags: u32, // exact-in = 1<<0
}

// unified adapter surface (builds full CPI into venue via your ARB program)
pub trait DexRoute: Send + Sync {
    fn quote_exact_in(&self, in_mint: Pubkey, out_mint: Pubkey, qty_in: u64) -> Option<Quote>;
    fn build_swap_ixs(
        &self,
        payer: &Pubkey,
        user_src_token: &Pubkey,
        user_dst_token: &Pubkey,
        in_mint: Pubkey,
        out_mint: Pubkey,
        qty_in: u64,
        min_out: u64,
        arb_program: &Pubkey,
    ) -> Vec<Instruction>;
}

// === [IMPROVED: PIECE ϜR1] Phoenix adapter ===
#[derive(Clone)]
pub struct PhoenixAdapter {
    pub program_id: Pubkey, // Phoenix program
    pub market: Pubkey,     // Phoenix market
    pub taker_fee_bps: u64, // venue taker fee
}

impl DexRoute for PhoenixAdapter {
    #[inline]
    fn quote_exact_in(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty_in: u64) -> Option<Quote> {
        let fee = bps_mul(qty_in, self.taker_fee_bps);
        let impact = bps_mul(qty_in, 2); // conservative buffer
        Some(Quote {
            out: qty_in.saturating_sub(fee.saturating_add(impact)),
            pool_fee_bps: self.taker_fee_bps,
            route_name: "phoenix",
            cu_estimate: 140_000,
            impact_bps: 2,
        })
    }

    #[inline]
    fn build_swap_ixs(
        &self,
        payer: &Pubkey,
        user_src_token: &Pubkey,
        user_dst_token: &Pubkey,
        in_mint: Pubkey,
        out_mint: Pubkey,
        qty_in: u64,
        min_out: u64,
        arb_program: &Pubkey,
    ) -> Vec<Instruction> {
        let data = RouteSwapData {
            kind: RouteKind::Phoenix,
            in_mint, out_mint, qty_in, min_out: min_out.max(1),
            pool_fee_bps: self.taker_fee_bps,
            flags: 1,
        }.try_to_vec().expect("RouteSwapData");

        vec![Instruction {
            program_id: *arb_program,
            accounts: vec![
                AccountMeta::new_readonly(*payer, true),
                AccountMeta::new(*user_src_token, false),
                AccountMeta::new(*user_dst_token, false),
                AccountMeta::new_readonly(self.program_id, false),
                AccountMeta::new(self.market, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        }]
    }
}

// === [IMPROVED: PIECE ϜR2] OpenBook v2 adapter ===
#[derive(Clone)]
pub struct OpenBookV2Adapter {
    pub program_id: Pubkey,
    pub market: Pubkey,
    pub taker_fee_bps: u64,
}

impl DexRoute for OpenBookV2Adapter {
    #[inline]
    fn quote_exact_in(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty_in: u64) -> Option<Quote> {
        let fee = bps_mul(qty_in, self.taker_fee_bps);
        let impact = bps_mul(qty_in, 3);
        Some(Quote {
            out: qty_in.saturating_sub(fee.saturating_add(impact)),
            pool_fee_bps: self.taker_fee_bps,
            route_name: "openbook_v2",
            cu_estimate: 110_000,
            impact_bps: 3,
        })
    }

    #[inline]
    fn build_swap_ixs(
        &self,
        payer: &Pubkey,
        user_src_token: &Pubkey,
        user_dst_token: &Pubkey,
        in_mint: Pubkey,
        out_mint: Pubkey,
        qty_in: u64,
        min_out: u64,
        arb_program: &Pubkey,
    ) -> Vec<Instruction> {
        let data = RouteSwapData {
            kind: RouteKind::OpenBookV2,
            in_mint, out_mint, qty_in, min_out: min_out.max(1),
            pool_fee_bps: self.taker_fee_bps,
            flags: 1,
        }.try_to_vec().expect("RouteSwapData");

        vec![Instruction {
            program_id: *arb_program,
            accounts: vec![
                AccountMeta::new_readonly(*payer, true),
                AccountMeta::new(*user_src_token, false),
                AccountMeta::new(*user_dst_token, false),
                AccountMeta::new_readonly(self.program_id, false),
                AccountMeta::new(self.market, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        }]
    }
}

// === [IMPROVED: PIECE ϜR3] Raydium CLMM adapter ===
#[derive(Clone)]
pub struct RaydiumClmmAdapter {
    pub program_id: Pubkey,
    pub pool: Pubkey,
    pub fee_bps: u64, // pool fee tier
}

impl DexRoute for RaydiumClmmAdapter {
    #[inline]
    fn quote_exact_in(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty_in: u64) -> Option<Quote> {
        let fee = bps_mul(qty_in, self.fee_bps);
        let impact = bps_mul(qty_in, 2);
        Some(Quote {
            out: qty_in.saturating_sub(fee.saturating_add(impact)),
            pool_fee_bps: self.fee_bps,
            route_name: "raydium_clmm",
            cu_estimate: 180_000,
            impact_bps: 2,
        })
    }

    #[inline]
    fn build_swap_ixs(
        &self,
        payer: &Pubkey,
        user_src_token: &Pubkey,
        user_dst_token: &Pubkey,
        in_mint: Pubkey,
        out_mint: Pubkey,
        qty_in: u64,
        min_out: u64,
        arb_program: &Pubkey,
    ) -> Vec<Instruction> {
        let data = RouteSwapData {
            kind: RouteKind::RaydiumClmm,
            in_mint, out_mint, qty_in, min_out: min_out.max(1),
            pool_fee_bps: self.fee_bps,
            flags: 1,
        }.try_to_vec().expect("RouteSwapData");

        vec![Instruction {
            program_id: *arb_program,
            accounts: vec![
                AccountMeta::new_readonly(*payer, true),
                AccountMeta::new(*user_src_token, false),
                AccountMeta::new(*user_dst_token, false),
                AccountMeta::new_readonly(self.program_id, false),
                AccountMeta::new(self.pool, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        }]
    }
}

// === [IMPROVED: PIECE ϜR4] Orca Whirlpool adapter ===
#[derive(Clone)]
pub struct OrcaWhirlpoolAdapter {
    pub program_id: Pubkey,
    pub pool: Pubkey,
    pub fee_bps: u64,
}

impl DexRoute for OrcaWhirlpoolAdapter {
    #[inline]
    fn quote_exact_in(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty_in: u64) -> Option<Quote> {
        let fee = bps_mul(qty_in, self.fee_bps);
        let impact = bps_mul(qty_in, 3);
        Some(Quote {
            out: qty_in.saturating_sub(fee.saturating_add(impact)),
            pool_fee_bps: self.fee_bps,
            route_name: "orca_whirlpool",
            cu_estimate: 210_000,
            impact_bps: 3,
        })
    }

    #[inline]
    fn build_swap_ixs(
        &self,
        payer: &Pubkey,
        user_src_token: &Pubkey,
        user_dst_token: &Pubkey,
        in_mint: Pubkey,
        out_mint: Pubkey,
        qty_in: u64,
        min_out: u64,
        arb_program: &Pubkey,
    ) -> Vec<Instruction> {
        let data = RouteSwapData {
            kind: RouteKind::OrcaWhirlpool,
            in_mint, out_mint, qty_in, min_out: min_out.max(1),
            pool_fee_bps: self.fee_bps,
            flags: 1,
        }.try_to_vec().expect("RouteSwapData");

        vec![Instruction {
            program_id: *arb_program,
            accounts: vec![
                AccountMeta::new_readonly(*payer, true),
                AccountMeta::new(*user_src_token, false),
                AccountMeta::new(*user_dst_token, false),
                AccountMeta::new_readonly(self.program_id, false),
                AccountMeta::new(self.pool, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        }]
    }
}

// === [IMPROVED: PIECE ϜR5] Best-route picker ===
pub struct RoutePick<'a> {
    pub adapter_idx: usize,
    pub quote: Quote,
    pub min_out: u64,
}

#[inline]
pub fn pick_best_route<'a>(
    routes: &'a [Box<dyn DexRoute>],
    in_mint: Pubkey,
    out_mint: Pubkey,
    qty_in: u64,
    slippage_bps: u64,
    cu_price_microlamports: u64,
) -> Option<RoutePick<'a>> {
    let mut best: Option<(usize, Quote, u128)> = None;
    for (i, r) in routes.iter().enumerate() {
        if let Some(q) = r.quote_exact_in(in_mint, out_mint, qty_in) {
            let min_out = safe_min_out(q.out, slippage_bps);
            let cu_penalty = (q.cu_estimate as u128)
                .saturating_mul(cu_price_microlamports as u128)
                / 1_000u128; // scale-down to token units
            let score = (min_out as u128).saturating_sub(cu_penalty);
            match &mut best {
                None => best = Some((i, q, score)),
                Some((_bi, _bq, bs)) if score > *bs => best = Some((i, q, score)),
                _ => {}
            }
        }
    }
    best.map(|(idx, q, _)| RoutePick { adapter_idx: idx, min_out: safe_min_out(q.out, slippage_bps), quote: q })
}

// === [IMPROVED: PIECE ϜR6] Two-hop composite adapter ===
#[derive(Clone)]
pub struct TwoHopComposite<'a> {
    pub hop1: &'a dyn DexRoute,
    pub hop2: &'a dyn DexRoute,
    pub mid_mint: Pubkey,
    pub route_name: &'static str,
}

impl<'a> TwoHopComposite<'a> {
    #[inline]
    fn split_slippage(total_slip_bps: u64) -> (u64, u64) {
        let a = total_slip_bps / 2;
        let b = total_slip_bps - a;
        (a, b)
    }
}

impl<'a> DexRoute for TwoHopComposite<'a> {
    #[inline]
    fn quote_exact_in(&self, in_mint: Pubkey, out_mint: Pubkey, qty_in: u64) -> Option<Quote> {
        let q1 = self.hop1.quote_exact_in(in_mint, self.mid_mint, qty_in)?;
        let q2 = self.hop2.quote_exact_in(self.mid_mint, out_mint, q1.out)?;
        Some(Quote {
            out: q2.out,
            pool_fee_bps: q1.pool_fee_bps.saturating_add(q2.pool_fee_bps),
            route_name: self.route_name,
            cu_estimate: q1.cu_estimate.saturating_add(q2.cu_estimate).saturating_add(10_000),
            impact_bps: q1.impact_bps.saturating_add(q2.impact_bps),
        })
    }

    #[inline]
    fn build_swap_ixs(
        &self,
        payer: &Pubkey,
        user_src_token: &Pubkey,
        user_dst_token: &Pubkey,
        in_mint: Pubkey,
        out_mint: Pubkey,
        qty_in: u64,
        min_out: u64,
        arb_program: &Pubkey,
    ) -> Vec<Instruction> {
        let (s1, s2) = Self::split_slippage(100);
        let mid_out_est = self.hop1
            .quote_exact_in(in_mint, self.mid_mint, qty_in)
            .map(|q| q.out).unwrap_or(0);
        let min_mid = safe_min_out(mid_out_est, s1);

        let mut ixs = self.hop1.build_swap_ixs(
            payer, user_src_token, user_dst_token,
            in_mint, self.mid_mint, qty_in, min_mid, arb_program,
        );

        let hop2_in = min_mid.max(1);
        let hop2_min_out = safe_min_out(min_out, s2);
        ixs.extend(self.hop2.build_swap_ixs(
            payer, user_src_token, user_dst_token,
            self.mid_mint, out_mint, hop2_in, hop2_min_out, arb_program,
        ));
        ixs
    }
}

// === [IMPROVED: PIECE ϜR7] Router with EWMA penalties + deterministic dither ===
use std::collections::HashMap;

#[derive(Default, Clone)]
pub struct RouteStats {
    pub ewma_succ: f64,
    pub ewma_cu: f64,
    pub count: u64,
}

impl RouteStats {
    #[inline] fn update(&mut self, success: bool, cu_used: u32) {
        let a = 0.20;
        self.ewma_succ = (1.0 - a) * self.ewma_succ + a * if success { 1.0 } else { 0.0 };
        self.ewma_cu   = (1.0 - a) * self.ewma_cu   + a * (cu_used as f64);
        self.count = self.count.saturating_add(1);
    }
}

pub struct Router<'a> {
    routes: Vec<Box<dyn DexRoute + 'a>>,
    stats: HashMap<&'static str, RouteStats>,
    jitter_salt: u64,
}

impl<'a> Router<'a> {
    pub fn new(routes: Vec<Box<dyn DexRoute + 'a>>, jitter_salt: u64) -> Self {
        Self { routes, stats: HashMap::new(), jitter_salt }
    }

    #[inline]
    fn dither(&self, key: &'static str, entropy: u64) -> u64 {
        let mut z = (self.jitter_salt ^ entropy)
            ^ (key.as_bytes().iter().fold(0u64, |h, &b| h.wrapping_mul(131).wrapping_add(b as u64)));
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        (z ^ (z >> 31)) & 0x3ff
    }

    pub fn pick<'b>(
        &'b self,
        in_mint: Pubkey,
        out_mint: Pubkey,
        qty_in: u64,
        slippage_bps: u64,
        cu_price_microlamports: u64,
        entropy: u64,
    ) -> Option<(usize, Quote, u64)> {
        let mut best: Option<(usize, Quote, u128)> = None;
        for (i, r) in self.routes.iter().enumerate() {
            if let Some(q) = r.quote_exact_in(in_mint, out_mint, qty_in) {
                let min_out = safe_min_out(q.out, slippage_bps);
                let mut score = (min_out as u128).saturating_sub(
                    ((q.cu_estimate as u128) * (cu_price_microlamports as u128)) / 1_000u128
                );
                if let Some(st) = self.stats.get(q.route_name) {
                    let succ_pen = ((1.0 - st.ewma_succ.clamp(0.0, 1.0)) * 500.0) as u128;
                    let cu_pen   = (st.ewma_cu.max(0.0) as u128) * (cu_price_microlamports as u128) / 4_000u128;
                    score = score.saturating_sub(succ_pen.saturating_add(cu_pen));
                }
                score = score.saturating_sub(self.dither(q.route_name, entropy) as u128);
                match &mut best {
                    None => best = Some((i, q, score)),
                    Some((_bi, _bq, bs)) if score > *bs => best = Some((i, q, score)),
                    _ => {}
                }
            }
        }
        best.map(|(idx, q, _)| (idx, q.clone(), safe_min_out(q.out, slippage_bps)))
    }

    #[inline]
    pub fn record_result(&mut self, route_name: &'static str, success: bool, cu_used: u32) {
        self.stats.entry(route_name).or_default().update(success, cu_used);
    }

    pub fn routes(&self) -> &[Box<dyn DexRoute + 'a>] { &self.routes }
}

// === [IMPROVED: PIECE ϜR8] Single-call build helper + ATA resolvers ===
#[inline]
pub fn user_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    spl_associated_token_account::get_associated_token_address(owner, mint)
}

pub struct BuiltSwap {
    pub ixs: Vec<Instruction>,
    pub min_out: u64,
    pub route_name: &'static str,
    pub cu_estimate: u32,
}

#[inline]
pub fn build_best_swap_ixs<'a>(
    router: &'a Router<'a>,
    payer: &Pubkey,
    in_mint: Pubkey,
    out_mint: Pubkey,
    qty_in: u64,
    slippage_bps: u64,
    cu_price_microlamports: u64,
    entropy: u64,
    arb_program: &Pubkey,
) -> Option<BuiltSwap> {
    let src_ata = user_ata(payer, &in_mint);
    let dst_ata = user_ata(payer, &out_mint);

    let (idx, q, min_out) = router.pick(
        in_mint, out_mint, qty_in, slippage_bps, cu_price_microlamports, entropy
    )?;

    let r = &router.routes()[idx];
    let ixs = r.build_swap_ixs(
        payer, &src_ata, &dst_ata, in_mint, out_mint, qty_in, min_out, arb_program,
    );

    Some(BuiltSwap { ixs, min_out, route_name: q.route_name, cu_estimate: q.cu_estimate })
}

// Race send to primary + optional secondary RPC (duplicate signature is safe)
async fn race_send(
    primary: &AsyncRpcClient,
    secondary_url: Option<&str>,
    tx: &VersionedTransaction,
    cfg: RpcSendTransactionConfig,
) -> anyhow::Result<Signature> {
    if let Some(url) = secondary_url {
        let secondary = AsyncRpcClient::new(url.to_string());
        let primary_fut = primary.send_transaction_with_config(tx, cfg.clone());
        let secondary_fut = secondary.send_transaction_with_config(tx, cfg.clone());
        tokio::select! {
            r = primary_fut => Ok(r?),
            r = secondary_fut => Ok(r?),
        }
    } else {
        Ok(primary.send_transaction_with_config(tx, cfg).await?)
    }
}

// Percentile fee helper (optionally address-scoped)
async fn pxx_fee_micros(
    rpc: &AsyncRpcClient,
    addrs: Option<Vec<Pubkey>>,
    pct: usize, // 0..=100
) -> Option<u64> {
    use solana_client::rpc_response::RpcPrioritizationFee;
    let fees = rpc.get_recent_prioritization_fees(addrs.as_ref()).await.ok()?;
    if fees.is_empty() { return None; }
    let mut v: Vec<u64> = fees.into_iter()
        .map(|f: RpcPrioritizationFee| f.prioritization_fee as u64)
        .collect();
    v.sort_unstable();
    let idx = (v.len().saturating_sub(1) * pct.min(100)) / 100;
    v.get(idx).cloned()
}

// Collect bounded, de-duplicated account hints from IX list
#[inline(always)]
fn collect_address_hints(ixs: &[Instruction], cap: usize) -> Vec<Pubkey> {
    let mut set: HashSet<Pubkey> = HashSet::with_capacity(cap.max(16));
    for ix in ixs {
        for m in &ix.accounts {
            if set.len() >= cap { break; }
            set.insert(m.pubkey);
        }
        if set.len() >= cap { break; }
    }
    set.into_iter().take(cap).collect()
}

// μ-lamports to lamports (ceiling division)
#[inline(always)]
fn micros_to_lamports_ceil(micros: u64) -> u64 {
    micros.saturating_add(999_999) / 1_000_000
}
// --- Risk, RPC pool, and guardrails ---
#[derive(Debug, Clone, Default)]
pub struct RiskState {
    pub per_mint_position: HashMap<Pubkey, u128>,
    pub daily_pnl: i128,
    pub max_drawdown: i128,
    pub cooldown_until: HashMap<(Pubkey, Pubkey), i64>, // (leader, mint) -> unix secs
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_atomic_arbitrage(
    rpc: &AsyncRpcClient,
    payer: &Keypair,
    alts: &[Pubkey],
    flash_loan_ixs: &[Instruction], // borrow -> route -> repay
    cu_limit: u32,
    base_tip_microlamports: u64,    // priority fee (μ-lamports per CU)
    leader: Pubkey,
    leader_model: &LeaderModel,
) -> anyhow::Result<()> {
    // --- 0) Leader-aware CU price + cluster p90 floor + optional MIN/MAX caps
    let mut tip_micros = choose_tip(&leader, leader_model, base_tip_microlamports);
    if let Some(p90) = cluster_p90_fee_micros(rpc).await {
        if p90 > tip_micros { tip_micros = p90; }
    }
    if let Ok(minv) = std::env::var("MIN_CU_PRICE_MICROS").ok().and_then(|v| v.parse::<u64>().ok()) {
        tip_micros = tip_micros.max(minv);
    }
    if let Ok(maxv) = std::env::var("MAX_CU_PRICE_MICROS").ok().and_then(|v| v.parse::<u64>().ok()) {
        tip_micros = tip_micros.min(maxv);
    }

    // Optional extra tip transfer (Jito or direct) via env
    let extra_tip_ix = (|| -> Option<Instruction> {
        let tip_addr = std::env::var("JITO_TIP_ADDRESS").ok()?;
        let lamports: u64 = std::env::var("EXTRA_TIP_LAMPORTS").ok()?.parse().ok()?;
        let to = Pubkey::from_str(&tip_addr).ok()?;
        (lamports > 0).then(|| system_instruction::transfer(&payer.pubkey(), &to, lamports))
    })();

    // Optional heap frame (env)
    let heap_ix = (|| -> Option<Instruction> {
        if let Ok(bytes) = std::env::var("CU_HEAP_BYTES").ok().and_then(|v| v.parse::<u32>().ok()) {
            Some(ComputeBudgetInstruction::set_heap_frame(bytes))
        } else { None }
    })();

    // --- 1) Fresh blockhash + slot
    let (mut blockhash, slot0) = {
        let bh = rpc.get_latest_blockhash().await?;
        let s  = rpc.get_slot().await.unwrap_or_default();
        (bh, s)
    };

    // --- 2) Build IX list (heap first, tip last)
    let mut all_ixs: Vec<Instruction> = Vec::with_capacity(flash_loan_ixs.len() + 2);
    if let Some(ix) = heap_ix { all_ixs.push(ix); }
    all_ixs.extend_from_slice(flash_loan_ixs);
    if let Some(ix) = extra_tip_ix { all_ixs.push(ix); }

    // Address-scoped and global p90 floors based on the actual accounts touched
    let addr_hints = collect_address_hints(&all_ixs, 128);
    if let Some(p90_scoped) = pxx_fee_micros(rpc, Some(addr_hints), 90).await {
        tip_micros = tip_micros.max(p90_scoped);
    }
    if let Some(p90_global) = pxx_fee_micros(rpc, None, 90).await {
        tip_micros = tip_micros.max(p90_global);
    }
    if let Ok(minv) = std::env::var("MIN_CU_PRICE_MICROS").ok().and_then(|v| v.parse::<u64>().ok()) { tip_micros = tip_micros.max(minv); }
    if let Ok(maxv) = std::env::var("MAX_CU_PRICE_MICROS").ok().and_then(|v| v.parse::<u64>().ok()) { tip_micros = tip_micros.min(maxv); }

    // --- 3) ALTs + v0 build
    let alts_decoded = fetch_alts(rpc, alts).await.unwrap_or_default();
    let alts_decoded = filter_live_alts(alts_decoded, slot0);
    let mut tx = build_v0_tx_with_alts(
        payer, all_ixs.clone(), blockhash, cu_limit, tip_micros, &alts_decoded
    )?;

    // --- 4) Exact simulation (replace blockhash; no sig verify)
    let sim_cfg = RpcSimulateTransactionConfig {
        sig_verify: false,
        replace_recent_blockhash: true,
        commitment: Some(CommitmentConfig::processed()),
        ..Default::default()
    };
    let sim = rpc.simulate_transaction_with_config(&tx, sim_cfg).await?;
    if let Some(err) = sim.value.err.clone() {
        record_exec(false, sim.value.units_consumed.unwrap_or(0), tip_micros, 0.0, &Pubkey::default(), &leader, 0, 0);
        anyhow::bail!("simulation failed: {err:?} | logs: {:?}", sim.value.logs.unwrap_or_default());
    }
    let mut cu_used = sim.value.units_consumed.unwrap_or(0);

    // Adaptive CU headroom: rebuild if within 10% of limit
    if cu_used > (cu_limit as u32 * 90 / 100) {
        let bumped = ((cu_used as f64) * 1.15).ceil() as u32;
        let new_limit = bumped.min(1_800_000);
        if new_limit > cu_limit {
            tx = build_v0_tx_with_alts(payer, all_ixs.clone(), blockhash, new_limit, tip_micros, &alts_decoded)?;
            let sim2 = rpc.simulate_transaction_with_config(&tx, sim_cfg).await?;
            if let Some(err) = sim2.value.err.clone() {
                record_exec(false, sim2.value.units_consumed.unwrap_or(0), tip_micros, 0.0, &Pubkey::default(), &leader, 0, 0);
                anyhow::bail!("simulation failed (after CU bump): {err:?} | logs: {:?}", sim2.value.logs.unwrap_or_default());
            }
            cu_used = sim2.value.units_consumed.unwrap_or(cu_used);
        }
    }

    // --- 5) Send with min_context_slot; skip_preflight (we simulated already)
    let send_cfg = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: None,
        max_retries: Some(4),
        min_context_slot: Some(slot0),
        ..Default::default()
    };
    let send_once = |client: &AsyncRpcClient, txv: &VersionedTransaction| async move {
        client.send_transaction_with_config(txv, send_cfg.clone()).await
    };

    let race_rpc = std::env::var("RACE_RPC_URL").ok();
    let race_rpc = race_rpc.as_deref();

    let sig = match race_send(rpc, race_rpc, &tx, send_cfg.clone()).await {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("{e}");
            let needs_bh = msg.contains("BlockhashNotFound") || msg.contains("blockhash not found");
            let oversized = msg.contains("Transaction too large") || msg.contains("packet too large");
            if oversized { anyhow::bail!("send failed: {msg}"); }
            if needs_bh {
                blockhash = rpc.get_latest_blockhash().await?;
                let tx2 = build_v0_tx_with_alts(payer, all_ixs.clone(), blockhash, cu_limit, tip_micros, &alts_decoded)?;
                race_send(rpc, race_rpc, &tx2, send_cfg.clone()).await
                    .map_err(|e2| anyhow::anyhow!("send failed after blockhash refresh: {e2}"))?
            } else {
                anyhow::bail!("send failed: {msg}");
            }
        }
    };

    // --- 6) Confirmation with 2-stage CU escalator
    // Stage A: 800ms window, poll every ~80ms
    let stage_a_deadline = std::time::Instant::now() + std::time::Duration::from_millis(800);
    let mut landed = false;
    while std::time::Instant::now() < stage_a_deadline {
        if let Ok(resp) = rpc.get_signature_statuses(&[sig]).await {
            if let Some(Some(st)) = resp.value.get(0) {
                if let Some(err) = &st.err {
                    let slot1 = rpc.get_slot().await.unwrap_or(slot0);
                    let slot_delta = slot1.saturating_sub(slot0);
                    record_exec(false, cu_used, tip_micros, 0.0, &Pubkey::default(), &leader, slot_delta, 0);
                    anyhow::bail!("on-chain error: {err:?}");
                }
                if matches!(st.confirmation_status,
                    Some(TransactionConfirmationStatus::Confirmed) |
                    Some(TransactionConfirmationStatus::Finalized)
                ) { landed = true; break; }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    }

    // Stage B (escalate once): if not landed, rebuild with higher CU price & resend
    if !landed {
        // +25% CU price, clamp to sane upper bound
        let escalated = ((tip_micros as f64) * 1.25).ceil() as u64;
        let max_micros: u64 = std::env::var("MAX_CU_PRICE_MICROS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(50_000);
        tip_micros = escalated.min(max_micros);

        // optional refresh of p90 floor before resending
        if let Some(p90b) = cluster_p90_fee_micros(rpc).await {
            tip_micros = tip_micros.max(p90b).min(max_micros);
        }

        blockhash = rpc.get_latest_blockhash().await?;
        let tx_escalated = build_v0_tx_with_alts(
            payer, all_ixs, blockhash, cu_limit, tip_micros, &alts_decoded
        )?;
        let sig2 = race_send(rpc, race_rpc, &tx_escalated, send_cfg.clone()).await
            .unwrap_or(sig); // if send fails, keep watching original sig

        // Another short window: 600ms, poll ~90ms
        let stage_b_deadline = std::time::Instant::now() + std::time::Duration::from_millis(600);
        while std::time::Instant::now() < stage_b_deadline {
            if let Ok(resp) = rpc.get_signature_statuses(&[sig2]).await {
                if let Some(Some(st)) = resp.value.get(0) {
                    if let Some(err) = &st.err {
                        let slot1 = rpc.get_slot().await.unwrap_or(slot0);
                        let slot_delta = slot1.saturating_sub(slot0);
                        record_exec(false, cu_used, tip_micros, 0.0, &Pubkey::default(), &leader, slot_delta, 1);
                        anyhow::bail!("on-chain error (escalated): {err:?}");
                    }
                    if matches!(st.confirmation_status,
                        Some(TransactionConfirmationStatus::Confirmed) |
                        Some(TransactionConfirmationStatus::Finalized)
                    ) { landed = true; break; }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(90)).await;
        }
    }

    // --- 7) Metrics & exit
    let slot1 = rpc.get_slot().await.unwrap_or(slot0);
    let slot_delta = slot1.saturating_sub(slot0);
    record_exec(landed, cu_used, tip_micros, 0.0, &Pubkey::default(), &leader, slot_delta, 1);
    if !landed { anyhow::bail!("tx not confirmed within SLO"); }
    Ok(())
}

#[cfg(test)]
mod replay {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_xoshiro::Xoshiro256Plus;
use borsh::{BorshDeserialize, BorshSerialize};
use solana_sdk::{instruction::{AccountMeta, Instruction}, pubkey::Pubkey};
use solana_client::nonblocking::rpc_client::RpcClient as AsyncRpcClient;

    #[test]
    fn stable_estimator_no_nan() {
        let mut rng = Xoshiro256Plus::seed_from_u64(42);
        let rets: Vec<f64> = (0..10_000).map(|_| {
            (rng.gen::<f64>() - 0.5) * 0.01
        }).collect();
        let (a, b) = LevyProcessPredictor::mcculloch_alpha_beta(&rets);
        assert!(a.is_finite() && b.is_finite());
    }
}

#[inline(always)]
fn allow_trade(risk: &RiskState, mint: &Pubkey, add: u128, caps: (u128, u128)) -> bool {
    let cur = *risk.per_mint_position.get(mint).unwrap_or(&0);
    let Some(next) = cur.checked_add(add) else { return false; };
    if next > caps.0 { return false; }
    let pnl_after_dd = risk.daily_pnl.saturating_sub(risk.max_drawdown);
    pnl_after_dd >= -(caps.1 as i128)
}

#[inline(always)]
fn allow_trade_ext(
    risk: &RiskState,
    leader: &Pubkey,
    mint: &Pubkey,
    add: u128,
    caps: (u128, u128),
    now_unix: i64,
) -> bool {
    if !allow_trade(risk, mint, add, caps) { return false; }
    if let Some(&until) = risk.cooldown_until.get(&(*leader, *mint)) {
        if until > now_unix { return false; }
    }
    true
}

#[derive(Debug, Clone)]
pub struct RpcNode { pub url: String, pub rtt_ms: f64, pub slot: u64, pub healthy: bool }

#[derive(Debug, Clone, Default)]
pub struct RpcPool { pub nodes: Vec<RpcNode> }

#[inline(always)]
fn ewma(prev: f64, newv: f64, alpha: f64) -> f64 { alpha * newv + (1.0 - alpha) * prev }

#[inline(always)]
fn best_rpc_url(pool: &RpcPool, cluster_slot_hint: u64) -> &str {
    const STALE_TOL: u64  = 5;    // allow tiny skew
    const STALE_HARD: u64 = 20;   // hard reject
    pool.nodes
        .iter()
        .filter(|n| n.healthy && cluster_slot_hint.saturating_sub(n.slot) <= STALE_HARD)
        .map(|n| {
            let skew = cluster_slot_hint.saturating_sub(n.slot);
            let freshness = if skew <= STALE_TOL { 1.0 - (skew as f64 / (STALE_TOL as f64 + 1.0)) } else { 0.0 };
            let rtt_penalty = n.rtt_ms.max(0.1).ln(); // stabilizes jitter
            let score = (freshness * 1000.0) - rtt_penalty;
            (score, n.slot, n.url.as_str())
        })
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
              .then_with(|| a.1.cmp(&b.1))
              .then_with(|| a.2.cmp(&b.2)))
        .map(|(_, _, url)| url)
        .unwrap_or("https://api.mainnet-beta.solana.com")
}

#[inline(always)]
fn best_two_rpc_urls<'a>(pool: &'a RpcPool, cluster_slot_hint: u64) -> (&'a str, Option<&'a str>) {
    const STALE_TOL: u64  = 5;
    const STALE_HARD: u64 = 20;
    let mut ranked: Vec<(f64,u64,&str)> = pool.nodes
        .iter()
        .filter(|n| n.healthy && cluster_slot_hint.saturating_sub(n.slot) <= STALE_HARD)
        .map(|n| {
            let skew = cluster_slot_hint.saturating_sub(n.slot);
            let freshness = if skew <= STALE_TOL { 1.0 - (skew as f64 / (STALE_TOL as f64 + 1.0)) } else { 0.0 };
            let rtt_penalty = n.rtt_ms.max(0.1).ln();
            let score = (freshness * 1000.0) - rtt_penalty;
            (score, n.slot, n.url.as_str())
        })
        .collect();
    ranked.sort_by(|a,b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| a.1.cmp(&b.1))
        .then_with(|| a.2.cmp(&b.2)));
    if let Some((_,_,u1)) = ranked.pop() {
        let u2 = ranked.pop().map(|t| t.2);
        (u1, u2)
    } else {
        ("https://api.mainnet-beta.solana.com", None)
    }
}

// Backwards-compatible minimal picker (kept for callers that still use it)
fn best_rpc(pool: &RpcPool) -> &str {
    best_rpc_url(pool, 0)
}

// --- ALT loader (multi-account) + v0 builder ---
async fn fetch_alts(
    rpc: &AsyncRpcClient,
    alt_keys: &[Pubkey],
) -> anyhow::Result<Vec<AddressLookupTableAccount>> {
    if alt_keys.is_empty() { return Ok(vec![]); }
    let resp = rpc.get_multiple_accounts(alt_keys).await?;
    let mut out = Vec::with_capacity(alt_keys.len());
    for opt in resp.value.into_iter().flatten() {
        if let Ok(alt) = AddressLookupTableAccount::deserialize(&opt.data) {
            out.push(alt);
        }
    }
    Ok(out)
}

#[inline(always)]
fn filter_live_alts(
    mut alts: Vec<AddressLookupTableAccount>,
    current_slot: u64,
) -> Vec<AddressLookupTableAccount> {
    alts.retain(|alt| alt.deactivation_slot.map(|s| s > current_slot).unwrap_or(true));
    alts
}

fn build_v0_tx_with_alts(
    payer: &Keypair,
    mut ixs: Vec<Instruction>,
    blockhash: Hash,
    cu_limit: u32,
    cu_price_microlamports: u64,
    alts: &[AddressLookupTableAccount],
) -> anyhow::Result<VersionedTransaction> {
    // Optional compute budget tunables via env (no-ops if unset)
    if let Ok(bytes) = std::env::var("CU_HEAP_BYTES").ok().and_then(|v| v.parse::<u32>().ok()) {
        ixs.insert(0, ComputeBudgetInstruction::set_heap_frame(bytes));
    }
    if let Ok(bytes) = std::env::var("LOADED_ACCOUNTS_DATA_LIMIT").ok().and_then(|v| v.parse::<u32>().ok()) {
        ixs.insert(0, ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(bytes));
    }

    let mut with_cb = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
        ComputeBudgetInstruction::set_compute_unit_price(cu_price_microlamports),
    ];
    with_cb.append(&mut ixs);

    let msg = v0::Message::try_compile(&payer.pubkey(), &with_cb, alts, blockhash)
        .map_err(|e| anyhow::anyhow!("v0 compile failed: {e:?}"))?;
    Ok(VersionedTransaction::new(VersionedMessage::V0(msg), &[payer]))
}

// --- Unified DEX routing primitives ---
#[derive(Clone, Debug)]
pub struct Quote {
    pub out: u64,
    pub pool_fee_bps: u64,
    pub route_name: &'static str,
}

pub trait DexRoute: Send + Sync {
    fn quote(&self, in_mint: Pubkey, out_mint: Pubkey, qty: u64) -> Option<Quote>;
    fn build_swap_ix(&self, payer: &Pubkey, qty: u64, min_out: u64) -> Instruction;
}

#[derive(Clone)]
pub struct PhoenixAdapter {
    pub program_id: Pubkey,
    pub market: Pubkey,
}

impl DexRoute for PhoenixAdapter {
    fn quote(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty: u64) -> Option<Quote> {
        // Placeholder: 4 bps pool fee, 1:1 price
        let fee_bps = 4;
        let fee = qty.saturating_mul(fee_bps) / 10_000;
        Some(Quote { out: qty.saturating_sub(fee), pool_fee_bps: fee_bps, route_name: "phoenix" })
    }
    fn build_swap_ix(&self, payer: &Pubkey, qty: u64, min_out: u64) -> Instruction {
        // Placeholder IX; replace with Phoenix program-specific instruction builder
        Instruction {
            program_id: self.program_id,
            accounts: vec![AccountMeta::new(*payer, true), AccountMeta::new(self.market, false)],
            data: vec![],
        }
    }
}

#[derive(Clone)]
pub struct OpenBookV2Adapter { pub program_id: Pubkey, pub market: Pubkey }
impl DexRoute for OpenBookV2Adapter {
    fn quote(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty: u64) -> Option<Quote> {
        let fee_bps = 22; // 2.2 bps illustrative
        let fee = qty.saturating_mul(fee_bps) / 10_000;
        Some(Quote { out: qty.saturating_sub(fee), pool_fee_bps: fee_bps, route_name: "openbook_v2" })
    }
    fn build_swap_ix(&self, payer: &Pubkey, _qty: u64, _min_out: u64) -> Instruction {
        Instruction { program_id: self.program_id, accounts: vec![AccountMeta::new(*payer, true), AccountMeta::new(self.market, false)], data: vec![] }
    }
}

#[derive(Clone)]
pub struct RaydiumClmmAdapter { pub program_id: Pubkey, pub pool: Pubkey }
impl DexRoute for RaydiumClmmAdapter {
    fn quote(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty: u64) -> Option<Quote> {
        let fee_bps = 30; // 3 bps illustrative
        let fee = qty.saturating_mul(fee_bps) / 10_000;
        Some(Quote { out: qty.saturating_sub(fee), pool_fee_bps: fee_bps, route_name: "raydium_clmm" })
    }
    fn build_swap_ix(&self, payer: &Pubkey, _qty: u64, _min_out: u64) -> Instruction {
        Instruction { program_id: self.program_id, accounts: vec![AccountMeta::new(*payer, true), AccountMeta::new(self.pool, false)], data: vec![] }
    }
}

#[derive(Clone)]
pub struct OrcaWhirlpoolAdapter { pub program_id: Pubkey, pub pool: Pubkey }
impl DexRoute for OrcaWhirlpoolAdapter {
    fn quote(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty: u64) -> Option<Quote> {
        let fee_bps = 60; // 6 bps illustrative
        let fee = qty.saturating_mul(fee_bps) / 10_000;
        Some(Quote { out: qty.saturating_sub(fee), pool_fee_bps: fee_bps, route_name: "orca_whirlpool" })
    }
    fn build_swap_ix(&self, payer: &Pubkey, _qty: u64, _min_out: u64) -> Instruction {
        Instruction { program_id: self.program_id, accounts: vec![AccountMeta::new(*payer, true), AccountMeta::new(self.pool, false)], data: vec![] }
    }
}

#[inline(always)]
fn required_min_profit_lamports(
    flash_fee_bps: u64,
    pool_fee_bps: u64,
    cu_price_micros: u64, // μ-lamports/CU
    cu_used: u32,
    rent: u64,
    safety_bps: u64,
    notional: u64,
) -> u64 {
    let fees_bps = flash_fee_bps.saturating_add(pool_fee_bps).saturating_add(safety_bps);
    let fee = notional.saturating_mul(fees_bps) / 10_000;
    let pri_fee_lamports = micros_to_lamports_ceil(cu_price_micros.saturating_mul(cu_used as u64));
    fee.saturating_add(pri_fee_lamports).saturating_add(rent)
}

#[inline(always)]
fn min_out_with_slip_guard(expected_out: u64, realized_vol_bps: u64, max_slip_bps: u64) -> Option<u64> {
    let band_bps = realized_vol_bps.max(5).min(max_slip_bps); // enforce micro slip floor
    if band_bps >= 10_000 { return None; }
    Some(expected_out.saturating_sub(expected_out.saturating_mul(band_bps) / 10_000))
}

// === [IMPROVED: PIECE ϜR9] Oracle-bounded min_out (price-integrity guard) ===
#[inline]
fn pow10_u128(d: u8) -> u128 { 10u128.saturating_pow(d as u32) }

/// Converts an in_amount (u64, in token-in base units) into token-out base units using decimals only.
#[inline]
fn amount_in_to_out_units(in_amount: u64, in_dec: u8, out_dec: u8) -> u128 {
    let a = in_amount as u128;
    let in_pow  = pow10_u128(in_dec);
    let out_pow = pow10_u128(out_dec);
    a.saturating_mul(out_pow).saturating_div(in_pow.max(1))
}

/// Oracle-bounded minimum out:
/// - oracle_px_out_per_in is out-token per in-token (e.g., 1.0023 OUT per 1 IN)
/// - raises min_out to max(slip_min, oracle_floor), both in out-token base units
#[inline]
pub fn oracle_bounded_min_out(
    quoted_out: u64,
    slippage_bps: u64,
    in_amount: u64,
    in_dec: u8,
    out_dec: u8,
    oracle_px_out_per_in: f64,
    max_dev_bps: u64,
) -> u64 {
    // standard slippage floor from venue quote
    let slip_bps = slippage_bps.min(9_999);
    let slip_min = quoted_out
        .saturating_sub(quoted_out.saturating_mul(slip_bps) / 10_000)
        .max(1);

    // expected out from oracle (scaled integer math with 1e9 price fixed-point)
    let scaled_in = amount_in_to_out_units(in_amount, in_dec, out_dec);
    let px_q = (oracle_px_out_per_in.max(0.0).min(1e12) * 1e9) as u128; // clamp and scale
    let exp_out = scaled_in
        .saturating_mul(px_q)
        .saturating_div(1_000_000_000u128);
    // allow deviation below oracle by max_dev_bps
    let oracle_floor = exp_out
        .saturating_mul((10_000u128 - (max_dev_bps.min(9_999) as u128)))
        .saturating_div(10_000u128) as u64;

    slip_min.max(oracle_floor).max(1)
}

use anchor_client::solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use anchor_lang::prelude::*;
use arrayref::array_ref;
use rayon::prelude::*;
use rust_decimal::prelude::*;
use serum_dex::state::{Market, MarketState};
use solana_client::{
    rpc_client::RpcClient,
    nonblocking::pubsub_client::PubsubClient,
};
use solana_client::nonblocking::rpc_client::RpcClient as AsyncRpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use tokio_stream::StreamExt;
use crossbeam::channel::{unbounded, Receiver};
use solana_program::{
    clock::Clock,
    instruction::{AccountMeta, Instruction},
    program_pack::Pack,
    system_program,
    system_instruction,
};
use solana_transaction_status::TransactionConfirmationStatus;
use solana_sdk::signature::Signature;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    message::{v0, VersionedMessage},
    transaction::VersionedTransaction,
};
use solana_sdk::address_lookup_table_account::AddressLookupTableAccount;
use spl_token::state::Account as TokenAccount;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
    str::FromStr,
};
use parking_lot::{Mutex, RwLock};
use once_cell::sync::{Lazy, OnceCell};
use arc_swap::ArcSwap;
use tracing::{info, warn, error, instrument};
use metrics::{counter, gauge, histogram};
use rand::{Rng, SeedableRng, RngCore};
use rand::rngs::OsRng;
use rand_xoshiro::Xoshiro256Plus;

static SERUM_V3: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str("9xQeWvG816bUx9EPfDdCkEo7arBgG4oMaw9uRc8rkyMt").unwrap());
#[cfg(feature = "openbook_v2")]
static OPENBOOK_V2: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str("srmqPvWfVZF3GQ8UC5QW4LxF25r61x6a4a9L7cVhPwC").unwrap());
#[cfg(feature = "phoenix")]
static PHOENIX: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str("PhoeNiXZ8bq71sC9s8uQDnLMp7stY8nX1WPU6GqsgQv").unwrap());

const LEVY_ALPHA: f64 = 1.7;
const LEVY_BETA: f64 = 0.5;
const LEVY_GAMMA: f64 = 0.01;
const LEVY_DELTA: f64 = 0.0;
const JUMP_THRESHOLD: f64 = 0.015;
const MIN_PROFIT_BPS: u64 = 50;
const MAX_SLIPPAGE_BPS: u64 = 30;
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const MAX_POSITION_SIZE: u64 = 100_000_000_000;
const COOLDOWN_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct LevyParameters {
    alpha: f64,
    beta: f64,
    gamma: f64,
    delta: f64,
    jump_intensity: f64,
    drift: f64,
    volatility: f64,
}

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    // Client-side wallclock for observability (not on-chain Clock)
    timestamp: u64,
    slot: u64,
    bid: f64,
    ask: f64,
    bid_size: f64,
    ask_size: f64,
    last_trade: f64,
    volume: f64,
    spread: f64,
    imbalance: f64,
}

#[derive(Clone, Debug)]
pub struct FlashLoanOpportunity {
    market_a: Pubkey,
    market_b: Pubkey,
    token_mint: Pubkey,
    entry_price: f64,
    exit_price: f64,
    size: u64,
    expected_profit: f64,
    confidence: f64,
    predicted_jump: f64,
    execution_window: u64,
}

pub struct LevyProcessPredictor {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    market_cache: Arc<RwLock<HashMap<Pubkey, MarketState>>>,
    price_history: Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
    // Lock-free latest snapshot map for hot readers
    latest_snapshots: Arc<ArcSwap<Arc<HashMap<Pubkey, MarketSnapshot>>>>,
    levy_params: Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
    pending_opportunities: Arc<Mutex<Vec<FlashLoanOpportunity>>>,
    last_execution: Arc<RwLock<HashMap<Pubkey, Instant>>>,
    leader_model: Arc<RwLock<HashMap<Pubkey, LeaderStats>>>,
    risk_state: Arc<RwLock<RiskState>>,
    rpc_pool: Arc<RwLock<RpcPool>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct LeaderStats { inclusion: f64, p95_tip: u64 }

#[derive(Clone, Debug, Default)]
struct LeaderModel { by_leader: HashMap<Pubkey, LeaderStats> }

fn choose_tip(leader: &Pubkey, model: &LeaderModel, base: u64) -> u64 {
    if let Some(s) = model.by_leader.get(leader) {
        let mult = (1.0 + (1.0 - s.inclusion)).ceil() as u64;
        base.max(s.p95_tip).saturating_mul(mult)
    } else { base }
}

struct MarketStreams {
    updates_rx: Receiver<(Pubkey, Pubkey, Vec<u8>, u64)>,
}

impl MarketStreams {
    fn recv(&self) -> Option<(Pubkey, Pubkey, Vec<u8>, u64)> {
        self.updates_rx.recv().ok()
    }
}

impl LevyProcessPredictor {
    pub fn new(rpc_url: &str, keypair: Keypair) -> Self {
        Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::processed(),
            )),
            keypair: Arc::new(keypair),
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            price_history: Arc::new(RwLock::new(HashMap::new())),
            latest_snapshots: Arc::new(ArcSwap::from_pointee(Arc::new(HashMap::new()))),
            levy_params: Arc::new(RwLock::new(HashMap::new())),
            pending_opportunities: Arc::new(Mutex::new(Vec::new())),
            last_execution: Arc::new(RwLock::new(HashMap::new())),
            leader_model: Arc::new(RwLock::new(HashMap::new())),
            risk_state: Arc::new(RwLock::new(RiskState::default())),
            rpc_pool: Arc::new(RwLock::new(RpcPool::default())),
        }
    }

    pub async fn run(&self) {
        let update_handle = self.spawn_market_updater();
        let predictor_handle = self.spawn_levy_predictor();
        let executor_handle = self.spawn_flashloan_executor();

        tokio::select! {
            _ = update_handle => {},
            _ = predictor_handle => {},
            _ = executor_handle => {},
        }
    }

    fn spawn_market_updater(&self) -> tokio::task::JoinHandle<()> {
        let rpc_client = self.rpc_client.clone();
        let market_cache = self.market_cache.clone();
        let price_history = self.price_history.clone();
        let latest = self.latest_snapshots.clone();

        tokio::spawn(async move {
            // Prefer websocket subscriptions; fallback to polling if WS fails
            let markets = vec![
                "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
                "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6",
                "DZjbn4XC8qoHKikZqzmhemykVzmossoayV9ffbsUqxVj",
                "HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1",
            ];

            let pubkeys: Vec<Pubkey> = markets.iter().filter_map(|s| s.parse().ok()).collect();
            let streams = Self::subscribe_markets_ws("wss://api.mainnet-beta.solana.com/", pubkeys).await.ok();

            if let Some(streams) = streams {
                // WS path: process updates via channel (no locks across await)
                loop {
                    if let Some((m, owner, data, slot)) = streams.recv() {
                        // Parse Serum v3 safely (no blind slicing) with owner validation
                        if let Some(market_state) = Self::parse_market_by_owner(&owner, &data) {
                            // Short critical sections, no awaits inside
                            {
                                let mut cache = market_cache.write();
                                cache.insert(m, market_state.inner);
                            }
                            let snapshot = Self::extract_market_snapshot(&market_state.inner, slot);
                            {
                                let mut history = price_history.write();
                                let mh = history.entry(m).or_insert_with(|| VecDeque::with_capacity(1000));
                                mh.push_back(snapshot.clone());
                                if mh.len() > 1000 { mh.pop_front(); }
                            }
                            // Update lock-free latest snapshots via ArcSwap clone-on-write
                            let current = latest.load();
                            let mut new_map: HashMap<Pubkey, MarketSnapshot> = (**current).clone();
                            new_map.insert(m, snapshot);
                            latest.store(Arc::new(new_map));
                        }
                    } else {
                        // channel closed; break to fallback
                        break;
                    }
                }
            }

            // Fallback polling if WS not available
            loop {
                for market_str in &markets {
                    if let Ok(market_pubkey) = market_str.parse::<Pubkey>() {
                        if let Ok(account) = rpc_client.get_account(&market_pubkey) {
                            if let Some(market_state) = Self::parse_market_by_owner(&account.owner, &account.data) {
                                {
                                    let mut cache = market_cache.write();
                                    cache.insert(market_pubkey, market_state.inner);
                                }
                                let snapshot = Self::extract_market_snapshot(&market_state.inner, 0);
                                {
                                    let mut history = price_history.write();
                                    let market_history = history.entry(market_pubkey).or_insert_with(|| VecDeque::with_capacity(1000));
                                    market_history.push_back(snapshot.clone());
                                    if market_history.len() > 1000 { market_history.pop_front(); }
                                }
                                // Update ArcSwap
                                let current = latest.load();
                                let mut new_map: HashMap<Pubkey, MarketSnapshot> = (**current).clone();
                                new_map.insert(market_pubkey, snapshot);
                                latest.store(Arc::new(new_map));
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }

    async fn subscribe_markets_ws(rpc_ws_url: &str, markets: Vec<Pubkey>) -> anyhow::Result<MarketStreams> {
        use solana_account_decoder::UiAccountEncoding;
        use solana_client::rpc_config::RpcAccountInfoConfig;
        let (tx, rx) = unbounded();
        let client = PubsubClient::new(rpc_ws_url).await?;
        for m in markets {
            let tx = tx.clone();
            let (_sub, mut stream) = client.account_subscribe(
                &m,
                Some(RpcAccountInfoConfig {
                    commitment: Some(CommitmentConfig::processed()),
                    encoding: Some(UiAccountEncoding::Base64),
                    data_slice: None,
                    min_context_slot: None,
                }),
            ).await?;

            tokio::spawn(async move {
                while let Some(update) = stream.next().await {
                    if let Ok(acc) = update.value.decode() {
                        let slot = update.context.slot;
                        let _ = tx.send((m, acc.owner, acc.data, slot));
                    }
                }
            });
        }
        Ok(MarketStreams { updates_rx: rx })
    }

    fn spawn_levy_predictor(&self) -> tokio::task::JoinHandle<()> {
        let price_history = self.price_history.clone();
        let levy_params = self.levy_params.clone();
        let pending_opportunities = self.pending_opportunities.clone();
        let market_cache = self.market_cache.clone();

        tokio::spawn(async move {
            loop {
                let markets: Vec<Pubkey> = {
                    let history = price_history.read();
                    history.keys().cloned().collect()
                };

                for i in 0..markets.len() {
                    for j in i+1..markets.len() {
                        let market_a = markets[i];
                        let market_b = markets[j];

                        if let Some(opportunity) = Self::detect_levy_opportunity(
                            &price_history,
                            &levy_params,
                            &market_cache,
                            market_a,
                            market_b,
                        ) {
                            let mut opportunities = pending_opportunities.lock();
                            if opportunities.len() < 50 {
                                opportunities.push(opportunity);
                            }
                        }
                    }
                }

                Self::update_levy_parameters(&price_history, &levy_params);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    }

    fn spawn_flashloan_executor(&self) -> tokio::task::JoinHandle<()> {
        let rpc_client = self.rpc_client.clone();
        let keypair = self.keypair.clone();
        let pending_opportunities = self.pending_opportunities.clone();
        let last_execution = self.last_execution.clone();

        tokio::spawn(async move {
            loop {
                let opportunity = {
                    let mut opportunities = pending_opportunities.lock();
                    opportunities.sort_by(|a, b| {
                        b.expected_profit.partial_cmp(&a.expected_profit).unwrap()
                    });
                    opportunities.pop()
                };

                if let Some(opp) = opportunity {
                    let should_execute = {
                        let executions = last_execution.read();
                        if let Some(last_time) = executions.get(&opp.token_mint) {
                            last_time.elapsed().as_millis() > COOLDOWN_MS as u128
                        } else {
                            true
                        }
                    };

                    if should_execute && opp.confidence > CONFIDENCE_THRESHOLD {
                        match Self::execute_flashloan(
                            &rpc_client,
                            &keypair,
                            &opp,
                        ).await {
                            Ok(_) => {
                                let mut executions = last_execution.write();
                                executions.insert(opp.token_mint, Instant::now());
                            },
                            Err(_) => {}
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    fn extract_market_snapshot(market: &MarketState, slot: u64) -> MarketSnapshot {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let bid = market.native_pc_bid_quote() as f64;
        let ask = market.native_pc_ask_quote() as f64;
        let spread = (ask - bid) / bid;
        let mid = (bid + ask) / 2.0;

        MarketSnapshot {
            timestamp,
            slot,
            bid,
            ask,
            bid_size: market.native_coin_bid() as f64,
            ask_size: market.native_coin_ask() as f64,
            last_trade: mid,
            volume: 0.0,
            spread,
            imbalance: (market.native_coin_bid() as f64 - market.native_coin_ask() as f64).abs(),
        }
    }

    fn detect_levy_opportunity(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
        levy_params: &Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
        market_cache: &Arc<RwLock<HashMap<Pubkey, MarketState>>>,
        market_a: Pubkey,
        market_b: Pubkey,
    ) -> Option<FlashLoanOpportunity> {
        let history = price_history.read();
        let _params = levy_params.read();
        let _cache  = market_cache.read();

        let ha = history.get(&market_a)?; let hb = history.get(&market_b)?;
        if ha.len() < 120 || hb.len() < 120 { return None; }

        // Robust returns + health gates
        let (ra, ta, stale_ok_a, vr_ok_a) = Self::robust_log_returns(ha, 512);
        let (rb, tb, stale_ok_b, vr_ok_b) = Self::robust_log_returns(hb, 512);
        if ra.len() < 64 || rb.len() < 64 { return None; }
        if !(stale_ok_a && stale_ok_b && vr_ok_a && vr_ok_b) { return None; }

        // HY diagnostics and guards
        let hy_c = Self::hy_corr(&ra, &ta, &rb, &tb);
        let (k_best, hy_corr_best) = Self::best_lead_lag_k(&ra, &ta, &rb, &tb, 3);
        if hy_c.abs() > 0.35 || k_best.abs() > 2 || hy_corr_best.abs() > 0.75 { return None; }

        let a_last = ha.back()?; let b_last = hb.back()?;
        let price_a = (0.5*(a_last.bid + a_last.ask)).max(1e-9);
        let price_b = (0.5*(b_last.bid + b_last.ask)).max(1e-9);
        let spread_a = a_last.spread.max(1e-12);
        let spread_b = b_last.spread.max(1e-12);
        let depth_qty = a_last.bid_size.min(b_last.ask_size).max(1.0);

        // Calibrated jump
        let jp_a = Self::jump_probability_calibrated(&ra, stale_ok_a, vr_ok_a);
        let jp_b = Self::jump_probability_calibrated(&rb, stale_ok_b, vr_ok_b);
        let jump_prob = jp_a.max(jp_b);
        if jump_prob < 0.35 { return None; }

        let (pred_a, stab_a, dir_a) = Self::predict_tail_move_signed(price_a, &ra, a_last.imbalance);
        let (pred_b, stab_b, _dir_b) = Self::predict_tail_move_signed(price_b, &rb, b_last.imbalance);
        if pred_a == 0.0 && pred_b == 0.0 { return None; }
        let _stability_ok = ((stab_a + stab_b) * 0.5).clamp(0.0, 1.0);

        let expected_abs_move = pred_a.abs() + pred_b.abs();
        if expected_abs_move <= 0.0 { return None; }
        let mid = (price_a + price_b) / 2.0;

        // Fee-aware sizing and PnL
        let expected_move_frac = (expected_abs_move / mid).clamp(0.0, 0.50);
        let half_spread_frac = ((spread_a + spread_b) / 2.0 / mid / 2.0).clamp(0.0, 0.03);
        let extra_fee_frac   = Self::env_fee_frac_bps().min(0.02);
        let impact_slope = ((spread_a + spread_b) / 2.0 / mid / depth_qty.max(1.0)).max(1e-9);

        let (size, depth_frac) = Self::optimal_size_quadratic_fee_aware(
            expected_move_frac, half_spread_frac, extra_fee_frac, impact_slope, depth_qty, MAX_POSITION_SIZE,
        );
        if size == 0 { return None; }

        let net_pnl_price = Self::net_expected_pnl_price_units(
            mid, expected_move_frac, half_spread_frac, extra_fee_frac, impact_slope, size, depth_qty
        );
        if net_pnl_price <= 0.0 { return None; }

        // Confidence and execution window
        let slip_ok = {
            let mut sa = 0.0; let mut sb = 0.0; let mut ca = 0.0; let mut cb = 0.0; let alpha = 0.2;
            for x in ha.iter().rev().take(32) { sa = alpha * x.spread + (1.0 - alpha) * sa; ca += 1.0; }
            for x in hb.iter().rev().take(32) { sb = alpha * x.spread + (1.0 - alpha) * sb; cb += 1.0; }
            let baseline = (sa / ca.max(1.0) + sb / cb.max(1.0)) / 2.0;
            let ratio = ((spread_a + spread_b) / 2.0) / (baseline + 1e-12);
            (1.0 / ratio).clamp(0.0, 1.0)
        };
        let hy_decor = (1.0 - hy_c.abs()).clamp(0.0, 1.0);
        let sim_ok = 0.82;
        let confidence = Self::combine_confidence(jump_prob, hy_decor, sim_ok, slip_ok);

        let min_hurdle = (size as f64) * (MIN_PROFIT_BPS as f64 / 10_000.0) * mid;
        if net_pnl_price < min_hurdle { return None; }

        let exec_window = {
            let n = ta.len().min(tb.len());
            let gaps = if n >= 4 {
                let mut gs: Vec<u64> = ta.windows(2).map(|w| w[1].saturating_sub(w[0])).collect();
                let mut gt: Vec<u64> = tb.windows(2).map(|w| w[1].saturating_sub(w[0])).collect();
                if !gs.is_empty() && !gt.is_empty() {
                    let gm = gs.len()/2; let tm = gt.len()/2;
                    let (_, &g_med, _) = gs.select_nth_unstable(gm);
                    let (_, &t_med, _) = gt.select_nth_unstable(tm);
                    g_med.saturating_add(t_med).saturating_div(2)
                } else { 1 }
            } else { 1 };
            let base = 24u64.saturating_add((gaps / 2).min(64));
            let adj  = (base as f64 * (1.0 - (depth_frac*0.3))).round() as u64;
            adj.clamp(16, 128)
        };

        Some(FlashLoanOpportunity {
            market_a,
            market_b,
            token_mint: Pubkey::default(),
            entry_price: price_a,
            exit_price: price_a + pred_a * dir_a.signum(),
            size,
            expected_profit: net_pnl_price,
            confidence,
            predicted_jump: pred_a,
            execution_window: exec_window,
        })
    }

    #[inline]
    fn combine_confidence(jump: f64, decor: f64, sim: f64, slip_ok: f64) -> f64 {
        let w1 = 0.35; let w2 = 0.25; let w3 = 0.25; let w4 = 0.15;
        (w1*jump + w2*decor + w3*sim + w4*slip_ok).clamp(0.0, 0.99)
    }

    fn estimate_levy_parameters(history: &VecDeque<MarketSnapshot>) -> LevyParameters {
        const MIN_TICKS: usize = 16;
        if history.len() < MIN_TICKS {
            return LevyParameters { alpha: LEVY_ALPHA, beta: LEVY_BETA, gamma: LEVY_GAMMA, delta: LEVY_DELTA, jump_intensity: 0.0, drift: 0.0, volatility: 0.0 };
        }

        // 1) Log-returns with pre-averaging (m=2) and inter-tick gaps
        let mut r0: Vec<f64> = Vec::with_capacity(history.len().saturating_sub(1));
        let mut gaps: Vec<u64> = Vec::with_capacity(history.len().saturating_sub(1));
        let mut prev_t = history[0].timestamp.max(1);
        for w in history.windows(2) {
            if w[1].spread <= 0.0 { continue; }
            if (w[1].last_trade - w[0].last_trade).abs() < f64::EPSILON { continue; }
            let rr = (w[1].last_trade / w[0].last_trade).ln();
            if rr.is_finite() {
                r0.push(rr);
                let te = w[1].timestamp.max(1);
                gaps.push(te.saturating_sub(prev_t));
                prev_t = te;
            }
        }
        if r0.len() < MIN_TICKS - 2 {
            return LevyParameters { alpha: LEVY_ALPHA, beta: LEVY_BETA, gamma: LEVY_GAMMA, delta: LEVY_DELTA, jump_intensity: 0.0, drift: 0.0, volatility: 0.0 };
        }
        let mut r: Vec<f64> = Vec::with_capacity(r0.len().saturating_sub(1));
        for w in r0.windows(2) { r.push(0.5 * (w[0] + w[1])); }

        // 2) Staleness and VR quality
        let stale_ok = if !gaps.is_empty() {
            let mut g = gaps.clone();
            let gm = g.len()/2; let (_, &g_med, _) = g.select_nth_unstable(gm);
            let last_gap = *gaps.last().unwrap_or(&g_med);
            last_gap <= g_med.saturating_mul(4).max(1)
        } else { false };
        let (vr_ok, q_quality) = {
            let n = r.len();
            if n < 20 { (false, 0.0) } else {
                let m = 4usize;
                let var1 = r.iter().map(|x| x*x).sum::<f64>() / (n as f64);
                let mut acc = 0.0; let mut cnt = 0usize;
                for i in 0..(n.saturating_sub(m)) { let mut s = 0.0; for k in 0..m { s += r[i+k]; } acc += s*s; cnt += 1; }
                let var_m = if cnt>0 { acc / (cnt as f64) } else { 0.0 };
                if var1 <= 1e-18 { (false, 0.0) } else {
                    let vr = var_m / ((m as f64) * var1);
                    let ok = vr >= 0.60;
                    let q = ((vr - 0.60) / (0.90 - 0.60)).clamp(0.0, 1.0);
                    (ok, q)
                }
            }
        };
        if !stale_ok || !vr_ok {
            return LevyParameters { alpha: LEVY_ALPHA, beta: LEVY_BETA, gamma: LEVY_GAMMA, delta: LEVY_DELTA, jump_intensity: 0.0, drift: 0.0, volatility: 0.0 };
        }

        // 3) Adaptive winsorization via MAD with quality-adjusted clip
        let mut tmp = r.clone(); let mid = tmp.len()/2; let (_, &median, _) = tmp.select_nth_unstable(mid);
        let mut dev: Vec<f64> = r.iter().map(|x| (x - median).abs()).collect(); let dmid = dev.len()/2; let (_, &mad_raw, _) = dev.select_nth_unstable(dmid);
        let scale = (1.4826 * mad_raw.max(1e-12)).max(1e-12);
        let clip_mult = 4.0 + 3.0*q_quality; let clip = clip_mult * scale; let hi = median + clip; let lo = median - clip;
        for x in r.iter_mut() { *x = if *x > hi { hi } else { if *x < lo { lo } else { *x } }; }

        // 4) Quantiles for McCulloch map
        #[inline] fn q_select(v: &Vec<f64>, p: f64) -> f64 { let n=v.len(); let idx=((n as f64-1.0)*p.clamp(0.0,1.0)).round() as usize; let mut c=v.clone(); let (_,val,_) = c.select_nth_unstable(idx.min(n-1)); *val }
        let q05 = q_select(&r, 0.05); let q25 = q_select(&r, 0.25); let q50 = q_select(&r, 0.50); let q75 = q_select(&r, 0.75); let q95 = q_select(&r, 0.95);
        let (alpha_hat, beta_hat) = Self::mcculloch_alpha_beta_calibrated(q05,q25,q50,q75,q95);

        // 5) Trimmed Kahan mean/var on central 80%
        let lo_cut = q_select(&r, 0.10); let hi_cut = q_select(&r, 0.90);
        let core: Vec<f64> = r.into_iter().filter(|x| *x>=lo_cut && *x<=hi_cut).collect();
        let (mut sum, mut csum) = (0.0f64, 0.0f64); for &x in &core { let y=x - csum; let t=sum + y; csum=(t - sum) - y; sum=t; }
        let mean = if core.is_empty() { 0.0 } else { sum / (core.len() as f64) };
        let mut var=0.0f64; let mut c2=0.0f64; for &x in &core { let d=x-mean; let y=d*d - c2; let t=var + y; c2=(t - var) - y; var=t; }
        let sigma_trim = if core.len()>1 { (var / (core.len() as f64)).sqrt() } else { 0.0 };

        // 6) BV vs RV max for sigma
        let rv = Self::bipower_variation_rv(&core);
        let bv = Self::bipower_variation_norm(&core);
        let sigma_bv = rv.max(bv).sqrt();
        let sigma_star = sigma_trim.max(sigma_bv);

        // 7) EVT Hill (top ~5%) blended into jump intensity
        let mut mags: Vec<f64> = core.iter().map(|x| x.abs().max(1e-18)).collect(); let n = mags.len(); let k = ((n as f64)*0.05).round() as usize;
        let alpha_hill = if n > 12 && k >= 6 { mags.select_nth_unstable(n - k); let mut tail = mags.split_off(n - k); tail.sort_by(|a,b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)); let xk = tail.last().copied().unwrap_or(1e-18); let mut s=0.0; for &xi in &tail { s += (xi / xk).ln(); } (1.0 / (s / (k as f64)).max(1e-6)).clamp(0.5, 4.0) } else { 2.0 };
        let j_bns = { let rvn = rv; let bvn = bv.max(0.0); let j=(rvn - bvn).max(0.0); (j / (rvn + 1e-12)).clamp(0.0, 0.99) };
        let heavy = (1.0 - (alpha_hill/2.0).clamp(0.0,1.0)).clamp(0.0, 1.0);
        let jump_intensity = (0.65*j_bns + 0.35*heavy).clamp(0.0, 0.99);

        let gamma = (sigma_star * (alpha_hat / 2.0).sqrt()).max(0.0);
        let delta = q50;
        LevyParameters { alpha: alpha_hat.clamp(1e-3, 2.0), beta: beta_hat.clamp(-0.99, 0.99), gamma, delta, jump_intensity, drift: mean, volatility: sigma_star }
    }

    fn mcculloch_alpha_beta(returns: &[f64]) -> (f64, f64) {
        let mut v = returns.to_vec();
        v.sort_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = |p: f64| -> usize {
            let n = v.len();
            ((p * ((n-1) as f64)).round() as usize).min(n-1)
        };
        let q = |p: f64| v[idx(p)];
        let q05 = q(0.05);
        let q25 = q(0.25);
        let q50 = q(0.50);
        let q75 = q(0.75);
        let q95 = q(0.95);
        let denom = (q75 - q25).abs().max(1e-9);
        let a_hat = ((q95 - q05) / denom).clamp(1e-3, 2.0);
        let num = (q95 + q05) - 2.0*q50;
        let skew = if (q95 - q05).abs() > 1e-12 { (num / (q95 - q05)).clamp(-0.99, 0.99) } else { 0.0 };
        (a_hat, skew)
    }

    #[inline]
    fn bipower_variation(r: &[f64]) -> f64 {
        const MU1: f64 = (2.0 / std::f64::consts::PI).sqrt();
        if r.len() < 2 { return 0.0; }
        let s: f64 = r.windows(2).map(|w| w[0].abs() * w[1].abs()).sum();
        s / (MU1 * MU1)
    }

    // Bipower helpers with finite-sample fix (ΞBV)
    #[inline]
    fn bipower_variation_norm(r: &[f64]) -> f64 {
        const MU1: f64 = (2.0 / std::f64::consts::PI).sqrt();
        let n = r.len(); if n < 2 { return 0.0; }
        let s: f64 = r.windows(2).map(|w| w[0].abs() * w[1].abs()).sum();
        (s / ((n - 1) as f64) / (MU1*MU1)) * (n as f64 / (n as f64 - 0.999))
    }
    #[inline]
    fn bipower_variation_rv(r: &[f64]) -> f64 {
        let n = r.len(); if n == 0 { return 0.0; }
        r.iter().map(|x| x*x).sum::<f64>() / (n as f64)
    }

    #[inline]
    fn jump_probability(returns: &[f64]) -> f64 {
        if returns.len() < 64 { return 0.0; }
        let rv: f64 = returns.iter().map(|x| x * x).sum();
        let bv = Self::bipower_variation(returns);
        let j = (rv - bv).max(0.0);
        (j / (rv + 1e-12)).clamp(0.0, 0.99)
    }

    // ΞJP: calibrated jump probability fuse (BNS × EVT × regime)
    #[inline]
    fn jump_probability_calibrated(returns_winsor: &[f64], stale_ok: bool, vr_ok: bool) -> f64 {
        let n = returns_winsor.len();
        if n < 24 || !stale_ok || !vr_ok { return 0.0; }

        let rv = Self::bipower_variation_rv(returns_winsor);
        let bv = Self::bipower_variation_norm(returns_winsor).max(0.0);
        let bns = ((rv - bv).max(0.0) / (rv + 1e-12)).clamp(0.0, 0.999);

        // EVT Hill on top ~5%
        let mut mags: Vec<f64> = returns_winsor.iter().map(|x| x.abs().max(1e-18)).collect();
        let k = ((n as f64)*0.05).round() as usize;
        let alpha_hill = if n > 12 && k >= 6 {
            mags.select_nth_unstable(n - k);
            let xk = *mags.iter().skip(n - k).min_by(|a,b| a.partial_cmp(b).unwrap()).unwrap_or(&1e-18);
            let mut s=0.0; for &xi in mags.iter().skip(n-k) { s += (xi / xk).ln(); }
            (1.0 / (s / (k as f64)).max(1e-6)).clamp(0.5, 4.0)
        } else { 2.0 };
        let heavy = (1.0 - (alpha_hill/2.0).clamp(0.0,1.0)).clamp(0.0, 1.0);

        let regime = ((n as f64 - 24.0) / 48.0).clamp(0.0, 1.0);
        let x = 2.25*bns + 0.70*heavy + 0.35*regime;
        (1.0 / (1.0 + (-x).exp())).min(0.97)
    }

    fn kendall_tau(a: &[f64], b: &[f64]) -> f64 {
        let n = a.len().min(b.len());
        if n < 2 { return 0.0; }
        let (a, b) = (&a[..n], &b[..n]);
        let mut concordant = 0i64;
        let mut discordant = 0i64;
        for i in 0..n {
            for j in (i+1)..n {
                let da = (a[j] - a[i]).partial_cmp(&0.0).unwrap_or(std::cmp::Ordering::Equal) as i32;
                let db = (b[j] - b[i]).partial_cmp(&0.0).unwrap_or(std::cmp::Ordering::Equal) as i32;
                let prod = da * db;
                if prod > 0 { concordant += 1; }
                else if prod < 0 { discordant += 1; }
            }
        }
        let denom = (concordant + discordant) as f64;
        if denom == 0.0 { 0.0 } else { (concordant as f64 - discordant as f64) / denom }
    }

    fn robust_corr(a: &[f64], b: &[f64]) -> f64 {
        let n = a.len().min(b.len());
        if n < 32 { return 0.0; }
        let a = &a[(a.len()-n)..];
        let b = &b[(b.len()-n)..];
        let mean = |x: &[f64]| x.iter().sum::<f64>() / (x.len() as f64).max(1.0);
        let ma = mean(a);
        let mb = mean(b);
        let (mut cov, mut va, mut vb) = (0.0, 0.0, 0.0);
        for i in 0..n {
            let da = (a[i] - ma).clamp(-10.0, 10.0);
            let db = (b[i] - mb).clamp(-10.0, 10.0);
            cov += da * db;
            va += da * da;
            vb += db * db;
        }
        if va > 1e-12 && vb > 1e-12 {
            cov / (va.sqrt() * vb.sqrt())
        } else {
            // Fallback to Kendall's tau when variance is tiny
            Self::kendall_tau(a, b)
        }
    }

    #[inline]
    fn kelly_fraction(edge: f64, var: f64) -> f64 {
        if var <= 1e-12 { return 0.0; }
        (edge / var).clamp(0.0, 0.25)
    }

    fn optimal_size(
        edge_per_unit: f64,
        var_per_unit: f64,
        max_position: u64,
        depth_qty: f64,
        slip_slope: f64,
    ) -> u64 {
        let f = Self::kelly_fraction(edge_per_unit, var_per_unit);
        // Simple impact guard: limit by slope of slip curve
        let impact_limited = (depth_qty * (0.5 / slip_slope.max(1e-9))).min(depth_qty * 0.1);
        ((f * impact_limited).min(max_position as f64)).max(0.0) as u64
    }

    // === [IMPROVED: PIECE ZΩ1] Robust returns + gates ===
    #[inline]
    fn robust_log_returns(
        h: &VecDeque<MarketSnapshot>,
        max_len: usize,
    ) -> (Vec<f64>, Vec<u64>, bool, bool) {
        if h.len() < 3 { return (vec![], vec![], false, false); }
        let mut r_raw: Vec<f64> = Vec::with_capacity(h.len().saturating_sub(1));
        let mut t_end: Vec<u64> = Vec::with_capacity(h.len().saturating_sub(1));
        let mut gaps:  Vec<u64> = Vec::with_capacity(h.len().saturating_sub(1));
        let mut prev_t = h[0].timestamp.max(1);
        for w in h.windows(2) {
            if w[1].spread <= 0.0 { continue; }
            if (w[1].last_trade - w[0].last_trade).abs() < f64::EPSILON { continue; }
            let rr = (w[1].last_trade / w[0].last_trade).ln();
            if rr.is_finite() {
                r_raw.push(rr);
                let te = w[1].timestamp.max(1);
                t_end.push(te);
                gaps.push(te.saturating_sub(prev_t));
                prev_t = te;
            }
        }
        if r_raw.is_empty() { return (vec![], vec![], false, false); }

        // O(n) median & MAD
        let mut tmp = r_raw.clone();
        let mid = tmp.len()/2;
        let (_, &median, _) = tmp.select_nth_unstable(mid);
        let mut dev: Vec<f64> = r_raw.iter().map(|x| (x - median).abs()).collect();
        let dmid = dev.len()/2;
        let (_, &mad_raw, _) = dev.select_nth_unstable(dmid);
        let scale = 1.4826 * mad_raw.max(1e-12);
        let clip  = 6.0 * scale;
        let hi = median + clip;
        let lo = median - clip;
        for x in r_raw.iter_mut() { *x = if *x > hi { hi } else { if *x < lo { lo } else { *x } }; }

        // staleness vs median gap
        let stale_ok = if !gaps.is_empty() {
            let mut g = gaps.clone();
            let gm = g.len()/2;
            let (_, &g_med, _) = g.select_nth_unstable(gm);
            let last_gap = *gaps.last().unwrap_or(&g_med);
            last_gap <= g_med.saturating_mul(4).max(1)
        } else { false };

        // variance-ratio microstructure gate (m=4)
        let vr_ok = {
            let n = r_raw.len();
            if n < 20 { false } else {
                let m = 4usize;
                let var1 = r_raw.iter().map(|x| x*x).sum::<f64>() / (n as f64);
                let mut varm_acc = 0.0; let mut cnt = 0usize;
                for i in 0..(n.saturating_sub(m)) {
                    let mut s = 0.0; for k in 0..m { s += r_raw[i+k]; }
                    varm_acc += s*s; cnt += 1;
                }
                let var_m = if cnt > 0 { varm_acc / (cnt as f64) } else { 0.0 };
                let vr = if var1 > 1e-18 { var_m / (m as f64 * var1) } else { 0.0 };
                vr >= 0.60
            }
        };

        let (r_final, t_final) = if r_raw.len() > max_len {
            let start = r_raw.len() - max_len;
            (r_raw[start..].to_vec(), t_end[start..].to_vec())
        } else { (r_raw, t_end) };
        (r_final, t_final, stale_ok, vr_ok)
    }

    // Kahan-compensated HY covariance and corr
    #[inline]
    fn hy_covariance(ra: &[f64], ta: &[u64], rb: &[f64], tb: &[u64]) -> f64 {
        if ra.is_empty() || rb.is_empty() { return 0.0; }
        let (mut i, mut j) = (0usize, 0usize);
        let mut a_prev = ta[0].saturating_sub(1);
        let mut b_prev = tb[0].saturating_sub(1);
        let mut cov = 0.0f64;
        let mut c   = 0.0f64;
        while i < ra.len() && j < rb.len() {
            let a_end = ta[i]; let b_end = tb[j];
            if (a_end > b_prev) && (b_end > a_prev) {
                let y = ra[i]*rb[j] - c;
                let t = cov + y;
                c = (t - cov) - y;
                cov = t;
                if a_end <= b_end { a_prev = a_end; i += 1; } else { b_prev = b_end; j += 1; }
            } else if a_end <= b_prev { a_prev = a_end; i += 1; }
            else { b_prev = b_end; j += 1; }
        }
        cov
    }

    // Unified name (ZΩX): keep a wrapper for compatibility where callers expect `hy_cov`
    #[inline]
    fn hy_cov(ra: &[f64], ta: &[u64], rb: &[f64], tb: &[u64]) -> f64 {
        Self::hy_covariance(ra, ta, rb, tb)
    }

    #[inline]
    fn hy_corr(ra: &[f64], ta: &[u64], rb: &[f64], tb: &[u64]) -> f64 {
        if ra.is_empty() || rb.is_empty() { return 0.0; }
        let cov = Self::hy_cov(ra, ta, rb, tb);
        let rv_a = ra.iter().map(|x| x*x).sum::<f64>().max(1e-18);
        let rv_b = rb.iter().map(|x| x*x).sum::<f64>().max(1e-18);
        (cov / (rv_a.sqrt() * rv_b.sqrt())).clamp(-0.999, 0.999)
    }

    #[inline]
    fn best_lead_lag_k(ra: &[f64], ta: &[u64], rb: &[f64], tb: &[u64], kmax: usize) -> (isize, f64) {
        if ra.len() < 16 || rb.len() < 16 { return (0, 0.0); }
        let mut best_k = 0isize; let mut best_abs = 0.0;
        let clamp = |r:&[f64], t:&[u64], k:isize| -> (&[f64], &[u64]) {
            if k==0 { return (r,t); }
            if k>0 { let kk=k as usize; if r.len()<=kk {(&[],&[])} else {(&r[kk..],&t[kk..])} }
            else   { let kk=(-k) as usize; if r.len()<=kk {(&[],&[])} else {(&r[..r.len()-kk],&t[..t.len()-kk])} }
        };
        for k in -(kmax as isize)..=(kmax as isize) {
            let (rb_s, tb_s) = clamp(rb, tb, k);
            if rb_s.len()<8 { continue; }
            let c = Self::hy_corr(ra,ta,rb_s,tb_s).abs();
            if c>best_abs { best_abs=c; best_k=k; }
        }
        (best_k, best_abs)
    }

    // === [IMPROVED: PIECE ZΩ2] EVT tails and direction ===
    #[inline]
    fn tail_quantile_abs_select(mut r: Vec<f64>, q: f64) -> f64 {
        if r.is_empty() { return 0.0; }
        for x in r.iter_mut() { *x = x.abs(); }
        let n = r.len();
        let idx = ((n as f64 - 1.0) * q.clamp(0.0, 1.0)).round() as usize;
        let (_, qv, _) = r.select_nth_unstable(idx.min(n.saturating_sub(1)));
        *qv
    }

    #[inline]
    fn side_magnitudes(returns: &[f64], positive: bool) -> Vec<f64> {
        if positive { returns.iter().filter(|&&x| x>0.0).map(|&x| x).collect() }
        else { returns.iter().filter(|&&x| x<0.0).map(|&x| -x).collect() }
    }

    #[inline]
    fn hill_tail_index(mut mags: Vec<f64>, k: usize) -> f64 {
        if mags.len() < k.saturating_add(1) { return 2.0; }
        for x in mags.iter_mut() { *x = x.max(1e-18); }
        let n = mags.len();
        let split = n.saturating_sub(k);
        mags.select_nth_unstable(split);
        let tail = &mut mags[split..];
        tail.sort_by(|a,b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        let xk = tail.last().copied().unwrap_or(1e-18);
        let mut s = 0.0; for &xi in tail.iter() { s += (xi / xk).ln(); }
        (1.0 / (s / (k as f64)).max(1e-6)).clamp(0.5, 4.0)
    }

    #[inline]
    fn momentum_sign(r: &[f64], lb: usize) -> f64 {
        let m: f64 = r.iter().rev().take(lb).sum();
        if m>0.0 {1.0} else if m<0.0 {-1.0} else {0.0}
    }

    #[inline]
    fn predict_tail_move_signed(
        price: f64,
        returns: &[f64],
        imbalance_hint: f64,
    ) -> (f64, f64, f64) {
        if returns.is_empty() || !price.is_finite() || price<=0.0 { return (0.0,0.0,0.0); }
        let dir_m = Self::momentum_sign(returns, 12);
        let dir_i = if imbalance_hint.is_sign_positive() { 1.0 } else if imbalance_hint.is_sign_negative() { -1.0 } else { 0.0 };
        let dir = (0.6*dir_m + 0.4*dir_i).clamp(-1.0,1.0).signum();
        let pos = dir >= 0.0;
        let mags = Self::side_magnitudes(returns, pos);
        if mags.len() < 8 { return (0.0,0.0,dir); }
        let q99  = { let mut v=mags.clone(); let n=v.len(); let idx=((n as f64-1.0)*0.990).round() as usize; let (_,q,_) = v.select_nth_unstable(idx.min(n-1)); *q }.max(1e-12);
        let q995 = { let mut v=mags.clone(); let n=v.len(); let idx=((n as f64-1.0)*0.995).round() as usize; let (_,q,_) = v.select_nth_unstable(idx.min(n-1)); *q }.min(0.25);
        let grow = (q995 / q99).clamp(0.0, 10.0);
        if grow > 3.2 { return (0.0,0.0,dir); }
        let stability_ok = ((3.2 - grow) / 3.2).clamp(0.0, 1.0);
        let k = ((mags.len() as f64)*0.05).round() as usize;
        let alpha = Self::hill_tail_index(mags, k.max(10).min(128)).max(0.6);
        let q_pred = q99 * ((1.0-0.990)/(1.0-0.995)).powf(1.0/alpha);
        let q_star = q_pred.clamp(q99, q995*1.2);
        let lr = if pos { q_star } else { -q_star };
        (price * (lr.exp() - 1.0), stability_ok, dir)
    }

    // === [IMPROVED: PIECE ZΩ3] Fee-aware sizing and net PnL ===
    #[inline]
    fn env_fee_frac_bps() -> f64 {
        std::env::var("DETECTOR_EXTRA_FEE_BPS")
            .ok().and_then(|s| s.parse::<f64>().ok())
            .map(|bps| (bps / 10_000.0).max(0.0))
            .unwrap_or(0.0)
    }

    #[inline]
    fn optimal_size_quadratic_fee_aware(
        expected_move_abs_frac: f64,
        half_spread_frac: f64,
        extra_linear_fee_frac: f64,
        impact_slope: f64,
        depth_qty: f64,
        max_position: u64,
    ) -> (u64, f64) {
        if expected_move_abs_frac <= 0.0 || depth_qty <= 0.0 { return (0,0.0); }
        let c1 = (half_spread_frac + extra_linear_fee_frac).min(0.05);
        let denom = (2.0 * impact_slope).max(1e-12);
        let x = ((expected_move_abs_frac - c1).max(0.0) / denom).min(0.15);
        let qty = (x * depth_qty).max(0.0).min(max_position as f64).floor() as u64;
        (qty, x)
    }

    #[inline]
    fn net_expected_pnl_price_units(
        mid: f64,
        expected_move_abs_frac: f64,
        half_spread_frac: f64,
        extra_linear_fee_frac: f64,
        impact_slope: f64,
        qty: u64,
        depth_qty: f64,
    ) -> f64 {
        if qty == 0 || depth_qty <= 0.0 || !mid.is_finite() || mid<=0.0 { return 0.0; }
        let q  = qty as f64;
        let x  = (q / depth_qty).min(1.0);
        let c1 = (half_spread_frac + extra_linear_fee_frac).min(0.05);
        mid * ( (expected_move_abs_frac - c1) * q - impact_slope * (q*q / depth_qty) )
    }

    // Lightweight jump probability surrogate on returns
    #[inline]
    fn jump_probability(r: &[f64]) -> f64 {
        if r.len() < 32 { return 0.0; }
        let mut v: Vec<f64> = r.iter().map(|x| x.abs()).collect();
        let n=v.len(); let idx=((n as f64-1.0)*0.99).round() as usize; let (_,q,_) = v.select_nth_unstable(idx.min(n-1));
        (q / (1e-6 + v.iter().sum::<f64>()/(n as f64))).clamp(0.0,1.0)
    }

    // === [IMPROVED: PIECE ZΩ4] Opportunity detection ===
    fn detect_levy_opportunity(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
        levy_params: &Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
        market_cache: &Arc<RwLock<HashMap<Pubkey, MarketState>>>,
        market_a: Pubkey,
        market_b: Pubkey,
    ) -> Option<FlashLoanOpportunity> {
        let history = price_history.read();
        let _params = levy_params.read();
        let _cache  = market_cache.read();
        let ha = history.get(&market_a)?; let hb = history.get(&market_b)?;
        if ha.len() < 120 || hb.len() < 120 { return None; }
        let (ra, ta, stale_ok_a, vr_ok_a) = Self::robust_log_returns(ha, 512);
        let (rb, tb, stale_ok_b, vr_ok_b) = Self::robust_log_returns(hb, 512);
        if ra.len() < 64 || rb.len() < 64 { return None; }
        let stale_ok = stale_ok_a && stale_ok_b; let vr_ok = vr_ok_a && vr_ok_b;
        let hy_corr = Self::hy_corr(&ra, &ta, &rb, &tb);
        let (k_best, hy_corr_best) = Self::best_lead_lag_k(&ra, &ta, &rb, &tb, 3);
        if hy_corr.abs() > 0.35 || k_best.abs() > 2 || hy_corr_best.abs() > 0.75 { return None; }
        if !stale_ok || !vr_ok { return None; }
        let a_last = ha.back()?; let b_last = hb.back()?;
        let price_a = a_last.last_trade.max(1e-9);
        let price_b = b_last.last_trade.max(1e-9);
        let spread_a = a_last.spread.max(1e-12);
        let spread_b = b_last.spread.max(1e-12);
        let depth_qty = a_last.bid_size.min(b_last.ask_size).max(1.0);
        let jump_prob_a = Self::jump_probability_calibrated(&ra, stale_ok_a, vr_ok_a);
        let jump_prob_b = Self::jump_probability_calibrated(&rb, stale_ok_b, vr_ok_b);
        let jump_prob = jump_prob_a.max(jump_prob_b);
        if jump_prob < 0.35 { return None; }
        let (pred_a, stab_a, dir_a) = Self::predict_tail_move_signed(price_a, &ra, a_last.imbalance);
        let (pred_b, stab_b, _dir_b) = Self::predict_tail_move_signed(price_b, &rb, b_last.imbalance);
        if pred_a == 0.0 && pred_b == 0.0 { return None; }
        let stability_ok = ((stab_a + stab_b) * 0.5).clamp(0.0, 1.0);
        let expected_abs_move = pred_a.abs() + pred_b.abs();
        if expected_abs_move <= 0.0 { return None; }
        let mid = (price_a + price_b) / 2.0;
        let expected_move_frac = (expected_abs_move / mid).clamp(0.0, 0.50);
        let half_spread_frac = ((spread_a + spread_b) / 2.0 / mid / 2.0).clamp(0.0, 0.03);
        let extra_fee_frac   = Self::env_fee_frac_bps().min(0.02);
        let impact_slope = ((spread_a + spread_b) / 2.0 / mid / depth_qty.max(1.0)).max(1e-9);
        let (size, depth_frac) = Self::optimal_size_quadratic_fee_aware(
            expected_move_frac, half_spread_frac, extra_fee_frac, impact_slope, depth_qty, MAX_POSITION_SIZE,
        );
        if size == 0 { return None; }
        let net_pnl_price = Self::net_expected_pnl_price_units(
            mid, expected_move_frac, half_spread_frac, extra_fee_frac, impact_slope, size, depth_qty
        );
        if net_pnl_price <= 0.0 { return None; }
        let slip_ok = {
            let mut sa = 0.0; let mut sb = 0.0; let mut ca = 0.0; let mut cb = 0.0; let alpha = 0.2;
            for x in ha.iter().rev().take(32) { sa = alpha * x.spread + (1.0 - alpha) * sa; ca += 1.0; }
            for x in hb.iter().rev().take(32) { sb = alpha * x.spread + (1.0 - alpha) * sb; cb += 1.0; }
            let baseline = (sa / ca.max(1.0) + sb / cb.max(1.0)) / 2.0;
            let ratio = ((spread_a + spread_b) / 2.0) / (baseline + 1e-12);
            (1.0 / ratio).clamp(0.0, 1.0)
        };
        let hy_decor = (1.0 - hy_corr.abs()).clamp(0.0, 1.0);
        let leadlag_ok = (1.0 - (k_best.abs() as f64 / 3.0)).clamp(0.0, 1.0);
        let sim_ok = 0.82;
        let confidence = Self::combine_confidence(
            jump_prob, hy_decor, leadlag_ok, slip_ok, sim_ok, stability_ok, true, true
        );
        let min_hurdle = (size as f64) * (MIN_PROFIT_BPS as f64 / 10_000.0) * mid;
        if net_pnl_price < min_hurdle { return None; }
        let exec_window = {
            let n = ta.len().min(tb.len());
            let gaps = if n >= 4 {
                let mut gs: Vec<u64> = ta.windows(2).map(|w| w[1].saturating_sub(w[0])).collect();
                let mut gt: Vec<u64> = tb.windows(2).map(|w| w[1].saturating_sub(w[0])).collect();
                if !gs.is_empty() && !gt.is_empty() {
                    let gm = gs.len()/2; let tm = gt.len()/2;
                    let (_, &g_med, _) = gs.select_nth_unstable(gm);
                    let (_, &t_med, _) = gt.select_nth_unstable(tm);
                    g_med.saturating_add(t_med).saturating_div(2)
                } else { 1 }
            } else { 1 };
            let base = 24u64.saturating_add((gaps / 2).min(64));
            let adj  = (base as f64 * (1.0 - (depth_frac*0.3))).round() as u64;
            adj.clamp(16, 128)
        };
        Some(FlashLoanOpportunity {
            market_a,
            market_b,
            token_mint: Pubkey::default(),
            entry_price: price_a,
            exit_price: price_a + pred_a * dir_a.signum(),
            size,
            expected_profit: net_pnl_price,
            confidence,
            predicted_jump: pred_a,
            execution_window: exec_window,
        })
    }

    // ΩJ1: robust jump probability via BNS + guards
    #[inline]
    fn calc_robust_returns(recent: &[MarketSnapshot]) -> (Vec<f64>, bool, bool) {
        if recent.len() < 3 { return (vec![], false, false); }
        let mut r: Vec<f64> = Vec::with_capacity(recent.len().saturating_sub(1));
        let mut gaps: Vec<u64> = Vec::with_capacity(recent.len().saturating_sub(1));
        let mut prev_t = recent[0].timestamp.max(1);
        for w in recent.windows(2) {
            if w[1].spread <= 0.0 { continue; }
            if (w[1].last_trade - w[0].last_trade).abs() < f64::EPSILON { continue; }
            let rr = (w[1].last_trade / w[0].last_trade).ln();
            if rr.is_finite() { r.push(rr); let te=w[1].timestamp.max(1); gaps.push(te.saturating_sub(prev_t)); prev_t=te; }
        }
        if r.is_empty() { return (r, false, false); }
        let mut tmp = r.clone();
        let mid = tmp.len()/2; let (_, &median, _) = tmp.select_nth_unstable(mid);
        let mut dev: Vec<f64> = r.iter().map(|x| (x - median).abs()).collect();
        let dmid = dev.len()/2; let (_, &mad_raw, _) = dev.select_nth_unstable(dmid);
        let scale = (1.4826 * mad_raw.max(1e-12)).max(1e-12);
        let clip  = 6.0 * scale; let hi = median + clip; let lo = median - clip;
        for x in r.iter_mut() { *x = if *x > hi { hi } else { if *x < lo { lo } else { *x } }; }
        let stale_ok = if !gaps.is_empty() {
            let mut g = gaps.clone(); let gm=g.len()/2; let (_, &g_med, _) = g.select_nth_unstable(gm);
            let last_gap = *gaps.last().unwrap_or(&g_med); last_gap <= g_med.saturating_mul(4).max(1)
        } else { false };
        let vr_ok = {
            let n = r.len(); if n < 20 { false } else {
                let m=4usize; let var1=r.iter().map(|x|x*x).sum::<f64>()/(n as f64);
                let mut acc=0.0; let mut c=0; for i in 0..(n.saturating_sub(m)) { let mut s=0.0; for k in 0..m { s+=r[i+k]; } acc+=s*s; c+=1; }
                let var_m = if c>0 { acc/(c as f64) } else { 0.0 }; let vr = if var1>1e-18 { var_m / ((m as f64)*var1) } else { 0.0 }; vr >= 0.60 }
        };
        (r, stale_ok, vr_ok)
    }

    #[inline]
    fn bipower_variation(r: &[f64]) -> f64 {
        const MU1: f64 = (2.0 / std::f64::consts::PI).sqrt();
        if r.len() < 2 { return 0.0; }
        let s: f64 = r.windows(2).map(|w| w[0].abs() * w[1].abs()).sum();
        (s / (r.len().saturating_sub(1) as f64)) / (MU1*MU1)
    }

    #[inline]
    fn calculate_jump_probability(params: &LevyParameters, history: &VecDeque<MarketSnapshot>) -> f64 {
        let n = history.len(); if n < 24 { return 0.0; }
        let take = n.min(64); let start = n - take; let recent = &history.make_contiguous()[start..];
        let (r, stale_ok, vr_ok) = Self::calc_robust_returns(recent);
        if r.len() < 16 || !stale_ok || !vr_ok { return 0.0; }
        let rv: f64 = r.iter().map(|x| x*x).sum::<f64>() / (r.len() as f64);
        let bv = Self::bipower_variation(&r).max(0.0);
        let j = (rv - bv).max(0.0);
        let bns = (j / (rv + 1e-12)).clamp(0.0, 0.999);
        let last = recent.last().unwrap(); let first = recent.first().unwrap();
        let spread_widening = (last.spread / first.spread.max(1e-12)).clamp(0.0, 5.0);
        let ofi = last.imbalance; let mom = (last.last_trade / recent[recent.len()/2].last_trade).ln();
        let base = params.jump_intensity.clamp(0.0, 0.99);
        let x = 2.2*bns + 0.6*(spread_widening - 1.0).max(0.0) + 0.15*ofi.tanh().abs() + 1.1*mom.abs().min(0.2);
        let prob = 1.0 / (1.0 + (-x).exp());
        (0.45*base + 0.55*prob).clamp(0.0, 0.95)
    }

    fn predict_levy_jump(params: &LevyParameters, current_price: f64) -> f64 {
        if !current_price.is_finite() || current_price <= 0.0 { return 0.0; }
        let alpha = params.alpha.clamp(1e-6, 2.0);
        let beta  = params.beta.clamp(-0.999, 0.999);
        let gamma = params.gamma.max(0.0);
        let delta = params.delta;

        let u = rng_f64().clamp(1e-12, 1.0 - 1e-12);
        let v = (rng_f64()*2.0 - 1.0) * (std::f64::consts::FRAC_PI_2 * 0.999999);
        let w = -u.ln();

        let x = if (alpha - 1.0).abs() < 1e-6 {
            let phi = v;
            let a = std::f64::consts::FRAC_PI_2 + beta * phi;
            let tan_phi = phi.tan();
            let cos_phi = phi.cos().abs().max(1e-12);
            let ln_term = ((std::f64::consts::FRAC_PI_2) * w * cos_phi / a.abs().max(1e-12)).ln();
            (2.0 / std::f64::consts::PI) * (a * tan_phi - beta * ln_term)
        } else {
            let phi = v;
            let zeta = -beta * (std::f64::consts::FRAC_PI_2 * alpha).tan();
            let xi = (1.0 + zeta*zeta).powf(1.0/(2.0*alpha));
            let cos_phi = phi.cos().signum() * phi.cos().abs().max(1e-12);
            let tan_phi = phi.tan();
            let num = (alpha * (phi + beta * tan_phi)).sin();
            let den = cos_phi.powf(1.0/alpha);
            let frac = (num / den) * xi;
            let expo = ((cos_phi - beta * (phi).sin()) / w).abs().max(1e-12)
                .powf((1.0 - alpha) / alpha);
            frac * expo
        };

        let jump = gamma * x + delta;
        let capped = jump.max(-0.5).min(0.5);
        current_price * capped
    }

    // ΩC1: HY-based cross-correlation (async-safe)
    #[inline]
    fn mk_returns_times(h: &VecDeque<MarketSnapshot>, max_len: usize) -> (Vec<f64>, Vec<u64>) {
        let mut r: Vec<f64> = Vec::with_capacity(h.len().saturating_sub(1));
        let mut t: Vec<u64> = Vec::with_capacity(h.len().saturating_sub(1));
        for w in h.windows(2) {
            if w[1].spread <= 0.0 { continue; }
            if (w[1].last_trade - w[0].last_trade).abs() < f64::EPSILON { continue; }
            let rr = (w[1].last_trade / w[0].last_trade).ln();
            if rr.is_finite() { r.push(rr); t.push(w[1].timestamp.max(1)); }
        }
        if r.len() > max_len {
            let s = r.len() - max_len;
            (r[s..].to_vec(), t[s..].to_vec())
        } else { (r, t) }
    }

    #[inline]
    fn hy_cov(ra: &[f64], ta: &[u64], rb: &[f64], tb: &[u64]) -> f64 {
        if ra.is_empty() || rb.is_empty() { return 0.0; }
        let (mut i, mut j) = (0usize, 0usize);
        let mut a_prev = ta[0].saturating_sub(1);
        let mut b_prev = tb[0].saturating_sub(1);
        let mut cov = 0.0f64; let mut c = 0.0f64;
        while i < ra.len() && j < rb.len() {
            let ae = ta[i]; let be = tb[j];
            if (ae > b_prev) && (be > a_prev) {
                let y = ra[i]*rb[j] - c; let t = cov + y; c = (t - cov) - y; cov = t;
                if ae <= be { a_prev = ae; i += 1; } else { b_prev = be; j += 1; }
            } else if ae <= b_prev { a_prev = ae; i += 1; } else { b_prev = be; j += 1; }
        }
        cov
    }

    #[inline]
    fn calculate_cross_correlation(
        history_a: &VecDeque<MarketSnapshot>,
        history_b: &VecDeque<MarketSnapshot>,
    ) -> f64 {
        let (ra, ta) = Self::mk_returns_times(history_a, 256);
        let (rb, tb) = Self::mk_returns_times(history_b, 256);
        if ra.len() < 16 || rb.len() < 16 { return 0.0; }
        let rv_a = ra.iter().map(|x| x*x).sum::<f64>().max(1e-18);
        let rv_b = rb.iter().map(|x| x*x).sum::<f64>().max(1e-18);
        let cov = Self::hy_cov(&ra, &ta, &rb, &tb);
        (cov / (rv_a.sqrt() * rv_b.sqrt())).clamp(-0.999, 0.999)
    }

    // ΩQ1: fee-aware Almgren–Chriss with quantization
    #[inline] fn env_qty_step() -> u64 {
        std::env::var("QUANTITY_STEP").ok().and_then(|s| s.parse::<u64>().ok()).filter(|&x| x>0).unwrap_or(1)
    }
    #[inline] fn env_min_qty() -> u64 {
        std::env::var("MIN_QUANTITY").ok().and_then(|s| s.parse::<u64>().ok()).filter(|&x| x>0).unwrap_or_else(Self::env_qty_step)
    }
    #[inline] fn quantize_qty(q: u64) -> u64 {
        let step = Self::env_qty_step(); let minq = Self::env_min_qty();
        let qq = (q / step).saturating_mul(step); if qq < minq { 0 } else { qq }
    }

    fn calculate_optimal_size(
        snapshot_a: &MarketSnapshot,
        snapshot_b: &MarketSnapshot,
        params_a: &LevyParameters,
        params_b: &LevyParameters,
    ) -> u64 {
        let depth_qty = snapshot_a.bid_size.min(snapshot_b.ask_size).max(1.0);
        let mid = ((snapshot_a.last_trade + snapshot_b.last_trade) / 2.0).max(1e-12);
        let half_spread_frac = (((snapshot_a.spread + snapshot_b.spread) / 2.0) / mid / 2.0).clamp(0.0, 0.03);
        let extra_fee = Self::env_fee_frac_bps().min(0.02);

        let sigma = ((params_a.volatility.powi(2) + params_b.volatility.powi(2)) / 2.0).sqrt();
        let expected_move_abs_frac = (3.0 * sigma).clamp(0.0, 0.50);

        let impact_slope = (((snapshot_a.spread + snapshot_b.spread) / 2.0) / mid / depth_qty).max(1e-9);

        let c1 = (half_spread_frac + extra_fee).min(0.05);
        if expected_move_abs_frac <= c1 { return 0; }
        let denom = (2.0 * impact_slope).max(1e-12);
        let x = ((expected_move_abs_frac - c1) / denom).clamp(0.0, 0.15);
        let raw_qty = (x * depth_qty).max(0.0).min(MAX_POSITION_SIZE as f64).floor() as u64;

        Self::quantize_qty(raw_qty)
    }

    pub fn update_levy_parameters(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
        levy_params: &Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
    ) {
        let history = price_history.read();
        let mut params = levy_params.write();

        for (market, snapshots) in history.iter() {
            if snapshots.len() < 96 { continue; }
            let newp = Self::estimate_levy_parameters(snapshots);

            // quick VR on last ~64
            let take = snapshots.len().min(64);
            let s = snapshots.len() - take;
            let recent = &snapshots.make_contiguous()[s..];
            let mut r: Vec<f64> = Vec::with_capacity(recent.len().saturating_sub(1));
            for w in recent.windows(2) {
                if w[1].spread <= 0.0 { continue; }
                if (w[1].last_trade - w[0].last_trade).abs() < f64::EPSILON { continue; }
                let rr = (w[1].last_trade / w[0].last_trade).ln();
                if rr.is_finite() { r.push(rr); }
            }
            let vr_ok = {
                let n = r.len();
                if n < 20 { false } else {
                    let m=4usize; let var1=r.iter().map(|x|x*x).sum::<f64>()/(n as f64);
                    let mut acc=0.0; let mut c=0;
                    for i in 0..(n.saturating_sub(m)) {
                        let mut s=0.0; for k in 0..m { s += r[i+k]; }
                        acc += s*s; c+=1;
                    }
                    let var_m = if c>0 { acc/(c as f64) } else { 0.0 };
                    var1>1e-18 && (var_m / ((m as f64)*var1) >= 0.60)
                }
            };

            let (a_fast, a_slow) = if vr_ok { (0.2, 0.1) } else { (0.1, 0.05) };
            let upd = |old: f64, new: f64, a: f64, lo: f64, hi: f64| -> f64 {
                if !new.is_finite() { return old; }
                ((1.0 - a) * old + a * new).clamp(lo, hi)
            };

            if let Some(ex) = params.get_mut(market) {
                ex.alpha = upd(ex.alpha, newp.alpha, a_slow, 1e-3, 2.0);
                ex.beta  = upd(ex.beta,  newp.beta,  a_slow, -0.999, 0.999);
                ex.gamma = upd(ex.gamma, newp.gamma, a_fast, 0.0, f64::INFINITY);
                ex.delta = upd(ex.delta, newp.delta, a_slow, -1.0, 1.0);
                ex.jump_intensity = upd(ex.jump_intensity, newp.jump_intensity, if vr_ok {0.25}else{0.15}, 0.0, 0.99);
                ex.drift = upd(ex.drift, newp.drift, a_slow, -1.0, 1.0);
                ex.volatility = upd(ex.volatility, newp.volatility, if vr_ok {0.25}else{0.15}, 0.0, f64::INFINITY);
            } else {
                params.insert(*market, LevyParameters {
                    alpha: newp.alpha.clamp(1e-3, 2.0),
                    beta:  newp.beta.clamp(-0.999, 0.999),
                    gamma: newp.gamma.max(0.0),
                    delta: newp.delta.clamp(-1.0, 1.0),
                    jump_intensity: newp.jump_intensity.clamp(0.0, 0.99),
                    drift: newp.drift.clamp(-1.0, 1.0),
                    volatility: newp.volatility.max(0.0),
                });
            }
        }
    }

    async fn execute_flashloan(
        rpc_client: &RpcClient,
        keypair: &Keypair,
        opportunity: &FlashLoanOpportunity,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if opportunity.token_mint == Pubkey::default() {
            return Err("missing token mint".into());
        }

        let blockhash = rpc_client.get_latest_blockhash()?;
        let current_slot = rpc_client.get_slot()?;

        let flash_loan_program = Pubkey::from_str("FLASHkGVm8yCvNxZrPGqXTcczn7zUfpnSzz7BwdeHvgp")?;
        let token_program = spl_token::id();

        let (authority_pda, _) = Pubkey::find_program_address(
            &[b"authority", keypair.pubkey().as_ref()],
            &flash_loan_program,
        );

        let user_token_account = spl_associated_token_account::get_associated_token_address(
            &keypair.pubkey(),
            &opportunity.token_mint,
        );

        if let Ok(acc) = rpc_client.get_account(&user_token_account) {
            if let Ok(tok) = TokenAccount::unpack(&acc.data) {
                if tok.mint != opportunity.token_mint || tok.owner != keypair.pubkey() {
                    return Err("token account mint/owner mismatch".into());
                }
            }
        }

        let (pool_account, _) = Pubkey::find_program_address(
            &[b"pool", opportunity.token_mint.as_ref()],
            &flash_loan_program,
        );

        let mut ixs: Vec<Instruction> = Vec::with_capacity(3);
        let borrow_data = BorrowInstruction { amount: opportunity.size, expected_fee: (opportunity.size * 3) / 1000 };
        ixs.push(Instruction {
            program_id: flash_loan_program,
            accounts: vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new(pool_account, false),
                AccountMeta::new(user_token_account, false),
                AccountMeta::new_readonly(authority_pda, false),
                AccountMeta::new_readonly(token_program, false),
                AccountMeta::new_readonly(system_program::id(), false),
            ],
            data: borrow_data.try_to_vec()?,
        });

        // slot-mixed nonce
        let nonce: u64 = (current_slot << 32) ^ rng_f64().to_bits();
        let arb_instruction = self.create_arbitrage_instruction(opportunity, &keypair.pubkey(), &user_token_account, nonce)?;
        ixs.push(arb_instruction);

        let repay_data = RepayInstruction { amount: opportunity.size, fee: (opportunity.size * 3) / 1000 };
        ixs.push(Instruction {
            program_id: flash_loan_program,
            accounts: vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new(pool_account, false),
                AccountMeta::new(user_token_account, false),
                AccountMeta::new_readonly(authority_pda, false),
                AccountMeta::new_readonly(token_program, false),
            ],
            data: repay_data.try_to_vec()?,
        });

        // env baseline CU / price / optional tip
        let mut cu_limit: u32 = std::env::var("CU_LIMIT").ok().and_then(|s| s.parse().ok()).unwrap_or(1_400_000);
        let mut cu_price: u64 = std::env::var("CU_PRICE_MICROLAMPORTS").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000);
        let jito_tip = match (std::env::var("JITO_TIP_ADDRESS").ok(), std::env::var("JITO_TIP_LAMPORTS").ok().and_then(|s| s.parse::<u64>().ok())) {
            (Some(addr), Some(lamports)) if lamports>0 => Some((Pubkey::from_str(&addr)?, lamports)),
            _ => None
        };

        // build & simulate
        let mut vtx = Self::build_v0_tx(keypair, ixs.clone(), blockhash, cu_limit, cu_price, jito_tip);
        let sim = rpc_client.simulate_transaction(&vtx)?;
        if let Some(err) = sim.value.err {
            let logs = sim.value.logs.unwrap_or_default().join("\n");
            return Err(format!("simulation failed: {err:?}\nlogs:\n{logs}").into());
        }
        if let Some(used) = sim.value.units_consumed {
            let target = ((used as f64) * 1.20).ceil() as u32;
            let max_cu: u32 = std::env::var("CU_MAX").ok().and_then(|s| s.parse().ok()).unwrap_or(1_800_000);
            let new_limit = target.min(max_cu);
            if new_limit > cu_limit {
                cu_limit = new_limit;
                vtx = Self::build_v0_tx(keypair, ixs.clone(), blockhash, cu_limit, cu_price, jito_tip);
                let sim2 = rpc_client.simulate_transaction(&vtx)?;
                if let Some(err) = sim2.value.err {
                    let logs = sim2.value.logs.unwrap_or_default().join("\n");
                    return Err(format!("simulation failed after CU adjust: {err:?}\nlogs:\n{logs}").into());
                }
            }
        }

        // robust send with context gate
        let min_context_slot = rpc_client.get_slot_with_commitment(CommitmentConfig::processed())?;
        let send_cfg = RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentConfig::processed()),
            max_retries: Some(5),
            min_context_slot: Some(min_context_slot),
            ..Default::default()
        };
        let sig = rpc_client.send_transaction_with_config(&vtx, send_cfg)?;
        rpc_client.confirm_transaction(&sig)?;
        Ok(())
    }

    fn build_tx(
        payer: &Keypair,
        mut ixs: Vec<Instruction>,
        blockhash: Hash,
        cu_limit: u32,
        cu_price: u64,
    ) -> Result<VersionedTransaction, Box<dyn std::error::Error>> {
        let mut with_cb = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        ];
        with_cb.append(&mut ixs);
        let msg = v0::Message::try_compile(
            &payer.pubkey(),
            &with_cb,
            &[],
            blockhash,
        )?;
        let vtx = VersionedTransaction::new(&VersionedMessage::V0(msg), &[payer]);
        Ok(vtx)
    }

    // ALT-aware builder (preferred)
    fn build_tx_with_alts(
        payer: &Keypair,
        mut ixs: Vec<Instruction>,
        blockhash: Hash,
        cu_limit: u32,
        cu_price: u64,
        alts: &[AddressLookupTableAccount],
        jito_tip: Option<(Pubkey, u64)>,
    ) -> VersionedTransaction {
        let mut with_cb = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        ];
        with_cb.append(&mut ixs);
        if let Some((to, lamports)) = jito_tip {
            with_cb.push(system_instruction::transfer(&payer.pubkey(), &to, lamports));
        }
        let msg_v0 = v0::Message::try_compile(
            &payer.pubkey(),
            &with_cb,
            alts,
            blockhash,
        ).expect("compile v0 alts");
        VersionedTransaction::new(VersionedMessage::V0(msg_v0), &[payer])
    }

    fn create_arbitrage_instruction(
        &self,
        opportunity: &FlashLoanOpportunity,
        payer: &Pubkey,
        token_account: &Pubkey,
        nonce: u64,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let prog_str = std::env::var("ARB_PROGRAM_ID").map_err(|_| "ARB_PROGRAM_ID not set")?;
        let arb_program = Pubkey::from_str(&prog_str)?;

        let min_profit_lamports = ((opportunity.expected_profit * 0.8) as u64).max(1);

        let data = ArbitrageData {
            market_a: opportunity.market_a,
            market_b: opportunity.market_b,
            amount: opportunity.size,
            min_profit: min_profit_lamports,
            max_slippage: MAX_SLIPPAGE_BPS,
            nonce,
        };

        Ok(Instruction {
            program_id: arb_program,
            accounts: vec![
                AccountMeta::new(*payer, true),
                AccountMeta::new(*token_account, false),
                AccountMeta::new(opportunity.market_a, false),
                AccountMeta::new(opportunity.market_b, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data: data.try_to_vec()?,
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct BorrowInstruction {
    amount: u64,
    expected_fee: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct RepayInstruction {
    amount: u64,
    fee: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ArbitrageData {
    market_a: Pubkey,
    market_b: Pubkey,
    amount: u64,
    min_profit: u64,
    max_slippage: u64,
    nonce: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levy_parameters() {
        let mut history = VecDeque::new();
        for i in 0..100 {
            history.push_back(MarketSnapshot {
                timestamp: i as u64,
                bid: 100.0 + (i as f64).sin(),
                ask: 100.1 + (i as f64).sin(),
                bid_size: 1000.0,
                ask_size: 1000.0,
                last_trade: 100.05 + (i as f64).sin(),
                volume: 10000.0,
                spread: 0.001,
                imbalance: 0.0,
            });
        }
        let params = LevyProcessPredictor::estimate_levy_parameters(&history);
        assert!(params.alpha > 0.0 && params.alpha <= 2.0);
    }
}

impl LevyProcessPredictor {
    pub async fn initialize_and_run(rpc_url: &str, keypair_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let keypair_bytes = std::fs::read(keypair_path)?;
        let keypair = Keypair::from_bytes(&keypair_bytes)?;
        
        let predictor = Arc::new(Self::new(rpc_url, keypair));
        
        // Pre-warm caches
        predictor.prewarm_caches().await?;
        
        // Start main execution loop
        predictor.run().await;
        
        Ok(())
    }

    async fn prewarm_caches(&self) -> Result<(), Box<dyn std::error::Error>> {
        let markets = vec![
            "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
            "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6",
            "DZjbn4XC8qoHKikZqzmhemykVzmossoayV9ffbsUqxVj",
            "HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1",
        ];

        for market_str in markets {
            if let Ok(market_pubkey) = market_str.parse::<Pubkey>() {
                if let Ok(account) = self.rpc_client.get_account(&market_pubkey) {
                    if let Some(market_state) = Self::parse_market_by_owner(&account.owner, &account.data) {
                        let mut cache = self.market_cache.write();
                        cache.insert(market_pubkey, market_state.inner);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn calculate_portfolio_risk(&self) -> f64 {
        let opportunities = self.pending_opportunities.lock().unwrap();
        if opportunities.is_empty() {
            return 0.0;
        }

        let total_exposure: f64 = opportunities.iter()
            .map(|o| o.size as f64 * o.entry_price)
            .sum();

        let risk_scores: Vec<f64> = opportunities.iter()
            .map(|o| {
                let position_risk = (o.size as f64) / MAX_POSITION_SIZE as f64;
                let confidence_risk = 1.0 - o.confidence;
                let jump_risk = o.predicted_jump.abs() / o.entry_price;
                (position_risk + confidence_risk + jump_risk) / 3.0
            })
            .collect();

        let avg_risk = risk_scores.iter().sum::<f64>() / risk_scores.len() as f64;
        let max_risk = risk_scores.iter().fold(0.0, |a, &b| a.max(b));

        (avg_risk * 0.7 + max_risk * 0.3).min(1.0)
    }

    fn validate_opportunity(&self, opportunity: &FlashLoanOpportunity) -> bool {
        // Check basic constraints
        if opportunity.size == 0 || opportunity.size > MAX_POSITION_SIZE {
            return false;
        }

        if opportunity.confidence < CONFIDENCE_THRESHOLD {
            return false;
        }

        if opportunity.expected_profit < (opportunity.size as f64 * MIN_PROFIT_BPS as f64 / 10000.0) {
            return false;
        }

        // Oracle freshness and price CI guard (placeholder)
        if !Self::oracle_freshness_ok(&opportunity.token_mint) { return false; }

        // Risk controls: per-mint and global drawdown; leader/mint cooldown
        let risk = self.risk_state.read();
        let per_mint_cap: u128 = 5_000_000_000; // customize
        let dd_cap: u128 = 10_000_000_000;      // customize
        if !allow_trade(&risk, &opportunity.token_mint, opportunity.size as u128, (per_mint_cap, dd_cap)) { return false; }
        // Cooldown keyed by (leader, mint) if present
        let leader = Pubkey::default(); // TODO: supply current slot leader
        if let Some(&until) = risk.cooldown_until.get(&(leader, opportunity.token_mint)) {
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
            if until > now { return false; }
        }

        // Check market conditions
        let markets = self.market_cache.read();
        if let (Some(market_a), Some(market_b)) = (markets.get(&opportunity.market_a), markets.get(&opportunity.market_b)) {
            let liquidity_a = market_a.native_coin_bid() + market_a.native_coin_ask();
            let liquidity_b = market_b.native_coin_bid() + market_b.native_coin_ask();
            
            if opportunity.size > liquidity_a.min(liquidity_b) / 10 {
                return false;
            }
        } else {
            return false;
        }

        true
    }

    #[inline]
    // Oracle freshness and CI guard
    #[derive(Clone, Debug, Default)]
    struct PriceFeed { publish_time: i64, price: f64, confidence_interval: f64 }
    #[inline]
    fn current_unix() -> i64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0) }
    #[inline]
    fn oracle_freshness_ok_with_price(_mint: &Pubkey, price: &PriceFeed) -> bool {
        let now = current_unix();
        (price.publish_time + 3 >= now) && ((price.confidence_interval / price.price).is_finite() && (price.confidence_interval / price.price) < 0.01)
    }
    #[inline]
    fn oracle_freshness_ok(_mint: &Pubkey) -> bool { true }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let executions = self.last_execution.read();
        let opportunities = self.pending_opportunities.lock();
        let history = self.price_history.read();

        let total_markets = history.len();
        let total_snapshots: usize = history.values().map(|v| v.len()).sum();
        let pending_opportunities = opportunities.len();
        let recent_executions = executions.values()
            .filter(|t| t.elapsed().as_secs() < 300)
            .count();

        PerformanceMetrics {
            total_markets,
            total_snapshots,
            pending_opportunities,
            recent_executions,
            portfolio_risk: self.calculate_portfolio_risk(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_markets: usize,
    pub total_snapshots: usize,
    pub pending_opportunities: usize,
    pub recent_executions: usize,
    pub portfolio_risk: f64,
}

// ΞRNG: entropy-safe RNG with periodic rekey per thread
fn rng_seed() -> u64 {
    static SEED: OnceCell<u64> = OnceCell::new();
    *SEED.get_or_init(|| {
        let mut b = [0u8; 8];
        OsRng.fill_bytes(&mut b);
        u64::from_le_bytes(b) ^ (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).ok()
            .map(|d| d.as_nanos() as u64).unwrap_or(0))
    })
}

thread_local! {
    static RNG_STATE: std::cell::RefCell<(Xoshiro256Plus, u32)> = std::cell::RefCell::new((
        Xoshiro256Plus::seed_from_u64(rng_seed()), 0
    ));
}
#[inline]
fn rng_f64() -> f64 {
    RNG_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        let (ref mut rng, ref mut ctr) = *st;
        *ctr += 1;
        if *ctr >= 1_000_000 {
            let mut b = [0u8; 8]; OsRng.fill_bytes(&mut b);
            let mix = u64::from_le_bytes(b) ^ rng_seed();
            *rng = Xoshiro256Plus::seed_from_u64(mix);
            *ctr = 0;
        }
        rng.gen::<f64>()
    })
}

// Tracing + metrics for execution
#[instrument(skip_all, fields(mint=%mint, leader=%leader))]
fn record_exec(success: bool, cu: u32, tip: u64, slip_bps: f64, mint: &Pubkey, leader: &Pubkey, slot_delta: u64, relay_id: u32) {
    let _ = (mint, leader); // captured by fields
    histogram!("exec.cu_used", cu as f64);
    histogram!("exec.slip_bps", slip_bps);
    histogram!("exec.tip_lamports", tip as f64);
    histogram!("exec.slot_delta", slot_delta as f64);
    counter!("exec.relay_id", relay_id as u64);
    if success { counter!("exec.success", 1); } else { counter!("exec.fail", 1); }
}

impl LevyProcessPredictor {
    pub async fn initialize_and_run(rpc_url: &str, keypair_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let keypair = solana_sdk::signature::read_keypair_file(keypair_path)
            .map_err(|e| format!("failed to read keypair {keypair_path}: {e}"))?;
        let predictor = Arc::new(Self::new(rpc_url, keypair));
        predictor.prewarm_caches().await?;
        let p = predictor.clone();
        let main_task = tokio::spawn(async move { p.run().await; });
        tokio::select! {
            _ = main_task => {},
            _ = tokio::signal::ctrl_c() => {
                tracing::warn!("ctrl-c received; shutting down cleanly");
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    let keypair_path = std::env::var("KEYPAIR_PATH")
        .unwrap_or_else(|_| "./keypair.json".to_string());
    tracing::info!(%rpc_url, "Starting Lévy Process Flash Loan Predictor");
    LevyProcessPredictor::initialize_and_run(&rpc_url, &keypair_path).await
}
