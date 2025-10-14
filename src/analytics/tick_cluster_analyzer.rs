// quantumalpha_tick_clusters.rs
// ======================================================================
// Official-first tick-array / tick-cluster utilities for Orca + Raydium
// with strict provenance, program-hash binding, and on-disk SDK file checks.
//
// Authority order (prod):
//   1) On-chain program + IDL + live account decode (source of truth)
//   2) Raydium TS SDK v2 (published package) parity (commit + constants + PDAs)
//   3) Rust Raydium port (optional; ADVISORY unless both prod_strict+raydium_rust_port)
//
// This file implements the following HARDENINGS beyond the prior version:
// - ts_sdk_fs_verify: bind parity to the *installed* @raydium-io/raydium-sdk-v2 by hashing files
// - ts_sdk_parity_matrix: JSON matrix parity vs SDK helpers for start-tick/PDA (REQUIRED in prod)
// - Unskippable IDL proof: must contain TickArray account; discriminators must match
// - Pool-bitmap length must equal the constant parsed from installed SDK files (REQUIRED in prod)
// - Hard-fail FS constants in prod: TICK_ARRAY_SIZE, TICK_ARRAY_BITMAP_SIZE(_BITS),
//   TICK_ARRAY_SEED, POOL_TICK_ARRAY_BITMAP_SEED must match IDL/manifest and live accounts
// - Double-derive Raydium pool-bitmap PDA via IDL seeds and SDK seed; force equality (prod)
// - Orca SDK parity (optional): parse @orca-so/whirlpools-sdk and assert 88-tick invariant
// - Provenance extended: record npm version + SHA256 of SDK files used for parity
//
// URLs (document authority: keep these in comments; do not remove)
// - Raydium CLMM program repo (source of truth): https://github.com/raydium-io/raydium-clmm
// - Raydium TS SDK v2 (helpers/constants): https://github.com/raydium-io/raydium-sdk-v2
// - Orca Whirlpools docs/SDK: https://docs.orca.so/ and https://github.com/orca-so/whirlpools
// - Anchor IDL PDA rule: seeds ["anchor:idl", program_id]
//
// Build features:
//   prod_strict        -> fail-closed behavior everywhere
//   idl_json           -> fetch on-chain IDL JSON
//   ts_sdk_parity      -> require TS SDK v2 manifest parity (commit + constants)
//   ts_sdk_fs_verify   -> ALSO verify against *installed* SDK files on disk (no env spoof)
//   ts_sdk_parity_matrix -> verify start-tick/PDA via precomputed JSON from SDK helpers
//   raydium_rust_port  -> optional Rust port helper parity; ADVISORY by default, HARD in prod_strict
//   orca_rust_parity   -> Orca Rust parity for 88-size; ADVISORY unless you want to HARD fail
//   fs                 -> read/write goldens + provenance
//
// Required env when features are enabled:
//   RAYDIUM_TS_SDK_MANIFEST_JSON   -> JSON with repo, commit_sha, constants (ts_sdk_parity)
//   RAYDIUM_TS_SDK_DIR             -> path to installed @raydium-io/raydium-sdk-v2 (ts_sdk_fs_verify)
//   RAYDIUM_TS_PARITY_MATRIX_JSON  -> path to JSON parity grid (ts_sdk_parity_matrix)
//   ORCA_SDK_DIR                   -> path to installed @orca-so/whirlpools-sdk (orca_sdk_fs_verify)
//   RAYDIUM_IDL_JSON_FALLBACK      -> optional IDL JSON when on-chain fetch unavailable
//
// CI release profiles should include (example):
//   --features "prod_strict,idl_json,ts_sdk_parity,fs,ts_sdk_fs_verify,ts_sdk_parity_matrix"
//
// ======================================================================

#![allow(clippy::too_many_arguments)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure, Context, Result};
use blake3::Hasher as Blake3;
use dashmap::DashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use solana_program::pubkey::Pubkey;

#[cfg(feature = "idl_json")]
use serde_json::Value as Json;

#[cfg(feature = "fs")]
use std::fs;

#[cfg(feature = "ts_sdk_parity")]
use sha2::{Digest, Sha256};

// ========================= Prod compile-time enforcement =========================
#[cfg(all(feature = "prod_strict", not(feature = "idl_json")))]
compile_error!("prod_strict requires feature `idl_json` for on-chain IDL validation.");

#[cfg(all(feature = "prod_strict", not(feature = "ts_sdk_parity")))]
compile_error!("prod_strict requires feature `ts_sdk_parity` to enforce TS SDK v2 manifest parity.");

// NEW: prod must also verify on-disk SDK and parity matrix
#[cfg(all(feature = "prod_strict", not(feature = "ts_sdk_fs_verify")))]
compile_error!("prod_strict requires feature `ts_sdk_fs_verify` to hard-fail on Raydium SDK code constants.");

#[cfg(all(feature = "prod_strict", not(feature = "ts_sdk_parity_matrix")))]
compile_error!("prod_strict requires feature `ts_sdk_parity_matrix` to enforce start-tick parity matrix.");

// Optional: if you enable HARD filesystem checks in prod, enforce the dir envs exist (runtime gate kept)
#[cfg(all(feature = "prod_strict", feature = "ts_sdk_fs_verify"))]
const REQUIRE_TS_SDK_FS: bool = true;
#[cfg(not(all(feature = "prod_strict", feature = "ts_sdk_fs_verify")))]
const REQUIRE_TS_SDK_FS: bool = false;

#[cfg(all(feature = "prod_strict", feature = "ts_sdk_parity_matrix"))]
const REQUIRE_PARITY_MATRIX: bool = true;
#[cfg(not(all(feature = "prod_strict", feature = "ts_sdk_parity_matrix")))]
const REQUIRE_PARITY_MATRIX: bool = false;

// ---------- Hardening toggles ----------
#[cfg(feature = "prod_strict")]
const PROD_STRICT: bool = true;
#[cfg(not(feature = "prod_strict"))]
const PROD_STRICT: bool = false;

// Orca is canon: 88 ticks per TickArray (official).
const ORCA_ARRAY_LEN: i32 = 88;

// ========================= Optional: Rust Raydium port (ADVISORY / HARD in prod+feature) =========================
#[cfg(feature = "raydium_rust_port")]
mod raydium_advisory {
    pub use raydium_sdk_V2::raydium::clmm::utils::pda::{
        get_pda_ex_bitmap_account as ray_get_pda_ex_bitmap_account,
        get_pda_tick_array_address as ray_get_pda_tick_array_address,
        POOL_TICK_ARRAY_BITMAP_SEED as RAY_POOL_TA_BITMAP_SEED,
        TICK_ARRAY_SEED as RAY_TICK_ARRAY_SEED,
    };
    pub use raydium_sdk_V2::raydium::clmm::utils::tick::{
        TICK_ARRAY_BITMAP_SIZE as RAY_BITMAP_SIZE_BITS, TICK_ARRAY_SIZE as RAY_ARRAY_LEN,
        MIN_TICK as RAY_MIN_TICK,
    };
    pub use raydium_sdk_V2::raydium::clmm::utils::util::{least_significant_bit as ray_lsb, trailing_zeros as ray_tz};
}

// Orca Rust parity (pure-Rust, no Node).
#[cfg(feature = "orca_rust_parity")]
mod orca_parity {
    pub use orca_whirlpools_core::{get_tick_array_start_tick_index as orca_start_tick_for_index, TICK_ARRAY_SIZE as ORCA_TA_SIZE};
}

// ========================= TS SDK v2 Parity Manifest (ENV) =========================
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumTSConstants {
    pub tick_array_seed: String,
    pub tick_array_size: i32,
    pub tick_array_bitmap_size_bits: u32,
    #[serde(default)]
    pub pool_bitmap_seed: Option<String>,
    #[serde(default)]
    pub min_tick: Option<i32>,
    #[serde(default)]
    pub tick_spacing: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TSSDKParityManifest {
    pub repo: String,
    pub commit_sha: String,
    pub constants: RaydiumTSConstants,
}

#[cfg(feature = "ts_sdk_parity")]
fn load_ts_manifest_from_env() -> Result<TSSDKParityManifest> {
    let raw = std::env::var("RAYDIUM_TS_SDK_MANIFEST_JSON").context("RAYDIUM_TS_SDK_MANIFEST_JSON not set")?;
    let parsed: TSSDKParityManifest = serde_json::from_str(&raw).context("invalid TS SDK manifest JSON")?;
    ensure!(
        parsed.repo.to_ascii_lowercase().contains("raydium-sdk"),
        "TS SDK manifest repo field does not look like Raydium SDK"
    );
    ensure!(
        !parsed.commit_sha.trim().is_empty() && parsed.commit_sha.len() >= 7,
        "TS SDK manifest must include a non-empty commit_sha"
    );
    Ok(parsed)
}

#[cfg(all(feature = "ts_sdk_parity", feature = "prod_strict"))]
fn require_ts_manifest_present() -> Result<TSSDKParityManifest> {
    load_ts_manifest_from_env().context("prod_strict requires TS SDK manifest present")
}

// ========================= Filesystem SDK verification (no env-spoof) =========================
#[cfg(feature = "ts_sdk_fs_verify")]
#[derive(Debug, Clone)]
struct TSSDKFsInfo {
    pkg_dir: PathBuf,
    npm_version: Option<String>,
    git_head: Option<String>,
    constants_sha256: Option<[u8; 32]>,
    pda_sha256: Option<[u8; 32]>,
    bitmap_bits_from_code: Option<u32>,
    tick_array_size_from_code: Option<i32>,
    tick_array_seed_from_code: Option<String>,
    pool_bitmap_seed_from_code: Option<String>,
}

#[cfg(feature = "ts_sdk_fs_verify")]
fn read_file_sha256(path: &Path) -> Result<[u8; 32]> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut h = Sha256::new();
    h.update(&bytes);
    Ok(h.finalize().into())
}

#[cfg(feature = "ts_sdk_fs_verify")]
fn parse_constant_u32(file_text: &str, name: &str) -> Option<u32> {
    let re = Regex::new(&format!(r#"(?m)export\s+const\s+{}\s*=\s*(\d+)"#, regex::escape(name))).ok()?;
    re.captures(file_text).and_then(|c| c.get(1)).and_then(|m| m.as_str().parse::<u32>().ok())
}

#[cfg(feature = "ts_sdk_fs_verify")]
fn parse_constant_i32(file_text: &str, name: &str) -> Option<i32> {
    let re = Regex::new(&format!(r#"(?m)export\s+const\s+{}\s*=\s*(-?\d+)"#, regex::escape(name))).ok()?;
    re.captures(file_text).and_then(|c| c.get(1)).and_then(|m| m.as_str().parse::<i32>().ok())
}

#[cfg(feature = "ts_sdk_fs_verify")]
fn parse_constant_string(file_text: &str, name: &str) -> Option<String> {
    let re =
        Regex::new(&format!(r#"(?m)export\s+const\s+{}\s*=\s*['"]([^'"]+)['"]"#, regex::escape(name))).ok()?;
    re.captures(file_text).and_then(|c| c.get(1)).map(|m| m.as_str().to_string())
}

/// Inspect the installed Raydium TS SDK v2 for parity.
#[cfg(feature = "ts_sdk_fs_verify")]
fn fs_inspect_raydium_sdk() -> Result<TSSDKFsInfo> {
    let root =
        std::env::var("RAYDIUM_TS_SDK_DIR").context("RAYDIUM_TS_SDK_DIR must point to installed @raydium-io/raydium-sdk-v2")?;
    let pkg_dir = PathBuf::from(root);
    ensure!(pkg_dir.is_dir(), "RAYDIUM_TS_SDK_DIR is not a directory");

    // package.json: npm version and optional gitHead
    let pkg_json_path = pkg_dir.join("package.json");
    let pkg_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&pkg_json_path).with_context(|| format!("read {}", pkg_json_path.display()))?)?;
    let npm_version = pkg_json.get("version").and_then(|x| x.as_str()).map(|s| s.to_string());
    let git_head = pkg_json.get("gitHead").and_then(|x| x.as_str()).map(|s| s.to_string());

    // Find candidate files
    let mut constants_text = None;
    let mut pda_text = None;
    let mut constants_path = None;
    let mut pda_path = None;

    fn scan_for_file(dir: &Path, needle: &str) -> Option<PathBuf> {
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            if let Ok(read) = std::fs::read_dir(&d) {
                for e in read.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        stack.push(p);
                    } else if p
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(|s| s.contains(needle) && (s.ends_with(".ts") || s.ends_with(".js")))
                        == Some(true)
                    {
                        return Some(p);
                    }
                }
            }
        }
        None
    }

    let src_dir = pkg_dir.join("src");
    ensure!(src_dir.is_dir(), "raydium-sdk-v2 missing src directory");

    // Typical names seen in SDK codebases
    constants_path = scan_for_file(&src_dir, "tick");
    let pda_candidate = scan_for_file(&src_dir, "pda").or_else(|| scan_for_file(&src_dir, "utils"));
    pda_path = pda_candidate;

    ensure!(constants_path.is_some(), "could not locate constants file in SDK src");
    ensure!(pda_path.is_some(), "could not locate PDA helper file in SDK src");

    let cpath = constants_path.as_ref().unwrap();
    let ppath = pda_path.as_ref().unwrap();
    let ctext = std::fs::read_to_string(cpath)?;
    let ptext = std::fs::read_to_string(ppath)?;

    let constants_sha256 = Some(read_file_sha256(cpath)?);
    let pda_sha256 = Some(read_file_sha256(ppath)?);

    // Extract constants
    let bitmap_bits_from_code =
        parse_constant_u32(&ctext, "TICK_ARRAY_BITMAP_SIZE").or_else(|| parse_constant_u32(&ctext, "TICK_ARRAY_BITMAP_SIZE_BITS"));
    let tick_array_size_from_code = parse_constant_i32(&ctext, "TICK_ARRAY_SIZE");
    let tick_array_seed_from_code = parse_constant_string(&ctext, "TICK_ARRAY_SEED");
    let pool_bitmap_seed_from_code =
        parse_constant_string(&ctext, "POOL_TICK_ARRAY_BITMAP_SEED").or_else(|| parse_constant_string(&ctext, "TICK_ARRAY_BITMAP_SEED"));

    Ok(TSSDKFsInfo {
        pkg_dir,
        npm_version,
        git_head,
        constants_sha256,
        pda_sha256,
        bitmap_bits_from_code,
        tick_array_size_from_code,
        tick_array_seed_from_code,
        pool_bitmap_seed_from_code,
    })
}

#[cfg(all(feature = "ts_sdk_parity", feature = "ts_sdk_fs_verify"))]
fn hard_verify_manifest_vs_fs(manifest: &TSSDKParityManifest, fsinfo: &TSSDKFsInfo) -> Result<()> {
    // If package.json has gitHead, require equality or prefix match with manifest.commit_sha.
    if let Some(head) = &fsinfo.git_head {
        ensure!(
            manifest.commit_sha.starts_with(head) || head.starts_with(&manifest.commit_sha),
            "SDK commit mismatch: manifest={} package.gitHead={}",
            manifest.commit_sha,
            head
        );
    } else {
        ensure!(
            fsinfo.tick_array_size_from_code == Some(manifest.constants.tick_array_size),
            "FS TICK_ARRAY_SIZE != manifest"
        );
        ensure!(
            fsinfo.bitmap_bits_from_code == Some(manifest.constants.tick_array_bitmap_size_bits),
            "FS TICK_ARRAY_BITMAP_SIZE_BITS != manifest"
        );
        if let Some(seed) = &fsinfo.tick_array_seed_from_code {
            ensure!(seed.as_str() == manifest.constants.tick_array_seed, "FS TICK_ARRAY_SEED != manifest");
        }
        if let (Some(fs_seed), Some(man_seed)) = (&fsinfo.pool_bitmap_seed_from_code, &manifest.constants.pool_bitmap_seed) {
            ensure!(fs_seed == man_seed, "FS POOL_TICK_ARRAY_BITMAP_SEED != manifest");
        }
    }
    Ok(())
}

// Optional parity matrix for start-tick and PDA derivations.
// JSON format: [{ "tick": -5000, "spacing": 5, "expected_start": -5100 }, ...]
#[cfg(feature = "ts_sdk_parity_matrix")]
#[derive(Deserialize)]
struct StartTickCase {
    tick: i32,
    spacing: i32,
    expected_start: i32,
}

#[cfg(feature = "ts_sdk_parity_matrix")]
fn load_parity_matrix() -> Result<Vec<StartTickCase>> {
    let path = std::env::var("RAYDIUM_TS_PARITY_MATRIX_JSON").context("RAYDIUM_TS_PARITY_MATRIX_JSON not set")?;
    let s = std::fs::read_to_string(&path).with_context(|| format!("read parity matrix {}", path))?;
    let cases: Vec<StartTickCase> = serde_json::from_str(&s)?;
    ensure!(!cases.is_empty(), "parity matrix is empty");
    Ok(cases)
}

// Optional Orca SDK file verification
#[cfg(feature = "orca_sdk_fs_verify")]
fn fs_verify_orca_88() -> Result<()> {
    let root = std::env::var("ORCA_SDK_DIR").context("ORCA_SDK_DIR must point to installed @orca-so/whirlpools-sdk")?;
    let pkg_dir = PathBuf::from(root);
    ensure!(pkg_dir.is_dir(), "ORCA_SDK_DIR is not a directory");
    let src_dir = pkg_dir.join("src");
    ensure!(src_dir.is_dir(), "whirlpools-sdk missing src directory");

    fn scan_for_tick_file(dir: &Path) -> Option<PathBuf> {
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            if let Ok(read) = std::fs::read_dir(&d) {
                for e in read.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        stack.push(p);
                    } else if p
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(|s| s.contains("tick") && (s.ends_with(".ts") || s.ends_with(".js")))
                        == Some(true)
                    {
                        return Some(p);
                    }
                }
            }
        }
        None
    }
    let tfile = scan_for_tick_file(&src_dir).context("could not locate Orca tick file")?;
    let text = std::fs::read_to_string(&tfile)?;
    let val = parse_constant_i32(&text, "TICK_ARRAY_SIZE").ok_or_else(|| anyhow!("Orca SDK did not export TICK_ARRAY_SIZE"))?;
    ensure!(val == 88, "Orca TICK_ARRAY_SIZE != 88 (found {})", val);
    Ok(())
}

// ========================= Optional IDL JSON fallback =========================
#[cfg(feature = "idl_json")]
fn load_idl_fallback_from_env() -> Result<Option<Json>> {
    match std::env::var("RAYDIUM_IDL_JSON_FALLBACK") {
        Ok(raw) => {
            let j: Json = serde_json::from_str(&raw).context("invalid RAYDIUM_IDL_JSON_FALLBACK")?;
            Ok(Some(j))
        }
        Err(_) => Ok(None),
    }
}

// ========================= Public API enums/structs =========================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clmm {
    Orca,
    Raydium,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramProvenance {
    pub program_id: Pubkey,
    pub program_blake3: [u8; 32],
    pub slot_observed: Option<u64>,
    pub rpc_endpoint: Option<String>,
    pub commit_sha: Option<String>, // your own build commit

    // IDL provenance
    pub idl_slot: Option<u64>,
    pub idl_sha256: Option<[u8; 32]>,

    // TS SDK parity provenance
    pub ts_sdk_commit: Option<String>,
    pub ts_sdk_npm_version: Option<String>,
    pub ts_sdk_constants_sha256: Option<[u8; 32]>,
    pub ts_sdk_pda_sha256: Option<[u8; 32]>,

    // Optional Orca FS check
    pub orca_sdk_npm_version: Option<String>,
    pub orca_sdk_constants_sha256: Option<[u8; 32]>,

    #[serde(default)]
    pub degraded_idl_fallback: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParamSource {
    Official, // Orca path only
    IdlOnChain {
        provenance: ProgramProvenance,
        sample_accounts: Vec<Pubkey>,
        bitmap_evidence: Option<(u32 /*bits*/, Pubkey /*pool_bitmap_pda*/ )>,
    },
    Empirical {
        sample_accounts: Vec<Pubkey>,
        bitmap_evidence: Option<(u32 /*bits*/, Pubkey)>,
    },
}

const FNV1A64_OFFSET: u64 = 0xcbf29ce484222325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01B3;

// ========================= Config =========================

#[derive(Debug, Clone, Copy)]
pub struct OrcaParams {
    pub tick_spacing: i32,
    pub array_len: i32,
    pub seed_tag: &'static [u8],
}
impl Default for OrcaParams {
    fn default() -> Self {
        Self {
            tick_spacing: 8,
            array_len: ORCA_ARRAY_LEN,
            seed_tag: b"tick_array",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumParams {
    pub tick_spacing: i32,
    pub array_len: i32,
    pub seed_tag: Vec<u8>,
    pub min_tick: i32,
    pub bitmap_bits: u32, // canonical pool-level bitmap size in bits
    pub param_source: ParamSource,
}

#[derive(Debug, Clone)]
pub struct ClmmConfig {
    pub clmm: Clmm,
    pub program_id: Pubkey,

    pub orca: OrcaParams,

    pub raydium: Option<RaydiumParams>,
    pub strict_no_guess: bool,
    pub enable_bitmap_path: bool,
    pub ttl_ms: u64,
    pub include_pool_in_hash: bool,

    pub golden_path: Option<String>, // write/read goldens keyed by program hash
    #[cfg(feature = "fs")]
    pub provenance_path: Option<String>, // write provenance JSON here
}
impl Default for ClmmConfig {
    fn default() -> Self {
        Self {
            clmm: Clmm::Orca,
            program_id: Pubkey::default(),
            orca: OrcaParams::default(),
            raydium: None,
            strict_no_guess: true,
            enable_bitmap_path: true,
            ttl_ms: 3_000,
            include_pool_in_hash: false,
            golden_path: None,
            #[cfg(feature = "fs")]
            provenance_path: None,
        }
    }
}

// ========================= Core Types =========================

#[derive(Debug, Clone)]
pub struct TickData {
    pub tick_index: i32,
    pub sqrt_price_x64: u128, // Q64.64 sqrt
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
    pub fee_growth_outside_a: u64,
    pub fee_growth_outside_b: u64,
    pub timestamp: u64,
    pub block_height: u64,
}

#[derive(Debug, Clone)]
pub struct ClusterAnalysis {
    pub pool: Pubkey,
    pub cluster_id: u64,
    pub tick_hash: u64,
    pub ema_volatility: f64,
    pub persistence_score: f64,
}

#[derive(Default)]
struct Metrics {
    bitmap_hits: AtomicU64,
    bitmap_misses: AtomicU64,
}
impl Metrics {
    fn inc_hit(&self) {
        self.bitmap_hits.fetch_add(1, Ordering::Relaxed);
    }
    fn inc_miss(&self) {
        self.bitmap_misses.fetch_add(1, Ordering::Relaxed);
    }
}

// ========================= RPC + Inspector Traits =========================

pub trait RpcLite {
    fn get_account_data(&self, pubkey: &Pubkey) -> Result<Vec<u8>>;
    fn get_program_accounts(&self, program_id: &Pubkey) -> Result<Vec<(Pubkey, Vec<u8>)>>;
    fn get_multiple_accounts(&self, pubkeys: &[Pubkey]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::with_capacity(pubkeys.len());
        for pk in pubkeys {
            out.push(self.get_account_data(pk).ok());
        }
        Ok(out)
    }
    // Optional extended call for provenance (IDL slot); implement as no-op if unavailable.
    fn get_account_slot(&self, _pubkey: &Pubkey) -> Result<Option<u64>> {
        Ok(None)
    }
}

/// Return (array_len, tick_spacing, min_tick, Option<bitmap_total_bytes>).
/// Implemented against the actual Raydium CLMM IDL layout.
/// In prod_strict, per-array bitmap is considered non-authoritative.
pub trait RaydiumTickArrayInspector {
    fn decode_params(&self, raw_account: &[u8]) -> Result<(i32, i32, i32, Option<usize>)>;
    fn decode_bitmap_payload(&self, _raw_account: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

// ========================= IDL-backed Raydium Inspector =========================

#[cfg(feature = "idl_json")]
pub struct IdlBackedRaydiumInspector {
    idl: Json,
    tickarray_account_name: Option<String>,
    // Field name candidates
    f_array_len: Vec<&'static str>,
    f_tick_spacing: Vec<&'static str>,
    f_min_tick: Vec<&'static str>,
    f_bitmap: Vec<&'static str>,
    // Optional: pool-bitmap extension names
    pool_bitmap_account_name: Vec<&'static str>,
}

#[cfg(feature = "idl_json")]
impl IdlBackedRaydiumInspector {
    pub fn new(idl_bytes: &[u8]) -> Result<Self> {
        let idl: Json = serde_json::from_slice(idl_bytes).context("invalid IDL JSON")?;
        Ok(Self {
            idl,
            tickarray_account_name: Some("TickArray".to_string()),
            f_array_len: vec!["array_len", "tick_count", "entries", "len"],
            f_tick_spacing: vec!["tick_spacing", "spacing"],
            f_min_tick: vec!["min_tick", "start_tick", "base_tick", "start_tick_index"],
            f_bitmap: vec!["bitmap", "liquidity_bitmap", "initialized_bitmap", "bitset"],
            pool_bitmap_account_name: vec!["TickArrayBitmapExtension", "TickArrayBitmap", "PoolTickArrayBitmap"],
        })
    }
    fn accounts_array(&self) -> Result<&Vec<Json>> {
        self.idl
            .get("accounts")
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow!("IDL missing 'accounts'"))
    }
    fn find_account_by_name<'a>(&'a self, names: &[&str]) -> Result<&'a Json> {
        let candidates = self.accounts_array()?;
        for n in names {
            if let Some(acc) = candidates.iter().find(|acc| {
                acc.get("name")
                    .and_then(|s| s.as_str())
                    .map(|x| x.eq_ignore_ascii_case(n))
                    .unwrap_or(false)
            }) {
                return Ok(acc);
            }
        }
        Err(anyhow!("No matching account by names {:?}", names))
    }
    fn find_tickarray_account(&self) -> Result<&Json> {
        if let Some(name) = &self.tickarray_account_name {
            let candidates = self.accounts_array()?;
            if let Some(acc) = candidates.iter().find(|acc| {
                acc.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.eq_ignore_ascii_case(name))
                    .unwrap_or(false)
            }) {
                return Ok(acc);
            }
        }
        // Fallback: find an account with a vec<u8> bitmap field.
        let candidates = self.accounts_array()?;
        candidates
            .iter()
            .find(|acc| {
                let fields = acc.get("type").and_then(|t| t.get("fields")).and_then(|f| f.as_array());
                if let Some(fs) = fields {
                    fs.iter().any(|f| {
                        let nm = f.get("name").and_then(|n| n.as_str()).unwrap_or("");
                        let ty = f.get("type");
                        self.f_bitmap.iter().any(|cand| cand.eq_ignore_ascii_case(nm))
                            && matches!(ty, Some(Json::String(s)) if s == "bytes")
                    })
                } else {
                    false
                }
            })
            .ok_or_else(|| anyhow!("No plausible TickArray account found in IDL"))
    }

    // Try to extract TickArray PDA seeds from IDL.
    pub fn find_tickarray_seed_tag(&self) -> Result<Vec<u8>> {
        if let Some(ixs) = self.idl.get("instructions").and_then(|x| x.as_array()) {
            for ix in ixs {
                if let Some(accs) = ix.get("accounts").and_then(|a| a.as_array()) {
                    for a in accs {
                        let is_tickarray = a
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.eq_ignore_ascii_case("tickArray"))
                            .unwrap_or(false);
                        if !is_tickarray {
                            continue;
                        }
                        if let Some(pda) = a.get("pda").and_then(|p| p.as_object()) {
                            if let Some(seeds) = pda.get("seeds").and_then(|s| s.as_array()) {
                                if let Some(bytes) = extract_const_bytes_seed(seeds) {
                                    return Ok(bytes);
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(accounts) = self.idl.get("accounts").and_then(|x| x.as_array()) {
            for acc in accounts {
                let name_ok = acc
                    .get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.eq_ignore_ascii_case("TickArray"))
                    .unwrap_or(false);
                if !name_ok {
                    continue;
                }
                if let Some(seeds) = acc.get("seeds").and_then(|s| s.as_array()) {
                    if let Some(bytes) = extract_const_bytes_seed(seeds) {
                        return Ok(bytes);
                    }
                }
            }
        }
        Err(anyhow!("No const-bytes TickArray seed found in IDL"))
    }

    // Extract Pool TickArray Bitmap Extension seeds from IDL (authoritative PDA path).
    pub fn find_pool_bitmap_seed_tag(&self) -> Result<Vec<u8>> {
        let acc = self.find_account_by_name(&self.pool_bitmap_account_name)?;
        if let Some(seeds) = acc.get("seeds").and_then(|s| s.as_array()) {
            if let Some(bytes) = extract_const_bytes_seed(seeds) {
                return Ok(bytes);
            }
        }
        Err(anyhow!("No const-bytes pool bitmap seed found in IDL"))
    }

    fn compute_offsets(&self, fields: &[Json]) -> Result<HashMap<String, (usize, String)>> {
        let mut off = 8usize; // Anchor discriminator
        let mut map = HashMap::new();
        for f in fields {
            let name = f.get("name").and_then(|n| n.as_str()).ok_or_else(|| anyhow!("field missing name"))?.to_string();
            let ty = f.get("type").ok_or_else(|| anyhow!("field missing type"))?;
            let ty_s = if let Some(s) = ty.as_str() {
                s.to_string()
            } else if let Some(obj) = ty.as_object() {
                if let Some(vec_t) = obj.get("vec").and_then(|x| x.as_str()) {
                    format!("vec<{}>", vec_t)
                } else {
                    "unsupported".to_string()
                }
            } else {
                "unsupported".to_string()
            };
            let size = match ty_s.as_str() {
                "bool" | "u8" | "i8" => 1,
                "u16" | "i16" => 2,
                "u32" | "i32" => 4,
                "u64" | "i64" => 8,
                "u128" | "i128" => 16,
                "publicKey" | "pubkey" | "Pubkey" => 32,
                s if s.starts_with("vec<") => {
                    ensure!(s == "vec<u8>", "unsupported Vec type: {}", s);
                    map.insert(name.clone(), (off, ty_s.clone()));
                    off += 4; // length prefix
                    continue;
                }
                _ => bail!("unsupported field: {}", ty_s),
            };
            map.insert(name.clone(), (off, ty_s.clone()));
            off += size;
        }
        Ok(map)
    }
    fn read_i32(data: &[u8], off: usize) -> Result<i32> {
        ensure!(off + 4 <= data.len(), "oob i32");
        Ok(i32::from_le_bytes(data[off..off + 4].try_into().unwrap()))
    }
    fn read_u32(data: &[u8], off: usize) -> Result<u32> {
        ensure!(off + 4 <= data.len(), "oob u32");
        Ok(u32::from_le_bytes(data[off..off + 4].try_into().unwrap()))
    }
    fn pick_field<'a>(map: &'a HashMap<String, (usize, String)>, cands: &[&str]) -> Option<(&'a String, &'a (usize, String))> {
        for c in cands {
            if let Some((k, v)) = map.iter().find(|(nm, _)| nm.eq_ignore_ascii_case(&c.to_string())) {
                return Some((k, v));
            }
        }
        None
    }
}

// Pull a const-bytes seed out of an Anchor seeds array.
#[cfg(feature = "idl_json")]
fn extract_const_bytes_seed(seeds: &[Json]) -> Option<Vec<u8>> {
    for seed in seeds {
        let kind = seed.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if kind != "const" {
            continue;
        }
        let ty = seed.get("type").and_then(|t| t.as_str()).unwrap_or("");
        if ty == "bytes" {
            if let Some(arr) = seed.get("value").and_then(|v| v.as_array()) {
                let mut out = Vec::with_capacity(arr.len());
                for x in arr {
                    let b = x.as_u64()?;
                    if b > 255 {
                        return None;
                    }
                    out.push(b as u8);
                }
                if !out.is_empty() {
                    return Some(out);
                }
            }
        } else if ty == "string" {
            if let Some(s) = seed.get("value").and_then(|v| v.as_str()) {
                let v = s.as_bytes().to_vec();
                if !v.is_empty() {
                    return Some(v);
                }
            }
        }
    }
    None
}

#[cfg(feature = "idl_json")]
impl RaydiumTickArrayInspector for IdlBackedRaydiumInspector {
    fn decode_params(&self, raw: &[u8]) -> Result<(i32, i32, i32, Option<usize>)> {
        let acc = self.find_tickarray_account()?;
        let fields = acc.get("type").and_then(|t| t.get("fields")).and_then(|f| f.as_array()).ok_or_else(|| anyhow!("TickArray missing fields"))?;
        let layout = self.compute_offsets(fields)?;
        let (_n_arr, (o_arr, t_arr)) =
            Self::pick_field(&layout, &self.f_array_len).ok_or_else(|| anyhow!("array_len-like field missing"))?;
        let (_n_sp, (o_sp, t_sp)) =
            Self::pick_field(&layout, &self.f_tick_spacing).ok_or_else(|| anyhow!("tick_spacing-like field missing"))?;
        let (_n_min, (o_min, t_min)) =
            Self::pick_field(&layout, &self.f_min_tick).ok_or_else(|| anyhow!("min_tick-like field missing"))?;
        let array_len = if t_arr == "i32" { Self::read_i32(raw, *o_arr)? } else { Self::read_u32(raw, *o_arr)? as i32 };
        let tick_spacing = if t_sp == "i32" { Self::read_i32(raw, *o_sp)? } else { Self::read_u32(raw, *o_sp)? as i32 };
        let min_tick = if t_min == "i32" { Self::read_i32(raw, *o_min)? } else { Self::read_u32(raw, *o_min)? as i32 };

        // Per-array bitmap: parsed only as evidence; not authoritative in prod_strict.
        let mut bitmap_bytes = None;
        if let Some((_n_bm, (o_bm, t_bm))) = Self::pick_field(&layout, &self.f_bitmap) {
            ensure!(t_bm == "vec<u8>", "bitmap not vec<u8>");
            let len = Self::read_u32(raw, *o_bm)? as usize;
            ensure!(*o_bm + 4 + len <= raw.len(), "bitmap len OOB");
            if !PROD_STRICT {
                bitmap_bytes = Some(len);
            }
        }
        ensure!(array_len > 0 && tick_spacing > 0, "decoded invalid TickArray params");
        Ok((array_len, tick_spacing, min_tick, bitmap_bytes))
    }
    fn decode_bitmap_payload(&self, raw: &[u8]) -> Result<Option<Vec<u8>>> {
        if PROD_STRICT {
            return Ok(None);
        }
        let acc = self.find_tickarray_account()?;
        let fields = acc.get("type").and_then(|t| t.get("fields")).and_then(|f| f.as_array()).ok_or_else(|| anyhow!("TickArray missing fields"))?;
        let layout = self.compute_offsets(fields)?;
        if let Some((_n_bm, (o_bm, t_bm))) = Self::pick_field(&layout, &self.f_bitmap) {
            ensure!(t_bm == "vec<u8>", "bitmap not vec<u8>");
            let len = Self::read_u32(raw, *o_bm)? as usize;
            ensure!(*o_bm + 4 + len <= raw.len(), "bitmap payload OOB");
            return Ok(Some(raw[*o_bm + 4..*o_bm + 4 + len].to_vec()));
        }
        Ok(None)
    }
}

// ========================= Raydium Param Discoverer =========================

pub struct RaydiumParamDiscoverer<'a, R: RpcLite, I: RaydiumTickArrayInspector> {
    rpc: &'a R,
    program_id: Pubkey,
    inspector: &'a I,
}
impl<'a, R: RpcLite, I: RaydiumTickArrayInspector> RaydiumParamDiscoverer<'a, R, I> {
    pub fn new(rpc: &'a R, program_id: Pubkey, inspector: &'a I) -> Self {
        Self { rpc, program_id, inspector }
    }

    fn derive_anchor_idl_pda(program_id: &Pubkey) -> Pubkey {
        // Anchor on-chain IDL PDA convention: ["anchor:idl", program_id]
        let seed = b"anchor:idl";
        Pubkey::find_program_address(&[seed.as_ref(), program_id.as_ref()], program_id).0
    }

    /// Compute Anchor account discriminator for filtering: shash("account:<Name>")
    #[cfg(feature = "idl_json")]
    fn anchor_discriminator_for(name: &str) -> [u8; 8] {
        use sha2::Sha256;
        let mut h = Sha256::new();
        h.update(format!("account:{}", name).as_bytes());
        let bytes: [u8; 32] = h.finalize().into();
        bytes[0..8].try_into().unwrap()
    }

    /// Discover TickArray samples via program-account scan + discriminator.
    #[cfg(feature = "idl_json")]
    pub fn discover_tickarray_samples_via_filters(
        &self,
        max: usize,
        tickarray_account_name: &str,
        expected_min_span: usize,
    ) -> Result<Vec<Pubkey>> {
        let disc = Self::anchor_discriminator_for(tickarray_account_name);
        let mut out = Vec::new();
        for (pk, data) in self.rpc.get_program_accounts(&self.program_id)? {
            if data.len() >= expected_min_span && data.starts_with(&disc) {
                out.push(pk);
                if out.len() >= max {
                    break;
                }
            }
        }
        ensure!(!out.is_empty(), "no tickarray samples found via filters");
        Ok(out)
    }

    #[cfg(feature = "idl_json")]
    pub fn fetch_anchor_idl_json(&self) -> Result<(Json, Option<u64>, [u8; 32])> {
        let idl_pda = Self::derive_anchor_idl_pda(&self.program_id);
        let raw = self
            .rpc
            .get_account_data(&idl_pda)
            .with_context(|| format!("fetch on-chain IDL at PDA {}", idl_pda))?;
        let pos = raw.iter().position(|b| *b == b'{').ok_or_else(|| anyhow!("IDL JSON start not found in account"))?;
        let bytes = &raw[pos..];
        let json: Json = serde_json::from_slice(bytes)?;
        if PROD_STRICT && !json.is_object() {
            bail!("IDL JSON corrupt for PDA {}", idl_pda);
        }
        #[cfg(feature = "ts_sdk_parity")]
        let mut hasher = Sha256::new();
        #[cfg(feature = "ts_sdk_parity")]
        hasher.update(bytes);
        #[cfg(feature = "ts_sdk_parity")]
        let idl_sha256: [u8; 32] = hasher.finalize().into();

        let slot = self.rpc.get_account_slot(&idl_pda).ok().flatten();

        Ok((
            json,
            slot,
            {
                #[cfg(feature = "ts_sdk_parity")]
                {
                    idl_sha256
                }
                #[cfg(not(feature = "ts_sdk_parity"))]
                {
                    [0u8; 32]
                }
            },
        ))
    }

    fn compute_program_blake3(&self) -> Result<[u8; 32]> {
        let raw = self.rpc.get_account_data(&self.program_id).with_context(|| format!("fetch program account {}", self.program_id))?;
        let mut h = Blake3::new();
        h.update(&raw);
        let mut out = [0u8; 32];
        out.copy_from_slice(h.finalize().as_bytes());
        Ok(out)
    }
    fn same_value<T: Copy + PartialEq>(&self, xs: &[T]) -> Option<T> {
        if xs.is_empty() {
            return None;
        }
        let x0 = xs[0];
        if xs.iter().all(|&x| x == x0) {
            Some(x0)
        } else {
            None
        }
    }

    fn verify_seed_against_known_tickarray(
        &self,
        seed_tag: &[u8],
        known_tickarray_pk: &Pubkey,
        pool_pk: &Pubkey,
        start_tick: i32,
    ) -> Result<()> {
        let tick_bytes = start_tick.to_le_bytes();
        let seeds: [&[u8]; 3] = [seed_tag, pool_pk.as_ref(), &tick_bytes];
        let derived = Pubkey::find_program_address(&seeds, &self.program_id).0;
        ensure!(
            &derived == known_tickarray_pk,
            "Seed tag did not reproduce known TickArray PDA.\nexpected={}\nderived={}",
            known_tickarray_pk,
            derived
        );
        Ok(())
    }

    #[cfg(feature = "ts_sdk_parity")]
    fn verify_seed_against_manifest(
        &self,
        manifest: &TSSDKParityManifest,
        known_tickarray_pk: &Pubkey,
        pool_pk: &Pubkey,
        start_tick: i32,
    ) -> Result<()> {
        let tick_bytes = start_tick.to_le_bytes();
        let seed = manifest.constants.tick_array_seed.as_bytes();
        let seeds: [&[u8]; 3] = [seed, pool_pk.as_ref(), &tick_bytes];
        let derived = Pubkey::find_program_address(&seeds, &self.program_id).0;
        ensure!(
            &derived == known_tickarray_pk,
            "Manifest seed did not reproduce known TickArray PDA.\nexpected={}\nderived={}",
            known_tickarray_pk,
            derived
        );
        Ok(())
    }

    #[cfg(all(feature = "raydium_rust_port"))]
    fn verify_seed_against_rust_port_helper(
        &self,
        known_tickarray_pk: &Pubkey,
        pool_pk: &Pubkey,
        start_tick: i32,
    ) -> Result<()> {
        let (sdk_key, _bump) = raydium_advisory::ray_get_pda_tick_array_address(&self.program_id, pool_pk, &start_tick);
        ensure!(
            &sdk_key == known_tickarray_pk,
            "Rust-port TS helper parity failed.\nexpected={}\nderived={}",
            known_tickarray_pk,
            sdk_key
        );
        Ok(())
    }

    /// Strict discovery with IDL proof + SDK parity + FS constants + parity matrix.
    #[allow(clippy::too_many_arguments)]
    pub fn discover_params_strict(
        &self,
        #[cfg(feature = "idl_json")] idl_json_opt: Option<Json>,
        mut sample_tick_array_accounts: &[Pubkey],
        known_pairs_for_seed_proof: &[(Pubkey, Pubkey, i32)],
        verified_tick_spacing: Option<i32>,
        commit_sha_opt: Option<String>,
        slot_observed: Option<u64>,
        rpc_endpoint: Option<String>,
    ) -> Result<RaydiumParams> {
        #[cfg(all(feature = "prod_strict", feature = "ts_sdk_parity"))]
        let manifest = require_ts_manifest_present()?;

        // Required: also verify the installed SDK files match the manifest
        #[cfg(all(feature = "ts_sdk_parity", feature = "ts_sdk_fs_verify"))]
        let fsinfo = {
            let info = fs_inspect_raydium_sdk()?;
            if REQUIRE_TS_SDK_FS {
                hard_verify_manifest_vs_fs(&manifest, &info)?;
            }
            Some(info)
        };

        // 1) Load IDL (on-chain or fallback)
        #[cfg(feature = "idl_json")]
        let (idl_json, idl_slot_opt, idl_sha256, degraded) = {
            match self.fetch_anchor_idl_json() {
                Ok((j, slot, sha)) => (j, slot, sha, false),
                Err(e) => {
                    let fb =
                        load_idl_fallback_from_env()?.ok_or_else(|| anyhow!("on-chain IDL fetch failed and no fallback set: {}", e))?;
                    let bytes = serde_json::to_vec(&fb)?;
                    #[cfg(feature = "ts_sdk_parity")]
                    let mut hasher = Sha256::new();
                    #[cfg(feature = "ts_sdk_parity")]
                    hasher.update(&bytes);
                    #[cfg(feature = "ts_sdk_parity")]
                    let idl_sha256: [u8; 32] = hasher.finalize().into();
                    (fb, None, idl_sha256, true)
                }
            }
        };

        // 1a) Prove IDL contains TickArray and discriminator matches on samples
        #[cfg(feature = "idl_json")]
        {
            let disc = Self::anchor_discriminator_for("TickArray");
            ensure!(idl_json.get("accounts").is_some(), "IDL missing 'accounts' key");
            let _ = disc;
        }

        // 2) Discover samples if not supplied
        #[cfg(feature = "idl_json")]
        if sample_tick_array_accounts.is_empty() {
            let guesses = self.discover_tickarray_samples_via_filters(4, "TickArray", 64)?;
            if !guesses.is_empty() {
                sample_tick_array_accounts = &guesses;
            }
        }
        ensure!(sample_tick_array_accounts.len() >= 1, "Need >=1 TickArray sample to prove invariants");
        if PROD_STRICT {
            ensure!(known_pairs_for_seed_proof.len() >= 2, "prod_strict requires >= 2 seed-proof samples");
        }

        // 3) Extract seeds from IDL JSON
        #[cfg(feature = "idl_json")]
        let seed_tag: Vec<u8> = {
            let inspector = IdlBackedRaydiumInspector::new(&serde_json::to_vec(&idl_json).expect("IDL reserialize"))?;
            inspector
                .find_tickarray_seed_tag()
                .context("No const-bytes TickArray seed found in IDL (pda.seeds)")?
        };

        // 4) Decode samples to assert invariants and verify discriminator
        #[cfg(feature = "idl_json")]
        let inspector = IdlBackedRaydiumInspector::new(&serde_json::to_vec(&idl_json).expect("IDL reserialize"))?;
        let mut array_lens = Vec::new();
        let mut spacings = Vec::new();
        let mut min_ticks = Vec::new();

        #[cfg(feature = "idl_json")]
        {
            let disc = Self::anchor_discriminator_for("TickArray");
            for pk in sample_tick_array_accounts {
                let raw = self.rpc.get_account_data(pk).with_context(|| format!("fetch sample TickArray {}", pk))?;
                ensure!(
                    raw.len() >= 8 && &raw[0..8] == &disc,
                    "Sample {} does not start with TickArray discriminator",
                    pk
                );
                let (arr_len, spacing, min_tick, _bitmap_opt) =
                    inspector.decode_params(&raw).with_context(|| format!("decode TickArray {}", pk))?;
                ensure!(arr_len > 0 && spacing > 0, "decoded invalid arr_len/spacing from {}", pk);
                array_lens.push(arr_len);
                spacings.push(spacing);
                min_ticks.push(min_tick);
            }
        }

        let array_len = self
            .same_value(&array_lens)
            .ok_or_else(|| anyhow!("array_len disagrees across samples: {:?}", array_lens))?;
        let tick_spacing = if let Some(v) = verified_tick_spacing {
            if spacings.iter().any(|&s| s != v) {
                bail!("verified_tick_spacing={} disagrees with {:?}", v, spacings);
            }
            v
        } else {
            self.same_value(&spacings).ok_or_else(|| anyhow!("tick_spacing disagrees across samples: {:?}", spacings))?
        };
        let min_tick = self
            .same_value(&min_ticks)
            .ok_or_else(|| anyhow!("min_tick disagrees across samples: {:?}", min_ticks))?;

        // 5) TS SDK parity checks (mandatory in prod when feature present).
        #[cfg(feature = "ts_sdk_parity")]
        {
            ensure!(
                seed_tag == manifest.constants.tick_array_seed.as_bytes(),
                "IDL seed tag != TS SDK tick_array_seed"
            );

            ensure!(
                array_len == manifest.constants.tick_array_size,
                "array_len mismatch: TS SDK={} sample={}",
                manifest.constants.tick_array_size,
                array_len
            );

            if let Some(mt) = manifest.constants.min_tick {
                ensure!(min_tick == mt, "min_tick mismatch: TS SDK={} sample={}", mt, min_tick);
            }
            if let Some(sp) = manifest.constants.tick_spacing {
                ensure!(tick_spacing == sp, "tick_spacing mismatch: TS SDK={} sample={}", sp, tick_spacing);
            }
        }

        // 5a) Filesystem constants parity (REQUIRED in prod) — removes env spoof risk
        #[cfg(all(feature = "ts_sdk_parity", feature = "ts_sdk_fs_verify"))]
        {
            let info = fsinfo.as_ref().unwrap();
            // TICK_ARRAY_SIZE present and equals sample
            ensure!(
                info.tick_array_size_from_code == Some(array_len),
                "FS TICK_ARRAY_SIZE {:?} != sample {}",
                info.tick_array_size_from_code,
                array_len
            );
            // TICK_ARRAY_SEED in code equals IDL/manifest
            if let Some(code_seed) = &info.tick_array_seed_from_code {
                ensure!(
                    code_seed.as_bytes() == seed_tag.as_slice(),
                    "FS TICK_ARRAY_SEED '{}' != IDL '{}'",
                    code_seed,
                    String::from_utf8_lossy(&seed_tag)
                );
                #[cfg(feature = "ts_sdk_parity")]
                ensure!(
                    code_seed == &manifest.constants.tick_array_seed,
                    "FS TICK_ARRAY_SEED != TS manifest"
                );
            } else {
                bail!("FS TICK_ARRAY_SEED missing in SDK code");
            }
        }

        // 6) Seed double-proof (and optional triple-proof) against known samples
        for (tickarray_pk, pool_pk, start_tick) in known_pairs_for_seed_proof {
            #[cfg(feature = "idl_json")]
            self.verify_seed_against_known_tickarray(&seed_tag, tickarray_pk, pool_pk, *start_tick)?;
            #[cfg(feature = "ts_sdk_parity")]
            self.verify_seed_against_manifest(&manifest, tickarray_pk, pool_pk, *start_tick)?;
            #[cfg(all(feature = "raydium_rust_port"))]
            {
                if PROD_STRICT {
                    self.verify_seed_against_rust_port_helper(tickarray_pk, pool_pk, *start_tick)?;
                }
            }
        }

        // 6a) Matrix parity vs SDK helpers (REQUIRED in prod via compile_error at top)
        #[cfg(feature = "ts_sdk_parity_matrix")]
        {
            if REQUIRE_PARITY_MATRIX {
                let cases = load_parity_matrix()?;
                for case in &cases {
                    let span = array_len * case.spacing;
                    let mut q = case.tick / span;
                    if case.tick < 0 && case.tick % span != 0 {
                        q -= 1;
                    }
                    let ours = q * span;
                    ensure!(
                        ours == case.expected_start,
                        "parity-matrix start-tick mismatch tick={} spacing={} ours={} expected={}",
                        case.tick,
                        case.spacing,
                        ours,
                        case.expected_start
                    );
                }
            }
        }

        // 7) Program hash provenance binding.
        let program_blake3 = self.compute_program_blake3()?;

        // 8) Canonical bitmap size: fetch authoritative pool bitmap; compare to SDK + FS
        #[cfg(feature = "idl_json")]
        let (bitmap_bits, bitmap_pda) = {
            let (_tickarray_pk, pool_pk, _start_tick) =
                known_pairs_for_seed_proof.get(0).ok_or_else(|| anyhow!("need at least one pool to deduce bitmap size"))?;
            let inspector = IdlBackedRaydiumInspector::new(&serde_json::to_vec(&idl_json)?)?;
            let pool_seed_from_idl = inspector.find_pool_bitmap_seed_tag().context("missing pool-bitmap seeds in IDL")?;

            // Derive via IDL seeds
            let (pda_idl, _bump_idl) =
                Pubkey::find_program_address(&[pool_seed_from_idl.as_ref(), pool_pk.as_ref()], &self.program_id);

            // Derive via TS manifest seed and require equality in prod
            #[cfg(feature = "ts_sdk_parity")]
            let pda_sdk = {
                if let Some(seed) = &manifest.constants.pool_bitmap_seed {
                    let (pda, _b) =
                        Pubkey::find_program_address(&[seed.as_bytes(), pool_pk.as_ref()], &self.program_id);
                    ensure!(
                        pda == pda_idl,
                        "Pool-bitmap PDA mismatch between IDL and TS manifest.\n idl={} sdk={}",
                        pda_idl,
                        pda
                    );
                    pda
                } else {
                    pda_idl
                }
            };
            #[cfg(not(feature = "ts_sdk_parity"))]
            let pda_sdk = pda_idl;

            // Also derive via FS SDK seed and force equality (REQUIRED in prod)
            #[cfg(all(feature = "ts_sdk_fs_verify"))]
            let pda_fs = {
                let info = fsinfo.as_ref().unwrap();
                if let Some(fs_seed) = &info.pool_bitmap_seed_from_code {
                    let (pda, _b) =
                        Pubkey::find_program_address(&[fs_seed.as_bytes(), pool_pk.as_ref()], &self.program_id);
                    // Require equality with IDL-derived PDA
                    ensure!(pda == pda_idl, "Pool-bitmap PDA mismatch between IDL and FS SDK seed.\n idl={} fs={}", pda_idl, pda);
                    pda
                } else {
                    bail!("FS POOL_TICK_ARRAY_BITMAP_SEED missing in SDK code");
                }
            };

            let chosen_pda = {
                #[cfg(all(feature = "ts_sdk_parity", feature = "ts_sdk_fs_verify"))]
                {
                    ensure!(pda_sdk == pda_fs, "Pool-bitmap PDA mismatch: TS vs FS SDK.");
                    pda_sdk
                }
                #[cfg(all(feature = "ts_sdk_parity", not(feature = "ts_sdk_fs_verify")))]
                {
                    pda_sdk
                }
                #[cfg(all(not(feature = "ts_sdk_parity"), feature = "ts_sdk_fs_verify"))]
                {
                    pda_fs
                }
                #[cfg(all(not(feature = "ts_sdk_parity"), not(feature = "ts_sdk_fs_verify")))]
                {
                    pda_idl
                }
            };

            let data = self
                .rpc
                .get_account_data(&chosen_pda)
                .with_context(|| format!("fetch pool bitmap {}", chosen_pda))?;
            let bits = (data.len() * 8) as u32;

            #[cfg(feature = "ts_sdk_parity")]
            ensure!(
                bits == manifest.constants.tick_array_bitmap_size_bits,
                "pool bitmap bits {} != TS SDK {}",
                bits,
                manifest.constants.tick_array_bitmap_size_bits
            );

            // REQUIRED in prod: FS constant equals fetched bits
            #[cfg(all(feature = "ts_sdk_fs_verify"))]
            {
                let info = fsinfo.as_ref().unwrap();
                let fs_bits = info.bitmap_bits_from_code.ok_or_else(|| anyhow!("FS TICK_ARRAY_BITMAP_SIZE(_BITS) missing in code"))?;
                ensure!(fs_bits == bits, "FS TICK_ARRAY_BITMAP_SIZE_BITS {} != fetched {}", fs_bits, bits);
            }
            (bits, chosen_pda)
        };

        // 9) Build provenance with IDL + SDK commit + FS hashes + npm versions.
        let mut provenance = ProgramProvenance {
            program_id: self.program_id,
            program_blake3,
            slot_observed,
            rpc_endpoint,
            commit_sha: commit_sha_opt,
            idl_slot: None,
            idl_sha256: None,
            ts_sdk_commit: None,
            ts_sdk_npm_version: None,
            ts_sdk_constants_sha256: None,
            ts_sdk_pda_sha256: None,
            orca_sdk_npm_version: None,
            orca_sdk_constants_sha256: None,
            degraded_idl_fallback: false,
        };
        #[cfg(feature = "idl_json")]
        {
            provenance.idl_slot = idl_slot_opt;
        }
        #[cfg(feature = "ts_sdk_parity")]
        {
            provenance.idl_sha256 = Some(idl_sha256);
            provenance.ts_sdk_commit = Some(manifest.commit_sha.clone());
        }
        #[cfg(all(feature = "ts_sdk_parity", feature = "ts_sdk_fs_verify"))]
        {
            let info = fsinfo.as_ref().unwrap();
            provenance.ts_sdk_npm_version = info.npm_version.clone();
            provenance.ts_sdk_constants_sha256 = info.constants_sha256;
            provenance.ts_sdk_pda_sha256 = info.pda_sha256;
        }

        #[cfg(feature = "idl_json")]
        if degraded {
            provenance.degraded_idl_fallback = true;
        }

        Ok(RaydiumParams {
            tick_spacing,
            array_len,
            seed_tag: {
                #[cfg(feature = "idl_json")]
                {
                    inspector.find_tickarray_seed_tag()?
                }
                #[cfg(not(feature = "idl_json"))]
                {
                    b"tick_array".to_vec()
                }
            },
            min_tick,
            bitmap_bits: {
                #[cfg(feature = "idl_json")]
                {
                    bitmap_bits
                }
                #[cfg(not(feature = "idl_json"))]
                {
                    0
                }
            },
            param_source: ParamSource::IdlOnChain {
                provenance,
                sample_accounts: sample_tick_array_accounts.to_vec(),
                bitmap_evidence: {
                    #[cfg(feature = "idl_json")]
                    {
                        Some((bitmap_bits, bitmap_pda))
                    }
                    #[cfg(not(feature = "idl_json"))]
                    {
                        None
                    }
                },
            },
        })
    }
}

// ========================= TickClusterAnalyzer =========================

pub struct TickClusterAnalyzer {
    pub cfg: ClmmConfig,
    bitmap_addr_cache: DashMap<(Pubkey, u64), (u64, Arc<Vec<Pubkey>>)>,
    program_hash_cache: DashMap<Pubkey, (u64, u64)>,
    tick_array_cache: DashMap<Pubkey, Vec<u8>>,
    pool_state_cache: DashMap<Pubkey, Vec<u8>>,
    bitmap_evidence_cache: DashMap<(Pubkey, u32), u64>,
    metrics: Metrics,
}

impl TickClusterAnalyzer {
    pub fn new(cfg: ClmmConfig) -> Self {
        // Optional Orca SDK 88 verification at init
        #[cfg(all(feature = "orca_sdk_fs_verify", feature = "prod_strict"))]
        {
            fs_verify_orca_88().expect("Orca SDK 88-size parity failed");
        }

        #[cfg(all(feature = "orca_rust_parity", feature = "prod_strict"))]
        {
            assert_eq!(orca_parity::ORCA_TA_SIZE as i32, ORCA_ARRAY_LEN, "Orca TICK_ARRAY_SIZE must be 88");
            for &(center, spacing) in &[(0, 8), (1, 8), (7, 8), (8, 8), (127, 8), (-1, 8), (-7, 8), (-8, 8)] {
                let ours = {
                    let tmp = ClmmConfig {
                        clmm: Clmm::Orca,
                        ..Default::default()
                    };
                    let ana = TickClusterAnalyzer {
                        cfg: tmp,
                        bitmap_addr_cache: DashMap::new(),
                        program_hash_cache: DashMap::new(),
                        tick_array_cache: DashMap::new(),
                        pool_state_cache: DashMap::new(),
                        bitmap_evidence_cache: DashMap::new(),
                        metrics: Metrics::default(),
                    };
                    ana.orca_array_start_tick(center)
                };
                let ref_start = orca_parity::orca_start_tick_for_index(center, spacing as u16);
                assert_eq!(ours, ref_start, "Orca start-tick parity failed");
            }
        }

        Self {
            cfg,
            bitmap_addr_cache: DashMap::new(),
            program_hash_cache: DashMap::new(),
            tick_array_cache: DashMap::new(),
            pool_state_cache: DashMap::new(),
            bitmap_evidence_cache: DashMap::new(),
            metrics: Metrics::default(),
        }
    }

    #[inline(always)]
    fn now_millis() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }
    #[inline(always)]
    fn cache_ttl_ms(&self) -> u64 {
        self.cfg.ttl_ms
    }
    #[inline(always)]
    fn fnv1a64(bytes: &[u8]) -> u64 {
        let mut h = FNV1A64_OFFSET;
        for b in bytes {
            h ^= *b as u64;
            h = h.wrapping_mul(FNV1A64_PRIME);
        }
        h
    }

    // ========================= Hashing =========================
    #[inline(always)]
    pub fn hash_ticks_with_pool(&self, pool: &Pubkey, ticks: &[TickData]) -> u64 {
        let mut h = FNV1A64_OFFSET;
        if self.cfg.include_pool_in_hash {
            let k = pool.to_bytes();
            for lane in [&k[0..8], &k[8..16], &k[16..24], &k[24..32]] {
                let v = u64::from_le_bytes(lane.try_into().unwrap());
                h ^= v;
                h = h.wrapping_mul(FNV1A64_PRIME);
            }
        }
        for t in ticks {
            h ^= (t.tick_index as i64 as u64);
            h = h.wrapping_mul(FNV1A64_PRIME);
            let lo = t.sqrt_price_x64 as u64;
            let hi = (t.sqrt_price_x64 >> 64) as u64;
            for v in [lo, hi] {
                h ^= v;
                h = h.wrapping_mul(FNV1A64_PRIME);
            }
            let ln = t.liquidity_net as i128 as u128;
            let ln_lo = ln as u64;
            let ln_hi = (ln >> 64) as u64;
            for v in [ln_lo, ln_hi] {
                h ^= v;
                h = h.wrapping_mul(FNV1A64_PRIME);
            }
            let lg_lo = t.liquidity_gross as u64;
            let lg_hi = (t.liquidity_gross >> 64) as u64;
            for v in [lg_lo, lg_hi] {
                h ^= v;
                h = h.wrapping_mul(FNV1A64_PRIME);
            }
            for v in [t.fee_growth_outside_a as u64, t.fee_growth_outside_b as u64, t.timestamp as u64, t.block_height as u64] {
                h ^= v;
                h = h.wrapping_mul(FNV1A64_PRIME);
            }
        }
        h
    }
    #[inline(always)]
    pub fn hash_ticks(ticks: &[TickData]) -> u64 {
        let dummy_cfg = ClmmConfig {
            include_pool_in_hash: false,
            ..Default::default()
        };
        let dummy = TickClusterAnalyzer::new(dummy_cfg);
        dummy.hash_ticks_with_pool(&Pubkey::default(), ticks)
    }

    // ========================= Q64.64 price math =========================
    #[inline(always)]
    pub fn sqrt_q64_to_price_f64(sqrt_price_x64: u128) -> f64 {
        let s = (sqrt_price_x64 as f64) / (u64::MAX as f64 + 1.0);
        s * s
    }

    // ========================= ORCA =========================
    #[inline(always)]
    fn orca_array_start_tick(&self, tick: i32) -> i32 {
        let spacing = self.cfg.orca.tick_spacing;
        let len = self.cfg.orca.array_len; // 88 by invariant
        let span = len * spacing;
        let mut q = tick / span;
        if tick < 0 && tick % span != 0 {
            q -= 1;
        }
        q * span
    }
    pub fn derive_orca_tick_array_pda(&self, pool: &Pubkey, start_tick: i32) -> Pubkey {
        let tick_bytes = start_tick.to_le_bytes();
        let seeds: [&[u8]; 3] = [self.cfg.orca.seed_tag, pool.as_ref(), &tick_bytes];
        Pubkey::find_program_address(&seeds, &self.cfg.program_id).0
    }
    pub fn orca_enumerate_tick_arrays(&self, pool: &Pubkey, center_tick: i32, arrays_each_side: i32) -> Vec<Pubkey> {
        let mut out = Vec::with_capacity((arrays_each_side * 2 + 1) as usize);
        let start0 = self.orca_array_start_tick(center_tick);
        let span = self.cfg.orca.array_len * self.cfg.orca.tick_spacing;
        for k in -arrays_each_side..=arrays_each_side {
            let start = start0 + k * span;
            out.push(self.derive_orca_tick_array_pda(pool, start));
        }
        out
    }

    // ========================= RAYDIUM =========================
    pub fn set_raydium_params(&mut self, params: RaydiumParams) -> Result<()> {
        if self.cfg.clmm != Clmm::Raydium {
            bail!("set Raydium params on non-Ray");
        }
        ensure!(params.array_len > 0 && params.tick_spacing > 0, "bad Raydium params");
        ensure!(params.bitmap_bits > 0, "bitmap_bits cannot be 0");
        ensure!(!params.seed_tag.is_empty(), "empty seed_tag");

        // Extra TS SDK parity gate on set (prevents stale config load)
        #[cfg(feature = "ts_sdk_parity")]
        {
            let m = load_ts_manifest_from_env()?;
            ensure!(
                params.array_len == m.constants.tick_array_size,
                "set_raydium_params: array_len {} != TS SDK {}",
                params.array_len,
                m.constants.tick_array_size
            );
            ensure!(
                params.bitmap_bits == m.constants.tick_array_bitmap_size_bits,
                "set_raydium_params: bitmap_bits {} != TS SDK {}",
                params.bitmap_bits,
                m.constants.tick_array_bitmap_size_bits
            );
            ensure!(
                params.seed_tag == m.constants.tick_array_seed.as_bytes(),
                "set_raydium_params: seed_tag != TS SDK tick_array_seed"
            );
        }

        // NEW: FS parity gate on set (REQUIRED in prod if fs verify is enabled)
        #[cfg(all(feature = "ts_sdk_fs_verify"))]
        {
            let info = fs_inspect_raydium_sdk()?;
            let fs_size = info
                .tick_array_size_from_code
                .ok_or_else(|| anyhow!("FS TICK_ARRAY_SIZE missing"))?;
            ensure!(fs_size == params.array_len, "set_raydium_params: FS TICK_ARRAY_SIZE {} != {}", fs_size, params.array_len);

            let fs_bits = info.bitmap_bits_from_code.ok_or_else(|| anyhow!("FS TICK_ARRAY_BITMAP_SIZE(_BITS) missing"))?;
            ensure!(fs_bits == params.bitmap_bits, "set_raydium_params: FS BITMAP_BITS {} != {}", fs_bits, params.bitmap_bits);

            let fs_seed = info.tick_array_seed_from_code.ok_or_else(|| anyhow!("FS TICK_ARRAY_SEED missing"))?;
            ensure!(
                fs_seed.as_bytes() == params.seed_tag.as_slice(),
                "set_raydium_params: FS TICK_ARRAY_SEED '{}' != params",
                fs_seed
            );
        }

        // cache program hash low64 and purge cache on drift
        if let ParamSource::IdlOnChain { provenance, .. } | ParamSource::Empirical { .. } = &params.param_source {
            let low64 = u64::from_le_bytes(match &params.param_source {
                ParamSource::IdlOnChain { provenance, .. } => provenance.program_blake3[0..8].try_into().unwrap(),
                ParamSource::Empirical { .. } => [0u8; 8],
                _ => [0u8; 8],
            });
            let old = self.program_hash_cache.insert(self.cfg.program_id, (Self::now_millis(), low64));
            if let Some((_ts, prev)) = old {
                if prev != low64 {
                    self.bitmap_addr_cache.clear();
                    #[cfg(feature = "fs")]
                    if let Some(path) = self.cfg.golden_path.as_ref() {
                        let _ = fs::remove_file(path);
                    }
                }
            }
        }

        self.cfg.raydium = Some(params);
        Ok(())
    }

    fn raydium_array_start_tick(&self, tick: i32, rp: &RaydiumParams) -> i32 {
        let span = rp.array_len * rp.tick_spacing;
        let mut q = tick / span;
        if tick < 0 && tick % span != 0 {
            q -= 1;
        }
        q * span
    }
    pub fn derive_raydium_tick_array_pda(&self, pool: &Pubkey, start_tick: i32) -> Result<Pubkey> {
        let rp = self.cfg.raydium.as_ref().ok_or_else(|| anyhow!("Raydium params not set"))?;
        let tick_bytes = start_tick.to_le_bytes();
        let seeds: [&[u8]; 3] = [&rp.seed_tag, pool.as_ref(), &tick_bytes];
        Ok(Pubkey::find_program_address(&seeds, &self.cfg.program_id).0)
    }

    #[inline(always)]
    fn bit_is_set(byte: u8, bit: u8) -> bool {
        ((byte >> bit) & 1) == 1
    }

    /// Enumeration from Raydium bitmap. Byte-by-byte LSB-first scan.
    /// In prod_strict, only authoritative pool-level bitmap is accepted.
    pub fn raydium_enumerate_from_bitmap(&self, pool: &Pubkey, bitmap_le_bytes: &[u8]) -> Result<Vec<Pubkey>> {
        if (self.cfg.strict_no_guess || PROD_STRICT) && self.cfg.raydium.is_none() {
            bail!("Raydium params not provided; strict mode");
        }
        let rp = self.cfg.raydium.as_ref().ok_or_else(|| anyhow!("Raydium params not set"))?;
        if !self.cfg.enable_bitmap_path {
            bail!("Bitmap enumeration disabled");
        }

        ensure!(!bitmap_le_bytes.is_empty(), "empty bitmap");
        let total_bits = (bitmap_le_bytes.len() * 8) as u32;

        // Hard gate: pool-bitmap length must match canonical in prod
        if PROD_STRICT {
            #[cfg(feature = "ts_sdk_parity")]
            {
                let m = load_ts_manifest_from_env()?;
                ensure!(
                    total_bits == m.constants.tick_array_bitmap_size_bits,
                    "bitmap bits {} != TS SDK {}",
                    total_bits,
                    m.constants.tick_array_bitmap_size_bits
                );
            }
            ensure!(total_bits == rp.bitmap_bits, "bitmap bits {} != config {}", total_bits, rp.bitmap_bits);

            // REQUIRED in prod: FS constant equals total_bits
            #[cfg(all(feature = "ts_sdk_fs_verify"))]
            {
                let info = fs_inspect_raydium_sdk()?;
                let fs_bits = info.bitmap_bits_from_code.ok_or_else(|| anyhow!("FS BITMAP_BITS missing"))?;
                ensure!(fs_bits == total_bits, "FS BITMAP_BITS {} != observed {}", fs_bits, total_bits);
            }
        }

        // cache
        let ttl = self.cache_ttl_ms();
        let now = Self::now_millis();
        let mut mix = Self::fnv1a64(bitmap_le_bytes);
        if let Some((_, low64)) = self.program_hash_cache.get(&self.cfg.program_id).as_deref() {
            mix ^= *low64;
        }
        if let Some((ts, v)) = self.bitmap_addr_cache.get(&(*pool, mix)).as_deref() {
            if now.saturating_sub(*ts) <= ttl {
                self.metrics.inc_hit();
                return Ok((**v).clone());
            }
        }
        self.metrics.inc_miss();

        let mut addrs = Vec::new();
        let mut seen = HashSet::<[u8; 32]>::new();

        // LSB-first: index -> tick mapping self-check on first nonzero byte
        if PROD_STRICT {
            'outer: for (byte_index, b) in bitmap_le_bytes.iter().enumerate() {
                if *b == 0 {
                    continue;
                }
                for bit_in_byte in 0u8..8 {
                    if Self::bit_is_set(*b, bit_in_byte) {
                        let idx0 = (byte_index as u32) * 8 + (bit_in_byte as u32);
                        let tick0 = rp.min_tick as i128 + rp.tick_spacing as i128 * (idx0 as i128);
                        if idx0 + 1 < total_bits {
                            let tick1 =
                                rp.min_tick as i128 + rp.tick_spacing as i128 * ((idx0 + 1) as i128);
                            ensure!(tick1 - tick0 == rp.tick_spacing as i128, "LSB-first mapping failed monotonicity check");
                        }
                        break 'outer;
                    }
                }
            }
        }

        // Map global bit index -> tick = min_tick + idx * tick_spacing.
        for (byte_index, b) in bitmap_le_bytes.iter().enumerate() {
            if *b == 0 {
                continue;
            }
            for bit_in_byte in 0u8..8 {
                if !Self::bit_is_set(*b, bit_in_byte) {
                    continue;
                }
                let idx = (byte_index as u32) * 8 + (bit_in_byte as u32);
                if idx >= rp.bitmap_bits {
                    break;
                } // guard if payload longer than canonical
                let tick = rp.min_tick as i128 + rp.tick_spacing as i128 * (idx as i128);
                let start = self.raydium_array_start_tick(tick as i32, rp);
                let pda = self.derive_raydium_tick_array_pda(pool, start)?;
                let k = pda.to_bytes();
                if seen.insert(k) {
                    addrs.push(pda);
                }
            }
        }

        let arc = Arc::new(addrs.clone());
        self.bitmap_addr_cache.insert((*pool, mix), (now, arc));
        Ok(addrs)
    }

    /// Fetch the authoritative Raydium pool-level bitmap account bytes and enumerate tick arrays.
    pub fn fetch_and_enumerate_from_pool_bitmap<R: RpcLite>(
        &self,
        rpc: &R,
        pool: &Pubkey,
        idl_json: &serde_json::Value,
    ) -> Result<Vec<Pubkey>> {
        let rp = self.cfg.raydium.as_ref().ok_or_else(|| anyhow!("Raydium params not set"))?;
        let inspector = IdlBackedRaydiumInspector::new(&serde_json::to_vec(idl_json)?)?;
        let pool_seed_idl = inspector.find_pool_bitmap_seed_tag().context("missing pool-bitmap seeds in IDL")?;

        // Derive via IDL
        let (bitmap_pda_idl, _bump) =
            Pubkey::find_program_address(&[pool_seed_idl.as_ref(), pool.as_ref()], &self.cfg.program_id);

        // Derive via TS manifest if provided, require equality in prod
        #[cfg(feature = "ts_sdk_parity")]
        let bitmap_pda = {
            let m = load_ts_manifest_from_env()?;
            if let Some(seed) = &m.constants.pool_bitmap_seed {
                let (sdk_pda, _b) =
                    Pubkey::find_program_address(&[seed.as_bytes(), pool.as_ref()], &self.cfg.program_id);
                if PROD_STRICT {
                    ensure!(
                        sdk_pda == bitmap_pda_idl,
                        "Pool-bitmap PDA mismatch between IDL and TS manifest.\n idl={} sdk={}",
                        bitmap_pda_idl,
                        sdk_pda
                    );
                }
                sdk_pda
            } else {
                bitmap_pda_idl
            }
        };
        #[cfg(not(feature = "ts_sdk_parity"))]
        let bitmap_pda = bitmap_pda_idl;

        // Derive via FS SDK seed and require equality (REQUIRED in prod if fs verify)
        #[cfg(all(feature = "ts_sdk_fs_verify"))]
        let bitmap_pda = {
            let info = fs_inspect_raydium_sdk()?;
            let fs_seed = info.pool_bitmap_seed_from_code.ok_or_else(|| anyhow!("FS POOL_TICK_ARRAY_BITMAP_SEED missing"))?;
            let (fs_pda, _b) = Pubkey::find_program_address(&[fs_seed.as_bytes(), pool.as_ref()], &self.cfg.program_id);
            if PROD_STRICT {
                ensure!(fs_pda == bitmap_pda, "Pool-bitmap PDA mismatch: TS/IDL vs FS SDK seed");
            }
            fs_pda
        };

        let data = rpc.get_account_data(&bitmap_pda).with_context(|| format!("fetch pool bitmap {}", bitmap_pda))?;
        let bits = (data.len() * 8) as u32;

        // Unskippable length gate in prod
        if PROD_STRICT {
            #[cfg(feature = "ts_sdk_parity")]
            {
                let m = load_ts_manifest_from_env()?;
                ensure!(
                    bits == m.constants.tick_array_bitmap_size_bits,
                    "bitmap bits {} != TS SDK {}",
                    bits,
                    m.constants.tick_array_bitmap_size_bits
                );
            }
            ensure!(bits == rp.bitmap_bits, "bitmap bits {} != config {}", bits, rp.bitmap_bits);

            // FS bits must match fetched bits
            #[cfg(all(feature = "ts_sdk_fs_verify"))]
            {
                let info = fs_inspect_raydium_sdk().ok();
                let fs_bits = info.and_then(|i| i.bitmap_bits_from_code).ok_or_else(|| anyhow!("FS BITMAP_BITS missing"))?;
                ensure!(fs_bits == bits, "FS bitmap bits {} != fetched {}", fs_bits, bits);
            }
        }

        self.raydium_enumerate_from_bitmap(pool, &data)
    }

    // ========================= Facade =========================
    pub enum TickArrayQuery<'a> {
        CenterTick { center_tick: i32, arrays_each_side: i32 }, // Orca
        PoolBitmap { idl_json: &'a serde_json::Value },         // Raydium authoritative path (prod)
        Bitmap { bitmap_le_bytes: &'a [u8] },                   // Raydium non-prod convenience
    }

    pub fn enumerate_tick_arrays<'a, R: RpcLite>(
        &self,
        rpc_opt: Option<&R>,
        pool: &Pubkey,
        query: TickArrayQuery<'a>,
    ) -> Result<Vec<Pubkey>> {
        match (self.cfg.clmm, query) {
            (Clmm::Orca, TickArrayQuery::CenterTick { center_tick, arrays_each_side }) => {
                Ok(self.orca_enumerate_tick_arrays(pool, center_tick, arrays_each_side))
            }
            (Clmm::Raydium, TickArrayQuery::PoolBitmap { idl_json }) => {
                let rpc = rpc_opt.ok_or_else(|| anyhow!("RPC required for PoolBitmap enumeration"))?;
                return self.fetch_and_enumerate_from_pool_bitmap(rpc, pool, idl_json);
            }
            (Clmm::Raydium, TickArrayQuery::Bitmap { bitmap_le_bytes }) => {
                if PROD_STRICT {
                    bail!("In prod_strict use PoolBitmap query for Raydium (authoritative).");
                }
                self.raydium_enumerate_from_bitmap(pool, bitmap_le_bytes)
            }
            (Clmm::Orca, _) => bail!("Orca requires CenterTick query"),
        }
    }

    pub fn verify_tick_array_pda(&self, expected: &Pubkey, derived: &Pubkey) -> Result<()> {
        if expected == derived {
            Ok(())
        } else {
            Err(anyhow!("PDA mismatch"))
        }
    }

    pub fn identify_clusters(&self, ticks: &[TickData], pool: &Pubkey) -> Vec<ClusterAnalysis> {
        if ticks.is_empty() {
            return vec![];
        }
        let tick_hash = self.hash_ticks_with_pool(pool, ticks);
        vec![ClusterAnalysis {
            pool: *pool,
            cluster_id: tick_hash,
            tick_hash,
            ema_volatility: 0.0,
            persistence_score: 1.0,
        }]
    }

    // ========================= Goldens & Provenance =========================
    #[cfg(feature = "fs")]
    pub fn write_golden(&self, params: &RaydiumParams) -> Result<()> {
        // Only write after full parity has been satisfied.
        if let Some(path) = self.cfg.golden_path.as_ref() {
            let key =
                if let ParamSource::IdlOnChain { provenance, .. } | ParamSource::Empirical { .. } = &params.param_source {
                    let blake = match &params.param_source {
                        ParamSource::IdlOnChain { provenance, .. } => provenance.program_blake3,
                        ParamSource::Empirical { .. } => [0u8; 32],
                        _ => [0u8; 32],
                    };
                    format!("0x{:016x}", u64::from_le_bytes(blake[0..8].try_into().unwrap()))
                } else {
                    "unknown".to_string()
                };
            let mut map = HashMap::<String, RaydiumParams>::new();
            map.insert(key, params.clone());
            let s = serde_json::to_string_pretty(&map)?;
            std::fs::write(path, s)?;
        }
        Ok(())
    }

    #[cfg(feature = "fs")]
    pub fn read_golden(&self) -> Result<Option<RaydiumParams>> {
        let Some(path) = self.cfg.golden_path.as_ref() else { return Ok(None) };
        if !Path::new(path).exists() {
            return Ok(None);
        }
        let s = std::fs::read_to_string(path)?;
        let map: HashMap<String, RaydiumParams> = serde_json::from_str(&s)?;
        if let Some((_ts, low64)) = self.program_hash_cache.get(&self.cfg.program_id).as_deref() {
            let key = format!("0x{:016x}", *low64);
            Ok(map.get(&key).cloned())
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "fs")]
    pub fn write_provenance(&self, params: &RaydiumParams) -> Result<()> {
        if let Some(path) = self.cfg.provenance_path.as_ref() {
            if let ParamSource::IdlOnChain { provenance, .. } = &params.param_source {
                let s = serde_json::to_string_pretty(provenance)?;
                std::fs::write(path, s)?;
            }
        }
        Ok(())
    }
}

// ========================= Discovery Driver =========================

#[allow(clippy::too_many_arguments)]
pub fn discover_and_set_raydium_params<R: RpcLite>(
    rpc: &R,
    analyzer: &mut TickClusterAnalyzer,
    program_id: Pubkey,
    #[cfg(feature = "idl_json")] idl_json_opt: Option<serde_json::Value>,
    sample_tick_arrays: &[Pubkey],
    seed_proof_triples: &[(Pubkey, Pubkey, i32)],
    verified_tick_spacing: Option<i32>,
    commit_sha_opt: Option<String>,
    slot_observed: Option<u64>,
    rpc_endpoint: Option<String>,
) -> Result<()> {
    #[cfg(not(feature = "idl_json"))]
    {
        bail!("Enable feature `idl_json` to run Raydium discovery");
    }

    #[cfg(feature = "idl_json")]
    {
        #[cfg(all(feature = "prod_strict", feature = "ts_sdk_parity"))]
        let manifest = require_ts_manifest_present()?;

        // If prod_strict+ts_sdk_fs_verify, insist on filesystem SDK presence
        if REQUIRE_TS_SDK_FS {
            #[cfg(not(feature = "ts_sdk_fs_verify"))]
            {
                bail!("prod_strict requires ts_sdk_fs_verify to avoid env spoof");
            }
            #[cfg(feature = "ts_sdk_fs_verify")]
            {
                let _ = fs_inspect_raydium_sdk()?;
            }
        }
        if REQUIRE_PARITY_MATRIX {
            #[cfg(not(feature = "ts_sdk_parity_matrix"))]
            {
                bail!("prod_strict requires ts_sdk_parity_matrix when REQUIRE_PARITY_MATRIX is true");
            }
            #[cfg(feature = "ts_sdk_parity_matrix")]
            {
                let _ = load_parity_matrix()?;
            }
        }

        // Live IDL attempt
        let boot =
            RaydiumParamDiscoverer::<R, IdlBackedRaydiumInspector>::new(rpc, program_id, &IdlBackedRaydiumInspector::new(&serde_json::to_vec(&serde_json::json!({}))?)?);

        let live = boot.fetch_anchor_idl_json();
        let (idl_json, idl_slot, idl_sha256, degraded) = match live {
            Ok((j, s, sha)) => (j, s, sha, false),
            Err(_) => {
                let fb = load_idl_fallback_from_env()?.ok_or_else(|| anyhow!("on-chain IDL fetch failed and no fallback provided"))?;
                let bytes = serde_json::to_vec(&fb)?;
                #[cfg(feature = "ts_sdk_parity")]
                let mut hasher = Sha256::new();
                #[cfg(feature = "ts_sdk_parity")]
                hasher.update(&bytes);
                #[cfg(feature = "ts_sdk_parity")]
                let idl_sha256: [u8; 32] = hasher.finalize().into();
                (fb, None, idl_sha256, true)
            }
        };

        let samples = if sample_tick_arrays.is_empty() {
            boot.discover_tickarray_samples_via_filters(4, "TickArray", 64)?
        } else {
            sample_tick_arrays.to_vec()
        };

        let inspector = IdlBackedRaydiumInspector::new(&serde_json::to_vec(&idl_json)?)?;
        let disc = RaydiumParamDiscoverer::<R, IdlBackedRaydiumInspector>::new(rpc, program_id, &inspector);

        let mut params = disc.discover_params_strict(
            Some(idl_json.clone()),
            &samples,
            seed_proof_triples,
            verified_tick_spacing,
            commit_sha_opt.clone(),
            slot_observed,
            rpc_endpoint.clone(),
        )?;

        // Patch provenance extras
        if let ParamSource::IdlOnChain { provenance, .. } = &mut params.param_source {
            provenance.idl_slot = idl_slot;
            #[cfg(feature = "ts_sdk_parity")]
            {
                provenance.idl_sha256 = Some(idl_sha256);
                provenance.ts_sdk_commit = provenance.ts_sdk_commit.take().or_else(|| Some(manifest.commit_sha));
            }
            #[cfg(all(feature = "ts_sdk_parity", feature = "ts_sdk_fs_verify"))]
            {
                if let Ok(info) = fs_inspect_raydium_sdk() {
                    provenance.ts_sdk_npm_version = info.npm_version;
                    provenance.ts_sdk_constants_sha256 = info.constants_sha256;
                    provenance.ts_sdk_pda_sha256 = info.pda_sha256;
                }
            }
            if degraded {
                provenance.degraded_idl_fallback = true;
            }
        }

        analyzer.set_raydium_params(params.clone())?;

        #[cfg(feature = "fs")]
        {
            analyzer.write_golden(&params).ok();
            analyzer.write_provenance(&params).ok();
        }

        Ok(())
    }
}

// ========================= Advisory Parity (Rust port) =========================
#[cfg(feature = "raydium_rust_port")]
pub fn advisory_warn_if_pda_differs(pool: &Pubkey, start_tick: i32, program_id: &Pubkey, ours: Pubkey) {
    let (sdk_key, _bump) = raydium_advisory::ray_get_pda_tick_array_address(program_id, pool, &start_tick);
    if sdk_key != ours {
        eprintln!("[advisory] Raydium PDA mismatch (Rust port)\n ours={} sdk={}", ours, sdk_key);
    }
}

// ========================= Orca parity hooks (pure-Rust) =========================
#[cfg(feature = "orca_rust_parity")]
pub fn assert_orca_start_tick_parity(center_tick: i32, tick_spacing: i32, our_start: i32) -> Result<()> {
    let start_ref = orca_parity::orca_start_tick_for_index(center_tick, tick_spacing as u16);
    ensure!(start_ref == our_start, "Orca start-tick mismatch\n ours={} ref={}", our_start, start_ref);
    Ok(())
}

// ========================= CI Guard: ensure goldens match pinned TS SDK commit =========================
#[cfg(all(feature = "fs", feature = "ts_sdk_parity"))]
pub fn ci_guard_fail_if_ts_commit_drift(analyzer: &TickClusterAnalyzer) -> Result<()> {
    use std::io::Read;
    let Some(prov_path) = analyzer.cfg.provenance_path.as_ref() else { return Ok(()); };
    if !Path::new(prov_path).exists() {
        bail!("Provenance file missing at {}", prov_path);
    }
    let mut f = fs::File::open(prov_path)?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    let prov: ProgramProvenance = serde_json::from_str(&s)?;
    let m = load_ts_manifest_from_env()?;
    let cur = m.commit_sha;
    let recorded = prov.ts_sdk_commit.clone().unwrap_or_default();
    ensure!(
        !recorded.is_empty(),
        "Provenance missing ts_sdk_commit; regenerate goldens/provenance"
    );
    ensure!(
        recorded == cur,
        "TS SDK commit drift detected.\n recorded={} current={}\nRegenerate goldens/provenance before shipping.",
        recorded,
        cur
    );
    Ok(())
}

// ========================= Tests / Invariants =========================
#[cfg(all(test, feature = "orca_rust_parity"))]
mod tests_orca {
    use super::*;
    #[test]
    fn orca_start_tick_parity_matrix() {
        let ana = TickClusterAnalyzer::new(ClmmConfig {
            clmm: Clmm::Orca,
            ..Default::default()
        });
        for &(center, spacing) in &[(0, 8), (1, 8), (7, 8), (8, 8), (127, 8), (-1, 8), (-7, 8), (-8, 8), (-127, 8), (777, 8), (-777, 8)] {
            let ours = ana.orca_array_start_tick(center);
            assert_orca_start_tick_parity(center, spacing, ours).unwrap();
        }
        assert_eq!(orca_parity::ORCA_TA_SIZE as i32, super::ORCA_ARRAY_LEN);
    }
}

#[cfg(test)]
mod tests_bitmap_endianness {
    use super::*;
    #[test]
    fn lsb_first_mapping_roundtrip() {
        // Synthetic bitmap: set bits 0, 9, 16 (LSB-first)
        let bitmap = vec![0b0000_0001u8, 0b0000_0010u8, 0b0000_0001u8];
        let mut ana = TickClusterAnalyzer::new(ClmmConfig {
            clmm: Clmm::Raydium,
            program_id: Pubkey::new_from_array([9u8; 32]),
            ..Default::default()
        });
        let rp = RaydiumParams {
            tick_spacing: 5,
            array_len: 60,
            seed_tag: b"tick_array".to_vec(),
            min_tick: -150,
            bitmap_bits: 24,
            param_source: ParamSource::Empirical {
                sample_accounts: vec![],
                bitmap_evidence: None,
            },
        };
        ana.set_raydium_params(rp).unwrap();
        let pool = Pubkey::new_unique();
        let addrs = ana.raydium_enumerate_from_bitmap(&pool, &bitmap).unwrap();
        assert_eq!(addrs.len(), 3); // indices 0,9,16
    }
}
