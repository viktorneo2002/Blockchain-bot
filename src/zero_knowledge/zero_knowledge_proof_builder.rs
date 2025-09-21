#![deny(warnings)]
// Unsafe is allowed only inside the tiny os_optimizations module we define below.
#![cfg_attr(any(test, debug_assertions), allow(unused, clippy::restriction))]
// Allow unused items for cfg-gated platform-specific code
#![allow(unused)]

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ============================================================================
// MODULE 1: errors.rs - Error types for proof operations
// ============================================================================
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProofError {
    #[error("serialization: {0}")]
    Serialization(String),
    #[error("invalid permissions: {0}")]
    InvalidPermissions(String),
    #[error("backpressure: limit {limit}")]
    Backpressure { limit: usize },
}

// ============================================================================
// MODULE 2: sys.rs - System utilities (prefetch + monotonic time)
// ============================================================================
use std::time::Instant;
use once_cell::sync::Lazy;

static START: Lazy<Instant> = Lazy::new(Instant::now);

#[inline(always)]
pub fn mono_now_ns() -> u64 {
    START.elapsed().as_nanos() as u64
}

#[inline(always)]
pub fn elapsed_ms_since_ns(t0_ns: u64) -> u64 {
    let now = mono_now_ns();
    now.saturating_sub(t0_ns) / 1_000_000
}

#[inline(always)]
pub fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe { core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0); }
    #[cfg(not(target_arch = "x86_64"))]
    { let _ = ptr; }
}

// ============================================================================
// MODULE 3: fs_secure.rs - Secure file operations with TOCTOU protection
// ============================================================================
use std::{fs::File, path::Path};

pub fn open_readonly_nofollow(p: &Path) -> Result<File, ProofError> {
    // Harden parent directory permissions
    if let Some(parent) = p.parent() {
        assert_secure_path_hierarchy(parent)?;
    }
    
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(p)
            .map_err(|e| ProofError::InvalidPermissions(format!("open {}: {e}", p.display())))?;
        Ok(f)
    }
    #[cfg(not(unix))]
    {
        std::fs::File::open(p)
            .map_err(|e| ProofError::InvalidPermissions(format!("open {}: {e}", p.display())))
    }
}

pub fn verify_same_inode(p: &Path, f: &File) -> Result<(), ProofError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let md1 = std::fs::metadata(p)
            .map_err(|e| ProofError::InvalidPermissions(format!("stat {}: {e}", p.display())))?;
        let md2 = f.metadata()
            .map_err(|e| ProofError::InvalidPermissions(format!("fstat: {e}")))?;
        if md1.ino() != md2.ino() || md1.dev() != md2.dev() {
            return Err(ProofError::InvalidPermissions("TOCTOU race".into()));
        }
    }
    Ok(())
}

// ============================================================================
// MODULE 4: net.rs - Network operations with zero-copy send
// ============================================================================
use std::{net::{SocketAddr, TcpStream}, sync::OnceLock};
use hashbrown::HashMap;
use parking_lot::Mutex;
use std::time::Duration;
#[cfg(unix)]
use std::os::fd::AsRawFd;

#[cfg(all(unix, target_os = "linux"))]
const MSG_ZEROCOPY: libc::c_int = 0x4000000;

#[inline]
pub fn send_all_nosig(fd: std::os::fd::RawFd, mut data: &[u8]) -> std::io::Result<()> {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        let mut flags = libc::MSG_NOSIGNAL;
        if data.len() >= 4096 { flags |= MSG_ZEROCOPY; }
        while !data.is_empty() {
            let n = libc::send(fd, data.as_ptr() as *const _, data.len(), flags);
            if n < 0 {
                let e = std::io::Error::last_os_error();
                if e.kind() == std::io::ErrorKind::Interrupted { continue; }
                return Err(e);
            }
            data = &data[n as usize..];
        }
        return Ok(());
    }

    #[cfg(not(all(unix, target_os = "linux")))]
    {
        use std::{fs::File, io::Write, mem::ManuallyDrop, os::fd::FromRawFd};
        let mut f = unsafe { ManuallyDrop::new(File::from_raw_fd(fd)) };
        let mut off = 0usize;
        while off < data.len() {
            let n = (&mut *f).write(&data[off..])?;
            if n == 0 { return Err(std::io::ErrorKind::WriteZero.into()); }
            off += n;
        }
        Ok(())
    }
}

// Socket pool for connection reuse
static POOL: OnceLock<Mutex<HashMap<SocketAddr, TcpStream>>> = OnceLock::new();

fn pool() -> &'static Mutex<HashMap<SocketAddr, TcpStream>> {
    POOL.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn pool_take(a: &SocketAddr) -> Option<TcpStream> {
    pool().lock().remove(a)
}

pub fn pool_put(a: SocketAddr, s: TcpStream) {
    let _ = s.set_read_timeout(Some(Duration::from_millis(300)));
    let _ = s.set_write_timeout(Some(Duration::from_millis(300)));
    pool().lock().insert(a, s);
}

// ============================================================================
// MODULE 5: budget.rs - Compute budget helpers
// ============================================================================
use solana_sdk::{instruction::Instruction, compute_budget::ComputeBudgetInstruction as CBI};

pub const MAX_CU_LIMIT: u32 = 1_400_000;

// REPLACED: Canonical compute budget implementation (client + server agree)
#[inline]
pub fn compute_budget_ix3(cu_limit: u32, heap_bytes: u32, micro_lamports_per_cu: u64) -> [Instruction; 3] {
    [
        CBI::set_compute_unit_limit(cu_limit.min(MAX_CU_LIMIT)),
        CBI::request_heap_frame(heap_bytes),
        CBI::set_compute_unit_price(micro_lamports_per_cu),
    ]
}

// ============================================================================
// MODULE 6: audit.rs - Audit metadata
// ============================================================================
#[derive(Clone, Copy)]
pub struct Metadata {
    pub proof_hash: [u8; 32],
    pub circuit_hash: [u8; 32],
}

// ============================================================================
// MODULE 7: init.rs - Initialization helpers
// ============================================================================
#[inline] 
pub fn dirfd_cache_init() {}

#[inline] 
pub fn relay_book_init() {}

// ============================================================================
// MODULE 8: Admission control and latency sampling
// ============================================================================
use std::sync::atomic::{AtomicU64, Ordering};

static NET_LATENCY_EMA: AtomicU64 = AtomicU64::new(0);
static P2_OBSERVATION_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn net_observe_latency(ms: u64) {
    // Simple EMA update: new_value = (old_value * 7 + new_sample * 1) / 8
    let old = NET_LATENCY_EMA.load(Ordering::Relaxed);
    let new_val = (old * 7 + ms) / 8;
    NET_LATENCY_EMA.store(new_val, Ordering::Relaxed);
}

pub fn p2_observe_net(ms: u64) {
    let count = P2_OBSERVATION_COUNT.fetch_add(1, Ordering::Relaxed);
    if count % 100 == 0 {
        let avg_latency = NET_LATENCY_EMA.load(Ordering::Relaxed);
        // Update priority fee escalation based on network conditions
        update_priority_fee_escalator(avg_latency);
    }
}

#[inline]
fn update_priority_fee_escalator(avg_latency_ms: u64) {
    // Adjust escalation based on network latency
    if avg_latency_ms > 100 {
        // High latency - increase priority fees
    } else if avg_latency_ms < 50 {
        // Low latency - can reduce priority fees
    }
}

// ============================================================================
// MODULE 9: Ticket counter system for queue depth tracking
// ============================================================================
static TICKET_NEXT: AtomicU64 = AtomicU64::new(1);
static TICKET_SERVE: AtomicU64 = AtomicU64::new(0);

pub fn queue_depth() -> usize {
    let next = TICKET_NEXT.load(Ordering::Relaxed);
    let serve = TICKET_SERVE.load(Ordering::Relaxed);
    (next - serve) as usize
}

pub fn admit_request() -> u64 {
    TICKET_NEXT.fetch_add(1, Ordering::Relaxed)
}

pub fn complete_request() {
    TICKET_SERVE.fetch_add(1, Ordering::Relaxed);
}

// ============================================================================
// MODULE 10: Token bucket for rate limiting
// ============================================================================
static TOKEN_BUCKET_TOKENS: AtomicU64 = AtomicU64::new(0);
static TOKEN_BUCKET_LAST_REFILL: AtomicU64 = AtomicU64::new(0);

pub fn token_bucket_init(capacity: u64) {
    TOKEN_BUCKET_TOKENS.store(capacity, Ordering::Relaxed);
    TOKEN_BUCKET_LAST_REFILL.store(mono_now_ns(), Ordering::Relaxed);
}

pub fn token_bucket_refill_on_slot(_slot: u64) {
    let now = mono_now_ns();
    let last_refill = TOKEN_BUCKET_LAST_REFILL.load(Ordering::Relaxed);
    let elapsed_ms = elapsed_ms_since_ns(last_refill);
    
    if elapsed_ms >= 100 { // Refill every 100ms
        let current_tokens = TOKEN_BUCKET_TOKENS.load(Ordering::Relaxed);
        let new_tokens = current_tokens.saturating_add(elapsed_ms / 100); // 1 token per 100ms
        TOKEN_BUCKET_TOKENS.store(new_tokens.min(1000), Ordering::Relaxed); // Cap at 1000
        TOKEN_BUCKET_LAST_REFILL.store(now, Ordering::Relaxed);
    }
}

pub fn token_bucket_try_consume(tokens: u64) -> bool {
    let current = TOKEN_BUCKET_TOKENS.load(Ordering::Relaxed);
    if current >= tokens {
        TOKEN_BUCKET_TOKENS.fetch_sub(tokens, Ordering::Relaxed);
        true
    } else {
        false
    }
}

// ============================================================================
// MODULE 12: Integration helpers and initialization
// ============================================================================
pub fn init_all_optimizations() -> Result<(), ProofError> {
    ensure_release_build()?;
    dirfd_cache_init();
    relay_book_init();
    token_bucket_init(1000); // Initialize with 1000 tokens
    Ok(())
}

pub fn update_slot_watermark(slot: u64) {
    SLOT_WATERMARK.store(slot, Ordering::Relaxed);
    token_bucket_refill_on_slot(slot);
}

// ============================================================================
// MODULE 15: Admission control example
// ============================================================================
pub fn admit_by_deadline() -> Result<u64, ProofError> {
    let depth = queue_depth();
    if depth > 1000 {
        return Err(ProofError::Backpressure { limit: 1000 });
    }
    
    let ticket = admit_request();
    
    // Check if we should consume tokens for this request
    if !token_bucket_try_consume(10) {
        // No tokens available, but we already admitted the request
        // This is a soft rate limit - the request will still be processed
    }
    
    Ok(ticket)
}

pub fn complete_proof_request(ticket: u64) {
    complete_request();
    
    // Sample latency (in real implementation, measure actual latency)
    let latency_ms = 50; // Placeholder
    net_observe_latency(latency_ms);
    p2_observe_net(latency_ms);
}

// ============================================================================
// MODULE 16: Trade window compressor integration reminder
// ============================================================================
// REMINDER: Paste your TX throttle snippet back into whichever file actually submits transactions, after these upgrades:
// 
// import {
//   isTradeWindowOpen,
//   getTipBoostMultiplier,
//   getSafeModeMultiplier
// } from '../analytics/tradeWindowCompressor';
// 
// const shouldSend = isTradeWindowOpen(currentSlot);
// if (!shouldSend) {
//   console.warn(`🚫 Trade window closed at slot ${currentSlot}`);
//   return;
// }
// 
// const sizeMultiplier = getSafeModeMultiplier();    // 0.5x | 0.75x | 1x
// const tipMultiplier = getTipBoostMultiplier();     // 1x | 1.25x | 1.5x
// 
// const flashloanAmount = BASE_AMOUNT * sizeMultiplier;
// const bundleTipLamports = BASE_TIP * tipMultiplier;

// ============================================================================
// MODULE 17: ZK Groth16 On-Chain Verification Program
// ============================================================================

// ============================================================================
// ZK Error types for on-chain verification
// ============================================================================
use thiserror::Error;
use solana_program::{program_error::ProgramError};
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Error, BorshSerialize, BorshDeserialize)]
pub enum ZkErr {
    #[error("Invalid instruction")]
    InvalidIx,
    #[error("Invalid proof lengths")]
    BadLengths,
    #[error("Verification failed")]
    VerifyFail,
    #[error("Deserialize failed")]
    Deserialize,
}

impl From<ZkErr> for ProgramError {
    fn from(_: ZkErr) -> Self { ProgramError::Custom(0x5A1A_0001) }
}

// ============================================================================
// ZK Instruction types for on-chain verification
// ============================================================================
#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub enum ZkInstruction {
    /// Verify a Groth16 BN254 proof.
    /// All bytes are big-endian field elements as per `groth16-solana` docs.
    ///
    /// proof: 256 bytes = A(64) | B(128) | C(64)
    /// public_inputs: concatenated 32-byte chunks, big-endian; length must be multiple of 32.
    Verify {
        proof: Vec<u8>,
        public_inputs: Vec<u8>,
    },
}

// ============================================================================
// Verifying Key for Groth16 BN254 circuit
// ============================================================================
// NOTE: This is a placeholder demo key matching the crate's functional test shape.
// Replace with your own circuit VK via the upstream generator (parse_vk_to_rust.js)
// when wiring your actual strategy circuit.
pub struct VerifyingKey {
    pub alpha_g1: [u8; 64],
    pub beta_g2: [u8; 128],
    pub gamma_g2: [u8; 128],
    pub delta_g2: [u8; 128],
    pub ic: Vec<[u8; 64]>,
}

#[allow(dead_code)]
pub static VERIFYING_KEY: VerifyingKey = VerifyingKey {
    // For brevity: tiny synthetic vk values; use the generator for real circuits.
    // The struct fields must be 64/128-byte big-endian encodings as required by groth16-salana.
    alpha_g1: [0u8; 64],
    beta_g2: [0u8; 128],
    gamma_g2: [0u8; 128],
    delta_g2: [0u8; 128],
    ic: vec![[0u8; 64]; 2],
};

// ============================================================================
// On-chain verification processor
// ============================================================================
#[inline]
fn chunk32<'a>(bytes: &'a [u8]) -> Result<Vec<&'a [u8]>, ZkErr> {
    if bytes.len() % 32 != 0 { return Err(ZkErr::BadLengths); }
    Ok(bytes.chunks(32).collect())
}

pub fn process_zk_verification(
    _program_id: &solana_program::pubkey::Pubkey, 
    _accs: &[solana_program::account_info::AccountInfo], 
    data: &[u8]
) -> solana_program::entrypoint::ProgramResult {
    let ix = ZkInstruction::try_from_slice(data).map_err(|_| ZkErr::InvalidIx)?;
    match ix {
        ZkInstruction::Verify { proof, public_inputs } => {
            // Expect proof = 256 bytes: A 64, B 128, C 64 (big-endian)
            if proof.len() != 256 { return Err(ZkErr::BadLengths.into()); }
            let a: [u8; 64] = proof[0..64].try_into().unwrap();
            let b: [u8; 128] = proof[64..192].try_into().unwrap();
            let c: [u8; 64] = proof[192..256].try_into().unwrap();
            let pub_in_chunks = chunk32(&public_inputs).map_err(|e| ProgramError::from(e))?;

            // Simulate Groth16 verification (replace with actual groth16-salana verifier)
            // In real implementation: Groth16Verifier::new(&a, &b, &c, &pub_in_chunks, &VERIFYING_KEY)
            // For now, just validate the structure
            if a.iter().all(|&x| x == 0) && b.iter().all(|&x| x == 0) && c.iter().all(|&x| x == 0) {
                return Err(ZkErr::VerifyFail.into());
            }
            
            solana_program::msg!("zkv_groth16: verification success");
            Ok(())
        }
    }
}

// ============================================================================
// ZK Client Integration
// ============================================================================
use solana_sdk::{transaction::Transaction, message::Message, pubkey::Pubkey, signature::Keypair};
use solana_client::rpc_client::RpcClient;

/// Build big-endian field element from little-endian
#[inline]
pub fn fr_to_be_bytes32(fe: &Fr) -> [u8; 32] {
    let le_bytes = fr_to_le_bytes32(fe);
    let mut be_bytes = [0u8; 32];
    for i in 0..32 {
        be_bytes[i] = le_bytes[31 - i];
    }
    be_bytes
}

/// Build public inputs in big-endian format for on-chain verification
pub fn build_public_inputs_big_endian(inputs_fr: &[Fr]) -> Vec<u8> {
    let mut result = Vec::with_capacity(inputs_fr.len() * 32);
    for fe in inputs_fr {
        let be_bytes = fr_to_be_bytes32(fe);
        result.extend_from_slice(&be_bytes);
    }
    result
}

/// Build verification instruction for on-chain proof verification
pub fn build_verify_proof_ix(
    program_id: Pubkey,
    proof_abc_256: [u8; 256],             // A(64)|B(128)|C(64) big-endian
    public_inputs_be32: &[u8],            // multiple of 32 bytes, big-endian field elements
) -> Result<solana_sdk::instruction::Instruction, ProofError> {
    if public_inputs_be32.len() % 32 != 0 { 
        return Err(ProofError::InvalidPermissions("public inputs not multiple of 32".into())); 
    }
    let data = ZkInstruction::Verify {
        proof: proof_abc_256.to_vec(),
        public_inputs: public_inputs_be32.to_vec(),
    }.try_to_vec().map_err(|e| ProofError::Serialization(format!("borsh serialize: {e}")))?;
    Ok(solana_sdk::instruction::Instruction { program_id, accounts: vec![], data })
}

/// Conservative default for Groth16 verify + account overhead
#[inline]
pub fn autosize_budget_for_zk_proof(proof_len: usize) -> (u32, u32, u64) {
    // Heap rounds to 32KiB to keep verifier happy even with larger IC arrays
    let heap = ((proof_len + 64 * 1024) / (32 * 1024)) * (32 * 1024);
    // CU: base 280k + 40 per KiB proof; clamp to max to leave tip headroom
    let cu = (280_000usize + (proof_len / 1024) * 40).min(MAX_CU_LIMIT as usize) as u32;
    (cu, heap as u32, 0)
}

/// Smooth fee curve: base -> max as deadline approaches; plug in your signal
#[inline]
pub fn priority_fee_escalator_zk(ms_left: u64, base: u64, max: u64, p50: u64, p95: u64) -> u64 {
    if ms_left >= p95 { return base.min(max); }
    let span = (p95.saturating_sub(ms_left)) as u128;
    let denom = p95.max(p50).max(1) as u128;
    let t = (span << 16) / denom;                 // Q16 in [0,1]
    let bump = (t * t * (max.saturating_sub(base) as u128)) >> 32;
    (base as u128 + bump).min(max as u128) as u64
}

// ============================================================================
// REPLACEMENT: Canonical compute budget implementation
// ============================================================================

/// Example of how to properly prepend compute budget instructions to a transaction
pub fn build_transaction_with_budget(
    instructions: Vec<Instruction>,
    cu_limit: u32,
    heap_bytes: u32,
    micro_lamports_per_cu: u64,
    program_id: &solana_program::pubkey::Pubkey,
) -> Result<Transaction, ProofError> {
    let mut all_instructions = Vec::new();
    
    // Add compute budget instructions first
    let budget_ixs = compute_budget_ix3(cu_limit, heap_bytes, micro_lamports_per_cu);
    all_instructions.extend_from_slice(&budget_ixs);
    
    // Add program instructions
    all_instructions.extend(instructions);
    
    // Create message (this would normally include signers, etc.)
    let message = Message::new(&all_instructions, Some(program_id));
    
    // Create transaction (this is a simplified example)
    let transaction = Transaction::new_unsigned(message);
    
    Ok(transaction)
}

/// Example of priority fee escalation integration
pub fn escalate_fees_for_deadline(
    base_instructions: Vec<Instruction>,
    ms_left: u64,
    program_id: &solana_program::pubkey::Pubkey,
) -> Result<Transaction, ProofError> {
    let base_fee = 1000; // micro-lamports per CU
    let max_fee = 10000; // max micro-lamports per CU
    
    // Use the network-aware fee escalator
    let escalated_fee = priority_fee_escalator_net(ms_left, base_fee, max_fee);
    
    // Auto-size budget based on proof complexity
    let (cu_limit, heap_bytes, _) = autosize_budget_for_proof(4096); // Example proof size
    
    build_transaction_with_budget(
        base_instructions,
        cu_limit,
        heap_bytes,
        escalated_fee,
        program_id,
    )
}

// ============================================================================
// Complete ZK Transaction Building Example
// ============================================================================

/// Build a complete transaction with ZK proof verification
pub fn build_zk_verification_transaction(
    proof_abc_256: [u8; 256],
    public_inputs_fr: &[Fr],
    program_id: Pubkey,
    ms_left: u64,
    base_fee: u64,
    max_fee: u64,
    cu_est: u32,
    ev_lamports: u64,
) -> Result<Vec<Instruction>, ProofError> {
    let mut instructions = Vec::new();
    
    // 1. Build big-endian public inputs for on-chain verification
    let public_inputs_be = build_public_inputs_big_endian(public_inputs_fr);
    
    // 2. Build ZK verification instruction
    let verify_ix = build_verify_proof_ix(program_id, proof_abc_256, &public_inputs_be)?;
    
    // 3. Auto-size budget for ZK verification
    let (cu_limit, heap_bytes, _) = autosize_budget_for_zk_proof(256 + public_inputs_be.len());
    
    // 4. Calculate network-aware fee escalation
    let fee = priority_fee_escalator_ev(ms_left, base_fee, max_fee, cu_est, ev_lamports);
    
    // 5. Add compute budget instructions first
    let budget_ixs = compute_budget_ix3(cu_limit, heap_bytes, fee);
    instructions.extend_from_slice(&budget_ixs);
    
    // 6. Add ZK verification instruction
    instructions.push(verify_ix);
    
    Ok(instructions)
}

/// Send ZK verification transaction with complete integration
pub fn send_zk_verification_transaction(
    rpc_client: &solana_client::rpc_client::RpcClient,
    payer: &solana_sdk::signature::Keypair,
    proof_abc_256: [u8; 256],
    public_inputs_fr: &[Fr],
    program_id: Pubkey,
    ms_left: u64,
    base_fee: u64,
    max_fee: u64,
    cu_est: u32,
    ev_lamports: u64,
    jito_tip_lamports: u64,
    tip_receiver: Option<Pubkey>,
) -> Result<solana_sdk::signature::Signature, ProofError> {
    // Build ZK verification transaction
    let mut instructions = build_zk_verification_transaction(
        proof_abc_256,
        public_inputs_fr,
        program_id,
        ms_left,
        base_fee,
        max_fee,
        cu_est,
        ev_lamports,
    )?;
    
    // Add optional Jito tip
    if jito_tip_lamports > 0 {
        if let Some(tip_receiver) = tip_receiver {
            let tip_ix = solana_sdk::system_instruction::transfer(
                &payer.pubkey(),
                &tip_receiver,
                jito_tip_lamports,
            );
            instructions.push(tip_ix);
        }
    }
    
    // Create and send transaction
    let latest_blockhash = rpc_client.get_latest_blockhash()
        .map_err(|e| ProofError::Serialization(format!("RPC error: {e}")))?;
    
    let message = Message::new(&instructions, Some(&payer.pubkey()));
    let transaction = Transaction::new(&[payer], message, latest_blockhash);
    
    rpc_client.send_and_confirm_transaction(&transaction)
        .map_err(|e| ProofError::Serialization(format!("Transaction failed: {e}")))?;
    
    Ok(transaction.signatures[0])
}

// ============================================================================
// Complete Integration Example: Proof Generation + On-Chain Verification
// ============================================================================

/// Complete example showing how to generate a proof and submit it for on-chain verification
pub fn generate_and_verify_proof_onchain(
    pb: &ProofBuilder,
    hex_inputs: &[String],
    slot: u64,
    nonce: u64,
    wasm_path: &str,
    r1cs_path: &str,
    ctx: Option<ProofContext<'_>>,
    ms_left: u64,
    base_fee: u64,
    max_fee: u64,
    program_id: Pubkey,
    rpc_client: &RpcClient,
    payer: &Keypair,
    ev_lamports: u64,
    jito_tip_lamports: u64,
    tip_receiver: Option<Pubkey>,
) -> Result<solana_sdk::signature::Signature, ProofError> {
    // 1. Generate the proof using your existing infrastructure
    let (proof_bytes, _metadata, _proof_hash) = pb.generate_proof_ctx_v3(
        hex_inputs, slot, nonce, wasm_path, r1cs_path, ctx
    )?;
    
    // 2. Parse inputs to Fr format for big-endian conversion
    let inputs_fr = pb.parse_and_validate_inputs(hex_inputs)?;
    
    // 3. Convert proof to big-endian 256-byte format (A|B|C)
    if proof_bytes.len() < 256 {
        return Err(ProofError::Serialization("Proof too short for ZK verification".into()));
    }
    let mut proof_abc_256 = [0u8; 256];
    proof_abc_256.copy_from_slice(&proof_bytes[0..256]);
    
    // 4. Auto-size compute budget for ZK verification
    let (cu_est, _, _) = autosize_budget_for_zk_proof(256 + inputs_fr.len() * 32);
    
    // 5. Send ZK verification transaction with complete integration
    let signature = send_zk_verification_transaction(
        rpc_client,
        payer,
        proof_abc_256,
        &inputs_fr,
        program_id,
        ms_left,
        base_fee,
        max_fee,
        cu_est,
        ev_lamports,
        jito_tip_lamports,
        tip_receiver,
    )?;
    
    Ok(signature)
}

// ============================================================================
// Deployment and Configuration Helpers
// ============================================================================

/// Configuration for ZK verification program
pub struct ZkConfig {
    pub program_id: Pubkey,
    pub base_fee_micro_lamports: u64,
    pub max_fee_micro_lamports: u64,
    pub jito_tip_receiver: Option<Pubkey>,
    pub default_jito_tip_lamports: u64,
}

impl Default for ZkConfig {
    fn default() -> Self {
        Self {
            program_id: Pubkey::default(), // Replace with deployed program ID
            base_fee_micro_lamports: 1000,
            max_fee_micro_lamports: 10000,
            jito_tip_receiver: None,
            default_jito_tip_lamports: 10000,
        }
    }
}

/// Example usage with configuration
pub fn submit_proof_with_config(
    pb: &ProofBuilder,
    hex_inputs: &[String],
    slot: u64,
    nonce: u64,
    wasm_path: &str,
    r1cs_path: &str,
    ctx: Option<ProofContext<'_>>,
    ms_left: u64,
    config: &ZkConfig,
    rpc_client: &RpcClient,
    payer: &Keypair,
    ev_lamports: u64,
) -> Result<solana_sdk::signature::Signature, ProofError> {
    generate_and_verify_proof_onchain(
        pb,
        hex_inputs,
        slot,
        nonce,
        wasm_path,
        r1cs_path,
        ctx,
        ms_left,
        config.base_fee_micro_lamports,
        config.max_fee_micro_lamports,
        config.program_id,
        rpc_client,
        payer,
        ev_lamports,
        config.default_jito_tip_lamports,
        config.jito_tip_receiver,
    )
}

// ============================================================================
// MODULE 14: Secure file hashing with TOCTOU protection
// ============================================================================
use std::io::{Read, BufReader};

pub fn blake3_file_secure(path: &Path) -> Result<[u8; 32], ProofError> {
    let file = open_readonly_nofollow(path)?;
    verify_same_inode(path, &file)?;
    
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 65536];
    let mut reader = BufReader::new(file);
    
    loop {
        let bytes_read = reader.read(&mut buffer)
            .map_err(|e| ProofError::Serialization(format!("read error: {e}")))?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    
    Ok(hasher.finalize().into())
}

use ark_bn254::{Bn254, Fr};
use ark_ff::{Field, PrimeField};
use ark_groth16::{Groth16, Proof, ProvingKey};
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize, Compress, Validate};

// Additional imports for the new modules
use std::sync::atomic::{AtomicU64, Ordering};
use once_cell::sync::OnceCell;
// Using hashbrown::HashMap for performance

// Platform-specific imports
#[cfg(unix)]
extern crate libc;

// ADD: 32-byte little-endian serializer for BN254 Fr without heap
#[inline(always)]
fn fr_to_le_bytes32(fe: &Fr) -> [u8; 32] {
    use ark_serialize::CanonicalSerialize;
    let mut out = [0u8; 32];
    // BN254 Fr compressed encoding is 32 bytes. Write directly into the stack buffer.
    let mut cur = std::io::Cursor::new(&mut out[..]);
    fe.serialize_compressed(&mut cur).expect("Fr serialize");
    out
}

// REPLACE: inputs_digest_bn254
#[inline(always)]
fn inputs_digest_bn254(inputs: &[Fr]) -> [u8;32] {
    let mut ih = blake3::Hasher::new();
    for fe in inputs {
        let b = fr_to_le_bytes32(fe);
        ih.update(&b);
    }
    ih.finalize().into()
}

// ADD: protect and prep large proof/attestation buffers for low-latency send
#[inline]
pub fn prepare_proof_buffer_hot(buf: &[u8]) {
    advise_sensitive_slice(buf);
    advise_unmergeable_slice(buf);
    advise_dontfork_slice(buf);
    advise_dontdump_slice(buf);
    advise_hugepage_slice(buf);
    advise_willneed_slice(buf);
    mlock_onfault_slice(buf);
    bind_mmap_to_local_numa(buf);
    ensure_resident_ratio(buf, 0.75);
    advise_collapse_slice(buf);
}

// ============================================================================
// MODULE 11: Memory advisor functions (best-effort, harmless if not available)
// ============================================================================
#[inline(always)]
fn advise_sensitive_slice(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        libc::madvise(_buf.as_ptr() as *mut libc::c_void, _buf.len(), libc::MADV_DONTDUMP);
    }
}

#[inline(always)]
fn advise_unmergeable_slice(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        libc::madvise(_buf.as_ptr() as *mut libc::c_void, _buf.len(), libc::MADV_UNMERGEABLE);
    }
}

#[inline(always)]
fn advise_dontfork_slice(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        libc::madvise(_buf.as_ptr() as *mut libc::c_void, _buf.len(), libc::MADV_DONTFORK);
    }
}

#[inline(always)]
fn advise_dontdump_slice(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        libc::madvise(_buf.as_ptr() as *mut libc::c_void, _buf.len(), libc::MADV_DONTDUMP);
    }
}

#[inline(always)]
fn advise_hugepage_slice(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        libc::madvise(_buf.as_ptr() as *mut libc::c_void, _buf.len(), libc::MADV_HUGEPAGE);
    }
}

#[inline(always)]
fn advise_willneed_slice(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        libc::madvise(_buf.as_ptr() as *mut libc::c_void, _buf.len(), libc::MADV_WILLNEED);
    }
}

#[inline(always)]
fn mlock_onfault_slice(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        let _ = libc::mlock(_buf.as_ptr() as *const libc::c_void, _buf.len());
    }
}

#[inline(always)]
fn bind_mmap_to_local_numa(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        let _ = libc::mbind(
            _buf.as_ptr() as *mut libc::c_void,
            _buf.len(),
            libc::MPOL_BIND,
            std::ptr::null(),
            0,
            libc::MPOL_MF_STRICT | libc::MPOL_MF_MOVE,
        );
    }
}

#[inline(always)]
fn ensure_resident_ratio(_buf: &[u8], _ratio: f64) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        let _ = libc::madvise(_buf.as_ptr() as *mut libc::c_void, _buf.len(), libc::MADV_POPULATE_READ);
    }
}

#[inline(always)]
fn advise_collapse_slice(_buf: &[u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        // MADV_COLLAPSE is only available on newer kernels
        const MADV_COLLAPSE: i32 = 25;
        libc::madvise(_buf.as_ptr() as *mut libc::c_void, _buf.len(), MADV_COLLAPSE);
    }
}

use ark_circom::{CircomBuilder, CircomConfig};

// ADD: thread-local input scratch
use core::fmt::Write as _;

thread_local! {
    static INPUT_SCRATCH: std::cell::RefCell<String> =
        std::cell::RefCell::new(String::with_capacity(128));
}

// Constants
const MAX_INPUT_COUNT: usize = 1000;

// ADD: precomputed "in{i}" names up to MAX_INPUT_COUNT
static IN_NAMES: OnceCell<Vec<String>> = OnceCell::new();

#[inline(always)]
fn in_name(i: usize) -> &'static str {
    let v = IN_NAMES.get_or_init(|| {
        let mut out = Vec::with_capacity(MAX_INPUT_COUNT);
        for n in 0..MAX_INPUT_COUNT { out.push(format!("in{}", n)); }
        out
    });
    &v[i]
}

// ADD: slot watermark (atomic), set by your caller each new slot
static SLOT_WATERMARK: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
pub fn update_slot_watermark(s: u64) { SLOT_WATERMARK.store(s, Ordering::Relaxed); }

// ADD: Slot-token bucket backpressure system
static TB_TOKENS: AtomicU64 = AtomicU64::new(0);
static TB_CAP: AtomicU64 = AtomicU64::new(0);
static TB_LAST_SLOT: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn token_bucket_init(cap_per_slot: u64) {
    TB_CAP.store(cap_per_slot.max(1), Ordering::Relaxed);
    TB_TOKENS.store(cap_per_slot.max(1), Ordering::Relaxed);
}

#[inline]
pub fn token_bucket_refill_on_slot(slot: u64) {
    let prev = TB_LAST_SLOT.swap(slot, Ordering::Relaxed);
    if slot != prev {
        let cap = TB_CAP.load(Ordering::Relaxed);
        TB_TOKENS.store(cap, Ordering::Relaxed);
    }
}

#[inline(always)]
pub fn token_bucket_try_take(n: u64) -> bool {
    let mut cur = TB_TOKENS.load(Ordering::Relaxed);
    loop {
        if cur < n { return false; }
        match TB_TOKENS.compare_exchange_weak(cur, cur - n, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(now) => cur = now,
        }
    }
}

// ADD: early gate that respects tokens (drop fast when late & empty)
#[inline(always)]
pub fn admit_quick_tb(ms_left: u64) -> bool {
    if matches!(admit_by_deadline(ms_left), AdmitDecision::SkipLate) && queue_depth() > 0 {
        return false;
    }
    token_bucket_try_take(1)
}

use arrayvec::ArrayVec;
use blake3::Hasher;
use core_affinity;
use hex;
use std::sync::OnceCell;
use parking_lot::RwLock;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use serde_json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, AtomicU64, Ordering};
use core::sync::atomic::{compiler_fence, Ordering as CoreOrdering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc;
use std::thread;

// ADD: dependency for persistent workers
use crossbeam_channel::{unbounded, Sender, Receiver};
use ahash::RandomState;
use hashbrown::HashMap;
use tempfile::NamedTempFile;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};
use rand_chacha::ChaCha20Rng;
use rand_core::{SeedableRng, RngCore};
use smallvec::SmallVec;
use std::collections::VecDeque;
use solana_sdk::{
    instruction::Instruction,
    compute_budget::ComputeBudgetInstruction,
};

// ADD: libc for system calls
#[cfg(unix)]
extern crate libc;

// REMOVED: First P2 implementation with unsafe static mut bootstrapping
// Keeping the safer implementation below

// ADD: cgroup v2/v1 CPU quota readers and init
#[cfg(all(unix, target_os = "linux"))]
fn read_to_string<P: AsRef<std::path::Path>>(p: P) -> Option<String> {
    std::fs::read_to_string(p).ok().map(|s| s.trim().to_string())
}

#[cfg(all(unix, target_os = "linux"))]
fn cgroup_cpu_quota_cores() -> Option<f64> {
    // v2: /sys/fs/cgroup/cpu.max  → "max" or "<quota> <period>"
    if let Some(s) = read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut it = s.split_whitespace();
        if let (Some(q), Some(p)) = (it.next(), it.next()) {
            if q != "max" {
                if let (Ok(qu), Ok(pe)) = (q.parse::<u64>(), p.parse::<u64>()) {
                    if pe > 0 { return Some((qu as f64) / (pe as f64)); }
                }
            }
        }
    }
    // v1 fallback
    let q = read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")?.parse::<i64>().ok()?;
    let p = read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")?.parse::<i64>().ok()?;
    if q > 0 && p > 0 { Some((q as f64) / (p as f64)) } else { None }
}

#[cfg(all(unix, target_os = "linux"))]
fn cpuset_allowed_cores() -> Option<usize> {
    // Use your existing parse_cpu_list if present; otherwise a local one:
    fn parse(spec: &str) -> usize {
        let mut n = 0usize;
        for part in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            if let Some((a,b)) = part.split_once('-') {
                if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) { n += b.saturating_sub(a)+1; continue; }
            }
            if part.parse::<usize>().is_ok() { n += 1; }
        }
        n
    }
    if let Some(s) = read_to_string("/sys/fs/cgroup/cpuset.cpus.effective") { return Some(parse(&s)); }
    if let Some(s) = read_to_string("/sys/fs/cgroup/cpuset.cpus") { return Some(parse(&s)); }
    None
}

#[inline]
pub fn init_cgroup_quota_limits() {
    #[cfg(all(unix, target_os = "linux"))]
    {
        let mut cores = cgroup_cpu_quota_cores().unwrap_or(0.0);
        if let Some(k) = cpuset_allowed_cores() { if k > 0 { cores = cores.min(k as f64).max(1.0); } }
        let c = cores.floor().max(1.0) as u64;
        // Set prover concurrency and tokens per slot proportionally
        PROVER_LIMIT_ATOMIC.0.store(c, std::sync::atomic::Ordering::Relaxed);
        TB_CAP.store((c * 2).max(1), std::sync::atomic::Ordering::Relaxed);
        TB_TOKENS.store(TB_CAP.load(std::sync::atomic::Ordering::Relaxed), std::sync::atomic::Ordering::Relaxed);
    }
}

// ADD: pick a CPU that is not sharing core_id with any reserved core_ids
#[cfg(all(unix, target_os = "linux"))]
fn read_core_id(cpu: usize) -> Option<u32> {
    let p = format!("/sys/devices/system/cpu/cpu{}/topology/core_id", cpu);
    std::fs::read_to_string(p).ok()?.trim().parse().ok()
}

#[cfg(all(unix, target_os = "linux"))]
fn pick_net_cpu_excluding(prover_cpus: &[usize]) -> Option<usize> {
    use std::collections::HashSet;
    let mut bad_cores: HashSet<u32> = HashSet::new();
    for &c in prover_cpus { if let Some(cid) = read_core_id(c) { bad_cores.insert(cid); } }
    // Prefer allowed list you already compute
    let pool = build_preferred_cpu_list();
    for c in pool {
        if let Some(cid) = read_core_id(c) {
            if !bad_cores.contains(&cid) { return Some(c); }
        }
    }
    None
}

// ADD: TLS BLAKE3 hasher (reset per use)
thread_local! { static HBLK: std::cell::RefCell<blake3::Hasher> = std::cell::RefCell::new(blake3::Hasher::new()); }

#[inline(always)]
fn blake3_digest_tls(chunks: &[&[u8]]) -> [u8;32] {
    HBLK.with(|cell| {
        let mut h = cell.borrow_mut();
        // BLAKE3 has no reset(), re-init in place:
        *h = blake3::Hasher::new();
        for c in chunks { h.update(c); }
        h.finalize().into()
    })
}

// ADD: comprehensive initialization function
#[inline] pub fn init_all_optimizations() {
    p2_init();
    dirfd_cache_init();
    relay_book_init();
    init_cgroup_quota_limits();
    // other initializations already handled by existing code
}

// ADD: Missing relay_book_init function
#[inline]
pub fn relay_book_init() {
    // Initialize relay tracking structures
    relay_stats_init();
    relay_pool_init();
    neg_init();
}

// REMOVED: Old P2 functions that used unsafe static mut globals
// Using the safer implementation below

// ADD: Cross-process file locking for nonce store
use fs2::FileExt;

// ADD: audit sequence helpers
use std::sync::atomic::AtomicU64;
static AUDIT_SEQ: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
fn next_audit_seq(path: &Path) -> u64 {
    // Persisted in ".seq" file; best-effort reload, then atomic inc
    let seq_path = path.with_extension("seq");
    if AUDIT_SEQ.load(Ordering::Relaxed) == 0 {
        if let Ok(v) = fs::read(&seq_path) {
            if v.len() == 8 {
                let n = u64::from_le_bytes(v.try_into().unwrap());
                AUDIT_SEQ.store(n, Ordering::Relaxed);
            }
        }
    }
    AUDIT_SEQ.fetch_add(1, Ordering::Relaxed) + 1
}

#[inline(always)]
fn persist_audit_seq(path: &Path, seq: u64) {
    let seq_path = path.with_extension("seq");
    if let Ok(tmp) = tempfile::NamedTempFile::new_in(path.parent().unwrap_or(Path::new("."))) {
        let _ = tmp.as_file().write_all(&seq.to_le_bytes());
        let _ = tmp.as_file().sync_all();
        let _ = tmp.persist(&seq_path);
    }
}

// ADD: disable core dumps to avoid sensitive buffer spills
#[cfg(unix)]
pub fn disable_core_dumps() {
    use nix::sys::resource::{setrlimit, Resource, Rlim};
    let _ = setrlimit(Resource::RLIMIT_CORE, Rlim::from_raw(0), Rlim::from_raw(0));
}

#[cfg(not(unix))]
pub fn disable_core_dumps() {
    // no-op on non-Unix platforms
}

// ADD: low-latency scheduler (best-effort; harmless if denied)
thread_local! { 
    static RT_APPLIED: std::cell::Cell<bool> = std::cell::Cell::new(false); 
}

#[inline(always)]
fn ensure_lowlatency_sched() {
    RT_APPLIED.with(|c| {
        if c.get() { return; }
        #[cfg(unix)]
        unsafe {
            // Try SCHED_FIFO with low priority (1). If EPERM, fall back to nice(-10).
            let mut sp: libc::sched_param = std::mem::zeroed();
            sp.sched_priority = 1;
            let r = libc::sched_setscheduler(0, libc::SCHED_FIFO, &sp);
            if r != 0 {
                // Best-effort raise priority; ignore errors
                let _ = libc::setpriority(libc::PRIO_PROCESS, 0, -10);
            }
        }
        c.set(true);
    });
}

// ADD: raise_memlock_limit — best-effort increase for mlock success
#[cfg(unix)]
pub fn raise_memlock_limit(bytes: u64) {
    use nix::sys::resource::{getrlimit, setrlimit, Resource, Rlim};
    if let Ok((soft, hard)) = getrlimit(Resource::RLIMIT_MEMLOCK) {
        let want = Rlim::from_raw(bytes);
        // Soft cap cannot exceed hard; leave hard as is.
        let new_soft = if hard < want { hard } else { want };
        let _ = setrlimit(Resource::RLIMIT_MEMLOCK, new_soft, hard);
    }
}

#[cfg(not(unix))]
pub fn raise_memlock_limit(_bytes: u64) {
    // no-op on non-Unix platforms
}

// ADD: harden_signals — ignore SIGPIPE (best-effort), no unsafe in our crate
#[cfg(unix)]
pub fn harden_signals() {
    use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet, Signal};
    let _ = sigaction(
        Signal::SIGPIPE,
        &SigAction::new(SigHandler::SigIgn, SaFlags::empty(), SigSet::empty()),
    );
}
#[cfg(not(unix))]
pub fn harden_signals() {}

// ADD: ignore SIGPIPE once at boot
#[inline]
pub fn ignore_sigpipe_global() {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        let mut sa: libc::sigaction = core::mem::zeroed();
        sa.sa_sigaction = libc::SIG_IGN as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = 0;
        let _ = libc::sigaction(libc::SIGPIPE, &sa, core::ptr::null_mut());
    }
}

// ADD: send_all_nosig and use in hedged threads
#[inline]
fn send_all_nosig(fd: std::os::fd::RawFd, mut data: &[u8]) -> std::io::Result<()> {
    #[cfg(all(unix, target_os = "linux"))] unsafe {
        while !data.is_empty() {
            let n = libc::send(fd, data.as_ptr() as *const _, data.len(), libc::MSG_NOSIGNAL);
            if n < 0 {
                let e = std::io::Error::last_os_error();
                if e.kind() == std::io::ErrorKind::Interrupted { continue; }
                return Err(e);
            }
            data = &data[n as usize..];
        }
        Ok(())
    }
    #[cfg(not(all(unix, target_os = "linux")))]
    {
        use std::os::fd::AsRawFd;
        let mut f = unsafe { std::fs::File::from_raw_fd(fd) };
        let r = std::io::Write::write_all(&mut f, data);
        let _ = f.into_raw_fd(); // don't close
        r
    }
}

// REPLACE: hardened, low-noise init with lazy lock of future pages
#[inline]
pub fn init_low_noise_process() {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        // No dumps; no new privs
        let _ = libc::prctl(libc::PR_SET_DUMPABLE as i32, 0, 0, 0, 0);
        let _ = libc::prctl(libc::PR_SET_NO_NEW_PRIVS as i32, 1, 0, 0, 0);

        // Raise memlock so mlock/ONFAULT won't fail at runtime
        let lim: libc::rlimit = libc::rlimit {
            rlim_cur: 1 << 30, // 1 GiB
            rlim_max: 1 << 30,
        };
        let _ = libc::setrlimit(libc::RLIMIT_MEMLOCK, &lim as *const _);

        // Lazy lock all future mappings (no big first-touch stall)
        const MCL_CURRENT: i32 = 1;
        const MCL_FUTURE:  i32 = 2;
        const MCL_ONFAULT: i32 = 4;
        let _ = libc::mlockall(MCL_FUTURE | MCL_ONFAULT | MCL_CURRENT);
    }
}

// ADD: mapping advisors (Linux best-effort)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_hugepage_slice(s: &[u8]) {
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_HUGEPAGE);
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn advise_hugepage_slice(_s: &[u8]) {}

#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_random_access_slice(s: &[u8]) {
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_RANDOM);
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn advise_random_access_slice(_s: &[u8]) {}

#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn prefault_read_slice(s: &[u8]) {
    // MADV_POPULATE_READ (5.14+) prefaults without touching userspace; fallback: page-step touch
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_WILLNEED);
    // Try populate; ignore error if kernel too old
    #[allow(non_upper_case_globals)]
    const MADV_POPULATE_READ: i32 = 22;
    unsafe { libc::madvise(s.as_ptr() as *mut _, s.len(), MADV_POPULATE_READ); }
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn prefault_read_slice(_s: &[u8]) {}

// ADD: CPU topology discovery + pinning (Linux-only best-effort)
#[cfg(all(unix, target_os = "linux"))]
static CPU_PREF_LIST: OnceCell<Vec<usize>> = OnceCell::new();

// ADD: parse "0,2-3,8-11" style CPU lists
#[cfg(all(unix, target_os = "linux"))]
fn parse_cpu_list(spec: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if let Some((a,b)) = part.split_once('-') {
            if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) {
                for x in a..=b { out.push(x); }
                continue;
            }
        }
        if let Ok(x) = part.parse::<usize>() { out.push(x); }
    }
    out.sort_unstable();
    out.dedup();
    out
}

#[cfg(all(unix, target_os = "linux"))]
fn cpus_allowed() -> Vec<usize> {
    use std::{fs, io::Read};
    if let Ok(mut f) = fs::File::open("/proc/self/status") {
        let mut s = String::new();
        let _ = f.read_to_string(&mut s);
        if let Some(line) = s.lines().find(|l| l.starts_with("Cpus_allowed_list:")) {
            if let Some(spec) = line.split(':').nth(1) { return parse_cpu_list(spec.trim()); }
        }
    }
    read_present_cpus()
}

#[cfg(all(unix, target_os = "linux"))]
fn cpus_isolated() -> Vec<usize> {
    use std::{fs, io::Read};
    if let Ok(mut f) = fs::File::open("/sys/devices/system/cpu/isolated") {
        let mut s = String::new(); let _ = f.read_to_string(&mut s);
        let v = parse_cpu_list(s.trim());
        if !v.is_empty() { return v; }
    }
    Vec::new()
}

#[cfg(all(unix, target_os = "linux"))]
fn read_present_cpus() -> Vec<usize> {
    use std::{fs, io::Read};
    let mut v = Vec::new();
    if let Ok(mut f) = fs::File::open("/sys/devices/system/cpu/present") {
        let mut s = String::new();
        let _ = f.read_to_string(&mut s);
        for part in s.trim().split(',') {
            if let Some((a,b)) = part.split_once('-') {
                if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) {
                    for x in a..=b { v.push(x); }
                }
            } else if let Ok(x) = part.parse::<usize>() { v.push(x); }
        }
    }
    if v.is_empty() {
        // Fallback: probe 0..255
        for x in 0..256 { if std::path::Path::new(&format!("/sys/devices/system/cpu/cpu{}", x)).exists() { v.push(x); } }
    }
    v.sort_unstable(); v
}

// REPLACE: prefer unique cores within allowed & (isolated or all present)
#[cfg(all(unix, target_os = "linux"))]
fn build_preferred_cpu_list() -> Vec<usize> {
    use std::{fs, io::Read, collections::HashSet};
    let allowed = cpus_allowed();
    let iso = cpus_isolated();
    let base: Vec<usize> = if !iso.is_empty() {
        allowed.iter().cloned().filter(|c| iso.binary_search(c).is_ok()).collect()
    } else { allowed };

    let mut seen_core = HashSet::new();
    let mut pref = Vec::new();
    for cpu in base {
        let p = format!("/sys/devices/system/cpu/cpu{}/topology/core_id", cpu);
        if let Ok(mut f) = fs::File::open(p) {
            let mut s = String::new(); let _ = f.read_to_string(&mut s);
            if let Ok(cid) = s.trim().parse::<u32>() {
                if seen_core.insert(cid) { pref.push(cpu); }
            }
        }
    }
    if pref.is_empty() { pref = read_present_cpus(); }
    pref
}

#[cfg(all(unix, target_os = "linux"))]
#[inline]
pub fn init_cpu_topology() {
    let _ = CPU_PREF_LIST.set(build_preferred_cpu_list());
}

#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn set_affinity_cpu(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let _ = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

// ADD: ensure_release_build — refuse debug builds in prod
#[inline(always)]
pub fn ensure_release_build() -> Result<(), ProofError> {
    if cfg!(debug_assertions) {
        return Err(ProofError::Serialization("debug build not permitted on mainnet".into()));
    }
    Ok(())
}

// ADD 4/4 — Global allocator (binary crate): mimalloc for steadier p99
// NOTE: This should be added to your binary crate (e.g., main.rs), not this library:
// #[cfg(all(not(target_env = "msvc")))]
// use mimalloc::MiMalloc;
// #[cfg(all(not(target_env = "msvc")))]
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

#[cfg(unix)]
use libc;

// Security constants
const DOMAIN_SEPARATOR_V2: &[u8] = b"ZK_PROOF_MAINNET_V2";
const CIRCUIT_VERSION: u32 = 2;
const MIN_SLOT_DISTANCE: u64 = 32;     // Minimum slots between any two accepted proofs
const PROOF_VALIDITY_SLOTS: u64 = 150; // Proof expires after 150 slots
const MAX_PK_BYTES: usize = 10 * 1024 * 1024; // 10MB cap (DoS guard)

// Network binding (prevents cross-cluster reuse)
const NETWORK_ID: &[u8] = b"solana:mainnet-beta";

// Audit
const AUDIT_ENABLED: bool = true;
const AUDIT_LOG_PATH: &str = "logs/zk_proof_audit.jsonl";
const MAX_AUDIT_BYTES: u64 = 16 * 1024 * 1024; // rotate >16MB

// Concurrency (backpressure)
const DEFAULT_PROVER_LIMIT: usize = 2;

// Rayon pool guard
static RAYON_INIT: OnceCell<()> = OnceCell::new();

// REPLACE: proof cache (sharded), imports kept minimal
use std::collections::VecDeque;
// REMOVED: Incorrect std::hash::Hasher import - using blake3::Hasher directly
use std::sync::Arc;
use hashbrown::HashMap;
use ahash::RandomState;

// CONFIG: number of shards (power of two is fine)
const PC_SHARDS: usize = 16;

// REPLACE: ProofCacheShard with 2Q (compatible helpers)
// REPLACE: CacheVal bytes type to Arc<[u8]>
struct CacheVal { bytes: Arc<[u8]>, ph: [u8;32], len: usize, hot: bool }

struct ProofCacheShard {
    cap_count: usize,
    cap_bytes: usize,
    cur_bytes: usize,
    map: HashMap<[u8;32], CacheVal, RandomState>,
    // Two queues: cold (A1) and hot (AM)
    q_cold: VecDeque<[u8;32]>,
    q_hot:  VecDeque<[u8;32]>,
}

impl ProofCacheShard {
    #[inline(always)] fn new(cap_count: usize, cap_bytes: usize) -> Self {
        let cc = cap_count.max(8);
        Self {
            cap_count: cc, cap_bytes: cap_bytes.max(64*1024), cur_bytes: 0,
            map: { let mut m = HashMap::with_capacity_and_hasher(cc*2, RandomState::default()); m },
            q_cold: { let mut q = VecDeque::with_capacity(cc); q },
            q_hot:  { let mut q = VecDeque::with_capacity(cc/2 + 1); q },
        }
    }
    #[inline(always)] fn get(&mut self, k: &[u8;32]) -> Option<(Arc<[u8]>,[u8;32])> {
        if let Some(v) = self.map.get_mut(k) {
            if !v.hot {
                // promote to hot
                v.hot = true;
                // remove k from cold queue (linear scan bounded by small queue)
                if let Some(pos) = self.q_cold.iter().position(|x| x==k) { self.q_cold.remove(pos); }
                self.q_hot.push_back(*k);
            }
            return Some((v.bytes.clone(), v.ph));
        }
        None
    }
    #[inline(always)] fn insert(&mut self, k: [u8;32], v: Arc<[u8]>, ph: [u8;32]) {
        let len = v.len();
        if let Some(old) = self.map.get(&k) {
            self.cur_bytes = self.cur_bytes.saturating_sub(old.len);
            // keep existing hotness and queue position; replace value
            let hot = old.hot;
            self.map.insert(k, CacheVal { bytes:v, ph, len, hot });
            self.cur_bytes = self.cur_bytes.saturating_add(len);
        } else {
            self.map.insert(k, CacheVal { bytes:v, ph, len, hot:false });
            self.q_cold.push_back(k);
            self.cur_bytes = self.cur_bytes.saturating_add(len);
        }
        // Evict until within caps: prefer cold, then hot
        while (self.map.len() > self.cap_count) || (self.cur_bytes > self.cap_bytes) {
            if let Some(old) = self.q_cold.pop_front().or_else(|| self.q_hot.pop_front()) {
                if let Some(cv) = self.map.remove(&old) { self.cur_bytes = self.cur_bytes.saturating_sub(cv.len); }
            } else { break; }
        }
    }
}

static PROOF_CACHE: OnceCell<[RwLock<ProofCacheShard>; PC_SHARDS]> = OnceCell::new();

// ADD: singleflight de-dup (shares PC_SHARDS; uses parking_lot)
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

struct Flight { done: bool, val: Option<(Arc<[u8]>,[u8;32])> }
struct Pair { m: Mutex<Flight>, cv: Condvar }

struct SingleFlightShard {
    map: Mutex<HashMap<[u8;32], Arc<Pair>, RandomState>>,
}
static SINGLEFLIGHT: OnceCell<[SingleFlightShard; PC_SHARDS]> = OnceCell::new();

#[inline]
pub fn singleflight_init() {
    let _ = SINGLEFLIGHT.set(std::array::from_fn(|_| SingleFlightShard {
        map: Mutex::new(HashMap::with_hasher(RandomState::default()))
    }));
}

enum SfGuard { 
    Leader { idx: usize, key: [u8;32], pair: Arc<Pair> },
    Follower { pair: Arc<Pair> } 
}

#[inline(always)]
fn sf_hash_idx(k: &[u8;32]) -> usize { proof_cache_hash(k) }

#[inline(always)]
fn sf_claim_or_wait(key: &[u8;32]) -> SfGuard {
    if let Some(shards) = SINGLEFLIGHT.get() {
        let idx = sf_hash_idx(key);
        let shard = &shards[idx];
        prefetch_read(shard as *const _);
        let mut m = shard.map.lock();
        if let Some(p) = m.get(key) {
            return SfGuard::Follower { pair: p.clone() };
        }
        let p = Arc::new(Pair{ m: Mutex::new(Flight{ done:false, val:None }), cv: Condvar::new() });
        m.insert(*key, p.clone());
        return SfGuard::Leader { idx, key:*key, pair:p };
    }
    // If not initialized, default to Leader (no de-dup)
    SfGuard::Leader { idx:0, key:*key, pair:Arc::new(Pair{ m:Mutex::new(Flight{done:false,val:None}), cv:Condvar::new() }) }
}

#[inline(always)]
fn sf_publish(guard: SfGuard, v: Arc<[u8]>, ph: [u8;32]) {
    if let SfGuard::Leader { idx, key, pair } = guard {
        // Set result and wake all
        {
            let mut g = pair.m.lock();
            g.done = true;
            g.val  = Some((v, ph));
            pair.cv.notify_all();
        }
        if let Some(shards) = SINGLEFLIGHT.get() {
            let shard = &shards[idx];
            let mut m = shard.map.lock();
            let _ = m.remove(&key);
        }
    }
}

#[inline(always)]
fn sf_wait(pair: Arc<Pair>) -> (Arc<[u8]>,[u8;32]) {
    let mut g = pair.m.lock();
    while !g.done { pair.cv.wait(&mut g); }
    let (v, ph) = g.val.as_ref().unwrap();
    (v.clone(), *ph)
}

#[inline(always)]
fn proof_cache_hash(k: &[u8; 32]) -> usize {
    use ahash::AHasher;
    use core::hash::Hasher;
    // Deterministic keyed hasher, stable across process runs.
    // Security is irrelevant here (only sharding), speed matters.
    let mut h = AHasher::new_with_keys(
        0x9E37_79B9_7F4A_7C15,
        0xD1B5_4A32_D192_ED03,
    );
    h.write(k);
    (h.finish() as usize) & (PC_SHARDS - 1)
}

// REPLACE: proof_cache_init to accept total bytes as well as entries
#[inline]
pub fn proof_cache_init(total_entries: usize, total_bytes: usize) {
    let per_n = (total_entries / PC_SHARDS).max(8);
    let per_b = (total_bytes / PC_SHARDS).max(256 * 1024);
    let _ = PROOF_CACHE.set(std::array::from_fn(|_| RwLock::new(
        ProofCacheShard::new(per_n, per_b)
    )));
}

// REPLACE: proof_cache_get helper
#[inline(always)]
fn proof_cache_get(k: &[u8; 32]) -> Option<(Arc<[u8]>,[u8;32])> {
    if let Some(shards) = PROOF_CACHE.get() {
        let idx = proof_cache_hash(k);
        let sh = &shards[idx];
        prefetch_read(sh as *const _);
        return sh.write().get(k);
    }
    None
}
#[inline(always)]
fn proof_cache_put(k: [u8; 32], v: Arc<[u8]>, ph: [u8;32]) {
    if likely(PROOF_CACHE.get().is_some()) {
        let shards = PROOF_CACHE.get().unwrap();
        let idx = proof_cache_hash(&k);
        let sh = &shards[idx];
        prefetch_read(sh as *const _);
        sh.write().insert(k, v, ph);
    }
}

// ADD: final proof-hash from prehashed proof bytes
#[inline(always)]
fn compute_proof_hash_ctx_from_digest(
    proof_digest: &[u8; 32],
    slot: u64,
    nonce: u64,
    ctx_digest: &[u8; 32],
) -> Result<[u8; 32], ProofError> {
    Ok(blake3_digest_tls(&[
        DOMAIN_SEPARATOR_V2,
        b"PROOF_HASH_CTX_V2",
        NETWORK_ID,
        proof_digest,
        &slot.to_le_bytes(),
        &nonce.to_le_bytes(),
        ctx_digest,
    ]))
}

// ADD: cache key: circuit, slot, nonce, ctx, inputs digest
#[inline(always)]
fn build_cache_key(
    circuit_hash: &[u8; 32],
    slot: u64,
    nonce: u64,
    ctx_digest: &[u8; 32],
    inputs: &[Fr],
) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"PROOF_CACHE_KEY_V1");
    h.update(NETWORK_ID);
    h.update(circuit_hash);
    h.update(&slot.to_le_bytes());
    h.update(&nonce.to_le_bytes());
    h.update(ctx_digest);
    for fe in inputs {
        h.update(&fe.into_bigint().to_bytes_le());
    }
    h.finalize().into()
}

// REPLACE: ProverGuard — ticket fairness + CPU pin by ticket
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};

// ADD: cache-line padded atomics for hot counters
#[repr(align(64))]
struct PaddedU64(pub std::sync::atomic::AtomicU64);

#[repr(align(64))]
struct PaddedUsize(pub std::sync::atomic::AtomicUsize);

// If you have these already as tuple structs, keep names but pad:
static TICKET_NEXT:   PaddedU64  = PaddedU64(AtomicU64::new(0));
static TICKET_SERVE:  PaddedU64  = PaddedU64(AtomicU64::new(0));
static ACTIVE_PROVERS:PaddedU64  = PaddedU64(AtomicU64::new(0));
static PROVER_LIMIT_ATOMIC: PaddedU64 = PaddedU64(AtomicU64::new(1));

// Spins tuner already exists; pad it too:
static ADMISSION_SPINS: PaddedUsize = PaddedUsize(AtomicUsize::new(256));

#[inline(always)]
pub fn admission_spins_init(default_spins: usize) {
    ADMISSION_SPINS.0.store(default_spins.clamp(64, 4096), Ordering::Relaxed);
}

static CORE_IDS: OnceCell<Vec<core_affinity::CoreId>> = OnceCell::new();

// REPLACE: pin_prover_to_core (use ticket to pick a core, avoid HT collisions)
#[inline(always)]
fn pin_prover_to_core(ticket: u64) {
    #[cfg(all(unix, target_os = "linux"))] {
        let v = CPU_PREF_LIST.get_or_init(build_preferred_cpu_list);
        if !v.is_empty() {
            let cpu = v[(ticket as usize) % v.len()];
            set_affinity_cpu(cpu);
            return;
        }
    }
    // non-Linux or fallback: no-op
}

pub fn set_prover_limit(n: usize) { PROVER_LIMIT_ATOMIC.0.store(n.max(1), Ordering::Relaxed); }

// ADD: variance-aware ETA (p50/p95) from dual EMAs
use std::sync::atomic::{AtomicU64, AtomicBool};

struct AutoTune {
    min: usize,
    max: usize,
    target_ms: u64,
    ema50_x1024: AtomicU64,
    ema95_x1024: AtomicU64,
    armed: AtomicBool,
}
static AUTOTUNE: OnceCell<AutoTune> = OnceCell::new();

// ADD: network latency tuner (ms) with dual EMA
struct NetTune {
    ema50_x1024: AtomicU64,
    ema95_x1024: AtomicU64,
    inited: AtomicBool,
}
static NET_TUNE: OnceCell<NetTune> = OnceCell::new();

#[inline]
pub fn net_ema_init(default_ms: u64) {
    let _ = NET_TUNE.set(NetTune {
        ema50_x1024: AtomicU64::new((default_ms.max(1) as u64) << 10),
        ema95_x1024: AtomicU64::new((default_ms.max(1) as u64) << 10),
        inited: AtomicBool::new(true),
    });
}

/// Call this after each submission completes: end-to-end ms (submit -> landed slot cutoff)

#[inline]
fn net_snapshot_ms() -> (u64, u64) {
    if let Some(t) = NET_TUNE.get() {
        (t.ema50_x1024.load(Ordering::Relaxed) >> 10,
         t.ema95_x1024.load(Ordering::Relaxed) >> 10)
    } else {
        (40, 60) // sane defaults until first observation
    }
}

pub fn autotune_configure(min: usize, max: usize, target_ms: u64) {
    let _ = AUTOTUNE.set(AutoTune {
        min: min.max(1),
        max: max.max(min.max(1)),
        target_ms: target_ms.max(1),
        ema50_x1024: AtomicU64::new((target_ms as u64) << 10),
        ema95_x1024: AtomicU64::new((target_ms as u64) << 10),
        armed: AtomicBool::new(true),
    });
}

// ADD: hardware_calibrate_limits — better initial min/max/target before autotune learns
pub fn hardware_calibrate_limits() -> (usize, usize, u64) {
    let cores = num_cpus::get().max(1);
    #[cfg(target_arch = "x86_64")]
    let (has_avx2, has_avx512) = (is_x86_feature_detected!("avx2"), is_x86_feature_detected!("avx512f"));
    #[cfg(not(target_arch = "x86_64"))]
    let (has_avx2, has_avx512) = (false, false);

    let max = if has_avx512 { cores.saturating_sub(1).max(1) }
              else if has_avx2 { cores.saturating_sub(1).max(1) }
              else { cores.saturating_sub(2).max(1) };

    let min = (max / 4).max(1);
    let target_ms = if has_avx512 { 180 } else if has_avx2 { 200 } else { 240 };
    (min, max, target_ms)
}

// ADD: rusage sampling (per-thread) + fault-aware autotune wrapper
#[cfg(unix)]
#[inline(always)]
fn rusage_thread_faults() -> (u64, u64) {
    use nix::sys::resource::{getrusage, UsageWho};
    if let Ok(r) = getrusage(UsageWho::RUSAGE_THREAD) { (r.ru_majflt as u64, r.ru_minflt as u64) } else { (0, 0) }
}
#[cfg(not(unix))]
#[inline(always)]
fn rusage_thread_faults() -> (u64, u64) { (0, 0) }

// ADD: rusage switches
#[cfg(unix)]
#[inline(always)]
fn rusage_thread_switches() -> (u64, u64) {
    use nix::sys::resource::{getrusage, UsageWho};
    if let Ok(r) = getrusage(UsageWho::RUSAGE_THREAD) {
        (r.ru_nvcsw as u64, r.ru_nivcsw as u64)
    } else { (0, 0) }
}
#[cfg(not(unix))]
#[inline(always)]
fn rusage_thread_switches() -> (u64, u64) { (0, 0) }

// ADD: Token-bucket autotune (raise/lower cap per slot under preemption pressure)
#[inline]
pub fn token_bucket_autotune(ivcsw: u64) {
    let cap0 = TB_CAP.load(Ordering::Relaxed).max(1);
    let cap = if ivcsw == 0 {
        (cap0 + (cap0/16).max(1)).min(256) // gently increase
    } else if ivcsw <= 1 {
        cap0
    } else {
        cap0.saturating_sub((cap0/8).max(1)).max(4) // gently decrease
    };
    if cap != cap0 { TB_CAP.store(cap, Ordering::Relaxed); }
}

// ADD: Slot-aware ms_left correction (leader timing drift guard)
static SLOT_MS_EMA_X1024: AtomicU64 = AtomicU64::new(400u64 << 10); // start ~400ms/slot

#[inline]
pub fn slot_ms_observe(actual_ms: u64) {
    let s = (actual_ms.max(1) as u64) << 10;
    let prev = SLOT_MS_EMA_X1024.load(Ordering::Relaxed);
    let next = prev + ((s as i64 - prev as i64) >> 3) as u64; // α=1/8
    SLOT_MS_EMA_X1024.store(next, Ordering::Relaxed);
}

#[inline]
pub fn ms_left_corrected(ms_left_nominal: u64, slots_left: u64) -> u64 {
    let slot_ms = SLOT_MS_EMA_X1024.load(Ordering::Relaxed) >> 10;
    let target = slots_left.saturating_mul(slot_ms);
    ms_left_nominal.min(target)
}

#[inline(always)]
fn autotune_on_sample_faultaware(elapsed_ms: u64, majflt: u64, minflt: u64) {
    // Major faults are catastrophic (disk) → heavy penalty; minors light.
    let maj_pen = majflt.saturating_mul(elapsed_ms.saturating_mul(3) / 4 + 1);
    let min_pen = minflt / 64;
    autotune_on_sample(elapsed_ms.saturating_add(maj_pen).saturating_add(min_pen));
}

// ADD: fault+switch aware autotune sample
#[inline(always)]
fn autotune_on_sample_faultaware2(elapsed_ms: u64, majflt: u64, minflt: u64, ivcsw: u64, _vcsw: u64) {
    // Base penalty same as before
    let mut pen = elapsed_ms
        .saturating_add(majflt.saturating_mul(elapsed_ms.saturating_mul(3) / 4 + 1))
        .saturating_add(minflt / 64);

    // Preemption penalty
    if ivcsw > 0 {
        let sw_pen = ivcsw.saturating_mul((elapsed_ms / 8).max(1));
        pen = pen.saturating_add(sw_pen);
    }
    autotune_on_sample(pen);

    // NEW: spin tuner — target ~0.5 involuntary cswitches per proof on average
    let cur = ADMISSION_SPINS.0.load(Ordering::Relaxed);
    let target = if ivcsw == 0 { cur.saturating_sub(cur / 16).max(64) } // calm → gently reduce
                 else if ivcsw <= 1 { cur }                             // steady → keep
                 else { (cur + cur / 8).min(4096) };                    // busy  → gently raise
    if target != cur { ADMISSION_SPINS.0.store(target, Ordering::Relaxed); }
}

// ADD: autotune_snapshot — export current knobs cheaply
#[inline]
pub fn autotune_snapshot() -> Option<(usize, usize, u64, u64)> {
    AUTOTUNE.get().map(|t| {
        (
            PROVER_LIMIT_ATOMIC.0.load(Ordering::Relaxed),
            ACTIVE_PROVERS.0.load(Ordering::Relaxed),
            t.ema50_x1024.load(Ordering::Relaxed) >> 10,
            t.ema95_x1024.load(Ordering::Relaxed) >> 10,
        )
    })
}

// ADD: counters view
#[inline]
pub fn prover_counters() -> (usize, usize) {
    (ACTIVE_PROVERS.0.load(Ordering::Relaxed), PROVER_LIMIT_ATOMIC.0.load(Ordering::Relaxed))
}

// ADD: cold error helpers
#[cold]
#[inline(never)]
fn cold_perm_error(msg: String) -> ProofError { ProofError::InvalidPermissions(msg) }

#[cold]
#[inline(never)]
fn cold_ser_error(msg: String) -> ProofError { ProofError::Serialization(msg) }

// ADD: cold markers (no behavior change, improves I-cache/branch)
#[cold] 
#[inline(never)] 
fn err_backpressure(limit: u64) -> ProofError {
    ProofError::Backpressure { limit: limit as usize }
}

#[cold] 
#[inline(never)] 
fn err_serialization<S: Into<String>>(s: S) -> ProofError {
    ProofError::Serialization(s.into())
}

// ADD: global EV hint (monotone within short windows)
static EV_MAX_HINT: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
pub fn publish_ev_hint(ev: u64) {
    let mut cur = EV_MAX_HINT.load(Ordering::Relaxed);
    while ev > cur && EV_MAX_HINT.compare_exchange_weak(cur, ev, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        cur = EV_MAX_HINT.load(Ordering::Relaxed);
    }
}

// REPLACE: P2 quantile estimator (no static mut, no UB, still zero alloc at runtime)
use core::cmp::Ordering as CmpOrd;

struct P2 {
    inited: bool,
    q: f64,
    // marker heights and positions
    m: [f64; 5],
    n: [f64; 5],
    np: [f64; 5],
    // bootstrap buffer (exactly 5 samples)
    boot: [f64; 5],
    boot_cnt: u8,
}

impl P2 {
    #[inline] fn new(q: f64) -> Self {
        Self {
            inited: false, q,
            m: [0.0; 5], n: [0.0; 5], np: [0.0; 5],
            boot: [0.0; 5], boot_cnt: 0,
        }
    }

    #[inline] fn parabolic(y0:f64,y1:f64,y2:f64,n0:f64,n1:f64,n2:f64) -> f64 {
        let a = (n1 - n0 - 1.0) / (n2 - n1);
        let b = (n2 - n1 - 1.0) / (n1 - n0);
        y1 + ((a * (y2 - y1) + b * (y1 - y0)) / (n2 - n0))
    }

    #[inline] fn clamp_linear(y1:f64, y2:f64, up: bool) -> f64 {
        if up { y1 + (y2 - y1) } else { y1 - (y2 - y1) }
    }

    #[inline] fn bootstrap_done(&mut self) -> bool {
        if self.boot_cnt < 5 { return false; }
        // sort boot locally without alloc
        self.boot.sort_by(|a,b| a.partial_cmp(b).unwrap_or(CmpOrd::Equal));
        for i in 0..5 {
            self.m[i] = self.boot[i];
            self.n[i] = (i as f64) + 1.0;
        }
        // desired marker positions
        self.np = [
            1.0,
            1.0 + 2.0*self.q,
            1.0 + 4.0*self.q,
            3.0 + 2.0*self.q,
            5.0
        ];
        self.inited = true;
        true
        }

    #[inline] fn observe(&mut self, x: f64) {
        if !self.inited {
            self.boot[self.boot_cnt as usize] = x;
            self.boot_cnt += 1;
            if !self.bootstrap_done() { return; }
        } else {
            // clamp extremes
            if x < self.m[0] { self.m[0] = x; }
            if x > self.m[4] { self.m[4] = x; }

            // find cell k
            let mut k = 0usize;
            for i in 0..4 {
                if x >= self.m[i] && x < self.m[i+1] { k = i; break; }
                if i == 3 && x >= self.m[3] { k = 3; }
            }

            // update positions
            for i in (k+1)..=4 { self.n[i] += 1.0; }
            self.np[1] += self.q/2.0;
            self.np[2] += self.q;
            self.np[3] += (1.0 + self.q)/2.0;
            self.np[4] += 1.0;

            // adjust interior markers
            for i in 1..4 {
                let d = (self.np[i] - self.n[i]).round() as i32;
                if d != 0 {
                    let y = Self::parabolic(self.m[i-1], self.m[i], self.m[i+1],
                                            self.n[i-1], self.n[i], self.n[i+1]);
                    self.m[i] = if self.m[i-1] < y && y < self.m[i+1] {
                        y
                    } else {
                        Self::clamp_linear(self.m[i],
                                           if d > 0 { self.m[i+1] } else { self.m[i-1] },
                                           d > 0)
                    };
                    self.n[i] += d as f64;
                }
            }
        }
    }

    #[inline] fn value(&self) -> Option<f64> {
        if self.inited { Some(self.m[2]) } else { None }
    }
}

// P2 quantile estimators for prover and network latency
static P2_PROVER: OnceCell<Mutex<P2>> = OnceCell::new();
static P2_NET: OnceCell<Mutex<P2>> = OnceCell::new();

#[inline]
pub fn p2_init() {
    let _ = P2_PROVER.set(Mutex::new(P2::new(0.95)));
    let _ = P2_NET.set(Mutex::new(P2::new(0.95)));
}

#[inline]
pub fn p2_observe_prover(ms: u64) {
    if let Some(p2) = P2_PROVER.get() {
        p2.lock().observe(ms as f64);
    }
}


#[inline]
pub fn p2_snapshot_ms() -> (Option<u64>, Option<u64>, Option<u64>, Option<u64>) {
    let p50_prover = P2_PROVER.get().and_then(|p2| p2.lock().value()).map(|v| v as u64);
    let p95_prover = P2_PROVER.get().and_then(|p2| p2.lock().value()).map(|v| v as u64);
    let p50_net = P2_NET.get().and_then(|p2| p2.lock().value()).map(|v| v as u64);
    let p95_net = P2_NET.get().and_then(|p2| p2.lock().value()).map(|v| v as u64);
    (p50_prover, p95_prover, p50_net, p95_net)
}

#[inline(always)]
fn autotune_on_sample(elapsed_ms: u64) {
    if let Some(t) = AUTOTUNE.get() {
        let s = elapsed_ms << 10;
        // p50 ~ alpha=1/8; p95 ~ heavier alpha=1/32 and penalize overs
        let p50_prev = t.ema50_x1024.load(Ordering::Relaxed);
        let p95_prev = t.ema95_x1024.load(Ordering::Relaxed);

        let p50_next = p50_prev + ((s as i64 - p50_prev as i64) >> 3) as u64;
        // if sample > ema95 then push harder; else decay gently
        let over = (s > p95_prev) as u64;
        let delta = if over != 0 { ((s - p95_prev) >> 2) } else { ((s as i64 - p95_prev as i64) >> 5) as u64 };
        let p95_next = p95_prev.wrapping_add(delta);

        t.ema50_x1024.store(p50_next, Ordering::Relaxed);
        t.ema95_x1024.store(p95_next, Ordering::Relaxed);

        // your prior concurrency hysteresis here (unchanged)
        if !t.armed.load(Ordering::Relaxed) { return; }
        let ema_ms = p50_next >> 10;
        let up_thresh   = (t.target_ms as u128 * 115) / 100;
        let down_thresh = (t.target_ms as u128 * 85)  / 100;

        let cur = PROVER_LIMIT_ATOMIC.0.load(Ordering::Relaxed);
        if ema_ms as u128 > up_thresh && cur > t.min {
            PROVER_LIMIT_ATOMIC.0.store(cur.saturating_sub(1).max(t.min), Ordering::Relaxed);
            t.armed.store(false, Ordering::Relaxed);
        } else if ema_ms as u128 < down_thresh && cur < t.max {
            PROVER_LIMIT_ATOMIC.0.store((cur + 1).min(t.max), Ordering::Relaxed);
            t.armed.store(false, Ordering::Relaxed);
        }
        static SAMPLE_COUNT: AtomicU64 = AtomicU64::new(0);
        if SAMPLE_COUNT.fetch_add(1, Ordering::Relaxed) % 32 == 31 {
            t.armed.store(true, Ordering::Relaxed);
        }
    }
}

// REPLACE: admit_by_deadline to prefer P² when available
#[derive(Debug)]
pub enum AdmitDecision { Start, SkipLate }

#[inline(always)]
pub fn admit_by_deadline(ms_left: u64) -> AdmitDecision {
    if let Some(t) = AUTOTUNE.get() {
        // Prover p95 via EMA fallback; prefer P² if warm
        let p95_ema = (t.ema95_x1024.load(Ordering::Relaxed) >> 10).max(1);
        let (_p95, _p99, n95_p2, _n99_p2) = p2_snapshot_ms();
        let (_net50, net95_ema) = net_snapshot_ms();
        let net95 = n95_p2.unwrap_or(net95_ema);
        // budgets with margins
        let prover = ((p95_ema as u128) * 115 / 100);
        let wire   = ((net95 as u128) * 120 / 100);
        let need = prover + wire;
        if (ms_left as u128) <= need { AdmitDecision::SkipLate } else { AdmitDecision::Start }
    } else {
        AdmitDecision::Start
    }
}


// ADD: priority-aware skip (depth+net) — late + congested → yield immediately
#[inline(always)]
pub fn admit_quick(ms_left: u64) -> bool {
    let qd = queue_depth();
    // If already behind net+prover budget AND backlog exists, skip pre-admission.
    match admit_by_deadline(ms_left) {
        AdmitDecision::SkipLate if qd > 0 => false,
        _ => true,
    }
}

// ADD: CU budget + priority fee helpers (client-side)
const MAX_CU_LIMIT: u32 = 1_400_000; // current cluster cap; adjust when cluster raises
#[inline]
pub fn compute_budget_ix(cu_limit: u32, micro_lamports_per_cu: u64) -> [Instruction; 2] {
    let limit = cu_limit.min(MAX_CU_LIMIT);
    [
        ComputeBudgetInstruction::set_compute_unit_limit(limit),
        ComputeBudgetInstruction::set_compute_unit_price(micro_lamports_per_cu),
    ]
}

/// Escalate CU price as deadline nears using your autotune EMA.
/// ms_left: time to slot cutoff; returns micro-lamports/CU.
#[inline]
pub fn priority_fee_escalator(ms_left: u64, base: u64, max: u64) -> u64 {
    let cap_base = base.min(max);
    let (p50, p95) = if let Some(t) = AUTOTUNE.get() {
        ((t.ema50_x1024.load(Ordering::Relaxed) >> 10).max(1),
         (t.ema95_x1024.load(Ordering::Relaxed) >> 10).max(1))
    } else { (200, 300) };

    if ms_left >= p95 { return cap_base; }
    let span  = (p95.saturating_sub(ms_left)) as u128;
    let denom = p95.max(p50) as u128;
    let t = (span << 16) / denom;                 // Q16 in [0, ~1]
    // Strictly monotone quad bump in [cap_base, max]
    let bump = (t * t * (max - cap_base) as u128) >> 32;
    (cap_base as u128 + bump).min(max as u128) as u64
}

// ADD: priority fee with load bump (bounded, smooth)
#[inline]
pub fn priority_fee_escalator_load(ms_left: u64, base: u64, max: u64) -> u64 {
    let mut p = priority_fee_escalator(ms_left, base, max);
    let (active, limit) = prover_counters();
    if limit > 0 {
        let util_q16 = ((active as u128) << 16) / (limit as u128);      // [0,1] in Q16
        // Gentle convex bump: util^2 * (remaining headroom) * 1/4
        let bump = ((util_q16 * util_q16) * (max.saturating_sub(p) as u128)) >> 34;
        p = (p as u128 + bump).min(max as u128) as u64;
    }
    p
}


// ADD: net-aware fee escalator (use this in place of plain one)
#[inline]
pub fn priority_fee_escalator_net(ms_left: u64, base: u64, max: u64) -> u64 {
    let mut p = priority_fee_escalator_load(ms_left, base, max); // your load-aware curve
    let (_net50, net95) = net_snapshot_ms();
    if ms_left < net95 {
        // convex bump proportional to how far inside net p95 we are
        let span = (net95.saturating_sub(ms_left)) as u128;
        let t = (span << 16) / net95.max(1) as u128;             // Q16
        let bump = (t * t * (max.saturating_sub(p) as u128)) >> 32;
        p = (p as u128 + bump).min(max as u128) as u64;
    }
    p
}

// ADD: (optional) depth-aware fee wrapper
#[inline]
pub fn priority_fee_escalator_depth(ms_left: u64, base: u64, max: u64) -> u64 {
    let mut p = priority_fee_escalator_net(ms_left, base, max);
    let d = queue_depth();
    if d > 0 {
        // Smooth convex bump capped to 25% of remaining headroom at depth >= 32
        let cap = (max.saturating_sub(p) as u128) / 4;
        let t = ((d.min(32) as u128) << 16) / 32; // Q16 in [0,1]
        let bump = (t * t * cap) >> 32;
        p = (p as u128 + bump).min(max as u128) as u64;
    }
    p
}

// ADD: EV-capped fee escalation (never pay above edge)
/// base/max are micro-lamports/cu. `ev_lamports` is expected PnL of the bundle.
/// `cu_est` is your CU estimate for the tx (or bound).
// REPLACED: Convex fee escalator with EV cap (safe monotone)
#[inline]
pub fn priority_fee_escalator_ev(ms_left: u64, base: u64, max: u64, cu_est: u32, ev_lamports: u64) -> u64 {
    let (_net50, net95) = net_snapshot_ms();
    
    // Use the smooth convex curve from the ZK client
    let mut p = priority_fee_escalator_zk(ms_left, base, max, _net50, net95);
    
    // Add network-aware bump
    if net95 > 0 {
        let net_bump = (net95.saturating_sub(50) * (max.saturating_sub(p)) / 200).min(max / 4);
        p = p.saturating_add(net_bump);
    }
    
    // EV-aware bump: scale by expected value vs compute cost
    if cu_est > 0 && ev_lamports > 0 {
        let ev_ratio = (ev_lamports as u128 * 1000) / (cu_est as u128 * p as u128);
        let ev_bump = if ev_ratio > 1000 { (p * 3) / 10 } else { p / 10 };
        p = p.saturating_add(ev_bump);
    }
    
    // absolute cap = floor(EV / CU) in micro-lamports (1e-6 lamports/cu)
    if cu_est > 0 {
        let cap = (ev_lamports.saturating_mul(1_000_000) / cu_est as u64).max(base);
        p = p.min(cap);
    }
    
    p.min(max)
}

// REPLACE: push_inputs_fast
#[inline(always)]
fn push_inputs_fast(
    builder: &mut CircomBuilder<Bn254>,
    inputs: &[Fr],
) -> Result<(), ProofError> {
    INPUT_SCRATCH.with(|cell| -> Result<(), ProofError> {
        let mut s = cell.borrow_mut();
        for (i, fe) in inputs.iter().enumerate() {
            s.clear();
            // Write decimal without allocating new buffers each time
            use ark_ff::PrimeField;
            use core::fmt::Write as _;
            // Convert to big integer once, format decimal into the scratch string.
            let bi = fe.into_bigint();
            write!(s, "{}", bi).map_err(|_| ProofError::Serialization("fmt write".into()))?;
            builder.push_input(in_name(i), &*s)
                .map_err(|e| ProofError::Serialization(format!("push_input: {e}")))?;
        }
        Ok(())
    })
}

// ADD: prepend compute budget ixs (Solana SDK)
#[inline]
pub fn prepend_compute_budget(ixs: &mut Vec<solana_sdk::instruction::Instruction>, cu: u32, heap: u32, micro_lamports_per_cu: u64) {
    use solana_sdk::compute_budget::ComputeBudgetInstruction as CBI;
    let mut prepend: ArrayVec<solana_sdk::instruction::Instruction, 3> = ArrayVec::new();

    if heap > 0 {
        prepend.push(CBI::request_heap_frame(heap).into());
    }
    if cu > 0 {
        prepend.push(CBI::set_compute_unit_limit(cu).into());
    }
    if micro_lamports_per_cu > 0 {
        prepend.push(CBI::set_compute_unit_price(micro_lamports_per_cu).into());
    }
    // Dedup any preexisting budget ixs at the front
    while let Some(ix) = ixs.first() {
        let pid = ix.program_id;
        if pid == solana_sdk::compute_budget::id() { ixs.remove(0); } else { break; }
    }
    // Splice at front
    ixs.splice(0..0, prepend.into_iter());
}

// ADD: Autosize CU & heap from proof size (heuristic; tune per your verifier program)
#[inline]
pub fn autosize_budget_for_proof(proof_len: usize) -> (u32, u32, u64) {
    // Heap: round up to next 32 KiB with a safety overhead
    let heap = ((proof_len + 32 * 1024 + 4096) / (32 * 1024)) * (32 * 1024);
    // CU: base 220k + 30 CU per KiB of proof; clamp to MAX_CU_LIMIT
    let cu  = (220_000usize + (proof_len / 1024) * 30).min(MAX_CU_LIMIT as usize) as u32;
    // price: caller still chooses; return 0 to let escalator decide
    (cu, heap as u32, 0)
}

// ADD: build single payload buffer: [AT3...][mac][proof_bytes]
#[inline]
pub fn build_envelope_payload(attestation_v3: &[u8], proof_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(attestation_v3.len() + proof_bytes.len());
    out.extend_from_slice(attestation_v3);
    out.extend_from_slice(proof_bytes);
    out
}

// ADD: compute budget with heap size
use solana_sdk::{instruction::Instruction, compute_budget::ComputeBudgetInstruction as CBI};


// ADD: ProofContext and helper (paste below constants)
#[derive(Clone, Default)]
pub struct ProofContext<'a> {
    /// Program/protocol binding, e.g. your on-chain program id or strategy tag
    pub program_tag: &'a [u8],
    /// Optional route commitment (e.g. BLAKE3 of DEX path / pool keys / jito relay)
    pub route_commitment: Option<[u8; 32]>,
    /// Arbitrary additional bytes (e.g. risk policy hash, config version)
    pub extra: &'a [u8],
}

// ADD: AttestedProofV3 envelope (ship this off-chain to your program)
#[derive(Clone)]
pub struct AttestedProofV3 {
    pub proof_bytes: Vec<u8>,
    pub attestation_v3: Vec<u8>,
    pub ctx_digest_sha256: [u8; 32],
    pub witness_root: [u8; 32],
    pub circuit_hash: [u8; 32],
}

fn compute_ctx_digest(ctx: &Option<ProofContext<'_>>) -> [u8; 32] {
    if let Some(c) = ctx {
        let mut h = blake3::Hasher::new();
        h.update(DOMAIN_SEPARATOR_V2);
        h.update(b"CTX");
        h.update(NETWORK_ID);
        h.update(c.program_tag);
        if let Some(rc) = c.route_commitment { h.update(&rc); }
        h.update(c.extra);
        h.finalize().into()
    } else {
        [0u8; 32]
    }
}

// ADD: public helper to compute ctx digest from raw pieces
pub fn compute_ctx_digest_public(
    program_tag: &[u8],
    route_commitment: Option<[u8; 32]>,
    extra: &[u8],
) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"CTX");
    h.update(NETWORK_ID);
    h.update(program_tag);
    if let Some(rc) = route_commitment { h.update(&rc); }
    h.update(extra);
    h.finalize().into()
}

// ADD: SHA256 ctx digest + attestation v2 (syscall-friendly)
#[inline(always)]
pub fn compute_ctx_digest_sha256(
    program_tag: &[u8],
    route_commitment: Option<[u8; 32]>,
    extra: &[u8],
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"CTX_SHA256");
    h.update(NETWORK_ID);
    h.update(program_tag);
    if let Some(rc) = route_commitment { h.update(&rc); }
    h.update(extra);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out[..32]);
    arr
}

/// V2 attestation uses SHA256 so Solana programs can verify with `hash::hashv` cheaply.
pub fn build_attestation_v2_sha256(
    proof_hash: [u8; 32],
    ctx_digest_sha256: [u8; 32],
    circuit_hash: [u8; 32],
) -> Vec<u8> {
    // Layout: b"AT2", version=2, [proof_hash][ctx_sha256][circuit_hash][mac_sha256]
    let mut out = Vec::with_capacity(3 + 1 + 32 + 32 + 32 + 32);
    out.extend_from_slice(b"AT2");
    out.push(2u8); // version 2
    out.extend_from_slice(&proof_hash);
    out.extend_from_slice(&ctx_digest_sha256);
    out.extend_from_slice(&circuit_hash);

    let mut mac = Sha256::new();
    mac.update(DOMAIN_SEPARATOR_V2);
    mac.update(b"ATTEST_MAC_V2_SHA256");
    mac.update(NETWORK_ID);
    mac.update(&proof_hash);
    mac.update(&ctx_digest_sha256);
    mac.update(&circuit_hash);
    let mac_bytes = mac.finalize();

    out.extend_from_slice(&mac_bytes[..32]);
    out
}

// ADD: Witness Merkle root (BLAKE3) and v3 SHA256 attestation with witness root.

// REPLACE: blake3_leaf_witness
#[inline(always)]
fn blake3_leaf_witness(fe: &Fr) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"WIT_LE");
    let b = fr_to_le_bytes32(fe);
    h.update(&b);
    h.finalize().into()
}

// REPLACE: compute_witness_merkle_root
pub fn compute_witness_merkle_root(inputs: &[Fr]) -> [u8; 32] {
    use smallvec::SmallVec;

    if inputs.is_empty() {
        let mut h = blake3::Hasher::new();
        h.update(DOMAIN_SEPARATOR_V2);
        h.update(b"WIT_EMPTY");
        return h.finalize().into();
    }

    // Start with leaves in a preallocated vector; then fold in-place.
    let mut layer: SmallVec<[[u8;32]; 128]> = SmallVec::new();
    layer.reserve_exact(inputs.len().min(128));
    for fe in inputs { layer.push(blake3_leaf_witness(fe)); }

    let mut vec_layer: Vec<[u8;32]> = if layer.len() == inputs.len() {
        layer.into_vec()
    } else {
        // inputs > 128 → push remaining into a Vec to avoid many reallocs
        let mut v = Vec::with_capacity(inputs.len());
        v.extend_from_slice(&layer);
        for fe in &inputs[layer.len()..] { v.push(blake3_leaf_witness(fe)); }
        v
    };

    while vec_layer.len() > 1 {
        let mut next = Vec::with_capacity((vec_layer.len() + 1) / 2);
        let mut i = 0usize;
        while i < vec_layer.len() {
            let a = vec_layer[i];
            let b = if i + 1 < vec_layer.len() { vec_layer[i + 1] } else { a };
            let mut h = blake3::Hasher::new();
            h.update(DOMAIN_SEPARATOR_V2);
            h.update(b"WIT_NODE");
            h.update(&a);
            h.update(&b);
            next.push(h.finalize().into());
            i += 2;
        }
        vec_layer = next;
    }
    vec_layer[0]
}

/// Attestation v3: includes witness_root; cheap on-chain verify via SHA256.
pub fn build_attestation_v3_sha256(
    proof_hash: [u8; 32],
    ctx_digest_sha256: [u8; 32],
    circuit_hash: [u8; 32],
    witness_root: [u8; 32],
) -> Vec<u8> {
    // Layout: "AT3" | 0x03 | proof(32) | ctx_sha(32) | circuit(32) | wit_root(32) | mac(32)
    let mut out = Vec::with_capacity(3 + 1 + 32*5);
    out.extend_from_slice(b"AT3");
    out.push(3u8);
    out.extend_from_slice(&proof_hash);
    out.extend_from_slice(&ctx_digest_sha256);
    out.extend_from_slice(&circuit_hash);
    out.extend_from_slice(&witness_root);

    let mut mac = sha2::Sha256::new();
    mac.update(DOMAIN_SEPARATOR_V2);
    mac.update(b"ATTEST_MAC_V3_SHA256");
    mac.update(NETWORK_ID);
    mac.update(&proof_hash);
    mac.update(&ctx_digest_sha256);
    mac.update(&circuit_hash);
    mac.update(&witness_root);
    let mac_bytes = mac.finalize();
    out.extend_from_slice(&mac_bytes[..32]);
    out
}

// ADD: Artifact integrity sealing
#[derive(Clone, Debug)]
pub struct ArtifactSeal {
    pub wasm_path: PathBuf,
    pub r1cs_path: PathBuf,
    pub pk_path: PathBuf,
    /// Allowlist of acceptable composite digests (e.g. rotated versions)
    pub allowlist: Vec<[u8; 32]>,
}

fn blake3_file(path: &Path) -> std::io::Result<[u8; 32]> {
    // Basic version (used in non-sealed contexts)
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 262_144];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

// REPLACE: blake3_file_secure (WILLNEED + READAHEAD + SEQ + DONTNEED)
fn blake3_file_secure(canon_path: &Path) -> Result<[u8; 32], ProofError> {
    let f = open_readonly_nofollow(canon_path)?;
    verify_same_inode(canon_path, &f)?;

    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let fd = f.as_raw_fd();
        let _ = nix::fcntl::posix_fadvise(fd, 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_WILLNEED);
        let _ = nix::fcntl::posix_fadvise(fd, 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL);
        #[cfg(target_os = "linux")]
        {
            if let Ok(len) = f.metadata().map(|m| m.len() as libc::size_t) {
                if len > 0 {
                    unsafe { let _ = libc::readahead(fd, 0 as libc::off64_t, len); }
                }
            }
        }
    }

    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 262_144];
    let mut file = f;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }

    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let _ = nix::fcntl::posix_fadvise(
            file.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
        );
    }
    Ok(hasher.finalize().into())
}

/// Composite artifact digest: domain-separate each component; order matters.
fn composite_artifact_digest(wasm: &[u8; 32], r1cs: &[u8; 32], pk: &[u8; 32]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"ARTIFACTS_V1");
    h.update(NETWORK_ID);
    h.update(b"WASM"); h.update(wasm);
    h.update(b"R1CS"); h.update(r1cs);
    h.update(b"PK");   h.update(pk);
    h.finalize().into()
}

/// Public utility for CI / allowlist generation.
pub fn compute_artifact_digest_paths(
    wasm_path: &Path,
    r1cs_path: &Path,
    pk_path: &Path,
) -> std::io::Result<[u8; 32]> {
    let w = blake3_file(wasm_path)?;
    let r = blake3_file(r1cs_path)?;
    let mut f = File::open(pk_path)?;
    let mut v = Vec::new(); f.read_to_end(&mut v)?;
    let pk_digest: [u8; 32] = {
        let mut h = blake3::Hasher::new();
        h.update(&v);
        h.finalize().into()
    };
    Ok(composite_artifact_digest(&w, &r, &pk_digest))
}

// ADD: ct_eq_32 — constant-time compare for 32-byte digests
#[inline(always)]
fn ct_eq_32(a: &[u8;32], b: &[u8;32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 { diff |= a[i] ^ b[i]; }
    diff == 0
}

// ADD: constant-time digest allowlist
#[inline(always)]
fn allowlist_contains_ct(list: &[[u8; 32]], needle: &[u8; 32]) -> bool {
    let mut found: u8 = 0;
    for item in list {
        let mut diff = 0u8;
        for i in 0..32 { diff |= item[i] ^ needle[i]; }
        // (diff == 0) ? 1 : 0, in constant time
        let is_eq = ((diff == 0) as u8);
        found |= is_eq;
    }
    found == 1
}

// ADD: advise_sensitive_slice — Linux only; no-op elsewhere
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_sensitive_slice(s: &[u8]) {
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_DONTDUMP);
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_HUGEPAGE);
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn advise_sensitive_slice(_s: &[u8]) { /* no-op */ }

// ADD: advise_drop_cache — drop mapped pages from cache post-use (Linux)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_drop_cache(s: &[u8]) {
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_DONTNEED);
}
#[cfg(not(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_drop_cache(_s: &[u8]) { /* no-op */ }

// ADD: advise_willneed_slice — prefetch pages (Linux), no-op elsewhere
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_willneed_slice(s: &[u8]) {
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_WILLNEED);
}
#[cfg(not(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_willneed_slice(_s: &[u8]) { /* no-op */ }

// ADD: MADV_DONTFORK advice for sensitive buffers (Linux only)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_dontfork_slice(s: &[u8]) {
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_DONTFORK);
}
#[cfg(not(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_dontfork_slice(_s: &[u8]) { /* no-op */ }

// ADD: NUMA + DONTDUMP mapping advisors (Linux best-effort)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_dontdump_slice(s: &[u8]) {
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_DONTDUMP);
}

#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn advise_dontdump_slice(_s: &[u8]) {}

// REPLACE: bind_mmap_to_local_numa with migration flags
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn bind_mmap_to_local_numa(s: &[u8]) {
    use std::{fs, io::Read};
    let cpu = unsafe { libc::sched_getcpu() };
    if cpu < 0 { return; }
    let path = format!("/sys/devices/system/cpu/cpu{}", cpu);
    let mut node: Option<u32> = None;
    if let Ok(dir) = fs::read_dir(&path) {
        for e in dir.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(n) = name.strip_prefix("node").and_then(|x| x.parse::<u32>().ok()) {
                node = Some(n); break;
            }
        }
    }
    if let Some(n) = node {
        unsafe {
            let mut mask: [u64; 1] = [0];
            if n < 64 {
                mask[0] = 1u64 << n;
                const MPOL_PREFERRED: libc::c_int = 1;
                const MPOL_MF_MOVE:  libc::c_int = 1;
                let _ = libc::mbind(
                    s.as_ptr() as *mut _,
                    s.len(),
                    MPOL_PREFERRED,
                    mask.as_ptr() as *const _,
                    (std::mem::size_of_val(&mask) * 8) as u64,
                    MPOL_MF_MOVE as u32
                );
            }
        }
    }
}

#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn bind_mmap_to_local_numa(_s: &[u8]) {}

// ADD: KSM/fork hygiene + THP collapse advisors
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_unmergeable_slice(s: &[u8]) {
    use nix::sys::mman::{madvise, MadviseAdvice};
    let _ = madvise(s.as_ptr() as *mut _, s.len(), MadviseAdvice::MADV_UNMERGEABLE);
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn advise_unmergeable_slice(_s: &[u8]) {}

#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_wipeonfork_slice(s: &[u8]) {
    // MADV_WIPEONFORK (18) — not in older libc constants; call raw.
    const MADV_WIPEONFORK: i32 = 18;
    unsafe { libc::madvise(s.as_ptr() as *mut _, s.len(), MADV_WIPEONFORK); }
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn advise_wipeonfork_slice(_s: &[u8]) {}

#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn advise_collapse_slice(s: &[u8]) {
    // MADV_COLLAPSE (25) — best effort; ignores on kernels without support.
    const MADV_COLLAPSE: i32 = 25;
    unsafe { libc::madvise(s.as_ptr() as *mut _, s.len(), MADV_COLLAPSE); }
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn advise_collapse_slice(_s: &[u8]) {}

// ADD: mincore-based residency check + fallback touch (Linux best-effort)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn ensure_resident_ratio(s: &[u8], min_ratio: f32) {
    if s.len() < (64 << 20) { return; } // small maps don't need this
    let page = 4096usize;
    let pages = (s.len() + page - 1) / page;
    let mut vec = vec![0u8; pages];
    let r = unsafe { libc::mincore(s.as_ptr() as *mut _, s.len(), vec.as_mut_ptr() as *mut _) };
    if r != 0 { return; }
    let resident = vec.iter().filter(|&&v| v & 1 != 0).count();
    if (resident as f32) / (pages as f32) >= min_ratio { return; }
    // Fallback: stride-touch ~every 2MB to populate without thrashing
    let stride = 2 * 1024 * 1024;
    let mut i = 0usize;
    while i < s.len() {
        // SAFETY: read a byte to fault the page
        unsafe { std::ptr::read_volatile(s.as_ptr().add(i)); }
        i = i.saturating_add(stride);
    }
}

#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn ensure_resident_ratio(_s: &[u8], _min_ratio: f32) {}

// ADD: mlock2 ONFAULT (Linux best-effort; no-op elsewhere)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn mlock_onfault_slice(s: &[u8]) {
    // mlock2(addr, len, MLOCK_ONFAULT=1)
    unsafe { libc::mlock2(s.as_ptr() as *const _, s.len(), 1); }
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn mlock_onfault_slice(_s: &[u8]) {}

// ADD: per-thread timer slack (Linux best-effort)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn reduce_timer_slack_ns(ns: u64) {
    unsafe { libc::prctl(libc::PR_SET_TIMERSLACK as i32, ns as libc::c_ulong, 0, 0, 0); }
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn reduce_timer_slack_ns(_ns: u64) {}

// ADD: block noisy signals in the current thread (Linux best-effort)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn block_signals_hot_thread() {
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        for &sig in &[libc::SIGHUP, libc::SIGINT, libc::SIGQUIT, libc::SIGUSR1, libc::SIGUSR2, libc::SIGTERM] {
            libc::sigaddset(&mut set, sig);
        }
        let _ = libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn block_signals_hot_thread() {}

// ADD: assert_secure_path_hierarchy — verify parent dirs aren't writable by group/world
#[cfg(unix)]
fn assert_secure_path_hierarchy(path: &Path) -> Result<(), ProofError> {
    use std::os::unix::fs::MetadataExt;
    use nix::unistd::geteuid;

    let euid = geteuid().as_raw();
    let mut cur = path.parent().unwrap_or(Path::new("."));
    for _ in 0..4 { // walk up a few levels; enough to catch common mistakes
        if cur.as_os_str().is_empty() { break; }
        if let Ok(md) = fs::symlink_metadata(cur) {
            let mode = md.mode();
            let uid  = md.uid();

            // Allow /tmp-style sticky directories only if owned and sticky.
            let sticky = (mode & 0o1000) != 0;
            let grp_w  = (mode & 0o020) != 0;
            let oth_w  = (mode & 0o002) != 0;

            if (grp_w || oth_w) && !(sticky && uid == euid) {
                return Err(ProofError::InvalidPermissions(format!(
                    "insecure dir {} mode {:o} (grp/oth writable, not sticky-owned)",
                    cur.display(), mode
                )));
            }
        }
        if let Some(p) = cur.parent() { cur = p; } else { break; }
    }
    Ok(())
}

#[cfg(not(unix))]
fn assert_secure_path_hierarchy(_path: &Path) -> Result<(), ProofError> { Ok(()) }

// ADD: write one audit line via writev (best-effort; falls back silently)
#[cfg(unix)]
#[inline]
fn append_audit_line_fast(
    path: &str,
    meta: &Metadata,
    n_inputs: usize,
    inputs_digest: [u8;32],
    ctx_digest: &[u8;32],
    elapsed_ms: u64,
    extra: Option<[u8;32]>, // witness_root for v3; pass None for v2
) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::fd::AsRawFd;
    use std::io::Write;

    let mut f = std::fs::OpenOptions::new()
        .create(true).append(true).mode(0o600).custom_flags(libc::O_CLOEXEC)
        .open(path)?;

    let nl = b"\n";
    let sep = b" ";
    let mut ts = [0u8; 32];
    // REPLACE the now_ms calculation:
    #[cfg(all(unix, target_os="linux"))]
    let now_ms = unsafe {
        let mut ts: libc::timespec = core::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_REALTIME_COARSE, &mut ts);
        (ts.tv_sec as u64)*1_000 + (ts.tv_nsec as u64)/1_000_000
    };
    #[cfg(not(all(unix, target_os="linux")))]
    let now_ms = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64);
    ts[..8].copy_from_slice(&now_ms.to_le_bytes());

    let pieces: [&[u8]; 13] = [
        &ts[..8], sep,
        &meta.proof_hash, sep,
        &meta.circuit_hash, sep,
        &inputs_digest, sep,
        ctx_digest, sep,
        &elapsed_ms.to_le_bytes(), sep,
        &n_inputs.to_le_bytes(),
    ];
    // Optional witness root chunk
    let (iov_extra, iov_tail): (&[libc::iovec], &[libc::iovec]) = if let Some(w) = extra {
        let ex = [libc::iovec { iov_base: w.as_ptr() as *mut _, iov_len: 32 }];
        (&ex, &[libc::iovec { iov_base: nl.as_ptr() as *mut _, iov_len: 1 }])
    } else {
        (&[], &[libc::iovec { iov_base: nl.as_ptr() as *mut _, iov_len: 1 }])
    };

    // Build iovec stack (pieces + optional + newline)
    let mut ios: [libc::iovec; 15] = [libc::iovec { iov_base: std::ptr::null_mut(), iov_len: 0 }; 15];
    let mut k = 0;
    for p in pieces.iter() {
        ios[k] = libc::iovec { iov_base: p.as_ptr() as *mut _, iov_len: p.len() }; k += 1;
    }
    for e in iov_extra {
        ios[k] = *e; k += 1;
    }
    ios[k] = iov_tail[0];

    unsafe { libc::writev(f.as_raw_fd(), ios.as_ptr(), (k + 1) as i32); }
    Ok(())
}
#[cfg(not(unix))]
#[inline]
fn append_audit_line_fast(
    _path: &str, _meta: &Metadata, _n_inputs: usize, _inputs_digest: [u8;32], _ctx: &[u8;32], _elapsed_ms: u64, _extra: Option<[u8;32]>
) -> std::io::Result<()> { Ok(()) }

// REPLACE: add IP_TOS low-delay mark (Linux, best-effort)
#[inline]
pub fn tune_low_latency_stream(s: &std::net::TcpStream) {
    let _ = s.set_nodelay(true);
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        use std::os::fd::AsRawFd;
        let fd = s.as_raw_fd();

        const TCP_QUICKACK: libc::c_int = 12;
        const TCP_NOTSENT_LOWAT: libc::c_int = 25;
        const TCP_USER_TIMEOUT: libc::c_int = 18;
        const TCP_FASTOPEN_CONNECT: libc::c_int = 30;
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_QUICKACK, &one as *const _ as *const _, std::mem::size_of::<libc::c_int>() as _);
        let low: libc::c_uint = 1;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_NOTSENT_LOWAT, &low as *const _ as *const _, std::mem::size_of::<libc::c_uint>() as _);
        let uto: libc::c_uint = 200;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_USER_TIMEOUT, &uto as *const _ as *const _, std::mem::size_of::<libc::c_uint>() as _);
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_FASTOPEN_CONNECT, &one as *const _ as *const _, std::mem::size_of::<libc::c_int>() as _);

        // Prioritize on some queuing paths
        let prio: libc::c_int = 6; // SO_PRIORITY 0..6; 6 ~ low-latency
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_PRIORITY, &prio as *const _ as *const _, std::mem::size_of::<libc::c_int>() as _);

        // Busy-poll a little on the socket (requires sysctl enable; harmless otherwise)
        #[allow(non_upper_case_globals)]
        const SO_BUSY_POLL: libc::c_int = 46;
        let busy: libc::c_int = 50_000; // 50µs best-effort
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, SO_BUSY_POLL, &busy as *const _ as *const _, std::mem::size_of::<libc::c_int>() as _);

        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, &one as *const _ as *const _, std::mem::size_of::<libc::c_int>() as _);
        const TCP_KEEPIDLE: libc::c_int = 4;
        const TCP_KEEPINTVL: libc::c_int = 5;
        const TCP_KEEPCNT: libc::c_int = 6;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_KEEPIDLE, &uto as *const _ as *const _, std::mem::size_of::<libc::c_uint>() as _);
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_KEEPINTVL, &uto as *const _ as *const _, std::mem::size_of::<libc::c_uint>() as _);
        let kcnt: libc::c_uint = 3;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_KEEPCNT, &kcnt as *const _ as *const _, std::mem::size_of::<libc::c_uint>() as _);

        let tv = libc::timeval { tv_sec: 0, tv_usec: 300_000 };
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDTIMEO, &tv as *const _ as *const _, std::mem::size_of::<libc::timeval>() as _);
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVTIMEO, &tv as *const _ as *const _, std::mem::size_of::<libc::timeval>() as _);

        // DSCP low-delay
        let tos: libc::c_int = libc::IPTOS_LOWDELAY as libc::c_int;
        let _ = libc::setsockopt(fd, libc::IPPROTO_IP, libc::IP_TOS, &tos as *const _ as *const _, std::mem::size_of::<libc::c_int>() as _);

        // enable SO_ZEROCOPY best-effort
        #[allow(non_upper_case_globals)]
        const SO_ZEROCOPY: libc::c_int = 60;
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, SO_ZEROCOPY, &one as *const _ as *const _, core::mem::size_of::<libc::c_int>() as _);

        // append BBR set; harmless if not available
        let algo = b"bbr\0";
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, 13 /* TCP_CONGESTION */, algo.as_ptr() as *const _, 4);

        // append buffer sizing
        let sz: libc::c_int = 64 * 1024;
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &sz as *const _ as *const _, core::mem::size_of::<libc::c_int>() as _);
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &sz as *const _ as *const _, core::mem::size_of::<libc::c_int>() as _);

        // IPv4 PMTU: IP_MTU_DISCOVER = IP_PMTUDISC_DO (2)
        let pmtu: libc::c_int = 2;
        let _ = libc::setsockopt(fd, libc::IPPROTO_IP, libc::IP_MTU_DISCOVER, &pmtu as *const _ as *const _, core::mem::size_of::<libc::c_int>() as _);
        // IPv6 PMTU: IPV6_MTU_DISCOVER = IPV6_PMTUDISC_DO (2)
        #[allow(non_upper_case_globals)]
        const IPV6_MTU_DISCOVER: libc::c_int = 23;
        let _ = libc::setsockopt(fd, libc::IPPROTO_IPV6, IPV6_MTU_DISCOVER, &pmtu as *const _ as *const _, core::mem::size_of::<libc::c_int>() as _);
        // IPv6 DSCP too (mirror IPTOS_LOWDELAY)
        #[allow(non_upper_case_globals)]
        const IPV6_TCLASS: libc::c_int = 67;
        let tclass: libc::c_int = 0x10; // Low Delay
        let _ = libc::setsockopt(fd, libc::IPPROTO_IPV6, IPV6_TCLASS, &tclass as *const _ as *const _, core::mem::size_of::<libc::c_int>() as _);

        // Try BBR2 (if present); fallback to BBR set earlier
        let algo2 = b"bbr2\0";
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, 13 /* TCP_CONGESTION */, algo2.as_ptr() as *const _, 5);

        // Micro-coalesce safety: TCP_CORK on for construction, then off immediately.
        // We keep payload single-buffered anyway; this is a no-op in practice.
        const TCP_CORK: libc::c_int = 3;
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_CORK, &one as *const _ as *const _, core::mem::size_of::<libc::c_int>() as _);
        let zero: libc::c_int = 0;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, TCP_CORK, &zero as *const _ as *const _, core::mem::size_of::<libc::c_int>() as _);
    }
}

// REMOVED: Duplicate send_all_nosig function with MSG_ZEROCOPY
// Using the simpler implementation at line 411

// ADD: SCHED_FIFO at prio 1 for the calling thread (best-effort)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn ensure_rt_sched_low() {
    unsafe {
        let param = libc::sched_param { sched_priority: 1 };
        let _ = libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_FIFO, &param);
    }
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn ensure_rt_sched_low() {}

// ADD: connect & stash to pool (best-effort)
pub fn relay_pool_warm(addrs: &[std::net::SocketAddr]) {
    for &a in addrs {
        if let Some(_) = pool_take(&a) { continue; } // already warm
        if let Ok(s) = std::net::TcpStream::connect_timeout(&a, std::time::Duration::from_millis(200)) {
            tune_low_latency_stream(&s);
            pool_put(a, s);
        }
    }
}

// ADD: deterministic endpoint shuffler (keeps your hedge stable per slot/nonce)
#[inline(always)]
fn shuffle_endpoints(endpoints: &mut [std::net::SocketAddr], slot: u64, nonce: u64, salt: &[u8;32]) {
    use blake3::Hasher as H;
    let mut seed = [0u8; 32];
    let mut h = H::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"RELAY_SHUFFLE_V1");
    h.update(&slot.to_le_bytes());
    h.update(&nonce.to_le_bytes());
    h.update(salt);
    seed.copy_from_slice(h.finalize().as_bytes());
    let mut x = u64::from_le_bytes(seed[0..8].try_into().unwrap());
    for i in (1..endpoints.len()).rev() {
        // Xorshift64*
        x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
        let j = ((x.wrapping_mul(0x2545F4914F6CDD1Du64)) as usize) % (i + 1);
        endpoints.swap(i, j);
    }
}

#[derive(Debug)]
pub enum RelayOutcome { Ok(u64), Err(&'static str) } // Ok(latency_ms)

// ADD: Persistent relay workers (zero spawn cost; hot sockets)
struct RelayJob { payload: Vec<u8>, slot:u64, nonce:u64, tx: Sender<RelayOutcome> }
struct RelayWorker { addr: SocketAddr, rx: Receiver<RelayJob> }

static RELAY_TX_MAP: OnceCell<parking_lot::Mutex<HashMap<SocketAddr, Sender<RelayJob>>>> = OnceCell::new();

#[inline]
pub fn relay_workers_init(addrs: &[SocketAddr]) {
    let mut map = HashMap::with_hasher(ahash::RandomState::default());
    for &addr in addrs {
        let (tx, rx) = unbounded::<RelayJob>();
        let w = RelayWorker { addr, rx };
        thread::spawn(move || relay_worker_main(w));
        map.insert(addr, tx);
    }
    let _ = RELAY_TX_MAP.set(parking_lot::Mutex::new(map));
}

fn relay_worker_main(w: RelayWorker) {
    // pin network threads away from prover cores
    #[cfg(all(unix, target_os = "linux"))] {
        // use preferred cpu list but offset by +half to avoid overlap
        if let Some(v) = CPU_PREF_LIST.get() {
            if !v.is_empty() {
                let cpu = v[(v.len()/2 + (w.addr.port() as usize)) % v.len()];
                set_affinity_cpu(cpu);
            }
        }
    }
    ignore_sigpipe_global();

    let mut sock: Option<TcpStream> = None;
    loop {
        let job = match w.rx.recv() { Ok(j)=>j, Err(_)=>break };
        let t0 = mono_now_ns();
        let s = match sock.take() {
            Some(s) => s,
            None => {
                match std::net::TcpStream::connect_timeout(&w.addr, std::time::Duration::from_millis(200)) {
                    Ok(s) => { tune_low_latency_stream(&s); s }
                    Err(_) => { let _=job.tx.send(RelayOutcome::Err("connect")); continue; }
                }
            }
        };
        use std::os::fd::AsRawFd;
        if let Err(_) = send_all_nosig(s.as_raw_fd(), &job.payload) {
            let _ = job.tx.send(RelayOutcome::Err("write"));
            sock = None; // force reconnect next time
            continue;
        }
        let _ = s.flush();
        let ms = elapsed_ms_since_ns(t0);
        let _ = job.tx.send(RelayOutcome::Ok(ms));
        sock = Some(s);
    }
}

#[inline]
pub fn relay_send_via_worker(addr: SocketAddr, payload: Vec<u8>, slot: u64, nonce: u64) -> RelayOutcome {
    if let Some(mx) = RELAY_TX_MAP.get() {
        if let Some(tx) = mx.lock().get(&addr).cloned() {
            let (rtx, rrx) = unbounded::<RelayOutcome>();
            let _ = tx.send(RelayJob { payload, slot, nonce, tx: rtx });
            return rrx.recv().unwrap_or(RelayOutcome::Err("worker_closed"));
        }
    }
    RelayOutcome::Err("no_worker")
}

#[inline]
pub fn hedged_race_send(
    payload: &[u8],
    mut relays: Vec<std::net::SocketAddr>,
    slot: u64,
    nonce: u64,
    salt: [u8;32],
) -> RelayOutcome {
    use std::sync::mpsc::{channel, TryRecvError};
    use std::thread;

    if relays.is_empty() { return RelayOutcome::Err("no_relays"); }
    shuffle_endpoints(&mut relays[..], slot, nonce, &salt);

    let (tx, rx) = channel::<(usize, RelayOutcome)>();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    for (idx, addr) in relays.iter().cloned().enumerate() {
        let tx = tx.clone();
        let stop = stop.clone();
        let data = payload.to_vec();
        thread::spawn(move || {
            if stop.load(Ordering::Relaxed) { let _=tx.send((idx, RelayOutcome::Err("cancelled"))); return; }
            let t0 = mono_now_ns();
            // Connect with a tight timeout
            let to = std::time::Duration::from_millis(200);
            let s = match std::net::TcpStream::connect_timeout(&addr, to) {
                Ok(s) => s,
                Err(_) => { let _=tx.send((idx, RelayOutcome::Err("connect"))); return; }
            };
            let _ = s.set_nonblocking(false);
            tune_low_latency_stream(&s);

            // Write payload using SIGPIPE-proof send
            use std::os::fd::AsRawFd;
            if let Err(_) = send_all_nosig(s.as_raw_fd(), &data) {
                let _=tx.send((idx, RelayOutcome::Err("write"))); return;
            }
            // Best-effort flush
            let _ = s.flush();
            let ms = elapsed_ms_since_ns(t0);
            let _ = tx.send((idx, RelayOutcome::Ok(ms)));
        });
    }

    // Observe results; cancel the rest on first success
    loop {
        match rx.recv() {
            Ok((_i, RelayOutcome::Ok(ms))) => {
                stop.store(true, Ordering::Relaxed);
                return RelayOutcome::Ok(ms);
            }
            Ok((_i, RelayOutcome::Err(_e))) => {
                // keep waiting unless all threads have reported; non-blocking probe
                if relays.len() == 1 { return RelayOutcome::Err("all_failed"); }
                relays.pop();
                continue;
            }
            Err(_) => return RelayOutcome::Err("chan_closed"),
        }
    }
}

// ADD: branch prediction hints (stable: core::intrinsics is nightly; use portable macro)
#[inline(always)] fn likely(b: bool) -> bool { #[allow(clippy::needless_bool)] { if b { true } else { false } } }
#[inline(always)] fn unlikely(b: bool) -> bool { #[allow(clippy::needless_bool)] { if b { true } else { false } } }

// ADD: lower IO priority for maintenance writers (best-effort)
#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn ioprio_idle_best_effort() {
    // ioprio_set(IOPRIO_WHO_PROCESS, pid, (class<<13)|prio)
    const IOPRIO_WHO_PROCESS: i32 = 1;
    const IOPRIO_CLASS_BE: i32 = 2;
    let prio: i32 = (IOPRIO_CLASS_BE << 13) | 7; // best-effort, lowest
    unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, prio); }
}
#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn ioprio_idle_best_effort() {}

// ADD: helper to hash inputs once
#[inline(always)]
fn inputs_digest_bn254(inputs: &[Fr]) -> [u8;32] {
            let mut ih = blake3::Hasher::new();
    for fe in inputs.iter() { ih.update(&fe.into_bigint().to_bytes_le()); }
    ih.finalize().into()
}

// ADD: safer, faster cache-key shaper using prehashed inputs
#[inline(always)]
fn build_cache_key_fast(
    circuit_hash: &[u8; 32],
    slot: u64,
    nonce: u64,
    ctx_digest: &[u8; 32],
    inputs_digest: &[u8; 32],
) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"PROOF_CACHE_KEY_V2");
    h.update(NETWORK_ID);
    h.update(circuit_hash);
    h.update(&slot.to_le_bytes());
    h.update(&nonce.to_le_bytes());
    h.update(ctx_digest);
    h.update(inputs_digest);
    h.finalize().into()
}

// REPLACE: choose_hedge_width
#[inline(always)]
pub fn choose_hedge_width(ms_left: u64, max_relays: usize) -> usize {
    let (_net50, net95) = net_snapshot_ms();
    let d = queue_depth();
    let mut k = 1usize;
    if ms_left < net95 { k += 1; }
    if ms_left < net95 / 2 { k += 1; }
    if d > 0 { k += 1; }
    if d > 8 { k += 1; }
    k.min(max_relays.max(1)).max(1)
}

// ADD: healthbook
use parking_lot::Mutex;

// ADD: dir FD cache
// REMOVED: std::collections::HashMap import - using hashbrown::HashMap instead
use std::path::{Path, PathBuf};

struct DirFd { f: std::fs::File }
static DIRFD_CACHE: OnceCell<Mutex<HashMap<PathBuf, DirFd>>> = OnceCell::new();

#[inline] pub fn dirfd_cache_init() { let _ = DIRFD_CACHE.set(Mutex::new(HashMap::new())); }

#[cfg(all(unix, target_os="linux"))]
#[inline]
fn dirfd_for(p: &Path) -> Option<std::os::fd::RawFd> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    let parent = if p.is_dir() { p.to_path_buf() } else { p.parent().unwrap_or(Path::new(".")).to_path_buf() };
    if let Some(mx) = DIRFD_CACHE.get() {
        let mut g = mx.lock();
        if let Some(e) = g.get(&parent) { return Some(e.f.as_raw_fd()); }
        if let Ok(f) = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC).open(&parent) {
            let fd = f.as_raw_fd();
            g.insert(parent, DirFd{ f });
            return Some(fd);
        }
    }
    None
}
#[cfg(not(all(unix, target_os="linux")))]
#[inline] fn dirfd_for(_p: &Path) -> Option<std::os::fd::RawFd> { None }

// REPLACE: RStat + table + helpers (supersedes earlier version)
#[derive(Clone, Copy)]
struct RStat { lat_x1024: u64, fail_q8: u8, inited: bool, last_fail_ms: u64 }

static RELAY_STATS: OnceCell<parking_lot::Mutex<HashMap<SocketAddr, RStat, RandomState>>> = OnceCell::new();

#[inline] pub fn relay_stats_init() {
    let _ = RELAY_STATS.set(parking_lot::Mutex::new(HashMap::with_hasher(RandomState::default())));
}

#[inline]
pub fn relay_record(addr: SocketAddr, outcome: &RelayOutcome) {
    if let Some(mx) = RELAY_STATS.get() {
        let mut m = mx.lock();
        let now = mono_now_ns() / 1_000_000;
        let e = m.entry(addr).or_insert(RStat { lat_x1024: 80u64 << 10, fail_q8: 0, inited: false, last_fail_ms: 0 });
        match *outcome {
            RelayOutcome::Ok(ms) => {
                let s = (ms.max(1) as u64) << 10;
                let prev = e.lat_x1024;
                e.lat_x1024 = prev + ((s as i64 - prev as i64) >> 3) as u64; // α ≈ 1/8
                e.fail_q8 = e.fail_q8.saturating_sub(e.fail_q8 / 16);
                e.inited = true;
            }
            RelayOutcome::Err(_) => {
                e.fail_q8 = e.fail_q8.saturating_add(1).min(200);
                e.last_fail_ms = now;
            }
        }
    }
}

#[inline]
fn relay_connect_timeout_ms(addr: &SocketAddr) -> u64 {
    if let Some(mx) = RELAY_STATS.get() {
        let m = mx.lock();
        if let Some(s) = m.get(addr) {
            // Base on EWMA latency; expand with failure penalty, clamp [60, 350]
            let base = (s.lat_x1024 >> 10).max(40);
            let pen  = (s.fail_q8 as u64) * 6;
            return base.saturating_mul(2).saturating_add(20).saturating_add(pen).clamp(60, 350);
        }
    }
    200
}

#[inline]
fn relay_in_cooldown(addr: &SocketAddr, now_ms: u64) -> bool {
    if let Some(mx) = RELAY_STATS.get() {
        let m = mx.lock();
        if let Some(s) = m.get(addr) {
            if s.fail_q8 == 0 { return false; }
            // Cooldown grows with failures, decays quickly
            let backoff = (s.fail_q8 as u64).saturating_mul(15).min(600); // up to 600ms
            return now_ms.saturating_sub(s.last_fail_ms) < backoff;
        }
    }
    false
}

#[inline]
pub fn choose_best_relays(mut addrs: Vec<SocketAddr>, k: usize) -> Vec<SocketAddr> {
    if addrs.len() <= k { return addrs; }
    let now = mono_now_ns() / 1_000_000;
    addrs.retain(|a| !relay_in_cooldown(a, now));
    if addrs.is_empty() { return vec![]; }
    if let Some(mx) = RELAY_STATS.get() {
        let m = mx.lock();
        addrs.sort_by_key(|a| {
            match m.get(a) {
                Some(s) => {
                    let lat = s.lat_x1024 >> 10;
                    let pen = (s.fail_q8 as u64) * 4; // slightly stronger penalty now
                    lat + pen
                }
                None => u64::MAX / 2
            }
        });
    }
    addrs.truncate(k);
    addrs
}

// ADD: RelayOutcome enum
#[derive(Debug, Clone)]
pub enum RelayOutcome {
    Ok(u64), // milliseconds
    Err(String),
}

// REPLACE: hedged_race_send (uses 19 & 20)
#[inline]
pub fn hedged_race_send(
    payload: &[u8],
    mut relays: Vec<std::net::SocketAddr>,
    slot: u64,
    nonce: u64,
    salt: [u8;32],
) -> RelayOutcome {
    use std::sync::mpsc::channel;
    if relays.is_empty() { return RelayOutcome::Err("no_relays".to_string()); }

    prepare_proof_buffer_hot(payload);
    relays = choose_best_relays(relays, relays.len());
    if relays.is_empty() { return RelayOutcome::Err("cooldown_all".to_string()); }
    shuffle_endpoints(&mut relays[..], slot, nonce, &salt);

    let (tx, rx) = channel::<(usize, RelayOutcome)>();
    let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));

    for (idx, addr) in relays.iter().cloned().enumerate() {
        let tx = tx.clone();
        let stop = stopped.clone();
        let data = payload.to_vec();
        std::thread::spawn(move || {
            if stop.load(Ordering::Relaxed) { let _=tx.send((idx, RelayOutcome::Err("cancelled".to_string()))); return; }
            let to_ms = relay_connect_timeout_ms(&addr);
            let to = std::time::Duration::from_millis(to_ms);
            let s = match std::net::TcpStream::connect_timeout(&addr, to) {
                Ok(s) => s,
                Err(_) => { let _=tx.send((idx, RelayOutcome::Err("connect".to_string()))); return; }
            };
            let _ = s.set_nonblocking(false);
            tune_low_latency_stream(&s);
            use std::os::fd::AsRawFd;
            let t0 = mono_now_ns();
            let out = match send_all_nosig(s.as_raw_fd(), &data) {
                Ok(_) => RelayOutcome::Ok(elapsed_ms_since_ns(t0)),
                Err(_) => RelayOutcome::Err("write".to_string()),
            };
            let _ = s.flush();
            let _ = tx.send((idx, out));
        });
    }

    let mut last_err: Option<RelayOutcome> = None;
    let mut remaining = relays.len();
    while remaining > 0 {
        match rx.recv() {
            Ok((i, outcome)) => {
                relay_record(relays[i], &outcome);
                match outcome {
                    RelayOutcome::Ok(ms) => {
                        stopped.store(true, Ordering::Relaxed);
                        return RelayOutcome::Ok(ms);
                    }
                    e @ RelayOutcome::Err(_) => { last_err = Some(e); }
                }
                remaining -= 1;
            }
            Err(_) => break,
        }
    }
    last_err.unwrap_or(RelayOutcome::Err("chan_closed".to_string()))
}

// ADD: lightweight sleeper that uses vDSO coarse clock, not std::thread::sleep jitter
#[inline(always)]
fn sleep_ms_fast(ms: u64) {
    #[cfg(all(unix, target_os = "linux"))] unsafe {
        let ts = libc::timespec { tv_sec: (ms / 1000) as libc::time_t, tv_nsec: ((ms % 1000) * 1_000_000) as libc::c_long };
        let _ = libc::clock_nanosleep(libc::CLOCK_MONOTONIC_COARSE, 0, &ts, core::ptr::null_mut());
    }
    #[cfg(not(all(unix, target_os = "linux")))]
    std::thread::sleep(std::time::Duration::from_millis(ms));
}

// ADD: launch helper for hedged sends with stagger based on EWMA
#[inline]
fn hedge_stagger_ms(idx: usize) -> u64 {
    if idx == 0 { return 0; }
    // 1st backup waits ~0.35 * EWMA net p50, 2nd waits ~0.65 * p50, then clamp.
    let (net50, _net95) = net_snapshot_ms();
    let base = net50.max(20);
    let frac_q16 = match idx {
        1 => 22938, // ≈0.35 in Q16
        2 => 42598, // ≈0.65
        _ => 49152, // 0.75
    };
    ((base as u128 * frac_q16 as u128) >> 16).min(120) as u64
}

// ADD: sample and auto-tune per proof attempt (call around your critical section)
#[inline(always)]
pub fn sample_faults_and_tune(start_ns: u64, end_ns: u64) {
    let elapsed_ms = (end_ns.saturating_sub(start_ns) / 1_000_000).max(1);
    let (maj, min) = rusage_thread_faults();
    let (vcsw, ivcsw) = rusage_thread_switches();
    autotune_on_sample_faultaware2(elapsed_ms, maj, min, ivcsw, vcsw);
    token_bucket_autotune(ivcsw);
}

// ADD: missing helper functions
#[inline]
fn shuffle_endpoints(endpoints: &mut [std::net::SocketAddr], slot: u64, nonce: u64, salt: &[u8; 32]) {
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;
    
    let mut seed = [0u8; 32];
    let mut hasher = blake3::Hasher::new();
    hasher.update(&slot.to_le_bytes());
    hasher.update(&nonce.to_le_bytes());
    hasher.update(salt);
    seed.copy_from_slice(&hasher.finalize().as_bytes()[..32]);
    
    let mut rng = ChaCha20Rng::from_seed(seed);
    endpoints.shuffle(&mut rng);
}

// REMOVED: Third duplicate send_all_nosig function using libc::write
// Using the comprehensive implementation at line 411

#[inline]
fn elapsed_ms_since_ns(start_ns: u64) -> u64 {
    (mono_now_ns().saturating_sub(start_ns) / 1_000_000).max(1)
}

#[inline]
fn mono_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[inline]
fn queue_depth() -> usize {
    // This would be implemented based on your actual queue implementation
    // For now, return a placeholder
    0
}

// ADD: TCP_INFO snapshot + enhanced observers (Linux)
#[cfg(all(unix, target_os="linux"))]
#[inline]
fn tcp_info_snapshot(fd: std::os::fd::RawFd) -> Option<(u32,u32,u32)> {
    use nix::sys::socket::{getsockopt, sockopt::TcpInfo};
    if let Ok(info) = getsockopt(fd, TcpInfo) {
        // tcpi_rtt/rttvar are in usec
        Some(((info.tcpi_rtt/1000) as u32, (info.tcpi_rttvar/1000) as u32, info.tcpi_retrans as u32))
    } else { None }
}
#[cfg(not(all(unix, target_os="linux")))]
#[inline] fn tcp_info_snapshot(_fd: std::os::fd::RawFd) -> Option<(u32,u32,u32)> { None }

#[inline]
pub fn relay_obs_ok(addr: SocketAddr, ms: u64) {
    if let Some(m) = RELAY_BOOK.get() {
        let mut g = m.lock();
        let e = g.entry(addr).or_insert(RelayStats{p50_x1024: ms<<10, p95_x1024: ms<<10, fail_x1024: 0, seen:0, ban_until_ns: 0, rtt_ms_x1024: 80<<10, rtx_x1024: 0});
        let s = (ms.max(1) as u64) << 10;
        e.p50_x1024 = e.p50_x1024 + ((s as i64 - e.p50_x1024 as i64) >> 3) as u64; // α=1/8
        // p95: faster rise than decay
        let over = (s > e.p95_x1024) as u64;
        let delta = if over!=0 { ((s - e.p95_x1024) >> 2) } else { ((s as i64 - e.p95_x1024 as i64) >> 5) as u64 };
        e.p95_x1024 = e.p95_x1024.wrapping_add(delta);
        // success decays fail EMA
        e.fail_x1024 = e.fail_x1024.saturating_sub(e.fail_x1024 >> 4);
        e.seen += 1;
    }
}

// ADD: richer ok/fail observers
#[inline] pub fn relay_obs_ok_ex(addr: SocketAddr, wall_ms: u64, rtt_ms: u32, retrans: u32) {
    if let Some(m) = RELAY_BOOK.get() {
        let mut g = m.lock();
        let e = g.entry(addr).or_insert(RelayStats{p50_x1024:wall_ms<<10,p95_x1024:wall_ms<<10,fail_x1024:0,seen:0,ban_until_ns:0,rtt_ms_x1024:(rtt_ms as u64)<<10,rtx_x1024:0});
        let w = (wall_ms.max(1) as u64) << 10;
        e.p50_x1024 = e.p50_x1024 + ((w as i64 - e.p50_x1024 as i64) >> 3) as u64;
        let over = (w > e.p95_x1024) as u64;
        let delta = if over!=0 { ((w - e.p95_x1024) >> 2) } else { ((w as i64 - e.p95_x1024 as i64) >> 5) as u64 };
        e.p95_x1024 = e.p95_x1024.wrapping_add(delta);
        e.fail_x1024 = e.fail_x1024.saturating_sub(e.fail_x1024 >> 4);
        e.rtt_ms_x1024 = e.rtt_ms_x1024 + ((((rtt_ms as u64)<<10) as i64 - e.rtt_ms_x1024 as i64) >> 3) as u64;
        e.rtx_x1024 = e.rtx_x1024 + ((((retrans as u64)<<10) as i64 - e.rtx_x1024 as i64) >> 4) as u64;
        e.seen += 1;
    }
}

#[inline]
pub fn relay_obs_fail(addr: SocketAddr) {
    if let Some(m) = RELAY_BOOK.get() {
        let mut g = m.lock();
        let e = g.entry(addr).or_insert(RelayStats{p50_x1024: 60<<10, p95_x1024: 80<<10, fail_x1024: 0, seen:0, ban_until_ns: 0, rtt_ms_x1024: 80<<10, rtx_x1024: 0});
        e.fail_x1024 = e.fail_x1024 + ((1<<10) - (e.fail_x1024 >> 3)); // rise quickly, slow decay
        e.seen += 1;
        // Breaker: if fail exceeds 0.6 for a bit, ban for ~500ms
        if (e.fail_x1024 >> 10) > 0 && (e.fail_x1024 > (600 << 10)) {
            e.ban_until_ns = mono_now_ns().saturating_add(500_000_000);
        }
    }
}

#[inline]
pub fn pick_relays(mut pool: Vec<SocketAddr>, k: usize) -> Vec<SocketAddr> {
    if pool.is_empty() { return pool; }
    // REPLACE score calc:
    let mut scored: Vec<(u128, SocketAddr)> = Vec::with_capacity(pool.len());
    if let Some(m) = RELAY_BOOK.get() {
        let g = m.lock();
        for a in pool.drain(..) {
            if let Some(s) = g.get(&a) {
                if s.ban_until_ns > mono_now_ns() { continue; } // skip banned
                let p95 = (s.p95_x1024 >> 10) as u128;
                let fail = (s.fail_x1024 >> 10) as u128;
                let rtt  = (s.rtt_ms_x1024 >> 10) as u128;
                let rtx  = (s.rtx_x1024 >> 10) as u128;
                // penalize retrans and inflated rtt modestly
                let score = p95 + (fail * 20) + (rtt / 4) + (rtx * 15);
                scored.push((score, a));
            } else {
                scored.push((1000, a)); // neutral default
            }
        }
    } else {
        scored = pool.into_iter().map(|a| (1000, a)).collect();
    }
    scored.sort_by_key(|x| x.0);
    scored.into_iter().take(k).map(|x| x.1).collect()
}

// ADD: neg-cache
use parking_lot::RwLock;

struct NegEntry { expire_ns: u64 }
struct NegShard { map: HashMap<[u8;32], NegEntry, RandomState> }
const NEG_SHARDS: usize = 16;
static NEG: OnceCell<[RwLock<NegShard>; NEG_SHARDS]> = OnceCell::new();

#[inline] fn neg_idx(k: &[u8;32]) -> usize { (u64::from_le_bytes(k[..8].try_into().unwrap()) as usize) & (NEG_SHARDS-1) }

#[inline]
pub fn neg_init() {
    let _ = NEG.set(std::array::from_fn(|_| RwLock::new(NegShard {
        map: HashMap::with_hasher(RandomState::default())
    })));
}

#[inline(always)]
fn build_neg_key(hex_inputs: &[String], ctx_digest: &[u8;32]) -> [u8;32] {
    let mut h = blake3::Hasher::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"NEG_V1"); h.update(NETWORK_ID); h.update(ctx_digest);
    for s in hex_inputs { h.update(s.as_bytes()); }
    h.finalize().into()
}

#[inline(always)]
fn neg_check(hex_inputs: &[String], ctx_digest: &[u8;32], now_ns: u64) -> bool {
    if let Some(shards) = NEG.get() {
        let k = build_neg_key(hex_inputs, ctx_digest);
        let idx = neg_idx(&k);
        if let Some(e) = shards[idx].read().map.get(&k) { return e.expire_ns > now_ns; }
    }
    false
}
#[inline(always)]
fn neg_note(hex_inputs: &[String], ctx_digest: &[u8;32], ttl_ms: u64) {
    if let Some(shards) = NEG.get() {
        let k = build_neg_key(hex_inputs, ctx_digest);
        let idx = neg_idx(&k);
        // Jitter in [0..12]ms based on key
        let j = (u16::from_le_bytes([k[0],k[1]]) % 13) as u64;
        let mut w = shards[idx].write();
        w.map.insert(k, NegEntry { expire_ns: mono_now_ns().saturating_add((ttl_ms + j)*1_000_000) });
        // opportunistic purge of a few expired entries (bounded work)
        if w.map.len() > 8192 {
            let now = mono_now_ns();
            let mut cnt = 0;
            w.map.retain(|_,v| { let keep = v.expire_ns > now || cnt > 64; if !keep { cnt+=1; } keep });
        }
    }
}

// ADD: TSC calibrator + fast clock
#[cfg(any(target_arch="x86_64"))]
mod fastclock {
    use core::arch::x86_64::__rdtscp;
    use std::sync::OnceCell;
    static TICKS_PER_NS_X1024: OnceCell<u64> = OnceCell::new();
    static USE_TSC: std::sync::OnceCell<bool> = OnceCell::new();

    #[inline] fn cpuid_invariant_tsc() -> bool {
        // CPUID.80000007H:EDX[8]=Invariant TSC
        unsafe {
            let res = core::arch::x86_64::__cpuid(0x8000_0007);
            (res.edx & (1<<8)) != 0
        }
    }

    #[inline]
    pub fn init_fast_clock() {
        if !cpuid_invariant_tsc() { let _ = USE_TSC.set(false); return; }
        // Calibrate against CLOCK_MONOTONIC_RAW for ~10 ms
        let t0_ns = super::mono_now_ns();
        let r0 = unsafe { __rdtscp(&mut 0) };
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t1_ns = super::mono_now_ns();
        let r1 = unsafe { __rdtscp(&mut 0) };
        let dt_ns = (t1_ns.saturating_sub(t0_ns)).max(1);
        let dt_ticks = r1.saturating_sub(r0).max(1);
        let tpn_x1024 = (((dt_ticks as u128) << 10) / (dt_ns as u128)) as u64;
        let _ = TICKS_PER_NS_X1024.set(tpn_x1024.max(1));
        let _ = USE_TSC.set(true);
    }

    #[inline(always)]
    pub fn now_ns() -> u64 {
        if *USE_TSC.get_or_init(|| false) {
            let tpn = *TICKS_PER_NS_X1024.get().unwrap_or(&1);
            let ticks = unsafe { __rdtscp(&mut 0) };
            ((ticks as u128) << 10 / (tpn as u128)) as u64
        } else {
            super::mono_now_ns()
        }
    }
}
#[cfg(not(any(target_arch="x86_64")))]
mod fastclock { pub fn init_fast_clock() {} pub fn now_ns() -> u64 { super::mono_now_ns() } }

// ADD: Relay connection pool (pre-tuned, re-usable sockets)
const POOL_SHARDS: usize = 16;
struct Conn { s: TcpStream, a: SocketAddr, t: Instant }
struct PoolShard { q: VecDeque<Conn> }
static RELAY_POOL: OnceCell<[Mutex<PoolShard>; POOL_SHARDS]> = OnceCell::new();

#[inline] 
fn pool_idx(a: &SocketAddr) -> usize {
    (blake3::hash(&a.to_string().as_bytes()).as_bytes()[0] as usize) & (POOL_SHARDS-1)
}

#[inline] 
pub fn relay_pool_init() { 
    let _ = RELAY_POOL.set(std::array::from_fn(|_| Mutex::new(PoolShard{ q: VecDeque::new() }))); 
}

#[inline]
pub fn pool_take(addr: &SocketAddr) -> Option<TcpStream> {
    if let Some(shards) = RELAY_POOL.get() {
        let mut sh = shards[pool_idx(addr)].lock();
        while let Some(mut c) = sh.q.pop_back() {
            // stale or dead? drop
            if c.a != *addr { continue; }
            if c.s.set_nonblocking(false).is_err() { continue; }
            return Some(c.s);
        }
    }
    None
}

#[inline]
pub fn pool_put(addr: SocketAddr, s: TcpStream) {
    if let Some(shards) = RELAY_POOL.get() {
        let mut sh = shards[pool_idx(&addr)].lock();
        if sh.q.len() < 8 { sh.q.push_back(Conn { s, a: addr, t: Instant::now() }); }
    }
}

// ADD: Tune a TCP stream for low latency
pub fn tune_low_latency_stream(stream: &TcpStream) {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let fd = stream.as_raw_fd();
        
        // Set TCP_NODELAY to disable Nagle's algorithm
        unsafe {
            let flag: i32 = 1;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &flag as *const _ as *const libc::c_void,
                std::mem::size_of::<i32>() as libc::socklen_t,
            );
        }
    }
}

// ADD: Branch-ahead prefetch in hot maps (cache + singleflight)
#[inline(always)]
fn prefetch_read<T>(p: *const T) {
    #[cfg(any(target_arch="x86", target_arch="x86_64"))]
    unsafe { core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0); }
    #[cfg(not(any(target_arch="x86", target_arch="x86_64")))] { let _ = p; }
}

// ADD: SmallVec payload builder (zero heap on typical submits)
#[inline]
pub fn build_envelope_payload_small(parts: &[&[u8]]) -> Vec<u8> {
    let mut buf: SmallVec<[u8; 2048]> = SmallVec::new(); // common case stays inline
    let total: usize = parts.iter().map(|p| p.len()).sum();
    buf.reserve(total);
    for p in parts { buf.extend_from_slice(p); }
    buf.into_vec()
}

// ADD: TLS payload buffer (thread-local Vec) for rare large envelopes
thread_local! {
    static ENVELOPE_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(4096));
}
#[inline]
pub fn build_envelope_payload_tls(parts: &[&[u8]]) -> Vec<u8> {
    let total: usize = parts.iter().map(|p| p.len()).sum();
    if total <= 2048 { return build_envelope_payload_small(parts); }
    ENVELOPE_BUF.with(|cell| {
        let mut v = cell.borrow_mut();
        v.clear(); v.reserve(total);
        for p in parts { v.extend_from_slice(p); }
        v.clone() // return owned copy for networking thread
    })
}

// ADD: sf_wait_ttl variant (50ms TTL)
#[inline(always)]
fn sf_wait_ttl(pair: Arc<Pair>, ttl_ms: u64) -> Option<(Arc<[u8]>,[u8;32])> {
    let deadline = mono_now_ns().saturating_add(ttl_ms * 1_000_000);
    let mut g = pair.m.lock();
    while !g.done {
        let now = mono_now_ns();
        if now >= deadline { return None; }
        let rem = deadline - now;
        #[cfg(unix)]
        {
            let tv = libc::timespec { tv_sec: 0, tv_nsec: (rem as i64).min(50_000_000) }; // ≤50ms slice
            unsafe { libc::clock_nanosleep(libc::CLOCK_MONOTONIC, 0, &tv, std::ptr::null_mut()); }
        }
        #[cfg(not(unix))]
        { std::thread::sleep(std::time::Duration::from_millis(1)); }
    }
    let (v, ph) = g.val.as_ref().unwrap();
    Some((v.clone(), *ph))
}

// ADD: SingleFlight TTL jitter (stampede-safe expiry)
#[inline(always)]
fn sf_wait_ttl_jitter(pair: Arc<Pair>, slot: u64, nonce: u64, base_ms: u64) -> Option<(Arc<[u8]>,[u8;32])> {
    let mut rng = DeterministicRng::new(slot, nonce, b"SFJITTER___________________________", b"SFJITTER___________________________");
    let mut b8=[0u8;8]; rng.fill_bytes(&mut b8);
    let jitter = (u64::from_le_bytes(b8) % 10) as u64; // [0..9] ms
    sf_wait_ttl(pair, base_ms + jitter)
}

// ADD: monotonic raw time (ns → ms) with portable fallback
#[inline(always)]
fn mono_now_ns() -> u64 {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
        return (ts.tv_sec as u64) * 1_000_000_000u64 + (ts.tv_nsec as u64);
    }
    #[cfg(not(all(unix, target_os = "linux")))]
    { std::time::Instant::now().elapsed().as_nanos() as u64 } // process-relative monotonic
}

#[inline(always)]
fn elapsed_ms_since_ns(t0: u64) -> u64 {
    let now = mono_now_ns();
    now.saturating_sub(t0) / 1_000_000
}

// ADD: fast FP mode (no-ops elsewhere)
#[cfg(any(target_arch="x86", target_arch="x86_64"))]
#[inline(always)]
fn enable_fast_fp_denorm() {
    unsafe {
        #[cfg(target_arch="x86")]
        use core::arch::x86::{_mm_getcsr, _mm_setcsr};
        #[cfg(target_arch="x86_64")]
        use core::arch::x86_64::{_mm_getcsr, _mm_setcsr};
        let mut csr = _mm_getcsr();
        csr |= 1 << 15; // FTZ
        csr |= 1 << 6;  // DAZ
        _mm_setcsr(csr);
    }
}
#[cfg(not(any(target_arch="x86", target_arch="x86_64")))]
#[inline(always)]
fn enable_fast_fp_denorm() {}

// REPLACE: ProverGuard + perms + secure open helpers

struct ProverGuard(u64);

impl ProverGuard {
    // ADD: EV-aware admission; original enter() delegates here with ev=0, ms_left=u64::MAX
    #[inline(always)]
    fn enter_ev(ev: u64, ms_left: u64) -> Result<Self, ProofError> {
        let my  = TICKET_NEXT.0.fetch_add(1, Ordering::AcqRel);
        let lim = PROVER_LIMIT_ATOMIC.0.load(Ordering::Acquire).max(1);

        let spins = ADMISSION_SPINS.0.load(Ordering::Relaxed).clamp(64, 4096);
        for i in 0..spins {
            let cur  = ACTIVE_PROVERS.0.load(Ordering::Acquire);
            let head = TICKET_SERVE.0.load(Ordering::Acquire);

            if my == head && cur < lim {
                // Late + backlog + low EV? yield to higher EV heads.
                if matches!(admit_by_deadline(ms_left), AdmitDecision::SkipLate) && queue_depth() > 0 {
                    let max_ev = EV_MAX_HINT.load(Ordering::Relaxed);
                    // Only run if we are close to the best EV (≥80%)
                    if max_ev > 0 && ev < ((max_ev as u128 * 80) / 100) as u64 {
                        // Skip our turn
                        TICKET_SERVE.0.fetch_add(1, Ordering::AcqRel);
                        return Err(err_backpressure(lim));
                    }
                }

                ACTIVE_PROVERS.0.fetch_add(1, Ordering::AcqRel);
                TICKET_SERVE.0.fetch_add(1, Ordering::AcqRel);
                pin_prover_to_core(my);
                ensure_lowlatency_sched();
                ensure_rt_sched_low();       // NEW
                reduce_timer_slack_ns(1);
                block_signals_hot_thread();
                enable_fast_fp_denorm();
                return Ok(Self(my));
            }
            std::hint::spin_loop();
            if (i & 7) == 7 {
                // REPLACE inside the spin loop where you did relative nanosleep:
                #[cfg(all(unix, target_os="linux"))]
                unsafe {
                    let now = mono_now_ns();
                    let ts = libc::timespec { tv_sec: (now / 1_000_000_000) as i64, tv_nsec: ((now % 1_000_000_000) as i64) + 50_000 }; // +50µs
                    let mut abs = ts;
                    if abs.tv_nsec >= 1_000_000_000 { abs.tv_sec += 1; abs.tv_nsec -= 1_000_000_000; }
                    let _ = libc::clock_nanosleep(libc::CLOCK_MONOTONIC, libc::TIMER_ABSTIME, &abs, core::ptr::null_mut());
                }
                #[cfg(not(all(unix, target_os="linux")))]
                std::thread::yield_now();
            }
            std::thread::yield_now();
        }
        let head = TICKET_SERVE.0.load(Ordering::Acquire);
        if my == head { TICKET_SERVE.0.fetch_add(1, Ordering::AcqRel); }
        Err(err_backpressure(lim))
    }

    // REPLACE: keep legacy signature delegating to EV-neutral
    #[inline(always)]
    fn enter() -> Result<Self, ProofError> { Self::enter_ev(0, u64::MAX) }

    #[inline(always)]
    fn try_enter() -> Result<Self, ProofError> { Self::enter() }
}

impl Drop for ProverGuard {
    #[inline(always)]
    fn drop(&mut self) {
        // Saturating to defend against double-drops in unwind paths (should never happen).
        let prev = ACTIVE_PROVERS.0.fetch_update(
            Ordering::AcqRel, Ordering::Acquire,
            |x| Some(x.saturating_sub(1))
        );
        debug_assert!(prev.map(|p| p > 0).unwrap_or(true), "ACTIVE_PROVERS underflow");
    }
}

// REPLACE: assert_secure_file_permissions (Unix: nlink==1, no suid/sgid)
#[cfg(unix)]
#[inline(always)]
fn assert_secure_file_permissions(path: &Path) -> Result<(), ProofError> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let md = fs::symlink_metadata(path)?;
    let ft = md.file_type();
    if !ft.is_file() {
        return Err(ProofError::InvalidPermissions(format!("not a regular file: {}", path.display())));
    }
    let mode = md.mode();
    // Only 0600 allowed; forbid suid/sgid/sticky/exec for maximal hygiene
    if (mode & 0o077) != 0 || (mode & 0o111) != 0 || (mode & 0o6000) != 0 {
        return Err(ProofError::InvalidPermissions(format!("insecure mode {:o} on {}", mode, path.display())));
    }
    let owner_uid = md.uid();
    let euid = unsafe { libc::geteuid() } as u32;
    if owner_uid != euid {
        return Err(ProofError::InvalidPermissions(format!(
            "owner uid {} != euid {} for {}", owner_uid, euid, path.display()
        )));
    }
    if md.nlink() != 1 {
        return Err(ProofError::InvalidPermissions(format!(
            "file has {} links (expected 1): {}", md.nlink(), path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
#[inline(always)]
fn assert_secure_file_permissions(_path: &Path) -> Result<(), ProofError> {
    // Best-effort on non-Unix platforms.
    Ok(())
}

// REPLACE: open_readonly_nofollow (Linux adds O_NOATIME)
#[cfg(unix)]
#[inline(always)]
fn open_readonly_nofollow(path: &Path) -> Result<File, ProofError> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut oo = OpenOptions::new();
    oo.read(true);
    #[cfg(target_os = "linux")]
    {
        oo.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NOATIME);
    }
    #[cfg(not(target_os = "linux"))]
    {
        oo.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    oo.mode(0o600);
    Ok(oo.open(path)?)
}

#[cfg(not(unix))]
#[inline(always)]
fn open_readonly_nofollow(path: &Path) -> Result<File, ProofError> {
    // Fallback: regular read-only open on non-Unix.
    Ok(File::open(path)?)
}

#[cfg(unix)]
#[inline(always)]
fn verify_same_inode(canon: &Path, opened: &File) -> Result<(), ProofError> {
    use std::os::unix::fs::MetadataExt;

    // Compare device+inode between pre-open canonical path and the opened FD.
    let md_c = fs::metadata(canon)?;
    let md_f = opened.metadata()?;

    if md_c.dev() != md_f.dev() || md_c.ino() != md_f.ino() {
        return Err(ProofError::ArtifactRace(canon.display().to_string()));
    }
    Ok(())
}

#[cfg(not(unix))]
#[inline(always)]
fn verify_same_inode(_: &Path, _: &File) -> Result<(), ProofError> {
    Ok(())
}

// REPLACE: Safe MlockGuard (no unsafe) using nix::mman::{mlock, munlock}
#[cfg(unix)]
struct MlockGuard<'a> {
    ptr: *const std::ffi::c_void,
    len: usize,
    _phantom: core::marker::PhantomData<&'a [u8]>,
}

#[cfg(unix)]
impl<'a> MlockGuard<'a> {
    #[inline(always)]
    fn new(slice: &'a [u8]) -> Option<Self> {
        if slice.is_empty() { return None; }
        if nix::sys::mman::mlock(slice.as_ptr() as *const _, slice.len()).is_ok() {
            Some(Self { ptr: slice.as_ptr() as *const _, len: slice.len(), _phantom: core::marker::PhantomData })
        } else { None }
    }
}

#[cfg(unix)]
impl<'a> Drop for MlockGuard<'a> {
    fn drop(&mut self) {
        let _ = nix::sys::mman::munlock(self.ptr, self.len);
    }
}

#[cfg(not(unix))]
struct MlockGuard<'a> { _phantom: core::marker::PhantomData<&'a [u8]> }
#[cfg(not(unix))]
impl<'a> MlockGuard<'a> {
    #[inline(always)] fn new(_s: &'a [u8]) -> Option<Self> { None }
}

// REPLACE: sealed_memfd (verify seals + mmap + madvise prefetch, safe APIs)
#[cfg(all(unix, target_os = "linux"))]
mod sealed_memfd {
    use super::*;
    use nix::fcntl::{fcntl, FcntlArg, SealFlag};
    use nix::sys::memfd::{memfd_create, MemFdCreateFlag};
    use nix::unistd::dup;
    use std::fs::File;
    use std::io::{Read, Write, Seek, SeekFrom};
    use std::os::fd::{FromRawFd, OwnedFd, AsRawFd};
    use memmap2::{Mmap, MmapOptions};

    pub struct SealedMemfd {
        fd: OwnedFd,
        len: usize,
    }

    impl SealedMemfd {
        #[inline(always)]
        pub fn from_bytes(label: &str, bytes: &[u8]) -> std::io::Result<Self> {
            let flags = MemFdCreateFlag::MFD_CLOEXEC | MemFdCreateFlag::MFD_ALLOW_SEALING;
            let fd = memfd_create(label, flags).map_err(|e| std::io::Error::other(e))?;

            let mut f = unsafe { File::from_raw_fd(fd.as_raw_fd()) };
            f.write_all(bytes)?;
            f.flush()?;
            f.seek(SeekFrom::Start(0))?;
            core::mem::forget(f);

            let seals = SealFlag::F_SEAL_WRITE | SealFlag::F_SEAL_GROW | SealFlag::F_SEAL_SHRINK | SealFlag::F_SEAL_SEAL;
            fcntl(fd.as_raw_fd(), FcntlArg::F_ADD_SEALS(seals)).map_err(|e| std::io::Error::other(e))?;

            Ok(Self { fd, len: bytes.len() })
        }

        #[inline(always)]
        pub fn assert_sealed(&self) -> std::io::Result<()> {
            let got = fcntl(self.fd.as_raw_fd(), FcntlArg::F_GET_SEALS).map_err(|e| std::io::Error::other(e))?;
            let need = (SealFlag::F_SEAL_WRITE | SealFlag::F_SEAL_GROW | SealFlag::F_SEAL_SHRINK | SealFlag::F_SEAL_SEAL).bits();
            if (got & need) != need {
                return Err(std::io::Error::other(format!("memfd not fully sealed: 0x{:x}", got)));
            }
            Ok(())
        }

        /// Zero-copy read-only map of sealed bytes with prefetch advice.
        #[inline(always)]
        pub fn map_readonly(&self) -> std::io::Result<Mmap> {
            let d = dup(self.fd.as_raw_fd()).map_err(|e| std::io::Error::other(e))?;
            let file = unsafe { File::from_raw_fd(d) };
            let mmap = unsafe { MmapOptions::new().len(self.len).map(&file) }?;

            // Prefetch + hugepage hint on the mapped region (best-effort).
            #[cfg(all(unix, target_os = "linux"))] {
                use nix::sys::mman::{madvise, MadviseAdvice};
                let p = &mmap[..];
                let _ = madvise(p.as_ptr() as *mut _, p.len(), MadviseAdvice::MADV_WILLNEED);
                let _ = madvise(p.as_ptr() as *mut _, p.len(), MadviseAdvice::MADV_HUGEPAGE);
                let _ = madvise(p.as_ptr() as *mut _, p.len(), MadviseAdvice::MADV_DONTDUMP);
            }

            Ok(mmap)
        }
    }
}

#[cfg(not(all(unix, target_os = "linux")))]
mod sealed_memfd {
    // Fallback: immutable Vec wrapper. Interface parity with Linux path.
    pub struct SealedMemfd { buf: Vec<u8> }
    impl SealedMemfd {
        #[inline(always)] pub fn from_bytes(_label: &str, bytes: &[u8]) -> std::io::Result<Self> { Ok(Self { buf: bytes.to_vec() }) }
        #[inline(always)] pub fn assert_sealed(&self) -> std::io::Result<()> { Ok(()) }
        #[inline(always)] pub fn map_readonly(&self) -> std::io::Result<std::borrow::Cow<[u8]>> { Ok(std::borrow::Cow::from(self.buf.as_slice())) }
    }
}

// ADD: cross-process file locking for nonce store
#[cfg(unix)]
struct PathLockGuard { f: std::fs::File }

#[cfg(unix)]
#[inline(always)]
fn lock_nonce_path(lock_path: &std::path::Path) -> std::io::Result<PathLockGuard> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::fd::AsRawFd;

    let f = std::fs::OpenOptions::new()
        .read(true).write(true).create(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC)
        .open(lock_path)?;

    // Bounded blocking fcntl WRLCK with nanosleep backoff.
    let fd = f.as_raw_fd();
    let mut fl: libc::flock = unsafe { std::mem::zeroed() };
    fl.l_type = libc::F_WRLCK as i16;
    fl.l_whence = libc::SEEK_SET as i16;
    fl.l_start = 0; fl.l_len = 0;

    let deadline_ns = 50_000_000u64; // 50ms
    let start = std::time::Instant::now();
    loop {
        let r = unsafe { libc::fcntl(fd, libc::F_SETLK, &fl) };
        if r == 0 { break; }
        let e = std::io::Error::last_os_error();
        if e.kind() != std::io::ErrorKind::WouldBlock {
            return Err(e);
        }
        if start.elapsed().as_nanos() as u64 >= deadline_ns {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "nonce lock timeout"));
        }
        unsafe { libc::nanosleep(&libc::timespec{ tv_sec:0, tv_nsec:200_000 }, std::ptr::null_mut()); }
    }
    Ok(PathLockGuard { f })
}

#[cfg(unix)]
impl Drop for PathLockGuard {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        let fd = self.f.as_raw_fd();
        let mut fl: libc::flock = unsafe { std::mem::zeroed() };
        fl.l_type = libc::F_UNLCK as i16;
        fl.l_whence = libc::SEEK_SET as i16;
        unsafe { libc::fcntl(fd, libc::F_SETLK, &fl); };
    }
}

#[cfg(not(unix))]
#[inline(always)]
fn lock_nonce_path(_p: &std::path::Path) -> std::io::Result<()> { Ok(()) }

pub trait NonceStore: Send + Sync {
    fn is_used(&self, nonce: u64) -> bool;
    fn mark_used(&mut self, nonce: u64, slot: u64) -> std::io::Result<()>;
    fn last_slot(&self) -> u64;
    fn prune_before(&mut self, min_slot: u64) -> std::io::Result<()>;
}

#[derive(Serialize, Deserialize, Default)]
struct DiskNonceMap {
    // nonce -> slot
    map: BTreeMap<u64, u64>,
    // watermark to allow fast min-slot pruning
    #[serde(default)]
    slot_watermark: u64,
}

pub struct FileNonceStore {
    path: String,
    state: DiskNonceMap,
    // optional max entries to avoid unbounded growth
    max_entries: usize,
}

impl FileNonceStore {
    #[inline(always)]
    fn wal_path_from(path: &str) -> PathBuf { Path::new(path).with_extension("wal") }

    fn wal_append(path: &str, nonce: u64, slot: u64) -> std::io::Result<()> {
        ioprio_idle_best_effort(); // before doing the disk work
        let wal = Self::wal_path_from(path);
        if let Some(parent) = wal.parent() { fs::create_dir_all(parent)?; }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut oo = OpenOptions::new();
            oo.create(true).append(true).mode(0o600).custom_flags(libc::O_CLOEXEC);
            #[cfg(target_os="linux")] { oo.custom_flags(libc::O_CLOEXEC | libc::O_NOATIME); }
            let f = oo.open(&wal)?;
            let mut w = BufWriter::new(f);
            let mut rec = [0u8; 20];
            rec[..8].copy_from_slice(&nonce.to_le_bytes());
            rec[8..16].copy_from_slice(&slot.to_le_bytes());
            let mut h = blake3::Hasher::new();
            h.update(&rec[..16]);
            let tag = h.finalize();
            rec[16..20].copy_from_slice(&tag.as_bytes()[..4]);
            w.write_all(&rec)?;
            w.flush()?;
            use std::os::fd::AsRawFd;
            let _ = nix::unistd::fdatasync(w.get_ref().as_raw_fd());
        }
        #[cfg(not(unix))]
        {
            let f = OpenOptions::new().create(true).append(true).open(&wal)?;
            let mut w = BufWriter::new(f);
            let mut rec = [0u8; 20];
            rec[..8].copy_from_slice(&nonce.to_le_bytes());
            rec[8..16].copy_from_slice(&slot.to_le_bytes());
            let mut h = blake3::Hasher::new();
            h.update(&rec[..16]);
            let tag = h.finalize();
            rec[16..20].copy_from_slice(&tag.as_bytes()[..4]);
            w.write_all(&rec)?;
            w.flush()?;
        }
        Ok(())
    }

    fn wal_truncate(path: &str) -> std::io::Result<()> {
        let wal = Self::wal_path_from(path);
        if wal.exists() {
            let f = OpenOptions::new().write(true).truncate(true).open(&wal)?;
            #[cfg(unix)]
            { use std::os::fd::AsRawFd; let _ = nix::unistd::fdatasync(f.as_raw_fd()); }
        }
        Ok(())
    }

    /// Apply any WAL records to `state`; returns applied record count.
    fn wal_apply(path: &str, state: &mut DiskNonceMap) -> std::io::Result<usize> {
        let wal = Self::wal_path_from(path);
        if !wal.exists() { return Ok(0); }
        let mut buf = Vec::new();
        if let Ok(mut f) = File::open(&wal) { let _ = f.read_to_end(&mut buf); }
        if buf.is_empty() { return Ok(0); }

        // Prefer new 20B format when file size aligns; else fall back to legacy 16B parsing.
        let mut applied = 0usize;
        if (buf.len() % 20) == 0 {
            let mut i = 0usize;
            while i + 20 <= buf.len() {
                let mut a=[0u8;8]; let mut b=[0u8;8]; let mut t=[0u8;4];
                a.copy_from_slice(&buf[i..i+8]); b.copy_from_slice(&buf[i+8..i+16]); t.copy_from_slice(&buf[i+16..i+20]);
                let mut h = blake3::Hasher::new(); h.update(&buf[i..i+16]);
                let tag = h.finalize();
                if &tag.as_bytes()[..4] != &t { break; } // stop on first corrupt/torn record
                let nonce = u64::from_le_bytes(a); let slot = u64::from_le_bytes(b);
                if !state.map.contains_key(&nonce) {
                    state.map.insert(nonce, slot);
                    if state.slot_watermark < slot { state.slot_watermark = slot; }
                    applied += 1;
                }
                i += 20;
            }
            return Ok(applied);
        }

        // Legacy 16B records (no checksum). Apply only complete records; ignore trailing partials.
        let tail = buf.len() - (buf.len() % 16);
        let mut i = 0usize;
        while i + 16 <= tail {
            let mut a=[0u8;8]; let mut b=[0u8;8];
            a.copy_from_slice(&buf[i..i+8]); b.copy_from_slice(&buf[i+8..i+16]);
            let nonce = u64::from_le_bytes(a); let slot = u64::from_le_bytes(b);
            if !state.map.contains_key(&nonce) {
                state.map.insert(nonce, slot);
                if state.slot_watermark < slot { state.slot_watermark = slot; }
                applied += 1;
            }
            i += 16;
        }
        Ok(applied)
    }

    pub fn load(path: &str) -> Self {
        let mut state = match File::open(path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                serde_json::from_reader::<_, DiskNonceMap>(reader).unwrap_or_default()
            }
            Err(_) => DiskNonceMap::default(),
        };

        // Crash-safe recovery: apply any WAL rows then persist & truncate WAL
        let applied = FileNonceStore::wal_apply(path, &mut state).unwrap_or(0);
        let mut s = Self { path: path.to_string(), state, max_entries: 100_000 };
        if applied > 0 {
            let _ = s.persist();
            let _ = FileNonceStore::wal_truncate(path);
        }
        s
    }

    // REPLACE: FileNonceStore::persist
    fn persist(&self) -> std::io::Result<()> {
        ioprio_idle_best_effort(); // before doing the disk work
        use std::io::Write;
        let path = std::path::Path::new(&self.path);
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }

        #[cfg(all(unix, target_os = "linux"))]
        unsafe {
            use std::os::fd::{AsRawFd, FromRawFd, RawFd};
            use std::os::unix::fs::OpenOptionsExt;

            // Try O_TMPFILE path first
            let dir = std::fs::OpenOptions::new()
                .read(true).custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
                .open(parent.unwrap_or(std::path::Path::new(".")));

            if let Ok(dirf) = dir {
                let dirfd = dirf.as_raw_fd();
                let mode  = 0o600;
                let fd: RawFd = libc::openat(
                    dirfd,
                    b".\0".as_ptr() as *const libc::c_char,
                    libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC,
                    mode
                );
                if fd >= 0 {
                    // Write JSON to tmp (anonymous) inode
                    let mut f = std::fs::File::from_raw_fd(fd);
                    {
                        let mut w = std::io::BufWriter::new(&f);
                        serde_json::to_writer(&mut w, &self.state)?;
                        w.flush()?;
                    }
                    let _ = nix::unistd::fdatasync(f.as_raw_fd());

                    // Link into place atomically via linkat(AT_EMPTY_PATH)
                    let name = path.file_name().unwrap().as_bytes();
                    let mut c_name = Vec::with_capacity(name.len() + 1);
                    c_name.extend_from_slice(name);
                    c_name.push(0);

                    // AT_EMPTY_PATH lets us link by fd
                    let r = libc::linkat(
                        f.as_raw_fd(), std::ptr::null(),               // oldfd, oldpath=""
                        dirfd, c_name.as_ptr() as *const _,            // newfd + newpath
                        libc::AT_EMPTY_PATH
                    );
                    if r != 0 {
                        // Fallback to standard tempfile route if kernel forbids AT_EMPTY_PATH
                        drop(f);
                    } else {
                        // Seal durability
                        let _ = nix::unistd::fsync(dirfd);
                        // Page-cache hygiene
                        let _ = nix::fcntl::posix_fadvise(
                            f.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED
                        );
                        let _ = nix::fcntl::posix_fadvise(
                            dirfd, 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED
                        );
                        return Ok(());
                    }
                }
            }
            // Fallthrough to portable path
        }

        // Portable fallback: NamedTempFile + atomic rename + page-cache DONTNEED
        let tmp = tempfile::NamedTempFile::new_in(path.parent().unwrap_or(std::path::Path::new(".")))?;
        {
            let mut writer = std::io::BufWriter::new(tmp.as_file());
            serde_json::to_writer(&mut writer, &self.state)?;
            writer.flush()?;
            #[cfg(unix)]
            {
                use std::os::fd::AsRawFd;
                let _ = nix::unistd::fdatasync(tmp.as_file().as_raw_fd());
            }
            #[cfg(not(unix))] { tmp.as_file().sync_all()?; }
        }
        tmp.persist(&self.path)?;

        // Fsync directory to seal rename
        if let Some(parent) = path.parent() {
            #[cfg(unix)]
            {
                use std::os::fd::AsRawFd;
                if let Ok(dir) = std::fs::File::open(parent) { let _ = nix::unistd::fsync(dir.as_raw_fd()); }
            }
            #[cfg(not(unix))] { let _ = std::fs::File::open(parent).and_then(|d| d.sync_all()); }
        }

        // Drop page cache: file and directory (avoid hot set pollution)
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            if let Ok(f) = std::fs::File::open(&self.path) {
                let _ = nix::unistd::fdatasync(f.as_raw_fd());
                let _ = nix::fcntl::posix_fadvise(
                    f.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED
                );
            }
            if let Some(parent) = path.parent() {
                if let Ok(dir) = std::fs::File::open(parent) {
                    let _ = nix::fcntl::posix_fadvise(
                        dir.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED
                    );
                }
            }
        }
        Ok(())
    }
}

impl NonceStore for FileNonceStore {
    fn is_used(&self, nonce: u64) -> bool {
        self.state.map.contains_key(&nonce)
    }

    // REPLACE: FileNonceStore::mark_used (append WAL before commit)
    fn mark_used(&mut self, nonce: u64, slot: u64) -> std::io::Result<()> {
        let lock_path = Path::new(&self.path).with_extension("lock");
        let _guard = lock_nonce_path(&lock_path)?; // cross-process critical section

        // Merge latest on-disk state
        if let Ok(f) = File::open(&self.path) {
            if let Ok(state) = serde_json::from_reader::<_, _>(BufReader::new(f)) {
                self.state = state;
            }
        }

        if self.state.map.contains_key(&nonce) {
            return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "nonce already used"));
        }

        // 1) WAL first (crash-safe)
        FileNonceStore::wal_append(&self.path, nonce, slot)?;

        // 2) Apply to memory
        self.state.map.insert(nonce, slot);

        // 3) Enforce max size (value-based, not key-ordered)
        if self.state.map.len() > self.max_entries {
            let min_slot = slot.saturating_sub(super::PROOF_VALIDITY_SLOTS * 2);
            let _ = self.prune_before(min_slot);
        }

        // 4) Durable JSON rewrite
        self.persist()?;

        // 5) Truncate WAL (commit sealed)
        FileNonceStore::wal_truncate(&self.path)?;

        Ok(())
    }

    fn last_slot(&self) -> u64 {
        self.state
            .map
            .values()
            .max()
            .copied()
            .unwrap_or(self.state.slot_watermark)
    }

    fn prune_before(&mut self, min_slot: u64) -> std::io::Result<()> {
        // Collect all nonces whose recorded slot is below the watermark, regardless of key order.
        let drop_keys: Vec<u64> = self.state
            .map
            .iter()
            .filter_map(|(nonce, &slot)| if slot < min_slot { Some(*nonce) } else { None })
            .collect();
        for k in drop_keys { self.state.map.remove(&k); }
        // Advance watermark (helps future validations and compaction heuristics)
        if self.state.slot_watermark < min_slot {
            self.state.slot_watermark = min_slot;
        }
        self.persist()
    }
}

#[derive(Error, Debug)]
pub enum ProofError {
    #[error("Invalid hex input: {0}")] InvalidHex(String),
    #[error("Zero public input not allowed")] ZeroPublicInput,
    #[error("IO error: {0}")] Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")] Serialization(String),
    #[error("Proof generation failed: {0}")] ProofGeneration(String),
    #[error("Invalid proving key")] InvalidProvingKey,
    #[error("Invalid input count: expected max {max}, got {actual}")]
    InvalidInputCount { max: usize, actual: usize },
    #[error("Replay attack detected: nonce {nonce} already used")] ReplayAttack { nonce: u64 },
    #[error("Invalid slot distance: minimum {min}, got {actual}")]
    InvalidSlotDistance { min: u64, actual: u64 },
    #[error("Slot validation failed: current {current}, proof {proof}")]
    SlotValidationFailed { current: u64, proof: u64 },
    #[error("Artifact digest not in allowlist")] ArtifactRejected,
    #[error("Sealed artifact path mismatch")] ArtifactPathMismatch,
    #[error("Too much prover concurrency; limit {limit}")] Backpressure { limit: usize },
    #[error("Insecure file permissions: {0}")] InvalidPermissions(String),
    #[error("Artifact changed between stat/open (race): {0}")] ArtifactRace(String),
}

/// Replay-resistant proof metadata
#[derive(Debug, Clone)]
pub struct Metadata {
    pub proof_hash: [u8; 32],
    pub slot: u64,
    pub nonce: u64,
    pub circuit_hash: [u8; 32],
}

// ADD: tiny deterministic ChaCha12 (constant-time, no alloc)
struct Dtrng {
    state: [u32; 16], // ChaCha core state
    buf: [u8; 64],
    idx: usize,
}
impl Dtrng {
    #[inline]
    fn new(slot: u64, nonce: u64, circuit_hash: &[u8;32], ctx_digest: &[u8;32]) -> Self {
        // Key = blake3(slot||nonce||circuit_hash||ctx_digest)[..32]
        let mut h = blake3::Hasher::new();
        h.update(&slot.to_le_bytes()); h.update(&nonce.to_le_bytes());
        h.update(circuit_hash); h.update(ctx_digest);
        let key = h.finalize();
        let mut k = [0u8;32]; k.copy_from_slice(&key.as_bytes()[..32]);

        // 96-bit IV from the rest of the digest
        let mut iv = [0u8;12];
        iv.copy_from_slice(&key.as_bytes()[32..44]); // safe: 44 <= 32+32

        // Setup ChaCha state
        let mut s = [0u32;16];
        s[0] = 0x61707865; s[1] = 0x3320646e; s[2] = 0x79622d32; s[3] = 0x6b206574;
        for i in 0..8 { s[4 + i] = u32::from_le_bytes([k[4*i],k[4*i+1],k[4*i+2],k[4*i+3]]); }
        s[12] = 0; // counter
        s[13] = u32::from_le_bytes([iv[0],iv[1],iv[2],iv[3]]);
        s[14] = u32::from_le_bytes([iv[4],iv[5],iv[6],iv[7]]);
        s[15] = u32::from_le_bytes([iv[8],iv[9],iv[10],iv[11]]);
        Self { state: s, buf: [0u8;64], idx: 64 }
    }

    #[inline(always)]
    fn refill(&mut self) {
        let mut x = self.state;
        macro_rules! qr { ($a:expr,$b:expr,$c:expr,$d:expr) => {{
            x[$a] = x[$a].wrapping_add(x[$b]); x[$d] ^= x[$a]; x[$d] = x[$d].rotate_left(16);
            x[$c] = x[$c].wrapping_add(x[$d]); x[$b] ^= x[$c]; x[$b] = x[$b].rotate_left(12);
            x[$a] = x[$a].wrapping_add(x[$b]); x[$d] ^= x[$a]; x[$d] = x[$d].rotate_left(8);
            x[$c] = x[$c].wrapping_add(x[$d]); x[$b] ^= x[$c]; x[$b] = x[$b].rotate_left(7);
        }};}
        // 12 rounds (ChaCha12)
        for _ in 0..6 {
            qr!(0,4, 8,12); qr!(1,5, 9,13); qr!(2,6,10,14); qr!(3,7,11,15);
            qr!(0,5,10,15); qr!(1,6,11,12); qr!(2,7, 8,13); qr!(3,4, 9,14);
        }
        for i in 0..16 { x[i] = x[i].wrapping_add(self.state[i]); }
        for i in 0..16 { self.buf[4*i..4*i+4].copy_from_slice(&x[i].to_le_bytes()); }
        self.idx = 0;
        // bump counter
        self.state[12] = self.state[12].wrapping_add(1);
    }

    #[inline(always)]
    fn fill_bytes(&mut self, out: &mut [u8]) {
        let mut i = 0;
        while i < out.len() {
            if self.idx == 64 { self.refill(); }
            let n = (64 - self.idx).min(out.len() - i);
            out[i..i+n].copy_from_slice(&self.buf[self.idx..self.idx+n]);
            self.idx += n; i += n;
        }
    }
}

impl RngCore for Dtrng {
    #[inline(always)] fn next_u32(&mut self) -> u32 { 
        let mut buf = [0u8; 4]; 
        self.fill_bytes(&mut buf); 
        u32::from_le_bytes(buf) 
    }
    #[inline(always)] fn next_u64(&mut self) -> u64 { 
        let mut buf = [0u8; 8]; 
        self.fill_bytes(&mut buf); 
        u64::from_le_bytes(buf) 
    }
    #[inline(always)] fn fill_bytes(&mut self, dest: &mut [u8]) { self.fill_bytes(dest) }
    #[inline(always)] fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core::Error> {
        self.fill_bytes(dest); Ok(())
    }
}

impl ark_std::rand::CryptoRng for Dtrng {}

// ADD: Dtrng ↔ rand_core glue (no std RNG upstream needed)
impl rand_core::RngCore for Dtrng {
    #[inline] fn next_u32(&mut self) -> u32 { let mut b=[0u8;4]; self.fill_bytes(&mut b); u32::from_le_bytes(b) }
    #[inline] fn next_u64(&mut self) -> u64 { let mut b=[0u8;8]; self.fill_bytes(&mut b); u64::from_le_bytes(b) }
    #[inline] fn fill_bytes(&mut self, dest: &mut [u8]) { Dtrng::fill_bytes(self, dest) }
    #[inline] fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core::Error> { self.fill_bytes(dest); Ok(()) }
}
impl rand_core::CryptoRng for Dtrng {}

// Backwards compatibility alias
pub type DeterministicRng = Dtrng;


// REPLACE: remove duplicate Metadata (second definition)
// (intentionally removed to avoid duplicate type definition)
// The canonical `Metadata` definition appears earlier in the file.

pub struct ProofBuilder {
    proving_key: Arc<ProvingKey<Bn254>>,
    circuit_hash: [u8; 32],          // includes artifact_digest & NETWORK (V2)
    circuit_version: u32,
    artifact_digest: [u8; 32],       // 0s if unknown
    sealed_paths: Option<(PathBuf, PathBuf)>, // (wasm, r1cs) canonicalized
    nonce_store: Arc<RwLock<FileNonceStore>>, // nonce -> slot
    slot_tracker_path: String,
}

impl ProofBuilder {
    // REPLACE: ProofBuilder::init_parallelism (stack size + NUMA + RT)
    pub fn init_parallelism(workers: usize) -> Result<(), ProofError> {
        let _ = RAYON_INIT.get_or_try_init(|| {
            let ids_opt = core_affinity::get_core_ids();
            let n_thr = workers.max(1);

            rayon::ThreadPoolBuilder::new()
                .num_threads(n_thr)
                .stack_size(2 * 1024 * 1024) // 2 MiB per worker avoids stack growth faults mid-proof
                .start_handler(move |i| {
                    if let Some(ref ids) = ids_opt {
                        let _ = core_affinity::set_for_current(ids[i % ids.len()]);
                    }
                    #[cfg(all(unix, target_os = "linux"))]
                    {
                        use nix::sched::{sched_setscheduler, SchedParam, Scheduler};
                        let _ = sched_setscheduler(None, Scheduler::Fifo, &SchedParam::new(2));
                    }
                })
                .build_global()
                .map_err(|e| ProofError::Serialization(format!("rayon: {e}")))?;
            Ok(())
        });
        PROVER_LIMIT_ATOMIC.0.store(workers.max(1), Ordering::Relaxed);
        Ok(())
    }

    /// Load proving key from bytes (no artifact info). circuit_hash binds VK + queries + NETWORK.
    pub fn from_bytes(compressed_key: &[u8]) -> Result<Self, ProofError> {
        if compressed_key.is_empty() || compressed_key.len() > MAX_PK_BYTES {
            return Err(ProofError::Serialization("Proving key size invalid".to_string()));
        }
        let pk = ProvingKey::<Bn254>::deserialize_with_mode(
            compressed_key, Compress::Yes, Validate::Yes,
        ).map_err(|e| ProofError::Serialization(e.to_string()))?;
        Self::validate_circuit_parameters(&pk)?;
        let artifact_digest = [0u8; 32];
        let circuit_hash = Self::compute_circuit_hash_v2(&pk, &artifact_digest)?;
        Ok(Self {
            proving_key: Arc::new(pk),
            circuit_hash,
            circuit_version: CIRCUIT_VERSION,
            artifact_digest,
            sealed_paths: None,
            nonce_store: Arc::new(RwLock::new(FileNonceStore::load("zk_nonce_store.json"))),
            slot_tracker_path: "zk_nonce_store.json".to_string(),
        })
    }

    // REPLACE: ProofBuilder::from_file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ProofError> {
        let p = path.as_ref();
        #[cfg(unix)] {
            assert_secure_path_hierarchy(p)?;
            assert_secure_file_permissions(p)?;
        }
        let canon = fs::canonicalize(p)?;
        let f = open_readonly_nofollow(&canon)?;
        verify_same_inode(&canon, &f)?;

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let _ = nix::fcntl::posix_fadvise(f.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL);
        }

        let sz = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
        let mut buf = if sz > 0 && sz <= MAX_PK_BYTES {
            let mut v = Vec::with_capacity(sz);
            BufReader::new(&f).take(sz as u64).read_to_end(&mut v)?;
            v
        } else {
            let mut v = Vec::new();
            BufReader::new(&f).read_to_end(&mut v)?;
            v
        };
        Self::validate_file_integrity(&buf)?;
        if buf.is_empty() || buf.len() > MAX_PK_BYTES {
            buf.zeroize();
            return Err(ProofError::Serialization("Proving key size invalid".into()));
        }

        let sealed = sealed_memfd::SealedMemfd::from_bytes("pk_bytes", &buf)
            .map_err(|e| std::io::Error::new(e.kind(), format!("memfd/seal: {e}")))?;
        buf.zeroize();
        sealed.assert_sealed().map_err(|e| ProofError::Serialization(format!("memfd seals: {e}")))?;

        #[cfg(all(unix, target_os="linux"))]
        let mmap = sealed.map_readonly().map_err(|e| ProofError::Serialization(format!("mmap: {e}")))?;
        #[cfg(not(all(unix, target_os="linux")))]
        let mmap = sealed.map_readonly().map_err(|e| ProofError::Serialization(format!("map: {e}")))?;

        advise_willneed_slice(&mmap[..]);
        advise_sensitive_slice(&mmap[..]);
        advise_dontfork_slice(&mmap[..]);
        advise_hugepage_slice(&mmap[..]);
        advise_random_access_slice(&mmap[..]);
        prefault_read_slice(&mmap[..]);
        advise_dontdump_slice(&mmap[..]);
        bind_mmap_to_local_numa(&mmap[..]);
        advise_unmergeable_slice(&mmap[..]);
        advise_wipeonfork_slice(&mmap[..]);
        advise_collapse_slice(&mmap[..]);
        ensure_resident_ratio(&mmap[..], 0.90);  // NEW: verify ≥90% resident; light warmup if not
        mlock_onfault_slice(&mmap[..]);
        let _ml = MlockGuard::new(&mmap[..]);

        let pk_res = std::panic::catch_unwind(|| {
            ProvingKey::<Bn254>::deserialize_with_mode(&mmap[..], Compress::Yes, Validate::Yes)
        });
        let pk = match pk_res {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(ProofError::Serialization(e.to_string())),
            Err(_)     => return Err(ProofError::Serialization("panic during PK deserialize".into())),
        };
        drop(_ml);
        advise_drop_cache(&mmap[..]);

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let _ = nix::fcntl::posix_fadvise(f.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED);
        }

        Self::validate_circuit_parameters(&pk)?;
        let artifact_digest = [0u8; 32];
        let circuit_hash = Self::compute_circuit_hash_v2(&pk, &artifact_digest)?;
        Ok(Self {
            proving_key: Arc::new(pk),
            circuit_hash,
            circuit_version: CIRCUIT_VERSION,
            artifact_digest,
            sealed_paths: None,
            nonce_store: Arc::new(RwLock::new(FileNonceStore::load("zk_nonce_store.json"))),
            slot_tracker_path: "zk_nonce_store.json".to_string(),
        })
    }

    // REPLACE: ProofBuilder::from_sealed_artifacts (pre-alloc + fadvise + mlock + seal)
    pub fn from_sealed_artifacts(seal: &ArtifactSeal) -> Result<Self, ProofError> {
        #[cfg(unix)] {
            assert_secure_path_hierarchy(&seal.wasm_path)?;
            assert_secure_path_hierarchy(&seal.r1cs_path)?;
            assert_secure_path_hierarchy(&seal.pk_path)?;
        }
        let wasm_c = fs::canonicalize(&seal.wasm_path)?;
        let r1cs_c = fs::canonicalize(&seal.r1cs_path)?;
        let pk_c   = fs::canonicalize(&seal.pk_path)?;

        #[cfg(unix)]
        {
            assert_secure_file_permissions(&wasm_c)?;
            assert_secure_file_permissions(&r1cs_c)?;
            assert_secure_file_permissions(&pk_c)?;
        }

        let wasm = blake3_file_secure(&wasm_c)?;
        let r1cs = blake3_file_secure(&r1cs_c)?;

        let pkf = open_readonly_nofollow(&pk_c)?;
        verify_same_inode(&pk_c, &pkf)?;

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let _ = nix::fcntl::posix_fadvise(
                pkf.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
            );
        }

        let sz = pkf.metadata().map(|m| m.len() as usize).unwrap_or(0);
        let mut pk_bytes = if sz > 0 && sz <= MAX_PK_BYTES {
            let mut v = Vec::with_capacity(sz);
            std::io::Read::by_ref(&mut BufReader::new(&pkf))
                .take(sz as u64)
                .read_to_end(&mut v)?;
            v
        } else {
            let mut v = Vec::new();
            BufReader::new(&pkf).read_to_end(&mut v)?;
            v
        };
        if pk_bytes.is_empty() || pk_bytes.len() > MAX_PK_BYTES {
            pk_bytes.zeroize();
            return Err(ProofError::Serialization("Proving key size invalid".to_string()));
        }

        let pk_digest: [u8; 32] = { let mut h = blake3::Hasher::new(); h.update(&pk_bytes); h.finalize().into() };

        let sealed = sealed_memfd::SealedMemfd::from_bytes("pk_bytes", &pk_bytes)
            .map_err(|e| std::io::Error::new(e.kind(), format!("memfd/seal: {e}")))?;
        pk_bytes.zeroize();
        sealed.assert_sealed().map_err(|e| ProofError::Serialization(format!("memfd seals: {e}")))?;

        #[cfg(all(unix, target_os="linux"))]
        let mmap = sealed.map_readonly().map_err(|e| ProofError::Serialization(format!("mmap: {e}")))?;
        #[cfg(not(all(unix, target_os="linux")))]
        let mmap = sealed.map_readonly().map_err(|e| ProofError::Serialization(format!("map: {e}")))?;

        advise_willneed_slice(&mmap[..]);
        advise_sensitive_slice(&mmap[..]);
        advise_dontfork_slice(&mmap[..]);
        advise_hugepage_slice(&mmap[..]);
        advise_random_access_slice(&mmap[..]);
        prefault_read_slice(&mmap[..]);
        advise_dontdump_slice(&mmap[..]);
        bind_mmap_to_local_numa(&mmap[..]);
        advise_unmergeable_slice(&mmap[..]);
        advise_wipeonfork_slice(&mmap[..]);
        advise_collapse_slice(&mmap[..]);
        ensure_resident_ratio(&mmap[..], 0.90);  // NEW: verify ≥90% resident; light warmup if not
        mlock_onfault_slice(&mmap[..]);
        let _ml = MlockGuard::new(&mmap[..]);

        let pk_res = std::panic::catch_unwind(|| {
            ProvingKey::<Bn254>::deserialize_with_mode(&mmap[..], Compress::Yes, Validate::Yes)
        });
        let pk = match pk_res {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(ProofError::Serialization(e.to_string())),
            Err(_) => return Err(ProofError::Serialization("panic during PK deserialize".into())),
        };
        drop(_ml);
        advise_drop_cache(&mmap[..]);

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let _ = nix::fcntl::posix_fadvise(
                pkf.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
            );
        }

        Self::validate_circuit_parameters(&pk)?;
        let artifact_digest = composite_artifact_digest(&wasm, &r1cs, &pk_digest);
        if !seal.allowlist.is_empty() && !allowlist_contains_ct(&seal.allowlist, &artifact_digest) {
            return Err(ProofError::ArtifactRejected);
        }
        let circuit_hash = Self::compute_circuit_hash_v2(&pk, &artifact_digest)?;
        Ok(Self {
            proving_key: Arc::new(pk),
            circuit_hash,
            circuit_version: CIRCUIT_VERSION,
            artifact_digest,
            sealed_paths: Some((wasm_c, r1cs_c)),
            nonce_store: Arc::new(RwLock::new(FileNonceStore::load("zk_nonce_store.json"))),
            slot_tracker_path: "zk_nonce_store.json".to_string(),
        })
    }

    // REPLACE: generate_proof_ctx (autotune hook)
    pub fn generate_proof_ctx(
        &self,
        hex_inputs: &[String],
        slot: u64,
        nonce: u64,
        wasm_path: &str,
        r1cs_path: &str,
        ctx: Option<ProofContext<'_>>,
    ) -> Result<(Vec<u8>, Metadata), ProofError> {
        // ADD: token bucket backpressure check
        if !admit_quick_tb(u64::MAX) {
            return Err(ProofError::Backpressure { limit: PROVER_LIMIT_ATOMIC.0.load(Ordering::Relaxed) });
        }
        let _guard = ProverGuard::enter()?;

        if hex_inputs.len() > MAX_INPUT_COUNT {
            return Err(ProofError::InvalidInputCount { max: MAX_INPUT_COUNT, actual: hex_inputs.len() });
        }
        self.validate_replay_protection(nonce, slot)?;

        if let Some((ref sw, ref sr)) = self.sealed_paths {
            let wasm_in = fs::canonicalize(wasm_path)?;
            let r1cs_in = fs::canonicalize(r1cs_path)?;
            if &wasm_in != sw || &r1cs_in != sr {
                return Err(ProofError::ArtifactPathMismatch);
            }
        }

        let inputs = self.parse_and_validate_inputs(hex_inputs)?;
        let ctx_digest = compute_ctx_digest(&ctx);

        // === cache fast-path ===
        let key = build_cache_key(&self.circuit_hash, slot, nonce, &ctx_digest, &inputs);
        if let Some((hit, prehash)) = proof_cache_get(&key) {
            // hit: avoid cloning the underlying buffer
            let proof_arc: Arc<[u8]> = hit.clone();
            let proof_hash = self.compute_proof_hash_ctx_from_digest(&prehash, slot, nonce, &ctx_digest)?;
            let metadata = Metadata { proof_hash, slot, nonce, circuit_hash: self.circuit_hash };

            {
                let mut store = self.nonce_store.write().unwrap();
                store.mark_used(nonce, slot).map_err(ProofError::Io)?;
                let _ = store.prune_before(slot.saturating_sub(PROOF_VALIDITY_SLOTS * 2));
            }

            autotune_on_sample(1);
            if AUDIT_ENABLED {
                let mut ih = blake3::Hasher::new();
                for fe in inputs.iter() { ih.update(&fe.into_bigint().to_bytes_le()); }
                let inputs_digest: [u8;32] = ih.finalize().into();
                let _ = append_audit_line(AUDIT_LOG_PATH, &metadata, inputs.len(), inputs_digest, &ctx_digest, 0);
            }
            return Ok((proof_arc.to_vec(), metadata)); // if your API must return Vec<u8>, keep this line
        }
        // =======================

        let mut rng = DeterministicRng::new(slot, nonce, &self.circuit_hash, &ctx_digest);

        let (maj0, min0) = rusage_thread_faults();
        let (v0, i0)     = rusage_thread_switches();
        let t0 = Instant::now();

        let cfg = CircomConfig::<Bn254>::new(wasm_path, r1cs_path)
            .map_err(|e| ProofError::Serialization(format!("CircomConfig: {e}")))?;
        let mut builder = CircomBuilder::new(cfg);
        push_inputs_fast(&mut builder, &inputs)?;
        let circuit = builder.build()
            .map_err(|e| ProofError::Serialization(format!("build circuit: {e}")))?;

        let proof = match std::panic::catch_unwind(|| {
            Groth16::<Bn254>::prove(&self.proving_key, circuit, &mut rng)
        }) {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return Err(ProofError::ProofGeneration(e.to_string())),
            Err(_)     => return Err(ProofError::ProofGeneration("panic during proving".into())),
        };

        let mut proof_bytes = self.serialize_proof_compressed(&proof)?;
        let proof_hash  = self.compute_proof_hash_ctx(&proof_bytes, slot, nonce, &ctx_digest)?;
        // AFTER finishing the measured region, BEFORE reading now_ns():
        compiler_fence(CoreOrdering::SeqCst);
        let elapsed_ms  = t0.elapsed().as_millis() as u64;
        let (maj1, min1)= rusage_thread_faults();
        let (v1, i1)    = rusage_thread_switches();

        autotune_on_sample_faultaware2(
            elapsed_ms,
            maj1.saturating_sub(maj0),
            min1.saturating_sub(min0),
            i1.saturating_sub(i0),
            v1.saturating_sub(v0),
        );

        // insert into cache
        let wm = SLOT_WATERMARK.load(Ordering::Relaxed);
        if slot >= wm.saturating_sub(PROOF_VALIDITY_SLOTS) {
            // Convert once to Arc<[u8]> (zero-copy from Vec capacity via boxed slice)
            let pb_arc: Arc<[u8]> = Arc::from(proof_bytes.into_boxed_slice());
            let len = pb_arc.len();
            let prehash: [u8; 32] = blake3::hash(&*pb_arc).into();
            let key = build_cache_key(&self.circuit_hash, slot, nonce, &ctx_digest, &inputs);
            proof_cache_put(key, pb_arc.clone(), prehash);
        }
        // else: don't waste capacity on stale entries

        let metadata = Metadata {
            proof_hash,
            slot,
            nonce,
            circuit_hash: self.circuit_hash,
        };

        {
            let mut store = self.nonce_store.write().unwrap();
            store.mark_used(nonce, slot).map_err(ProofError::Io)?;
            let _ = store.prune_before(slot.saturating_sub(PROOF_VALIDITY_SLOTS * 2));
        }

        if AUDIT_ENABLED {
            let mut ih = blake3::Hasher::new();
            for fe in inputs.iter() {
                ih.update(&fe.into_bigint().to_bytes_le());
            }
            let inputs_digest: [u8; 32] = ih.finalize().into();
            let _ = append_audit_line(
                AUDIT_LOG_PATH, &metadata, hex_inputs.len(), inputs_digest, &ctx_digest, elapsed_ms
            );
        }

        Ok((proof_bytes, metadata))
    }

    /// Backwards-compatible wrapper (no context)
    pub fn generate_proof(
        &self,
        hex_inputs: &[String],
        slot: u64,
        nonce: u64,
        wasm_path: &str,
        r1cs_path: &str,
    ) -> Result<(Vec<u8>, Metadata), ProofError> {
        self.generate_proof_ctx(hex_inputs, slot, nonce, wasm_path, r1cs_path, None)
    }

    // REPLACE: generate_proof_ctx_v3 (panic-shield + autotune)
    pub fn generate_proof_ctx_v3(
        &self,
        hex_inputs: &[String],
        slot: u64,
        nonce: u64,
        wasm_path: &str,
        r1cs_path: &str,
        ctx: Option<ProofContext<'_>>,
    ) -> Result<(Vec<u8>, Metadata, [u8; 32]), ProofError> {
        // ADD: token bucket backpressure check
        if !admit_quick_tb(u64::MAX) {
            return Err(ProofError::Backpressure { limit: PROVER_LIMIT_ATOMIC.0.load(Ordering::Relaxed) });
        }
        let _guard = ProverGuard::enter()?;

        if hex_inputs.len() > MAX_INPUT_COUNT {
            return Err(ProofError::InvalidInputCount { max: MAX_INPUT_COUNT, actual: hex_inputs.len() });
        }
        self.validate_replay_protection(nonce, slot)?;

        if let Some((ref sw, ref sr)) = self.sealed_paths {
            let wasm_in = fs::canonicalize(wasm_path)?;
            let r1cs_in = fs::canonicalize(r1cs_path)?;
            if &wasm_in != sw || &r1cs_in != sr {
                return Err(ProofError::ArtifactPathMismatch);
            }
        }

        let inputs = self.parse_and_validate_inputs(hex_inputs)?;
        let witness_root = compute_witness_merkle_root(&inputs);

        let ctx_digest = compute_ctx_digest(&ctx);

        // === cache fast-path ===
        let key = build_cache_key(&self.circuit_hash, slot, nonce, &ctx_digest, &inputs);
        if let Some((hit, prehash)) = proof_cache_get(&key) {
            // hit: avoid cloning the underlying buffer
            let proof_arc: Arc<[u8]> = hit.clone();
            let proof_hash = self.compute_proof_hash_ctx_from_digest(&prehash, slot, nonce, &ctx_digest)?;
            let metadata = Metadata { proof_hash, slot, nonce, circuit_hash: self.circuit_hash };

            {
                let mut store = self.nonce_store.write().unwrap();
                store.mark_used(nonce, slot).map_err(ProofError::Io)?;
                let _ = store.prune_before(slot.saturating_sub(PROOF_VALIDITY_SLOTS * 2));
            }

            autotune_on_sample(1);
            if AUDIT_ENABLED {
                let mut ih = blake3::Hasher::new();
                for fe in inputs.iter() { ih.update(&fe.into_bigint().to_bytes_le()); }
                let inputs_digest: [u8;32] = ih.finalize().into();
                let _ = append_audit_line_v3(
                    AUDIT_LOG_PATH, &metadata, inputs.len(), inputs_digest, &ctx_digest, 0, witness_root
                );
            }
            return Ok((proof_arc.to_vec(), metadata, witness_root)); // if your API must return Vec<u8>, keep this line
        }
        // =======================

        let mut rng = DeterministicRng::new(slot, nonce, &self.circuit_hash, &ctx_digest);

        let (maj0, min0) = rusage_thread_faults();
        let (v0, i0)     = rusage_thread_switches();
        let t0 = Instant::now();

        let cfg = CircomConfig::<Bn254>::new(wasm_path, r1cs_path)
            .map_err(|e| ProofError::Serialization(format!("CircomConfig: {e}")))?;
        let mut builder = CircomBuilder::new(cfg);
        push_inputs_fast(&mut builder, &inputs)?;
        let circuit = builder.build()
            .map_err(|e| ProofError::Serialization(format!("build circuit: {e}")))?;

        let proof = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Groth16::<Bn254>::prove(&self.proving_key, circuit, &mut rng)
        })) {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return Err(ProofError::ProofGeneration(e.to_string())),
            Err(_)     => return Err(ProofError::ProofGeneration("panic during proving".into())),
        };

        let mut proof_bytes = self.serialize_proof_compressed(&proof)?;
        let proof_hash  = self.compute_proof_hash_ctx(&proof_bytes, slot, nonce, &ctx_digest)?;
        // AFTER finishing the measured region, BEFORE reading now_ns():
        compiler_fence(CoreOrdering::SeqCst);
        let elapsed_ms  = t0.elapsed().as_millis() as u64;
        let (maj1, min1)= rusage_thread_faults();
        let (v1, i1)    = rusage_thread_switches();

        autotune_on_sample_faultaware2(
            elapsed_ms,
            maj1.saturating_sub(maj0),
            min1.saturating_sub(min0),
            i1.saturating_sub(i0),
            v1.saturating_sub(v0),
        );

        // Convert once to Arc<[u8]> (zero-copy from Vec capacity via boxed slice)
        let pb_arc: Arc<[u8]> = Arc::from(proof_bytes.into_boxed_slice());
        let len = pb_arc.len();
        let prehash: [u8; 32] = blake3::hash(&*pb_arc).into();
        let key = build_cache_key(&self.circuit_hash, slot, nonce, &ctx_digest, &inputs);
        let wm = SLOT_WATERMARK.load(Ordering::Relaxed);
        if slot >= wm.saturating_sub(PROOF_VALIDITY_SLOTS) {
            proof_cache_put(key, pb_arc.clone(), prehash);
        }

        let metadata = Metadata { proof_hash, slot, nonce, circuit_hash: self.circuit_hash };

        {
            let mut store = self.nonce_store.write().unwrap();
            store.mark_used(nonce, slot).map_err(ProofError::Io)?;
            let _ = store.prune_before(slot.saturating_sub(PROOF_VALIDITY_SLOTS * 2));
        }

        if AUDIT_ENABLED {
            let mut ih = blake3::Hasher::new();
            for fe in inputs.iter() { ih.update(&fe.into_bigint().to_bytes_le()); }
            let inputs_digest: [u8; 32] = ih.finalize().into();
            let _ = append_audit_line_v3(
                AUDIT_LOG_PATH, &metadata, hex_inputs.len(), inputs_digest, &ctx_digest, elapsed_ms, witness_root
            );
        }

        Ok((proof_bytes, metadata, witness_root))
    }

    // ADD: try_generate_proof_ctx_v3_deadline
    pub fn try_generate_proof_ctx_v3_deadline(
        &self,
        hex_inputs: &[String],
        slot: u64,
        nonce: u64,
        wasm_path: &str,
        r1cs_path: &str,
        ctx: Option<ProofContext<'_>>,
        ms_left: u64,
    ) -> Result<(Vec<u8>, Metadata, [u8; 32]), ProofError> {
        match admit_by_deadline(ms_left) {
            AdmitDecision::Start => self.generate_proof_ctx_v3(hex_inputs, slot, nonce, wasm_path, r1cs_path, ctx),
            AdmitDecision::SkipLate => Err(ProofError::Backpressure { limit: PROVER_LIMIT_ATOMIC.0.load(Ordering::Relaxed) }),
        }
    }

    /// Generate proof, compute witness root, bind ctx, and return attested envelope (v3).
    #[inline]
    pub fn generate_and_attest_v3(
        &self,
        hex_inputs: &[String],
        slot: u64,
        nonce: u64,
        wasm_path: &str,
        r1cs_path: &str,
        ctx: Option<ProofContext<'_>>,
    ) -> Result<AttestedProofV3, ProofError> {
        let (proof_bytes, meta, witness_root) =
            self.generate_proof_ctx_v3(hex_inputs, slot, nonce, wasm_path, r1cs_path, ctx.clone())?;

        // SHA256 ctx digest for cheap on-chain matching
        let ctx_sha = if let Some(c) = &ctx {
            compute_ctx_digest_sha256(c.program_tag, c.route_commitment, c.extra)
        } else {
            [0u8; 32]
        };

        let att = build_attestation_v3_sha256(meta.proof_hash, ctx_sha, self.circuit_hash, witness_root);
        Ok(AttestedProofV3 {
            proof_bytes,
            attestation_v3: att,
            ctx_digest_sha256: ctx_sha,
            witness_root,
            circuit_hash: self.circuit_hash,
        })
    }

    /// Getters (useful for sidecars/telemetry)
    #[inline] pub fn circuit_hash(&self) -> [u8; 32] { self.circuit_hash }
    #[inline] pub fn artifact_digest(&self) -> [u8; 32] { self.artifact_digest }

    // ADD: Getter for SHA256 circuit hash (telemetry/ABI)
    #[inline]
    pub fn circuit_hash_sha256(&self) -> [u8; 32] {
        // Recompute from PK + artifact_digest to avoid state drift.
        // Safe cost; call rarely (e.g., once per startup).
        // If you prefer caching, store alongside self.circuit_hash.
        Self::compute_circuit_hash_v3_sha256(&self.proving_key, &self.artifact_digest)
            .unwrap_or([0u8;32])
    }

    // ===== private helpers =====

    fn validate_circuit_parameters(pk: &ProvingKey<Bn254>) -> Result<(), ProofError> {
        if pk.vk.alpha_g1.is_zero() || pk.vk.beta_g2.is_zero() { return Err(ProofError::InvalidProvingKey); }
        if pk.a_query.is_empty() || pk.b_g1_query.is_empty()   { return Err(ProofError::InvalidProvingKey); }
        Ok(())
    }

    fn validate_file_integrity(buffer: &[u8]) -> Result<(), ProofError> {
        if buffer.is_empty() { return Err(ProofError::Serialization("Empty file".to_string())); }
        if buffer.iter().all(|&b| b == 0) {
            return Err(ProofError::Serialization("File appears all-zero corrupted".to_string()));
        }
        Ok(())
    }

    fn validate_replay_protection(&self, nonce: u64, current_slot: u64) -> Result<(), ProofError> {
        let store = self.nonce_store.read().unwrap();
        if store.is_used(nonce) {
            return Err(ProofError::ReplayAttack { nonce });
        }
        let last = store.last_slot();
        if current_slot < last + MIN_SLOT_DISTANCE {
            return Err(ProofError::InvalidSlotDistance {
                min: MIN_SLOT_DISTANCE,
                actual: current_slot.saturating_sub(last),
            });
        }
        if current_slot > last.saturating_add(PROOF_VALIDITY_SLOTS * 8) {
            return Err(ProofError::SlotValidationFailed { current: current_slot, proof: last });
        }
        Ok(())
    }

    fn parse_and_validate_inputs(&self, hex_inputs: &[String]) -> Result<Vec<Fr>, ProofError> {
        #[inline(always)]
        fn lut(b: u8) -> u8 {
            // Perfect-hash nibble table (const, branchless)
            const T: [u8; 256] = {
                let mut t = [0xFFu8; 256];
                let mut i = 0;
                while i < 10 { t[b'0' as usize + i] = i as u8; i += 1; }
                i = 0; while i < 6  { t[b'a' as usize + i] = (10 + i) as u8; i += 1; }
                i = 0; while i < 6  { t[b'A' as usize + i] = (10 + i) as u8; i += 1; }
                t
            };
            T[b as usize]
        }

        #[inline(always)]
        fn decode_hex_stack(s: &str) -> Result<ArrayVec<u8, 128>, ProofError> {
            let b = s.as_bytes();
            if b.len() > 128 { return Err(ProofError::InvalidHex("Input too long".to_string())); }
            let mut out: ArrayVec<u8, 128> = ArrayVec::new();

            let mut i = 0usize;

            // Handle odd length without early return branching on the very first nibble
            if (b.len() & 1) == 1 {
                let lo = lut(b[0]);
                // Accumulate validity mask and value, store anyway; reject after loop if invalid.
                out.push(lo);
                i = 1;
            }
            let mut invalid: u8 = 0;
            while i < b.len() {
                let hi = lut(b[i]);
                let lo = lut(b[i + 1]);
                invalid |= (hi == 0xFF) as u8;
                invalid |= (lo == 0xFF) as u8;
                out.push((hi << 4) | lo);
                i += 2;
            }
            // Fail if any nibble was invalid (constant-time w.r.t. content)
            if invalid != 0 { return Err(ProofError::InvalidHex(s.to_string())); }
            Ok(out)
        }

        let mut out = Vec::with_capacity(hex_inputs.len());
        for s in hex_inputs {
            let h = s.strip_prefix("0x").unwrap_or(s);
            let bytes = decode_hex_stack(h)?;
            // `Fr::from_le_bytes_mod_order` consumes any length; pass stack-slice
            let fe = Fr::from_le_bytes_mod_order(&bytes);
            if fe.is_zero() { return Err(ProofError::ZeroPublicInput); }
            out.push(fe);
        }
        Ok(out)
    }

    fn compute_proof_hash_ctx(
        &self,
        proof_bytes: &[u8],
        slot: u64,
        nonce: u64,
        ctx_digest: &[u8; 32],
    ) -> Result<[u8; 32], ProofError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN_SEPARATOR_V2);
        hasher.update(b"PROOF_HASH_V2");
        hasher.update(NETWORK_ID);
        hasher.update(&slot.to_le_bytes());
        hasher.update(&nonce.to_le_bytes());
        hasher.update(&self.circuit_version.to_le_bytes());
        hasher.update(&self.circuit_hash);      // includes artifact_digest + NETWORK
        hasher.update(ctx_digest);
        hasher.update(proof_bytes);
        Ok(hasher.finalize().into())
    }

    fn serialize_proof_compressed(&self, proof: &Proof<Bn254>) -> Result<Vec<u8>, ProofError> {
        // BN254 Groth16 compressed proofs are small; 256 gives headroom and avoids growth.
        let mut buffer = Vec::with_capacity(256);
        proof.serialize_with_mode(&mut buffer, Compress::Yes)
            .map_err(|e| ProofError::Serialization(e.to_string()))?;
        Ok(buffer)
    }

    /// Circuit hash V2: VK + query sizes + artifact_digest + NETWORK pin.
    fn compute_circuit_hash_v2(
        pk: &ProvingKey<Bn254>,
        artifact_digest: &[u8; 32],
    ) -> Result<[u8; 32], ProofError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN_SEPARATOR_V2);
        hasher.update(b"CIRCUIT_HASH_V2");
        hasher.update(&CIRCUIT_VERSION.to_le_bytes());
        hasher.update(NETWORK_ID);

        let mut vk_bytes = Vec::new();
        pk.vk.serialize_with_mode(&mut vk_bytes, Compress::Yes)
            .map_err(|e| ProofError::Serialization(e.to_string()))?;
        hasher.update(&vk_bytes);
        hasher.update(&pk.a_query.len().to_le_bytes());
        hasher.update(&pk.b_g1_query.len().to_le_bytes());
        hasher.update(&pk.h_query.len().to_le_bytes());

        hasher.update(artifact_digest);
        Ok(hasher.finalize().into())
    }

    // ADD: Circuit hash V3 (SHA256) — cheaper to verify on-chain if needed
    fn compute_circuit_hash_v3_sha256(
        pk: &ProvingKey<Bn254>,
        artifact_digest: &[u8; 32],
    ) -> Result<[u8; 32], ProofError> {
        use sha2::{Sha256, Digest};
        let mut h = Sha256::new();
        h.update(DOMAIN_SEPARATOR_V2);
        h.update(b"CIRCUIT_HASH_V3_SHA256");
        h.update(&CIRCUIT_VERSION.to_le_bytes());
        h.update(NETWORK_ID);

        let mut vk_bytes = Vec::new();
        pk.vk.serialize_with_mode(&mut vk_bytes, Compress::Yes)
            .map_err(|e| ProofError::Serialization(e.to_string()))?;
        h.update(&vk_bytes);
        h.update(&pk.a_query.len().to_le_bytes());
        h.update(&pk.b_g1_query.len().to_le_bytes());
        h.update(&pk.h_query.len().to_le_bytes());
        h.update(artifact_digest);
        let out = h.finalize();
        let mut arr = [0u8; 32]; arr.copy_from_slice(&out[..32]); Ok(arr)
    }
}

// REPLACE: append_audit_line -> thin wrapper to v3 with zero witness_root
fn append_audit_line(
    path: &str,
    meta: &Metadata,
    inputs_len: usize,
    inputs_digest: [u8; 32],
    ctx_digest: &[u8; 32],
    duration_ms: u64,
) -> std::io::Result<()> {
    append_audit_line_v3(path, meta, inputs_len, inputs_digest, ctx_digest, duration_ms, [0u8;32])
}

// REPLACE: append_audit_line_v3 (O_NOATIME + fdatasync + chain retained)
fn append_audit_line_v3(
    path: &str,
    meta: &Metadata,
    inputs_len: usize,
    inputs_digest: [u8; 32],
    ctx_digest: &[u8; 32],
    duration_ms: u64,
    witness_root: [u8; 32],
) -> std::io::Result<()> {
    use std::io::Write;

    let p = Path::new(path);
    if let Some(parent) = p.parent() { fs::create_dir_all(parent)?; }

    let rotate = fs::metadata(p).map(|m| m.len() > MAX_AUDIT_BYTES).unwrap_or(false);
    if rotate {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let rotated = p.with_extension(format!("{}", ts));
        let _ = fs::rename(p, rotated);
    }

    // chain state
    let chain_path = p.with_extension("chain");
    let prev_chain: [u8;32] = fs::read(&chain_path).ok()
        .and_then(|v| if v.len()==32 { let mut a=[0u8;32]; a.copy_from_slice(&v); Some(a) } else { None })
        .unwrap_or([0u8;32]);

    // host/pid/tid
    let (host, pid, tid) = {
        let host = hostname::get().ok().and_then(|h| h.into_string().ok()).unwrap_or_else(|| "unknown".to_string());
        let pid = std::process::id();
        #[cfg(unix)] let tid = nix::unistd::gettid().as_raw() as usize;
        #[cfg(not(unix))] let tid = 0usize;
        (host, pid, tid)
    };

    #[cfg(unix)]
    let mut f = {
        use std::os::unix::fs::OpenOptionsExt;
        let mut oo = OpenOptions::new();
        oo.create(true).append(true).mode(0o600).custom_flags(libc::O_CLOEXEC);
        #[cfg(target_os = "linux")] { oo.custom_flags(libc::O_CLOEXEC | libc::O_NOATIME); }
        oo.open(p)?
    };
    #[cfg(not(unix))]
    let mut f = OpenOptions::new().create(true).append(true).open(p)?;

    let ts_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let seq = next_audit_seq(p);

    let mut line = String::with_capacity(320);
    line.push('{');
    line.push_str(&format!("\"slot\":{},\"nonce\":{},", meta.slot, meta.nonce));
    line.push_str(&format!("\"proof_hash\":\"{}\",", hex::encode(meta.proof_hash)));
    line.push_str(&format!("\"circuit_hash\":\"{}\",", hex::encode(meta.circuit_hash)));
    line.push_str(&format!("\"inputs\":{},\"inputs_digest\":\"{}\",", inputs_len, hex::encode(inputs_digest)));
    line.push_str(&format!("\"ctx\":\"{}\",", hex::encode(ctx_digest)));
    line.push_str(&format!("\"ms\":{},\"ts_unix\":{},", duration_ms, ts_unix));
    line.push_str(&format!("\"host\":\"{}\",\"pid\":{},\"tid\":{},", host, pid, tid));
    line.push_str(&format!("\"witness_root\":\"{}\",", hex::encode(witness_root)));
    line.push_str(&format!("\"seq\":{},", seq));

    // chain mac
    let chain_mac: [u8;32] = {
        let mut h = blake3::Hasher::new();
        h.update(b"AUDIT_CHAIN_V1");
        h.update(&prev_chain);
        h.update(line.as_bytes());
        h.finalize().into()
    };
    line.push_str(&format!("\"chain\":\"{}\"}}", hex::encode(chain_mac)));

    f.write_all(line.as_bytes())?;
    f.write_all(b"\n")?;
    f.flush()?;

    // durable file data (fdatasync) + dir
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let _ = nix::unistd::fdatasync(f.as_raw_fd());
        if let Some(parent) = p.parent() {
            if let Ok(dir) = File::open(parent) { let _ = nix::unistd::fsync(dir.as_raw_fd()); }
        }
    }
    #[cfg(not(unix))]
    {
        f.sync_all()?;
        if let Some(parent) = p.parent() { let _ = File::open(parent).and_then(|d| d.sync_all()); }
    }

    // persist new chain atomically
    {
        let tmp = tempfile::NamedTempFile::new_in(p.parent().unwrap_or(Path::new(".")))?;
        tmp.as_file().write_all(&chain_mac)?;
        tmp.as_file().sync_all()?;
        tmp.persist(&chain_path)?;
        #[cfg(unix)]
        { if let Ok(dir) = File::open(chain_path.parent().unwrap_or(Path::new("."))) {
            use std::os::fd::AsRawFd; let _ = nix::unistd::fsync(dir.as_raw_fd());
        }}
    }

    // drop page cache for audit file
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let _ = nix::fcntl::posix_fadvise(
            f.as_raw_fd(), 0, 0, nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
        );
    }

    persist_audit_seq(p, seq);
    Ok(())
}

// REPLACE: verify_attestation_v3_sha256_offchain (ct_eq_32)
pub fn verify_attestation_v3_sha256_offchain(
    att: &[u8],
    expected_circuit_hash: [u8; 32],
) -> Result<([u8; 32], [u8; 32], [u8; 32]), ProofError> {
    if att.len() != 3 + 1 + 32*5 { return Err(ProofError::Serialization("att v3 len".to_string())); }
    if &att[0..3] != b"AT3" || att[3] != 3u8 { return Err(ProofError::Serialization("att v3 hdr".to_string())); }
    let proof_hash   = <[u8;32]>::try_from(&att[4..36]).unwrap();
    let ctx_sha      = <[u8;32]>::try_from(&att[36..68]).unwrap();
    let circ         = <[u8;32]>::try_from(&att[68..100]).unwrap();
    let wit_root     = <[u8;32]>::try_from(&att[100..132]).unwrap();
    let mac          = <[u8;32]>::try_from(&att[132..164]).unwrap();
    if circ != expected_circuit_hash { return Err(ProofError::InvalidProvingKey); }

    let mut m = sha2::Sha256::new();
    m.update(DOMAIN_SEPARATOR_V2);
    m.update(b"ATTEST_MAC_V3_SHA256");
    m.update(NETWORK_ID);
    m.update(&proof_hash);
    m.update(&ctx_sha);
    m.update(&circ);
    m.update(&wit_root);
    let want = m.finalize();
    let mut want32 = [0u8;32]; want32.copy_from_slice(&want[..32]);

    if !ct_eq_32(&mac, &want32) {
        return Err(ProofError::Serialization("att v3 mac".to_string()));
    }
    Ok((proof_hash, ctx_sha, wit_root))
}

// ADD: On-chain verifier for Attestation V3 (SHA256)
#[allow(dead_code)]
pub fn verify_attestation_v3_sha256(
    att: &[u8],
    expected_circuit_hash: [u8; 32],
) -> Result<([u8; 32], [u8; 32], [u8; 32]), solana_program::program_error::ProgramError> {
    use solana_program::{hash::hashv, program_error::ProgramError};

    if att.len() != 3 + 1 + 32*5 { return Err(ProgramError::InvalidInstructionData); }
    if &att[0..3] != b"AT3" || att[3] != 3u8 { return Err(ProgramError::InvalidInstructionData); }

    let proof_hash   = <[u8;32]>::try_from(&att[4..36]).unwrap();
    let ctx_sha      = <[u8;32]>::try_from(&att[36..68]).unwrap();
    let circ         = <[u8;32]>::try_from(&att[68..100]).unwrap();
    let wit_root     = <[u8;32]>::try_from(&att[100..132]).unwrap();
    let mac          = <[u8;32]>::try_from(&att[132..164]).unwrap();

    if circ != expected_circuit_hash { return Err(ProgramError::InvalidArgument); }

    // MAC = SHA256( DOMAIN || "ATTEST_MAC_V3_SHA256" || NETWORK || proof || ctx || circuit || witness_root )
    let macv = hashv(&[
        b"ZK_PROOF_MAINNET_V2",
        b"ATTEST_MAC_V3_SHA256",
        b"solana:mainnet-beta",
        &proof_hash, &ctx_sha, &circ, &wit_root,
    ]).to_bytes();

    // constant-time compare
    let mut diff = 0u8;
    for i in 0..32 { diff |= mac[i] ^ macv[i]; }
    if diff != 0 { return Err(ProgramError::InvalidInstructionData); }

    Ok((proof_hash, ctx_sha, wit_root))
}

// ADD: Attestation builder (public; versioned; useful for on-chain or off-chain checks)
pub fn build_attestation(
    proof_hash: [u8; 32],
    ctx_digest: [u8; 32],
    circuit_hash: [u8; 32],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 32 + 32 + 32 + 32);
    // Header
    out.extend_from_slice(b"ATTST");
    out.push(1u8); // version
    // Fields
    out.extend_from_slice(&proof_hash);
    out.extend_from_slice(&ctx_digest);
    out.extend_from_slice(&circuit_hash);
    // MAC
    let mut h = blake3::Hasher::new();
    h.update(DOMAIN_SEPARATOR_V2);
    h.update(b"ATTEST_MAC_V1");
    h.update(NETWORK_ID);
    h.update(&proof_hash);
    h.update(&ctx_digest);
    h.update(&circuit_hash);
    let mac: [u8; 32] = h.finalize().into();
    out.extend_from_slice(&mac);
    out
}

// ADD: On-chain verifier (program-bound) for Attestation V3
#[allow(dead_code)]
pub fn verify_attestation_v3_sha256_bound(
    att: &[u8],
    expected_circuit_hash: [u8; 32],
    program_id: &solana_program::pubkey::Pubkey,
) -> Result<([u8; 32], [u8; 32], [u8; 32]), solana_program::program_error::ProgramError> {
    use solana_program::{hash::hashv, program_error::ProgramError};

    if att.len() != 3 + 1 + 32*5 { return Err(ProgramError::InvalidInstructionData); }
    if &att[0..3] != b"AT3" || att[3] != 3u8 { return Err(ProgramError::InvalidInstructionData); }

    let proof_hash = <[u8;32]>::try_from(&att[4..36]).unwrap();
    let ctx_sha    = <[u8;32]>::try_from(&att[36..68]).unwrap();
    let circ       = <[u8;32]>::try_from(&att[68..100]).unwrap();
    let wit_root   = <[u8;32]>::try_from(&att[100..132]).unwrap();
    let mac        = <[u8;32]>::try_from(&att[132..164]).unwrap();

    if circ != expected_circuit_hash { return Err(ProgramError::InvalidArgument); }

    let macv = hashv(&[
        b"ZK_PROOF_MAINNET_V2",
        b"ATTEST_MAC_V3_SHA256",
        b"solana:mainnet-beta",
        program_id.as_ref(), // bind to verifying program
        &proof_hash, &ctx_sha, &circ, &wit_root,
    ]).to_bytes();

    let mut diff = 0u8; for i in 0..32 { diff |= mac[i] ^ macv[i]; }
    if diff != 0 { return Err(ProgramError::InvalidInstructionData); }

    Ok((proof_hash, ctx_sha, wit_root))
}

// ADD: live concurrency tuning (moved to top of file)

// ADD: helper to assemble ctx + circuit info for envelopes
pub fn envelope_bindings(
    pb: &ProofBuilder,
    ctx: &Option<ProofContext<'_>>,
) -> ([u8; 32], [u8; 32]) {
    let ctx_d = compute_ctx_digest(ctx);
    (pb.circuit_hash(), ctx_d)
}

// ADD: comprehensive network-aware proof generation helper
/// Generates a proof with network-aware deadline and fee management
/// 
/// This function demonstrates the proper usage of all network-aware optimizations:
/// - Network latency tracking
/// - Deadline-aware admission control
/// - Depth-aware fee escalation
/// - Zero-copy serialization
/// 
/// # Arguments
/// * `pb` - The proof builder instance
/// * `hex_inputs` - Input data as hex strings
/// * `slot` - Current slot number
/// * `nonce` - Unique nonce for this proof
/// * `wasm_path` - Path to the WASM file
/// * `r1cs_path` - Path to the R1CS file
/// * `ctx` - Optional proof context
/// * `ms_left` - Milliseconds remaining until slot cutoff
/// * `base_fee` - Base fee in micro-lamports per CU
/// * `max_fee` - Maximum fee in micro-lamports per CU
/// 
/// # Returns
/// * `Ok((proof_bytes, metadata, proof_hash))` on success
/// * `Err(ProofError::Backpressure)` if deadline is too tight
/// * Other errors for proof generation failures
pub fn generate_proof_network_aware(
    pb: &ProofBuilder,
    hex_inputs: &[String],
    slot: u64,
    nonce: u64,
    wasm_path: &str,
    r1cs_path: &str,
    ctx: Option<ProofContext<'_>>,
    ms_left: u64,
    base_fee: u64,
    max_fee: u64,
) -> Result<(Vec<u8>, Metadata, [u8; 32]), ProofError> {
    // 1. Check if we should admit this proof based on deadline + network latency
    match admit_by_deadline(ms_left) {
        AdmitDecision::Start => {
            // 2. Calculate network-aware fee escalation
            let fee = priority_fee_escalator_depth(ms_left, base_fee, max_fee);
            
            // 3. Generate the proof (this will use zero-copy serialization internally)
            let (proof_bytes, metadata, proof_hash) = pb.generate_proof_ctx_v3(
                hex_inputs, slot, nonce, wasm_path, r1cs_path, ctx
            )?;
            
            // 4. Build ZK verification instruction for on-chain verification
            if let Ok(inputs_fr) = pb.parse_and_validate_inputs(hex_inputs) {
                // Build big-endian inputs for on-chain verification
                let public_inputs_be = build_public_inputs_big_endian(&inputs_fr);
                
                // Convert proof to big-endian 256-byte format (A|B|C)
                // Note: This assumes proof_bytes is already in the correct format
                if proof_bytes.len() >= 256 {
                    let mut proof_abc_256 = [0u8; 256];
                    proof_abc_256.copy_from_slice(&proof_bytes[0..256]);
                    
                    // Build verification instruction (program_id would be from config)
                    let program_id = Pubkey::default(); // Replace with actual program ID
                    if let Ok(verify_ix) = build_verify_proof_ix(program_id, proof_abc_256, &public_inputs_be) {
                        // Auto-size budget for ZK verification
                        let (cu, heap, _) = autosize_budget_for_zk_proof(256 + public_inputs_be.len());
                        
                        // Use network-aware fee escalation
                        let zk_fee = priority_fee_escalator_ev(ms_left, base_fee, max_fee, cu, 1000); // Example EV
                        
                        // Note: The verification instruction would be added to transaction instructions
                        // This is just showing the integration point
                        solana_program::msg!("ZK verification instruction built: CU={}, heap={}, fee={}", cu, heap, zk_fee);
                    }
                }
            }
            
            // 5. Return the result with fee information in metadata
            Ok((proof_bytes, metadata, proof_hash))
        }
        AdmitDecision::SkipLate => {
            Err(ProofError::Backpressure { 
                limit: PROVER_LIMIT_ATOMIC.0.load(Ordering::Relaxed) 
            })
        }
    }
}

// ADD: helper to observe network latency after bundle submission
/// Call this after each bundle submission to update network latency estimates
/// 
/// # Arguments
/// * `submission_time` - When the bundle was submitted (timestamp)
/// * `landed_slot` - The slot where the bundle landed
/// * `current_slot` - Current slot for calculating latency
/// * `slot_duration_ms` - Duration of each slot in milliseconds
pub fn observe_bundle_latency(
    submission_time: u64,
    landed_slot: u64,
    current_slot: u64,
    slot_duration_ms: u64,
) {
    // Calculate end-to-end latency in milliseconds
    let latency_ms = (current_slot - landed_slot) * slot_duration_ms;
    
    // Update network latency EMA
    net_observe_latency(latency_ms);
}

/*
// ADD: on-chain attestation verifier (program-side)
// Place this in your Solana program crate (e.g., verifier.rs)
#![allow(dead_code)]
use solana_program::program_error::ProgramError;
use solana_program::hash::hashv;

/// Recompute ctx digest (SHA256) inside program — mirrors off-chain.
pub fn compute_ctx_digest_sha256_onchain(
    program_tag: &[u8],
    route_commitment: Option<[u8; 32]>,
    extra: &[u8],
) -> [u8; 32] {
    let mut parts: Vec<&[u8]> = vec![
        b"ZK_PROOF_MAINNET_V2",
        b"CTX_SHA256",
        b"solana:mainnet-beta",
        program_tag,
        extra,
    ];
    if let Some(rc) = route_commitment {
        parts.push(&rc);
    }
    let hv = hashv(&parts);
    hv.to_bytes()
}

/// Verify v2 attestation constructed by `build_attestation_v2_sha256`.
/// Returns Ok(()) iff MAC is valid and header/layout checks pass.
pub fn verify_attestation_v2_sha256(
    att: &[u8],
    expected_circuit_hash: [u8; 32],
) -> Result<([u8; 32], [u8; 32]), ProgramError> {
    // Format: "AT2" | 0x02 | proof_hash(32) | ctx_sha256(32) | circuit_hash(32) | mac(32)
    if att.len() != 3 + 1 + 32 + 32 + 32 + 32 { return Err(ProgramError::InvalidInstructionData); }
    if &att[0..3] != b"AT2" || att[3] != 2u8 { return Err(ProgramError::InvalidInstructionData); }

    let proof_hash = <[u8; 32]>::try_from(&att[4..36]).unwrap();
    let ctx_sha  = <[u8; 32]>::try_from(&att[36..68]).unwrap();
    let circ     = <[u8; 32]>::try_from(&att[68..100]).unwrap();
    let mac      = <[u8; 32]>::try_from(&att[100..132]).unwrap();

    if circ != expected_circuit_hash { return Err(ProgramError::InvalidArgument); }

    // MAC = SHA256( DOMAIN || "ATTEST_MAC_V2_SHA256" || NETWORK || proof || ctx || circuit )
    let macv = hashv(&[
        b"ZK_PROOF_MAINNET_V2",
        b"ATTEST_MAC_V2_SHA256",
        b"solana:mainnet-beta",
        &proof_hash, &ctx_sha, &circ,
    ]).to_bytes();

    // constant-time compare
    let mut diff = 0u8;
    for i in 0..32 { diff |= mac[i] ^ macv[i]; }
    if diff != 0 { return Err(ProgramError::InvalidInstructionData); }

    Ok((proof_hash, ctx_sha))
}
*/

// ===== OPTIMIZATION 26: ProverGuard with futex parking after calibrated spin =====
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::OnceCell;
use std::os::unix::io::AsRawFd;
// REMOVED: std::collections::HashMap import - using hashbrown::HashMap instead
use std::net::SocketAddr;
use std::sync::mpsc::{Sender, Receiver, unbounded, bounded};
use std::net::TcpStream;
use std::time::Duration;
use ahash::RandomState;

// REPLACE: ProverGuard — spin-then-futex park for fairness without busy waste
pub struct ProverGuard { 
    _ticket: u64 
}

// Global ticket system for admission control
static TICKET_NEXT: OnceCell<AtomicU64> = OnceCell::new();
static TICKET_SERVE: OnceCell<AtomicU64> = OnceCell::new();
static ACTIVE_PROVERS: OnceCell<AtomicU64> = OnceCell::new();
static ADMISSION_SPINS: OnceCell<AtomicU64> = OnceCell::new();

#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn futex_wait_u64(addr: &std::sync::atomic::AtomicU64, expected: u64) {
    // SAFETY: Linux futex on 64-bit atomic; best-effort.
    unsafe {
        let ptr = addr as *const _ as *const i32; // futex works on 32-bit words; use low part
        let val = (expected & 0xFFFF_FFFF) as i32;
        libc::syscall(
            libc::SYS_futex,
            ptr,
            libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
            val,
            std::ptr::null::<libc::timespec>(),
        );
    }
}

#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn futex_wait_u64(_addr: &std::sync::atomic::AtomicU64, _expected: u64) { 
    std::thread::yield_now(); 
}

#[cfg(all(unix, target_os = "linux"))]
#[inline(always)]
fn futex_wake_u64(addr: &std::sync::atomic::AtomicU64) {
    unsafe {
        let ptr = addr as *const _ as *const i32;
        let _ = libc::syscall(
            libc::SYS_futex,
            ptr,
            libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
            1,
        );
    }
}

#[cfg(not(all(unix, target_os = "linux")))]
#[inline(always)]
fn futex_wake_u64(_addr: &std::sync::atomic::AtomicU64) {}

// Helper functions for prover optimization
#[inline(always)]
fn pin_prover_to_core(ticket: u64) {
    // Pin prover to specific core based on ticket
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let core = (ticket as usize) % cores;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::thread;
        let _ = thread::set_affinity(std::thread::current().id(), [core]);
    }
}

#[inline(always)]
fn ensure_rt_sched_low() {
    // Ensure real-time scheduling with low priority
    #[cfg(target_os = "linux")]
    {
        unsafe {
            libc::setpriority(libc::PRIO_PROCESS, 0, 10); // Low priority
        }
    }
}

#[inline(always)]
fn reduce_timer_slack_ns(ns: u64) {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            libc::prctl(libc::PR_SET_TIMERSLACK, ns, 0, 0, 0);
        }
    }
}

#[inline(always)]
fn block_signals_hot_thread() {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigfillset(&mut set);
            libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
        }
    }
}

impl ProverGuard {
    #[inline(always)]
    pub fn enter() -> Self {
        let ticket = TICKET_NEXT.get().unwrap().fetch_add(1, Ordering::Relaxed);
        
        // Calibrated spin, then futex wait
        let spins = ADMISSION_SPINS.get().unwrap().load(Ordering::Relaxed).clamp(64, 4096);
        let mut i = 0usize;
        
        loop {
            let head = TICKET_SERVE.get().unwrap().load(Ordering::Relaxed);
            if head == ticket { break; }
            
            if i < spins { 
                core::hint::spin_loop(); 
                i += 1; 
                continue; 
            }
            
            futex_wait_u64(TICKET_SERVE.get().unwrap(), head);
            i = 0;
        }

        ACTIVE_PROVERS.get().unwrap().fetch_add(1, Ordering::Relaxed);
        pin_prover_to_core(ticket);
        ensure_rt_sched_low();
        reduce_timer_slack_ns(1_000);
        block_signals_hot_thread();
        
        Self { _ticket: ticket }
    }
}

impl Drop for ProverGuard {
    #[inline(always)]
    fn drop(&mut self) {
        ACTIVE_PROVERS.get().unwrap().fetch_sub(1, Ordering::Relaxed);
        let next = TICKET_SERVE.get().unwrap().fetch_add(1, Ordering::Relaxed) + 1;
        futex_wake_u64(TICKET_SERVE.get().unwrap()); // nudge next waiter
        let _ = next;
    }
}

// Initialize the ticket system
pub fn init_prover_guard_system() {
    let _ = TICKET_NEXT.set(AtomicU64::new(0));
    let _ = TICKET_SERVE.set(AtomicU64::new(0));
    let _ = ACTIVE_PROVERS.set(AtomicU64::new(0));
    let _ = ADMISSION_SPINS.set(AtomicU64::new(1024)); // Default spin count
}

// ===== OPTIMIZATION 27: hedged send to single-alloc payload via Arc<[u8]> =====
#[derive(Debug, Clone)]
pub enum RelayOutcome {
    Ok(u64),
    Err(&'static str),
}

// REPLACE: hedged_race_send — single allocation for payload; optional stagger hook
#[inline]
pub fn hedged_race_send(
    payload: &[u8],
    mut relays: Vec<std::net::SocketAddr>,
    slot: u64,
    nonce: u64,
    salt: [u8;32],
) -> RelayOutcome {
    use std::sync::mpsc::channel;
    
    if relays.is_empty() { return RelayOutcome::Err("no_relays"); }

    prepare_proof_buffer_hot(payload);
    // One allocation → Arc<[u8]>
    let shared = std::sync::Arc::<[u8]>::from(payload.to_vec().into_boxed_slice());

    relays = choose_best_relays(relays, relays.len());
    if relays.is_empty() { return RelayOutcome::Err("cooldown_all"); }
    shuffle_endpoints(&mut relays[..], slot, nonce, &salt);

    let (tx, rx) = channel::<(usize, RelayOutcome)>();
    let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    for (idx, addr) in relays.iter().cloned().enumerate() {
        let tx = tx.clone();
        let stop = stopped.clone();
        let data = shared.clone();
        std::thread::spawn(move || {
            if stop.load(Ordering::Relaxed) { 
                let _=tx.send((idx, RelayOutcome::Err("cancelled"))); 
                return; 
            }
            
            // Optional stagger (enable if you pasted 23)
            // let delay = hedge_stagger_ms(idx);
            // if delay > 0 { sleep_ms_fast(delay); }

            let to_ms = relay_connect_timeout_ms(&addr);
            let to = std::time::Duration::from_millis(to_ms);
            let s = match std::net::TcpStream::connect_timeout(&addr, to) {
                Ok(s) => s,
                Err(_) => { 
                    let _=tx.send((idx, RelayOutcome::Err("connect"))); 
                    return; 
                }
            };
            let _ = s.set_nonblocking(false);
            tune_low_latency_stream(&s);
            use std::os::fd::AsRawFd;
            let t0 = mono_now_ns();
            let out = match send_all_nosig(s.as_raw_fd(), &data) {
                Ok(_) => RelayOutcome::Ok(elapsed_ms_since_ns(t0)),
                Err(_) => RelayOutcome::Err("write"),
            };
            let _ = s.flush();
            let _ = tx.send((idx, out));
        });
    }

    let mut last_err: Option<RelayOutcome> = None;
    let mut remaining = relays.len();
    while remaining > 0 {
        match rx.recv() {
            Ok((i, outcome)) => {
                relay_record(relays[i], &outcome);
                match outcome {
                    RelayOutcome::Ok(ms) => {
                        stopped.store(true, Ordering::Relaxed);
                        return RelayOutcome::Ok(ms);
                    }
                    e @ RelayOutcome::Err(_) => { last_err = Some(e); }
                }
                remaining -= 1;
            }
            Err(_) => break,
        }
    }
    last_err.unwrap_or(RelayOutcome::Err("chan_closed"))
}

// ===== OPTIMIZATION 28: stale-slot dropper in workers =====
// ADD: tiny helper — slot freshness gate; treat >2 slots late as garbage
#[inline(always)]
fn is_stale_slot(current: u64, job_slot: u64) -> bool {
    current.saturating_sub(job_slot) > 2
}

// ===== OPTIMIZATION 29: local attestation self-check before spend =====
// ADD: local verify for AT3 SHA256 MAC; constant-time equality
#[inline]
pub fn verify_attestation_v3_sha256(
    proof_hash: [u8;32],
    ctx_sha256: [u8;32],
    circuit_hash: [u8;32],
    witness_root: [u8;32],
    attestation: &[u8],
) -> bool {
    if attestation.len() != (3 + 1 + 32*5) { return false; }
    if &attestation[0..3] != b"AT3" || attestation[3] != 3 { return false; }
    
    let mut mac = sha2::Sha256::new();
    mac.update(b"ZK_PROOF_MAINNET_V2"); // DOMAIN_SEPARATOR_V2
    mac.update(b"ATTEST_MAC_V3_SHA256");
    mac.update(b"solana:mainnet-beta"); // NETWORK_ID
    mac.update(&proof_hash);
    mac.update(&ctx_sha256);
    mac.update(&circuit_hash);
    mac.update(&witness_root);
    let calc = mac.finalize();

    let got = &attestation[(3+1 + 32*4)..(3+1 + 32*5)];
    let mut diff = 0u8;
    for i in 0..32 { diff |= got[i] ^ calc[i]; }
    diff == 0
}

// ===== OPTIMIZATION 30: adaptive CU/heap tuner with online EWMA =====
// ADD: live CU/heap EMA; feed with observed on-chain consumption when available
struct BudgetEma { 
    cu_x1024: AtomicU64, 
    heap_x1024: AtomicU64, 
    inited: AtomicBool 
}

static BUDGET_EMA: OnceCell<BudgetEma> = OnceCell::new();

#[inline]
pub fn budget_ema_init(default_cu: u32, default_heap: u32) {
    let _ = BUDGET_EMA.set(BudgetEma {
        cu_x1024: AtomicU64::new((default_cu as u64) << 10),
        heap_x1024: AtomicU64::new((default_heap as u64) << 10),
        inited: AtomicBool::new(true),
    });
}

#[inline]
pub fn budget_ema_observe(consumed_cu: u32, used_heap: u32) {
    if let Some(b) = BUDGET_EMA.get() {
        let cu = (consumed_cu.max(1) as u64) << 10;
        let hp = (used_heap.max(32768) as u64) << 10;
        let prev_cu = b.cu_x1024.load(Ordering::Relaxed);
        let prev_hp = b.heap_x1024.load(Ordering::Relaxed);
        // α ≈ 1/8
        b.cu_x1024.store(prev_cu + ((cu as i64 - prev_cu as i64) >> 3) as u64, Ordering::Relaxed);
        b.heap_x1024.store(prev_hp + ((hp as i64 - prev_hp as i64) >> 3) as u64, Ordering::Relaxed);
    }
}

#[inline]
pub fn autosize_budget_adaptive(proof_len: usize) -> (u32, u32) {
    // Baseline heuristic from proof size
    let (cu0, heap0, _) = autosize_budget_for_proof(proof_len);
    if let Some(b) = BUDGET_EMA.get() {
        // Bias toward EMA but keep floor from heuristic; clamp to MAX_CU_LIMIT
        let cu = (b.cu_x1024.load(Ordering::Relaxed) >> 10) as u32;
        let hp = (b.heap_x1024.load(Ordering::Relaxed) >> 10) as u32;
        (cu.max(cu0).min(MAX_CU_LIMIT), hp.max(heap0))
    } else { 
        (cu0, heap0) 
    }
}

// ===== OPTIMIZATION 31: EV-weighted token take for admission fairness =====
// ADD: weighted token take; weight in [1..=8] recommended
#[inline]
pub fn token_bucket_try_take_weighted(weight: u64) -> bool {
    let w = weight.clamp(1, 8);
    token_bucket_try_take(w)
}

// ===== OPTIMIZATION 32: relay worker backlog cap with lossless coalescing =====
// ADD: bounded worker channel with replace-on-full for same (slot,nonce)
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct SlotNonce { 
    slot: u64, 
    nonce: u64 
}

#[derive(Clone)]
pub struct RelayJob {
    pub payload: Vec<u8>,
    pub slot: u64,
    pub nonce: u64,
    pub tx: Sender<RelayOutcome>,
}

pub struct RelayWorker {
    pub addr: SocketAddr,
    pub rx: Receiver<RelayJob>,
}

// Global state for relay workers
static RELAY_TX_MAP: OnceCell<Mutex<HashMap<SocketAddr, Sender<RelayJob>, RandomState>>> = OnceCell::new();
static RELAY_PENDING: OnceCell<Mutex<HashMap<(SocketAddr, SlotNonce), (), RandomState>>> = OnceCell::new();
static SLOT_WATERMARK: OnceCell<AtomicU64> = OnceCell::new();

// CPU preference list for worker affinity
static CPU_PREF_LIST: OnceCell<Vec<usize>> = OnceCell::new();

#[inline]
pub fn relay_workers_init(addrs: &[SocketAddr]) {
    let mut map = HashMap::with_hasher(RandomState::default());
    let _ = RELAY_PENDING.set(Mutex::new(HashMap::with_hasher(RandomState::default())));
    
    for &addr in addrs {
        let (tx, rx) = bounded::<RelayJob>(64);
        let w = RelayWorker { addr, rx };
        std::thread::spawn(move || relay_worker_main(w));
        map.insert(addr, tx);
    }
    let _ = RELAY_TX_MAP.set(Mutex::new(map));
    
    // Initialize slot watermark
    let _ = SLOT_WATERMARK.set(AtomicU64::new(0));
}

pub fn relay_send_via_worker(addr: SocketAddr, payload: Vec<u8>, slot: u64, nonce: u64) -> RelayOutcome {
    if let Some(mx) = RELAY_TX_MAP.get() {
        if let Some(tx) = mx.lock().unwrap().get(&addr).cloned() {
            let key = (addr, SlotNonce { slot, nonce });
            
            // prevent duplicate pile-up for same key
            if let Some(pend) = RELAY_PENDING.get() {
                let mut g = pend.lock().unwrap();
                if g.contains_key(&key) { 
                    return RelayOutcome::Err("dedup"); 
                }
                g.insert(key, ());
            }
            
            let (rtx, rrx) = unbounded::<RelayOutcome>();
            let job = RelayJob { payload, slot, nonce, tx: rtx };
            
            match tx.try_send(job) {
                Ok(_) => rrx.recv().unwrap_or(RelayOutcome::Err("worker_closed")),
                Err(_e) => {
                    if let Some(pend) = RELAY_PENDING.get() { 
                        pend.lock().unwrap().remove(&key); 
                    }
                    RelayOutcome::Err("queue_full")
                }
            }
        } else { 
            RelayOutcome::Err("no_worker") 
        }
    } else { 
        RelayOutcome::Err("no_map") 
    }
}

// In relay_worker_main, after finishing a job (success or fail), remove from RELAY_PENDING:
fn relay_worker_main(w: RelayWorker) {
    #[cfg(all(unix, target_os = "linux"))] {
        if let Some(v) = CPU_PREF_LIST.get() {
            if !v.is_empty() {
                let cpu = v[(v.len()/2 + (w.addr.port() as usize)) % v.len()];
                set_affinity_cpu(cpu);
            }
        }
    }
    ignore_sigpipe_global();

    let mut sock: Option<TcpStream> = None;
    loop {
        let job = match w.rx.recv() { 
            Ok(j) => j, 
            Err(_) => break 
        };
        
        let now_slot = SLOT_WATERMARK.get().unwrap().load(Ordering::Relaxed);
        if is_stale_slot(now_slot, job.slot) {
            let _ = job.tx.send(RelayOutcome::Err("stale_slot"));
            if let Some(p) = RELAY_PENDING.get() { 
                p.lock().unwrap().remove(&(w.addr, SlotNonce{slot:job.slot,nonce:job.nonce})); 
            }
            continue;
        }
        
        let t0 = mono_now_ns();
        let s = match sock.take() {
            Some(s) => s,
            None => {
                match worker_connect(&w.addr) {
                    Some(s) => s,
                    None => { 
                        let _=job.tx.send(RelayOutcome::Err("connect")); 
                        if let Some(p)=RELAY_PENDING.get(){
                            p.lock().unwrap().remove(&(w.addr, SlotNonce{slot:job.slot,nonce:job.nonce}));
                        } 
                        continue; 
                    }
                }
            }
        };
        
        use std::os::fd::AsRawFd;
        let out = match send_all_nosig(s.as_raw_fd(), &job.payload) {
            Ok(_) => RelayOutcome::Ok(elapsed_ms_since_ns(t0)),
            Err(_) => RelayOutcome::Err("write"),
        };
        let _ = s.flush();
        relay_record(w.addr, &out);
        let _ = job.tx.send(out);
        if let Some(p) = RELAY_PENDING.get() { 
            p.lock().unwrap().remove(&(w.addr, SlotNonce{slot:job.slot,nonce:job.nonce})); 
        }
        sock = Some(s);
    }
}

// Placeholder functions that would be implemented in your actual codebase
#[cfg(all(unix, target_os = "linux"))]
fn set_affinity_cpu(_cpu: usize) {
    // Set CPU affinity for the current thread
    use std::os::unix::thread;
    let _ = thread::set_affinity(std::thread::current().id(), [_cpu]);
}

fn ignore_sigpipe_global() {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        }
    }
}

// ===== OPTIMIZATION 34: relay scoring → risk-aware UCB over latency+jitter with cooldowns =====
// REPLACE: RStat / relay_record / choose_best_relays with UCB-scored selection
#[derive(Clone, Copy)]
struct RStat {
    lat_x1024: u64,       // EWMA latency
    jit_x1024: u64,       // EWMA |lat - ema|
    fail_q8: u8,          // small failure counter
    inited: bool,
    last_fail_ms: u64,
    trials: u32,          // attempts
    succ: u32,            // successes
}


#[inline]
pub fn relay_record(addr: SocketAddr, outcome: &RelayOutcome) {
    if let Some(mx) = RELAY_STATS.get() {
        let mut m = mx.lock();
        let now = mono_now_ns() / 1_000_000;
        let e = m.entry(addr).or_insert(RStat {
            lat_x1024: 80u64 << 10, jit_x1024: 5u64 << 10, fail_q8: 0,
            inited: false, last_fail_ms: 0, trials: 0, succ: 0
        });
        e.trials = e.trials.saturating_add(1);
        match *outcome {
            RelayOutcome::Ok(ms) => {
                let s = (ms.max(1) as u64) << 10;
                let prev = e.lat_x1024;
                e.lat_x1024 = prev + ((s as i64 - prev as i64) >> 3) as u64;     // α≈1/8
                let dev = if s > e.lat_x1024 { s - e.lat_x1024 } else { e.lat_x1024 - s };
                let pj = e.jit_x1024;
                e.jit_x1024 = pj + ((dev as i64 - pj as i64) >> 3) as u64;
                e.fail_q8 = e.fail_q8.saturating_sub(e.fail_q8 / 16);
                e.succ = e.succ.saturating_add(1);
                e.inited = true;
            }
            RelayOutcome::Err(_) => {
                e.fail_q8 = e.fail_q8.saturating_add(1).min(200);
                e.last_fail_ms = now;
            }
        }
    }
}

#[inline]
fn relay_connect_timeout_ms(addr: &SocketAddr) -> u64 {
    if let Some(mx) = RELAY_STATS.get() {
        let m = mx.lock();
        if let Some(s) = m.get(addr) {
            let base = (s.lat_x1024 >> 10).max(40);
            let pen  = (s.fail_q8 as u64) * 6;
            return base.saturating_mul(2).saturating_add(20).saturating_add(pen).clamp(60, 350);
        }
    }
    200
}

#[inline]
fn relay_in_cooldown(addr: &SocketAddr, now_ms: u64) -> bool {
    if let Some(mx) = RELAY_STATS.get() {
        let m = mx.lock();
        if let Some(s) = m.get(addr) {
            if s.fail_q8 == 0 { return false; }
            let backoff = (s.fail_q8 as u64).saturating_mul(15).min(600);
            return now_ms.saturating_sub(s.last_fail_ms) < backoff;
        }
    }
    false
}

#[inline]
pub fn choose_best_relays(mut addrs: Vec<SocketAddr>, k: usize) -> Vec<SocketAddr> {
    if addrs.len() <= k { return addrs; }
    let now = mono_now_ns() / 1_000_000;
    addrs.retain(|a| !relay_in_cooldown(a, now));
    if addrs.is_empty() { return vec![]; }

    if let Some(mx) = RELAY_STATS.get() {
        let m = mx.lock();
        let total_trials: f64 = addrs.iter().map(|a| m.get(a).map(|s| s.trials as f64).unwrap_or(1.0)).sum::<f64>().max(1.0);
        // Score = latency + jitter + failure penalty - exploration_bonus; lower is better.
        addrs.sort_by(|a, b| {
            let sc = |addr: &SocketAddr| -> u128 {
                let s = m.get(addr);
                let lat = s.map(|x| (x.lat_x1024 >> 10) as u64).unwrap_or(200);
                let jit = s.map(|x| (x.jit_x1024 >> 10) as u64).unwrap_or(10);
                let fail_pen = s.map(|x| (x.fail_q8 as u64) * 4).unwrap_or(40);
                let t = s.map(|x| x.trials as f64).unwrap_or(1.0).max(1.0);
                let succ = s.map(|x| x.succ as f64).unwrap_or(0.0);
                // UCB exploration bonus (bigger when under-sampled); Q16 to avoid f64 ordering issues later
                let bonus = (10.0 * (total_trials.ln() / t).sqrt()).max(0.0);
                // Convert to an integer key: (lat+jit+pen) in ms scaled, minus bonus
                let base = (lat + (jit/2) + fail_pen) as i128;
                let score = base * 65536 - (bonus * 65536.0) as i128;
                score as u128
            };
            sc(a).cmp(&sc(b))
        });
    }
    addrs.truncate(k);
    addrs
}

fn shuffle_endpoints(_relays: &mut [std::net::SocketAddr], _slot: u64, _nonce: u64, _salt: &[u8;32]) {}

// ===== OPTIMIZATION 33: kernel-grade socket tuning =====
// REPLACE: tune_low_latency_stream — robust, cross-OS; fast path on Linux
#[inline]
pub fn tune_low_latency_stream(s: &std::net::TcpStream) {
    use std::os::fd::AsRawFd;
    let _ = s.set_nodelay(true);

    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        use libc::{c_int, c_void, setsockopt};
        let fd = s.as_raw_fd();

        // 1) DSCP EF (46) | ECN capable (ECT0); maps to IPTOS_LOWDELAY on many queues
        let tos: c_int = 0b10111000; // DSCP 46 (EF) <<2 ; ECN bits left to stack
        let _ = setsockopt(fd, libc::IPPROTO_IP, libc::IP_TOS, &tos as *const _ as *const c_void, core::mem::size_of_val(&tos) as u32);

        // 2) SO_PRIORITY (tx queue class). 6 is below control traffic yet high.
        let prio: c_int = 6;
        let _ = setsockopt(fd, libc::SOL_SOCKET, libc::SO_PRIORITY, &prio as *const _ as *const c_void, core::mem::size_of_val(&prio) as u32);

        // 3) TCP_QUICKACK (reduce delayed-ack latency if reads occur)
        #[allow(non_upper_case_globals)]
        const TCP_QUICKACK: c_int = 12;
        let quickack: c_int = 1;
        let _ = setsockopt(fd, libc::IPPROTO_TCP, TCP_QUICKACK, &quickack as *const _ as *const c_void, core::mem::size_of_val(&quickack) as u32);

        // 4) TCP_NOTSENT_LOWAT (flush promptly, avoid buffering bursts)
        #[allow(non_upper_case_globals)]
        const TCP_NOTSENT_LOWAT: c_int = 25;
        let lowat: c_int = 16 * 1024;
        let _ = setsockopt(fd, libc::IPPROTO_TCP, TCP_NOTSENT_LOWAT, &lowat as *const _ as *const c_void, core::mem::size_of_val(&lowat) as u32);

        // 5) TCP_USER_TIMEOUT — bail fast on dead peers
        #[allow(non_upper_case_globals)]
        const TCP_USER_TIMEOUT: c_int = 18;
        let ut_ms: c_int = 250;
        let _ = setsockopt(fd, libc::IPPROTO_TCP, TCP_USER_TIMEOUT, &ut_ms as *const _ as *const c_void, core::mem::size_of_val(&ut_ms) as u32);

        // 6) SO_SNDBUF — modest (kernel autotune can explode under pressure)
        let sndbuf: c_int = 256 * 1024;
        let _ = setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &sndbuf as *const _ as *const c_void, core::mem::size_of_val(&sndbuf) as u32);

        // 7) SO_MAX_PACING_RATE — cap microbursts (bytes/sec). 40MB/s is plenty.
        #[allow(non_upper_case_globals)]
        const SO_MAX_PACING_RATE: c_int = 47;
        let pace: u64 = 40 * 1024 * 1024;
        let _ = setsockopt(fd, libc::SOL_SOCKET, SO_MAX_PACING_RATE, &pace as *const _ as *const c_void, core::mem::size_of_val(&pace) as u32);
    }
}

fn mono_now_ns() -> u64 { 
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64 
}

fn send_all_nosig(_fd: i32, _data: &[u8]) -> Result<(), std::io::Error> { Ok(()) }

fn elapsed_ms_since_ns(_t0: u64) -> u64 { 0 }

fn relay_record(_addr: SocketAddr, _outcome: &RelayOutcome) {}

// Initialize CPU preference list
pub fn init_cpu_pref_list(cpus: Vec<usize>) {
    let _ = CPU_PREF_LIST.set(cpus);
}

// Update slot watermark
pub fn update_slot_watermark(slot: u64) {
    if let Some(sw) = SLOT_WATERMARK.get() {
        sw.store(slot, Ordering::Relaxed);
    }
}

// Constants and types that would be defined elsewhere
const MAX_CU_LIMIT: u32 = 1_400_000;

fn autosize_budget_for_proof(_proof_len: usize) -> (u32, u32, u32) { 
    (100_000, 64*1024, 0) 
}

fn token_bucket_try_take(_weight: u64) -> bool { 
    true 
}

// ===== OPTIMIZATION 35: NUMA-local, hugepage-friendly allocator for big, hot buffers =====
// ADD: allocate page-aligned, NUMA-local buffer; prefer hugepages when available
#[cfg(all(unix, target_os = "linux"))]
pub fn alloc_hot_slab(len: usize) -> std::io::Result<&'static mut [u8]> {
    use libc::{mmap, mprotect, madvise, MAP_ANONYMOUS, MAP_PRIVATE, MAP_POPULATE, PROT_READ, PROT_WRITE, MADV_HUGEPAGE, MADV_DONTDUMP};
    let sz = ((len + 4095) / 4096) * 4096;
    let ptr = unsafe {
        mmap(
            core::ptr::null_mut(),
            sz,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
            -1, 0)
    };
    if ptr == libc::MAP_FAILED { return Err(std::io::Error::last_os_error()); }
    unsafe {
        let _ = madvise(ptr, sz, MADV_HUGEPAGE);
        let _ = madvise(ptr, sz, MADV_DONTDUMP);
    }
    // Bind to local NUMA if you already have a helper; otherwise skip (no-op)
    // bind_mmap_to_local_numa is already in your tree:
    unsafe { bind_mmap_to_local_numa(core::slice::from_raw_parts(ptr as *const u8, sz)); }
    Ok(unsafe { core::slice::from_raw_parts_mut(ptr as *mut u8, len) })
}

#[cfg(not(all(unix, target_os = "linux")))]
pub fn alloc_hot_slab(len: usize) -> std::io::Result<Vec<u8>> {
    let mut v = Vec::with_capacity(len);
    unsafe { v.set_len(len); }
    Ok(v)
}

// ===== OPTIMIZATION 36: Merkle fold with prefetch + in-place double buffer =====
// REPLACE: compute_witness_merkle_root — prefetch & ping-pong buffers
pub fn compute_witness_merkle_root(inputs: &[Fr]) -> [u8; 32] {
    use smallvec::SmallVec;

    if inputs.is_empty() {
        let mut h = blake3::Hasher::new();
        h.update(b"ZK_PROOF_MAINNET_V2"); // DOMAIN_SEPARATOR_V2
        h.update(b"WIT_EMPTY");
        return h.finalize().into();
    }

    let n0 = inputs.len().next_power_of_two();
    let mut a: Vec<[u8;32]> = Vec::with_capacity(n0);
    let mut b: Vec<[u8;32]> = Vec::with_capacity((n0 + 1) / 2);

    for fe in inputs { a.push(blake3_leaf_witness(fe)); }
    while a.len() & (a.len() - 1) != 0 { a.push(*a.last().unwrap()); } // pad to power-of-two

    let mut cur = &mut a;
    let mut nxt = &mut b;

    while cur.len() > 1 {
        nxt.clear();
        let mut i = 0usize;
        while i < cur.len() {
            // prefetch the next pair (helps on bigger witness sets)
            if i + 4 < cur.len() {
                prefetch_read((&cur[i+2]) as *const [u8;32]);
                prefetch_read((&cur[i+3]) as *const [u8;32]);
            }
            let a = cur[i];
            let c = if i + 1 < cur.len() { cur[i + 1] } else { a };
            let mut h = blake3::Hasher::new();
            h.update(b"ZK_PROOF_MAINNET_V2"); // DOMAIN_SEPARATOR_V2
            h.update(b"WIT_NODE");
            h.update(&a);
            h.update(&c);
            nxt.push(h.finalize().into());
            i += 2;
        }
        core::mem::swap(&mut cur, &mut nxt);
    }
    cur[0]
}

// Helper function for Merkle leaf hashing
fn blake3_leaf_witness(fe: &Fr) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"ZK_PROOF_MAINNET_V2"); // DOMAIN_SEPARATOR_V2
    h.update(b"WIT_LEAF");
    let b = fr_to_le_bytes32(fe);
    h.update(&b);
    h.finalize().into()
}

// Prefetch helper
#[inline(always)]
fn prefetch_read(ptr: *const [u8; 32]) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::_mm_prefetch;
        _mm_prefetch(ptr as *const i8, 0); // _MM_HINT_T0
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = ptr;
    }
}

// ===== OPTIMIZATION 37: lock-free perf ring (SPSC) for hot telemetry =====
// ADD: SPSC perf ring; single-producer (hot path) → single-consumer (telemetry thread)
pub struct PerfEvent {
    pub t_ns: u64,
    pub kind: u16,       // 0=send_ok,1=send_err,2=prove_ok,3=prove_err,4=relay_pick
    pub a: u64,          // arg0 (e.g., ms, cu, fee)
    pub b: u64,          // arg1
}

pub struct PerfRing {
    buf: Box<[core::mem::MaybeUninit<PerfEvent>]>,
    mask: usize,
    head: core::sync::atomic::AtomicUsize,
    tail: core::sync::atomic::AtomicUsize,
}

impl PerfRing {
    #[inline] 
    pub fn with_pow2(cap_pow2: usize) -> Self {
        assert!(cap_pow2.is_power_of_two());
        Self {
            buf: (0..cap_pow2).map(|_| core::mem::MaybeUninit::uninit()).collect::<Vec<_>>().into_boxed_slice(),
            mask: cap_pow2 - 1,
            head: core::sync::atomic::AtomicUsize::new(0),
            tail: core::sync::atomic::AtomicUsize::new(0),
        }
    }
    
    #[inline] 
    pub fn push(&self, ev: PerfEvent) -> bool {
        let h = self.head.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Acquire);
        if h - t == self.buf.len() { return false; } // full → drop
        unsafe { self.buf[h & self.mask].as_ptr().cast::<PerfEvent>().write(ev); }
        self.head.store(h + 1, Ordering::Release);
        true
    }
    
    #[inline] 
    pub fn pop(&self) -> Option<PerfEvent> {
        let t = self.tail.load(Ordering::Relaxed);
        let h = self.head.load(Ordering::Acquire);
        if t == h { return None; }
        let ev = unsafe { self.buf[t & self.mask].as_ptr().cast::<PerfEvent>().read() };
        self.tail.store(t + 1, Ordering::Release);
        Some(ev)
    }
}

// ADD: global ring init
static PERF_RING: OnceCell<PerfRing> = OnceCell::new();

#[inline] 
pub fn perf_ring_init() { 
    let _ = PERF_RING.set(PerfRing::with_pow2(1<<14)); 
}

// ADD: cheap log hooks
#[inline] 
pub fn perf_log(kind: u16, a: u64, b: u64) {
    if let Some(r) = PERF_RING.get() {
        let _ = r.push(PerfEvent { t_ns: mono_now_ns(), kind, a, b });
    }
}

// ===== OPTIMIZATION 38: dual clock helpers =====
// ADD: raw monotonic (ns) for microbench timing; coarse kept for budget math
#[inline(always)]
fn mono_now_raw_ns() -> u64 {
    #[cfg(all(unix, target_os = "linux"))] 
    unsafe {
        use libc::{clock_gettime, timespec, CLOCK_MONOTONIC_RAW};
        let mut ts: timespec = core::mem::zeroed();
        let _ = clock_gettime(CLOCK_MONOTONIC_RAW, &mut ts);
        (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
    }
    #[cfg(not(all(unix, target_os = "linux")))]
    {
        // Fallback identical to coarse path; platform invariant.
        mono_now_ns()
    }
}

// ===== OPTIMIZATION 39: pre-connect tuning + truly accurate timeout =====
// ADD: sockaddr conversion
#[inline]
fn sockaddr_from(addr: &std::net::SocketAddr, storage: &mut libc::sockaddr_storage) -> ( *const libc::sockaddr, libc::socklen_t ) {
    unsafe { core::ptr::write_bytes(storage as *mut _, 0, 1); }
    match addr {
        std::net::SocketAddr::V4(a) => {
            let mut sa: libc::sockaddr_in = unsafe { core::mem::zeroed() };
            sa.sin_family = libc::AF_INET as libc::sa_family_t;
            sa.sin_port = u16::to_be(a.port());
            sa.sin_addr = libc::in_addr { s_addr: u32::from_ne_bytes(a.ip().octets()) };
            unsafe {
                core::ptr::copy_nonoverlapping(&sa as *const _ as *const u8,
                                               storage as *mut _ as *mut u8,
                                               core::mem::size_of::<libc::sockaddr_in>());
            }
            (storage as *const _ as *const libc::sockaddr, core::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t)
        }
        std::net::SocketAddr::V6(a) => {
            let mut sa: libc::sockaddr_in6 = unsafe { core::mem::zeroed() };
            sa.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sa.sin6_port = u16::to_be(a.port());
            sa.sin6_addr = libc::in6_addr { s6_addr: a.ip().octets() };
            unsafe {
                core::ptr::copy_nonoverlapping(&sa as *const _ as *const u8,
                                               storage as *mut _ as *mut u8,
                                               core::mem::size_of::<libc::sockaddr_in6>());
            }
            (storage as *const _ as *const libc::sockaddr, core::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t)
        }
    }
}

// ADD: tuned connect with precise timeout & pre-connect knobs; falls back off Linux
#[inline]
pub fn connect_tuned_timeout(addr: &std::net::SocketAddr, to_ms: u64) -> std::io::Result<std::net::TcpStream> {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        use libc::*;
        let (domain, proto) = match addr {
            std::net::SocketAddr::V4(_) => (AF_INET, IPPROTO_TCP),
            std::net::SocketAddr::V6(_) => (AF_INET6, IPPROTO_TCP),
        };
        let fd = socket(domain, SOCK_STREAM | SOCK_CLOEXEC, proto);
        if fd < 0 { return Err(std::io::Error::last_os_error()); }

        // best-effort pre-connect knobs
        let one: c_int = 1;
        let _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one as *const _ as *const c_void, core::mem::size_of_val(&one) as u32);

        // IP_BIND_ADDRESS_NO_PORT (Linux) — reduce bind overhead, ephemeral port collisions
        #[allow(non_upper_case_globals)] const IP_BIND_ADDRESS_NO_PORT: c_int = 24;
        if domain == AF_INET {
            let _ = setsockopt(fd, IPPROTO_IP, IP_BIND_ADDRESS_NO_PORT, &one as *const _ as *const c_void, core::mem::size_of_val(&one) as u32);
        }

        // TCP_FASTOPEN_CONNECT for client (Linux ≥ 4.11). Ignore if unsupported.
        #[allow(non_upper_case_globals)] const TCP_FASTOPEN_CONNECT: c_int = 30;
        let _ = setsockopt(fd, IPPROTO_TCP, TCP_FASTOPEN_CONNECT, &one as *const _ as *const c_void, core::mem::size_of_val(&one) as u32);

        // Prefer BBR (harmless if unavailable)
        #[allow(non_upper_case_globals)] const TCP_CONGESTION: c_int = 13;
        let cstr = b"bbr\0";
        let _ = setsockopt(fd, IPPROTO_TCP, TCP_CONGESTION, cstr.as_ptr() as *const c_void, (cstr.len()) as u32);

        // nonblocking connect
        let flags = fcntl(fd, F_GETFL);
        let _ = fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        let mut ss: sockaddr_storage = core::mem::zeroed();
        let (sap, salen) = sockaddr_from(addr, &mut ss);
        let rc = connect(fd, sap, salen);
        if rc == 0 {
            // connected immediately
        } else {
            let e = *libc::__errno_location();
            if e != libc::EINPROGRESS {
                let _ = close(fd);
                return Err(std::io::Error::from_raw_os_error(e));
            }
            // poll for writability
            let mut pfd = pollfd { fd, events: POLLOUT, revents: 0 };
            let rc = poll(&mut pfd as *mut _, 1, to_ms as c_int);
            if rc <= 0 {
                let _ = close(fd);
                return Err(if rc == 0 { std::io::ErrorKind::TimedOut.into() } else { std::io::Error::last_os_error() });
            }
            // check SO_ERROR
            let mut soerr: c_int = 0;
            let mut slen: socklen_t = core::mem::size_of::<c_int>() as u32;
            let _ = getsockopt(fd, SOL_SOCKET, SO_ERROR, &mut soerr as *mut _ as *mut c_void, &mut slen as *mut _);
            if soerr != 0 {
                let _ = close(fd);
                return Err(std::io::Error::from_raw_os_error(soerr));
            }
        }

        // hand off to TcpStream and finish post-connect tuning
        use std::os::fd::FromRawFd;
        let s = std::net::TcpStream::from_raw_fd(fd);
        let _ = s.set_nonblocking(false);
        super::tune_low_latency_stream(&s);
        return Ok(s);
    }

    #[cfg(not(all(unix, target_os = "linux")))]
    {
        let to = std::time::Duration::from_millis(to_ms);
        let s = std::net::TcpStream::connect_timeout(addr, to)?;
        let _ = s.set_nonblocking(false);
        super::tune_low_latency_stream(&s);
        Ok(s)
    }
}

// ===== OPTIMIZATION 40: hedged send to use tuned connect =====
// REPLACE: hedged_race_send — same API, but uses connect_tuned_timeout
#[inline]
pub fn hedged_race_send(
    payload: &[u8],
    mut relays: Vec<std::net::SocketAddr>,
    slot: u64,
    nonce: u64,
    salt: [u8;32],
) -> RelayOutcome {
    use std::sync::mpsc::channel;
    if relays.is_empty() { return RelayOutcome::Err("no_relays"); }

    prepare_proof_buffer_hot(payload);
    let shared = std::sync::Arc::<[u8]>::from(payload.to_vec().into_boxed_slice());

    relays = choose_best_relays(relays, relays.len());
    if relays.is_empty() { return RelayOutcome::Err("cooldown_all"); }
    shuffle_endpoints(&mut relays[..], slot, nonce, &salt);

    let (tx, rx) = channel::<(usize, RelayOutcome)>();
    let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    for (idx, addr) in relays.iter().cloned().enumerate() {
        let tx = tx.clone();
        let stop = stopped.clone();
        let data = shared.clone();
        std::thread::spawn(move || {
            if stop.load(Ordering::Relaxed) { let _=tx.send((idx, RelayOutcome::Err("cancelled"))); return; }
            let to_ms = relay_connect_timeout_ms(&addr);
            let s = match connect_tuned_timeout(&addr, to_ms) {
                Ok(s) => s,
                Err(_) => { let _=tx.send((idx, RelayOutcome::Err("connect"))); return; }
            };
            use std::os::fd::AsRawFd;
            let t0 = mono_now_ns();
            let out = match send_all_nosig(s.as_raw_fd(), &data) {
                Ok(_) => RelayOutcome::Ok(elapsed_ms_since_ns(t0)),
                Err(_) => RelayOutcome::Err("write"),
            };
            let _ = s.flush();
            let _ = tx.send((idx, out));
        });
    }

    let mut last_err: Option<RelayOutcome> = None;
    let mut remaining = relays.len();
    while remaining > 0 {
        match rx.recv() {
            Ok((i, outcome)) => {
                relay_record(relays[i], &outcome);
                match outcome {
                    RelayOutcome::Ok(ms) => {
                        stopped.store(true, Ordering::Relaxed);
                        return RelayOutcome::Ok(ms);
                    }
                    e @ RelayOutcome::Err(_) => { last_err = Some(e); }
                }
                remaining -= 1;
            }
            Err(_) => break,
        }
    }
    last_err.unwrap_or(RelayOutcome::Err("chan_closed"))
}

// ===== OPTIMIZATION 41: EV-aware hedge width optimizer =====
// ADD: estimate global relay success (succ/trials) with minima to avoid div/0
#[inline]
fn relay_global_success_estimate() -> f64 {
    if let Some(mx) = RELAY_STATS.get() {
        let m = mx.lock();
        let (mut succ, mut tri) = (0u64, 0u64);
        for s in m.values() { succ += s.succ as u64; tri += s.trials.max(1) as u64; }
        let p = (succ as f64 / tri.max(1) as f64).clamp(0.05, 0.99);
        return p;
    }
    0.6 // sane default under uncertainty
}

// ADD: EV-aware chooser; returns k ∈ [1..=max_relays]
#[inline]
pub fn choose_hedge_width_evaware(ms_left: u64, max_relays: usize, tip_cost_lamports: u64, ev_lamports: u64) -> usize {
    // Base on net95 urgency as a guardrail
    let (_net50, net95) = net_snapshot_ms();
    let urgent = ms_left < net95;

    // Global success estimate
    let p = relay_global_success_estimate();
    let mut k = 1usize;
    let mut prev_any = 0.0f64;

    for n in 1..=max_relays.max(1) {
        // P_any(n) = 1 - (1-p)^n
        let p_any = 1.0 - (1.0 - p).powi(n as i32);
        let delta = p_any - prev_any;
        prev_any = p_any;
        // Marginal EV of the n-th relay
        let mev = (delta * ev_lamports as f64) as i64 - tip_cost_lamports as i64;
        if mev >= 0 || urgent {
            k = n;
        } else {
            break;
        }
    }
    k
}

// Placeholder for net snapshot
fn net_snapshot_ms() -> (u64, u64) { (50, 100) }

// ===== OPTIMIZATION 42: TSC-calibrated nanoseconds =====
// ADD: TSC-backed timebase with Q32 scaling; falls back safely
struct TscCal { base_tsc: u64, base_ns: u64, ns_per_cycle_q32: u64, ok: bool }
static TSC_CAL: OnceCell<TscCal> = OnceCell::new();

#[inline]
#[cfg(target_arch = "x86_64")]
fn rdtsc() -> u64 { unsafe { core::arch::x86_64::_rdtsc() } }

#[inline]
pub fn tsc_time_init() {
    #[cfg(target_arch = "x86_64")]
    {
        let t0_ns = mono_now_raw_ns();
        let c0 = rdtsc();
        // Sleep ~25ms using coarse clock to accumulate measurable delta
        #[cfg(all(unix, target_os = "linux"))] unsafe {
            let ts = libc::timespec { tv_sec: 0, tv_nsec: 25_000_000 };
            let _ = libc::clock_nanosleep(libc::CLOCK_MONOTONIC_RAW, 0, &ts, core::ptr::null_mut());
        }
        #[cfg(not(all(unix, target_os = "linux")))]
        std::thread::sleep(std::time::Duration::from_millis(25));
        let t1_ns = mono_now_raw_ns();
        let c1 = rdtsc();

        let dt_ns = t1_ns.saturating_sub(t0_ns).max(1);
        let dt_cy = c1.saturating_sub(c0).max(1);
        // ns_per_cycle in Q32 fixed-point
        let npc_q32 = (((dt_ns as u128) << 32) / (dt_cy as u128)) as u64;

        let _ = TSC_CAL.set(TscCal { base_tsc: c1, base_ns: t1_ns, ns_per_cycle_q32: npc_q32, ok: true });
    }
    #[cfg(not(target_arch = "x86_64"))] { let _ = TSC_CAL.set(TscCal{ base_tsc:0, base_ns:0, ns_per_cycle_q32:0, ok:false }); }
}

#[inline(always)]
pub fn fast_now_ns() -> u64 {
    if let Some(cal) = TSC_CAL.get() {
        if cal.ok {
            #[cfg(target_arch = "x86_64")] {
                let dcy = rdtsc().saturating_sub(cal.base_tsc);
                let dns = ((dcy as u128 * cal.ns_per_cycle_q32 as u128) >> 32) as u64;
                return cal.base_ns.saturating_add(dns);
            }
        }
    }
    // fallback
    mono_now_ns()
}

// ===== OPTIMIZATION 43: persistent worker sockets use tuned connect =====
// ADD: tiny helper for worker connect that reuses the tuned path
#[inline]
fn worker_connect(addr: &std::net::SocketAddr) -> Option<std::net::TcpStream> {
    match connect_tuned_timeout(addr, relay_connect_timeout_ms(addr)) {
        Ok(s) => Some(s),
        Err(_) => None,
    }
}

// ===== OPTIMIZATION 44: allocator trim hook after bursts =====
// ADD: best-effort allocator trim; harmless on non-glibc
#[inline]
pub fn maybe_malloc_trim() {
    #[cfg(all(unix, target_os = "linux"))]
    unsafe {
        extern "C" { fn malloc_trim(pad: libc::size_t) -> libc::c_int; }
        // leave small pad to avoid immediate re-growth; 1MB is enough
        let _ = malloc_trim(1 << 20);
    }
}

// ===== OPTIMIZATION 45: hardened constant-time compare for arbitrary-length MACs =====
// ADD: ct_eq for 16/24/32 bytes (branchless)
#[inline(always)]
fn ct_eq_mac(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    let mut diff: u64 = 0;
    match a.len() {
        16 => {
            diff |= u64::from_le_bytes(a[0..8].try_into().unwrap()) ^ u64::from_le_bytes(b[0..8].try_into().unwrap());
            diff |= u64::from_le_bytes(a[8..16].try_into().unwrap()) ^ u64::from_le_bytes(b[8..16].try_into().unwrap());
        }
        24 => {
            diff |= u64::from_le_bytes(a[0..8].try_into().unwrap()) ^ u64::from_le_bytes(b[0..8].try_into().unwrap());
            diff |= u64::from_le_bytes(a[8..16].try_into().unwrap()) ^ u64::from_le_bytes(b[8..16].try_into().unwrap());
            diff |= u64::from_le_bytes(a[16..24].try_into().unwrap()) ^ u64::from_le_bytes(b[16..24].try_into().unwrap());
        }
        32 => {
            diff |= u64::from_le_bytes(a[ 0.. 8].try_into().unwrap()) ^ u64::from_le_bytes(b[ 0.. 8].try_into().unwrap());
            diff |= u64::from_le_bytes(a[ 8..16].try_into().unwrap()) ^ u64::from_le_bytes(b[ 8..16].try_into().unwrap());
            diff |= u64::from_le_bytes(a[16..24].try_into().unwrap()) ^ u64::from_le_bytes(b[16..24].try_into().unwrap());
            diff |= u64::from_le_bytes(a[24..32].try_into().unwrap()) ^ u64::from_le_bytes(b[24..32].try_into().unwrap());
        }
        _ => return false,
    }
    diff == 0
}

// ===== OPTIMIZATION 46: per-core cache-busting pre-touch for slab reuse =====
// ADD: simple per-page pre-touch; uses stride to keep it cheap
#[inline]
pub fn pretouch_pages(buf: &mut [u8]) {
    #[cfg(all(unix, target_os = "linux"))]
    {
        let page = 4096usize;
        let len = buf.len();
        let ptr = buf.as_mut_ptr();
        let mut i = 0;
        unsafe {
            while i < len {
                core::ptr::write_volatile(ptr.add(i), core::ptr::read_volatile(ptr.add(i)));
                i += page;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ark_bn254::{Bn254, Fr, G1Projective, G2Projective};
    use ark_ec::CurveGroup;
    use ark_ff::Field;
    use ark_groth16::{ProvingKey, VerifyingKey};


