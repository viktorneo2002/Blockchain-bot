// ========================= PATCH 24: route_cost_scurve module =========================
#[cfg(feature = "route_cost_scurve")]
pub mod route_cost_scurve {
    use std::env;

    pub fn env_u64(key: &str, default: u64) -> u64 {
        env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    pub fn env_i32(key: &str, default: i32) -> i32 {
        env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
    }

    // Saturating, smooth S-curve: credit_ns = CAP * (1 - exp(- (profit * slope) / CAP))
    pub fn profit_ns_credit_scurve(profit_lamports: u64, slope_ns_per_lamport: u64) -> u64 {
        const CAP: u64 = 1_000_000; // 1ms max credit per tx
        if profit_lamports == 0 { return 0; }
        let scaled = (profit_lamports as u128).saturating_mul(slope_ns_per_lamport as u128);
        let exp_arg = -(scaled as f64 / CAP as f64);
        let credit = CAP as f64 * (1.0 - exp_arg.exp());
        credit as u64
    }

    // Phase basis-points given a predicted FINISH timestamp (us within slot)
    pub fn phase_bp_finish_us(finish_us: u64) -> i32 {
        // Slot timing constants (us)
        const SLOT_START: u64 = 0;
        const SLOT_MID: u64 = 400_000;  // 400ms
        const SLOT_TAIL: u64 = 450_000; // 450ms
        const SLOT_END: u64 = 500_000;  // 500ms

        // Phase thresholds (basis points, 0-10_000)
        const PHASE_EARLY_BP: i32 = 200;    // 2% into slot
        const PHASE_MID_BP: i32 = 8000;     // 80% into slot
        const PHASE_TAIL_BP: i32 = 9000;    // 90% into slot

        let phase = if finish_us < SLOT_MID {
            // Early phase: linear ramp from 0 to PHASE_MID_BP
            let progress = (finish_us - SLOT_START) as f64 / (SLOT_MID - SLOT_START) as f64;
            (progress * PHASE_MID_BP as f64) as i32
        } else if finish_us < SLOT_TAIL {
            // Mid phase: constant at PHASE_MID_BP
            PHASE_MID_BP
        } else if finish_us < SLOT_END {
            // Tail phase: linear ramp from PHASE_MID_BP to PHASE_TAIL_BP
            let progress = (finish_us - SLOT_TAIL) as f64 / (SLOT_END - SLOT_TAIL) as f64;
            PHASE_MID_BP + (progress * (PHASE_TAIL_BP - PHASE_MID_BP) as f64) as i32
        } else {
            // End phase: linear penalty beyond SLOT_END
            let over_ms = (finish_us - SLOT_END) / 1_000;
            PHASE_TAIL_BP - (over_ms * 10) as i32  // -10bp per ms over
        };

        phase.clamp(-10_000, 10_000)  // Clamp to ±100%
    }

    // Back-compat alias used by older call sites; keeps API stable.
    #[inline]
    pub fn phase_bp_predicted_finish(now_us: u64) -> i32 {
        phase_bp_finish_us(now_us)
    }

    // ========================= PATCH 36: route_cost::price (REPLACE) =========================
    #[inline]
    pub fn price(wait_ns: u64, cwnd_head: i32, profit_ns_credit: u64) -> i128 {
        use core::cmp::max;

        // Base components (signed i128 to keep monotonic arithmetic)
        let mut w = (*W_WAIT * wait_ns as f64) as i128;
        let c = if cwnd_head < 0 {
            (*W_CWND * (-(cwnd_head as f64))) as i128
        } else { 0 };

        // Profit credit (already time-priced via scurve)
        let mut p = (*W_PROFIT * profit_ns_credit as f64) as i128;

        // Phase bps at predicted FINISH time (finish ≈ now + wait)
        let finish_us = crate::clock::now_us().saturating_add(wait_ns / 1_000);
        let phase_bp  = phase_bp_finish_us(finish_us) as i128;

        // Apply bps skew to profit (existing behavior)
        p = p + (p.saturating_mul(phase_bp)) / 10_000;

        // NEW: tail penalty also boosts the *weight of waiting* near slot end.
        // Gain factor in bps (default 100bp). Only applies to negative bps (penalty).
        let gain_bp: i128 = std::env::var("QAB_TAIL_WAIT_BOOST_BP")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(100) as i128;

        if phase_bp < 0 && gain_bp > 0 {
            // w += w * (|phase_bp| * gain_bp) / 1e8   (two bps factors)
            let bump = (w.saturating_mul((-phase_bp) * gain_bp)) / 100_000_000;
            w = w.saturating_add(bump);
        }

        // Lower is better
        w + c - p
    }
}

// ========================= PATCH 20: clock::cycles helpers (ADD) =========================
// ========================= PATCH 30: clock::cycles drift-guard (REPLACE + ADD periodic resync) =========================
#[cfg(all(target_os="linux", target_arch="x86_64"))]
#[inline]
pub fn now_cycles() -> u64 { 
    unsafe { core::arch::x86_64::_rdtsc() } 
}

// ========================= PATCH 57: triple_cache (REPLACE module) =========================
pub mod triple_cache {
    use once_cell::sync::Lazy;
    use std::collections::HashMap;
    use std::sync::RwLock;
    use core::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    #[inline] 
    fn shards() -> usize {
        std::env::var("QAB_TRIPLE_CACHE_SHARDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64)
            .clamp(1, 1024)
    }

    #[inline]
    fn max_entries() -> usize {
        std::env::var("QAB_TRIPLE_CACHE_MAX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(262_144)
            .max(1024)
    }

    #[inline]
    fn ttl_us() -> u64 {
        std::env::var("QAB_TRIPLE_CACHE_TTL_US")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(250_000)
            .max(10_000)
    }

    struct Shard {
        map: RwLock<HashMap<u64, (u64, u8)>>,
        ins: AtomicUsize,
    }

    static SHARDS: Lazy<Box<[Shard]>> = Lazy::new(|| {
        let n = shards();
        (0..n)
            .map(|_| Shard { 
                map: RwLock::new(HashMap::new()), 
                ins: AtomicUsize::new(0) 
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
    });

    #[inline] 
    fn pick(hv: u64) -> &'static Shard {
        let mask = SHARDS.len() as u64 - 1;
        // round up to power-of-two if not already; simple fallback to modulo
        let idx = if SHARDS.len().is_power_of_two() {
            (hv & mask) as usize
        } else {
            (hv % (SHARDS.len() as u64)) as usize
        };
        &SHARDS[idx]
    }

    #[inline]
    pub fn get(now_us: u64, hv: u64) -> Option<bool> {
        let s = pick(hv);
        let g = s.map.read().unwrap();
        if let Some(&(exp, v)) = g.get(&hv) {
            if now_us <= exp { return Some(v != 0); }
        }
        None
    }

    #[inline]
    pub fn put(now_us: u64, hv: u64, verdict_ok: bool) {
        let s = pick(hv);
        let exp = now_us.saturating_add(ttl_us());
        {
            let mut w = s.map.write().unwrap();
            w.insert(hv, (exp, if verdict_ok {1} else {0}));
            // Per-shard opportunistic GC
            if s.ins.fetch_add(1, Relaxed) & 0x7FF == 0 {
                let cap = max_entries() / SHARDS.len().max(1);
                if w.len() > cap {
                    let want = w.len() - cap;
                    let now = now_us;
                    let mut freed = 0usize;
                    let keys: Vec<u64> = w.iter()
                        .filter_map(|(k,(e,_))| if *e < now { Some(*k) } else { None })
                        .take(want)
                        .collect();
                    for k in keys { w.remove(&k); freed += 1; if freed >= want { break; } }
                    if freed < want {
                        // simple bounded eviction of remaining
                        for k in w.keys().cloned().take(want - freed).collect::<Vec<_>>() {
                            if w.remove(&k).is_some() { freed += 1; if freed >= want { break; } }
                        }
                    }
                }
            }
        }
    }
}

// ========================= PATCH 53B: dalek_chunk_ctl module =========================
pub mod dalek_chunk_ctl {
    use once_cell::sync::Lazy;
    use core::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    static CUR: Lazy<AtomicUsize> = Lazy::new(|| {
        let base = std::env::var("QAB_DALEK_CHUNK")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2048);
        AtomicUsize::new(base.clamp(64, 8192))
    });

    #[inline]
    fn cmin() -> usize {
        std::env::var("QAB_DALEK_CHUNK_MIN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(512)
            .clamp(64, 8192)
    }

    #[inline]
    fn cmax() -> usize {
        std::env::var("QAB_DALEK_CHUNK_MAX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8192)
            .clamp(256, 32768)
    }

    #[inline]
    pub fn cur_chunk() -> usize {
        CUR.load(Relaxed)
    }

    // If we had to bisect → shrink a bit; if clean pass → grow a bit
    #[inline]
    pub fn report(bisected: bool) {
        let cur = CUR.load(Relaxed);
        let next = if bisected {
            core::cmp::max(cmin(), (cur * 7) / 8) // −12.5%
        } else {
            core::cmp::min(cmax(), (cur * 9) / 8) // +12.5%
        };
        CUR.store(next, Relaxed);
    }
}

#[cfg(all(target_os="linux", target_arch="x86_64"))]
mod cycles_cal {
    use once_cell::sync::Lazy;
    use core::sync::atomic::{AtomicU64, Ordering::Relaxed};

    pub(super) static MULT_Q32_NS: Lazy<AtomicU64> = Lazy::new(|| {
        AtomicU64::new(init_mult_q32_ns())
    });
    static LAST_T_NS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
    static LAST_CYC:  Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

    #[inline]
    unsafe fn read_ns_raw() -> u64 {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
        (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64)
    }

    fn init_mult_q32_ns() -> u64 {
        unsafe {
            let t0 = read_ns_raw();
            let c0 = super::now_cycles();
            let target = t0 + 2_000_000; // 2ms
            let mut t1 = t0;
            while t1 < target { core::hint::spin_loop(); t1 = read_ns_raw(); }
            let c1 = super::now_cycles();
            let dns = t1.saturating_sub(t0).max(1);
            let dcy = c1.saturating_sub(c0).max(1);
            let mult = ((u128::from(dns) << 32) / u128::from(dcy)).clamp(1, u128::from(u64::MAX)) as u64;
            LAST_T_NS.store(t1, Relaxed);
            LAST_CYC.store(c1, Relaxed);
            mult
        }
    }

    // Called occasionally from worker loop to keep drift <~50ppm; EWMA to avoid steps
    #[inline]
    pub fn resync_tick() {
        // Every ~100ms wall time at most; cheap read-only when not due
        let now_c = super::now_cycles();
        let last_c = LAST_CYC.load(Relaxed);
        if now_c.wrapping_sub(last_c) < 100_000_000 { return; } // ~100ms at 1 GHz; safe heuristic

        unsafe {
            let t0 = LAST_T_NS.load(Relaxed);
            let c0 = LAST_CYC.load(Relaxed);
            let t1 = read_ns_raw();
            let c1 = super::now_cycles();
            let dns = t1.saturating_sub(t0).max(1);
            let dcy = c1.saturating_sub(c0).max(1);
            let meas = ((u128::from(dns) << 32) / u128::from(dcy)).clamp(1, u128::from(u64::MAX)) as u64;

            // EWMA update: 1/16 step (smooth)
            let prev = MULT_Q32_NS.load(Relaxed);
            let next = ((prev as u128 * 15) + meas as u128 + 15) >> 4;
            MULT_Q32_NS.store(next as u64, Relaxed);

            LAST_T_NS.store(t1, Relaxed);
            LAST_CYC.store(c1, Relaxed);
        }
    }
}

#[cfg(all(target_os="linux", target_arch="x86_64"))]
#[inline]
pub fn cycles_to_ns(dcycles: u64) -> u64 {
    let m = cycles_cal::MULT_Q32_NS.load(core::sync::atomic::Ordering::Relaxed);
    ((u128::from(dcycles) * u128::from(m)) >> 32) as u64
}

#[cfg(all(target_os="linux", target_arch="x86_64"))]
#[inline]
pub fn cycles_resync_tick() { cycles_cal::resync_tick() }

#[cfg(not(all(target_os="linux", target_arch="x86_64")))]
#[inline] 
pub fn now_cycles() -> u64 { now_us() * 1_000 }

#[cfg(not(all(target_os="linux", target_arch="x86_64")))]
#[inline] 
pub fn cycles_to_ns(dcycles: u64) -> u64 { dcycles }

#[cfg(not(all(target_os="linux", target_arch="x86_64")))]
#[inline] 
pub fn cycles_resync_tick() {}

#[cfg(not(all(target_os="linux", target_arch="x86_64")))]
#[inline]
pub fn now_cycles() -> u64 {
    // portable fallback: approximate in ns using clock; good enough off x86_64/linux
    crate::clock::now_us() * 1_000
}

#[cfg(not(all(target_os="linux", target_arch="x86_64")))]
#[inline]
pub fn cycles_to_ns(dcycles: u64) -> u64 { dcycles }

// ========================= PATCH 56: pin_to_core_and_node with RT scheduling =========================
// cpuset-aware mapping with optional NUMA mem-bind and RT scheduling
#[cfg(all(feature = "core_affinity", target_os = "linux"))]
fn pin_to_core_and_node(core_id: usize) {
    use core_affinity::CoreId;
    use std::str::FromStr;
    use std::fs;
    use std::env;
    use libc::{sched_param, sched_setscheduler, SCHED_RR, SCHED_FIFO, SCHED_OTHER, c_int};
    
    // Helper to set real-time priority for the current thread
    fn set_realtime_priority(rt_prio: i32) -> Result<(), String> {
        if rt_prio <= 0 {
            return Ok(());  // No RT priority requested
        }
        
        let max_prio = unsafe { libc::sched_get_priority_max(SCHED_RR) };
        let min_prio = unsafe { libc::sched_get_priority_min(SCHED_RR) };
        
        // Clamp priority to valid range
        let prio = rt_prio.clamp(min_prio, max_prio);
        
        let param = sched_param { sched_priority: prio };
        
        // Try SCHED_RR first, fall back to SCHED_FIFO if not available
        let policies = [SCHED_RR, SCHED_FIFO, SCHED_OTHER];
        
        for policy in policies.iter() {
            let result = unsafe {
                sched_setscheduler(0, *policy, &param)
            };
            
            if result == 0 {
                if *policy == SCHED_RR || *policy == SCHED_FIFO {
                    return Ok(());
                }
                return Err("Only non-RT scheduling available".to_string());
            }
        }
        
        let err = std::io::Error::last_os_error();
        Err(format!("Failed to set RT priority: {}", err))
    }
    use std::os::unix::thread::JoinHandleExt;
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    use libc::{sched_param, sched_setscheduler, SCHED_RR, sched_setaffinity, cpu_set_t, CPU_SET};
    use std::mem::{size_of, zeroed};
    use std::io::Error as IoError;
    use std::ptr;

    // Environment config with safe defaults
    static CPUSET: once_cell::sync::Lazy<Option<Vec<usize>>> = once_cell::sync::Lazy::new(|| {
        std::env::var("QAB_CPUSET").ok().and_then(|s| {
            let m: Vec<_> = s.split(',').filter_map(|t| t.trim().parse().ok()).collect();
            if m.is_empty() { None } else { Some(m) }
        })
    });
    
    static AVOID_SMT: bool = std::env::var("QAB_AVOID_SMT")
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true")).unwrap_or(true);
    
    static RT_PRIO: i32 = std::env::var("QAB_RT_PRIO")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(70);
    
    static MLOCKALL: bool = std::env::var("QAB_MLOCKALL")
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true")).unwrap_or(false);
    
    static THP_HINT: bool = std::env::var("QAB_THP_HINT")
        .map(|s| s != "0" && !s.eq_ignore_ascii_case("false")).unwrap_or(true);
    
    // One-time init for NUMA and CPU topology
    static NODE_CPUS: once_cell::sync::Lazy<Vec<Vec<usize>>> = once_cell::sync::Lazy::new(|| {
        let mut nodes = Vec::new();
        if let Ok(entries) = fs::read_dir("/sys/devices/system/node") {
            for entry in entries.filter_map(Result::ok) {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("node") {
                        if let Ok(cpus) = fs::read_to_string(entry.path().join("cpulist")) {
                            let cpus: Vec<_> = cpus.trim().split(',')
                                .flat_map(|r| {
                                    let mut parts = r.splitn(2, '-');
                                    let start = parts.next()?.parse::<usize>().ok()?;
                                    let end = parts.next().and_then(|e| e.parse::<usize>().ok()).unwrap_or(start);
                                    Some(start..=end)
                                })
                                .flatten()
                                .collect();
                            if !cpus.is_empty() { nodes.push(cpus); }
                        }
                    }
                }
            }
        }
        if nodes.is_empty() {
            // Fallback: assume all cores on node 0
            if let Some(cores) = core_affinity::get_core_ids() {
                nodes.push(cores.into_iter().map(|c| c.id).collect());
            }
        }
        nodes
    });

    // Get CPU siblings from sysfs
    fn get_siblings(cpu: usize) -> Vec<usize> {
        let path = format!("/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list", cpu);
        fs::read_to_string(path).ok()
            .and_then(|s| {
                let cpus: Vec<_> = s.trim().split(',')
                    .flat_map(|r| {
                        let mut parts = r.splitn(2, '-');
                        let start = parts.next()?.parse::<usize>().ok()?;
                        let end = parts.next().and_then(|e| e.parse::<usize>().ok()).unwrap_or(start);
                        Some(start..=end)
                    })
                    .flatten()
                    .collect();
                if cpus.len() > 1 { Some(cpus) } else { None }
            })
            .unwrap_or_else(Vec::new)
    }

    // Bind memory to NUMA node
    #[cfg(feature = "jemallocator")]
    fn bind_memory(node: usize) -> Result<(), String> {
        use jemalloc_ctl::{arena, Error};
        let mib = arena::mib(arena::MibOptions::new(node as u32))?;
        mib.set(Some(node as u32)).map_err(|e| e.to_string())
    }

    // Set RT priority
    fn set_rt_priority(prio: i32) -> Result<(), String> {
        if prio <= 0 { return Ok(()); }
        let param = sched_param { sched_priority: prio };
        let res = unsafe { sched_setscheduler(0, SCHED_RR, &param) };
        if res == 0 { Ok(()) } else { Err(IoError::last_os_error().to_string()) }
    }

    // Lock all current and future memory
    fn lock_memory() -> Result<(), String> {
        if !MLOCKALL { return Ok(()); }
        #[cfg(target_os = "linux")] {
            use libc::{mlockall, MCL_CURRENT, MCL_FUTURE};
            let res = unsafe { mlockall(MCL_CURRENT | MCL_FUTURE) };
            if res == 0 { Ok(()) } else { Err(IoError::last_os_error().to_string()) }
        }
        #[cfg(not(target_os = "linux"))] {
            Ok(())
        }
    }

    // Set transparent hugepage hint
    fn set_thp_hint() {
        if !THP_HINT { return; }
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .write(true).open("/sys/kernel/mm/transparent_hugepage/defrag") {
            let _ = std::io::Write::write_all(&mut f, b"madvise\n");
        }
    }

    // Main pinning logic
    let cores: Vec<usize> = CPUSET.clone().unwrap_or_else(|| {
        core_affinity::get_core_ids()
            .map(|ids| ids.into_iter().map(|c| c.id).collect())
            .unwrap_or_else(|| (0..num_cpus::get()).collect())
    });

    if cores.is_empty() { return; }
    
    let core_id = core_id % cores.len();
    let target_core = cores[core_id];
    
    // Find NUMA node for this core
    let node = NODE_CPUS.iter().position(|node_cpus| 
        node_cpus.contains(&target_core)
    ).unwrap_or(0);
    
    // Avoid SMT siblings if requested
    let mut cpus = vec![target_core];
    if AVOID_SMT {
        let siblings: Vec<_> = get_siblings(target_core).into_iter()
            .filter(|&c| c != target_core)
            .collect();
        if !siblings.is_empty() {
            if let Some(coreset) = CPUSET.as_ref() {
                // Only exclude siblings that are in our cpuset
                for sib in siblings {
                    if coreset.contains(&sib) {
                        cpus.push(sib);
                    }
                }
            } else {
                cpus.extend(siblings);
            }
        }
    }
    
    // Set CPU affinity
    unsafe {
        let mut set: cpu_set_t = zeroed();
        for &cpu in &cpus {
            CPU_SET(cpu, &mut set);
        }
        let _ = sched_setaffinity(
            0, // current thread
            size_of::<cpu_set_t>(),
            &set as *const _
        );
    }
    
    // Set RT priority
    if let Err(e) = set_rt_priority(RT_PRIO) {
        eprintln!("Failed to set RT priority: {}", e);
    }
    
    // Bind memory to NUMA node
    #[cfg(feature = "jemallocator")]
    if let Err(e) = bind_memory(node) {
        eprintln!("Failed to bind memory to NUMA node: {}", e);
    }
    
    // Lock memory
    if let Err(e) = lock_memory() {
        eprintln!("Failed to lock memory: {}", e);
    }
    
    // Set THP hint
    set_thp_hint();
    
    // Set thread name for debugging
    let _ = std::thread::current().name().map(|name| {
        std::thread::Builder::new()
            .name(format!("{}_t{}", name, core_id))
            .spawn(|| {})
            .ok()
            .map(|h| h.join().ok());
    });
    
    // Set core affinity as fallback
    if let Some(cores) = core_affinity::get_core_ids() {
        if let Some(&core) = cores.get(core_id % cores.len()) {
            core_affinity::set_for_current(core);
        }
    }

// ========================= PATCH: l3_cwnd AIMD+EWMA (REPLACE struct/new/on_occupancy) =========================
#[cfg(all(feature="l3_cwnd", feature="numa_hotcold"))]
mod l3_cwnd {
    use once_cell::sync::Lazy;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
    
    static CWND_MAX:    Lazy<u32> = Lazy::new(|| std::env::var("QAB_L3_CWND_MAX").ok().and_then(|s| s.parse().ok()).unwrap_or(4096));
    static CWND_MIN:    Lazy<u32> = Lazy::new(|| std::env::var("QAB_L3_CWND_MIN").ok().and_then(|s| s.parse().ok()).unwrap_or(64));
    static ECN_HIGH_BP: Lazy<u32> = Lazy::new(|| std::env::var("QAB_L3_ECN_HIGH_BP").ok().and_then(|s| s.parse().ok()).unwrap_or(8500));
    static ECN_LOW_BP:  Lazy<u32> = Lazy::new(|| std::env::var("QAB_L3_ECN_LOW_BP").ok().and_then(|s| s.parse().ok()).unwrap_or(5000));
    static BETA_NUM:    Lazy<u32> = Lazy::new(|| std::env::var("QAB_L3_BETA_NUM").ok().and_then(|s| s.parse().ok()).unwrap_or(7));
    static BETA_DEN:    Lazy<u32> = Lazy::new(|| std::env::var("QAB_L3_BETA_DEN").ok().and_then(|s| s.parse().ok()).unwrap_or(10));
    
    pub struct Cwnd {
        cwnd: AtomicU32,
        ssthresh: AtomicU32,
        hi_cnt: AtomicU32,
        lo_cnt: AtomicU32,
        pub ce_marks: AtomicU64,
        occ_ewma_bp: AtomicU32, // 0..10000 basis points
    }
    
    impl Cwnd {
        pub fn new(init: u32) -> Self {
            let init = init.clamp(*CWND_MIN, *CWND_MAX);
            Self {
                cwnd: AtomicU32::new(init),
                ssthresh: AtomicU32::new(*CWND_MAX),
                hi_cnt: AtomicU32::new(0),
                lo_cnt: AtomicU32::new(0),
                ce_marks: AtomicU64::new(0),
                occ_ewma_bp: AtomicU32::new(*ECN_LOW_BP),
            }
        }
        
        #[inline] pub fn window(&self) -> u32 { self.cwnd.load(Relaxed) }
        #[inline] pub fn headroom(&self, inflight: u32) -> i32 { self.window() as i32 - inflight as i32 }

        // ========================= PATCH 27: l3_cwnd::on_occupancy (REPLACE) =========================
        // 
        // Implements a guarded slow-start burst mode with the following behavior:
        // - In slow-start (cwnd < ssthresh): aggressive multiplicative increase
        // - In congestion avoidance: standard additive increase
        // - On congestion: multiplicative decrease with beta factor
        // - Maintains EWMA of occupancy for stability
        // - Uses environment variables for tuning parameters
        pub fn on_occupancy(&self, occ_bp: u32) {
            // Load current state with appropriate ordering
            let cwnd = self.cwnd.load(Relaxed);
            let ssthresh = self.ssthresh.load(Relaxed);
            
            // Update EWMA of occupancy (0..10000 basis points, 10_000 = 100%)
            let prev = self.occ_ewma_bp.load(Relaxed) as u64;
            let gamma = 128u64; // 0.128 fixed-point smoothing (out of 1000)
            let next = ((prev * (1000 - gamma)) + (occ_bp.min(10_000) as u64 * gamma)) / 1000;
            self.occ_ewma_bp.store(next as u32, Relaxed);
            
            // Load and update state counters with relaxed ordering (single-writer)
            let mut hi = self.hi_cnt.load(Relaxed);
            let mut lo = self.lo_cnt.load(Relaxed);
            
            // Check for high occupancy (congestion)
            if next as u32 >= *ECN_HIGH_BP {
                hi = hi.saturating_add(1);
                self.hi_cnt.store(hi, Relaxed);
                self.lo_cnt.store(0, Relaxed);
                
                // Only react to sustained high occupancy (2+ consecutive samples)
                if hi >= 2 {
                    // Multiplicative decrease (AIMD)
                    let dec = (cwnd as u64 * (*BETA_NUM as u64)) / (*BETA_DEN as u64);
                    let new_cwnd = dec.max(*CWND_MIN).min(*CWND_MAX);
                    
                    // Update state
                    self.cwnd.store(new_cwnd, Relaxed);
                    self.ssthresh.store(new_cwnd.max(2), Relaxed); // Ensure ssthresh >= 2
                    self.hi_cnt.store(0, Relaxed);
                    self.ce_marks.fetch_add(1, Relaxed);
                }
            } 
            // Check for low occupancy (underutilization)
            else if next as u32 <= *ECN_LOW_BP {
                lo = lo.saturating_add(1);
                self.lo_cnt.store(lo, Relaxed);
                self.hi_cnt.store(0, Relaxed);
                
                // Only react to sustained low occupancy (2+ consecutive samples)
                if lo >= 2 {
                    // Slow start (exponential growth) or congestion avoidance (linear growth)
                    let new_cwnd = if cwnd < ssthresh {
                        // Slow start: double the window (capped at CWND_MAX)
                        cwnd.saturating_mul(2).min(*CWND_MAX)
                    } else {
                        // Congestion avoidance: additive increase (capped at CWND_MAX)
                        cwnd.saturating_add(1).min(*CWND_MAX)
                    };
                    
                    // Update state
                    self.cwnd.store(new_cwnd, Relaxed);
                    self.lo_cnt.store(0, Relaxed);
                }
            } 
            // Moderate occupancy: reset counters but don't change window
            else {
                self.hi_cnt.store(0, Relaxed);
                self.lo_cnt.store(0, Relaxed);
}

// ========================= PATCH 50: route_cost::price (REPLACE) =========================
#[cfg(all(feature="route_cost", feature="numa_hotcold"))]
mod route_cost {
    use once_cell::sync::Lazy;
    static W_WAIT:   Lazy<f64> = Lazy::new(|| std::env::var("QAB_ROUTE_W_WAIT").ok().and_then(|s| s.parse().ok()).unwrap_or(1.0));
    static W_CWND:   Lazy<f64> = Lazy::new(|| std::env::var("QAB_ROUTE_W_CWND").ok().and_then(|s| s.parse().ok()).unwrap_or(0.4));
    static W_PROFIT: Lazy<f64> = Lazy::new(|| std::env::var("QAB_ROUTE_W_PROFIT").ok().and_then(|s| s.parse().ok()).unwrap_or(0.8));
    
    #[inline]
    pub fn price(wait_ns: u64, cwnd_head: i32, profit_ns_credit: u64) -> i128 {
        use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
        use once_cell::sync::Lazy;

        #[inline] 
        fn iabs128(x: i128) -> u128 { 
            if x >= 0 { x as u128 } else { (-x) as u128 } 
        }

        static AUTOSCALE: Lazy<bool> = Lazy::new(|| matches!(std::env::var("QAB_ROUTE_AUTOSCALE").as_deref(), Ok("1")));
        static EWMA_W: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1));
        static EWMA_C: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1));
        static EWMA_P: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1));

        // Base components
        let mut w = (*W_WAIT * wait_ns as f64) as i128;
        let mut c = 0i128;
        if cwnd_head < 0 {
            c = (*W_CWND * (-(cwnd_head as f64))) as i128; // penalty when overfull
        }
        let mut p = (*W_PROFIT * profit_ns_credit as f64) as i128;

        // cwnd positive headroom → small profit bonus (bps per unit headroom, capped)
        if cwnd_head > 0 {
            let gain_bp: i128 = std::env::var("QAB_CWND_HEAD_BONUS_BP")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(40) as i128; // 0.40% per unit headroom
            let cap_units: i128 = std::env::var("QAB_CWND_HEAD_BONUS_CAP")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(8) as i128;
            let units = core::cmp::min(cwnd_head as i128, cap_units).max(0);
            let bump = (p.saturating_mul(units * gain_bp)) / 10_000;
            p = p.saturating_add(bump);
        }

        // Finish-phase skew (profit) + tail wait upweight (kept)
        #[cfg(all(feature="route_cost_scurve"))]
        {
            let svc_est_ns = crate::svc_obs::est_ns();
            let finish_us  = crate::clock::now_us().saturating_add((wait_ns.saturating_add(svc_est_ns)) / 1_000);
            let phase_bp   = crate::route_cost_scurve::phase_bp_finish_us(finish_us) as i128;
            
            p = p + (p.saturating_mul(phase_bp)) / 10_000;

            let gain_bp: i128 = std::env::var("QAB_TAIL_WAIT_BOOST_BP")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(100) as i128;
            if phase_bp < 0 && gain_bp > 0 {
                let bump = (w.saturating_mul((-phase_bp) * gain_bp)) / 100_000_000;
                w = w.saturating_add(bump);
            }
        }

        // Autoscale (optional)
        let upd = |a: &AtomicU64, x: u64| {
            let prev = a.load(Relaxed).max(1);
            let next = (((prev as u128) * 15) + (x as u128) + 15) >> 4;
            a.store(next as u64, Relaxed);
            next as u64
        };
        
        let ew_w = upd(&EWMA_W, (iabs128(w).min(u128::from(u64::MAX))) as u64).max(1);
        let ew_c = upd(&EWMA_C, (iabs128(c).min(u128::from(u64::MAX))) as u64).max(1);
        let ew_p = upd(&EWMA_P, (iabs128(p).min(u128::from(u64::MAX))) as u64).max(1);

        if *AUTOSCALE {
            let wn = ((iabs128(w) << 10) / ew_w as u128) as i128 * if w >= 0 { 1 } else { -1 };
            let cn = ((iabs128(c) << 10) / ew_c as u128) as i128 * if c >= 0 { 1 } else { -1 };
            let pn = ((iabs128(p) << 10) / ew_p as u128) as i128 * if p >= 0 { 1 } else { -1 };
            wn + cn - pn
        } else {
            w + c - p
        }
    }
}

// (helpers impl placed later in file)

// ========================= PATCH: l3_hotcold_promotion::promote_bounded (REPLACE) =========================
#[cfg(all(target_os="linux", feature="numa_hotcold"))]
mod l3_hotcold_promotion {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    pub static PROMOTE_CREDIT: once_cell::sync::Lazy<usize> =
        once_cell::sync::Lazy::new(|| std::env::var("QAB_PROMOTE_CREDIT").ok().and_then(|s| s.parse().ok()).unwrap_or(64));

    #[inline]
    fn promo_score(it: &ShardItem) -> u128 {
        #[cfg(feature="advanced_sched")]
        {
            let p = it.profit.min((1u64<<28)-1) as u128;
            let p2 = p*p;
            let age = (clock::now_us().saturating_sub(it.enq_us)).min((1u64<<56)-1) as u128;
            (p2 << 64) | (age << 8)
        }
        #[cfg(not(feature="advanced_sched"))]
        { 0 }
    }

    pub fn promote_bounded(g: &super::l3_hotcold::L3GroupHC) -> usize {
        use std::sync::atomic::Ordering::Relaxed;
        let budget = (*super::l3_hotcold::PROMOTE_BUDGET).min(*PROMOTE_CREDIT);
        let window = (budget * 2).max(8);
        let mut moved = 0usize;

        // Optional cwnd guard
        #[cfg(feature="l3_cwnd")]
        {
            let inflight = g.load.load(Relaxed) as u32;
            if g.cwnd.headroom(inflight) <= 0 { return 0; }
        }

        let mut buf: smallvec::SmallVec<[super::ShardItem; 128]> = smallvec::SmallVec::with_capacity(window);
        for _ in 0..window {
            if let Some(it) = g.cold.pop() { buf.push(it); } else { break; }
        }
        if buf.is_empty() { return 0; }

        buf.sort_unstable_by(|a,b| promo_score(b).cmp(&promo_score(a)));
        for it in buf.into_iter() {
            if moved >= budget {
                let _ = g.cold.push(it);
                continue;
            }
            if g.hot.push(Arc::clone(&it)).is_ok() {
                g.hot_load.fetch_add(1, Relaxed);
                moved += 1;
            } else {
                let _ = g.cold.push(it);
            }
        }
        moved
    }
}

// ========================= PATCH 13: FastMpmc ring (feature fast_ring) =========================
#[cfg(feature = "fast_ring")]
mod fast_ring {
    use core::cell::UnsafeCell;
    use core::mem::MaybeUninit;
    use core::sync::atomic::{AtomicUsize, AtomicU64, Ordering::*};
    use std::alloc::{alloc_zeroed, dealloc, Layout};

    #[repr(C, align(128))]
    struct Slot<T> { seq: AtomicU64, val: UnsafeCell<MaybeUninit<T>> }
    unsafe impl<T: Send> Send for Slot<T> {}
    unsafe impl<T: Send> Sync for Slot<T> {}

    #[repr(C, align(128))]
    pub struct FastMpmc<T> {
        mask: usize,
        head: AtomicUsize,
        _pad0: [u8; 64],
        tail: AtomicUsize,
        _pad1: [u8; 64],
        buf: *mut Slot<T>,
        cap: usize,
    }
    unsafe impl<T: Send> Send for FastMpmc<T> {}
    unsafe impl<T: Send> Sync for FastMpmc<T> {}

    #[inline] fn ceil_pow2(mut x: usize) -> usize { if x.is_power_of_two() { x } else { x.next_power_of_two() } }

    impl<T> FastMpmc<T> {
        pub fn with_capacity(mut cap: usize) -> Self {
            cap = ceil_pow2(cap.max(8));
            let size = cap * core::mem::size_of::<Slot<T>>();
            let layout = Layout::from_size_align(size, 2 * 1024 * 1024).unwrap();
            let mut ptr = unsafe { alloc_zeroed(layout) } as *mut Slot<T>;
            if ptr.is_null() {
                let l2 = Layout::array::<Slot<T>>(cap).unwrap();
                ptr = unsafe { alloc_zeroed(l2) } as *mut Slot<T>;
            } else {
                #[cfg(target_os="linux")]
                unsafe { libc::madvise(ptr as *mut _, size as libc::size_t, libc::MADV_HUGEPAGE); }
            }
            if std::env::var("QAB_RING_PRETOUCH").ok().as_deref() == Some("1") {
                unsafe {
                    let base = ptr as *mut u8; let page = 4096usize; let mut off = 0usize;
                    while off < size { core::ptr::write_volatile(base.add(off), 0u8); off += page; }
                }
            }
            if std::env::var("QAB_RING_MLOCK").ok().as_deref() == Some("1") {
                #[cfg(target_os="linux")]
                unsafe { libc::mlock(ptr as *const _, size as libc::size_t); }
            }
            for i in 0..cap { unsafe { (*ptr.add(i)).seq.store(i as u64, Relaxed); } }
            Self { mask: cap - 1, head: AtomicUsize::new(0), _pad0: [0;64], tail: AtomicUsize::new(0), _pad1: [0;64], buf: ptr, cap }
        }
        #[inline] pub fn push(&self, v: T) -> Result<(), T> {
            let mut pos = self.head.load(Relaxed);
            loop {
                let slot = unsafe { &*self.buf.add(pos & self.mask) };
                let seq = slot.seq.load(Acquire);
                let dif = (seq as isize) - (pos as isize);
                if dif == 0 {
                    if self.head.compare_exchange_weak(pos, pos + 1, Acquire, Relaxed).is_ok() {
                        unsafe { (*slot.val.get()).write(MaybeUninit::new(v)); }
                        slot.seq.store((pos + 1) as u64, Release); return Ok(());
                    }
                } else if dif < 0 { return Err(v); } else { pos = self.head.load(Relaxed); }
            }
        }
        #[inline] pub fn pop(&self) -> Option<T> {
            let mut pos = self.tail.load(Relaxed);
            loop {
                let slot = unsafe { &*self.buf.add(pos & self.mask) };
                let seq = slot.seq.load(Acquire);
                let dif = (seq as isize) - ((pos + 1) as isize);
                if dif == 0 {
                    if self.tail.compare_exchange_weak(pos, pos + 1, Acquire, Relaxed).is_ok() {
                        let v = unsafe { (*slot.val.get()).assume_init_read() };
                        slot.seq.store((pos + self.mask + 1) as u64, Release);
                        return Some(v);
                    }
                } else if dif < 0 { return None; } else { pos = self.tail.load(Relaxed); }
            }
        }
        #[inline] pub fn is_empty(&self) -> bool { self.len() == 0 }
        #[inline] pub fn len(&self) -> usize { let h = self.head.load(Acquire); let t = self.tail.load(Acquire); h.saturating_sub(t) }
    }
    impl<T> Drop for FastMpmc<T> { fn drop(&mut self) { while let Some(_v) = self.pop() {} let size = self.cap * core::mem::size_of::<Slot<T>>(); unsafe { let layout = std::alloc::Layout::from_size_align_unchecked(size, 2*1024*1024); dealloc(self.buf as *mut u8, layout); } } }
}

// Patch 29: enable metadata-augmented ring when ring_meta is on
#[cfg(all(feature="fast_ring", feature="ring_meta"))]
type InboxQ<T> = fast_ring_meta::FastMpmc<T>;
#[cfg(all(feature="fast_ring", not(feature="ring_meta")))]
type InboxQ<T> = fast_ring::FastMpmc<T>;
#[cfg(not(feature="fast_ring"))]
type InboxQ<T> = ArrayQueue<T>;

// ========================= PATCH 29: FastMpmc ring metadata =========================
// Features: fast_ring, ring_meta
#[cfg(all(feature="fast_ring", feature="ring_meta"))]
mod fast_ring_meta {
    use core::cell::UnsafeCell;
    use core::mem::MaybeUninit;
    use core::sync::atomic::{AtomicUsize, AtomicU64, AtomicU32, Ordering::*};
    use std::alloc::{alloc_zeroed, dealloc, Layout};

    pub const META_F_HOT0: u32 = 1<<0;
    pub const META_F_HOT1: u32 = 1<<1;
    pub const META_F_DEADLINE: u32 = 1<<2;

    #[repr(C, align(128))]
    struct Slot<T> { seq: AtomicU64, val: UnsafeCell<MaybeUninit<T>> }
    unsafe impl<T: Send> Send for Slot<T> {}
    unsafe impl<T: Send> Sync for Slot<T> {}

    #[repr(C, align(128))]
    pub struct FastMpmc<T> {
        mask: usize,
        head: AtomicUsize,
        _pad0: [u8; 64],
        tail: AtomicUsize,
        _pad1: [u8; 64],
        buf: *mut Slot<T>,
        cap: usize,
        meta_deadline: *mut AtomicU64,
        meta_flags: *mut AtomicU32,
    }
    unsafe impl<T: Send> Send for FastMpmc<T> {}
    unsafe impl<T: Send> Sync for FastMpmc<T> {}

    #[inline] fn ceil_pow2(mut x: usize) -> usize { if x.is_power_of_two(){x}else{x.next_power_of_two()} }

    impl<T> FastMpmc<T> {
        pub fn with_capacity(mut cap: usize) -> Self {
            use libc::{madvise, mlock, MADV_HUGEPAGE};
            use core::sync::atomic::Ordering::Relaxed;
            
            cap = ceil_pow2(cap.max(8));
            let size = cap * core::mem::size_of::<Slot<T>>();
            let layout = Layout::from_size_align(size, 2 * 1024 * 1024).unwrap();
            let mut ptr = unsafe { alloc_zeroed(layout) } as *mut Slot<T>;
            if ptr.is_null() {
                let l2 = Layout::array::<Slot<T>>(cap).unwrap();
                ptr = unsafe { alloc_zeroed(l2) } as *mut Slot<T>;
            } else {
                #[cfg(target_os="linux")]
                unsafe { madvise(ptr as *mut _, size as libc::size_t, MADV_HUGEPAGE); }
            }
            for i in 0..cap { unsafe { (*ptr.add(i)).seq.store(i as u64, Relaxed); } }

            // meta arrays
            let meta_deadline = unsafe { alloc_zeroed(Layout::array::<AtomicU64>(cap).unwrap()) } as *mut AtomicU64;
            let meta_flags    = unsafe { alloc_zeroed(Layout::array::<AtomicU32>(cap).unwrap()) } as *mut AtomicU32;

            #[cfg(target_os="linux")]
            unsafe {
                let md_sz = (cap * core::mem::size_of::<AtomicU64>()) as libc::size_t;
                let mf_sz = (cap * core::mem::size_of::<AtomicU32>()) as libc::size_t;
                let _ = madvise(meta_deadline as *mut _, md_sz, MADV_HUGEPAGE);
                let _ = madvise(meta_flags as *mut _,    mf_sz, MADV_HUGEPAGE);

                if std::env::var("QAB_RING_MLOCK").ok().as_deref() == Some("1") {
                    let _ = mlock(ptr as *const _, size as libc::size_t);
                    let _ = mlock(meta_deadline as *const _, md_sz);
                    let _ = mlock(meta_flags as *const _,    mf_sz);
                }

                // NEW: optional NUMA bind (preferred node)
                if let Ok(s) = std::env::var("QAB_RING_NUMA_NODE") {
                    if let Ok(node) = s.parse::<i32>() {
                        if node >= 0 {
                            #[inline]
                            unsafe fn mbind_prefer(addr: *mut u8, len: usize, node: i32) {
                                // nodemask as u64; MPOL_PREFERRED
                                let mask: u64 = 1u64 << (node as u64);
                                let _ = libc::syscall(
                                    libc::SYS_mbind,
                                    addr as *mut libc::c_void,
                                    len as libc::size_t,
                                    libc::MPOL_PREFERRED,
                                    &mask as *const u64,
                                    8 * core::mem::size_of::<u64>(),
                                    0u64
                                );
                            }
                            mbind_prefer(ptr as *mut u8, size, node);
                            mbind_prefer(meta_deadline as *mut u8, md_sz as usize, node);
                            mbind_prefer(meta_flags as *mut u8,    mf_sz as usize, node);
                        }
                    }
                }
            }

            if std::env::var("QAB_RING_PRETOUCH").ok().as_deref() == Some("1") {
                unsafe {
                    let page = 4096usize;
                    let mut off = 0usize;
                    while off < size { core::ptr::write_volatile((ptr as *mut u8).add(off), 0u8); off += page; }
                    let mut o2 = 0usize;
                    while o2 < cap * core::mem::size_of::<AtomicU64>() { core::ptr::write_volatile((meta_deadline as *mut u8).add(o2), 0u8); o2 += page; }
                    let mut o3 = 0usize;
                    while o3 < cap * core::mem::size_of::<AtomicU32>() { core::ptr::write_volatile((meta_flags as *mut u8).add(o3), 0u8); o3 += page; }
                }
            }

            Self {
                mask: cap-1,
                head: AtomicUsize::new(0), _pad0: [0;64],
                tail: AtomicUsize::new(0), _pad1: [0;64],
                buf: ptr, cap,
                meta_deadline, meta_flags
            }            core::ptr::write_volatile((meta_flags as *mut u8).add(o3), 0u8); 
                        o3 += page; 
                    }
                }
            }
            
{{ ... }}
            Self { 
                mask: cap-1, 
                head: AtomicUsize::new(0), 
                _pad0: [0;64], 
                tail: AtomicUsize::new(0), 
                _pad1: [0;64], 
                buf: ptr, 
                cap, 
                meta_deadline, 
                meta_flags 
            }
        }
        #[inline] pub fn capacity(&self) -> usize { self.cap }
        #[inline]
        pub fn push_with_meta(&self, v: T, flags: u32, deadline_us: u64) -> Result<(), T> {
            let mut pos = self.head.load(Relaxed);
            loop {
                let slot = unsafe { &*self.buf.add(pos & self.mask) };
                let seq = slot.seq.load(Acquire);
                let dif = (seq as isize) - (pos as isize);
                if dif == 0 {
                    if self.head.compare_exchange_weak(pos, pos + 1, Acquire, Relaxed).is_ok() {
                        unsafe { (*slot.val.get()).write(MaybeUninit::new(v)); }
                        unsafe {
                            (*self.meta_flags.add(pos & self.mask)).store(flags, Relaxed);
                            (*self.meta_deadline.add(pos & self.mask)).store(deadline_us, Relaxed);
                        }
                        slot.seq.store((pos + 1) as u64, Release);
                        return Ok(());
                    }
                } else if dif < 0 { return Err(v); } else { pos = self.head.load(Relaxed); }
            }
        }
        #[inline]
        pub fn pop_with_meta(&self) -> Option<(T, u32, u64)> {
            let mut pos = self.tail.load(Relaxed);
            loop {
                let slot = unsafe { &*self.buf.add(pos & self.mask) };
                let seq = slot.seq.load(Acquire);
                let dif = (seq as isize) - ((pos + 1) as isize);
                if dif == 0 {
                    if self.tail.compare_exchange_weak(pos, pos + 1, Acquire, Relaxed).is_ok() {
                        let v = unsafe { (*slot.val.get()).assume_init_read() };
                        let f = unsafe { (*self.meta_flags.add(pos & self.mask)).load(Relaxed) };
                        let d = unsafe { (*self.meta_deadline.add(pos & self.mask)).load(Relaxed) };
                        slot.seq.store((pos + self.mask + 1) as u64, Release);
                        return Some((v, f, d));
                    }
                } else if dif < 0 { return None; } else { pos = self.tail.load(Relaxed); }
            }
        }
        // Back-compat shims
        #[inline] pub fn push(&self, v: T) -> Result<(), T> { self.push_with_meta(v, 0, 0) }
        #[inline] pub fn pop(&self) -> Option<T> { self.pop_with_meta().map(|(v,_,_)| v) }
        #[inline] pub fn is_empty(&self) -> bool { self.len() == 0 }
        #[inline] pub fn len(&self) -> usize { let h = self.head.load(Acquire); let t = self.tail.load(Acquire); h.saturating_sub(t) }
    }
    impl<T> Drop for FastMpmc<T> {
        fn drop(&mut self) {
            while let Some(_v) = self.pop() {}
            unsafe {
                let size = self.cap * core::mem::size_of::<Slot<T>>();
                let layout = std::alloc::Layout::from_size_align_unchecked(size, 2*1024*1024);
                dealloc(self.buf as *mut u8, layout);
                let l64 = std::alloc::Layout::array::<AtomicU64>(self.cap).unwrap();
                dealloc(self.meta_deadline as *mut u8, l64);
                let l32 = std::alloc::Layout::array::<AtomicU32>(self.cap).unwrap();
                dealloc(self.meta_flags as *mut u8, l32);
            }
        }
    }
}

// ========================= PATCH 20: L3 HOT/COLD rings w/ promotion =========================
// Features: fast_ring, numa_rings, numa_hotcold
#[cfg(all(target_os="linux", feature="numa_hotcold"))]
mod l3_hotcold {
    use super::*;
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicU32, Ordering::Relaxed};

    // Tuning knobs (env):
    pub(super) static HOT_PRIO_MIN: Lazy<u8> = Lazy::new(|| std::env::var("QAB_HOT_PRIO_MIN").ok().and_then(|s| s.parse().ok()).unwrap_or(192));
    pub(super) static HOT_PROFIT_MIN: Lazy<u64> = Lazy::new(|| std::env::var("QAB_HOT_PROFIT_MIN_LAMPORTS").ok().and_then(|s| s.parse().ok()).unwrap_or(10_000_000));
    pub(super) static COLD_PROMOTE_US: Lazy<u64> = Lazy::new(|| std::env::var("QAB_COLD_PROMOTE_US").ok().and_then(|s| s.parse().ok()).unwrap_or(150));
    pub(super) static PROMOTE_BUDGET: Lazy<usize> = Lazy::new(|| std::env::var("QAB_PROMOTE_BUDGET").ok().and_then(|s| s.parse().ok()).unwrap_or(32));

    #[derive(Clone)]
    pub struct L3GroupHC {
        pub cpus: Vec<usize>,
        pub hot: Arc<InboxQ<ShardItem>>,
        pub cold: Arc<InboxQ<ShardItem>>,
        pub load: Arc<AtomicU64>,      // hot+cold approx
        pub hot_load: Arc<AtomicU64>,  // hot approx
        pub futex_seq: Arc<AtomicU32>, // for PATCH 21 wake-one
        pub idle_mask: Arc<AtomicU32>, // for PATCH 23 bitset lanes
        #[cfg(feature = "l3_cwnd")]
        pub cwnd: Arc<l3_cwnd::Cwnd>,  // PATCH 30 congestion window
    }

    // Build groups from effective cpuset and L3 topology
    static GROUPS: Lazy<Vec<L3GroupHC>> = Lazy::new(|| {
        use std::collections::BTreeMap;
        // Discover allowed CPUs. Prefer cpuset if available; otherwise use core_affinity
        let allowed: Vec<usize> = (|| {
            #[cfg(feature = "numa")]
            {
                if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/cpuset.cpus") {
                    // parse like "0-7" or "0,2,4,6"
                    let mut v = Vec::new();
                    for part in s.trim().split(',') {
                        if let Some((a,b)) = part.split_once('-') {
                            if let (Ok(mut lo), Ok(hi)) = (a.parse::<usize>(), b.parse::<usize>()) {
                                if lo>hi { core::mem::swap(&mut lo, &mut (lo)); }
                                for x in lo..=hi { v.push(x); }
                            }
                        } else if let Ok(x) = part.parse::<usize>() { v.push(x); }
                    }
                    if !v.is_empty() { return v; }
                }
            }
            core_affinity::get_core_ids()
                .map(|v| v.into_iter().map(|c| c.id).collect())
                .unwrap_or_else(|| vec![0])
        })();

        // Group by shared L3 key if available, else single group
        let mut byk: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        #[cfg(feature = "numa_rings")]
        {
            // Best-effort Linux topology; fall back to single group on error
            for &c in &allowed {
                let key = crate::linux_topology::l3_key(c).unwrap_or_else(|| "unknown".to_string());
                byk.entry(key).or_default().push(c);
            }
        }
        #[cfg(not(feature = "numa_rings"))]
        {
            byk.insert("all".to_string(), allowed.clone());
        }

        let base_cap = (super::MAX_BATCH_SIZE * 64).next_power_of_two();
        let mut out = Vec::with_capacity(byk.len());
        for (_k, mut cpus) in byk.into_iter() {
            cpus.sort_unstable();
            let hot = Arc::new(InboxQ::with_capacity(base_cap));
            let cold = Arc::new(InboxQ::with_capacity(base_cap));
            out.push(L3GroupHC {
                cpus,
                hot,
                cold,
                load: Arc::new(AtomicU64::new(0)),
                hot_load: Arc::new(AtomicU64::new(0)),
                futex_seq: Arc::new(AtomicU32::new(0)),
                idle_mask: Arc::new(AtomicU32::new(0)),
                #[cfg(feature = "l3_cwnd")]
                cwnd: Arc::new(l3_cwnd::Cwnd::new(1024)),
            });
        }
        out
    });

    #[inline] pub fn groups() -> &'static [L3GroupHC] { &*GROUPS }

    #[inline]
    pub fn cpu_to_group(cpu: usize) -> usize {
        for (i, g) in GROUPS.iter().enumerate() {
            if g.cpus.binary_search(&cpu).is_ok() { return i; }
        }
        0
    }

    // Decide HOT vs COLD
    #[inline]
    pub fn is_hot(item: &ShardItem) -> bool {
        #[cfg(feature="advanced_sched")]
        {
            if item.priority >= *HOT_PRIO_MIN { return true; }
            if item.profit >= *HOT_PROFIT_MIN { return true; }
            let age = clock::now_us().saturating_sub(item.enq_us);
            if age >= *COLD_PROMOTE_US { return true; }
            return false;
        }
        #[cfg(not(feature="advanced_sched"))]
        { true }
    }

    // Promotion from cold to hot if age exceeded. Returns promoted count.
    #[inline]
    pub fn promote(g: &L3GroupHC, budget: usize) -> usize {
        let mut moved = 0usize;
        let now = clock::now_us();
        while moved < budget {
            if let Some(it) = g.cold.pop() {
                #[cfg(feature="advanced_sched")]
                {
                    if now.saturating_sub(it.enq_us) < *COLD_PROMOTE_US {
                        if g.cold.push(it).is_ok() { break; } else { let _ = g.hot.push(it); g.hot_load.fetch_add(1, Relaxed); }
                    } else {
                        if g.hot.push(it).is_ok() { g.hot_load.fetch_add(1, Relaxed); moved += 1; }
                        else { /* hot full, give back to cold if possible */ }
                    }
                }
                #[cfg(not(feature="advanced_sched"))]
                { let _ = g.hot.push(it); moved += 1; }
            } else { break; }
        }
        moved
    }

    #[inline] pub fn enqueue(g: &L3GroupHC, it: ShardItem) {
        // Stamp flags/deadline when ring_meta is enabled
        #[cfg(feature = "ring_meta")]
        {
            let deadline = {
                #[cfg(feature = "slot_phase")] { crate::slot_phase::align_deadline_us(it.enq_us) }
                #[cfg(not(feature = "slot_phase"))] { it.enq_us }
            };
            if is_hot(&it) {
                let _ = g.hot.push_with_meta(Arc::clone(&it), fast_ring_meta::META_F_HOT0 | fast_ring_meta::META_F_DEADLINE, deadline)
                    .or_else(|x| g.cold.push_with_meta(x, 0, deadline));
                g.hot_load.fetch_add(1, Relaxed);
            } else {
                let _ = g.cold.push_with_meta(it, 0, deadline);
            }
            g.load.fetch_add(1, Relaxed);
            return;
        }
        // Fallback path without ring_meta
        if is_hot(&it) {
            if g.hot.push(Arc::clone(&it)).is_ok() { g.hot_load.fetch_add(1, Relaxed); }
            else { let _ = g.cold.push(it); }
        } else {
            let _ = g.cold.push(it);
        }
        g.load.fetch_add(1, Relaxed);
    }
    #[inline] pub fn drain_hot(g: &L3GroupHC, out: &mut Vec<ShardItem>, target: usize) {
        #[cfg(feature = "ring_meta")]
        {
            while out.len() < target { if let Some((it, _f, _d)) = g.hot.pop_with_meta() { g.hot_load.fetch_sub(1, Relaxed); out.push(it); } else { break; } }
            return;
        }
        while out.len() < target { if let Some(it) = g.hot.pop() { g.hot_load.fetch_sub(1, Relaxed); out.push(it); } else { break; } }
    }
    #[inline] pub fn drain_cold(g: &L3GroupHC, out: &mut Vec<ShardItem>, target: usize) {
        #[cfg(feature = "ring_meta")]
        {
            while out.len() < target { if let Some((it, _f, _d)) = g.cold.pop_with_meta() { out.push(it); } else { break; } }
            return;
        }
        while out.len() < target { if let Some(it) = g.cold.pop() { out.push(it); } else { break; } }
    }
    #[inline] pub fn total_load(g: &L3GroupHC) -> u64 { g.load.load(Relaxed) }
    #[inline] pub fn hot_only_load(g: &L3GroupHC) -> u64 { g.hot_load.load(Relaxed) }
    #[inline] pub fn on_pop(g: &L3GroupHC, popped: usize) { if popped > 0 { g.load.fetch_sub(popped as u64, Relaxed); } }
}

// ========================= PATCH 21: per-group futex wake (Linux only) =========================
#[cfg(all(target_os="linux", feature="group_futex"))]
mod group_futex {
    use super::*;
    #[inline] fn futex_wait(addr: &std::sync::atomic::AtomicU32, val: u32, timeout_us: u32) {
        unsafe {
            let ts = libc::timespec { tv_sec: (timeout_us / 1_000_000) as i64, tv_nsec: ((timeout_us % 1_000_000) * 1000) as i64 };
            let ptr = addr as *const _ as *mut u32;
            let _ = libc::syscall(libc::SYS_futex, ptr, libc::FUTEX_WAIT, val, &ts, 0, 0);
        }
    }
    #[inline] fn futex_wake(addr: &std::sync::atomic::AtomicU32, n: i32) {
        unsafe {
            let ptr = addr as *const _ as *mut u32;
            let _ = libc::syscall(libc::SYS_futex, ptr, libc::FUTEX_WAKE, n, 0, 0, 0);
        }
    }
    pub fn wake_one(seq: &std::sync::atomic::AtomicU32) { let _ = seq.fetch_add(1, std::sync::atomic::Ordering::Release); futex_wake(seq, 1); }
    pub fn park(seq: &std::sync::atomic::AtomicU32, idle_backoff_us: u64) { let v = seq.load(std::sync::atomic::Ordering::Acquire); futex_wait(seq, v, idle_backoff_us as u32); }
}
#[cfg(not(all(target_os="linux", feature="group_futex")))]
mod group_futex { pub fn wake_one(_seq: &std::sync::atomic::AtomicU32) {} pub fn park(_seq: &std::sync::atomic::AtomicU32, _idle_backoff_us: u64) {} }

// ========================= PATCH 32: HOT0/HOT1 tiers with ring metadata =========================
// Features: numa_hotcold_tiers, fast_ring, ring_meta
#[cfg(all(target_os="linux", feature="numa_hotcold_tiers"))]
mod l3_tiers {
    use super::*;
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    static HOT0_AGE_US: Lazy<u64> = Lazy::new(|| std::env::var("QAB_HOT0_AGE_US").ok().and_then(|s| s.parse().ok()).unwrap_or(100));
    static HOT1_PROMOTE_BUDGET: Lazy<usize> = Lazy::new(|| std::env::var("QAB_HOT1_PROMOTE_BUDGET").ok().and_then(|s| s.parse().ok()).unwrap_or(64));

    #[derive(Clone)]
    pub struct L3GroupTier {
        pub cpus: Vec<usize>,
        pub hot0: Arc<InboxQ<ShardItem>>,
        pub hot1: Arc<InboxQ<ShardItem>>,
        pub cold: Arc<InboxQ<ShardItem>>,
        pub load: Arc<AtomicU64>,
        pub hot0_load: Arc<AtomicU64>,
        pub hot1_load: Arc<AtomicU64>,
        pub futex_seq: Arc<std::sync::atomic::AtomicU32>,
        pub idle_mask: Arc<std::sync::atomic::AtomicU32>,
        #[cfg(feature="l3_cwnd")]
        pub cwnd: Arc<crate::l3_cwnd::Cwnd>,
        #[cfg(all(feature="token_wake"))]
        pub wake_tokens: Arc<std::sync::atomic::AtomicI32>,
    }

    static GROUPS: Lazy<Vec<L3GroupTier>> = Lazy::new(|| {
        use crate::linux_topology::*;
        use std::collections::BTreeMap;
        let allowed = discover_cpuset_effective()
            .unwrap_or_else(|| core_affinity::get_core_ids().map(|v| v.into_iter().map(|c| c.id).collect()).unwrap_or(vec![0]));
        let mut byk: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for &c in &allowed { let k = l3_key(c).unwrap_or_else(|| "unknown".to_string()); byk.entry(k).or_default().push(c); }
        let base_cap = (super::MAX_BATCH_SIZE * 64).next_power_of_two();
        let mut out = Vec::with_capacity(byk.len());
        for (_, mut cpus) in byk.into_iter() {
            cpus.sort_unstable();
            let hot0 = Arc::new(InboxQ::with_capacity(base_cap));
            let hot1 = Arc::new(InboxQ::with_capacity(base_cap));
            let cold = Arc::new(InboxQ::with_capacity(base_cap));
            out.push(L3GroupTier {
                cpus,
                hot0, hot1, cold,
                load: Arc::new(AtomicU64::new(0)),
                hot0_load: Arc::new(AtomicU64::new(0)),
                hot1_load: Arc::new(AtomicU64::new(0)),
                futex_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
                idle_mask: Arc::new(std::sync::atomic::AtomicU32::new(0)),
                #[cfg(feature="l3_cwnd")]
                cwnd: Arc::new(crate::l3_cwnd::Cwnd::new(1024)),
                #[cfg(all(feature="token_wake"))]
                wake_tokens: Arc::new(std::sync::atomic::AtomicI32::new(0)),
            });
        }
        out
    });

    #[inline] pub fn groups() -> &'static [L3GroupTier] { &*GROUPS }
    #[inline] pub fn cpu_to_group(cpu: usize) -> usize { for (i,g) in GROUPS.iter().enumerate() { if g.cpus.binary_search(&cpu).is_ok() { return i; } } 0 }

    // Enqueue with flags and deadline via metadata
    #[inline]
    pub fn enqueue(g: &L3GroupTier, it: ShardItem, flags: u32, deadline_us: u64) {
        #[cfg(all(feature="ring_meta"))]
        {
            let f = flags | fast_ring_meta::META_F_DEADLINE;
            let hot0 = (f & fast_ring_meta::META_F_HOT0) != 0;
            let hot1 = (f & fast_ring_meta::META_F_HOT1) != 0 && !hot0;
            let ok = if hot0 {
                g.hot0.push_with_meta(Arc::clone(&it), f, deadline_us).is_ok().then(|| { g.hot0_load.fetch_add(1, Relaxed); true }).unwrap_or(false)
            } else if hot1 {
                g.hot1.push_with_meta(Arc::clone(&it), f, deadline_us).is_ok().then(|| { g.hot1_load.fetch_add(1, Relaxed); true }).unwrap_or(false)
            } else {
                g.cold.push_with_meta(it, f, deadline_us).is_ok()
            };
            if ok { g.load.fetch_add(1, Relaxed); }
            return;
        }
        // Fallback to existing hot/cold if ring_meta disabled (should not happen under tiers)
        let _ = g.cold.push(it);
        g.load.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn drain_tiered(g: &L3GroupTier, out: &mut Vec<ShardItem>, target: usize) {
        // 1) HOT0
        while out.len() < target {
            if let Some((it, _f, _dl)) = g.hot0.pop_with_meta() { g.hot0_load.fetch_sub(1, Relaxed); out.push(it); } else { break; }
        }
        if out.len() >= target { return; }
        // 2) Promote HOT1 -> HOT0 by nearing deadline, else execute
        let now = crate::clock::now_us(); let mut moved = 0usize;
        while moved < *HOT1_PROMOTE_BUDGET && out.len() < target {
            if let Some((it, _f, dl)) = g.hot1.pop_with_meta() {
                g.hot1_load.fetch_sub(1, Relaxed);
                if dl > 0 && now + *HOT0_AGE_US >= dl {
                    if g.hot0.push_with_meta(Arc::clone(&it), fast_ring_meta::META_F_HOT0 | fast_ring_meta::META_F_DEADLINE, dl).is_ok() { g.hot0_load.fetch_add(1, Relaxed); moved += 1; continue; }
                }
                out.push(it);
            } else { break; }
        }
        if out.len() >= target { return; }
        // 3) HOT1 rest
        while out.len() < target { if let Some((it, _f, _dl)) = g.hot1.pop_with_meta() { g.hot1_load.fetch_sub(1, Relaxed); out.push(it); } else { break; } }
        if out.len() >= target { return; }
        // 4) COLD
        while out.len() < target { if let Some((it, _f, _dl)) = g.cold.pop_with_meta() { out.push(it); } else { break; } }
    }

    #[inline] pub fn on_pop(g: &L3GroupTier, popped: usize) { if popped > 0 { g.load.fetch_sub(popped as u64, Relaxed); } }
}

// ========================= PATCH 33: Token-grant wake (cwnd × bitset) =========================
#[cfg(all(target_os="linux", feature="token_wake", feature="group_futex_bitset", feature="l3_cwnd", feature="numa_hotcold_tiers"))]
mod group_token_wake {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering::{AcqRel, Acquire, Relaxed}};
    #[inline]
    pub fn grant_and_wake(g: &crate::l3_tiers::L3GroupTier, tokens: &AtomicI32, want: i32) {
        let inflight = g.load.load(Relaxed) as u32;
        let head = g.cwnd.headroom(inflight).max(0) as i32;
        let idle = g.idle_mask.load(Relaxed).count_ones() as i32;
        let grant = want.min(head).min(idle).max(0);
        if grant <= 0 { return; }
        let _ = tokens.fetch_add(grant, AcqRel);
        let mut left = grant; let mut snap = g.idle_mask.load(Relaxed);
        while left > 0 && snap != 0 {
            let bit = snap.trailing_zeros() as u8;
            let _ = g.futex_seq.fetch_add(1, std::sync::atomic::Ordering::Release);
            unsafe { let ptr = &g.futex_seq as *const _ as *mut u32; libc::syscall(libc::SYS_futex, ptr, 10, 1, 0, 0, 1u32 << bit); }
            snap &= !(1u32 << bit); left -= 1;
        }
    }
    #[inline]
    pub fn try_consume(tokens: &AtomicI32) -> bool {
        let mut cur = tokens.load(Acquire);
        while cur > 0 { match tokens.compare_exchange_weak(cur, cur-1, AcqRel, Acquire) { Ok(_) => return true, Err(n) => cur = n } }
        false
    }
}

// ========================= PATCH 34: Profit S-curve + deadline/phase routing =========================
#[cfg(all(feature="route_cost_scurve", feature="route_cost", feature="numa_hotcold_tiers"))]
mod route_cost_scurve {
    use once_cell::sync::Lazy;
    static LAMPORTS_MID: Lazy<f64> = Lazy::new(|| std::env::var("QAB_PROFIT_MID").ok().and_then(|s| s.parse().ok()).unwrap_or(5e7));
    static LAMPORTS_K:   Lazy<f64> = Lazy::new(|| std::env::var("QAB_PROFIT_K").ok().and_then(|s| s.parse().ok()).unwrap_or(1.2e7));
    static PHASE_BONUS_BP: Lazy<i64> = Lazy::new(|| std::env::var("QAB_PHASE_BONUS_BP").ok().and_then(|s| s.parse().ok()).unwrap_or(800));
    #[inline] pub fn profit_ns_credit_scurve(profit_lamports: u64, slope_ns_per_lamport: u64) -> u64 {
        let x = profit_lamports as f64; let s = 1.0 / (1.0 + (-(x - *LAMPORTS_MID) / *LAMPORTS_K).exp());
        let raw = (slope_ns_per_lamport as f64) * x; (raw * s).min(9.22e18).max(0.0) as u64
    }
    #[inline] pub fn phase_bp_predicted_finish(pred_finish_us: u64) -> i64 {
        #[cfg(feature="slot_phase")] {
            let slot = crate::slot_phase::slot_us(); let pos = pred_finish_us % slot;
            if pos <= slot/4 || pos >= slot*3/4 { *PHASE_BONUS_BP as i64 } else { -(*PHASE_BONUS_BP as i64) }
        }
        #[cfg(not(feature="slot_phase"))] { 0 }
    }
}
// ========================= PATCH 23: Futex bitset wake masks =========================
// Features: group_futex_bitset, numa_hotcold
#[cfg(all(target_os="linux", feature="group_futex_bitset", feature="numa_hotcold"))]
mod group_futex_bitset {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

    #[allow(non_upper_case_globals)] const FUTEX_WAIT_BITSET: i32 = 9;
    #[allow(non_upper_case_globals)] const FUTEX_WAKE_BITSET: i32 = 10;

    #[inline] fn futex_wait_bitset(addr: &AtomicU32, val: u32, mask: u32, timeout_us: u32) {
        unsafe {
            let ts = libc::timespec { tv_sec: (timeout_us / 1_000_000) as i64, tv_nsec: ((timeout_us % 1_000_000) * 1000) as i64 };
            let ptr = addr as *const _ as *mut u32;
            let _ = libc::syscall(libc::SYS_futex, ptr, FUTEX_WAIT_BITSET, val, &ts, 0, mask);
        }
    }
    #[inline] fn futex_wake_bitset(addr: &AtomicU32, mask: u32) {
        unsafe {
            let ptr = addr as *const _ as *mut u32;
            let _ = libc::syscall(libc::SYS_futex, ptr, FUTEX_WAKE_BITSET, 1, 0, 0, mask);
        }
    }

    #[inline] pub fn set_idle(mask: &AtomicU32, bit: u8)   { let m = 1u32 << bit; mask.fetch_or(m, Relaxed); }
    #[inline] pub fn clear_idle(mask: &AtomicU32, bit: u8) { let m = !(1u32 << bit); mask.fetch_and(m, Relaxed); }

    #[inline]
    fn pick_lane_rr(seq: &AtomicU32, mask: u32) -> Option<u8> {
        if mask == 0 { return None; }
        // Rotate starting position by seq to avoid bias
        let base = (seq.load(std::sync::atomic::Ordering::Relaxed) & 31) as u8;
        for off in 0..32u8 {
            let bit = ((base as u32 + off as u32) & 31) as u8;
            if (mask & (1u32 << bit)) != 0 { return Some(bit); }
        }
        None
    }

    #[inline] pub fn wake_one(seq: &AtomicU32, idle_mask: &AtomicU32) {
        let snap = idle_mask.load(std::sync::atomic::Ordering::Relaxed);
        if let Some(bit) = pick_lane_rr(seq, snap) {
            let _ = seq.fetch_add(1, std::sync::atomic::Ordering::Release);
            futex_wake_bitset(seq, 1u32 << bit);
        }
    }

    #[inline] pub fn park(seq: &AtomicU32, idle_mask: &AtomicU32, lane_bit: u8, idle_backoff_us: u64) {
        let seqv = seq.load(std::sync::atomic::Ordering::Acquire);
        let lane_mask = 1u32 << lane_bit;
        futex_wait_bitset(seq, seqv, lane_mask, idle_backoff_us as u32);
    }

    // Auto-scale wakeups based on system load. Returns the number of workers woken.
    #[inline] pub fn wake_auto(
        seq: &AtomicU32, 
        idle_mask: &AtomicU32, 
        work_queue_depth: usize,  // Current depth of the work queue
        max_workers: usize,       // Maximum number of workers to wake
        min_workers: usize,       // Minimum workers to ensure progress
    ) -> usize {
        // Load current idle mask
        let idle = idle_mask.load(Relaxed);
        if idle == 0 {
            return 0;  // No idle workers to wake
        }

        // Calculate how many workers to wake based on queue depth
        // Use log2(work_queue_depth + 1) to scale wakeups with load
        let log2_plus1 = 64 - (work_queue_depth + 1).leading_zeros() as usize;
        let k = (log2_plus1 * 2).clamp(min_workers, max_workers.min(32));
        
        if k == 0 {
            return 0;
        }

        // Find up to k idle workers to wake
        let mut to_wake = 0u32;
        let mut count = 0;
        let mut mask = idle;
        
        while mask != 0 && count < k {
            let bit = mask.trailing_zeros() as u8;
            to_wake |= 1u32 << bit;
            mask ^= 1u32 << bit;  // Clear the bit
            count += 1;
        }

        if to_wake != 0 {
            // Use fetch_or to avoid races with concurrent wake_auto calls
            let old = idle_mask.fetch_and(!to_wake, Relaxed);
            let actually_woke = old & to_wake;
            if actually_woke != 0 {
                futex_wake_bitset(seq, actually_woke);
                actually_woke.count_ones() as usize
            } else {
                0
            }
        } else {
            0
        }
    }

    // Proportional multi-wake with CWND awareness. Returns the number of workers woken.
    //
    // This is similar to wake_auto but uses the congestion window (cwnd) to scale
    // the number of workers woken, which helps maintain high throughput under
    // congestion while avoiding excessive wakeups during backoff.
    //
    // # Arguments
    // - seq: Sequence number for futex operations (must be the same for all calls)
    // - idle_mask: Bitmask of idle workers (1 = idle)
    // - cwnd: Current congestion window (packets in flight)
    // - min_cwnd: Minimum cwnd to start waking workers
    // - max_workers: Maximum number of workers to wake in one call
    // - min_workers: Minimum workers to ensure progress
    //
    // # Returns
    // Number of workers actually woken (0 if none available or cwnd too low)
    #[inline] 
    pub fn wake_auto_cwnd(
        seq: &AtomicU32,
        idle_mask: &AtomicU32,
        cwnd: i32,              // Current congestion window
        min_cwnd: i32,          // Minimum cwnd to start waking workers
        max_workers: usize,     // Maximum workers to wake
        min_workers: usize,     // Minimum workers to ensure progress
    ) -> usize {
        // If we're below min_cwnd, don't wake any workers (exponential backoff)
        if cwnd < min_cwnd {
            return 0;
        }

        // Load current idle mask
        let idle = idle_mask.load(Relaxed);
        if idle == 0 {
            return 0;  // No idle workers to wake
        }

        // Calculate how many workers to wake based on cwnd headroom
        // This gives us a smooth scaling from min_workers to max_workers
        // as cwnd grows from min_cwnd to 2*min_cwnd
        let cwnd_ratio = ((cwnd - min_cwnd) as f32 / min_cwnd as f32).clamp(0.0, 1.0);
        let target_workers = min_workers + ((max_workers - min_workers) as f32 * cwnd_ratio.sqrt()) as usize;
        
        // Ensure we don't wake more workers than are actually idle
        let k = target_workers.min(idle.count_ones() as usize);
        if k == 0 {
            return 0;
        }

        // Find up to k idle workers to wake
        let mut to_wake = 0u32;
        let mut count = 0;
        let mut mask = idle;
        
        while mask != 0 && count < k {
            let bit = mask.trailing_zeros() as u8;
            to_wake |= 1u32 << bit;
            mask ^= 1u32 << bit;  // Clear the bit
            count += 1;
        }

        if to_wake != 0 {
            // Atomically clear the bits we're trying to wake
            let old = idle_mask.fetch_and(!to_wake, Relaxed);
            let actually_woke = old & to_wake;
            if actually_woke != 0 {
                // Wake the workers
                futex_wake_bitset(seq, actually_woke);
                return actually_woke.count_ones() as usize;
            }
        }
        
        0
        }
    }
}

#[cfg(all(feature="numa_hotcold"))]
mod group_bitlanes_assign {
    pub fn assign(my_group: &[usize], groups_len: usize, num_workers: usize) -> Vec<u8> {
        let mut lanes = vec![0xFFu8; num_workers];
        let mut used = vec![0u32; groups_len];
        for w in 0..num_workers {
            let gid = my_group[w];
            let m = used[gid];
            if m.count_ones() < 32 {
                let bit = (!m).trailing_zeros() as u8;
                used[gid] |= 1u32 << bit;
                lanes[w] = bit;
            } else {
                lanes[w] = 0xFF;
            }
        }
        lanes
    }
}

// ========================= PATCH 25: CU-budget controller =========================
#[cfg(feature="cu_budget")]
mod cu_budget {
    use once_cell::sync::Lazy;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static CU_PER_TX_EST: Lazy<AtomicU64> = Lazy::new(|| {
        let v = std::env::var("QAB_CU_PER_TX_EST").ok().and_then(|s| s.parse().ok()).unwrap_or(200_000);
        AtomicU64::new(v.max(1))
    });
    static CU_BUDGET: Lazy<AtomicU64> = Lazy::new(|| {
        let v = std::env::var("QAB_RPC_INIT_CREDIT").ok().and_then(|s| s.parse().ok()).unwrap_or(20_000_000);
        AtomicU64::new(v)
    });
    static MAX_INFLIGHT: Lazy<AtomicU64> = Lazy::new(|| {
        let v = std::env::var("QAB_MAX_INFLIGHT").ok().and_then(|s| s.parse().ok()).unwrap_or(4096);
        AtomicU64::new(v.max(8))
    });
    #[inline] pub fn set_budget_cu(cu: u64)       { CU_BUDGET.store(cu, Relaxed); }
    #[inline] pub fn add_budget_cu(delta: i64)    { let cur = CU_BUDGET.load(Relaxed) as i128 + delta as i128; CU_BUDGET.store(cur.max(0) as u64, Relaxed); }
    #[inline] pub fn set_cu_per_tx_est(v: u64)    { CU_PER_TX_EST.store(v.max(1), Relaxed); }
    #[inline] pub fn set_max_inflight(v: u64)     { MAX_INFLIGHT.store(v.max(8), Relaxed); }
    #[inline] pub fn inflight_allow() -> u64 {
        let cu = CU_BUDGET.load(Relaxed);
        let per = CU_PER_TX_EST.load(Relaxed);
        let allow = cu / per; allow.clamp(8, MAX_INFLIGHT.load(Relaxed))
    }
}

// ========================= PATCH 28α: RDPMC fast-path (no syscalls steady-state) =========================
#[cfg(all(target_os="linux", feature="rdpmc_ipc"))]
mod perf_ipc {
    use std::mem::{size_of, transmute};
    use std::os::unix::io::RawFd;
    use std::ptr::{null, null_mut};

    #[repr(C)]
    struct perf_event_attr {
        type_: u32, size: u32, config: u64,
        sample_period: u64, sample_type: u64, read_format: u64,
        flags: u64,
    }
    const PERF_TYPE_HARDWARE: u32 = 0;
    const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
    const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
    const PERF_FLAG_FD_CLOEXEC: u64 = 1;

    #[repr(C)]
    pub(super) struct PerfPage {
        _cap_0: [u8; 32],
        pub index: u32,
        _cap_1: [u8; 20],
        pub offset: i64,
        _cap_2: [u8; 24],
        pub capabilities: u64,
        pub pmc_width: u16,
        _pad2: [u8; 4064],
    }

    #[inline(always)]
    #[cfg(target_arch="x86_64")]
    fn rdpmc(idx: u32) -> u64 {
        let lo: u32; let hi: u32;
        unsafe { core::arch::asm!("rdpmc", in("ecx") idx, out("eax") lo, out("edx") hi, options(nostack, preserves_flags)); }
        ((hi as u64) << 32) | (lo as u64)
    }
    #[cfg(not(target_arch="x86_64"))]
    #[inline(always)] fn rdpmc(_idx: u32) -> u64 { 0 }

    pub(super) struct Ctr { pub fd: RawFd, pub page: *const PerfPage, prev: u64, have_rdpmc: bool }
    unsafe impl Send for Ctr {} unsafe impl Sync for Ctr {}
    impl Ctr {
        fn open(config: u64) -> Option<Self> {
            let attr = perf_event_attr {
                type_: PERF_TYPE_HARDWARE,
                size: size_of::<perf_event_attr>() as u32,
                config,
                sample_period: 0, sample_type: 0, read_format: 0,
                flags: (1<<0) | (1<<5) | (1<<6) | (1<<3),
            };
            let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, &attr as *const perf_event_attr, 0, -1, -1, PERF_FLAG_FD_CLOEXEC as u64) as RawFd };
            if fd < 0 { return None; }
            let pg = unsafe { libc::mmap(null_mut(), 4096, libc::PROT_READ, libc::MAP_SHARED, fd, 0) };
            if pg == libc::MAP_FAILED { unsafe { libc::close(fd); } return None; }
            unsafe { libc::ioctl(fd, libc::PERF_EVENT_IOC_RESET, 0); libc::ioctl(fd, libc::PERF_EVENT_IOC_ENABLE, 0); }
            let perf: &PerfPage = unsafe { &*(pg as *const PerfPage) };
            let have = perf.index != 0 && ((perf.capabilities & (1<<1)) != 0);
            Some(Self { fd, page: pg as *const PerfPage, prev: 0, have_rdpmc: have })
        }
        #[inline(always)] fn read_fast(&self) -> u64 {
            if self.have_rdpmc {
                let perf = unsafe { &*self.page };
                let idx = perf.index;
                if idx != 0 {
                    let width = perf.pmc_width.max(32) as u32;
                    let mask = if width >= 63 { u64::MAX } else { (1u64 << width) - 1 };
                    let raw = rdpmc(idx);
                    return (raw & mask).wrapping_add(perf.offset as u64);
                }
            }
            unsafe { let mut v: u64 = 0; let r = libc::read(self.fd, &mut v as *mut u64 as *mut _, size_of::<u64>()); if r as usize == size_of::<u64>() { v } else { 0 } }
        }
        #[inline(always)] fn delta(&mut self) -> u64 { let cur = self.read_fast(); let d = cur.wrapping_sub(self.prev); self.prev = cur; d }
    }

    thread_local! {
        pub(super) static CYC: std::cell::UnsafeCell<Option<Ctr>> = std::cell::UnsafeCell::new(None);
        pub(super) static INS: std::cell::UnsafeCell<Option<Ctr>> = std::cell::UnsafeCell::new(None);
        static LAST_SNAP_TSC: std::cell::Cell<u64> = std::cell::Cell::new(0);
    }

    pub fn init_thread() {
        CYC.with(|c| unsafe { if (*c.get()).is_none() { *c.get() = Ctr::open(PERF_COUNT_HW_CPU_CYCLES); }});
        INS.with(|i| unsafe { if (*i.get()).is_none() { *i.get() = Ctr::open(PERF_COUNT_HW_INSTRUCTIONS); }});
        LAST_SNAP_TSC.with(|t| t.set(crate::clock::now_cycles_serialized()));
    }

    #[inline]
    pub fn sample_ipc_throttled(min_cyc_gap: u64) -> Option<(u64, u64, f64)> {
        let now = crate::clock::now_cycles_serialized();
        let mut last = 0u64;
        let mut gate_ok = false;
        LAST_SNAP_TSC.with(|t| { last = t.get(); if now.wrapping_sub(last) >= min_cyc_gap { t.set(now); gate_ok = true; } });
        if !gate_ok { return None; }
        let dc = CYC.with(|c| unsafe { (*c.get()).as_mut().map(|x| x.delta()) }).unwrap_or(0);
        let di = INS.with(|i| unsafe { (*i.get()).as_mut().map(|x| x.delta()) }).unwrap_or(0);
        if dc == 0 { return None; }
        Some((dc, di, (di as f64)/(dc as f64)))
    }

    #[inline]
    pub fn sample_ipc() -> Option<(u64, u64, f64)> { sample_ipc_throttled(0) }
}
#[cfg(not(all(target_os="linux", feature="rdpmc_ipc")))]
mod perf_ipc { pub fn init_thread() {} pub fn sample_ipc() -> Option<(u64,u64,f64)> { None } }

// ========================= PATCH 28β: IPC EWMA + smooth scaler =========================
#[cfg(all(target_os="linux", feature="rdpmc_ipc"))]
mod ipc_scale {
    use once_cell::sync::Lazy;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static IPC_ALPHA_MILLIS: Lazy<u64> = Lazy::new(|| std::env::var("QAB_IPC_EWMA_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(50));
    static IPC_TARGET:      Lazy<f64> = Lazy::new(|| std::env::var("QAB_IPC_TARGET").ok().and_then(|s| s.parse().ok()).unwrap_or(2.0));
    static SCALE_MIN:       Lazy<f64> = Lazy::new(|| std::env::var("QAB_IPC_SCALE_MIN").ok().and_then(|s| s.parse().ok()).unwrap_or(0.80));
    static SCALE_MAX:       Lazy<f64> = Lazy::new(|| std::env::var("QAB_IPC_SCALE_MAX").ok().and_then(|s| s.parse().ok()).unwrap_or(1.35));
    thread_local! { static EWMA_IPC: std::cell::Cell<f64> = std::cell::Cell::new(*IPC_TARGET); static LAST_US:  std::cell::Cell<u64> = std::cell::Cell::new(0); }
    #[inline]
    pub fn update_and_scale(ipc_sample: f64) -> f64 {
        let now = crate::clock::now_us();
        // Load previous state
        let (prev_ipc, last_us) = (
            EWMA_IPC.with(|s| s.get()),
            LAST_US.with(|t| t.get())
        );
        let dt_us = now.saturating_sub(last_us).max(1);
        LAST_US.with(|t| t.set(now));

        let tau_us = (*IPC_ALPHA_MILLIS).saturating_mul(1_000).max(1);
        let alpha  = 1.0 - (- (dt_us as f64) / (tau_us as f64)).exp(); // time-aware EWMA

        let next = prev_ipc + alpha * (ipc_sample - prev_ipc);
        EWMA_IPC.with(|s| s.set(next));

        // Scale target→actual smoothly; clamp to safe envelope
        (*IPC_TARGET / next.max(0.25)).powf(0.7).clamp(*SCALE_MIN, *SCALE_MAX)
    }
}

// ========================= PATCH 28γ: Recovery on rdpmc index loss =========================
#[cfg(all(target_os="linux", feature="rdpmc_ipc"))]
mod perf_ipc_recover {
    use super::perf_ipc::{PerfPage, CYC, INS};
    pub fn ensure_ready() {
        CYC.with(|c| unsafe { if let Some(ref mut ctr) = *c.get() { let pg = &*ctr.page; if pg.index == 0 { libc::ioctl(ctr.fd, libc::PERF_EVENT_IOC_DISABLE, 0); libc::ioctl(ctr.fd, libc::PERF_EVENT_IOC_ENABLE, 0); } } });
        INS.with(|i| unsafe { if let Some(ref mut ctr) = *i.get() { let pg = &*ctr.page; if pg.index == 0 { libc::ioctl(ctr.fd, libc::PERF_EVENT_IOC_DISABLE, 0); libc::ioctl(ctr.fd, libc::PERF_EVENT_IOC_ENABLE, 0); } } });
    }
}

// ========================= PATCH 61: CPU pinning and real-time scheduling =========================

/// Set real-time scheduling priority (FIFO/RR) if running on Linux with sufficient permissions
#[inline]
pub fn maybe_set_fifo() {
    #[cfg(target_os = "linux")]
    unsafe {
        let class = std::env::var("QAB_SCHED_CLASS")
            .ok()
            .unwrap_or_else(|| "fifo".to_string());
        let prio = std::env::var("QAB_SCHED_PRIO")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50)
            .clamp(1, 99);
        let policy = if class.eq_ignore_ascii_case("rr") {
            libc::SCHED_RR
        } else {
            libc::SCHED_FIFO
        };
        let mut sp = libc::sched_param {
            sched_priority: prio,
        };
        let _ = libc::sched_setscheduler(0, policy, &mut sp as *mut _);
    }
}

/// Parse CPU set from string (e.g., "0-3,8,10-11")
#[cfg(target_os = "linux")]
unsafe fn parse_cpuset(s: &str) -> Vec<usize> {
    let mut v = Vec::new();
    for part in s.split(',') {
        if let Some((a, b)) = part.split_once('-') {
            if let (Ok(lo), Ok(hi)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>()) {
                for c in lo..=hi {
                    v.push(c);
                }
            }
        } else if let Ok(x) = part.trim().parse::<usize>() {
            v.push(x);
        }
    }
    v
}

/// Pin the current thread to specific CPUs based on worker_id and environment configuration
/// 
/// # Arguments
/// * `worker_id` - The ID of the worker thread (used to determine which CPU to pin to)
/// 
/// # Environment Variables
/// - `QAB_CPUSET`: Comma-separated list of CPU IDs or ranges (e.g., "0-3,8-11")
/// - `QAB_CPUSET_MODE`: Set to "exclusive" to pin each worker to a different CPU,
///   or any other value to allow sharing of CPUs between workers
#[inline]
pub fn maybe_pin_cpuset(worker_id: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        if let Ok(mask_s) = std::env::var("QAB_CPUSET") {
            let exclusive = matches!(
                std::env::var("QAB_CPUSET_MODE").as_deref(),
                Ok("exclusive")
            );
            let mut list = parse_cpuset(&mask_s);
            if list.is_empty() {
                return;
            }
            list.sort_unstable();
            list.dedup();

            let cpu = if exclusive {
                // In exclusive mode, assign each worker to a different CPU in round-robin fashion
                list[worker_id % list.len()]
            } else {
                // In shared mode, all workers can use all CPUs
                usize::MAX
            };

            let mut set: libc::cpu_set_t = core::mem::zeroed();
            let sz = core::mem::size_of::<libc::cpu_set_t>();

            if cpu == usize::MAX {
                // Set all CPUs in the list
                for &c in &list {
                    libc::CPU_SET(c, &mut set);
                }
            } else {
                // Set only the specific CPU
                libc::CPU_SET(cpu, &mut set);
            }

            let _ = libc::sched_setaffinity(0, sz, &set as *const _);
        }
    }
}

// ========================= PATCH 6+: vDSO getcpu() + TLS CPU id =========================
#[cfg(target_os = "linux")]
mod fast_getcpu {
    use once_cell::sync::Lazy;
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    type VdsoGetcpu = unsafe extern "C" fn(cpu: *mut u32, node: *mut u32, unused: *mut core::ffi::c_void) -> i32;
    static VDSO_GETCPU: Lazy<Option<VdsoGetcpu>> = Lazy::new(|| unsafe {
        let h = libc::dlopen(b"linux-vdso.so.1\0".as_ptr() as *const i8, libc::RTLD_LAZY);
        if h.is_null() { return None; }
        let sym = libc::dlsym(h, b"__vdso_getcpu\0".as_ptr() as *const i8);
        if sym.is_null() { None } else { Some(std::mem::transmute::<_, VdsoGetcpu>(sym)) }
    });
    #[inline]
    pub fn getcpu() -> usize {
        unsafe {
            if let Some(f) = *VDSO_GETCPU {
                let mut cpu: u32 = 0;
                let rc = f(&mut cpu as *mut u32, core::ptr::null_mut(), core::ptr::null_mut());
                if rc == 0 { return cpu as usize; }
            }
            let c = libc::sched_getcpu(); if c >= 0 { c as usize } else { 0 }
        }
    }
    thread_local! { static TLS_CPU: std::cell::Cell<usize> = std::cell::Cell::new(usize::MAX); }
    static CPU_COUNT: Lazy<usize> = Lazy::new(|| {
        // Try reading online CPUs quickly; fallback to 1
        let online = std::fs::read_to_string("/sys/devices/system/cpu/online").ok();
        if let Some(s) = online {
            // parse like "0-7" or "0,2,4,6"
            let mut v = Vec::new();
            for part in s.trim().split(',') {
                if let Some((a,b)) = part.split_once('-') {
                    if let (Ok(mut lo), Ok(hi)) = (a.parse::<usize>(), b.parse::<usize>()) {
                        if lo>hi { std::mem::swap(&mut lo, &mut (lo)); }
                        for x in lo..=hi { v.push(x); }
                    }
                } else if let Ok(x) = part.parse::<usize>() { v.push(x); }
            }
            return v.len().max(1);
        }
        1
    });
    #[inline] pub fn online_cpus() -> usize { *CPU_COUNT }
    #[inline] pub fn tls_cpu_id() -> usize {
        TLS_CPU.with(|c| { let cur = c.get(); if cur != usize::MAX { return cur; } let v = getcpu() % online_cpus(); c.set(v); v })
    }
    #[inline] pub fn tls_cpu_refresh() { TLS_CPU.with(|c| c.set(getcpu() % online_cpus())); }
}
#[cfg(not(target_os = "linux"))]
mod fast_getcpu { #[inline] pub fn tls_cpu_id() -> usize { 0 } #[inline] pub fn tls_cpu_refresh() {} #[inline] pub fn online_cpus() -> usize { 1 } }
#[inline] fn cpu_lane() -> usize { fast_getcpu::tls_cpu_id() }
#[inline] fn cpu_lanes() -> usize { fast_getcpu::online_cpus() }

// ========================= PATCH 12: rseq per-CPU epoch (Linux) =========================
#[cfg(all(target_os="linux", feature="rseq"))]
mod rseq_fast {
    use once_cell::sync::Lazy;
    use std::cell::{Cell, UnsafeCell};

    #[repr(C)]
    struct Rseq { cpu_id_start: u32, cpu_id: u32, rseq_cs: u64, flags: u32 }

    #[cfg(target_arch="x86_64")] const __NR_RSEQ: i64 = 334;
    #[cfg(target_arch="aarch64")] const __NR_RSEQ: i64 = 293;
    const RSEQ_SIG: u32 = 0x5305_3053;

    thread_local! {
        static TLS_RSEQ: UnsafeCell<Rseq> = UnsafeCell::new(Rseq { cpu_id_start: u32::MAX, cpu_id: u32::MAX, rseq_cs: 0, flags: 0 });
        static RSEQ_READY: Cell<bool> = Cell::new(false);
    }

    #[inline]
    pub fn register() {
        TLS_RSEQ.with(|cell| {
            if RSEQ_READY.with(|r| r.get()) { return; }
            let ptr = cell.get() as *mut libc::c_void;
            let len = std::mem::size_of::<Rseq>() as u32;
            let rc = unsafe { libc::syscall(__NR_RSEQ, ptr, len, 0, RSEQ_SIG) };
            if rc == 0 { RSEQ_READY.with(|r| r.set(true)); }
        })
    }

    #[inline]
    pub fn current_cpu() -> usize {
        TLS_RSEQ.with(|cell| {
            if !RSEQ_READY.with(|r| r.get()) { return super::fast_getcpu::tls_cpu_id(); }
            let cpu = unsafe { (&*cell.get()).cpu_id as usize };
            if cpu == usize::MAX { super::fast_getcpu::tls_cpu_id() } else { cpu }
        })
    }
}

#[cfg(not(all(target_os="linux", feature="rseq")))]
mod rseq_fast { #[inline] pub fn register() {} #[inline] pub fn current_cpu() -> usize { super::fast_getcpu::tls_cpu_id() } }

#[inline] fn lane_cpu() -> usize { rseq_fast::current_cpu() }
#[inline] fn lane_init() { rseq_fast::register(); fast_getcpu::tls_cpu_refresh(); }

// ========================= PATCH 7+: per-CPU counters with batch drain =========================
#[cfg(feature = "advanced_sched")]
mod per_cpu_counters {
    use super::*;
    use once_cell::sync::Lazy;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    pub struct Sharded { lanes: Vec<AtomicU64> }
    impl Sharded {
        pub fn new() -> Self { Self { lanes: (0..cpu_lanes()).map(|_| AtomicU64::new(0)).collect() } }
        #[inline] pub fn inc_by(&self, v: u64) { let lane = cpu_lane(); self.lanes[lane].fetch_add(v, Relaxed); }
        pub fn swap_all(&self) -> u64 { let mut sum=0; for a in &self.lanes { sum = sum.wrapping_add(a.swap(0, Relaxed)); } sum }
    }
    pub static VERIFIED_OK: Lazy<Sharded> = Lazy::new(Sharded::new);
    pub static VERIFIED_FAIL: Lazy<Sharded> = Lazy::new(Sharded::new);
    pub fn drain_to_prom() {
        let ok = VERIFIED_OK.swap_all(); if ok > 0 { PROM_OK.inc_by(ok); }
        let fl = VERIFIED_FAIL.swap_all(); if fl > 0 { PROM_FAIL.inc_by(fl); }
    }
}

#[cfg(feature = "advanced_sched")]
impl Ed25519BatchVerifier {
    #[inline]
    fn dedupe_batch<'a>(&self, batch: &'a [ShardItem]) -> (Vec<ShardItem>, Vec<usize>) {
        use ahash::AHashMap;
        let mut map: AHashMap<u64, usize> = AHashMap::with_capacity(batch.len());
        let mut unique: Vec<ShardItem> = Vec::with_capacity(batch.len());
        let mut remap: Vec<usize> = Vec::with_capacity(batch.len());
        for it in batch.iter() {
            let idx = *map.entry(it.cache_key).or_insert_with(|| {
                let k = unique.len();
                unique.push(Arc::clone(it));
                k
            });
            remap.push(idx);
        }
        (unique, remap)
    }
}
        })
    }

    #[inline]
    fn choose_core(cid: usize, all: &[CoreId], coreset: Option<&[usize]>) -> CoreId {
        if let Some(cs) = coreset {
            let c = cs[cid % cs.len()];
            all.get(c).cloned().unwrap_or_else(|| all[cid % all.len()])
        } else {
            all[cid % all.len()]
        }
    }

    if let Some(all) = core_affinity::get_core_ids() {
        let coreset = parse_coreset();
        let picked = choose_core(core_id, &all, coreset.as_deref());
        let _ = core_affinity::set_for_current(picked);

        // Parse RT priority from environment (0 = no RT scheduling)
        let rt_prio = std::env::var("QAB_RT_PRIORITY")
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        
        // Set real-time scheduling if requested
        if rt_prio > 0 {
            if let Err(e) = set_realtime_priority(rt_prio) {
                eprintln!("WARN: {}", e);
            }
        }
        
        // Bind to NUMA node if possible
        if let Some(node) = numa_node_for_core(picked.id) {
            bind_node(node);
        }

        #[allow(unused)]
        fn bind_node(cpu_index: usize) {
            #[cfg(feature = "numa")]
            {
                use numa::{bind, Node};
                let node = Node::of_cpu(cpu_index).unwrap_or(0);
                let _ = bind(Node::new(node));
            }
        }
        bind_node(picked.id);

        #[cfg(feature = "rt_prio")]
        unsafe {
            use libc::{pthread_self, pthread_setschedparam, sched_param, SCHED_FIFO};
            if let Ok(p) = std::env::var("QAB_RT_PRIO") {
                if let Ok(prio) = p.parse::<i32>() {
                    let th = pthread_self();
                    let sp = sched_param { sched_priority: prio.clamp(1, 99) };
                    let _rc = pthread_setschedparam(th, SCHED_FIFO, &sp);
                }
            }
        }
    }
}

static FASTLANE_PROFIT: Lazy<i64> = Lazy::new(|| {
    std::env::var("QAB_FASTLANE_PROFIT_LAMPORTS")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(50_000_000)
});

static SAMPLE_MASK: Lazy<u32> = Lazy::new(|| {
    std::env::var("QAB_METRIC_SAMPLE_MASK").ok().and_then(|s| s.parse::<u32>().ok()).unwrap_or(7)
});
#[inline(always)]
fn sample_pow2() -> bool { (fastrand::u32(..) & *SAMPLE_MASK) == 0 }

// Enable DAZ/FTZ fast FP modes on supported targets
#[inline(always)]
fn enable_fast_fp() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::{_mm_getcsr, _mm_setcsr};
        let mut mx = _mm_getcsr();
        // DAZ (bit6), FTZ (bit15)
        mx |= 1 << 6;
        mx |= 1 << 15;
        _mm_setcsr(mx);
        core::arch::x86_64::_mm_lfence();
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        // FPCR FZ (bit 24) and FZ16 (bit 26)
        let mut fpcr: u64;
        core::arch::asm!("mrs {0}, fpcr", out(reg) fpcr);
        fpcr |= (1u64 << 24) | (1u64 << 26);
        core::arch::asm!("msr fpcr, {0}", in(reg) fpcr);
        core::arch::asm!("isb");
    }
}

// Parse a usize from environment (used for scheduler core pin)
fn parse_usize_env(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|s| s.parse::<usize>().ok())
}

// ========================= Advanced sync core with NUMA work-stealing =========================
#[cfg(feature = "advanced_sched")]
type ShardItem = Arc<ParsedItem>;
#[cfg(feature = "advanced_sched")]
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "advanced_sched")]
static STEAL_ROT: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "advanced_sched")]
use crossbeam_utils::sync::{Parker, Unparker};

// Token-based wakeup guard to prevent lost wake-ups
#[cfg(feature = "advanced_sched")]
struct WakeTokenGuard {
    token: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "advanced_sched")]
impl WakeTokenGuard {
    fn new() -> Self {
        Self {
            token: std::sync::atomic::AtomicBool::new(false),
        }
    }

    // Prepare to park, returning true if we should actually park
    fn prepare_park(&self) -> bool {
        // Set token to false (consume any pending wakeup)
        !self.token.swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    // Wake up the waiter if it's parked
    fn unpark(&self) {
        // Set token to true (indicate wakeup)
        if !self.token.swap(true, std::sync::atomic::Ordering::Release) {
            // If we transitioned from false to true, we need to wake
            // The actual unpark will be handled by the parker
        }
    }

    // Park with a timeout, using the token to avoid lost wakeups
    fn park_timeout(&self, parker: &Parker, timeout: Duration) -> bool {
        if self.prepare_park() {
            // Double-check if we got a wakeup between prepare_park and park
            if !self.token.load(std::sync::atomic::Ordering::Acquire) {
                parker.park_timeout(timeout);
            }
            // Consume the token
            self.token.swap(false, std::sync::atomic::Ordering::Release);
            true
        } else {
            false
        }
    }
}
#[cfg(feature = "advanced_sched")]
use crossbeam_utils::CachePadded;
#[cfg(feature = "advanced_sched")]
use crossbeam_queue::ArrayQueue;
#[cfg(feature = "advanced_sched")]
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering as AOrd};

#[cfg(feature = "advanced_sched")]
struct SchedulerSignals { sched_parker: Parker, sched_unparker: Unparker, worker_unparkers: Vec<std::sync::Arc<Unparker>> }
#[cfg(feature = "advanced_sched")]
impl SchedulerSignals {
    fn new(num_workers: usize) -> Self {
        let p = Parker::new();
        let u = p.unparker().clone();
        let mut ups = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let wp = Parker::new();
            ups.push(std::sync::Arc::new(wp.unparker().clone()));
        }
        Self { sched_parker: p, sched_unparker: u, worker_unparkers: ups }
    }
}

#[cfg(feature = "advanced_sched")]
struct PriorityShardsParsed { shards: Vec<Injector<ShardItem>>, notify: std::sync::Arc<Unparker> }
#[cfg(feature = "advanced_sched")]
impl PriorityShardsParsed {
    fn new(notify: std::sync::Arc<Unparker>) -> Self {
        let mut shards = Vec::with_capacity(PRIORITY_LEVELS);
        for _ in 0..PRIORITY_LEVELS { shards.push(Injector::new()); }
        Self { shards, notify, need_wake: AtomicU8::new(0) }
    }
    #[inline]
    fn push(&self, item: ShardItem) {
        self.shards[item.priority as usize].push(item);
        if self.need_wake.swap(1, AOrd::Release) == 0 { self.notify.unpark(); }
    }
    fn drain_ordered(&self, target: usize, out: &mut Vec<ShardItem>) {
        out.clear();
        for pr in (0..PRIORITY_LEVELS).rev() {
            while out.len() < target {
                match self.shards[pr].steal() {
                    Steal::Success(it) => out.push(it),
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }
            if out.len() >= target { break; }
        }
    }
}

#[cfg(feature = "advanced_sched")]
pub struct Ed25519BatchVerifier {
    shutdown: Arc<AtomicBool>,
    inflight: Arc<AtomicU64>,
    // scheduling
    priority_inbox: Arc<PriorityShardsParsed>,
    injector: Arc<Injector<ShardItem>>,
    workers: Vec<Worker<ShardItem>>,
    stealers: Vec<Stealer<ShardItem>>,
    // rotating steal order per worker to avoid bias
    steal_order: Vec<Vec<usize>>,
    signals: Arc<SchedulerSignals>,
    // safe scheduler->worker handoff
    inbox: Vec<Arc<InboxQ<ShardItem>>>,
    inbox_wake: Vec<CachePadded<AtomicU8>>, // 0 -> 1 transition for wake coalescing
    // per-worker outstanding items (approximate)
    backlog: Vec<CachePadded<AtomicU64>>,
    // EWMA ns per item (dispatch cost predictor)
    svc_ns: Vec<CachePadded<AtomicU64>>,
    // cache + admission
    cache: TinyLfuCache<u64, bool>,
    admit: Arc<Admittance>,
    // metrics + tuning
    metrics: Arc<Metrics>,
    pid: Mutex<Pid>,
    target_latency_us: u64,
    batch_size: Mutex<usize>,
    // slot-phase latency modulator
    phase: Phase,
    // L3 HOT/COLD rings (enabled with feature "numa_hotcold")
    #[cfg(feature = "numa_hotcold")]
    l3_groups_hc: Vec<l3_hotcold::L3GroupHC>,
    #[cfg(feature = "numa_hotcold")]
    my_group: Vec<usize>,
    // Bitset futex lane per worker (0..31) or 0xFF for fallback
    #[cfg(all(feature = "group_futex_bitset", feature = "numa_hotcold"))]
    group_lane_bit: Vec<u8>,
    // Per-group svc_ns estimate for routing (ns/item)
    #[cfg(all(feature = "route_cost", feature = "numa_hotcold"))]
    group_svc_ns: Vec<std::sync::atomic::AtomicU64>,
    // Tiers groups (HOT0/HOT1) when enabled
    #[cfg(feature = "numa_hotcold_tiers")]
    l3_groups_tier: Vec<l3_tiers::L3GroupTier>,
}

#[cfg(feature = "advanced_sched")]
impl Ed25519BatchVerifier {
    pub fn new(num_workers: usize, target_latency_us: u64, cache_capacity: u64) -> Arc<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let injector = Arc::new(Injector::new());
        let signals = Arc::new(SchedulerSignals::new(num_workers));
        let mut workers = Vec::with_capacity(num_workers);
        let mut stealers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let w = Worker::new_fifo();
            stealers.push(w.stealer());
            workers.push(w);
        }
        // build rotating steal order per worker (exclude self)
        let mut steal_order: Vec<Vec<usize>> = Vec::with_capacity(num_workers);
        for i in 0..num_workers {
            let mut v = Vec::with_capacity(num_workers.saturating_sub(1));
            for off in 1..=num_workers.saturating_sub(1) {
                v.push((i + off) % num_workers);
            }
            steal_order.push(v);
        }
        // inbox capacity: power-of-two multiple of batch size
        fn pow2_at_least(mut x: usize) -> usize { x = x.max(8); x.next_power_of_two() }
        let base_cap = MAX_BATCH_SIZE.saturating_mul(8);
        let inbox_cap = pow2_at_least(base_cap);
        let inbox: Vec<Arc<InboxQ<ShardItem>>> = (0..num_workers)
            .map(|_| Arc::new(InboxQ::with_capacity(inbox_cap)))
            .collect();
        let inbox_wake: Vec<CachePadded<AtomicU8>> = (0..num_workers)
            .map(|_| CachePadded::new(AtomicU8::new(0)))
            .collect();
        let backlog: Vec<CachePadded<AtomicU64>> = (0..num_workers)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect();
        let svc_ns: Vec<CachePadded<AtomicU64>> = (0..num_workers)
            .map(|_| CachePadded::new(AtomicU64::new(50_000))) // ~50µs default per item
            .collect();
        let cache = TinyLfuCache::builder()
            .max_capacity(cache_capacity)
            .time_to_live(Duration::from_secs(60))
            .build();
        // Build L3 HOT/COLD groups if enabled
        #[cfg(feature = "numa_hotcold")]
        fn build_l3_groups_hc(num_workers: usize) -> (Vec<l3_hotcold::L3GroupHC>, Vec<usize>) {
            let gs = l3_hotcold::groups().to_vec();
            let mut map = Vec::with_capacity(num_workers);
            for i in 0..num_workers { let gid = l3_hotcold::cpu_to_group(i); map.push(gid); }
            (gs, map)
        }
        #[cfg(feature = "numa_hotcold")]
        let (l3_groups_hc, my_group) = build_l3_groups_hc(num_workers);
        #[cfg(all(feature = "group_futex_bitset", feature = "numa_hotcold"))]
        let group_lane_bit = group_bitlanes_assign::assign(&my_group, l3_groups_hc.len(), num_workers);
        #[cfg(all(feature = "route_cost", feature = "numa_hotcold"))]
        let group_svc_ns = (0..l3_groups_hc.len()).map(|_| std::sync::atomic::AtomicU64::new(1)).collect();
        #[cfg(feature = "numa_hotcold_tiers")]
        let l3_groups_tier = l3_tiers::groups().to_vec();
        let this = Arc::new(Self {
            shutdown: shutdown.clone(),
            inflight: Arc::new(AtomicU64::new(0)),
            priority_inbox: Arc::new(PriorityShardsParsed::new(Arc::new(signals.sched_unparker.clone()))),
            injector,
            workers,
            stealers,
            steal_order,
            signals: signals.clone(),
            inbox,
            inbox_wake,
            backlog,
            svc_ns,
            cache,
            admit: Arc::new(Admittance::new()),
            metrics: Arc::new(Metrics::new()),
            pid: Mutex::new(Pid::new(0.015, 0.002, 0.010)),
            target_latency_us,
            batch_size: Mutex::new(MIN_BATCH_SIZE),
            phase: Phase::new(),
            #[cfg(feature = "numa_hotcold")]
            l3_groups_hc,
            #[cfg(feature = "numa_hotcold")]
            my_group,
            #[cfg(all(feature = "group_futex_bitset", feature = "numa_hotcold"))]
            group_lane_bit,
            #[cfg(all(feature = "route_cost", feature = "numa_hotcold"))]
            group_svc_ns,
            #[cfg(feature = "numa_hotcold_tiers")]
            l3_groups_tier,
        });
        // Snapshot CPU features and export gauges
        {
            let feats = CpuFeatures::detect();
            register_int_gauge!("ed25519_cpu_cores", "CPU cores detected").unwrap().set(feats.cores as i64);
            register_int_gauge!("ed25519_cpu_avx2", "Host AVX2 available").unwrap().set(feats.avx2 as i64);
            register_int_gauge!("ed25519_cpu_avx512", "Host AVX512F available").unwrap().set(feats.avx512 as i64);
            register_int_gauge!("ed25519_cpu_neon", "Host NEON available").unwrap().set(feats.neon as i64);
        }
        // Spawn worker threads (pinned + optional RT); each with its own Parker
for i in 0..num_workers {
    let s = Arc::clone(&this);
    let wu = s.signals.worker_unparkers[i].clone();
    std::thread::Builder::new()
        .name("ed25519-worker-".into() + &i.to_string())
        .spawn(move || {
            enable_fast_fp();
            pin_to_core_and_node(i);
            fast_getcpu::tls_cpu_refresh();
            let worker_parker = Parker::new();
            s.worker_loop(i, worker_parker, wu);
        })
        .expect("spawn worker");
}


if t1 <= t2 { k1 } else { k2 }
}

// ========================= PATCH 60: p2c_pick adaptive 2-of-K chooser =========================
#[inline(always)]
fn p2c_pick(&self, cache_key: u64, profit: u64) -> usize {
    use core::sync::atomic::Ordering::Relaxed;

    #[inline(always)]
    fn fast_range_u64(x: u64, n: usize) -> usize {
        ((u128::from(x) * u128::from(n as u64)) >> 64) as usize
    }

    #[inline]
    fn cand_cost(me: &Ed25519BatchVerifier, wid: usize, profit: u64) -> i128 {
        let backlog = me.backlog[wid].load(Relaxed);
        let svc_ns  = me.svc_ns[wid].load(Relaxed).max(1);
        let wait_ns = (backlog.saturating_mul(svc_ns)) as u64;

        #[cfg(all(feature="numa_hotcold", feature="l3_cwnd"))]
        let cwnd_head = {
            let gid = me.my_group[wid];
            let g = &me.l3_groups_hc[gid];
            let inflight = g.load.load(Relaxed) as u32;
            g.cwnd.headroom(inflight)
        };
        #[cfg(not(all(feature="numa_hotcold", feature="l3_cwnd")))]
        let cwnd_head = 0;

        #[cfg(feature="route_cost_scurve")]
        let profit_credit = crate::route_cost_scurve::profit_ns_credit_scurve(
            profit,
            std::env::var("QAB_PROFIT_NS_PER_LAMPORT").ok().and_then(|s| s.parse().ok()).unwrap_or(25)
        );
        #[cfg(not(feature="route_cost_scurve"))]
        let profit_credit: u64 = profit;

        crate::route_cost::price(wait_ns, cwnd_head, profit_credit)
    }

    let n = self.inbox.len();
    if n <= 1 { return 0; }

    // Adaptive K selection based on system load and performance
    let k = {
        // Get system load metrics
        let total_load: u64 = self.backlog.iter().map(|b| b.load(Relaxed)).sum();
        let avg_load = total_load / n as u64;
        let max_load = self.backlog.iter().map(|b| b.load(Relaxed)).max().unwrap_or(1);
        let load_imbalance = max_load.saturating_sub(avg_load).saturating_mul(100) / avg_load.max(1);
        
        // Base K on load characteristics (4-16 candidates)
        let base_k = if load_imbalance > 50 {
            // High imbalance → more candidates
            8 + (load_imbalance.min(200) / 25) as usize
        } else {
            // Low imbalance → fewer candidates
            4 + (load_imbalance / 10) as usize
        };
        
        // Adjust K based on profit (higher profit → more candidates)
        let profit_scale = (profit as f64).log2().max(1.0) / 10.0;
        let scaled_k = (base_k as f64 * profit_scale).round() as usize;
        
        // Clamp to reasonable range
        (scaled_k).clamp(4, 16).min(n)
    };

    // Generate K unique candidates using hash mixing
    let keyb = cache_key.to_le_bytes();
    let mut hashes = Vec::with_capacity(k);
    let h0 = xxhash_rust::xxh3::xxh3_64(&keyb);
    hashes.push(h0);
    
    // Generate additional hashes by mixing with previous hashes and profit
    for i in 1..k {
        let mut mix = [0u8; 16];
        mix[..8].copy_from_slice(&hashes[i-1].to_le_bytes());
        mix[8..].copy_from_slice(&profit.to_le_bytes());
        hashes.push(xxhash_rust::xxh3::xxh3_64(&mix));
    }
    
    // Map hashes to worker indices and ensure uniqueness
    let mut ks = Vec::with_capacity(k);
    for &h in &hashes {
        let mut idx = fast_range_u64(h, n);
        // Ensure uniqueness with linear probing
        while ks.contains(&idx) {
            idx = (idx + 1) % n;
        }
        ks.push(idx);
    }

    // Evaluate costs for all candidates
    let mut cand: Vec<_> = ks.into_iter()
        .map(|wid| (cand_cost(self, wid, profit), wid))
        .collect();
    
    // Sort by cost (ascending)
    cand.sort_unstable_by_key(|&(cost, _)| cost);
    
    // Get the two best candidates
    let (c1, a) = cand[0];
    let (c2, b) = cand[1];

    // Adaptive tie-breaking with dynamic epsilon band
    let eps_bp = if k > 8 { 100 } else { 200 }; // Tighter band with more candidates
    let base = core::cmp::min(core::cmp::abs(c1), core::cmp::abs(c2)).max(1);
    let eps = (base * eps_bp as i128) / 10_000;
    
    if (c1 - c2).abs() <= eps {
        // Affinity by key (reuse the same hash as before for consistency)
        let affinity = fast_range_u64(xxhash_rust::xxh3::xxh3_64(&keyb), n);
        if a == affinity && b != affinity { return a; }
        if b == affinity && a != affinity { return b; }

        // Prefer lane with service time below cluster average
        let lanes = self.svc_ns.len().max(1);
        let sum_ns: u64 = self.svc_ns.iter().map(|a| a.load(Relaxed)).sum::<u64>().max(1);
        let avg = sum_ns / lanes as u64;
        let s1 = self.svc_ns[a].load(Relaxed);
        let s2 = self.svc_ns[b].load(Relaxed);
        if (s1 <= avg) != (s2 <= avg) { 
            return if s1 <= avg { a } else { b }; 
        }

        // Fallback to low-discrepancy tie rotation (deterministic)
        thread_local! { 
            static TIE_ROT: std::cell::Cell<u32> = std::cell::Cell::new(0); 
        }
        let rot = TIE_ROT.with(|t| { 
            let v = t.get(); 
            t.set(v.wrapping_add(1)); 
            v.reverse_bits() 
        });
        return if (rot & 1) == 0 { a } else { b };
    }

    // Return the better candidate based on cost
    if c1 <= c2 { a } else { b }
}

fn dispatch_p2c(&self, item: ShardItem) {
    let pick = self.p2c_pick(item.cache_key, item.profit);
    if self.inbox[pick].push(Arc::clone(&item)).is_ok() {
        self.backlog[pick].fetch_add(1, AOrd::Relaxed);
        if self.inbox_wake[pick].swap(1, AOrd::Release) == 0 {
            if let Some(u) = self.signals.worker_unparkers.get(pick) { u.unpark(); }
        }
    } else {
        // Overflow path
        #[cfg(feature = "numa_hotcold")]
        { self.overflow_enqueue(item, pick); }
        #[cfg(not(feature = "numa_hotcold"))]
        { self.injector.push(item); }
    }
}

// ...

fn worker_loop(&self, worker_id: usize, parker: Parker, unparker: std::sync::Arc<Unparker>) {
    // Set CPU affinity for this worker thread
    #[cfg(target_os = "linux")]
    {
        thread_local! { 
            static AFF_SET: std::cell::Cell<bool> = std::cell::Cell::new(false); 
        }
        AFF_SET.with(|f| if !f.get() { 
            // Use the new maybe_pin_cpuset function from this module
            maybe_pin_cpuset(worker_id);
            // Also set real-time scheduling if configured
            maybe_set_fifo();
            f.set(true); 
        });
    }

    // Create a wake token guard for this worker
    let wake_guard = WakeTokenGuard::new();
    let local = &self.workers[worker_id];
    let stealers = &self.stealers;
    let mut batch: Vec<ShardItem> = Vec::with_capacity(MAX_BATCH_SIZE);
    let ord = &self.steal_order[worker_id];
    loop {
        if self.shutdown.load(Ordering::Acquire) { break; }
        batch.clear();
        let mut target = self.cur_batch_size();
        #[cfg(feature = "cu_budget")]
        { target = self.clamp_target_by_credit(target); }
        // 1) drain inbox first
        let mut from_inbox: usize = 0;
        while batch.len() < target {
            if let Some(it) = self.inbox[worker_id].pop() { batch.push(it); from_inbox += 1; continue; }
            break;
        }
        if self.inbox[worker_id].is_empty() { 
            self.inbox_wake[worker_id].store(0, AOrd::Release); 
            
            // PATCH 22W: Wake multiple workers after bulk drain if needed
            #[cfg(all(target_os = "linux", feature = "group_futex_bitset", feature = "numa_hotcold"))]
            {
                let gid = self.my_group[worker_id];
                let g = &self.l3_groups_hc[gid];
                // Wake up to 4 idle workers if we processed a significant batch
                if from_inbox >= 4 {
                    wake_many(&g.futex_seq, &g.idle_mask, 4);
                }
            }
        }
        // 2) local deque
        while batch.len() < target { if let Some(it) = local.pop() { batch.push(it); continue; } break; }
        // 3) injector with bulk wake support
        let mut bulk_count = 0;
        while batch.len() < target {
            match self.injector.steal() {
                Steal::Success(it) => { 
                    batch.push(it); 
                    bulk_count += 1;
                    // PATCH 22W: Wake multiple workers after bulk steal
                    if bulk_count % 8 == 0 { // Wake others every 8 items
                        #[cfg(all(target_os = "linux", feature = "group_futex_bitset", feature = "numa_hotcold"))]
                        {
                            let gid = self.my_group[worker_id];
                            let g = &self.l3_groups_hc[gid];
                            wake_many(&g.futex_seq, &g.idle_mask, 2); // Wake up to 2 workers
                        }
                    }
                }
                Steal::Retry => continue,
                Steal::Empty => break,
            }
        }
        // 3b) L3 ring before any cold-steal (HOT/COLD with promotion)
        #[cfg(feature = "numa_hotcold")]
        {
            // NUMA drain tick promotion: keep HOT ring fresh before draining
            if batch.len() < target {
                let gid = self.my_group[worker_id];
                let g = &self.l3_groups_hc[gid];
                // Only promote if we have room in the HOT ring
                if g.hot_load.load(Ordering::Relaxed) < g.hot.capacity() {
                    let _ = l3_hotcold_promotion::promote_bounded(g);
                }
            }
            self.try_drain_l3(worker_id, &mut batch, target);
            
            // Periodically run maintenance tasks
        #[cfg(feature = "advanced_sched")]
        {
            static TICK: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let tick = TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            
            // Run VK cache GC every 16K ticks (~2.6s at 6.1us/tick)
            if tick & 0x3FFF == 0 {
                vk_cache_gc_tick();
            }
            
            // Run HOT maintenance every 4K ticks (~25ms at 6.1us/tick)
            if tick & 0xFFF == 0 {
                let gid = self.my_group[worker_id];
                let g = &self.l3_groups_hc[gid];
                hot_maint_prune_stale(g);
            }
        }          }
        }
        // 4) hybrid spin, then NUMA-aware steal
        if batch.len() < target {
            const SPINS: usize = 256; let mut stole_any = false;
            for _ in 0..SPINS {
                if let Some(it) = self.inbox[worker_id].pop() { batch.push(it); from_inbox += 1; stole_any = true; break; }
                std::hint::spin_loop();
            }
            if !stole_any {
                // Cold-steal round across L3 groups preferring HOT-heavy groups
                #[cfg(feature = "numa_hotcold")]
                {
                    if batch.len() < target {
                        let _ = self.cold_steal_round(worker_id, &self.steal_order[worker_id], &mut batch, target);
                    }
                }
                let start = STEAL_ROT.fetch_add(1, AOrd::Relaxed);
                for t in 0..ord.len() {
                    let j = ord[(start + t) % ord.len()];
                    match self.stealers[j].steal() {
                        Steal::Success(it) => { batch.push(it); stole_any = true; if batch.len() >= target { break; } }
                        Steal::Retry => continue,
                        Steal::Empty => {}
                    }
                }
                if !stole_any && batch.is_empty() {
                    // Futex-based group park if enabled, else fallback to parker
                    #[cfg(all(feature = "group_futex_bitset", feature = "numa_hotcold"))]
                    {
                        let gid = self.my_group[worker_id];
                        let g = &self.l3_groups_hc[gid];
                        let bit = self.group_lane_bit[worker_id];
                        if bit != 0xFF {
                            group_futex_bitset::set_idle(&g.idle_mask, bit);
                            group_futex_bitset::park(&g.futex_seq, &g.idle_mask, bit, 50);
                            group_futex_bitset::clear_idle(&g.idle_mask, bit);
                            continue;
                        }
                    }
                    #[cfg(all(feature = "numa_hotcold", feature = "group_futex"))]
                    {
                        let gid = self.my_group[worker_id];
                        let g = &self.l3_groups_hc[gid];
                        // Use the wake guard for futex-based parking as well
                        if wake_guard.prepare_park() {
                            group_futex::park(&g.futex_seq, 50);
                            wake_guard.token.store(false, std::sync::atomic::Ordering::Release);
                        }
                        continue;
                    }
                    #[cfg(not(all(feature = "numa_hotcold", feature = "group_futex")))]
                    { 
                        wake_guard.park_timeout(&parker, Duration::from_micros(50));
                        continue; 
                    }
                }
            }
        }
        if batch.is_empty() { 
            // ========================= PATCH 52: Coalesced worker heartbeat =========================
            // Only run maintenance on one lane per NUMA node to reduce contention
            static MAINT_TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let maint_tick = MAINT_TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            
            // Each NUMA node designates one worker to handle maintenance
            let is_maint_worker = worker_id % 16 == (maint_tick as usize % 16);
            
            if is_maint_worker {
                // Run VK cache GC every 16K ticks (~2.6s at 6.1us/tick)
                if maint_tick % 16_384 == 0 {
                    vk_cache_gc_tick();
                }
                
                // Run HOT maintenance every 4K ticks (~25ms at 6.1us/tick)
                if maint_tick % 4_096 == 0 {
                    #[cfg(feature = "numa_hotcold")]
                    {
                        let gid = self.my_group[worker_id];
                        let g = &self.l3_groups_hc[gid];
                        hot_maint_prune_stale(g);
                    }
                }
                
                // Resync clock every 1M ticks (~6.5s at 6.1us/tick)
                if maint_tick % 1_048_576 == 0 {
                    cycles_resync_tick();
                }
            }
            
            // Backoff to avoid tight spinning
            std::thread::yield_now();
            continue; 
        }

        // queue-wait metrics using serialized TSC
        let now_cyc = now_cycles();
        for it in &batch { if sample_pow2() { self.metrics.queue_wait_us.observe(it.age_us_cyc(now_cyc) as f64); } }
        // de-duplicate within-batch before verification
        let (uniq_batch, remap) = self.dedupe_batch(&batch);
        
        // PATCH 46: Preempt-aware service time measurement with fenced cycles
        // Start/end stamps with OoO fencing
        let c_start = now_cycles_fenced();
        let uniq_results = Self::verify_batch_dalek(&uniq_batch);
        let c_end = now_cycles_fenced();
        let dcy = c_end.wrapping_sub(c_start);
        let dns = cycles_to_ns(dcy).max(1);
        let done = batch.len().max(1) as u64;
        let per_item = (dns / done).max(1);
        
        // Preemption/migration guard (best-effort)
        #[inline]
        fn cur_cpu() -> i32 {
            #[cfg(all(target_os="linux", target_arch="x86_64"))] { 
                unsafe { libc::sched_getcpu() } 
            }
            #[cfg(not(all(target_os="linux", target_arch="x86_64")))] { -1 }
        }
        
        static mut LAST_CPU: i32 = -1;
        let this_cpu = cur_cpu();
        let migrated = unsafe {
            let prev = LAST_CPU;
            LAST_CPU = this_cpu;
            prev >= 0 && prev != this_cpu
        };

        // Jitter guard vs previous lane svc_ns
        use core::sync::atomic::Ordering::Relaxed;
        let prev = self.svc_ns[worker_id].load(Relaxed).max(1);
        let hard_hi = prev.saturating_mul(8);   // reject >8x spikes
        let hard_lo = prev / 8;                 // reject <1/8 dips (timer glitches)
        let accept = !migrated && per_item >= hard_lo && per_item <= hard_hi;

        // Update lane svc_ns with asymmetric smoothing; feed global svc_obs iff accepted
        if accept {
            let next = if per_item >= prev {
                // slow grow toward worse (25%)
                (((prev as u128 * 3) + per_item as u128 + 3) >> 2) as u64
            } else {
                // fast cut toward better (50%)
                (((prev as u128) + per_item as u128) >> 1) as u64
            };
            self.svc_ns[worker_id].store(next.max(1), Relaxed);
            svc_obs::update_sample(per_item);
        } else {
            // soft decay toward better when rejecting
            let next = (((prev as u128 * 7) + prev as u128) >> 3) as u64;
            self.svc_ns[worker_id].store(next.max(1), Relaxed);
        }
        
        // fan-out results to original order
        let mut results: Vec<Result<bool, BatchVerifierError>> = Vec::with_capacity(batch.len());
        for &u in remap.iter() { results.push(uniq_results[u].clone()); }

        // per-CPU counters: aggregate ok/fail
        let mut okc: u64 = 0; let mut flc: u64 = 0;
        for r in &results { match r { Ok(true) => okc+=1, _ => flc+=1 } }
        if okc>0 { per_cpu_counters::VERIFIED_OK.inc_by(okc); }
        if flc>0 { per_cpu_counters::VERIFIED_FAIL.inc_by(flc); }

        let processed = results.len() as u64;
        self.inflight.fetch_sub(processed, AOrd::Relaxed);
        if from_inbox > 0 { self.backlog[worker_id].fetch_sub(from_inbox as u64, AOrd::Relaxed); }
    }
}

// ===== CU-budget clamp wiring =====
#[cfg(all(feature="advanced_sched"))]
impl Ed25519BatchVerifier {
    #[inline]
    fn ceil_pow2_u32(mut v: u32) -> u32 { v -= 1; v |= v>>1; v |= v>>2; v |= v>>4; v |= v>>8; v |= v>>16; v + 1 }
    #[inline]
    fn floor_pow2_u32(v: u32) -> u32 { 1u32 << (31 - v.leading_zeros()) }
    
    // ========================= PATCH 51: cur_batch_size with global svc guardrail =========================
    #[inline]
    fn cur_batch_size(&self) -> usize {
        use core::sync::atomic::Ordering::Relaxed;
        
        // Live svc stats across lanes
        let lanes = self.svc_ns.len().max(1);
        let mut sum: u64 = 0;
        let mut mx:  u64 = 1;
        for a in &self.svc_ns {
            let v = a.load(Relaxed).max(1);
            sum = sum.saturating_add(v);
            if v > mx { mx = v; }
        }
        let avg = (sum / lanes as u64).max(1);

        // Effective svc: blend toward max to hedge tails (25% weight on max)
        let mut eff_svc = avg.saturating_add((mx.saturating_sub(avg)) / 4).max(1);
        
        // Global guardrail: don't be too optimistic if global svc is worse
        #[cfg(feature = "svc_obs")] {
            let global_est = crate::svc_obs::est_ns();
            if global_est > eff_svc {
                // Blend 25% toward global estimate when it's worse
                eff_svc = eff_svc.saturating_add(
                    (global_est.saturating_sub(eff_svc) + 3) / 4
                );
            }
        }

        // Raw target to meet latency bound (with 10% headroom for safety)
        let raw_want = ((self.target_latency_us as u64 * 900) / eff_svc.max(1))
            .clamp(MIN_BATCH_SIZE as u64, MAX_BATCH_SIZE as u64) as usize;

        // Asymmetric smoothing: fast to cut, slow to grow
        let mut bs = self.batch_size.lock();
        let prev = (*bs).clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);
        
        let next = if raw_want < prev {
            // Fast cut: 50% toward target when reducing
            (prev + raw_want) / 2
        } else {
            // Slow growth: 12.5% toward target when increasing
            ((prev as u64 * 7 + raw_want as u64 + 7) >> 3) as usize
        };

        // Snap to nearest Po2
        let lo = floor_pow2_u32(next as u32).max(MIN_BATCH_SIZE as u32);
        let hi = ceil_pow2_u32(next as u32).min(MAX_BATCH_SIZE as u32);
        let next = if (hi - next as u32) < (next as u32 - lo) { 
            hi as usize 
        } else { 
            lo as usize 
        };

        // Enforce min/max bounds
        let next = next.clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);
        *bs = next;
        next
        let want = ((self.target_latency_us as u64 * 1_000) / svc_ns_avg)
            .clamp(MIN_BATCH_SIZE as u64, MAX_BATCH_SIZE as u64) as usize;

        // Smooth towards 'want' (25% step) to avoid oscillation
        let mut bs = self.batch_size.lock();
        let prev = *bs;
        let next = (((prev as u64 * 3) + want as u64 + 3) >> 2) as usize;
        
        // PATCH 23: Snap to nearest power of two for better alloc/SIMD behavior
        let next = if next <= 1 {
            next // 0 and 1 are already powers of two
        } else {
            let lo = self.floor_pow2_u32(next as u32);
            let hi = self.ceil_pow2_u32(next as u32);
            
            // Choose the nearest power of two, with preference for lower on tie
            if next - lo as usize <= hi as usize - next {
                lo as usize
            } else {
                hi as usize
            }
        }.clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);
        
        *bs = next;
        next
    }

    #[inline]
    fn cold_steal_round(
        &self,
        worker_id: usize,
        ord: &[usize],
        out: &mut Vec<ShardItem>,
        target: usize
    ) -> usize {
        use core::sync::atomic::Ordering::Relaxed;
        let need0 = target.saturating_sub(out.len());
        if need0 == 0 { return 0; }

        // Rank groups by route_cost::price; prefer own group first
        let mut cand: smallvec::SmallVec<[(i128, usize); 32]> = smallvec::SmallVec::new();
        let myg = self.my_group[worker_id];

        for (gid, g) in self.l3_groups_hc.iter().enumerate() {
            let inflight = g.load.load(Relaxed) as u32;
            #[cfg(feature="l3_cwnd")]
            let head = g.cwnd.headroom(inflight);
            #[cfg(not(feature="l3_cwnd"))]
            let head = 0;

            #[cfg(all(feature="route_cost"))]
            let svc = self.group_svc_ns.get(gid).map(|a| a.load(Relaxed)).unwrap_or(1);
            #[cfg(not(all(feature="route_cost")))]
            let svc = 1;

            let wait_ns = (g.hot_load.load(Relaxed).saturating_add(g.load.load(Relaxed))) * svc;
            let price = crate::route_cost::price(wait_ns, head, 0);
            // Bias own group slightly
            let price = if gid == myg { price - 64 } else { price };
            cand.push((price, gid));
        }
        cand.sort_unstable_by(|a,b| a.0.cmp(&b.0));

        let mut taken = 0usize;
        for &(_, gid) in cand.iter() {
            let g = &self.l3_groups_hc[gid];

            // JIT promotion (bounded)
            let _ = crate::l3_hotcold_promotion::promote_bounded(g);

            // Drain HOT first
            let before = out.len();
            crate::l3_hotcold::drain_hot(g, out, target);
            let got_hot = out.len() - before;

            if out.len() < target {
                crate::l3_hotcold::drain_cold(g, out, target);
            }
            let got = out.len() - before;
            taken += got;
            if taken >= need0 { break; }
            // If neither hot nor cold yielded, move on quickly
            if got == 0 && got_hot == 0 { continue; }
        }

        // Hard fallback: crossbeam deque stealing if we still need items
        if out.len() < target {
            #[inline]
            fn fast_range_u64(x: u64, n: usize) -> usize {
                ((u128::from(x) * u128::from(n as u64)) >> 64) as usize
            }
            let need = target - out.len();
            let burst = need.min(32);

            // Unbiased rotation using time+worker hash
            let seed = crate::clock::now_cycles().wrapping_mul(0x9E3779B185EBCA87);
            let mut idx = fast_range_u64(seed ^ (worker_id as u64).rotate_left(17), ord.len());

            for _ in 0..ord.len() {
                let j = ord[idx];
                let mut pulled = 0usize;
                // Try to take a small burst from this victim before moving on
                while pulled < burst && out.len() < target {
                    match self.stealers[j].steal() {
                        crossbeam_deque::Steal::Success(it) => { out.push(it); pulled += 1; }
                        crossbeam_deque::Steal::Retry => continue,
                        crossbeam_deque::Steal::Empty => break,
                    }
                }
                if out.len() >= target { break; }
                idx = (idx + 1) % ord.len();
            }
        }
        taken
    }

    #[cfg(all(feature = "ring_meta"))]
    #[inline]
    fn try_drain_l3(&self, worker_id: usize, out: &mut Vec<ShardItem>, target: usize) {
        use core::sync::atomic::Ordering::Relaxed;
        let gid = self.my_group[worker_id];
        let g = &self.l3_groups_hc[gid];

        let mut tmp: smallvec::SmallVec<[(ShardItem, u64); 64]> = smallvec::SmallVec::new();
        let sample = target.saturating_mul(2).min(64);

                (true,  false) => core::cmp::Ordering::Greater,
                (false, true ) => core::cmp::Ordering::Less,
                (true,  true ) => core::cmp::Ordering::Equal,
            }
        });

        // Take the best; requeue any overflow back to HOT (stable)
        for (i, (it, dl)) in tmp.into_iter().enumerate() {
            if out.len() < target {
                out.push(it);
            } else {
                let _ = g.hot.push_with_meta(it, crate::fast_ring_meta::META_F_HOT0 | crate::fast_ring_meta::META_F_DEADLINE, dl);
                g.hot_load.fetch_add(1, Relaxed);
            }
        }
    }
}

#[cfg(all(feature="advanced_sched", feature="cu_budget"))]
impl Ed25519BatchVerifier {
    #[inline]
    fn clamp_target_by_credit(&self, mut target: usize) -> usize {
        let inflight = cu_budget::inflight_allow() as usize;
        let fair = (inflight + self.workers.len() - 1) / self.workers.len();
        if target > fair { target = fair; }
        target.max(MIN_BATCH_SIZE).min(MAX_BATCH_SIZE)
    }
}

// ...

fn verify_batch_dalek(items: &[ShardItem]) -> Vec<Result<bool, BatchVerifierError>> {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey, SignatureError,
        SIGNATURE_LENGTH, PUBLIC_KEY_LENGTH
    };
    use std::{cell::RefCell, thread_local};
    thread_local! {
        static SCRATCH: RefCell<(Vec<usize>, Vec<&'static [u8]>, Vec<Signature>, Vec<VerifyingKey>, Vec<(usize,usize)>)> = RefCell::new((Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()));
    }
    let n = items.len();
    if n == 0 { return vec![]; }
    SCRATCH.with(|rc| {
        let (idx, msgs, sigs, keys, stack) = &mut *rc.borrow_mut();
        idx.clear(); msgs.clear(); sigs.clear(); keys.clear(); stack.clear();
        idx.extend(0..n);
        idx.sort_unstable_by(|&a, &b| items[b].vk_bytes.cmp(&items[a].vk_bytes));
        for &i in idx.iter() {
            let it = &items[i];
            msgs.push(it.msg.as_ref());
            sigs.push(it.sig.clone());
            keys.push(it.vk.clone());
        }
        let mut out = vec![Err(BatchVerifierError::VerificationFailed); n];
        if VerifyingKey::verify_batch(&msgs[..], &sigs[..], &keys[..]).is_ok() {
            for (pos, &orig) in idx.iter().enumerate() { out[orig] = Ok(true); }
            return out;
#[cfg(feature = "advanced_sched")]
pub struct Ed25519BatchVerifierAsync { inner: Arc<Ed25519BatchVerifier> }
#[cfg(feature = "advanced_sched")]
impl Ed25519BatchVerifierAsync {
    pub fn new(inner: Arc<Ed25519BatchVerifier>) -> Self { Self { inner } }
    pub async fn enqueue(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.enqueue(item)).await.map_err(|_| BatchVerifierError::VerifierShutdown)??;
        Ok(())
    }
    pub fn export_metrics(&self) -> String { self.inner.export_metrics() }
    pub fn shutdown(&self) { self.inner.shutdown() }
}

// Failure stats with blacklisting
#[cfg(feature = "advanced_sched")]
struct FailStats { map: parking_lot::Mutex<AHashMap<[u8; PUBLIC_KEY_LENGTH], u32>> }
#[cfg(feature = "advanced_sched")]
impl FailStats {
    fn new() -> Self { Self { map: parking_lot::Mutex::new(AHashMap::new()) } }
    fn note(&self, pk: [u8; PUBLIC_KEY_LENGTH], ok: bool, admit: &Admittance) {
        if ok { return; }
        let mut m = self.map.lock();
        let c = m.entry(pk).or_insert(0);
        *c += 1;
        const FAIL_BLACKLIST_THRESHOLD: u32 = 32;
        if *c >= FAIL_BLACKLIST_THRESHOLD { admit.blacklist(pk); }
    }
}
    }
}
// ========================= PATCH: pin_to_core_and_node (non-Linux fallback) =========================
#[cfg(all(feature = "core_affinity", not(target_os = "linux")))]
fn pin_to_core_and_node(core_id: usize) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    static NEXT_CORE: AtomicUsize = AtomicUsize::new(0);
    
    // Simple round-robin core assignment if no specific core is requested
    let core_id = core_id % num_cpus::get();
    
    if let Some(cores) = core_affinity::get_core_ids() {
        if let Some(&core) = cores.get(core_id % cores.len()) {
            if core_affinity::set_for_current(core) {
                // Successfully set affinity
                return;
            }
        }
        
        // Fallback: round-robin assignment
        let idx = NEXT_CORE.fetch_add(1, Ordering::Relaxed) % cores.len();
        if let Some(&core) = cores.get(idx) {
            let _ = core_affinity::set_for_current(core);
        }
    }
}

#[cfg(all(feature = "core_affinity", not(target_os = "linux")))]
fn parse_env_coreset() -> Option<Vec<usize>> {
    std::env::var("QAB_CPUSET").ok().and_then(|s| {
        let m: Vec<_> = s.split(',').filter_map(|t| usize::from_str(t.trim()).ok()).collect();
        if m.is_empty() { None } else { Some(m) }
    })
}
#[cfg(feature = "advanced_sched")]
impl PriorityShards {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(PRIORITY_LEVELS);
        for _ in 0..PRIORITY_LEVELS { shards.push(Injector::new()); }
        Self { shards }
    }
    #[inline]
    fn push(&self, item: VerificationItem) {
        self.shards[item.priority as usize].push(item);
    }
    // Drain highest priorities first into out up to target
    fn drain_ordered(&self, target: usize, out: &mut Vec<VerificationItem>) {
        out.clear();
        for pr in (0..PRIORITY_LEVELS).rev() {
            while out.len() < target {
                match self.shards[pr].steal() {
                    Steal::Success(it) => out.push(it),
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }
            if out.len() >= target { break; }
        }
    }
}
// Simple per-priority queue with aging info (used in non-advanced scheduler path)
struct PriQueue {
    q: VecDeque<VerificationItem>,
    last_served: Instant,
}

// Admission filter combining a tiny-LFU cache and a Bloom filter to shed obvious spam/dupes
#[cfg(feature = "advanced_sched")]
struct Admittance {
    bloom: Bloom<[u8; 8]>,
    blacklist: RwLock<AHashSet<[u8; PUBLIC_KEY_LENGTH]>>,
    bucket: Mutex<AHashMap<[u8; PUBLIC_KEY_LENGTH], (Instant, u32)>>,
}

#[cfg(feature = "advanced_sched")]
impl Admittance {
    fn new() -> Self {
        let bloom = Bloom::new_for_fp_rate(1_000_000, 0.01);
        Self { bloom, blacklist: RwLock::new(AHashSet::new()), bucket: Mutex::new(AHashMap::new()) }
    }
    #[inline] fn key64(k: u64) -> [u8;8] { k.to_le_bytes() }
    fn seen_maybe(&self, cache_key: u64) -> bool { self.bloom.check(&Self::key64(cache_key)) }
    fn note(&self, cache_key: u64) { self.bloom.set(&Self::key64(cache_key)); }
    fn is_blacklisted(&self, pk: &[u8; PUBLIC_KEY_LENGTH]) -> bool { self.blacklist.read().contains(pk) }
    fn blacklist(&self, pk: [u8; PUBLIC_KEY_LENGTH]) { self.blacklist.write().insert(pk); }
    // simple per-key leaky bucket: max 128 enqueues per 1s window
    fn allow(&self, pk: &[u8; PUBLIC_KEY_LENGTH]) -> bool {
        let mut m = self.bucket.lock();
        let now = Instant::now();
        let entry = m.entry(*pk).or_insert((now, 0u32));
        let (ref mut ts, ref mut cnt) = entry;
        if now.duration_since(*ts).as_millis() > 1000 {
            *ts = now; *cnt = 0;
        }
        if *cnt >= 128 { return false; }
        *cnt += 1; true
    }
}

// ========================= ADDED: fast TL counters =========================
#[cfg(feature = "advanced_sched")]
struct TlCounters { ok: Cell<u64>, fail: Cell<u64> }
#[cfg(feature = "advanced_sched")]
thread_local! { static TL: TlCounters = TlCounters { ok: Cell::new(0), fail: Cell::new(0) }; }
#[cfg(feature = "advanced_sched")]
static PROM_OK: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("ed25519_fast_ok", "Fast-path verified OK").unwrap());
#[cfg(feature = "advanced_sched")]
static PROM_FAIL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("ed25519_fast_fail", "Fast-path failed verifications").unwrap());
#[cfg(feature = "advanced_sched")]
pub struct FastMetrics;
#[cfg(feature = "advanced_sched")]
impl FastMetrics {
    #[inline] pub fn total_verified(&self) { TL.with(|t| t.ok.set(t.ok.get().wrapping_add(1))); }
    #[inline] pub fn failed(&self) { TL.with(|t| t.fail.set(t.fail.get().wrapping_add(1))); }
    #[inline] pub fn flush(&self) {
        TL.with(|t| {
            let ok = t.ok.replace(0);
            let fl = t.fail.replace(0);
            if ok > 0 { PROM_OK.inc_by(ok); }
            if fl > 0 { PROM_FAIL.inc_by(fl); }
        });
    }
}
#[cfg(feature = "advanced_sched")]
pub static FAST_METRICS: FastMetrics = FastMetrics;

// Optional Prometheus metrics bundle (feature-gated)
#[cfg(feature = "advanced_sched")]
struct Metrics {
    total_verified: IntCounter,
    failed_verifications: IntCounter,
    cache_hits: IntCounter,
    cache_misses: IntCounter,
    batches: IntCounter,
    batch_time_us: Histogram,
    verify_time_us: Histogram,
    queue_wait_us: Histogram,
    profit_weighted_latency: Histogram,
}
#[cfg(feature = "advanced_sched")]
impl Metrics {
    fn new() -> Self {
        Self {
            total_verified: register_int_counter!("ed25519_total_verified", "Total successful verifications").unwrap(),
            failed_verifications: register_int_counter!("ed25519_failed", "Failed verifications").unwrap(),
            cache_hits: register_int_counter!("ed25519_cache_hits", "Cache hits").unwrap(),
            cache_misses: register_int_counter!("ed25519_cache_misses", "Cache misses").unwrap(),
            batches: register_int_counter!("ed25519_batches", "Batches processed").unwrap(),
            batch_time_us: register_histogram!("ed25519_batch_time_us", "Batch wall time (us)", vec![50.0, 100.0, 200.0, 400.0, 800.0, 1600.0, 3200.0]).unwrap(),
            verify_time_us: register_histogram!("ed25519_verify_time_us", "Verify time (us)", vec![10.0, 25.0, 50.0, 100.0, 200.0, 400.0]).unwrap(),
            queue_wait_us: register_histogram!("ed25519_queue_wait_us", "Queue wait (us)", vec![10.0, 50.0, 100.0, 200.0, 500.0, 1000.0]).unwrap(),
            profit_weighted_latency: register_histogram!("ed25519_profit_weighted_latency", "Profit-weighted latency (us)", vec![10.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 5000.0]).unwrap(),
        }
    }
}

// PID controller for adaptive batch sizing
struct Pid { kp: f64, ki: f64, kd: f64, integ: f64, prev_err: f64, last: Instant }
impl Pid {
    fn new(kp: f64, ki: f64, kd: f64) -> Self { Self { kp, ki, kd, integ: 0.0, prev_err: 0.0, last: Instant::now() } }
    fn update(&mut self, target_us: f64, actual_us: f64) -> f64 {
        let now = Instant::now();
        let dt = (now - self.last).as_secs_f64().max(1e-6);
        self.last = now;
        let err = target_us - actual_us;
        self.integ = (self.integ + err * dt).clamp(-1e6, 1e6);
        let deriv = (err - self.prev_err) / dt;
        self.prev_err = err;
        self.kp * err + self.ki * self.integ + self.kd * deriv
    }
}
use ed25519_dalek::{
    Signature, Verifier, VerifyingKey, SignatureError,
    SIGNATURE_LENGTH, PUBLIC_KEY_LENGTH
};
 
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering, AtomicI64}};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use thiserror::Error;
use parking_lot::{Mutex, RwLock};
use crossbeam_channel::{bounded, Sender, Receiver};
use crossbeam_queue::ArrayQueue;
use bytes::Bytes;
use arrayvec::ArrayVec;
use std::cell::RefCell;
use std::thread_local as std_thread_local;
use blake3::Hasher;
use smallvec::SmallVec;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use metrics::{counter, histogram};
#[cfg(feature = "jemalloc")]
use tikv_jemallocator::Jemalloc;
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
use rand::random;
#[cfg(feature = "advanced_sched")]
use crossbeam_deque::{Injector, Stealer, Worker, Steal};
#[cfg(feature = "advanced_sched")]
use prometheus::{Encoder, IntCounter, IntGauge, Histogram, TextEncoder, register_int_counter, register_histogram, register_int_gauge};
#[cfg(feature = "advanced_sched")]
use moka::sync::Cache as TinyLfuCache;
#[cfg(feature = "advanced_sched")]
use bloomfilter::Bloom;
#[cfg(feature = "advanced_sched")]
use ahash::{AHashSet, AHashMap};
#[cfg(feature = "advanced_sched")]
use seqlock::SeqLock;
#[cfg(feature = "advanced_sched")]
use std::cell::Cell;
#[cfg(feature = "advanced_sched")]
use std::thread_local;
#[cfg(feature = "advanced_sched")]
use once_cell::sync::Lazy;

const MAX_BATCH_SIZE: usize = 128;
const MIN_BATCH_SIZE: usize = 8;
const VERIFICATION_TIMEOUT_MS: u64 = 50;
const CACHE_SIZE: usize = 65536;
const PARALLEL_THRESHOLD: usize = 16;
const MAX_PENDING_BATCHES: usize = 256;
const RETRY_ATTEMPTS: u8 = 3;
const BACKOFF_BASE_MS: u64 = 1;
const AGE_BOOST_MS: u64 = 5; // aging threshold to prevent starvation
const AVG_MSG_LEN_THRESH: usize = 1024; // bytes; scale down batch size when average exceeds this
static PROFIT_THRESHOLD_LAMPORTS: AtomicI64 = AtomicI64::new(0);
const SIMD_CHUNK: usize = 32; // target chunk for SIMD-friendly batch
#[cfg(feature = "gpu_verify")]
const GPU_THRESHOLD: usize = 96; // speculative threshold for GPU path
const PRIORITY_LEVELS: usize = 256;
const MAX_INFLIGHT_ITEMS: usize = 128 * 1024;

// ========================= PATCH 44: svc_obs - Global per-item service time estimator =========================
pub mod svc_obs {
    use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use once_cell::sync::Lazy;

    // Global robust EWMA of per-item service time (ns)
    static EWMA_NS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1_000)); // sane nonzero seed

    #[inline]
    pub fn est_ns() -> u64 {
        EWMA_NS.load(Relaxed).max(1)
    }

    // Robust update: clamp sample into [ewma/4, ewma*4] then 1/8 smoothing
    #[inline]
    pub fn update_sample(sample_ns: u64) {
        let prev = EWMA_NS.load(Relaxed).max(1);
        let lo = prev / 4;
        let hi = prev.saturating_mul(4);
        let s = sample_ns.clamp(lo.max(1), hi);
        let next = (((prev as u128) * 7) + (s as u128) + 7) >> 3;
        EWMA_NS.store(next as u64, Relaxed);
    }
}

// ========================= Inline TSC clock (cycles -> microseconds) =========================
mod clock {
    use once_cell::sync::Lazy;
    use std::time::{Duration, Instant};

    pub struct TscCal { pub mult: u64, pub shift: u32, pub t0_cyc: u64, pub fallback0: Instant }
    const SHIFT: u32 = 24;

    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    fn rdtsc() -> u64 { unsafe { core::arch::x86_64::_rdtsc() as u64 } }
    #[inline(always)]
    #[cfg(not(target_arch = "x86_64"))]
    fn rdtsc() -> u64 { let now = Instant::now(); now.elapsed().as_nanos() as u64 }

    static CAL: Lazy<TscCal> = Lazy::new(|| {
        #[cfg(target_arch = "x86_64")]
        {
            let t0 = Instant::now();
            let c0 = rdtsc();
            let target = t0 + Duration::from_millis(200);
            while Instant::now() < target { core::hint::spin_loop() }
            let dt = t0.elapsed();
            let c1 = rdtsc();
            let cyc = c1.saturating_sub(c0).max(1);
            let us = dt.as_micros() as u64;
            let mult = (((us as u128) << SHIFT) / (cyc as u128)) as u64;
            TscCal { mult: mult.max(1), shift: SHIFT, t0_cyc: rdtsc(), fallback0: t0 }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            TscCal { mult: 1, shift: 0, t0_cyc: 0, fallback0: Instant::now() }
        }
    });

    #[inline(always)] pub fn now_cycles() -> u64 { rdtsc() }
    
    #[inline(always)] 
    pub fn now_cycles_fenced() -> u64 {
        #[cfg(all(target_os="linux", target_arch="x86_64"))] {
            use once_cell::sync::Lazy;
            static USE_RDTSCP: Lazy<bool> = Lazy::new(|| matches!(std::env::var("QAB_TSC_SERIALIZE").as_deref(), Ok("rdtscp")));
            
            unsafe {
                if *USE_RDTSCP {
                    let mut aux: u32 = 0;
                    // rdtscp is ordered after all prior ops; followed by lfence to order subsequent ops
                    let t = core::arch::x86_64::_rdtscp(&mut aux);
                    core::arch::x86_64::_mm_lfence();
                    t as u64
                } else {
                    core::arch::x86_64::_mm_lfence();
                    let t = core::arch::x86_64::_rdtsc();
                    core::arch::x86_64::_mm_lfence();
                    t as u64
                }
            }
        }
        #[cfg(not(all(target_os="linux", target_arch="x86_64")))] {
            now_cycles()
        }
    }
    
    #[inline(always)] 
    pub fn now_cycles_serialized() -> u64 {
        #[cfg(target_arch = "x86_64")] {
            unsafe {
                core::arch::x86_64::_mm_lfence();
                let mut aux: u32 = 0;
                let t = core::arch::x86_64::_rdtscp(&mut aux);
                core::arch::x86_64::_mm_lfence();
                t as u64
            }
        }
        #[cfg(not(target_arch = "x86_64"))] { now_cycles() }
    }
    #[inline(always)] pub fn cycles_to_us(cyc: u64) -> u64 {
        #[cfg(target_arch = "x86_64")] { (cyc.saturating_mul(CAL.mult)) >> CAL.shift }
        #[cfg(not(target_arch = "x86_64"))] { cyc }
    }
    #[inline(always)] pub fn now_us() -> u64 {
        #[cfg(target_arch = "x86_64")] { cycles_to_us(now_cycles().saturating_sub(CAL.t0_cyc)) }
        #[cfg(not(target_arch = "x86_64"))] { CAL.fallback0.elapsed().as_micros() as u64 }
    }
    #[inline(always)] pub fn tsc_trusted() -> bool {
        #[cfg(target_arch = "x86_64")] { true }
        #[cfg(not(target_arch = "x86_64"))] { false }
    }
}

// Prefetch helpers for message payloads
#[inline(always)]
fn prefetch_t0(ptr: *const u8) {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
    unsafe { core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0); }
    #[cfg(all(target_arch = "aarch64"))]
    unsafe { core::arch::aarch64::__prefetch(ptr as *const _, 0, 3, 1); }
}
#[inline(always)]
fn prefetch_nta(ptr: *const u8) {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
    unsafe { core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_NTA); }
}

// Compose a monotonic priority key from (priority, profit, age, tiebreaker)
#[inline]
fn mk_priority_key(priority: u8, profit: u64, enq_ts: Instant) -> u128 {
    let now = Instant::now();
    let age_us = now.saturating_duration_since(enq_ts).as_micros() as u64;
    let profit_b = (profit.min((1u64 << 56) - 1)) as u128;
    let age_b = (age_us.min((1u64 << 56) - 1)) as u128;
    let pri = priority as u128;
    let tb = ((profit ^ age_us.rotate_left(13)) & 0xff) as u128;
    (pri << 120) | (profit_b << 64) | (age_b << 8) | tb
}

#[derive(Error, Debug, Clone)]
pub enum BatchVerifierError {
    #[error("Invalid signature length: {0}")]
    InvalidSignatureLength(usize),
    
    #[error("Invalid public key length: {0}")]
    InvalidPublicKeyLength(usize),
    
    #[error("Signature verification failed")]
    VerificationFailed,
    
    #[error("Batch timeout exceeded")]
    BatchTimeout,
    
    #[error("Queue full")]
    QueueFull,
    
    #[error("Verifier shutdown")]
    VerifierShutdown,
    
    #[error("Invalid batch size: {0}")]
    InvalidBatchSize(usize),
}

#[derive(Clone, Debug)]
pub struct VerificationItem {
    pub signature: [u8; SIGNATURE_LENGTH],
    pub public_key: [u8; PUBLIC_KEY_LENGTH],
    pub message: Bytes,
    pub priority: u8,
    pub timestamp: Instant,
    pub retry_count: u8,
    pub estimated_profit_lamports: i64,
    pub cache_key: u64,
}

impl VerificationItem {
    pub fn new(
        signature: [u8; SIGNATURE_LENGTH],
        public_key: [u8; PUBLIC_KEY_LENGTH],
        message: Bytes,
        priority: u8,
        timestamp: Instant,
        retry_count: u8,
        estimated_profit_lamports: i64,
    ) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(&signature);
        hasher.update(&public_key);
        hasher.update(&message);
        let cache_key = u64::from_le_bytes(
            hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8; 8])
        );
        Self {
            signature,
            public_key,
            message,
            priority,
            timestamp,
            retry_count,
            estimated_profit_lamports,
            cache_key,
        }
    }
    fn cache_key(&self) -> u64 {
        if self.cache_key != 0 { return self.cache_key; }
        let mut hasher = Hasher::new();
        hasher.update(&self.signature);
        hasher.update(&self.public_key);
        hasher.update(&self.message);
        u64::from_le_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8;8]))
    }
}

// Parsed item to avoid repeated decoding and cloning
pub struct ParsedItem {
    pub sig: Signature,
    pub vk: VerifyingKey,
    pub vk_bytes: [u8; PUBLIC_KEY_LENGTH],
    pub msg: Bytes,
    pub cache_key: u64,
    #[cfg(feature = "advanced_sched")] pub priority: u8,
    #[cfg(feature = "advanced_sched")] pub profit: u64,
    #[cfg(feature = "advanced_sched")] pub enq_ts: Instant,
    #[cfg(feature = "advanced_sched")] pub enq_us: u64,
    #[cfg(feature = "advanced_sched")] pub enq_cyc: u64,
}

impl ParsedItem {
    #[inline(always)]
    pub fn age_us_cyc(&self, now_cyc: u64) -> u64 {
        clock::cycles_to_us(now_cyc.saturating_sub(self.enq_cyc))
    }
    #[inline]
    pub fn age_us(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.enq_ts).as_micros() as u64
    }
}

impl TryFrom<VerificationItem> for ParsedItem {
    type Error = BatchVerifierError;
    fn try_from(v: VerificationItem) -> Result<Self, Self::Error> {
        let sig = Signature::from_bytes(&v.signature)
            .map_err(|_| BatchVerifierError::InvalidSignatureLength(v.signature.len()))?;
        // VK micro-cache (TL) + global fallback
        let pk_arr: [u8; PUBLIC_KEY_LENGTH] = v.public_key;
        let vk = if let Some(x) = vk_fast_lookup(&pk_arr) {
            x
        } else if let Some(cached) = VK_CACHE.read().get(&pk_arr).cloned() {
            vk_fast_insert(pk_arr, cached.clone());
            cached
        } else {
            let parsed = VerifyingKey::from_bytes(&pk_arr)
                .map_err(|_| BatchVerifierError::InvalidPublicKeyLength(pk_arr.len()))?;
            {
                let mut w = VK_CACHE.write();
                w.insert(pk_arr, parsed.clone());
            }
            vk_fast_insert(pk_arr, parsed.clone());
            parsed
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(&v.signature);
        hasher.update(&v.public_key);
        hasher.update(&v.message);
        let cache_key = u64::from_le_bytes(
            hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8;8])
        );
        #[cfg(feature = "advanced_sched")]
        let enq_us = clock::now_us();
        #[cfg(feature = "advanced_sched")]
        let enq_cyc = clock::now_cycles();
        Ok(Self {
            sig,
            vk_bytes: vk.to_bytes(),
            vk,
            msg: v.message,
            cache_key,
            #[cfg(feature = "advanced_sched")] priority: v.priority,
            #[cfg(feature = "advanced_sched")] profit: v.estimated_profit_lamports.max(0) as u64,
            #[cfg(feature = "advanced_sched")] enq_ts: v.timestamp,
            #[cfg(feature = "advanced_sched")] enq_us: enq_us,
            #[cfg(feature = "advanced_sched")] enq_cyc: enq_cyc,
        })
    }
}

// ========================= TL micro-cache for VerifyingKey (32 entries) =========================
std_thread_local! {
    static TL_VK32: RefCell<ArrayVec<([u8; PUBLIC_KEY_LENGTH], VerifyingKey), 32>> = RefCell::new(ArrayVec::new());
}

#[inline]
fn vk_fast_lookup(pk: &[u8; PUBLIC_KEY_LENGTH]) -> Option<VerifyingKey> {
    TL_VK32.with(|tl| {
        let mut v = tl.borrow_mut();
        for i in 0..v.len() {
            if &v[i].0 == pk {
                let hit = v.remove(i);
                v.insert(0, (hit.0, hit.1.clone()));
                return Some(hit.1);
            }
        }
        None
    })
}

#[inline]
fn vk_fast_insert(pk: [u8; PUBLIC_KEY_LENGTH], vk: VerifyingKey) {
    TL_VK32.with(|tl| {
        let mut v = tl.borrow_mut();
        if v.len() == v.capacity() { let _ = v.pop(); }
        v.insert(0, (pk, vk));
    });
}

static VK_CACHE: once_cell::sync::Lazy<RwLock<AHashMap<[u8; PUBLIC_KEY_LENGTH], VerifyingKey>>> = once_cell::sync::Lazy::new(|| RwLock::new(AHashMap::new()));

impl BatchVerifier {
    fn verify_uncached_batch_dalek(items: &[VerificationItem]) -> Vec<Result<bool, BatchVerifierError>> {
        // Pre-parse to avoid repeated decoding and heap churn
        let mut parsed: Vec<ParsedItem> = Vec::with_capacity(items.len());
        for it in items.iter().cloned() {
            match ParsedItem::try_from(it) {
                Ok(p) => parsed.push(p),
                Err(_) => return items.iter().map(|x| Self::verify_item(x)).collect(),
    }

    // Slow path: bisect to identify bads
    fn mark_range(
        let process_chunk = |start: usize, end: usize, out: &mut [Result<bool, BatchVerifierError>]| {
            let len = end - start;
            if len == 0 { return; }
            let m = &msgs_sorted[start..end];
            let s = &sigs_sorted[start..end];
            let k = &keys_sorted[start..end];
            if VerifyingKey::verify_batch(m, s, k).is_ok() {
                for j in start..end { out[idx[j]] = Ok(true); }
            } else {
                // Fallback: single verifies within the chunk
                for j in start..end {
                    let ok = keys_sorted[j].verify(msgs_sorted[j], &sigs_sorted[j]).is_ok();
                    out[idx[j]] = Ok(ok);
                }
            }
        };

        // Parallelize over chunks of SIMD_CHUNK
        let mut pos = 0;
        while pos + SIMD_CHUNK <= n {
            let end = pos + SIMD_CHUNK;
            process_chunk(pos, end, &mut out);
            pos = end;
        }
        // Leftover
        if pos < n { process_chunk(pos, n, &mut out); }
        // Optional GPU offload first
        #[cfg(feature = "gpu_verify")]
        {
            if Self::verify_batch_gpu(&msgs, &sigs, &keys) {
                return vec![Ok(true); n];
            }
        }
        out
    }
}

// ========================= PATCH 22: group_futex_bitset::wake_many (ADD) =========================
#[inline]
pub fn wake_many(seq: &std::sync::atomic::AtomicU32, idle_mask: &std::sync::atomic::AtomicU32, mut k: usize) {
    use std::sync::atomic::Ordering::{Relaxed, Release};
    if k == 0 { return; }
    let mut snap = idle_mask.load(Relaxed);
    if snap == 0 { return; }

    // Round-robin start to keep fairness
    let base = (seq.load(Relaxed) & 31) as u8;
    let mut woke = 0usize;

    for off in 0..32u8 {
        if woke >= k { break; }
        let bit = ((base as u32 + off as u32) & 31) as u8;
        let mask = 1u32 << bit;
        if snap & mask == 0 { continue; }
        // Clear this idle bit optimistically (avoid thundering herd)
        let prev = idle_mask.fetch_and(!mask, Release);
        if prev & mask != 0 {
            let _ = seq.fetch_add(1, Release);
            // futex_wake_bitset(seq, mask);
            unsafe {
                let _ = libc::syscall(
                    libc::SYS_futex,
                    seq as *const _ as *const u32,
                    libc::FUTEX_WAKE_BITSET | libc::FUTEX_PRIVATE_FLAG,
                    1u32,
                    0 as *const libc::timespec,
                    0 as *mut libc::c_void,
                    mask
                );
            }
            woke += 1;
        }
        snap &= !mask;
        if snap == 0 { break; }
    }
}

// ========================= PATCH 37: verify_batch_dalek with iterative bisect and stronger prefetch =========================
fn verify_batch_dalek(items: &[ShardItem]) -> Vec<Result<bool, BatchVerifierError>> {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    use smallvec::SmallVec;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;
    use std::intrinsics::prefetch_read_data;

    // Thread-local storage for verification state to avoid allocations
    thread_local! {
        static TLS: RefCell<(
            SmallVec<[usize; 1024]>,            // Original indices
            SmallVec<[&'static [u8]; 1024]>,    // Message references
            SmallVec<[Signature; 1024]>,        // Signatures
            SmallVec<[VerifyingKey; 1024]>,     // Verification keys
            HashMap<u64, usize>,                // Deduplication map (hash -> index)
            SmallVec<[Option<usize>; 1024]>     // Mapping from original index to dedup index
        )> = RefCell::new(Default::default());
    }

    let n = items.len();
    if n == 0 { return Vec::new(); }

    // Output preset to "failed" (will be overwritten on success)
    let mut out = vec![Err(BatchVerifierError::VerificationFailed); n];

    TLS.with(|cell| {
        let (ref mut idx, ref mut msgs, ref mut sigs, ref mut keys, 
             ref mut dedup, ref mut orig_to_dedup) = *cell.borrow_mut();
        
        // Clear and reserve space
        idx.clear(); msgs.clear(); sigs.clear(); keys.clear();
        dedup.clear(); orig_to_dedup.clear();
        
        // Pre-allocate to avoid reallocation
        idx.reserve(n); 
        msgs.reserve(n); 
        sigs.reserve(n); 
        keys.reserve(n);
        orig_to_dedup.resize(n, None);
        
        // First pass: deduplicate (vk, sig, msg) tuples using a hash map
        let mut hasher = DefaultHasher::new();
        for (i, item) in items.iter().enumerate() {
            // Skip empty messages
            if item.msg.is_empty() { continue; }
            
            // Compute a combined hash of (vk_bytes || sig_bytes || msg)
            hasher.write(&item.vk_bytes);
            hasher.write(&item.sig.to_bytes());
            hasher.write(item.msg.as_ref());
            let hash = hasher.finish();
            
            // Check if we've seen this (vk, sig, msg) before
            match dedup.entry(hash) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    // Duplicate found - just reference the existing entry
                    orig_to_dedup[i] = Some(*entry.get());
                },
                std::collections::hash_map::Entry::Vacant(entry) => {
                    // New unique entry - add to our working sets
                    let new_idx = idx.len();
                    entry.insert(new_idx);
                    orig_to_dedup[i] = Some(new_idx);
                    
                    // SAFETY: We ensure the slice outlives this function call
                    let msg_slice = unsafe { 
                        core::slice::from_raw_parts(item.msg.as_ptr(), item.msg.len()) 
                    };
                    
                    idx.push(i);
                    msgs.push(msg_slice);
                    sigs.push(item.sig.clone());
                    keys.push(item.vk.clone());
                }
            }
            
            hasher = DefaultHasher::new(); // Reset hasher for next iteration
        }
        
        if idx.is_empty() { return; }

        // Sort for locality by vk_bytes (descending): reduces cache thrash in dalek
        {
            let mut indices: Vec<_> = (0..idx.len()).collect();
            indices.sort_unstable_by(|&a, &b| {
                items[idx[b]].vk_bytes.cmp(&items[idx[a]].vk_bytes)
            });
            
            // Apply the permutation to all parallel arrays
            let mut pos = 0;
            while pos < indices.len() {
                let target = indices[pos];
                if target == pos { 
                    pos += 1; 
                    continue; 
                }
                
                // Swap all parallel arrays
                msgs.swap(pos, target);
                sigs.swap(pos, target);
                keys.swap(pos, target);
                idx.swap(pos, target);
                
                // Update the permutation
                for i in pos+1..indices.len() {
                    if indices[i] == pos { 
                        indices[i] = target; 
                        break; 
                    }
                }
                indices[pos] = pos;
                
                pos += 1;
            }
        }

        // Fast path: whole batch passes
        if VerifyingKey::verify_batch(&msgs[..], &sigs[..], &keys[..]).is_ok() {
            // Mark all original items as verified, including duplicates
            for (orig_idx, &dedup_idx_opt) in orig_to_dedup.iter().enumerate() {
                if dedup_idx_opt.is_some() {
                    out[orig_idx] = Ok(true);
                }
            }
            return;
        }

        // Iterative bisect implementation with explicit stack to avoid recursion overhead
        // and prefetching for better cache utilization
        let mut stack = SmallVec::<[(usize, usize, bool); 32]>::new();
        stack.push((0, idx.len(), false));  // (lo, hi, is_legacy_ok)
        
        while let Some((lo, hi, is_legacy_ok)) = stack.pop() {
            if hi <= lo { continue; }
            
            // Prefetch data for the next iteration
            if hi - lo > 1 {
                let mid = lo + ((hi - lo) >> 1);
                unsafe {
                    // Prefetch verification keys, signatures, and messages for both halves
                    prefetch_read_data(keys[lo].as_bytes().as_ptr(), 1);
                    prefetch_read_data(sigs[lo].as_bytes().as_ptr(), 1);
                    if let Some(msg) = msgs.get(lo) { prefetch_read_data(msg.as_ptr(), 1); }
                    
                    if mid < keys.len() {
                        prefetch_read_data(keys[mid].as_bytes().as_ptr(), 1);
                        prefetch_read_data(sigs[mid].as_bytes().as_ptr(), 1);
                        if let Some(msg) = msgs.get(mid) { prefetch_read_data(msg.as_ptr(), 1); }
                    }
                }
            }
            
            // Try batch verification first
            if VerifyingKey::verify_batch(&msgs[lo..hi], &sigs[lo..hi], &keys[lo..hi]).is_ok() {
                // Mark all items in this range as verified
                for i in lo..hi {
                    for (orig_idx, &dedup_idx_opt) in orig_to_dedup.iter().enumerate() {
                        if let Some(dedup_idx) = dedup_idx_opt {
                            if dedup_idx == i {
                                out[orig_idx] = Ok(true);
                            }
                        }
                    }
                }
                continue;
            }
            
            // If down to one item, verify it individually
            if hi - lo == 1 {
                let verify_result = if is_legacy_ok {
                    // Try strict verification first, fall back to non-strict
                    keys[lo].verify_strict(msgs[lo], &sigs[lo])
                        .or_else(|_| keys[lo].verify(msgs[lo], &sigs[lo]))
                } else {
                    // Strict verification only
                    keys[lo].verify_strict(msgs[lo], &sigs[lo])
                };
                
                if verify_result.is_ok() {
                    // Mark all original items that map to this deduplicated entry
                    for (orig_idx, &dedup_idx_opt) in orig_to_dedup.iter().enumerate() {
                        if let Some(dedup_idx) = dedup_idx_opt {
                            if dedup_idx == lo {
                                out[orig_idx] = Ok(true);
                            }
                        }
                    }
                }
                continue;
            }
            
            // Split the range and process both halves (right first to maintain LIFO order)
            let mid = lo + ((hi - lo) >> 1);
            stack.push((lo, mid, is_legacy_ok));
            stack.push((mid, hi, is_legacy_ok));
        }
        
        // If any items failed strict verification, retry with legacy support
        if out.iter().any(|r| r.is_err()) {
            // Reset failed items
            for (i, res) in out.iter_mut().enumerate() {
                if res.is_err() && orig_to_dedup[i].is_some() {
                    *res = Err(BatchVerifierError::VerificationFailed);
                }
            }
            
            // Retry with legacy verification
            stack.push((0, idx.len(), true));
            while let Some((lo, hi, _)) = stack.pop() {
                // Same logic as above but with is_legacy_ok=true
                if hi <= lo { continue; }
                
                if VerifyingKey::verify_batch(&msgs[lo..hi], &sigs[lo..hi], &keys[lo..hi]).is_ok() {
                    // Mark all items in this range as verified
                    for i in lo..hi {
                        for (orig_idx, &dedup_idx_opt) in orig_to_dedup.iter().enumerate() {
                            if let Some(dedup_idx) = dedup_idx_opt {
                                if dedup_idx == i && out[orig_idx].is_err() {
                                    out[orig_idx] = Ok(true);
                                }
                            }
                        }
                    }
                    continue;
                }
                
                if hi - lo == 1 {
                    if keys[lo].verify(msgs[lo], &sigs[lo]).is_ok() {
                        for (orig_idx, &dedup_idx_opt) in orig_to_dedup.iter().enumerate() {
                            if let Some(dedup_idx) = dedup_idx_opt {
                                if dedup_idx == lo && out[orig_idx].is_err() {
                                    out[orig_idx] = Ok(true);
                                }
                            }
                        }
                    }
                    continue;
                }
                
                let mid = lo + ((hi - lo) >> 1);
                stack.push((lo, mid, true));
                stack.push((mid, hi, true));
            }
        }
    });

    out
}

// ========================= PATCH 48: HOT maintenance for deadline aging and COLD demotion =========================
#[cfg(all(feature = "advanced_sched", feature = "numa_hotcold"))]
fn hot_maint_prune_stale(g: &crate::l3_hotcold::L3GroupHC) {
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::{Instant, Duration};
    
    // Environment variable for max age in HOT (default 50ms)
    const MAX_HOT_AGE_MS: u64 = 50;
    const NS_PER_MS: u64 = 1_000_000;
    
    // Only run on the leader to avoid contention
    if !g.is_leader() { return; }
    
    // Skip if HOT is empty or we can't acquire the lock
    let hot_load = g.hot_load.load(Relaxed);
    if hot_load == 0 { return; }
    
    // Try to lock HOT (non-blocking)
    let now = Instant::now();
    if let Ok(mut hot) = g.hot.try_lock() {
        let mut i = 0;
        let mut pruned = 0;
        let max_age = Duration::from_millis(MAX_HOT_AGE_MS);
        
        // Scan HOT ring in LRU order (oldest first)
        while i < hot.len() && pruned < hot_load / 4 { // Limit to 25% per scan
            if let Some((item, enq_time)) = hot.get_mut(i) {
                if now.duration_since(*enq_time) > max_age {
                    // Move to COLD if possible, else drop
                    if let Err(old_item) = g.cold.push(item.take().unwrap()) {
                        // COLD full, drop the item
                        pruned += 1;
                    }
                    hot.remove(i);
                    g.hot_load.fetch_sub(1, Relaxed);
                    continue;
                }
            }
            i += 1;
        }
        
        // If we pruned anything, update the COLD load counter
        if pruned > 0 {
            g.cold_load.fetch_add(pruned as u32, Relaxed);
        }
    }
}

// ========================= PATCH 53C: verify_batch_dalek (REPLACE) =========================
#[cfg(feature = "advanced_sched")]
fn verify_batch_dalek(items: &[ShardItem]) -> Vec<Result<bool, BatchVerifierError>> {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    use smallvec::SmallVec;
    use std::{cell::RefCell, collections::HashMap};

    #[inline] fn pf_t0(p: *const u8) {
        #[cfg(target_arch="x86_64")]
        unsafe { core::arch::x86_64::_mm_prefetch(p as *const i8, 3) }
    }
    #[inline] fn pf_nta(p: *const u8) {
        #[cfg(target_arch="x86_64")]
        unsafe { core::arch::x86_64::_mm_prefetch(p as *const i8, 0) }
    }

    thread_local! {
        static TLS: RefCell<(
            SmallVec<[usize; 4096]>,        // idx (global)
            SmallVec<[&'static [u8]; 4096]>,// msgs
            SmallVec<[Signature; 4096]>,    // sigs
            SmallVec<[VerifyingKey; 4096]>, // keys
            SmallVec<[u64; 4096]>,          // hv (aligned with idx/msgs/sigs/keys)
            HashMap<u64, usize>,            // hv -> unique index
            SmallVec<[Option<usize>; 4096]>,// dup_of
            SmallVec<[(usize,usize); 4096]> // bisect stack
        )> = RefCell::new((
            SmallVec::new(), SmallVec::new(), SmallVec::new(), SmallVec::new(),
            SmallVec::new(), HashMap::new(), SmallVec::new(), SmallVec::new()
        ));
    }

    let n = items.len();
    if n == 0 { return Vec::new(); }
    let now_us = crate::clock::now_us();
    let mut out = vec![Err(BatchVerifierError::VerificationFailed); n];

    TLS.with(|cell| {
        let (ref mut idx, ref mut msgs, ref mut sigs, ref mut keys, ref mut hvs, ref mut seen, ref mut dup_of, ref mut stk) = *cell.borrow_mut();
        idx.clear(); msgs.clear(); sigs.clear(); keys.clear(); hvs.clear(); seen.clear(); dup_of.clear(); stk.clear();
        idx.reserve_exact(n); msgs.reserve_exact(n); sigs.reserve_exact(n); keys.reserve_exact(n); hvs.reserve_exact(n);
        dup_of.resize(n, None);

        // Stage uniques w/ cross-batch cache short-circuit + strong dedup
        for i in 0..n {
            let it = &items[i];
            if it.msg.is_empty() { continue; }

            if i + 8 < n {
                let nxt = &items[i + 8];
                pf_t0(nxt.msg.as_ptr());
                pf_nta(nxt.sig.as_ref().as_ptr());
                pf_nta(nxt.vk.as_bytes().as_ptr());
            }

            // hv over vk||sig||msg (seeded)
            let mut seed = xxhash_rust::xxh3::xxh3_64(&it.vk_bytes);
            let sig_bytes = it.sig.to_bytes();
            seed = xxhash_rust::xxh3::xxh3_64_with_seed(&sig_bytes, seed);
            let hv = xxhash_rust::xxh3::xxh3_64_with_seed(it.msg.as_ref(), seed);

            // Cross-batch cache hit → immediate verdict (no work)
            if let Some(ok) = crate::triple_cache::get(now_us, hv) {
                out[i] = if ok { Ok(true) } else { Err(BatchVerifierError::VerificationFailed) };
                continue;
            }

            // Intra-batch dedup
            if let Some(&j) = seen.get(&hv) {
                let same =
                    items[j].vk_bytes == it.vk_bytes &&
                    items[j].sig.to_bytes() == sig_bytes &&
                    items[j].msg.as_ref() == it.msg.as_ref();
                if same { dup_of[i] = Some(j); continue; }
            }

            idx.push(i);
            msgs.push(unsafe { core::slice::from_raw_parts(it.msg.as_ptr(), it.msg.len()) });
            sigs.push(it.sig.clone());
            keys.push(it.vk.clone());
            hvs.push(hv);
            seen.insert(hv, idx.len() - 1);
        }

        if idx.is_empty() { return; }

        // Locality order by vk_bytes desc; apply in-place to side arrays (incl. hvs)
        idx.sort_unstable_by(|&a, &b| items[b].vk_bytes.cmp(&items[a].vk_bytes));
        let mut p = 0usize;
        while p < idx.len() {
            let want = idx[p];
            if want == p { p += 1; continue; }
            msgs.swap(p, want); sigs.swap(p, want); keys.swap(p, want); hvs.swap(p, want);
            idx.swap(p, want);
        }

        // Chunked unique verify (caps tail)
        let chunk = std::env::var("QAB_DALEK_CHUNK").ok().and_then(|s| s.parse().ok()).unwrap_or(2048).max(64);
        let mut start = 0usize;
        while start < idx.len() {
            let chunk = crate::dalek_chunk_ctl::cur_chunk();
            let end = core::cmp::min(start + chunk, idx.len());
            let mut did_bisect = false;

            if VerifyingKey::verify_batch(&msgs[start..end], &sigs[start..end], &keys[start..end]).is_ok() {
                for k in start..end {
                    out[idx[k]] = Ok(true);
                    crate::triple_cache::put(now_us, hvs[k], true);
                }
            } else {
                // Iterative bisect (no recursion)
                did_bisect = true;
                stk.clear();
                stk.push((start, end));
                while let Some((lo, hi)) = stk.pop() {
                    if hi <= lo { continue; }
                    if (hi - lo) == 1 {
                        let strict_only = matches!(std::env::var("QAB_ED25519_STRICT_ONLY").as_deref(), Ok("1"));
                        let ok = if strict_only {
                            keys[lo].verify_strict(msgs[lo], &sigs[lo]).is_ok()
                        } else {
                            keys[lo].verify_strict(msgs[lo], &sigs[lo]).is_ok()
                            || keys[lo].verify(msgs[lo], &sigs[lo]).is_ok()
                        };
                        out[idx[lo]] = if ok { Ok(true) } else { Err(BatchVerifierError::VerificationFailed) };
                        crate::triple_cache::put(now_us, hvs[lo], ok);
                        continue;
                    }
                    if VerifyingKey::verify_batch(&msgs[lo..hi], &sigs[lo..hi], &keys[lo..hi]).is_ok() {
                        for k in lo..hi {
                            out[idx[k]] = Ok(true);
                            crate::triple_cache::put(now_us, hvs[k], true);
                        }
                        continue;
                    }
                    let mid = lo + ((hi - lo) >> 1);
                    stk.push((mid, hi));
                    stk.push((lo,  mid));
                }
            }

            crate::dalek_chunk_ctl::report(did_bisect);
            start = end;
        }

        // Propagate verdicts to duplicates
        for i in 0..n {
            if let Some(juniq) = dup_of[i] {
                let orig_global = idx[juniq];
                out[i] = out[orig_global].clone();
            }
        }
    });

    out
}

// ========================= VK Cache GC =========================
#[cfg(feature = "advanced_sched")]
const VK_CACHE_MAX: usize = {
    match option_env!("QAB_VK_CACHE_MAX") {
        Some(s) => match s.parse::<usize>() { 
            Ok(v) if v >= 1024 => v, 
            _ => 262_144 
        },
        None => 262_144
    }
};

#[cfg(feature = "advanced_sched")]
#[inline]
fn vk_cache_gc_tick() {
    use core::sync::atomic::Ordering::Relaxed;
    use rand::Rng as _;
    
    static TICK: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    
    // Only run GC every ~16k batches to reduce overhead
    if TICK.fetch_add(1, Relaxed) & 0x3FFF != 0 { 
        return; 
    }
    
    // Check if we're over the cache limit
    let len = VK_CACHE.read().len();
    if len <= VK_CACHE_MAX { 
        return; 
    }
    
    // Perform random eviction if cache is too large
    let mut w = VK_CACHE.write();
    let mut cnt = len.saturating_sub(VK_CACHE_MAX);
    let mut rng = rand::rngs::SmallRng::from_entropy();
    
    while cnt > 0 && !w.is_empty() {
        // Sample and remove a random key
        if let Some(k) = w.keys().next().cloned() {
            let _ = w.remove(&k);
            cnt -= 1;
            
            // Randomly skip a few to avoid hot-looping on the same buckets
            let skip = rng.gen_range(1..=7);
            for _ in 0..skip {
                if let Some(k2) = w.keys().next().cloned() {
                    let _ = w.remove(&k2);
                    if cnt == 0 { break; }
                    cnt -= 1;
                } else {
                    break;
                }
            }
        } else {
            break;
        }
    }
}

// ========================= CPU feature snapshot =========================
struct CpuFeatures { avx2: bool, avx512: bool, neon: bool, cores: usize }
impl CpuFeatures {
    fn detect() -> Self {
        let cores = core_affinity::get_core_ids().map(|v| v.len()).unwrap_or(1);
        #[cfg(target_arch="x86_64")]
        { return Self { avx2: std::is_x86_feature_detected!("avx2"), avx512: std::is_x86_feature_detected!("avx512f"), neon: false, cores }; }
        #[cfg(target_arch="aarch64")]
        { return Self { avx2: false, avx512: false, neon: true, cores }; }
        #[allow(unreachable_code)]
        Self { avx2: false, avx512: false, neon: false, cores }
    }
}

pub struct BatchVerifier {
    verification_cache: Arc<VerifCache>,
    pending_queue: Arc<ArrayQueue<VerificationItem>>,
    // carry original indices along with items
    batch_sender: Sender<Vec<(usize, VerificationItem)>>,
    result_receiver: Receiver<Vec<(usize, Result<bool, BatchVerifierError>)>>,
    stats: Arc<VerifierStats>,
    shutdown: Arc<AtomicBool>,
    worker_handles: Vec<std::thread::JoinHandle<()>>,
}

struct VerifierStats {
    total_verified: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    failed_verifications: AtomicU64,
    batch_count: AtomicU64,
    avg_batch_time_us: AtomicU64,
}

impl VerifierStats {
    fn new() -> Self {
        Self {
            total_verified: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            failed_verifications: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
            avg_batch_time_us: AtomicU64::new(0),
        }
    }
}

impl BatchVerifier {
    #[inline]
    fn recv_with_deadline(
        rx: &Receiver<Vec<(usize, Result<bool, BatchVerifierError>)>>,
        expected: usize,
        timeout: Duration,
        out: &mut [Result<bool, BatchVerifierError>],
    ) {
        let deadline = Instant::now() + timeout;
        let mut received = 0usize;
        while received < expected {
            let now = Instant::now();
            if now >= deadline { break; }
            let remaining = deadline - now;
            match rx.recv_timeout(remaining) {
                Ok(batch) => {
                    for (idx, res) in batch {
                        if let Err(BatchVerifierError::VerificationFailed) = out[idx] {
                            out[idx] = res;
                            received += 1;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        // Mark unresolved as timeout
        for r in out.iter_mut() {
            if matches!(r, Err(BatchVerifierError::VerificationFailed)) {
                *r = Err(BatchVerifierError::BatchTimeout);
            }
        }
    }
    pub fn new(worker_threads: usize) -> Self {
        let (batch_sender, batch_receiver) = bounded::<Vec<(usize, VerificationItem)>>(MAX_PENDING_BATCHES);
        let (result_sender, result_receiver) = bounded::<Vec<(usize, Result<bool, BatchVerifierError>)>>(MAX_PENDING_BATCHES * MAX_BATCH_SIZE);
        
        let verification_cache = Arc::new(VerifCache::new(CACHE_SIZE));
        let pending_queue = Arc::new(ArrayQueue::new(MAX_BATCH_SIZE * 4));
        let stats = Arc::new(VerifierStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        #[cfg(feature = "advanced_sched")]
        let inbox_per_priority = Arc::new(PriorityShards::new());
        #[cfg(feature = "advanced_sched")]
        let admission = Admittance::new();
        
        let mut worker_handles = Vec::with_capacity(worker_threads);
        
        for i in 0..worker_threads {
            let batch_receiver = batch_receiver.clone();
            let result_sender = result_sender.clone();
            let stats = stats.clone();
            let shutdown = shutdown.clone();
            let worker_idx = i;
            
            let handle = std::thread::spawn(move || {
                // Optional NUMA/core pinning if feature enabled
                #[cfg(feature = "core_affinity")]
                if let Some(cores) = core_affinity::get_core_ids() {
                    let core = cores[worker_idx % cores.len()];
                    let _ = core_affinity::set_for_current(core);
                }
                while !shutdown.load(Ordering::Acquire) {
                    match batch_receiver.recv_timeout(Duration::from_millis(10)) {
                        Ok(batch) => {
                            let start = Instant::now();
                            let items: Vec<VerificationItem> = batch.iter().map(|(_, it)| it.clone()).collect();
                            let results = Self::verify_batch_internal(&items);
                            
                            let batch_time = start.elapsed().as_micros() as u64;
                            stats.batch_count.fetch_add(1, Ordering::Relaxed);
                            
                            let current_avg = stats.avg_batch_time_us.load(Ordering::Relaxed);
                            let new_avg = (current_avg * 7 + batch_time) / 8;
                            stats.avg_batch_time_us.store(new_avg, Ordering::Relaxed);
                            histogram!("verifier.batch_time_us", batch_time as f64);
                            histogram!("verifier.batch_size", items.len() as f64);
        }
        
        Self {
            verification_cache,
            pending_queue,
            batch_sender,
            result_receiver,
            stats,
            shutdown,
            worker_handles,
        }
    }
    
    pub fn verify_single(&self, item: VerificationItem) -> Result<bool, BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        
        let cache_key = item.cache_key();
        
        if let Some(hit) = self.verification_cache.get(cache_key, Duration::from_secs(60)) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(hit);
        }
        
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        
        let result = self.verify_single_internal(&item);
        
        self.verification_cache.insert(cache_key, result.is_ok());
        
        result
    }
    
    pub async fn verify_batch(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        if items.is_empty() {
            return vec![];
        }
        
        if items.len() == 1 {
            return vec![self.verify_single(items.into_iter().next().unwrap())];
        }
        
        let mut results = vec![Err(BatchVerifierError::VerificationFailed); items.len()];
        let mut cached_indices = SmallVec::<[(usize, bool); 32]>::new();
        let mut uncached_items = Vec::with_capacity(items.len());
        let mut uncached_indices = Vec::with_capacity(items.len());

        for (idx, item) in items.iter().enumerate() {
            let cache_key = item.cache_key();
            
            if let Some(hit) = self.verification_cache.get(cache_key, Duration::from_secs(60)) {
                cached_indices.push((idx, hit));
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            uncached_items.push(item.clone());
            uncached_indices.push(idx);
        }
        
        for (idx, result) in cached_indices {
            results[idx] = Ok(result);
        }
        
        if uncached_items.is_empty() {
            return results;
        }
        
        // enqueue chunks with original indices carried along
        let total = uncached_items.len();
        let mut offset = 0;
        while offset < total {
            let end = (offset + MAX_BATCH_SIZE).min(total);
            let work_chunk: Vec<(usize, VerificationItem)> = uncached_indices[offset..end]
                .iter()
                .cloned()
                .zip(uncached_items[offset..end].iter().cloned())
                .collect();
            if self.batch_sender.send(work_chunk).is_err() {
                for idx in &uncached_indices[offset..end] {
                    results[*idx] = Err(BatchVerifierError::QueueFull);
                }
                offset = end;
                continue;
            }
            // Await results up to deadline, accumulating partials
            Self::recv_with_deadline(
                &self.result_receiver,
                end - offset,
                Duration::from_millis(VERIFICATION_TIMEOUT_MS),
                &mut results,
            );
            // Cache only resolved successes
            for gi in &uncached_indices[offset..end] {
                if let Ok(true) = results[*gi] {
                    let item = &items[*gi];
                    self.verification_cache.insert(item.cache_key(), true);
                }
            }
            offset = end;
        }
        
        results
    }

    // Synchronous core alias for clarity at call sites
    #[inline]
    pub fn verify_batch_sync(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        self.verify_batch_blocking(items)
    }

    // Blocking variant for non-async contexts
    pub fn verify_batch_blocking(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        // Reuse the async path logic without requiring a runtime by inlining the core
        // Note: This duplicates the structure above but without async signature
        if items.is_empty() { return vec![]; }
        if items.len() == 1 { return vec![self.verify_single(items.into_iter().next().unwrap())]; }

        let mut results = vec![Err(BatchVerifierError::VerificationFailed); items.len()];
        let mut cached_indices = SmallVec::<[(usize, bool); 32]>::new();
        let mut uncached_items = Vec::with_capacity(items.len());
        let mut uncached_indices = Vec::with_capacity(items.len());

        for (idx, item) in items.iter().enumerate() {
            let cache_key = item.cache_key();
            if let Some(hit) = self.verification_cache.get(cache_key, Duration::from_secs(60)) {
                cached_indices.push((idx, hit));
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            uncached_items.push(item.clone());
            uncached_indices.push(idx);
        }
        for (idx, result) in cached_indices { results[idx] = Ok(result); }
        if uncached_items.is_empty() { return results; }

        let total = uncached_items.len();
        let mut offset = 0;
        while offset < total {
            let end = (offset + MAX_BATCH_SIZE).min(total);
            let work_chunk: Vec<(usize, VerificationItem)> = uncached_indices[offset..end]
                .iter()
                .cloned()
                .zip(uncached_items[offset..end].iter().cloned())
                .collect();
            if self.batch_sender.send(work_chunk).is_err() {
                for idx in &uncached_indices[offset..end] { results[*idx] = Err(BatchVerifierError::QueueFull); }
                offset = end; continue;
            }
            Self::recv_with_deadline(
                &self.result_receiver,
                end - offset,
                Duration::from_millis(VERIFICATION_TIMEOUT_MS),
                &mut results,
            );
            for gi in &uncached_indices[offset..end] {
                if let Ok(true) = results[*gi] {
                    let item = &items[*gi];
                    self.verification_cache.insert(item.cache_key(), true);
                }
            }
            offset = end;
        }
        results
    }
    
    fn verify_single_internal(&self, item: &VerificationItem) -> Result<bool, BatchVerifierError> {
        let signature = Signature::from_bytes(&item.signature)
            .map_err(|_| BatchVerifierError::InvalidSignatureLength(item.signature.len()))?;
        
        let public_key = VerifyingKey::from_bytes(&item.public_key)
            .map_err(|_| BatchVerifierError::InvalidPublicKeyLength(item.public_key.len()))?;
        
        match public_key.verify(&item.message, &signature) {
            Ok(_) => {
                self.stats.total_verified.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(_) => {
                self.stats.failed_verifications.fetch_add(1, Ordering::Relaxed);
                Ok(false)
            }
        }
    }
    
    fn verify_batch_internal(items: &[VerificationItem]) -> Vec<Result<bool, BatchVerifierError>> {
        // For very small batches, the overhead of batch MSM can dominate; keep simple path
        if items.len() < PARALLEL_THRESHOLD {
            return items.iter().map(|item| Self::verify_item(item)).collect();
        }

        // True batch verification using dalek's multi-scalar algorithm
        Self::verify_uncached_batch_dalek(items)
    }
    
    fn verify_item(item: &VerificationItem) -> Result<bool, BatchVerifierError> {
        let signature = match Signature::from_bytes(&item.signature) {
            Ok(sig) => sig,
            Err(_) => return Err(BatchVerifierError::InvalidSignatureLength(item.signature.len())),
        };
        
        let public_key = match VerifyingKey::from_bytes(&item.public_key) {
            Ok(pk) => pk,
            Err(_) => return Err(BatchVerifierError::InvalidPublicKeyLength(item.public_key.len())),
        };
        
        match public_key.verify(&item.message, &signature) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    pub fn queue_verification(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        if self.pending_queue.is_full() {
            return Err(BatchVerifierError::QueueFull);
        }
        self.pending_queue.push(item).map_err(|_| BatchVerifierError::QueueFull)?;
        if self.pending_queue.len() >= MIN_BATCH_SIZE {
            self.process_pending_queue_pop()?;
        }
        Ok(())
    }

    fn process_pending_queue_pop(&self) -> Result<(), BatchVerifierError> {
        let mut drained: Vec<VerificationItem> = Vec::with_capacity(MAX_BATCH_SIZE);
        while drained.len() < MAX_BATCH_SIZE {
            if let Some(it) = self.pending_queue.pop() {
                drained.push(it);
            } else { break; }
        }
        if drained.is_empty() { return Ok(()); }

        // Adaptive batch sizing: cap by average message length and add small jitter
        let avg_len = drained.iter().map(|i| i.message.len()).sum::<usize>() / drained.len().max(1);
        let batch_cap = if avg_len > AVG_MSG_LEN_THRESH { 32 } else { MAX_BATCH_SIZE };
        let jitter = 0.8 + 0.2 * (random::<f64>());
        let dynamic_batch = ((batch_cap as f64) * jitter).round() as usize;
        let dynamic_batch = dynamic_batch.max(MIN_BATCH_SIZE).min(drained.len());

        // Truncate to dynamic size; attempt to requeue remainder if any
        let remainder = if drained.len() > dynamic_batch { Some(drained.split_off(dynamic_batch)) } else { None };
        if let Some(rest) = remainder {
            for it in rest {
                let _ = self.pending_queue.push(it);
            }
        }

        let batch: Vec<(usize, VerificationItem)> = drained.into_iter().enumerate().collect();

        if batch.is_empty() {
            return Ok(());
        }

        self.batch_sender
            .send(batch)
            .map_err(|_| BatchVerifierError::QueueFull)
    }

    pub fn flush_pending(&self) -> Result<Vec<Result<bool, BatchVerifierError>>, BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        let mut all_results = Vec::new();
        loop {
            let mut drained: Vec<VerificationItem> = Vec::with_capacity(MAX_BATCH_SIZE);
            while drained.len() < MAX_BATCH_SIZE {
                if let Some(it) = self.pending_queue.pop() {
                    drained.push(it);
                } else { break; }
            }
            // Locally index items 0..n for this batch; receiver maps back using provided indices
            let batch: Vec<(usize, VerificationItem)> = drained.into_iter().enumerate().collect();
            let batch_len = batch.len();

            if self.batch_sender.send(batch).is_err() {
                return Err(BatchVerifierError::QueueFull);
            }
            match self.result_receiver.recv_timeout(Duration::from_millis(VERIFICATION_TIMEOUT_MS * 2)) {
                Ok(results_vec) => {
                    all_results.extend(results_vec.into_iter().map(|(_, r)| r));
                }
                Err(_) => {
                    all_results.extend(vec![Err(BatchVerifierError::BatchTimeout); batch_len]);
                }
            }
            if self.pending_queue.is_empty() { break; }
        }

        Ok(all_results)
    }

    // ... (rest of the methods remain the same)
}

impl Drop for BatchVerifier {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        
        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }
    }
}

#[derive(Debug, Clone)]
pub struct VerifierStatsSnapshot {
    pub total_verified: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub failed_verifications: u64,
    pub batch_count: u64,
    pub avg_batch_time_us: u64,
    pub cache_size: usize,
}

pub struct PriorityBatchVerifier {
    verifier: Arc<BatchVerifier>,
    priority_queue: Arc<Mutex<Vec<PriQueue>>>,
    // Adaptive slot scheduler: nearest-deadline-first
    sched_pq: Arc<Mutex<BinaryHeap<(Reverse<u128>, VerificationItem)>>>,
    #[cfg(feature = "advanced_sched")]
    inbox_per_priority: Arc<PriorityShards>,
    #[cfg(feature = "advanced_sched")]
    admission: Admittance,
    processing_thread: Option<std::thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl PriorityBatchVerifier {
    pub fn new(worker_threads: usize) -> Self {
        let verifier = Arc::new(BatchVerifier::new(worker_threads));
        let priority_queue = Arc::new(Mutex::new({
            let mut v = Vec::with_capacity(256);
            for _ in 0..256 {
                v.push(PriQueue { q: VecDeque::new(), last_served: Instant::now() });
            }
            v
        }));
        let sched_pq = Arc::new(Mutex::new(BinaryHeap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        
        let verifier_clone = verifier.clone();
        let queue_clone = priority_queue.clone();
        let sched_clone = sched_pq.clone();
        let shutdown_clone = shutdown.clone();
        #[cfg(feature = "advanced_sched")]
        let inbox_clone = inbox_per_priority.clone();
        
        let processing_thread = std::thread::spawn(move || {
            let mut last_flush = Instant::now();
            let mut consecutive_empty = 0;
            
            while !shutdown_clone.load(Ordering::Acquire) {
                let mut found_items = false;
                let mut batch: Vec<VerificationItem> = Vec::with_capacity(MAX_BATCH_SIZE);

                // 1) Prefer scheduled items with nearest deadline first
                {
                    let mut pq = sched_clone.lock();
                    while batch.len() < MAX_BATCH_SIZE {
                        if let Some((_prio, it)) = pq.pop() {
                            batch.push(it);
                            found_items = true;
                        } else {
                            break;
                        }
                    }
                }

                // 1b) Drain lock-free inboxes staged by priority (if enabled)
                #[cfg(feature = "advanced_sched")]
                {
                    let mut buf = Vec::with_capacity(MAX_BATCH_SIZE);
                    inbox_clone.drain_ordered(MAX_BATCH_SIZE - batch.len(), &mut buf);
                    if !buf.is_empty() { found_items = true; batch.extend(buf.into_iter()); }
                }

                // 2) If still room, build batch with aging-aware priority without holding lock too long
                if batch.len() < MAX_BATCH_SIZE {
                    let mut queues = queue_clone.lock();
                    let now = Instant::now();
                    let mut picked = batch.len();
                    for pr in (0u16..=255u16).rev() {
                        if picked >= MAX_BATCH_SIZE { break; }
                        for idx in (0..=255usize).rev() {
                            if picked >= MAX_BATCH_SIZE { break; }
                            let pq = &mut queues[idx];
                            if pq.q.is_empty() { continue; }
                            let aged = now.duration_since(pq.last_served).as_millis() as u64 >= AGE_BOOST_MS;
                            if aged || idx as u16 == pr {
                                while picked < MAX_BATCH_SIZE {
                                    if let Some(it) = pq.q.pop_front() {
                                        batch.push(it);
                                        picked += 1;
                                        found_items = true;
                                    } else { break; }
                                }
                                pq.last_served = now;
                                if picked >= MAX_BATCH_SIZE { break; }
                            }
                        }
                    }
                }

                if !batch.is_empty() {
                    // Use blocking variant to avoid any runtime coupling
                    let _ = verifier_clone.verify_batch_blocking(batch);
                    consecutive_empty = 0;
                } else {
                    consecutive_empty += 1;
                }
                
                if last_flush.elapsed() >= Duration::from_millis(10) || 
                   (found_items) {
                    let _ = verifier_clone.flush_pending();
                    last_flush = Instant::now();
                }
                
                if consecutive_empty > 10 {
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
        });
        
        Self {
            verifier,
            priority_queue,
            sched_pq,
            #[cfg(feature = "advanced_sched")]
            inbox_per_priority,
            #[cfg(feature = "advanced_sched")]
            admission,
            processing_thread: Some(processing_thread),
            shutdown,
        }
    }
    
    pub fn queue_with_priority(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        
        let priority = item.priority as usize;
        #[cfg(feature = "advanced_sched")]
        {
            if self.admission.is_blacklisted(&item.public_key) {
                counter!("verifier.tx_admission_reject", 1);
                return Ok(());
            }
            if !self.admission.seen_maybe(item.cache_key()) { self.admission.note(item.cache_key()); }
            self.inbox_per_priority.push(item);
            histogram!("verifier.queue_len", 0.0);
            return Ok(());
        }
        let mut queues = self.priority_queue.lock();
        // Adaptive backpressure: drop low-profit when queue is congested
        let congested = queues[priority].q.len() >= MAX_BATCH_SIZE * 2;
        if congested && item.estimated_profit_lamports < PROFIT_THRESHOLD_LAMPORTS.load(Ordering::Relaxed) {
            counter!("verifier.tx_dropped_low_profit", 1);
            return Ok(()); // drop early
        }
        if queues[priority].q.len() >= MAX_BATCH_SIZE * 2 {
            return Err(BatchVerifierError::QueueFull);
        }
        queues[priority].q.push_back(item);
        histogram!("verifier.queue_len", queues[priority].q.len() as f64);
        Ok(())
    }

    /// Enqueue with explicit deadline for adaptive slot scheduling
    pub fn queue_with_deadline(&self, item: VerificationItem, deadline: Instant) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        // Adaptive backpressure for scheduled path
        if item.estimated_profit_lamports < PROFIT_THRESHOLD_LAMPORTS.load(Ordering::Relaxed) {
            // Only drop if many scheduled are pending
            let pq_len = { self.sched_pq.lock().len() };
            if pq_len >= MAX_PENDING_BATCHES {
                counter!("verifier.tx_dropped_low_profit", 1);
                return Ok(());
            }
        }
        let pq_len_after = {
            #[cfg(feature = "advanced_sched")]
            {
                if self.admission.is_blacklisted(&item.public_key) {
                    counter!("verifier.tx_admission_reject", 1);
                    0usize
                } else {
                    if !self.admission.seen_maybe(item.cache_key()) { self.admission.note(item.cache_key()); }
                    let k = mk_priority_key(item.priority, item.estimated_profit_lamports as u64, item.timestamp);
                    let mut pq = self.sched_pq.lock();
                    pq.push((Reverse(k), item));
                    pq.len()
                }
            }
            #[cfg(not(feature = "advanced_sched"))]
            {
                let ts = deadline.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as u128;
                let mut pq = self.sched_pq.lock();
                pq.push((Reverse(ts), item));
                pq.len()
            }
        };
        if pq_len_after > 0 { histogram!("verifier.sched_pq_len", pq_len_after as f64); }
        Ok(())
    }
    
    pub async fn verify_critical(&self, item: VerificationItem) -> Result<bool, BatchVerifierError> {
        let mut item = item;
        item.priority = 255;
        
        for attempt in 0..RETRY_ATTEMPTS {
            match self.verifier.verify_single(item.clone()) {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempt < RETRY_ATTEMPTS - 1 {
                        // Exponential backoff with jitter to avoid retry stampedes
                        let base = BACKOFF_BASE_MS * (1u64 << attempt);
                        let jitter = (random::<u64>() % BACKOFF_BASE_MS);
                        tokio::time::sleep(Duration::from_millis(base + jitter)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        
        Err(BatchVerifierError::VerificationFailed)
    }

    /// Profit^2-weighted scheduling enqueue (P^2 fairness)
    pub fn queue_profit_weighted(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        // Profit^2, penalize retries, favor freshness
        let profit = (item.estimated_profit_lamports.max(1) as u128);
        let mut score = profit.saturating_mul(profit) / (1 + item.retry_count as u128);
        let age_ms = item.timestamp.elapsed().as_millis() as u128;
        score = score.saturating_sub(age_ms);
        let mut pq = self.sched_pq.lock();
        pq.push((Reverse(score), item));
        histogram!("verifier.sched_pq_len", pq.len() as f64);
        Ok(())
    }
    
    pub fn get_verifier(&self) -> Arc<BatchVerifier> {
        self.verifier.clone()
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.verifier.shutdown();
    }
}

impl Drop for PriorityBatchVerifier {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        
        if let Some(thread) = self.processing_thread.take() {
            let _ = thread.join();
        }
    }
}

pub struct AdaptiveBatchVerifier {
    verifier: Arc<PriorityBatchVerifier>,
    batch_size: Arc<AtomicU64>,
    success_rate: Arc<AtomicU64>,
    latency_target_us: u64,
    min_success_rate: f64,
    latency_ewma: Ewma,
}

impl AdaptiveBatchVerifier {
    pub fn new(worker_threads: usize, latency_target_us: u64, min_success_rate: f64) -> Self {
        Self {
            verifier: Arc::new(PriorityBatchVerifier::new(worker_threads)),
            batch_size: Arc::new(AtomicU64::new(MIN_BATCH_SIZE as u64)),
            success_rate: Arc::new(AtomicU64::new(f64::to_bits(1.0))),
            latency_target_us,
            min_success_rate,
            latency_ewma: Ewma::new(latency_target_us as f64),
        }
    }
    
    /// Verifies a batch of verification items adaptively, adjusting the batch size based on latency and success rate.
    pub async fn verify_adaptive(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        let start = Instant::now();
        let current_batch_size = self.batch_size.load(Ordering::Relaxed) as usize;
        
        let mut results = Vec::with_capacity(items.len());
        let chunks: Vec<_> = items.chunks(current_batch_size).collect();
        
        let mut successful = 0u64;
        let mut total = 0u64;
        let mut total_msg_len: usize = 0;
        let mut total_msgs: usize = 0;
        
        for chunk in chunks {
            let chunk_results = self.verifier.get_verifier().verify_batch(chunk.to_vec()).await;
            
            for result in &chunk_results {
                total += 1;
                if result.is_ok() {
                    successful += 1;
                }
            }
            for it in chunk.iter() {
                total_msg_len = total_msg_len.saturating_add(it.message.len());
                total_msgs = total_msgs.saturating_add(1);
            }
            
            results.extend(chunk_results);
        }
        
        let elapsed_us = start.elapsed().as_micros() as u64;
        let success_rate = if total > 0 { successful as f64 / total as f64 } else { 1.0 };
        
        self.success_rate.store(f64::to_bits(success_rate), Ordering::Relaxed);
        
        let avg_len = if total_msgs > 0 { (total_msg_len / total_msgs) as u64 } else { 0 };
        self.adjust_batch_size(elapsed_us, success_rate, avg_len);
        
        results
    }

    /// Adjusts the batch size based on latency, success rate, and average message length.
    fn adjust_batch_size(&self, latency_us: u64, success: f64, avg_msg_len: u64) {
        // EWMA with clipping to reduce noise sensitivity
        let alpha = 0.2;
        let clip = (latency_us.min(self.latency_target_us.saturating_mul(4))) as f64;
        self.latency_ewma.update(clip, alpha);

        let l = f64::from_bits(self.latency_ewma.val.load(Ordering::Relaxed));
        let target = self.latency_target_us as f64;
        let pressure = (l / target).powf(3.0); // cubic response for faster convergence

        let mut size = self.batch_size.load(Ordering::Relaxed) as f64;
        if pressure > 1.0 {
            // Under pressure: contract smoothly; denominator grows with pressure
            size = (size / (1.0 + 0.5 * (pressure - 1.0))).max(MIN_BATCH_SIZE as f64);
        } else if success >= self.min_success_rate && pressure < 0.5 {
            // Plenty of headroom and high success: expand more aggressively
            size = (size * 1.25).min(MAX_BATCH_SIZE as f64);
        }

        // Admission control by message size: reduce size if payloads are large
        if (avg_msg_len as usize) > AVG_MSG_LEN_THRESH {
            let scale = (AVG_MSG_LEN_THRESH as f64) / (avg_msg_len as f64);
            // Clip scale to [0.5, 1.0]
            let scale = scale.clamp(0.5, 1.0);
            size = (size * scale).max(MIN_BATCH_SIZE as f64);
        }

        self.batch_size.store(size.round() as u64, Ordering::Relaxed);
    }
}

// EWMA helper for latency smoothing
struct Ewma { val: AtomicU64 }
impl Ewma {
    fn new(init: f64) -> Self { Self { val: AtomicU64::new(init.to_bits()) } }
    fn update(&self, sample: f64, alpha: f64) {
        let cur = f64::from_bits(self.val.load(Ordering::Relaxed));
        let new = cur + alpha * (sample - cur);
        self.val.store(new.to_bits(), Ordering::Relaxed);
    }
}
    
    pub fn get_current_batch_size(&self) -> usize {
        self.batch_size.load(Ordering::Relaxed) as usize
    }
    
{{ ... }}
        f64::from_bits(self.success_rate.load(Ordering::Relaxed))
    }
}

#[inline(always)]
pub fn quick_verify(signature: &[u8; 64], public_key: &[u8; 32], message: &[u8]) -> bool {
    if let Ok(sig) = Signature::from_bytes(signature) {
        if let Ok(pk) = VerifyingKey::from_bytes(public_key) {
            return pk.verify(message, &sig).is_ok();
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_batch_verifier_creation() {
        let verifier = BatchVerifier::new(4);
        assert!(!verifier.is_shutdown());
    }
}

