use anyhow::{Context, Result};
use core_affinity::{self, CoreId};
use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
use nix::sched::{sched_setscheduler, CpuSet, Policy};
use nix::sys::resource::{setrlimit, Resource, Rlimit};
use nix::unistd::Pid;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use procfs::process::Process;
use procfs::{CpuInfo, Meminfo};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{CpuExt, System, SystemExt};
use tracing::{debug, error, info, warn};

const CACHE_LINE_SIZE: usize = 64;
const PERFORMANCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const OPTIMIZATION_INTERVAL: Duration = Duration::from_secs(30);
const MAX_JITTER_NS: u64 = 1000;
const TARGET_CPU_USAGE: f32 = 85.0;
const CRITICAL_THREAD_PRIORITY: i32 = 99;
const HIGH_THREAD_PRIORITY: i32 = 90;
const NORMAL_THREAD_PRIORITY: i32 = 50;

static CPU_TOPOLOGY: Lazy<Arc<RwLock<CpuTopology>>> = Lazy::new(|| {
    Arc::new(RwLock::new(CpuTopology::detect().unwrap_or_default()))
});

static OPTIMIZER: Lazy<Arc<RwLock<CpuAffinityOptimizer>>> = Lazy::new(|| {
    Arc::new(RwLock::new(CpuAffinityOptimizer::new()))
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuTopology {
    pub cores: Vec<CoreInfo>,
    pub numa_nodes: Vec<NumaNode>,
    pub cache_hierarchy: CacheHierarchy,
    pub total_cores: usize,
    pub physical_cores: usize,
    pub hyperthreading: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreInfo {
    pub id: usize,
    pub physical_id: usize,
    pub numa_node: usize,
    pub siblings: Vec<usize>,
    pub cache_sharing: HashMap<CacheLevel, Vec<usize>>,
    pub isolated: bool,
    pub irq_affinity: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaNode {
    pub id: usize,
    pub cores: Vec<usize>,
    pub memory_mb: u64,
    pub distance: HashMap<usize, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheHierarchy {
    pub l1d: Vec<CacheInfo>,
    pub l1i: Vec<CacheInfo>,
    pub l2: Vec<CacheInfo>,
    pub l3: Vec<CacheInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInfo {
    pub size_kb: u64,
    pub line_size: u32,
    pub associativity: u32,
    pub shared_cores: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CacheLevel {
    L1Data,
    L1Instruction,
    L2,
    L3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreadRole {
    NetworkReceive,
    NetworkSend,
    TransactionProcessing,
    BlockProcessing,
    Simulation,
    StateAccess,
    Monitoring,
    Background,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadAffinity {
    pub thread_id: u64,
    pub role: ThreadRole,
    pub core_ids: Vec<usize>,
    pub numa_node: Option<usize>,
    pub priority: i32,
    pub policy: Policy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub core_usage: HashMap<usize, f32>,
    pub cache_misses: HashMap<usize, u64>,
    pub context_switches: u64,
    pub interrupts: HashMap<usize, u64>,
    pub numa_misses: HashMap<usize, u64>,
    pub timestamp: Instant,
}

pub struct CpuAffinityOptimizer {
    topology: Arc<RwLock<CpuTopology>>,
    thread_assignments: HashMap<u64, ThreadAffinity>,
    performance_history: Vec<PerformanceMetrics>,
    optimization_enabled: bool,
    isolated_cores: HashSet<usize>,
    critical_cores: Vec<usize>,
    last_optimization: Instant,
}

impl Default for CpuTopology {
    fn default() -> Self {
        Self {
            cores: vec![],
            numa_nodes: vec![],
            cache_hierarchy: CacheHierarchy {
                l1d: vec![],
                l1i: vec![],
                l2: vec![],
                l3: vec![],
            },
            total_cores: 0,
            physical_cores: 0,
            hyperthreading: false,
        }
    }
}

impl CpuTopology {
    pub fn detect() -> Result<Self> {
        let cpu_info = CpuInfo::new()?;
        let mut system = System::new_all();
        system.refresh_all();
        
        let total_cores = system.cpus().len();
        let mut cores = Vec::with_capacity(total_cores);
        let mut numa_nodes = HashMap::new();
        let mut physical_cores_set = HashSet::new();
        
        for cpu_id in 0..total_cores {
            let core_info = Self::parse_core_info(cpu_id)?;
            physical_cores_set.insert(core_info.physical_id);
            
            numa_nodes.entry(core_info.numa_node)
                .or_insert_with(Vec::new)
                .push(cpu_id);
            
            cores.push(core_info);
        }
        
        let numa_node_vec: Vec<NumaNode> = numa_nodes.into_iter()
            .map(|(id, core_list)| {
                let memory_mb = Self::get_numa_memory(id).unwrap_or(0);
                let distance = Self::get_numa_distances(id).unwrap_or_default();
                
                NumaNode {
                    id,
                    cores: core_list,
                    memory_mb,
                    distance,
                }
            })
            .collect();
        
        let cache_hierarchy = Self::detect_cache_hierarchy()?;
        let hyperthreading = total_cores > physical_cores_set.len();
        
        Ok(Self {
            cores,
            numa_nodes: numa_node_vec,
            cache_hierarchy,
            total_cores,
            physical_cores: physical_cores_set.len(),
            hyperthreading,
        })
    }
    
    fn parse_core_info(cpu_id: usize) -> Result<CoreInfo> {
        let topology_path = format!("/sys/devices/system/cpu/cpu{}/topology", cpu_id);
        
        let physical_id = fs::read_to_string(format!("{}/physical_package_id", topology_path))
            .unwrap_or_else(|_| "0".to_string())
            .trim()
            .parse::<usize>()
            .unwrap_or(0);
        
        let siblings = Self::parse_cpu_list(&format!("{}/thread_siblings_list", topology_path))
            .unwrap_or_else(|_| vec![cpu_id]);
        
        let numa_node = Self::get_cpu_numa_node(cpu_id).unwrap_or(0);
        let cache_sharing = Self::detect_cache_sharing(cpu_id)?;
        let isolated = Self::is_core_isolated(cpu_id);
        let irq_affinity = Self::get_irq_affinity(cpu_id)?;
        
        Ok(CoreInfo {
            id: cpu_id,
            physical_id,
            numa_node,
            siblings,
            cache_sharing,
            isolated,
            irq_affinity,
        })
    }
    
    fn parse_cpu_list(path: &str) -> Result<Vec<usize>> {
        let content = fs::read_to_string(path)?;
        let mut cpus = Vec::new();
        
        for part in content.trim().split(',') {
            if let Some((start, end)) = part.split_once('-') {
                let start = start.parse::<usize>()?;
                let end = end.parse::<usize>()?;
                cpus.extend(start..=end);
            } else {
                cpus.push(part.parse::<usize>()?);
            }
        }
        
        Ok(cpus)
    }
    
    fn get_cpu_numa_node(cpu_id: usize) -> Result<usize> {
        let numa_path = format!("/sys/devices/system/cpu/cpu{}/node", cpu_id);
        if let Ok(entries) = fs::read_dir(&numa_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.starts_with("node") {
                            return name[4..].parse::<usize>()
                                .context("Failed to parse NUMA node ID");
                        }
                    }
                }
            }
        }
        Ok(0)
    }
    
    fn detect_cache_sharing(cpu_id: usize) -> Result<HashMap<CacheLevel, Vec<usize>>> {
        let mut cache_sharing = HashMap::new();
        let cache_path = format!("/sys/devices/system/cpu/cpu{}/cache", cpu_id);
        
        if let Ok(entries) = fs::read_dir(&cache_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let index_path = entry.path();
                    if let Ok(cache_type) = fs::read_to_string(index_path.join("type")) {
                        let level = fs::read_to_string(index_path.join("level"))
                            .unwrap_or_else(|_| "0".to_string())
                            .trim()
                            .parse::<u32>()
                            .unwrap_or(0);
                        
                        let cache_level = match (level, cache_type.trim()) {
                            (1, "Data") => Some(CacheLevel::L1Data),
                            (1, "Instruction") => Some(CacheLevel::L1Instruction),
                            (2, _) => Some(CacheLevel::L2),
                            (3, _) => Some(CacheLevel::L3),
                            _ => None,
                        };
                        
                        if let Some(cache_level) = cache_level {
                            let shared_cpus = Self::parse_cpu_list(
                                &index_path.join("shared_cpu_list").to_string_lossy()
                            ).unwrap_or_else(|_| vec![cpu_id]);
                            
                            cache_sharing.insert(cache_level, shared_cpus);
                        }
                    }
                }
            }
        }
        
        Ok(cache_sharing)
    }
    
    fn is_core_isolated(cpu_id: usize) -> bool {
        if let Ok(cmdline) = fs::read_to_string("/proc/cmdline") {
            if let Some(isolated) = cmdline.split_whitespace()
                .find(|s| s.starts_with("isolcpus=")) {
                let cpu_list = isolated.trim_start_matches("isolcpus=");
                if let Ok(isolated_cpus) = Self::parse_cpu_list(&format!("/tmp/{}", cpu_list)) {
                    return isolated_cpus.contains(&cpu_id);
                }
            }
        }
        false
    }
    
    fn get_irq_affinity(cpu_id: usize) -> Result<Vec<u32>> {
        let mut irqs = Vec::new();
        
        if let Ok(entries) = fs::read_dir("/proc/irq") {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Ok(irq_num) = entry.file_name().to_str().unwrap_or("").parse::<u32>() {
                        let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq_num);
                        if let Ok(affinity) = fs::read_to_string(&affinity_path) {
                            if let Ok(cpus) = Self::parse_cpu_list(&format!("/tmp/{}", affinity.trim())) {
                                if cpus.contains(&cpu_id) {
                                    irqs.push(irq_num);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(irqs)
    }
    
    fn get_numa_memory(node_id: usize) -> Result<u64> {
        let meminfo_path = format!("/sys/devices/system/node/node{}/meminfo", node_id);
        if let Ok(content) = fs::read_to_string(&meminfo_path) {
            for line in content.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        return parts[1].parse::<u64>()
                            .map(|kb| kb / 1024)
                            .context("Failed to parse memory size");
                    }
                }
            }
        }
        Ok(0)
    }
    
        fn get_numa_distances(node_id: usize) -> Result<HashMap<usize, u32>> {
        let mut distances = HashMap::new();
        let distance_path = format!("/sys/devices/system/node/node{}/distance", node_id);
        
        if let Ok(content) = fs::read_to_string(&distance_path) {
            for (target_node, distance_str) in content.split_whitespace().enumerate() {
                if let Ok(distance) = distance_str.parse::<u32>() {
                    distances.insert(target_node, distance);
                }
            }
        }
        
        Ok(distances)
    }
    
    fn detect_cache_hierarchy() -> Result<CacheHierarchy> {
        let mut l1d = Vec::new();
        let mut l1i = Vec::new();
        let mut l2 = Vec::new();
        let mut l3 = Vec::new();
        
        let cache_base = "/sys/devices/system/cpu/cpu0/cache";
        if let Ok(entries) = fs::read_dir(cache_base) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let index_path = entry.path();
                    
                    let cache_type = fs::read_to_string(index_path.join("type"))
                        .unwrap_or_default()
                        .trim()
                        .to_string();
                    
                    let level = fs::read_to_string(index_path.join("level"))
                        .unwrap_or_else(|_| "0".to_string())
                        .trim()
                        .parse::<u32>()
                        .unwrap_or(0);
                    
                    let size_str = fs::read_to_string(index_path.join("size"))
                        .unwrap_or_else(|_| "0K".to_string());
                    let size_kb = Self::parse_size_kb(&size_str);
                    
                    let line_size = fs::read_to_string(index_path.join("coherency_line_size"))
                        .unwrap_or_else(|_| "64".to_string())
                        .trim()
                        .parse::<u32>()
                        .unwrap_or(64);
                    
                    let associativity = fs::read_to_string(index_path.join("ways_of_associativity"))
                        .unwrap_or_else(|_| "8".to_string())
                        .trim()
                        .parse::<u32>()
                        .unwrap_or(8);
                    
                    let shared_cpus = Self::parse_cpu_list(
                        &index_path.join("shared_cpu_list").to_string_lossy()
                    ).unwrap_or_else(|_| vec![0]);
                    
                    let cache_info = CacheInfo {
                        size_kb,
                        line_size,
                        associativity,
                        shared_cores: shared_cpus,
                    };
                    
                    match (level, cache_type.as_str()) {
                        (1, "Data") => l1d.push(cache_info),
                        (1, "Instruction") => l1i.push(cache_info),
                        (2, _) => l2.push(cache_info),
                        (3, _) => l3.push(cache_info),
                        _ => {}
                    }
                }
            }
        }
        
        Ok(CacheHierarchy { l1d, l1i, l2, l3 })
    }
    
    fn parse_size_kb(size_str: &str) -> u64 {
        let size_str = size_str.trim();
        if let Some(pos) = size_str.find(|c: char| c.is_alphabetic()) {
            let (num_str, unit) = size_str.split_at(pos);
            if let Ok(num) = num_str.parse::<u64>() {
                match unit {
                    "K" => return num,
                    "M" => return num * 1024,
                    "G" => return num * 1024 * 1024,
                    _ => return num,
                }
            }
        }
        0
    }
}

impl CpuAffinityOptimizer {
    pub fn new() -> Self {
        let topology = CPU_TOPOLOGY.clone();
        let isolated_cores = Self::detect_isolated_cores();
        let critical_cores = Self::select_critical_cores(&topology.read(), &isolated_cores);
        
        Self {
            topology,
            thread_assignments: HashMap::new(),
            performance_history: Vec::with_capacity(1000),
            optimization_enabled: true,
            isolated_cores,
            critical_cores,
            last_optimization: Instant::now(),
        }
    }
    
    pub fn initialize() -> Result<()> {
        info!("Initializing CPU affinity optimizer");
        
        // Set resource limits
        setrlimit(
            Resource::RLIMIT_RTPRIO,
            &Rlimit { rlim_cur: CRITICAL_THREAD_PRIORITY as u64, rlim_max: CRITICAL_THREAD_PRIORITY as u64 }
        )?;
        
        // Disable CPU frequency scaling for performance
        Self::set_cpu_governor("performance")?;
        
        // Disable CPU idle states for low latency
        Self::configure_cpu_idle_states()?;
        
        // Configure interrupt affinity
        Self::optimize_interrupt_affinity()?;
        
        // Start performance monitoring thread
        thread::spawn(|| {
            if let Err(e) = Self::performance_monitor_loop() {
                error!("Performance monitor failed: {}", e);
            }
        });
        
        info!("CPU affinity optimizer initialized successfully");
        Ok(())
    }
    
    pub fn assign_thread(&self, thread_id: u64, role: ThreadRole) -> Result<()> {
        let topology = self.topology.read();
        let core_ids = self.select_cores_for_role(role, &topology)?;
        
        let priority = match role {
            ThreadRole::NetworkReceive | ThreadRole::NetworkSend => CRITICAL_THREAD_PRIORITY,
            ThreadRole::TransactionProcessing | ThreadRole::BlockProcessing => HIGH_THREAD_PRIORITY,
            ThreadRole::Simulation | ThreadRole::StateAccess => HIGH_THREAD_PRIORITY,
            ThreadRole::Monitoring | ThreadRole::Background => NORMAL_THREAD_PRIORITY,
        };
        
        let policy = match role {
            ThreadRole::NetworkReceive | ThreadRole::NetworkSend => Policy::Fifo,
            ThreadRole::TransactionProcessing | ThreadRole::BlockProcessing => Policy::Fifo,
            _ => Policy::Normal,
        };
        
        let numa_node = topology.cores.get(core_ids[0])
            .map(|c| c.numa_node);
        
        self.set_thread_affinity(thread_id, &core_ids, priority, policy)?;
        
        let affinity = ThreadAffinity {
            thread_id,
            role,
            core_ids,
            numa_node,
            priority,
            policy,
        };
        
        OPTIMIZER.write().thread_assignments.insert(thread_id, affinity);
        
        debug!("Assigned thread {} with role {:?} to cores {:?}", thread_id, role, core_ids);
        Ok(())
    }
    
    fn select_cores_for_role(&self, role: ThreadRole, topology: &CpuTopology) -> Result<Vec<usize>> {
        let available_cores: Vec<usize> = match role {
            ThreadRole::NetworkReceive | ThreadRole::NetworkSend => {
                self.critical_cores.iter()
                    .filter(|&&c| self.isolated_cores.contains(&c))
                    .copied()
                    .collect()
            },
            ThreadRole::TransactionProcessing | ThreadRole::BlockProcessing => {
                self.critical_cores.iter()
                    .filter(|&&c| !self.is_core_busy(c))
                    .copied()
                    .collect()
            },
            ThreadRole::Simulation => {
                topology.cores.iter()
                    .filter(|c| !self.critical_cores.contains(&c.id))
                    .filter(|c| c.siblings.len() == 1)
                    .map(|c| c.id)
                    .collect()
            },
            ThreadRole::StateAccess => {
                self.find_cores_with_shared_cache(CacheLevel::L3, topology)
            },
            ThreadRole::Monitoring | ThreadRole::Background => {
                topology.cores.iter()
                    .filter(|c| !self.critical_cores.contains(&c.id))
                    .filter(|c| !self.isolated_cores.contains(&c.id))
                    .map(|c| c.id)
                    .collect()
            },
        };
        
        if available_cores.is_empty() {
            return Err(anyhow::anyhow!("No suitable cores available for role {:?}", role));
        }
        
        let num_cores = match role {
            ThreadRole::NetworkReceive | ThreadRole::NetworkSend => 1,
            ThreadRole::TransactionProcessing => 2,
            ThreadRole::BlockProcessing => 2,
            ThreadRole::Simulation => 4,
            ThreadRole::StateAccess => 2,
            ThreadRole::Monitoring | ThreadRole::Background => 1,
        };
        
        Ok(available_cores.into_iter().take(num_cores).collect())
    }
    
    fn set_thread_affinity(&self, thread_id: u64, core_ids: &[usize], priority: i32, policy: Policy) -> Result<()> {
        let mut cpu_set = CpuSet::new();
        for &core_id in core_ids {
            cpu_set.set(core_id)?;
        }
        
        let pid = Pid::from_raw(thread_id as i32);
        
        // Set CPU affinity
        unsafe {
            let mut set: cpu_set_t = mem::zeroed();
            CPU_ZERO(&mut set);
            for &core_id in core_ids {
                CPU_SET(core_id, &mut set);
            }
            
            let ret = sched_setaffinity(thread_id as i32, mem::size_of::<cpu_set_t>(), &set);
            if ret != 0 {
                return Err(anyhow::anyhow!("Failed to set CPU affinity"));
            }
        }
        
        // Set scheduling policy and priority
        sched_setscheduler(pid, policy, priority)?;
        
        Ok(())
    }
    
    fn detect_isolated_cores() -> HashSet<usize> {
        let mut isolated = HashSet::new();
        
        if let Ok(cmdline) = fs::read_to_string("/proc/cmdline") {
            if let Some(isolcpus) = cmdline.split_whitespace()
                .find(|s| s.starts_with("isolcpus=")) {
                let cpu_list = isolcpus.trim_start_matches("isolcpus=");
                if let Ok(cpus) = CpuTopology::parse_cpu_list(&format!("/tmp/{}", cpu_list)) {
                    isolated.extend(cpus);
                }
            }
        }
        
        isolated
    }
    
    fn select_critical_cores(topology: &CpuTopology, isolated_cores: &HashSet<usize>) -> Vec<usize> {
        let mut critical = Vec::new();
        
        // Prefer isolated cores first
        critical.extend(isolated_cores.iter().copied());
        
        // Add physical cores (no hyperthreading) with exclusive L3 cache
        for core in &topology.cores {
            if core.siblings.len() == 1 && !critical.contains(&core.id) {
                critical.push(core.id);
            }
        }
        
        // Limit to reasonable number
        critical.truncate(8);
        critical.sort();
        critical
    }
    
    fn is_core_busy(&self, core_id: usize) -> bool {
        if let Some(metrics) = self.performance_history.last() {
            if let Some(&usage) = metrics.core_usage.get(&core_id) {
                return usage > TARGET_CPU_USAGE;
            }
        }
        false
    }
    
    fn find_cores_with_shared_cache(&self, level: CacheLevel, topology: &CpuTopology) -> Vec<usize> {
        let mut shared_cores = Vec::new();
        let mut visited = HashSet::new();
        
        for core in &topology.cores {
            if visited.contains(&core.id) {
                continue;
            }
            
            if let Some(sharing) = core.cache_sharing.get(&level) {
                if sharing.len() > 1 {
                    shared_cores.extend(sharing.iter().copied());
                    visited.extend(sharing.iter().copied());
                }
            }
        }
        
        shared_cores
    }
    
    fn set_cpu_governor(governor: &str) -> Result<()> {
        let cpus = core_affinity::get_core_ids().unwrap_or_default();
        
        for cpu in cpus {
            let path = format!("/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor", cpu.id);
            if Path::new(&path).exists() {
                fs::write(&path, governor)?;
            }
        }
        
        Ok(())
    }
    
    fn configure_cpu_idle_states() -> Result<()> {
        let idle_path = "/sys/devices/system/cpu/cpu0/cpuidle";
        
        if Path::new(idle_path).exists() {
            for entry in fs::read_dir(idle_path)? {
                let entry = entry?;
                if entry.file_name().to_str().unwrap_or("").starts_with("state") {
                    let disable_path = entry.path().join("disable");
                    if disable_path.exists() {
                        fs::write(&disable_path, "1")?;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn optimize_interrupt_affinity() -> Result<()> {
        let mut network_irqs = Vec::new();
        
        // Find network interface IRQs
        if let Ok(entries) = fs::read_dir("/proc/irq") {
            for entry in entries {
                if let Ok(entry) = entry {
                    let irq_path = entry.path();
                    let irq_name_path = irq_path.join("actions");
                    
                     if let Ok(content) = fs::read_to_string(&irq_name_path) {
                        if content.contains("eth") || content.contains("mlx") || content.contains("ixgbe") {
                            if let Ok(irq_num) = entry.file_name().to_str().unwrap_or("").parse::<u32>() {
                                network_irqs.push(irq_num);
                            }
                        }
                    }
                }
            }
        }
        
        // Distribute network IRQs across isolated cores
        let optimizer = OPTIMIZER.read();
        let isolated_vec: Vec<usize> = optimizer.isolated_cores.iter().copied().collect();
        
        if !isolated_vec.is_empty() && !network_irqs.is_empty() {
            for (i, irq) in network_irqs.iter().enumerate() {
                let target_core = isolated_vec[i % isolated_vec.len()];
                let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq);
                
                if let Err(e) = fs::write(&affinity_path, target_core.to_string()) {
                    warn!("Failed to set IRQ {} affinity: {}", irq, e);
                }
            }
        }
        
        // Set RPS/RFS for network optimization
        Self::configure_network_rps_rfs()?;
        
        Ok(())
    }
    
    fn configure_network_rps_rfs() -> Result<()> {
        let interfaces = ["eth0", "eth1", "ens0", "ens1", "enp0s3"];
        
        for iface in &interfaces {
            let rps_cpus_path = format!("/sys/class/net/{}/queues/rx-0/rps_cpus", iface);
            let rps_flow_cnt_path = format!("/sys/class/net/{}/queues/rx-0/rps_flow_cnt", iface);
            let rfs_entries_path = "/proc/sys/net/core/rps_sock_flow_entries";
            
            if Path::new(&rps_cpus_path).exists() {
                // Set RPS to use all non-isolated cores
                let optimizer = OPTIMIZER.read();
                let rps_cores: Vec<usize> = (0..optimizer.topology.read().total_cores)
                    .filter(|c| !optimizer.isolated_cores.contains(c))
                    .collect();
                
                if !rps_cores.is_empty() {
                    let cpu_mask = Self::cores_to_hex_mask(&rps_cores);
                    let _ = fs::write(&rps_cpus_path, cpu_mask);
                    let _ = fs::write(&rps_flow_cnt_path, "4096");
                }
            }
            
            if Path::new(rfs_entries_path).exists() {
                let _ = fs::write(rfs_entries_path, "32768");
            }
        }
        
        Ok(())
    }
    
    fn cores_to_hex_mask(cores: &[usize]) -> String {
        let mut mask: u128 = 0;
        for &core in cores {
            if core < 128 {
                mask |= 1u128 << core;
            }
        }
        format!("{:032x}", mask)
    }
    
    fn performance_monitor_loop() -> Result<()> {
        let mut system = System::new_all();
        let mut last_sample = Instant::now();
        
        loop {
            thread::sleep(PERFORMANCE_SAMPLE_INTERVAL);
            
            if last_sample.elapsed() >= PERFORMANCE_SAMPLE_INTERVAL {
                system.refresh_all();
                
                let metrics = Self::collect_performance_metrics(&mut system)?;
                
                let mut optimizer = OPTIMIZER.write();
                optimizer.performance_history.push(metrics);
                
                // Keep only recent history
                if optimizer.performance_history.len() > 1000 {
                    optimizer.performance_history.remove(0);
                }
                
                // Run optimization if needed
                if optimizer.optimization_enabled && 
                   optimizer.last_optimization.elapsed() >= OPTIMIZATION_INTERVAL {
                    if let Err(e) = optimizer.optimize_assignments() {
                        error!("Optimization failed: {}", e);
                    }
                    optimizer.last_optimization = Instant::now();
                }
                
                last_sample = Instant::now();
            }
        }
    }
    
    fn collect_performance_metrics(system: &mut System) -> Result<PerformanceMetrics> {
        let mut core_usage = HashMap::new();
        let mut cache_misses = HashMap::new();
        let mut interrupts = HashMap::new();
        let mut numa_misses = HashMap::new();
        
        // Collect CPU usage
        for (i, cpu) in system.cpus().iter().enumerate() {
            core_usage.insert(i, cpu.cpu_usage());
        }
        
        // Collect cache misses from perf counters
        for i in 0..system.cpus().len() {
            let cache_miss_count = Self::read_perf_counter(i, "cache-misses").unwrap_or(0);
            cache_misses.insert(i, cache_miss_count);
        }
        
        // Collect interrupt counts
        if let Ok(content) = fs::read_to_string("/proc/interrupts") {
            for line in content.lines().skip(1) {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > system.cpus().len() {
                    for (i, &count_str) in parts[1..=system.cpus().len()].iter().enumerate() {
                        if let Ok(count) = count_str.parse::<u64>() {
                            *interrupts.entry(i).or_insert(0) += count;
                        }
                    }
                }
            }
        }
        
        // Collect NUMA statistics
        for node_id in 0..4 {
            let numa_stat_path = format!("/sys/devices/system/node/node{}/numastat", node_id);
            if let Ok(content) = fs::read_to_string(&numa_stat_path) {
                for line in content.lines() {
                    if line.starts_with("numa_miss") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            if let Ok(misses) = parts[1].parse::<u64>() {
                                numa_misses.insert(node_id, misses);
                            }
                        }
                    }
                }
            }
        }
        
        // Read context switches
        let context_switches = Self::read_context_switches().unwrap_or(0);
        
        Ok(PerformanceMetrics {
            core_usage,
            cache_misses,
            context_switches,
            interrupts,
            numa_misses,
            timestamp: Instant::now(),
        })
    }
    
    fn read_perf_counter(cpu: usize, event: &str) -> Result<u64> {
        let path = format!("/sys/devices/system/cpu/cpu{}/events/{}", cpu, event);
        if Path::new(&path).exists() {
            let content = fs::read_to_string(&path)?;
            return content.trim().parse::<u64>().context("Failed to parse perf counter");
        }
        Ok(0)
    }
    
    fn read_context_switches() -> Result<u64> {
        let content = fs::read_to_string("/proc/stat")?;
        for line in content.lines() {
            if line.starts_with("ctxt") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    return parts[1].parse::<u64>().context("Failed to parse context switches");
                }
            }
        }
        Ok(0)
    }
    
    fn optimize_assignments(&mut self) -> Result<()> {
        debug!("Running CPU affinity optimization");
        
        // Analyze recent performance
        let avg_metrics = self.calculate_average_metrics();
        
        // Identify hotspots and bottlenecks
        let hot_cores: Vec<usize> = avg_metrics.core_usage.iter()
            .filter(|(_, &usage)| usage > TARGET_CPU_USAGE)
            .map(|(&core, _)| core)
            .collect();
        
        let cold_cores: Vec<usize> = avg_metrics.core_usage.iter()
            .filter(|(_, &usage)| usage < 30.0)
            .map(|(&core, _)| core)
            .collect();
        
        // Rebalance if needed
        if !hot_cores.is_empty() && !cold_cores.is_empty() {
            self.rebalance_threads(&hot_cores, &cold_cores)?;
        }
        
        // Optimize NUMA placement
        self.optimize_numa_placement(&avg_metrics)?;
        
        // Update interrupt affinity based on load
        Self::rebalance_interrupts(&hot_cores, &cold_cores)?;
        
        info!("CPU affinity optimization completed");
        Ok(())
    }
    
    fn calculate_average_metrics(&self) -> PerformanceMetrics {
        let recent_count = 100.min(self.performance_history.len());
        if recent_count == 0 {
            return PerformanceMetrics {
                core_usage: HashMap::new(),
                cache_misses: HashMap::new(),
                context_switches: 0,
                interrupts: HashMap::new(),
                numa_misses: HashMap::new(),
                timestamp: Instant::now(),
            };
        }
        
        let recent = &self.performance_history[self.performance_history.len() - recent_count..];
        
        let mut avg_core_usage = HashMap::new();
        let mut avg_cache_misses = HashMap::new();
        let mut avg_interrupts = HashMap::new();
        let mut avg_numa_misses = HashMap::new();
        let mut total_ctx_switches = 0u64;
        
        for metrics in recent {
            for (&core, &usage) in &metrics.core_usage {
                *avg_core_usage.entry(core).or_insert(0.0) += usage;
            }
            for (&core, &misses) in &metrics.cache_misses {
                *avg_cache_misses.entry(core).or_insert(0) += misses;
            }
            for (&core, &intrs) in &metrics.interrupts {
                *avg_interrupts.entry(core).or_insert(0) += intrs;
            }
            for (&node, &misses) in &metrics.numa_misses {
                *avg_numa_misses.entry(node).or_insert(0) += misses;
            }
            total_ctx_switches += metrics.context_switches;
        }
        
        let count = recent_count as f32;
        for usage in avg_core_usage.values_mut() {
            *usage /= count;
        }
        for misses in avg_cache_misses.values_mut() {
            *misses = (*misses as f32 / count) as u64;
        }
        for intrs in avg_interrupts.values_mut() {
            *intrs = (*intrs as f32 / count) as u64;
        }
        for misses in avg_numa_misses.values_mut() {
            *misses = (*misses as f32 / count) as u64;
        }
        
        PerformanceMetrics {
            core_usage: avg_core_usage,
            cache_misses: avg_cache_misses,
            context_switches: (total_ctx_switches as f32 / count) as u64,
            interrupts: avg_interrupts,
            numa_misses: avg_numa_misses,
            timestamp: Instant::now(),
        }
    }
    
    fn rebalance_threads(&mut self, hot_cores: &[usize], cold_cores: &[usize]) -> Result<()> {
        let mut moves = Vec::new();
        
        for (&thread_id, affinity) in &self.thread_assignments {
            if affinity.core_ids.iter().any(|c| hot_cores.contains(c)) {
                if let Some(&target_core) = cold_cores.first() {
                    if affinity.role != ThreadRole::NetworkReceive && 
                       affinity.role != ThreadRole::NetworkSend {
                        moves.push((thread_id, vec![target_core]));
                    }
                }
            }
        }
        
        for (thread_id, new_cores) in moves {
            if let Some(mut affinity) = self.thread_assignments.get_mut(&thread_id) {
                self.set_thread_affinity(thread_id, &new_cores, affinity.priority, affinity.policy)?;
                affinity.core_ids = new_cores;
                debug!("Rebalanced thread {} to cores {:?}", thread_id, affinity.core_ids);
            }
        }
        
        Ok(())
    }
    
    fn optimize_numa_placement(&mut self, metrics: &PerformanceMetrics) -> Result<()> {
        let topology = self.topology.read();
        
        for (&thread_id, affinity) in &mut self.thread_assignments {
            if let Some(current_node) = affinity.numa_node {
                if let Some(&misses) = metrics.numa_misses.get(&current_node) {
                    if misses > 1000000 {
                        // Find better NUMA node
                        let best_node = topology.numa_nodes.iter()
                            .min_by_key(|n| metrics.numa_misses.get(&n.id).unwrap_or(&0))
                            .map(|n| n.id);
                        
                        if let Some(new_node) = best_node {
                            if new_node != current_node {
                                let new_cores: Vec<usize> = topology.numa_nodes.iter()
                                    .find(|n| n.id == new_node)
                                    .map(|n| n.cores.iter().take(affinity.core_ids.len()).copied().collect())
                                    .unwrap_or_default();
                                
                                if !new_cores.is_empty() {
                                    self.set_thread_affinity(thread_id, &new_cores, affinity.priority, affinity.policy)?;
                                    affinity.core_ids = new_cores;
                                    affinity.numa_node = Some(new_node);
                                    debug!("Moved thread {} to NUMA node {}", thread_id, new_node);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn rebalance_interrupts(hot_cores: &[usize], cold_cores: &[usize]) -> Result<()> {
        if cold_cores.is_empty() {
            return Ok(());
        }
        
        let mut irq_counts = HashMap::new();
        
                        if let Ok(irq_num) = parts[0].trim_end_matches(':').parse::<u32>() {
                    let total: u64 = parts[1..].iter()
                        .filter_map(|s| s.parse::<u64>().ok())
                        .sum();
                    irq_counts.insert(irq_num, total);
                }
            }
        }
        
        // Find high-traffic IRQs on hot cores
        let mut irqs_to_move = Vec::new();
        for (&irq, &count) in &irq_counts {
            if count > 100000 {
                let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq);
                if let Ok(current_affinity) = fs::read_to_string(&affinity_path) {
                    if let Ok(current_cores) = CpuTopology::parse_cpu_list(&format!("/tmp/{}", current_affinity.trim())) {
                        if current_cores.iter().any(|c| hot_cores.contains(c)) {
                            irqs_to_move.push(irq);
                        }
                    }
                }
            }
        }
        
        // Redistribute high-traffic IRQs to cold cores
        for (i, irq) in irqs_to_move.iter().enumerate() {
            let target_core = cold_cores[i % cold_cores.len()];
            let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq);
            
            if let Err(e) = fs::write(&affinity_path, target_core.to_string()) {
                warn!("Failed to rebalance IRQ {} to core {}: {}", irq, target_core, e);
            } else {
                debug!("Rebalanced IRQ {} to core {}", irq, target_core);
            }
        }
        
        Ok(())
    }
    
    pub fn get_thread_cores(&self, thread_id: u64) -> Option<Vec<usize>> {
        OPTIMIZER.read().thread_assignments.get(&thread_id)
            .map(|a| a.core_ids.clone())
    }
    
    pub fn enable_optimization(&self, enable: bool) {
        OPTIMIZER.write().optimization_enabled = enable;
        info!("CPU affinity optimization {}", if enable { "enabled" } else { "disabled" });
    }
    
    pub fn get_performance_stats(&self) -> Option<PerformanceMetrics> {
        let optimizer = OPTIMIZER.read();
        optimizer.performance_history.last().cloned()
    }
    
    pub fn force_optimize(&self) -> Result<()> {
        let mut optimizer = OPTIMIZER.write();
        optimizer.optimize_assignments()?;
        optimizer.last_optimization = Instant::now();
        Ok(())
    }
}

// Public API functions
pub fn initialize() -> Result<()> {
    CpuAffinityOptimizer::initialize()
}

pub fn assign_current_thread(role: ThreadRole) -> Result<()> {
    let thread_id = unsafe { libc::gettid() } as u64;
    let optimizer = OPTIMIZER.read();
    optimizer.assign_thread(thread_id, role)
}

pub fn assign_thread(thread_id: u64, role: ThreadRole) -> Result<()> {
    let optimizer = OPTIMIZER.read();
    optimizer.assign_thread(thread_id, role)
}

pub fn get_optimal_cores(role: ThreadRole) -> Result<Vec<usize>> {
    let optimizer = OPTIMIZER.read();
    let topology = optimizer.topology.read();
    optimizer.select_cores_for_role(role, &topology)
}

pub fn pin_to_cores(cores: &[usize]) -> Result<()> {
    let thread_id = unsafe { libc::gettid() } as u64;
    let optimizer = OPTIMIZER.read();
    optimizer.set_thread_affinity(thread_id, cores, NORMAL_THREAD_PRIORITY, Policy::Normal)
}

pub fn set_realtime_priority(priority: i32) -> Result<()> {
    let pid = Pid::this();
    sched_setscheduler(pid, Policy::Fifo, priority)?;
    Ok(())
}

pub fn get_numa_node_for_cores(cores: &[usize]) -> Option<usize> {
    let topology = CPU_TOPOLOGY.read();
    cores.first().and_then(|&core_id| {
        topology.cores.get(core_id).map(|c| c.numa_node)
    })
}

pub fn optimize_memory_allocation(numa_node: usize) -> Result<()> {
    use libc::{mbind, MPOL_BIND, MPOL_MF_MOVE};
    
    unsafe {
        let mut nodemask: libc::c_ulong = 1 << numa_node;
        let ret = mbind(
            std::ptr::null_mut(),
            0,
            MPOL_BIND,
            &mut nodemask as *mut _ as *mut libc::c_ulong,
            numa_node as libc::c_ulong + 1,
            MPOL_MF_MOVE,
        );
        
        if ret != 0 {
            return Err(anyhow::anyhow!("Failed to set NUMA memory policy"));
        }
    }
    
    Ok(())
}

pub fn prefetch_cache_lines(addresses: &[*const u8]) {
    for &addr in addresses {
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(addr as *const i8);
        }
    }
}

pub fn configure_huge_pages() -> Result<()> {
    use libc::{madvise, MADV_HUGEPAGE};
    
    let page_size = 2 * 1024 * 1024; // 2MB huge pages
    let addr = std::ptr::null_mut();
    
    unsafe {
        let ret = madvise(addr, page_size, MADV_HUGEPAGE);
        if ret != 0 {
            warn!("Failed to enable huge pages");
        }
    }
    
    // Set huge page pool size
    if let Err(e) = fs::write("/proc/sys/vm/nr_hugepages", "1024") {
        warn!("Failed to set huge page pool size: {}", e);
    }
    
    Ok(())
}

pub fn get_performance_stats() -> Option<PerformanceMetrics> {
    let optimizer = OPTIMIZER.read();
    optimizer.get_performance_stats()
}

pub fn enable_optimization(enable: bool) {
    let optimizer = OPTIMIZER.read();
    optimizer.enable_optimization(enable);
}

pub fn force_optimization() -> Result<()> {
    let optimizer = OPTIMIZER.read();
    optimizer.force_optimize()
}

pub fn get_cpu_topology() -> Arc<RwLock<CpuTopology>> {
    CPU_TOPOLOGY.clone()
}

pub fn cleanup() -> Result<()> {
    // Reset CPU governor to default
    CpuAffinityOptimizer::set_cpu_governor("ondemand").ok();
    
    // Re-enable CPU idle states
    let idle_path = "/sys/devices/system/cpu/cpu0/cpuidle";
    if Path::new(idle_path).exists() {
        for entry in fs::read_dir(idle_path)? {
            let entry = entry?;
            if entry.file_name().to_str().unwrap_or("").starts_with("state") {
                let disable_path = entry.path().join("disable");
                if disable_path.exists() {
                    fs::write(&disable_path, "0").ok();
                }
            }
        }
    }
    
    info!("CPU affinity optimizer cleaned up");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cpu_topology_detection() {
        let topology = CpuTopology::detect().unwrap();
        assert!(topology.total_cores > 0);
        assert!(topology.physical_cores > 0);
        assert!(!topology.cores.is_empty());
    }
    
    #[test]
    fn test_core_assignment() {
        let optimizer = CpuAffinityOptimizer::new();
        let topology = optimizer.topology.read();
        
        for role in [
            ThreadRole::NetworkReceive,
            ThreadRole::TransactionProcessing,
            ThreadRole::Simulation,
            ThreadRole::Background,
        ] {
            let cores = optimizer.select_cores_for_role(role, &topology).unwrap();
            assert!(!cores.is_empty());
        }
    }
    
    #[test]
    fn test_hex_mask_conversion() {
        let cores = vec![0, 2, 4, 8];
        let mask = CpuAffinityOptimizer::cores_to_hex_mask(&cores);
        assert_eq!(mask, "00000000000000000000000000000115");
    }
}

