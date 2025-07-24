use std::sync::Arc;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use anyhow::{Result, Context};
use dashmap::DashMap;
use parking_lot::RwLock as ParkingRwLock;
use tch::{nn, nn::Module, Device, Tensor, Kind, Reduction};
use solana_sdk::{
    transaction::Transaction,
    pubkey::Pubkey,
    hash::Hash,
    clock::Slot,
};
use serde::{Deserialize, Serialize};

const MODEL_DIM: i64 = 256;
const NUM_HEADS: i64 = 8;
const NUM_LAYERS: i64 = 4;
const MAX_SEQ_LEN: i64 = 64;
const DROPOUT: f64 = 0.1;
const FEATURE_DIM: i64 = 32;
const CACHE_TTL_MS: u64 = 1000;
const MAX_CACHE_SIZE: usize = 100000;

#[derive(Debug, Clone)]
pub struct TxFeatures {
    pub compute_units: f32,
    pub priority_fee_per_cu: f32,
    pub num_instructions: f32,
    pub num_signers: f32,
    pub num_writable: f32,
    pub num_readonly: f32,
    pub tx_size: f32,
    pub account_contention: f32,
    pub program_diversity: f32,
    pub slot_offset: f32,
    pub recent_success_rate: f32,
    pub network_congestion: f32,
    pub leader_schedule_weight: f32,
    pub instruction_complexity: f32,
    pub has_compute_budget: f32,
    pub priority_percentile: f32,
}

struct TransformerModel {
    embedding: nn::Linear,
    positional_encoding: Tensor,
    encoder_layers: Vec<EncoderLayer>,
    output_projection: nn::Sequential,
    device: Device,
}

struct EncoderLayer {
    self_attention: MultiHeadAttention,
    feed_forward: nn::Sequential,
    norm1: nn::LayerNorm,
    norm2: nn::LayerNorm,
}

struct MultiHeadAttention {
    qkv_proj: nn::Linear,
    output_proj: nn::Linear,
    num_heads: i64,
    head_dim: i64,
}

#[derive(Clone)]
struct CachedPrediction {
    score: f32,
    timestamp: Instant,
    tx_hash: Hash,
}

#[derive(Default, Clone)]
struct AccountStats {
    total_access: u64,
    write_access: u64,
    last_seen: Instant,
    success_count: u64,
    failure_count: u64,
}

#[derive(Default)]
struct NetworkStats {
    avg_compute_units: f32,
    avg_priority_fee: f32,
    congestion_level: f32,
    recent_txs: VecDeque<(Instant, bool, f32)>,
    slot_times: VecDeque<(Slot, Duration)>,
}

pub struct TransformerTxPredictor {
    model: Arc<Mutex<TransformerModel>>,
    vs: Arc<nn::VarStore>,
    cache: Arc<DashMap<Hash, CachedPrediction>>,
    account_stats: Arc<DashMap<Pubkey, AccountStats>>,
    network_stats: Arc<ParkingRwLock<NetworkStats>>,
    program_ids: Arc<HashMap<Pubkey, u8>>,
    device: Device,
}

impl TransformerModel {
    fn new(vs: &nn::Path, device: Device) -> Result<Self> {
        let embedding = nn::linear(vs / "embed", FEATURE_DIM, MODEL_DIM, Default::default());
        let positional_encoding = Self::create_positional_encoding(MAX_SEQ_LEN, MODEL_DIM, device);
        
        let mut encoder_layers = Vec::new();
        for i in 0..NUM_LAYERS {
            encoder_layers.push(EncoderLayer::new(&(vs / format!("layer_{}", i)), MODEL_DIM));
        }
        
        let output_projection = nn::seq()
            .add(nn::linear(vs / "out1", MODEL_DIM, MODEL_DIM / 2, Default::default()))
            .add_fn(|x| x.relu())
            .add(nn::linear(vs / "out2", MODEL_DIM / 2, 1, Default::default()));
        
        Ok(Self {
            embedding,
            positional_encoding,
            encoder_layers,
            output_projection,
            device,
        })
    }

    fn create_positional_encoding(max_len: i64, d_model: i64, device: Device) -> Tensor {
        let mut pe = vec![0.0f32; (max_len * d_model) as usize];
        
        for pos in 0..max_len {
            for i in (0..d_model).step_by(2) {
                let angle = pos as f32 / (10000.0_f32).powf(i as f32 / d_model as f32);
                pe[(pos * d_model + i) as usize] = angle.sin();
                if i + 1 < d_model {
                    pe[(pos * d_model + i + 1) as usize] = angle.cos();
                }
            }
        }
        
        Tensor::of_slice(&pe).view([max_len, d_model]).to(device)
    }

    fn forward(&self, features: &Tensor, training: bool) -> Tensor {
        let batch_size = features.size()[0];
        let seq_len = 1i64;
        
        let x = self.embedding.forward(features);
        let x = x.view([batch_size, seq_len, MODEL_DIM]);
        
        let pos_enc = self.positional_encoding.narrow(0, 0, seq_len);
        let mut x = x + pos_enc.unsqueeze(0);
        
        if training {
            x = x.dropout(DROPOUT, training);
        }
        
        for layer in &self.encoder_layers {
            x = layer.forward(&x, training);
        }
        
        let pooled = x.mean_dim(&[1i64][..], false, Kind::Float);
        self.output_projection.forward(&pooled).sigmoid()
    }
}

impl EncoderLayer {
    fn new(vs: &nn::Path, d_model: i64) -> Self {
        let self_attention = MultiHeadAttention::new(vs / "attn", d_model, NUM_HEADS);
        
        let feed_forward = nn::seq()
            .add(nn::linear(vs / "ff1", d_model, d_model * 4, Default::default()))
            .add_fn(|x| x.gelu("tanh"))
            .add(nn::linear(vs / "ff2", d_model * 4, d_model, Default::default()));
        
        let norm1 = nn::layer_norm(vs / "norm1", vec![d_model], Default::default());
        let norm2 = nn::layer_norm(vs / "norm2", vec![d_model], Default::default());
        
        Self {
            self_attention,
            feed_forward,
            norm1,
            norm2,
        }
    }

    fn forward(&self, x: &Tensor, training: bool) -> Tensor {
        let residual = x;
        let x = self.norm1.forward(x);
        let mut x = self.self_attention.forward(&x);
        
        if training {
            x = x.dropout(DROPOUT, training);
        }
        let x = x + residual;
        
        let residual = &x;
        let normed = self.norm2.forward(&x);
        let mut ff_out = self.feed_forward.forward(&normed);
        
        if training {
            ff_out = ff_out.dropout(DROPOUT, training);
        }
        ff_out + residual
    }
}

impl MultiHeadAttention {
    fn new(vs: &nn::Path, d_model: i64, num_heads: i64) -> Self {
        let head_dim = d_model / num_heads;
        
        Self {
            qkv_proj: nn::linear(vs / "qkv", d_model, d_model * 3, Default::default()),
            output_proj: nn::linear(vs / "out", d_model, d_model, Default::default()),
            num_heads,
            head_dim,
        }
    }

    fn forward(&self, x: &Tensor) -> Tensor {
        let (batch_size, seq_len, _) = x.size3().unwrap();
        
        let qkv = self.qkv_proj.forward(x);
        let qkv = qkv.view([batch_size, seq_len, 3, self.num_heads, self.head_dim]);
        let qkv = qkv.permute(&[2, 0, 3, 1, 4]);
        
        let q = qkv.get(0);
        let k = qkv.get(1);
        let v = qkv.get(2);
        
        let scores = q.matmul(&k.transpose(-2, -1)) / (self.head_dim as f64).sqrt();
        let attn_weights = scores.softmax(-1, Kind::Float);
        
        let context = attn_weights.matmul(&v);
        let context = context.transpose(1, 2).contiguous();
        let context = context.view([batch_size, seq_len, self.num_heads * self.head_dim]);
        
        self.output_proj.forward(&context)
    }
}

impl TransformerTxPredictor {
    pub async fn new(model_path: Option<&str>) -> Result<Self> {
        let device = Device::cuda_if_available();
        let vs = nn::VarStore::new(device);
        
        let model = TransformerModel::new(&vs.root(), device)?;
        
        if let Some(path) = model_path {
            if std::path::Path::new(path).exists() {
                vs.load(path).context("Failed to load model")?;
            }
        }
        
        let program_ids = Self::init_program_ids();
        
        Ok(Self {
            model: Arc::new(Mutex::new(model)),
            vs: Arc::new(vs),
            cache: Arc::new(DashMap::new()),
            account_stats: Arc::new(DashMap::new()),
            network_stats: Arc::new(ParkingRwLock::new(NetworkStats::default())),
            program_ids: Arc::new(program_ids),
            device,
        })
    }

    pub async fn predict(&self, tx: &Transaction, recent_blockhash: &Hash) -> Result<f32> {
        let tx_hash = tx.message.hash();
        
        if let Some(cached) = self.cache.get(&tx_hash) {
            if cached.timestamp.elapsed() < Duration::from_millis(CACHE_TTL_MS) {
                return Ok(cached.score);
            }
        }
        
        let features = self.extract_features(tx, recent_blockhash).await?;
        let input_tensor = self.features_to_tensor(&features)?;
        
        let model = self.model.lock().await;
        let output = tch::no_grad(|| model.forward(&input_tensor, false));
        let score = output.double_value(&[0, 0]) as f32;
        
        self.cache.insert(tx_hash, CachedPrediction {
            score,
            timestamp: Instant::now(),
            tx_hash,
        });
        
        if self.cache.len() > MAX_CACHE_SIZE {
            self.cleanup_cache();
        }
        
        Ok(score)
    }

    async fn extract_features(&self, tx: &Transaction, _blockhash: &Hash) -> Result<TxFeatures> {
        let (compute_units, priority_fee) = self.extract_compute_budget(tx);
        let tx_size = bincode::serialize(tx)?.len();
        
        let num_instructions = tx.message.instructions.len() as f32;
        let num_signers = tx.message.header.num_required_signatures as f32;
        let num_writable = self.count_writable_accounts(tx) as f32;
        let num_readonly = tx.message.header.num_readonly_unsigned_accounts as f32;
        
        let account_contention = self.calculate_account_contention(tx).await;
        let program_diversity = self.calculate_program_diversity(tx);
        let instruction_complexity = self.calculate_instruction_complexity(tx);
        let has_compute_budget = if compute_units > 200_000 { 1.0 } else { 0.0 };
        
        let network_stats = self.network_stats.read();
        let network_congestion = network_stats.congestion_level;
        let recent_success_rate = self.calculate_recent_success_rate(&network_stats);
        let priority_percentile = self.calculate_priority_percentile(priority_fee, &network_stats);
        
        let priority_fee_per_cu = if compute_units > 0 {
            priority_fee as f32 / compute_units as f32
        } else {
            0.0
        };
        
        Ok(TxFeatures {
            compute_units: (compute_units as f32 / 1_400_000.0).min(1.0),
            priority_fee_per_cu: (priority_fee_per_cu * 1000.0).min(1.0),
            num_instructions: (num_instructions / 25.0).min(1.0),
            num_signers: (num_signers / 5.0).min(1.0),
            num_writable: (num_writable / 10.0).min(1.0),
            num_readonly: (num_readonly / 15.0).min(1.0),
            tx_size: (tx_size as f32 / 1232.0).min(1.0),
            account_contention,
            program_diversity,
            slot_offset: 0.0,
            recent_success_rate,
            network_congestion,
            leader_schedule_weight: 0.95,
            instruction_complexity,
            has_compute_budget,
            priority_percentile,
        })
    }

    fn extract_compute_budget(&self, tx: &Transaction) -> (u32, u64) {
        let mut compute_units = 200_000u32;
        let mut priority_fee = 0u64;

                for ix in &tx.message.instructions {
            let program_idx = ix.program_id_index as usize;
            if program_idx >= tx.message.account_keys.len() {
                continue;
            }

            let program_id = &tx.message.account_keys[program_idx];
            if program_id == &solana_sdk::compute_budget::id() {
                if ix.data.len() >= 5 {
                    match ix.data[0] {
                        2 => { // SetComputeUnitLimit
                            if ix.data.len() >= 5 {
                                compute_units = u32::from_le_bytes([
                                    ix.data[1], ix.data[2], ix.data[3], ix.data[4]
                                ]);
                            }
                        },
                        3 => { // SetComputeUnitPrice
                            if ix.data.len() >= 9 {
                                priority_fee = u64::from_le_bytes([
                                    ix.data[1], ix.data[2], ix.data[3], ix.data[4],
                                    ix.data[5], ix.data[6], ix.data[7], ix.data[8]
                                ]);
                            }
                        },
                        _ => {}
                    }
                }
            }
        }

        (compute_units.min(1_400_000), priority_fee)
    }

    fn count_writable_accounts(&self, tx: &Transaction) -> usize {
        let num_signers = tx.message.header.num_required_signatures as usize;
        let readonly_signed = tx.message.header.num_readonly_signed_accounts as usize;
        let readonly_unsigned = tx.message.header.num_readonly_unsigned_accounts as usize;
        
        let total_accounts = tx.message.account_keys.len();
        let writable_signed = num_signers - readonly_signed;
        let writable_unsigned = total_accounts - num_signers - readonly_unsigned;
        
        writable_signed + writable_unsigned
    }

    async fn calculate_account_contention(&self, tx: &Transaction) -> f32 {
        let mut total_score = 0.0f32;
        let mut count = 0;
        
        for (idx, pubkey) in tx.message.account_keys.iter().enumerate() {
            if self.is_writable_account(tx, idx) {
                count += 1;
                
                let mut entry = self.account_stats.entry(*pubkey).or_default();
                let stats = entry.value_mut();
                
                let time_since = stats.last_seen.elapsed().as_millis().max(1) as f32;
                let access_rate = stats.total_access as f32 / (time_since / 1000.0);
                let write_ratio = if stats.total_access > 0 {
                    stats.write_access as f32 / stats.total_access as f32
                } else {
                    0.0
                };
                
                total_score += (access_rate * write_ratio).min(10.0) / 10.0;
                
                stats.total_access += 1;
                stats.write_access += 1;
                stats.last_seen = Instant::now();
            }
        }
        
        if count > 0 {
            (total_score / count as f32).min(1.0)
        } else {
            0.0
        }
    }

    fn is_writable_account(&self, tx: &Transaction, idx: usize) -> bool {
        let num_signers = tx.message.header.num_required_signatures as usize;
        let readonly_signed = tx.message.header.num_readonly_signed_accounts as usize;
        let readonly_unsigned = tx.message.header.num_readonly_unsigned_accounts as usize;
        
        if idx < num_signers {
            idx >= readonly_signed
        } else {
            idx < tx.message.account_keys.len() - readonly_unsigned
        }
    }

    fn calculate_program_diversity(&self, tx: &Transaction) -> f32 {
        let mut unique_programs = std::collections::HashSet::new();
        
        for ix in &tx.message.instructions {
            let program_idx = ix.program_id_index as usize;
            if program_idx < tx.message.account_keys.len() {
                unique_programs.insert(&tx.message.account_keys[program_idx]);
            }
        }
        
        if tx.message.instructions.is_empty() {
            return 0.0;
        }
        
        (unique_programs.len() as f32 / tx.message.instructions.len() as f32).min(1.0)
    }

    fn calculate_instruction_complexity(&self, tx: &Transaction) -> f32 {
        let mut total_complexity = 0.0f32;
        
        for ix in &tx.message.instructions {
            let data_size = (ix.data.len() as f32 / 256.0).min(1.0);
            let account_count = (ix.accounts.len() as f32 / 16.0).min(1.0);
            
            let program_idx = ix.program_id_index as usize;
            let program_weight = if program_idx < tx.message.account_keys.len() {
                let program_id = &tx.message.account_keys[program_idx];
                self.program_ids.get(program_id).copied().unwrap_or(50) as f32 / 100.0
            } else {
                0.5
            };
            
            total_complexity += data_size * 0.3 + account_count * 0.3 + program_weight * 0.4;
        }
        
        if tx.message.instructions.is_empty() {
            return 0.0;
        }
        
        (total_complexity / tx.message.instructions.len() as f32).min(1.0)
    }

    fn calculate_recent_success_rate(&self, stats: &NetworkStats) -> f32 {
        let cutoff = Instant::now() - Duration::from_secs(60);
        let recent: Vec<_> = stats.recent_txs.iter()
            .filter(|(time, _, _)| *time > cutoff)
            .collect();
        
        if recent.is_empty() {
            return 0.5;
        }
        
        let successes = recent.iter().filter(|(_, success, _)| *success).count();
        successes as f32 / recent.len() as f32
    }

    fn calculate_priority_percentile(&self, priority_fee: u64, stats: &NetworkStats) -> f32 {
        if stats.recent_txs.is_empty() {
            return 0.5;
        }
        
        let mut fees: Vec<f32> = stats.recent_txs.iter()
            .map(|(_, _, fee)| *fee)
            .collect();
        
        fees.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let current_fee = priority_fee as f32;
        let position = fees.iter().filter(|&&f| f < current_fee).count();
        
        position as f32 / fees.len() as f32
    }

    fn features_to_tensor(&self, features: &TxFeatures) -> Result<Tensor> {
        let values = vec![
            features.compute_units,
            features.priority_fee_per_cu,
            features.num_instructions,
            features.num_signers,
            features.num_writable,
            features.num_readonly,
            features.tx_size,
            features.account_contention,
            features.program_diversity,
            features.slot_offset,
            features.recent_success_rate,
            features.network_congestion,
            features.leader_schedule_weight,
            features.instruction_complexity,
            features.has_compute_budget,
            features.priority_percentile,
        ];
        
        let mut padded = vec![0.0f32; FEATURE_DIM as usize];
        for (i, &val) in values.iter().enumerate() {
            if i < padded.len() {
                padded[i] = val;
            }
        }
        
        Ok(Tensor::of_slice(&padded)
            .view([1, FEATURE_DIM])
            .to(self.device))
    }

    fn cleanup_cache(&self) {
        let now = Instant::now();
        let ttl = Duration::from_millis(CACHE_TTL_MS);
        
        let mut to_remove = Vec::new();
        for entry in self.cache.iter() {
            if now.duration_since(entry.value().timestamp) > ttl {
                to_remove.push(*entry.key());
            }
        }
        
        for key in to_remove {
            self.cache.remove(&key);
        }
        
        if self.cache.len() > MAX_CACHE_SIZE / 2 {
            let mut entries: Vec<_> = self.cache.iter()
                .map(|e| (*e.key(), e.value().timestamp))
                .collect();
            
            entries.sort_by_key(|(_, time)| *time);
            
            let remove_count = self.cache.len() - MAX_CACHE_SIZE / 2;
            for (hash, _) in entries.into_iter().take(remove_count) {
                self.cache.remove(&hash);
            }
        }
    }

    pub async fn update_network_stats(&self, congestion: f32, priority_fee: f64, success: bool) {
        let mut stats = self.network_stats.write();
        
        stats.congestion_level = congestion;
        stats.recent_txs.push_back((Instant::now(), success, priority_fee as f32));
        
        if stats.recent_txs.len() > 5000 {
            stats.recent_txs.pop_front();
        }
        
        let recent_fees: Vec<f32> = stats.recent_txs.iter()
            .map(|(_, _, fee)| *fee)
            .collect();
        
        if !recent_fees.is_empty() {
            stats.avg_priority_fee = recent_fees.iter().sum::<f32>() / recent_fees.len() as f32;
        }
    }

    pub async fn record_tx_result(&self, tx_hash: &Hash, success: bool) {
        if let Some(cached) = self.cache.get(tx_hash) {
            let prediction = cached.score;
            let accuracy = if success {
                prediction
            } else {
                1.0 - prediction
            };
            
            let mut stats = self.network_stats.write();
            stats.recent_txs.push_back((Instant::now(), success, accuracy));
            
            if stats.recent_txs.len() > 5000 {
                stats.recent_txs.pop_front();
            }
        }
    }

    pub async fn train_batch(&self, samples: Vec<(TxFeatures, bool)>) -> Result<f32> {
        if samples.is_empty() {
            return Ok(0.0);
        }

        self.vs.unfreeze();
        let mut total_loss = 0.0;

        for (features, success) in samples {
            let input = self.features_to_tensor(&features)?;
            let target = Tensor::of_slice(&[if success { 1.0f32 } else { 0.0f32 }])
                .view([1, 1])
                .to(self.device);

            let model = self.model.lock().await;
            let output = model.forward(&input, true);
            
            let loss = output.binary_cross_entropy::<Tensor>(
                &target,
                None,
                Reduction::Mean
            );
            
            self.vs.backward_step(&loss);
            total_loss += f32::from(loss.double_value(&[]));
        }

        self.vs.freeze();
        Ok(total_loss / samples.len() as f32)
    }

    pub async fn save_model(&self, path: &str) -> Result<()> {
        self.vs.save(path)?;
        Ok(())
    }

    pub async fn batch_predict(&self, transactions: Vec<(Transaction, Hash)>) -> Result<Vec<f32>> {
        let mut predictions = Vec::with_capacity(transactions.len());
        
        for (tx, blockhash) in transactions {
            let score = self.predict(&tx, &blockhash).await.unwrap_or(0.0);
            predictions.push(score);
        }
        
        Ok(predictions)
    }

    pub fn get_cache_stats(&self) -> (usize, f32) {
        let size = self.cache.len();
        let now = Instant::now();
        let valid = self.cache.iter()
            .filter(|e| now.duration_since(e.value().timestamp) < Duration::from_millis(CACHE_TTL_MS))
            .count();
        
        let hit_rate = if size > 0 { valid as f32 / size as f32 } else { 0.0 };
        (size, hit_rate)
    }

    fn init_program_ids() -> HashMap<Pubkey, u8> {
        let mut map = HashMap::new();
        
        // System & Core
        map.insert("11111111111111111111111111111111".parse().unwrap(), 1);
        map.insert("ComputeBudget111111111111111111111111111111".parse().unwrap(), 2);
        
        // Token Programs
        map.insert("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".parse().unwrap(), 10);
        map.insert("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL".parse().unwrap(), 11);
        
        // DEXes
        map.insert("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".parse().unwrap(), 20);
        map.insert("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".parse().unwrap(), 22);
        map.insert("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".parse().unwrap(), 25);
        
                map
    }

    pub async fn clear_cache(&self) {
        self.cache.clear();
    }

    pub async fn reset_stats(&self) {
        self.account_stats.clear();
        let mut stats = self.network_stats.write();
        *stats = NetworkStats::default();
    }

    pub async fn get_model_confidence(&self) -> f32 {
        let stats = self.network_stats.read();
        let recent_success_rate = self.calculate_recent_success_rate(&stats);
        let (_, cache_hit_rate) = self.get_cache_stats();
        
        (recent_success_rate * 0.7 + cache_hit_rate * 0.3).max(0.1).min(0.99)
    }
}

impl Clone for TransformerTxPredictor {
    fn clone(&self) -> Self {
        Self {
            model: Arc::clone(&self.model),
            vs: Arc::clone(&self.vs),
            cache: Arc::clone(&self.cache),
            account_stats: Arc::clone(&self.account_stats),
            network_stats: Arc::clone(&self.network_stats),
            program_ids: Arc::clone(&self.program_ids),
            device: self.device,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        signature::{Keypair, Signer},
        system_instruction,
        message::Message,
    };

    #[tokio::test]
    async fn test_predictor_creation() {
        let predictor = TransformerTxPredictor::new(None).await;
        assert!(predictor.is_ok());
    }

    #[tokio::test]
    async fn test_prediction() {
        let predictor = TransformerTxPredictor::new(None).await.unwrap();
        
        let payer = Keypair::new();
        let to = Keypair::new();
        let blockhash = Hash::default();
        
        let ix = system_instruction::transfer(&payer.pubkey(), &to.pubkey(), 1_000_000);
        let message = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], message, blockhash);
        
        let score = predictor.predict(&tx, &blockhash).await.unwrap();
        assert!(score >= 0.0 && score <= 1.0);
    }

    #[tokio::test]
    async fn test_cache() {
        let predictor = TransformerTxPredictor::new(None).await.unwrap();
        
        let payer = Keypair::new();
        let to = Keypair::new();
        let blockhash = Hash::default();
        
        let ix = system_instruction::transfer(&payer.pubkey(), &to.pubkey(), 1_000_000);
        let message = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], message, blockhash);
        
        let start = Instant::now();
        let score1 = predictor.predict(&tx, &blockhash).await.unwrap();
        let first_time = start.elapsed();
        
        let start = Instant::now();
        let score2 = predictor.predict(&tx, &blockhash).await.unwrap();
        let cached_time = start.elapsed();
        
        assert_eq!(score1, score2);
        assert!(cached_time < first_time);
    }

    #[tokio::test]
    async fn test_batch_predict() {
        let predictor = TransformerTxPredictor::new(None).await.unwrap();
        let mut txs = Vec::new();
        
        for _ in 0..3 {
            let payer = Keypair::new();
            let to = Keypair::new();
            let blockhash = Hash::new_unique();
            
            let ix = system_instruction::transfer(&payer.pubkey(), &to.pubkey(), 1_000_000);
            let message = Message::new(&[ix], Some(&payer.pubkey()));
            let tx = Transaction::new(&[&payer], message, blockhash);
            
            txs.push((tx, blockhash));
        }
        
        let scores = predictor.batch_predict(txs).await.unwrap();
        assert_eq!(scores.len(), 3);
        for score in scores {
            assert!(score >= 0.0 && score <= 1.0);
        }
    }

    #[tokio::test]
    async fn test_feature_extraction() {
        let predictor = TransformerTxPredictor::new(None).await.unwrap();
        
        let payer = Keypair::new();
        let to = Keypair::new();
        let blockhash = Hash::default();
        
        let ix = system_instruction::transfer(&payer.pubkey(), &to.pubkey(), 1_000_000);
        let message = Message::new(&[ix], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], message, blockhash);
        
        let features = predictor.extract_features(&tx, &blockhash).await.unwrap();
        
        assert!(features.compute_units >= 0.0 && features.compute_units <= 1.0);
        assert!(features.num_instructions > 0.0);
        assert!(features.num_signers > 0.0);
    }

    #[tokio::test]
    async fn test_network_stats() {
        let predictor = TransformerTxPredictor::new(None).await.unwrap();
        
        predictor.update_network_stats(0.7, 1000, true).await;
        predictor.update_network_stats(0.8, 2000, false).await;
        predictor.update_network_stats(0.9, 1500, true).await;
        
        let confidence = predictor.get_model_confidence().await;
        assert!(confidence > 0.0 && confidence < 1.0);
    }

    #[tokio::test]
    async fn test_model_save_load() {
        let predictor = TransformerTxPredictor::new(None).await.unwrap();
        let path = "/tmp/test_transformer_model.pt";
        
        predictor.save_model(path).await.unwrap();
        
        let loaded = TransformerTxPredictor::new(Some(path)).await;
        assert!(loaded.is_ok());
        
        std::fs::remove_file(path).ok();
    }
}

