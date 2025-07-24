use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use rayon::prelude::*;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::algo::{dijkstra, strongly_connected_components};
use petgraph::visit::EdgeRef;
use ndarray::{Array1, Array2, ArrayView1};
use ordered_float::OrderedFloat;

const MAX_GRAPH_NODES: usize = 10000;
const MAX_EDGES_PER_NODE: usize = 100;
const GNN_LAYERS: usize = 3;
const EMBEDDING_DIM: usize = 128;
const ATTENTION_HEADS: usize = 8;
const FLOW_DECAY_FACTOR: f64 = 0.95;
const MIN_PROFIT_THRESHOLD: f64 = 0.001;
const MAX_PATH_LENGTH: usize = 5;
const FLOW_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
const CRITICAL_FLOW_THRESHOLD: f64 = 0.8;
const ANOMALY_DETECTION_WINDOW: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionNode {
    pub address: [u8; 32],
    pub program_id: [u8; 32],
    pub timestamp: u64,
    pub volume: u64,
    pub gas_used: u64,
    pub success_rate: f64,
    pub interaction_count: u64,
    pub mev_exposure: f64,
}

#[derive(Debug, Clone)]
pub struct FlowEdge {
    pub weight: f64,
    pub volume: u64,
    pub frequency: u64,
    pub avg_profit: f64,
    pub volatility: f64,
    pub last_update: Instant,
    pub flow_type: FlowType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowType {
    Swap,
    Liquidation,
    Arbitrage,
    FlashLoan,
    CrossProgram,
}

#[derive(Debug, Clone)]
pub struct GNNLayer {
    pub weight_matrix: Array2<f64>,
    pub attention_weights: Array2<f64>,
    pub bias: Array1<f64>,
    pub dropout_rate: f64,
}

#[derive(Debug)]
pub struct GraphNeuralNetworkFlowAnalyzer {
    graph: Arc<RwLock<DiGraph<TransactionNode, FlowEdge>>>,
    node_embeddings: Arc<RwLock<HashMap<NodeIndex, Array1<f64>>>>,
    gnn_layers: Vec<GNNLayer>,
    flow_patterns: Arc<RwLock<HashMap<Vec<NodeIndex>, FlowPattern>>>,
    anomaly_detector: AnomalyDetector,
    path_cache: Arc<RwLock<HashMap<(NodeIndex, NodeIndex), Vec<NodeIndex>>>>,
    centrality_scores: Arc<RwLock<HashMap<NodeIndex, f64>>>,
    community_detection: Arc<RwLock<HashMap<NodeIndex, usize>>>,
}

#[derive(Debug, Clone)]
pub struct FlowPattern {
    pub nodes: Vec<NodeIndex>,
    pub total_volume: u64,
    pub avg_profit: f64,
    pub frequency: u64,
    pub last_seen: Instant,
    pub mev_probability: f64,
    pub risk_score: f64,
}

#[derive(Debug)]
pub struct AnomalyDetector {
    flow_history: VecDeque<FlowMetrics>,
    mean_volume: f64,
    std_volume: f64,
    mean_frequency: f64,
    std_frequency: f64,
}

#[derive(Debug, Clone)]
pub struct FlowMetrics {
    pub timestamp: Instant,
    pub volume: u64,
    pub frequency: u64,
    pub unique_paths: usize,
    pub avg_path_length: f64,
}

#[derive(Debug, Clone)]
pub struct MEVOpportunity {
    pub path: Vec<NodeIndex>,
    pub expected_profit: f64,
    pub confidence: f64,
    pub risk_score: f64,
    pub execution_time: Duration,
    pub gas_estimate: u64,
}

impl GraphNeuralNetworkFlowAnalyzer {
    pub fn new() -> Self {
        let gnn_layers = (0..GNN_LAYERS)
            .map(|i| {
                let input_dim = if i == 0 { EMBEDDING_DIM } else { EMBEDDING_DIM };
                GNNLayer {
                    weight_matrix: Array2::from_shape_fn((input_dim, EMBEDDING_DIM), |_| {
                        rand::random::<f64>() * 0.1 - 0.05
                    }),
                    attention_weights: Array2::from_shape_fn((EMBEDDING_DIM, ATTENTION_HEADS), |_| {
                        rand::random::<f64>() * 0.1
                    }),
                    bias: Array1::from_shape_fn(EMBEDDING_DIM, |_| 0.01),
                    dropout_rate: 0.1,
                }
            })
            .collect();

        Self {
            graph: Arc::new(RwLock::new(DiGraph::new())),
            node_embeddings: Arc::new(RwLock::new(HashMap::new())),
            gnn_layers,
            flow_patterns: Arc::new(RwLock::new(HashMap::new())),
            anomaly_detector: AnomalyDetector::new(),
            path_cache: Arc::new(RwLock::new(HashMap::new())),
            centrality_scores: Arc::new(RwLock::new(HashMap::new())),
            community_detection: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_transaction(&self, from: [u8; 32], to: [u8; 32], details: TransactionDetails) -> Result<(), String> {
        let mut graph = self.graph.write().map_err(|_| "Failed to acquire graph lock")?;
        
        if graph.node_count() >= MAX_GRAPH_NODES {
            self.prune_old_nodes(&mut graph)?;
        }

        let from_node = self.get_or_create_node(&mut graph, from, details.program_id);
        let to_node = self.get_or_create_node(&mut graph, to, details.program_id);

        let edge_exists = graph.find_edge(from_node, to_node);
        match edge_exists {
            Some(edge) => {
                let edge_weight = graph.edge_weight_mut(edge).ok_or("Edge not found")?;
                edge_weight.volume = edge_weight.volume.saturating_add(details.volume);
                edge_weight.frequency += 1;
                edge_weight.weight = self.calculate_edge_weight(edge_weight);
                edge_weight.avg_profit = (edge_weight.avg_profit * (edge_weight.frequency - 1) as f64 
                    + details.profit) / edge_weight.frequency as f64;
                edge_weight.volatility = self.update_volatility(edge_weight.volatility, details.profit);
                edge_weight.last_update = Instant::now();
            }
            None => {
                let flow_edge = FlowEdge {
                    weight: details.volume as f64 / 1e9,
                    volume: details.volume,
                    frequency: 1,
                    avg_profit: details.profit,
                    volatility: 0.0,
                    last_update: Instant::now(),
                    flow_type: details.flow_type,
                };
                graph.add_edge(from_node, to_node, flow_edge);
            }
        }

        drop(graph);
        self.update_embeddings(vec![from_node, to_node])?;
        self.detect_flow_patterns()?;
        
        Ok(())
    }

    pub fn analyze_mev_opportunities(&self) -> Result<Vec<MEVOpportunity>, String> {
        let graph = self.graph.read().map_err(|_| "Failed to acquire graph lock")?;
        let embeddings = self.node_embeddings.read().map_err(|_| "Failed to acquire embeddings lock")?;
        let patterns = self.flow_patterns.read().map_err(|_| "Failed to acquire patterns lock")?;
        
        let mut opportunities = Vec::new();
        
        let high_value_nodes: Vec<NodeIndex> = graph
            .node_indices()
            .filter(|&node| {
                graph.node_weight(node)
                    .map(|n| n.volume > 1_000_000_000_000 && n.mev_exposure > 0.5)
                    .unwrap_or(false)
            })
            .collect();

        for &source in &high_value_nodes {
            for &target in &high_value_nodes {
                if source == target { continue; }
                
                let paths = self.find_profitable_paths(&graph, source, target, MAX_PATH_LENGTH)?;
                
                for path in paths {
                    if path.len() < 2 { continue; }
                    
                    let path_embedding = self.compute_path_embedding(&path, &embeddings)?;
                    let (profit, confidence) = self.predict_mev_profit(&path, &graph, &path_embedding)?;
                    
                    if profit > MIN_PROFIT_THRESHOLD {
                        let risk_score = self.calculate_risk_score(&path, &graph, &patterns)?;
                        let gas_estimate = self.estimate_gas_cost(&path, &graph)?;
                        
                        opportunities.push(MEVOpportunity {
                            path: path.clone(),
                            expected_profit: profit,
                            confidence,
                            risk_score,
                            execution_time: Duration::from_micros((path.len() as u64 * 50)),
                            gas_estimate,
                        });
                    }
                }
            }
        }
        
        opportunities.sort_by_key(|o| OrderedFloat(-o.expected_profit / (1.0 + o.risk_score)));
        opportunities.truncate(100);
        
        Ok(opportunities)
    }

    fn get_or_create_node(&self, graph: &mut DiGraph<TransactionNode, FlowEdge>, 
                          address: [u8; 32], program_id: [u8; 32]) -> NodeIndex {
        let existing = graph.node_indices().find(|&idx| {
            graph.node_weight(idx).map(|n| n.address == address).unwrap_or(false)
        });
        
        existing.unwrap_or_else(|| {
            graph.add_node(TransactionNode {
                address,
                program_id,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                volume: 0,
                gas_used: 0,
                success_rate: 1.0,
                interaction_count: 0,
                mev_exposure: 0.0,
            })
        })
    }

    fn update_embeddings(&self, nodes: Vec<NodeIndex>) -> Result<(), String> {
        let graph = self.graph.read().map_err(|_| "Failed to acquire graph lock")?;
        let mut embeddings = self.node_embeddings.write().map_err(|_| "Failed to acquire embeddings lock")?;
        
        for node in nodes {
            let neighbors: Vec<NodeIndex> = graph.neighbors(node).collect();
            let mut node_features = Array1::zeros(EMBEDDING_DIM);
            
            if let Some(node_data) = graph.node_weight(node) {
                node_features[0] = (node_data.volume as f64).ln() / 50.0;
                node_features[1] = node_data.success_rate;
                node_features[2] = (node_data.interaction_count as f64).ln() / 10.0;
                node_features[3] = node_data.mev_exposure;
                node_features[4] = (node_data.gas_used as f64) / 1e9;
            }
            
            for (layer_idx, layer) in self.gnn_layers.iter().enumerate() {
                let aggregated = self.aggregate_neighbor_features(
                    &graph, node, &neighbors, &embeddings, layer_idx
                )?;
                
                node_features = self.apply_gnn_layer(node_features.view(), aggregated.view(), layer)?;
            }
            
            embeddings.insert(node, node_features);
        }
        
        Ok(())
    }

    fn aggregate_neighbor_features(&self, graph: &DiGraph<TransactionNode, FlowEdge>,
                                  node: NodeIndex, neighbors: &[NodeIndex],
                                  embeddings: &HashMap<NodeIndex, Array1<f64>>,
                                  layer_idx: usize) -> Result<Array1<f64>, String> {
        let mut aggregated = Array1::zeros(EMBEDDING_DIM);
        let mut total_weight = 0.0;
        
        for &neighbor in neighbors {
            if let Some(edge) = graph.find_edge(neighbor, node) {
                if let Some(edge_data) = graph.edge_weight(edge) {
                    let weight = edge_data.weight * (1.0 + edge_data.avg_profit).max(0.1);
                    
                    if let Some(neighbor_embedding) = embeddings.get(&neighbor) {
                        aggregated = aggregated + neighbor_embedding * weight;
                        total_weight += weight;
                    }
                }
            }
        }
        
        if total_weight > 0.0 {
            aggregated /= total_weight;
        }
        
        Ok(aggregated)
    }

    fn apply_gnn_layer(&self, node_features: ArrayView1<f64>, 
                      aggregated: ArrayView1<f64>, 
                      layer: &GNNLayer) -> Result<Array1<f64>, String> {
        let combined = &node_features + &aggregated;
        let mut output = layer.weight_matrix.dot(&combined) + &layer.bias;
        
        output.mapv_inplace(|x| x.max(0.0));
        
        let dropout_mask: Array1<f64> = Array1::from_shape_fn(EMBEDDING_DIM, |_| {
                        if rand::random::<f64>() > layer.dropout_rate { 1.0 } else { 0.0 }
        });
        
        output *= &dropout_mask;
        output /= 1.0 - layer.dropout_rate;
        
        Ok(output)
    }

    fn detect_flow_patterns(&self) -> Result<(), String> {
        let graph = self.graph.read().map_err(|_| "Failed to acquire graph lock")?;
        let mut patterns = self.flow_patterns.write().map_err(|_| "Failed to acquire patterns lock")?;
        
        let strong_components = strongly_connected_components(&*graph);
        
        for component in strong_components.iter().filter(|c| c.len() >= 3) {
            let subgraph_paths = self.extract_cyclic_paths(&graph, component)?;
            
            for path in subgraph_paths {
                let path_key = path.clone();
                let pattern = patterns.entry(path_key).or_insert_with(|| FlowPattern {
                    nodes: path.clone(),
                    total_volume: 0,
                    avg_profit: 0.0,
                    frequency: 0,
                    last_seen: Instant::now(),
                    mev_probability: 0.0,
                    risk_score: 0.0,
                });
                
                pattern.frequency += 1;
                pattern.last_seen = Instant::now();
                
                let (volume, profit) = self.calculate_path_metrics(&graph, &path)?;
                pattern.total_volume = pattern.total_volume.saturating_add(volume);
                pattern.avg_profit = (pattern.avg_profit * (pattern.frequency - 1) as f64 + profit) 
                    / pattern.frequency as f64;
                
                pattern.mev_probability = self.calculate_mev_probability(&graph, &path)?;
                pattern.risk_score = self.calculate_pattern_risk(&graph, &path)?;
            }
        }
        
        patterns.retain(|_, pattern| {
            pattern.last_seen.elapsed() < Duration::from_secs(300) &&
            pattern.frequency > 2 &&
            pattern.avg_profit > MIN_PROFIT_THRESHOLD
        });
        
        Ok(())
    }

    fn extract_cyclic_paths(&self, graph: &DiGraph<TransactionNode, FlowEdge>, 
                           component: &[NodeIndex]) -> Result<Vec<Vec<NodeIndex>>, String> {
        let mut paths = Vec::new();
        
        for &start in component {
            let mut visited = HashSet::new();
            let mut current_path = vec![start];
            visited.insert(start);
            
            self.dfs_cycles(graph, start, start, &mut current_path, &mut visited, &mut paths, 0)?;
        }
        
        paths.dedup();
        paths.truncate(50);
        
        Ok(paths)
    }

    fn dfs_cycles(&self, graph: &DiGraph<TransactionNode, FlowEdge>,
                  start: NodeIndex, current: NodeIndex,
                  path: &mut Vec<NodeIndex>, visited: &mut HashSet<NodeIndex>,
                  paths: &mut Vec<Vec<NodeIndex>>, depth: usize) -> Result<(), String> {
        if depth > MAX_PATH_LENGTH {
            return Ok(());
        }
        
        for neighbor in graph.neighbors(current) {
            if neighbor == start && path.len() >= 3 {
                paths.push(path.clone());
            } else if !visited.contains(&neighbor) && path.len() < MAX_PATH_LENGTH {
                visited.insert(neighbor);
                path.push(neighbor);
                
                self.dfs_cycles(graph, start, neighbor, path, visited, paths, depth + 1)?;
                
                path.pop();
                visited.remove(&neighbor);
            }
        }
        
        Ok(())
    }

    fn find_profitable_paths(&self, graph: &DiGraph<TransactionNode, FlowEdge>,
                            source: NodeIndex, target: NodeIndex, 
                            max_length: usize) -> Result<Vec<Vec<NodeIndex>>, String> {
        let mut paths = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back((vec![source], 0.0, 0));
        
        while let Some((path, profit, length)) = queue.pop_front() {
            if length >= max_length {
                continue;
            }
            
            let current = *path.last().unwrap();
            
            for edge in graph.edges(current) {
                let next = edge.target();
                
                if path.contains(&next) && next != target {
                    continue;
                }
                
                let edge_data = edge.weight();
                let new_profit = profit + edge_data.avg_profit - (edge_data.volatility * 0.1);
                
                let mut new_path = path.clone();
                new_path.push(next);
                
                if next == target && new_profit > MIN_PROFIT_THRESHOLD {
                    paths.push(new_path);
                } else if length + 1 < max_length {
                    queue.push_back((new_path, new_profit, length + 1));
                }
            }
        }
        
        paths.sort_by(|a, b| {
            let profit_a = self.calculate_path_profit(graph, a).unwrap_or(0.0);
            let profit_b = self.calculate_path_profit(graph, b).unwrap_or(0.0);
            profit_b.partial_cmp(&profit_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        paths.truncate(10);
        Ok(paths)
    }

    fn calculate_path_profit(&self, graph: &DiGraph<TransactionNode, FlowEdge>, 
                           path: &[NodeIndex]) -> Result<f64, String> {
        let mut profit = 0.0;
        
        for window in path.windows(2) {
            if let Some(edge) = graph.find_edge(window[0], window[1]) {
                if let Some(edge_data) = graph.edge_weight(edge) {
                    profit += edge_data.avg_profit * (edge_data.frequency as f64).ln().max(1.0);
                    profit *= 1.0 - (edge_data.volatility * 0.05);
                }
            }
        }
        
        Ok(profit)
    }

    fn compute_path_embedding(&self, path: &[NodeIndex], 
                            embeddings: &HashMap<NodeIndex, Array1<f64>>) -> Result<Array1<f64>, String> {
        let mut path_embedding = Array1::zeros(EMBEDDING_DIM);
        let mut count = 0.0;
        
        for (i, &node) in path.iter().enumerate() {
            if let Some(node_embedding) = embeddings.get(&node) {
                let weight = 1.0 / (1.0 + i as f64 * 0.1);
                path_embedding = path_embedding + node_embedding * weight;
                count += weight;
            }
        }
        
        if count > 0.0 {
            path_embedding /= count;
        }
        
        Ok(path_embedding)
    }

    fn predict_mev_profit(&self, path: &[NodeIndex], 
                         graph: &DiGraph<TransactionNode, FlowEdge>,
                         path_embedding: &Array1<f64>) -> Result<(f64, f64), String> {
        let base_profit = self.calculate_path_profit(graph, path)?;
        
        let embedding_signal = path_embedding.iter()
            .enumerate()
            .filter(|(i, _)| *i < 10)
            .map(|(_, &v)| v)
            .sum::<f64>() / 10.0;
        
        let mut edge_quality = 1.0;
        for window in path.windows(2) {
            if let Some(edge) = graph.find_edge(window[0], window[1]) {
                if let Some(edge_data) = graph.edge_weight(edge) {
                    edge_quality *= (edge_data.frequency as f64).ln().max(1.0) / 10.0;
                    edge_quality *= 1.0 + edge_data.weight / 100.0;
                }
            }
        }
        
        let temporal_factor = self.calculate_temporal_advantage(graph, path)?;
        let competition_factor = self.estimate_competition_level(graph, path)?;
        
        let predicted_profit = base_profit * edge_quality * temporal_factor * (1.0 - competition_factor);
        let confidence = (edge_quality * temporal_factor).min(0.95) * (1.0 + embedding_signal).min(1.0);
        
        Ok((predicted_profit, confidence))
    }

    fn calculate_risk_score(&self, path: &[NodeIndex], 
                          graph: &DiGraph<TransactionNode, FlowEdge>,
                          patterns: &HashMap<Vec<NodeIndex>, FlowPattern>) -> Result<f64, String> {
        let mut risk = 0.0;
        
        for window in path.windows(2) {
            if let Some(edge) = graph.find_edge(window[0], window[1]) {
                if let Some(edge_data) = graph.edge_weight(edge) {
                    risk += edge_data.volatility * 0.3;
                    
                    if edge_data.frequency < 10 {
                        risk += 0.2;
                    }
                    
                    if edge_data.last_update.elapsed() > Duration::from_secs(60) {
                        risk += 0.1;
                    }
                }
            }
        }
        
        if let Some(pattern) = patterns.get(path) {
            risk *= 1.0 - (pattern.frequency as f64).ln().max(1.0) / 20.0;
            risk += pattern.risk_score * 0.5;
        } else {
            risk += 0.3;
        }
        
        let path_length_penalty = (path.len() as f64 - 2.0) * 0.1;
        risk = (risk + path_length_penalty).min(1.0);
        
        Ok(risk)
    }

    fn estimate_gas_cost(&self, path: &[NodeIndex], 
                        graph: &DiGraph<TransactionNode, FlowEdge>) -> Result<u64, String> {
        let mut gas_cost = 5000u64;
        
        for &node in path {
            if let Some(node_data) = graph.node_weight(node) {
                gas_cost += match self.classify_program(node_data.program_id) {
                    ProgramType::Serum => 80000,
                    ProgramType::Raydium => 120000,
                    ProgramType::Orca => 100000,
                    ProgramType::Token => 30000,
                    ProgramType::Unknown => 150000,
                };
            }
        }
        
        gas_cost += (path.len() as u64 - 1) * 10000;
        
        Ok(gas_cost)
    }

    fn classify_program(&self, program_id: [u8; 32]) -> ProgramType {
        const SERUM_V3: [u8; 32] = [0x9b, 0xd6, 0x50, 0x27, 0xc6, 0xa8, 0xda, 0x88, 
                                     0x58, 0x32, 0x34, 0x85, 0x65, 0xbb, 0x91, 0xa3,
                                     0xf8, 0xb4, 0x60, 0xdd, 0xfb, 0x4a, 0x2e, 0xba,
                                     0xd9, 0x0b, 0x5e, 0x22, 0xcd, 0x93, 0x5f, 0xeb];
        
        if program_id == SERUM_V3 {
            ProgramType::Serum
        } else if program_id[0] == 0x67 && program_id[1] == 0x5b {
            ProgramType::Raydium
        } else if program_id[0] == 0x9b && program_id[1] == 0x12 {
            ProgramType::Orca
        } else if program_id[0] == 0x06 && program_id[1] == 0xdf {
            ProgramType::Token
        } else {
            ProgramType::Unknown
        }
    }

    fn calculate_temporal_advantage(&self, graph: &DiGraph<TransactionNode, FlowEdge>,
                                   path: &[NodeIndex]) -> Result<f64, String> {
        let mut advantage = 1.0;
        let now = Instant::now();
        
        for window in path.windows(2) {
            if let Some(edge) = graph.find_edge(window[0], window[1]) {
                if let Some(edge_data) = graph.edge_weight(edge) {
                    let age = edge_data.last_update.elapsed().as_secs_f64();
                    advantage *= (-age / 30.0).exp();
                }
            }
        }
        
        Ok(advantage)
    }

    fn estimate_competition_level(&self, graph: &DiGraph<TransactionNode, FlowEdge>,
                                 path: &[NodeIndex]) -> Result<f64, String> {
        let mut competition = 0.0;
        
        for &node in path {
            let in_degree = graph.neighbors_directed(node, petgraph::Incoming).count();
            let out_degree = graph.neighbors_directed(node, petgraph::Outgoing).count();
            
            competition += (in_degree + out_degree) as f64 / 100.0;
        }
        
        competition = (competition / path.len() as f64).min(0.9);
        Ok(competition)
    }

    fn calculate_edge_weight(&self, edge: &FlowEdge) -> f64 {
        let volume_weight = (edge.volume as f64 / 1e9).ln().max(1.0);
        let frequency_weight = (edge.frequency as f64).ln().max(1.0);
        let profit_weight = (1.0 + edge.avg_profit * 100.0).max(0.1);
        let recency_weight = (-edge.last_update.elapsed().as_secs_f64() / 3600.0).exp();
        
        volume_weight * frequency_weight * profit_weight * recency_weight
    }

    fn update_volatility(&self, current_volatility: f64, new_profit: f64) -> f64 {
        let alpha = 0.1;
        current_volatility * (1.0 - alpha) + (new_profit.abs() * alpha)
    }

        fn calculate_mev_probability(&self, graph: &DiGraph<TransactionNode, FlowEdge>,
                                path: &[NodeIndex]) -> Result<f64, String> {
        let mut probability = 1.0;
        
        for (i, &node) in path.iter().enumerate() {
            if let Some(node_data) = graph.node_weight(node) {
                probability *= node_data.success_rate;
                probability *= 1.0 / (1.0 + node_data.mev_exposure * 2.0);
                
                if i > 0 {
                    let prev_node = path[i - 1];
                    if let Some(edge) = graph.find_edge(prev_node, node) {
                        if let Some(edge_data) = graph.edge_weight(edge) {
                            let edge_reliability = (edge_data.frequency as f64).ln().max(1.0) / 10.0;
                            probability *= edge_reliability.min(0.95);
                        }
                    }
                }
            }
        }
        
        Ok(probability.max(0.0).min(1.0))
    }

    fn calculate_pattern_risk(&self, graph: &DiGraph<TransactionNode, FlowEdge>,
                             path: &[NodeIndex]) -> Result<f64, String> {
        let mut risk = 0.0;
        let mut max_volatility = 0.0;
        
        for window in path.windows(2) {
            if let Some(edge) = graph.find_edge(window[0], window[1]) {
                if let Some(edge_data) = graph.edge_weight(edge) {
                    max_volatility = max_volatility.max(edge_data.volatility);
                    
                    match edge_data.flow_type {
                        FlowType::FlashLoan => risk += 0.3,
                        FlowType::Liquidation => risk += 0.25,
                        FlowType::Arbitrage => risk += 0.1,
                        FlowType::Swap => risk += 0.05,
                        FlowType::CrossProgram => risk += 0.15,
                    }
                }
            }
        }
        
        risk += max_volatility * 0.5;
        risk = (risk / path.len() as f64).min(1.0);
        
        Ok(risk)
    }

    fn calculate_path_metrics(&self, graph: &DiGraph<TransactionNode, FlowEdge>,
                             path: &[NodeIndex]) -> Result<(u64, f64), String> {
        let mut total_volume = 0u64;
        let mut total_profit = 0.0;
        
        for window in path.windows(2) {
            if let Some(edge) = graph.find_edge(window[0], window[1]) {
                if let Some(edge_data) = graph.edge_weight(edge) {
                    total_volume = total_volume.saturating_add(edge_data.volume);
                    total_profit += edge_data.avg_profit;
                }
            }
        }
        
        Ok((total_volume, total_profit))
    }

    fn prune_old_nodes(&self, graph: &mut DiGraph<TransactionNode, FlowEdge>) -> Result<(), String> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let nodes_to_remove: Vec<NodeIndex> = graph
            .node_indices()
            .filter(|&node| {
                graph.node_weight(node)
                    .map(|n| {
                        let age = current_time.saturating_sub(n.timestamp);
                        let low_activity = n.interaction_count < 10 && n.volume < 1_000_000_000;
                        age > 3600 && low_activity
                    })
                    .unwrap_or(true)
            })
            .collect();
        
        for node in nodes_to_remove {
            graph.remove_node(node);
        }
        
        Ok(())
    }

    pub fn update_centrality_scores(&self) -> Result<(), String> {
        let graph = self.graph.read().map_err(|_| "Failed to acquire graph lock")?;
        let mut scores = self.centrality_scores.write().map_err(|_| "Failed to acquire centrality lock")?;
        
        scores.clear();
        
        for node in graph.node_indices() {
            let in_degree = graph.neighbors_directed(node, petgraph::Incoming).count() as f64;
            let out_degree = graph.neighbors_directed(node, petgraph::Outgoing).count() as f64;
            
            let mut edge_weight_sum = 0.0;
            for edge in graph.edges(node) {
                edge_weight_sum += edge.weight().weight;
            }
            
            let centrality = (in_degree + out_degree + 1.0).ln() * edge_weight_sum.ln().max(1.0);
            scores.insert(node, centrality);
        }
        
        Ok(())
    }

    pub fn detect_communities(&self) -> Result<(), String> {
        let graph = self.graph.read().map_err(|_| "Failed to acquire graph lock")?;
        let mut communities = self.community_detection.write().map_err(|_| "Failed to acquire community lock")?;
        
        let components = strongly_connected_components(&*graph);
        
        for (community_id, component) in components.iter().enumerate() {
            for &node in component {
                communities.insert(node, community_id);
            }
        }
        
        Ok(())
    }

    pub fn get_flow_metrics(&self) -> FlowMetrics {
        let graph = self.graph.read().unwrap();
        let patterns = self.flow_patterns.read().unwrap();
        
        let volume: u64 = graph.edge_references()
            .map(|e| e.weight().volume)
            .sum();
        
        let frequency: u64 = graph.edge_references()
            .map(|e| e.weight().frequency)
            .sum();
        
        let unique_paths = patterns.len();
        
        let avg_path_length = if unique_paths > 0 {
            patterns.values()
                .map(|p| p.nodes.len() as f64)
                .sum::<f64>() / unique_paths as f64
        } else {
            0.0
        };
        
        FlowMetrics {
            timestamp: Instant::now(),
            volume,
            frequency,
            unique_paths,
            avg_path_length,
        }
    }

    pub fn analyze_anomalies(&mut self) -> Vec<NodeIndex> {
        let graph = self.graph.read().unwrap();
        let metrics = self.get_flow_metrics();
        
        self.anomaly_detector.update(metrics);
        
        let mut anomalous_nodes = Vec::new();
        
        for node in graph.node_indices() {
            if let Some(node_data) = graph.node_weight(node) {
                let node_volume = node_data.volume as f64;
                let z_score = (node_volume - self.anomaly_detector.mean_volume) 
                    / self.anomaly_detector.std_volume.max(1.0);
                
                if z_score.abs() > 3.0 {
                    anomalous_nodes.push(node);
                }
            }
        }
        
        anomalous_nodes
    }
}

impl AnomalyDetector {
    fn new() -> Self {
        Self {
            flow_history: VecDeque::with_capacity(ANOMALY_DETECTION_WINDOW),
            mean_volume: 0.0,
            std_volume: 1.0,
            mean_frequency: 0.0,
            std_frequency: 1.0,
        }
    }

    fn update(&mut self, metrics: FlowMetrics) {
        self.flow_history.push_back(metrics);
        
        if self.flow_history.len() > ANOMALY_DETECTION_WINDOW {
            self.flow_history.pop_front();
        }
        
        if self.flow_history.len() >= 10 {
            let volumes: Vec<f64> = self.flow_history.iter()
                .map(|m| m.volume as f64)
                .collect();
            
            self.mean_volume = volumes.iter().sum::<f64>() / volumes.len() as f64;
            
            let variance = volumes.iter()
                .map(|&v| (v - self.mean_volume).powi(2))
                .sum::<f64>() / volumes.len() as f64;
            
            self.std_volume = variance.sqrt();
            
            let frequencies: Vec<f64> = self.flow_history.iter()
                .map(|m| m.frequency as f64)
                .collect();
            
            self.mean_frequency = frequencies.iter().sum::<f64>() / frequencies.len() as f64;
            
            let freq_variance = frequencies.iter()
                .map(|&f| (f - self.mean_frequency).powi(2))
                .sum::<f64>() / frequencies.len() as f64;
            
            self.std_frequency = freq_variance.sqrt();
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionDetails {
    pub volume: u64,
    pub profit: f64,
    pub program_id: [u8; 32],
    pub flow_type: FlowType,
}

#[derive(Debug, Clone, Copy)]
enum ProgramType {
    Serum,
    Raydium,
    Orca,
    Token,
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gnn_initialization() {
        let analyzer = GraphNeuralNetworkFlowAnalyzer::new();
        assert_eq!(analyzer.gnn_layers.len(), GNN_LAYERS);
    }

    #[test]
    fn test_transaction_addition() {
        let analyzer = GraphNeuralNetworkFlowAnalyzer::new();
        let from = [1u8; 32];
        let to = [2u8; 32];
        let details = TransactionDetails {
            volume: 1_000_000_000,
            profit: 0.01,
            program_id: [0u8; 32],
            flow_type: FlowType::Swap,
        };
        
        assert!(analyzer.add_transaction(from, to, details).is_ok());
    }
}

