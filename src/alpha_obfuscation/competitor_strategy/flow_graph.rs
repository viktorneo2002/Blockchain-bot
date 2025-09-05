use petgraph::{graph::NodeIndex, Graph, Directed};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

pub type FlowGraph = Graph<Pubkey, String, Directed>;

/// Build a flow graph from decoded instructions
pub fn build_flow_graph(instructions: &[super::DecodedInstruction]) -> FlowGraph {
    let mut g: FlowGraph = Graph::new();
    let mut node_map: HashMap<Pubkey, NodeIndex> = HashMap::new();

    let mut ensure_node = |pk: Pubkey, g: &mut FlowGraph, node_map: &mut HashMap<Pubkey, NodeIndex>| {
        node_map.entry(pk).or_insert_with(|| g.add_node(pk))
    };

    // Create nodes for all accounts
    for ix in instructions {
        for &acct in &ix.accounts {
            ensure_node(acct, &mut g, &mut node_map);
        }
    }

    // Create edges
    for (i, ix) in instructions.iter().enumerate() {
        // Sequential edges between accounts
        for w in ix.accounts.windows(2) {
            let from = w[0];
            let to = w[1];
            let label = format!("ix{}:{:?}", i, ix.opcode);
            g.add_edge(node_map[&from], node_map[&to], label);
        }

        // Special edges for transfers
        if matches!(ix.opcode, super::InstructionOpcode::Transfer | super::InstructionOpcode::CLOBSettleFunds) {
            if let (Some(&src), Some(&dst)) = (ix.accounts.get(0), ix.accounts.get(1)) {
                g.add_edge(node_map[&src], node_map[&dst], format!("transfer_ix{}", i));
            }
        }
    }

    g
}

/// Detect cycles up to length limit
pub fn detect_cycles(g: &FlowGraph, max_len: usize) -> Vec<Vec<Pubkey>> {
    use petgraph::algo::tarjan_scc;
    
    tarjan_scc(g)
        .into_iter()
        .filter(|cycle| cycle.len() <= max_len && cycle.len() >= 2)
        .map(|cycle| cycle.into_iter().map(|ni| g[ni]).collect())
        .collect()
}
