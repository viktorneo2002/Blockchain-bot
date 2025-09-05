use std::{collections::{HashMap, HashSet}, sync::Arc};
use solana_sdk::pubkey::Pubkey;

#[derive(Clone, Default)]
pub struct OwnerCluster {
    roots: HashSet<Pubkey>,
    graph: HashMap<Pubkey, HashSet<Pubkey>>,
}

impl OwnerCluster {
    pub fn new() -> Self { Self::default() }

    pub fn add_edge(&mut self, a: Pubkey, b: Pubkey) {
        self.graph.entry(a).or_default().insert(b);
        self.graph.entry(b).or_default().insert(a);
    }

    pub fn add_root(&mut self, p: Pubkey) { self.roots.insert(p); }

    pub fn union_from(&self, seed: &Pubkey, limit: usize) -> HashSet<Pubkey> {
        // BFS with cap
        let mut seen = HashSet::new();
        let mut q = vec![*seed];
        while let Some(curr) = q.pop() {
            if seen.len() >= limit { break; }
            if seen.insert(curr) {
                if let Some(neigh) = self.graph.get(&curr) {
                    for &n in neigh {
                        if !seen.contains(&n) { q.push(n); }
                    }
                }
            }
        }
        seen
    }
}

// Heuristics helpers
pub fn derive_ata(owner: &Pubkey, mint: &Pubkey) -> Option<Pubkey> {
    // Implement SPL Associated Token Account derivation
    // spl_associated_token_account::get_associated_token_address
    None
}
