use super::*;

#[test]
fn test_build_flow_graph() {
    let instructions = vec![
        DecodedInstruction {
            opcode: InstructionOpcode::Transfer,
            accounts: vec![Pubkey::new_unique(), Pubkey::new_unique()],
            data: vec![],
        },
    ];
    
    let graph = build_flow_graph(&instructions);
    assert_eq!(graph.node_count(), 2);
    assert_eq!(graph.edge_count(), 1);
}

#[test]
fn test_detect_cycles() {
    let mut instructions = vec![];
    let a = Pubkey::new_unique();
    let b = Pubkey::new_unique();
    
    // Create a cycle: a -> b -> a
    instructions.push(DecodedInstruction {
        opcode: InstructionOpcode::Transfer,
        accounts: vec![a, b],
        data: vec![],
    });
    instructions.push(DecodedInstruction {
        opcode: InstructionOpcode::Transfer,
        accounts: vec![b, a],
        data: vec![],
    });
    
    let graph = build_flow_graph(&instructions);
    let cycles = detect_cycles(&graph, 3);
    assert_eq!(cycles.len(), 1);
    assert_eq!(cycles[0].len(), 2);
}
