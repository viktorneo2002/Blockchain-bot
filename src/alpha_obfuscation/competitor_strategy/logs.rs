use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct InstructionCuUsage {
    pub index: usize,        // top-level instruction index
    pub program: String,     // program id string
    pub cu_consumed: u64,    // attributed CU
}

pub fn parse_cu_by_instruction(logs: &[String]) -> Vec<InstructionCuUsage> {
    // Heuristic: track current stack depth and attribute "consumed ... units of compute" to last invoked program.
    let mut stack: Vec<(String, usize)> = Vec::new(); // (program, idx)
    let mut out: Vec<InstructionCuUsage> = Vec::new();

    for l in logs {
        // Program invoke: "Program <pid> invoke [X]"
        if let Some(pid) = l.strip_prefix("Program ").and_then(|s| s.split_whitespace().next()) {
            if l.contains("invoke [") {
                // Attempt to extract top-level index from bracket depth; fallback 0
                let idx = stack.last().map(|x| x.1).unwrap_or(0);
                stack.push((pid.to_string(), idx));
                continue;
            }
        }
        // Program consumed: 'consumed N of M compute units'
        if let Some(pos) = l.find("consumed ") {
            if let Some(end) = l[pos + 9..].find(' ') {
                let n_str = &l[pos + 9..pos + 9 + end];
                if let Ok(n) = n_str.parse::<u64>() {
                    if let Some((prog, idx)) = stack.last() {
                        out.push(InstructionCuUsage {
                            index: *idx,
                            program: prog.clone(),
                            cu_consumed: n,
                        });
                    }
                }
            }
        }
        // Program return: "Program <pid> success"
        if l.starts_with("Program ") && l.contains(" success") {
            let _ = stack.pop();
        }
    }
    out
}
