use super::{ProgramDecoder, DecodedInstruction, InstructionOpcode, AccountRoles};
use async_trait::async_trait;
use solana_sdk::{instruction::CompiledInstruction, pubkey::Pubkey};
use std::sync::Arc;

const METEORA_DLMM: &str = "DLMMv1JZq5aUCrSgTWzKXJm8xkzq3x3rPuJ3hHLi9Jm"; // Replace with actual

#[derive(Default)]
pub struct MeteoraDlmmDecoder;

#[async_trait]
impl ProgramDecoder for MeteoraDlmmDecoder {
    fn can_decode(&self, pid: &Pubkey) -> bool { pid.to_string().as_str() == METEORA_DLMM }

    fn decode_instruction(
        &self,
        ix: &CompiledInstruction,
        accounts: &[Pubkey],
        data: &[u8],
    ) -> Result<DecodedInstruction, crate::DecoderError> {
        if data.len() < 1 { return Err(crate::DecoderError::InvalidInstruction); }
        // Discriminators: populate from official IDL; here illustrate common ops
        let opcode = match data[0] {
            0x01 => InstructionOpcode::Swap,
            0x02 => InstructionOpcode::IncreaseLiquidity,
            0x03 => InstructionOpcode::DecreaseLiquidity,
            0x04 => InstructionOpcode::Deposit,
            0x05 => InstructionOpcode::Withdraw,
            _ => InstructionOpcode::Unknown(data[0]),
        };
        Ok(DecodedInstruction {
            program_id: *pid_from(ix, accounts),
            opcode,
            accounts: accounts.iter().copied().collect(),
            data: Arc::from(data.to_vec().into_boxed_slice()),
            roles: AccountRoles::default(),
        })
    }

    fn identify_strategy(&self, ixs: &[DecodedInstruction]) -> Option<crate::StrategyType> {
        let add = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::IncreaseLiquidity | InstructionOpcode::Deposit));
        let rem = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::DecreaseLiquidity | InstructionOpcode::Withdraw));
        if add && rem {
            let pool = ixs.first().and_then(|i| i.accounts.first().copied()).unwrap_or_default();
            Some(crate::StrategyType::JitLiquidity { pool, duration_ms: 200 })
        } else if ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::Swap)) {
            Some(crate::StrategyType::AtomicArbitrage { dex_sequence: vec!["MeteoraDLMM".into()] })
        } else { None }
    }
}

fn pid_from<'a>(ix: &CompiledInstruction, accounts: &'a [Pubkey]) -> &'a Pubkey {
    accounts.get(ix.program_id_index as usize).unwrap_or(&Pubkey::default())
}
