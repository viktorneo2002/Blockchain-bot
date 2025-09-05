use super::{ProgramDecoder, DecodedInstruction, InstructionOpcode, AccountRoles};
use async_trait::async_trait;
use solana_sdk::{instruction::CompiledInstruction, pubkey::Pubkey};
use std::sync::Arc;

const PHOENIX_V1: &str = "PhoeNiXZ8bq88cKQDduZLhGdK7xZ6iH6iWJfM3Wz5wX"; // Replace with actual

#[derive(Default)]
pub struct PhoenixDecoder;

#[async_trait]
impl ProgramDecoder for PhoenixDecoder {
    fn can_decode(&self, pid: &Pubkey) -> bool { pid.to_string().as_str() == PHOENIX_V1 }

    fn decode_instruction(
        &self,
        ix: &CompiledInstruction,
        accounts: &[Pubkey],
        data: &[u8],
    ) -> Result<DecodedInstruction, crate::DecoderError> {
        if data.is_empty() { return Err(crate::DecoderError::InvalidInstruction); }
        // Phoenix encodes opcodes; adapt per spec
        let opcode = match data[0] {
            0x01 | 0x02 => InstructionOpcode::CLOBNewOrder,
            0x10 => InstructionOpcode::CLOBSettleFunds,
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
        let new_order = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::CLOBNewOrder));
        let settle = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::CLOBSettleFunds));
        if new_order && settle {
            Some(crate::StrategyType::AtomicArbitrage { dex_sequence: vec!["Phoenix".into()] })
        } else { None }
    }
}

fn pid_from<'a>(ix: &CompiledInstruction, accounts: &'a [Pubkey]) -> &'a Pubkey {
    accounts.get(ix.program_id_index as usize).unwrap_or(&Pubkey::default())
}
