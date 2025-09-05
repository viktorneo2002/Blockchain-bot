use super::{ProgramDecoder, DecodedInstruction, InstructionOpcode, AccountRoles};
use async_trait::async_trait;
use solana_sdk::{instruction::CompiledInstruction, pubkey::Pubkey};
use std::sync::Arc;

const LIFINITY_V2: &str = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c";

#[derive(Default)]
pub struct LifinityDecoder;

#[async_trait]
impl ProgramDecoder for LifinityDecoder {
    fn can_decode(&self, pid: &Pubkey) -> bool { pid.to_string().as_str() == LIFINITY_V2 }

    fn decode_instruction(
        &self,
        ix: &CompiledInstruction,
        accounts: &[Pubkey],
        data: &[u8],
    ) -> Result<DecodedInstruction, crate::DecoderError> {
        if data.len() < 8 { return Err(crate::DecoderError::InvalidInstruction); }
        let discr = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let opcode = match discr {
            // Fill with official mapping; placeholders:
            0x01 => InstructionOpcode::Swap,
            0x02 => InstructionOpcode::Deposit,
            0x03 => InstructionOpcode::Withdraw,
            _ => InstructionOpcode::Unknown((discr & 0xff) as u8),
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
        let swaps = ixs.iter().any(|i| matches!(i.opcode, InstructionOpcode::Swap));
        if swaps { Some(crate::StrategyType::AtomicArbitrage { dex_sequence: vec!["Lifinity".into()] }) } else { None }
    }
}

fn pid_from<'a>(ix: &CompiledInstruction, accounts: &'a [Pubkey]) -> &'a Pubkey {
    accounts.get(ix.program_id_index as usize).unwrap_or(&Pubkey::default())
}
