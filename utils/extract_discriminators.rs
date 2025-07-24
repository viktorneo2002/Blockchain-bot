// extract_discriminators.rs
use sha2::{Digest, Sha256};

fn main() {
    let instructions = vec![
        "swap_base_in", // Raydium
        "swap",         // Orca Whirlpool
        "route",        // Jupiter
        "flash_loan", "repay_flash_loan", "liquidate_spot", // Mango
        "flash_loan", "repay_flash_loan", "liquidate_obligation", // Solend
        "begin_flash_loan", "end_flash_loan", "liquidate_spot" // Drift
    ];

    for name in instructions {
        let hash = Sha256::digest(format!("global:{}", name).as_bytes());
        let bytes = &hash[..8];
        print!("pub const {}_DISCRIMINATOR: [u8; 8] = [", name.to_uppercase());
        for (i, b) in bytes.iter().enumerate() {
            if i != 0 {
                print!(", ");
            }
            print!("0x{:02x}", b);
        }
        println!("];");
    }
}
