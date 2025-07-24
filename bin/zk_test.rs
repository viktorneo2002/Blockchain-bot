use std::fs;
use ark_bn254::Bn254;
use ark_groth16::{Groth16, Proof};
use ark_serialize::{CanonicalDeserialize, Compress, Validate};
use solana_mev_bot::ProofBuilder; // 👈 твојот crate name!

fn main() {
    println!("🧠 [LIVE TEST] ProofBuilder mainnet simulation");

    let pk_path = "test_data/dummy_proving_key.bin"; // промени ако имаш друго
    let hex_inputs = vec!["0x1234567890abcdef".to_string()];
    let slot = 101010;
    let nonce = 777;

    let pk_bytes = fs::read(pk_path).expect("❌ Missing proving key file");
    let mut builder = ProofBuilder::from_bytes(&pk_bytes).expect("❌ Failed to load PK");

    match builder.generate_proof(&hex_inputs, slot, nonce) {
        Ok((proof_bytes, metadata)) => {
            println!("✅ Proof created ({} bytes)", proof_bytes.len());
            println!("🔐 Metadata: slot={}, nonce={}, hash={:?}", metadata.slot, metadata.nonce, metadata.proof_hash);

            let proof: Proof<Bn254> = Proof::deserialize_with_mode(&*proof_bytes, Compress::Yes, Validate::Yes)
                .expect("❌ Deserialize failed");

            let inputs = builder.parse_and_validate_inputs(&hex_inputs).unwrap();
            let verified = Groth16::<Bn254>::verify(&builder.proving_key().vk, &inputs, &proof);

            if verified {
                println!("✅✅✅ PROOF VERIFIED — MAINNET SAFE");
            } else {
                println!("🛑🛑🛑 PROOF FAILED TO VERIFY — YOUR ZK IS GARBAGE");
            }
        }
        Err(e) => {
            println!("🔥 ZK GENERATION FAILED: {}", e);
        }
    }
}
