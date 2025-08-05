use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let rustc_version = rustc_version::version().unwrap();
    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc_version);
    
    // Get the output directory for generated files
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    // Schema file path
    let schema_file = PathBuf::from("schemas/model_weights.fbs");
    
    // Only run flatc if the schema file exists
    if schema_file.exists() {
        // Compile the FlatBuffers schema
        flatc_rust::run(flatc_rust::Args {
            inputs: &[&schema_file],
            out_dir: &out_dir,
            ..Default::default()
        })
        .expect("flatc failed to compile schema");
        
        println!("cargo:rerun-if-changed=schemas/model_weights.fbs");
    }
}
