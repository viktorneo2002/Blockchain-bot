fn main() {
    let rustc_version = rustc_version::version().unwrap();
    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc_version);
}
