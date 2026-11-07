//! Puts the memory map where the linker script of `cortex-m-rt` looks for it, and links the
//! scripts of `cortex-m-rt` and `defmt`.

use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // The scripts are the firmware's; a build for the host (the tests) links none of them.
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("none") {
        return;
    }
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("cargo sets OUT_DIR"));
    fs::copy("memory.x", out.join("memory.x")).expect("memory.x sits beside the manifest");
    println!("cargo:rustc-link-search={}", out.display());
    println!("cargo:rustc-link-arg-bins=-Tlink.x");
    println!("cargo:rustc-link-arg-bins=-Tdefmt.x");
    println!("cargo:rerun-if-changed=memory.x");
}
