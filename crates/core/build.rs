//! Tells the crates that depend on the contract which variant this build is: they read
//! `DEP_RUSTSTREAM_CORE_ALLOC` in their own build scripts and switch representation on it, so a
//! build has one variant throughout, whichever crate turned `alloc` on. The same holds for the
//! diagnostics sink: `DEP_RUSTSTREAM_CORE_TRACING` is set when they are `tracing` events.

use std::env;

fn main() {
    // The diagnostics are `tracing` events where the crate is: its feature is on and the target
    // has pointer atomics, which the crate needs.
    println!("cargo::rustc-check-cfg=cfg(diag_tracing)");
    if env::var_os("CARGO_FEATURE_TRACING").is_some()
        && env::var("CARGO_CFG_TARGET_HAS_ATOMIC")
            .is_ok_and(|atomics| atomics.split(',').any(|width| width == "ptr"))
    {
        println!("cargo::rustc-cfg=diag_tracing");
        println!("cargo::metadata=tracing=1");
    }
    if env::var_os("CARGO_FEATURE_ALLOC").is_some() {
        println!("cargo::metadata=alloc=1");
    }
}
