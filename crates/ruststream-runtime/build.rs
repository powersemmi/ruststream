//! Reads the variant of the build from the contract crate: `ruststream_alloc` is on when
//! `ruststream-core` was built with `alloc`, whichever crate asked for it, so every crate of the
//! build holds one representation.
//!
//! The runtime's heap dependencies come with its own `alloc` feature, which another crate's
//! request for the contract's cannot switch on. A build that has one without the other is refused
//! here, naming the feature to enable, instead of failing later on a missing import.

use std::env;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(ruststream_alloc)");
    println!("cargo::rerun-if-env-changed=DEP_RUSTSTREAM_CORE_ALLOC");
    if env::var_os("DEP_RUSTSTREAM_CORE_ALLOC").is_none() {
        return;
    }
    if env::var_os("CARGO_FEATURE_ALLOC").is_none() {
        println!(
            "cargo::error=`ruststream-core` is built with `alloc` but `ruststream-runtime` is not: \
             enable the `alloc` feature of `ruststream-runtime` (the `alloc` feature of `ruststream` \
             enables both)"
        );
    }
    println!("cargo::rustc-cfg=ruststream_alloc");
}
