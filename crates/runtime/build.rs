//! Reads the variant of the build from the contract crate: `ruststream_alloc` is on when
//! `ruststream-core` was built with `alloc`, whichever crate asked for it, so every crate of the
//! build holds one representation.

use std::env;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(ruststream_alloc)");
    println!("cargo::rerun-if-env-changed=DEP_RUSTSTREAM_CORE_ALLOC");
    println!("cargo::rustc-check-cfg=cfg(ruststream_tracing)");
    println!("cargo::rerun-if-env-changed=DEP_RUSTSTREAM_CORE_TRACING");
    if env::var_os("DEP_RUSTSTREAM_CORE_TRACING").is_some() {
        println!("cargo::rustc-cfg=ruststream_tracing");
    }
    if env::var_os("DEP_RUSTSTREAM_CORE_ALLOC").is_some() {
        println!("cargo::rustc-cfg=ruststream_alloc");
    }
}
