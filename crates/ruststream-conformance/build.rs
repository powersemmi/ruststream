//! Reads the variant of the build from the contract crate: `ruststream_alloc` is on when
//! `ruststream-core` was built with `alloc`, whichever crate asked for it, so every crate of the
//! build holds one representation.
//!
//! `ruststream_send_family` is on when the crate's own `alloc` feature is on as well: the suites
//! of the `Send` family run handlers on a foreign runtime, which asks the `send` feature of the
//! contract that this feature turns on.

use std::env;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(ruststream_alloc)");
    println!("cargo::rustc-check-cfg=cfg(ruststream_send_family)");
    println!("cargo::rerun-if-env-changed=DEP_RUSTSTREAM_CORE_ALLOC");
    let alloc = env::var_os("DEP_RUSTSTREAM_CORE_ALLOC").is_some();
    if alloc {
        println!("cargo::rustc-cfg=ruststream_alloc");
    }
    if alloc && env::var_os("CARGO_FEATURE_ALLOC").is_some() {
        println!("cargo::rustc-cfg=ruststream_send_family");
    }
}
