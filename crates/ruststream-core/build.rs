//! Tells the crates that depend on the contract which variant this build is: they read
//! `DEP_RUSTSTREAM_CORE_ALLOC` in their own build scripts and switch representation on it, so a
//! build has one variant throughout, whichever crate turned `alloc` on.

use std::env;

fn main() {
    if env::var_os("CARGO_FEATURE_ALLOC").is_some() {
        println!("cargo::metadata=alloc=1");
    }
}
