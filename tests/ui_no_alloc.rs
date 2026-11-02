//! Compile-fail snapshots of a build without `alloc`: what a device service writes wrong where the
//! fixed rooms are the only rooms there are.
//!
//! ```text
//! TRYBUILD=overwrite RUN_UI_TESTS=1 cargo test --no-default-features \
//!     --features embassy,memory,macros,testing --test ui_no_alloc
//! ```
#![cfg(not(ruststream_alloc))]

use std::env;

/// Whether this run skips the snapshots, and whether skipping them is allowed.
///
/// # Panics
///
/// Panics when a run that requires the snapshots is not set up to run them.
fn skip_snapshots() -> bool {
    let opted_in = env::var("RUN_UI_TESTS").as_deref() == Ok("1");
    let required = env::var("REQUIRE_UI_TESTS").as_deref() == Ok("1");
    assert!(
        opted_in || !required,
        "REQUIRE_UI_TESTS=1 but RUN_UI_TESTS is not 1: this run would have skipped the UI \
         snapshots and reported success"
    );
    !opted_in
}

#[test]
fn ui_no_alloc() {
    if skip_snapshots() {
        eprintln!("skipping trybuild UI tests; set RUN_UI_TESTS=1 (stable toolchain) to run them");
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui_no_alloc/*.rs");
}
