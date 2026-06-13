//! Compile-fail snapshots for the `ruststream-macros` diagnostics.
//!
//! Each `tests/ui/*.rs` case feeds the `#[subscriber]` / `#[ruststream::app]` / `derive(Message)`
//! macros a misuse and pins the compile error against a `.stderr` snapshot, so a regression in a
//! macro's diagnostics (a reworded or dropped message, a shifted span) fails the build.
//!
//! The snapshots are rustc-version-sensitive, and CI builds a 1.85..stable matrix, so they are
//! pinned to a single toolchain: the run sets `RUN_UI_TESTS=1` (only the stable `cargo test` job
//! does), and every other run - the 1.85 job, a local `cargo test` without the flag - skips. To
//! refresh the snapshots after an intentional message change, run on stable:
//!
//! ```text
//! TRYBUILD=overwrite RUN_UI_TESTS=1 cargo test --test ui --features macros,memory,json
//! ```

#[test]
fn ui() {
    if std::env::var("RUN_UI_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping trybuild UI tests; set RUN_UI_TESTS=1 (stable toolchain) to run them");
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
