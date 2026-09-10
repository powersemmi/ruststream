//! Compile-fail snapshots for the `ruststream-macros` diagnostics.
//!
//! Each `tests/ui/*.rs` case feeds the `#[subscriber]` / `#[ruststream::app]` / `derive(MessageInfo)`
//! macros a misuse and pins the compile error against a `.stderr` snapshot, so a regression in a
//! macro's diagnostics (a reworded or dropped message, a shifted span) fails the build.
//!
//! The snapshots are rustc-version-sensitive, and CI builds a floor..stable matrix, so they are
//! pinned to a single toolchain: the run sets `RUN_UI_TESTS=1` (only the stable `cargo test` job
//! does), and every other run - the floor job, a local `cargo test` without the flag - skips. To
//! refresh the snapshots after an intentional message change, run on stable:
//!
//! ```text
//! TRYBUILD=overwrite RUN_UI_TESTS=1 cargo test --test ui --features macros,memory,json
//! ```

/// Whether this run skips the snapshots, and whether skipping them is allowed.
///
/// `RUN_UI_TESTS=1` opts in. `REQUIRE_UI_TESTS=1` says a skip is not acceptable in this run: CI
/// sets it wherever it sets the opt-in, so dropping or misspelling the opt-in turns the job red
/// instead of leaving a green run that checked nothing. Both unset is the local default, where
/// the snapshots stay opt-in because they record one toolchain's exact wording.
///
/// # Panics
///
/// Panics when a run that requires the snapshots is not set up to run them.
fn skip_snapshots() -> bool {
    let opted_in = std::env::var("RUN_UI_TESTS").as_deref() == Ok("1");
    let required = std::env::var("REQUIRE_UI_TESTS").as_deref() == Ok("1");
    assert!(
        opted_in || !required,
        "REQUIRE_UI_TESTS=1 but RUN_UI_TESTS is not 1: this run would have skipped the UI \
         snapshots and reported success"
    );
    !opted_in
}

#[test]
fn ui() {
    if skip_snapshots() {
        eprintln!("skipping trybuild UI tests; set RUN_UI_TESTS=1 (stable toolchain) to run them");
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
