//! Compile-fail snapshots for the codec surface in a build with no codec feature.
//!
//! "Nothing named a codec" is only an error where the build has no default to fall back to, so
//! these cases cannot live in `tests/ui.rs` (which runs under `--all-features`, where the default
//! codec always resolves). They are compiled only in the codec-free build:
//!
//! ```text
//! TRYBUILD=overwrite RUN_UI_TESTS=1 cargo test --no-default-features \
//!     --features testing,memory,macros --test ui_codec_free
//! ```
#![cfg(all(
    feature = "memory",
    feature = "macros",
    not(any(feature = "json", feature = "cbor", feature = "msgpack"))
))]

/// Whether this run skips the snapshots, and whether skipping them is allowed. The counterpart of
/// the guard in `tests/ui.rs`; each test binary carries its own, because this one compiles only
/// where no codec feature resolves.
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
fn ui_codec_free() {
    if skip_snapshots() {
        eprintln!("skipping trybuild UI tests; set RUN_UI_TESTS=1 (stable toolchain) to run them");
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui_codec_free/*.rs");
}
