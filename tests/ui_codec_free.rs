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

#[test]
fn ui_codec_free() {
    if std::env::var("RUN_UI_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping trybuild UI tests; set RUN_UI_TESTS=1 (stable toolchain) to run them");
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui_codec_free/*.rs");
}
