set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo check --workspace --all-targets --all-features
    cargo check --workspace --no-default-features
    # The codec-free build. A codec is optional, so the self-carrying lanes
    # (`Serialized` / `Deserialized`) and the typed publish entry point over them must stand with
    # no codec feature at all; only encoding and decoding may demand one.
    cargo check --workspace --no-default-features --features testing,memory,macros
    # Rustdoc sees what rustc cannot: broken intra-doc links and redundant targets. CI gates on
    # it, so a link to an item a refactor removed must fail here rather than three jobs later.
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

test:
    cargo test --workspace --all-features
    # The reduced-feature legs CI runs, in CI's order. A target compiled only with a feature off
    # is invisible to the all-features run above, so an API a refactor removed can survive there
    # until CI says otherwise: `codec_free_lanes` and `ui_codec_free` build only where no codec
    # resolves, and `lane_traits_without_macros` only where the derives are gone. The UI
    # snapshots themselves stay opt-in (`RUN_UI_TESTS=1`), because they record one toolchain's
    # exact wording; the run here is what compiles the target.
    cargo test --no-default-features --lib
    cargo test --no-default-features --features macros,memory,testing --test raw_subscriber
    cargo test --no-default-features --features macros,memory,testing --test codec_free_lanes
    cargo test --no-default-features --features macros,memory,testing --test ui_codec_free
    cargo test --no-default-features --features memory,testing --test lane_traits_without_macros
    # Both feature edges, because an all-features run hides a doc example that names a
    # feature-gated item without gating itself.
    cargo test --workspace --doc
    cargo test --workspace --doc --no-default-features

fmt:
    cargo fmt --all

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

# Line-coverage gate, same threshold as CI. The floor sits at the 95% target;
# raise it here and in ci.yml together if coverage climbs further. Needs
# cargo-llvm-cov: cargo install cargo-llvm-cov --locked
cov:
    cargo llvm-cov --workspace --all-features --fail-under-lines 95

# HTML coverage report at target/llvm-cov/html/index.html, for finding the
# uncovered lines the gate complains about.
cov-html:
    cargo llvm-cov --workspace --all-features --html

build:
    cargo build --workspace --release --all-features

clean:
    cargo clean

ci: check test
