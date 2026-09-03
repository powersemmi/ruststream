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

test:
    cargo test --workspace --all-features
    # Runs the lanes that must not need a codec. Only this target is codec-free: the rest of
    # `tests/` decodes JSON models and is compiled by the all-features run above.
    cargo test --no-default-features --features testing,memory,macros --test codec_free_lanes

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
