set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

# What the benchmarks are built with: the production surface of a service that consumes JSON over
# the in-memory broker, and nothing else. `testing` in particular is a compile error in the
# benchmarks - it compiles a recording branch into every delivery.
bench_features := "memory,macros,json"

# The scenarios that count instructions and allocations, one benchmark file each. The wall-clock
# one is not in the list: it runs under a different harness, which takes none of the arguments
# below.
cost_benches := "--bench consume_json --bench consume_json_kilobyte --bench consume_lane --bench middleware --bench batch --bench reply --bench reply_lending --bench out_slot --bench typed_headers_write --bench typed_headers_read --bench request_reply --bench retry_copy"

default: check

check:
    cargo fmt --all -- --check
    # The benchmarks are left out of the all-features legs on purpose: they are built with the
    # production feature set, and the harness feature is a compile error in them. Their own leg
    # follows.
    cargo clippy --workspace --lib --bins --tests --examples --all-features -- -D warnings
    # Compilation only: this feature combination has lints of its own that no gate has ever run,
    # and cleaning them is not what a benchmark change is for.
    cargo check --benches --no-default-features --features {{ bench_features }}
    cargo check --workspace --lib --bins --tests --examples --all-features
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

# What a change costs per message: instructions and allocations through valgrind, then the
# wall-clock pair, then the document the benchmarks page publishes.
#
# RUSTFLAGS is emptied on purpose. A machine-specific `-C target-cpu=native` makes the numbers
# incomparable with anyone else's, and valgrind aborts outright on the instructions a recent CPU
# advertises. Needs valgrind and the runner pinned to the crate:
# cargo install --locked gungraun-runner --version =0.19.4
#
# Extra arguments reach the benchmark runner: `just bench --save-baseline=main` records a
# baseline, `just bench --baseline=main` measures against it.
# `messages` is the deliveries per measured run: the default is what the published document and
# the CI gate are measured at, a larger count buys a steadier number for a longer run
# (`just bench 5000`). The benches read it at build time, so a new count rebuilds them.
bench messages="1000" *ARGS:
    RUSTFLAGS="" RUSTSTREAM_BENCH_MESSAGES={{ messages }} cargo bench {{ cost_benches }} --no-fail-fast \
        --no-default-features --features {{ bench_features }} \
        -- --output-format=json {{ ARGS }} > target/bench-summary.json
    RUSTFLAGS="" RUSTSTREAM_BENCH_MESSAGES={{ messages }} cargo bench --bench wall_clock \
        --no-default-features --features {{ bench_features }}
    python3 scripts/bench_results.py --messages {{ messages }} target/bench-summary.json docs/benchmarks/results.json

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
