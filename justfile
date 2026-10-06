set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

# The gates build once and run, so the incremental cache buys them nothing, and it is most of a
# build directory: one cache per test target. An edit-and-rerun loop keeps it with
# `CARGO_INCREMENTAL=1 just test`.
export CARGO_INCREMENTAL := env("CARGO_INCREMENTAL", "0")

# A rustc warning fails every build here, so a feature combination that clippy never lints cannot
# collect dead code unseen. Cargo's `build.warnings` does it without touching the build
# fingerprint, where `-D warnings` in RUSTFLAGS would rebuild every dependency. Cargo honours it
# from 1.97 on. A loop that tolerates warnings runs `CARGO_BUILD_WARNINGS=warn just test`.
export CARGO_BUILD_WARNINGS := env("CARGO_BUILD_WARNINGS", "deny")

# What the benchmarks are built with: the production surface of a service that consumes JSON over
# the in-memory broker, and nothing else. `testing` in particular is a compile error in the
# benchmarks - it compiles a recording branch into every delivery.
bench_features := "memory,macros,json"

# The scenarios that count instructions and allocations, one benchmark file each. A run against a
# baseline holds the gated ones to an instruction limit; the ungated one is measured and reported,
# and held to no number. The wall-clock one is in neither list: it runs under a different harness,
# which takes none of the arguments below.
gated_benches := "--bench consume_json --bench consume_json_kilobyte --bench consume_lane --bench consume_pool --bench consume_threads --bench middleware --bench batch --bench reply --bench reply_lending --bench out_slot --bench typed_headers_write --bench typed_headers_read --bench request_reply"
ungated_benches := "--bench retry_copy"

default: check

check:
    cargo fmt --all -- --check
    # The benchmarks are left out of the all-features legs on purpose: they are built with the
    # production feature set, and the harness feature is a compile error in them. Their own leg
    # follows.
    cargo clippy --workspace --lib --bins --tests --examples --all-features -- -D warnings
    # No clippy here: this feature combination has clippy lints of its own that no gate has ever
    # run, and cleaning them is not what a benchmark change is for.
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
    # The compile-fail snapshots: rustc's exact wording on the stable toolchain this repository
    # selects, which is what CI records them with. REQUIRE_UI_TESTS turns a skip into a failure,
    # so a lost opt-in shows here. `ui_codec_free` holds the errors that exist only where no
    # codec resolves, so it builds without one.
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test --all-features --test ui
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test --no-default-features --features macros,memory,testing --test ui_codec_free

test:
    cargo test --workspace --all-features
    # The reduced-feature legs CI runs, in CI's order. A target compiled only with a feature off
    # is invisible to the all-features run above, so an API a refactor removed can survive there
    # until CI says otherwise: `codec_free_lanes` and `ui_codec_free` build only where no codec
    # resolves, and `lane_traits_without_macros` only where the derives are gone. The UI
    # snapshots themselves run in `check`; the run here is what compiles the target.
    cargo test --no-default-features --lib
    cargo test --no-default-features --features macros,memory,testing --test raw_subscriber
    cargo test --no-default-features --features macros,memory,testing --test codec_free_lanes
    cargo test --no-default-features --features macros,memory,testing --test ui_codec_free
    cargo test --no-default-features --features memory,testing --test lane_traits_without_macros
    # Plain `cargo test`: every target needing a non-default feature has to gate itself.
    cargo test --workspace
    # Both feature edges, because an all-features run hides a doc example that names a
    # feature-gated item without gating itself.
    cargo test --workspace --doc
    cargo test --workspace --doc --no-default-features

# What a change costs per message: instructions and allocations through valgrind, then the
# wall-clock pair, then the document the benchmarks page publishes.
#
# RUSTFLAGS is emptied on purpose. A machine-specific `-C target-cpu=native` makes the numbers
# incomparable with anyone else's, and valgrind aborts outright on the instructions a recent CPU
# advertises. Needs valgrind.
#
# The benchmarks hand the measurement to gungraun's runner, which has to be the release of the
# library the lock file pins. The recipe installs that release into `target/gungraun-runner` on
# the first run and after the library moves, and puts it first on PATH, where the benchmarks look
# the runner up. A `GUNGRAUN_RUNNER` in the environment would win over PATH when the benchmarks
# build, so the recipe clears it.
#
# A leading number is the deliveries per measured run: the default of 1000 is what the published
# document is measured at, a larger count buys a steadier number for a longer run
# (`just bench 5000`). The benches read it at build time, so a new count rebuilds them. The other
# arguments reach the benchmark runner: `just bench --save-baseline=main` records a baseline,
# `just bench --baseline=main` measures against it. Totals over another count are not comparable,
# so each count keeps its runs and baselines in a directory of its own, `target/gungraun/<count>`.
#
# A run against a baseline, named with `--baseline` or in `GUNGRAUN_BASELINE`, fails on two
# percent more instructions than the baseline in a gated scenario. The limit is relative, so it
# applies only there: a plain run would be held to the previous run, and the dedicated-threads
# scenario counts more or fewer instructions with the machine's load. The allocation limits are
# absolute, and every run is held to them.
#
# A benchmark that breaches a limit fails the run, and the run still goes to the end: the table
# prints, every breach under it with the value it was compared against beside the new one, and
# the recipe fails after that. A build error stops it before anything runs.
[positional-arguments]
bench *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    messages=1000
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
        messages="$1"
        shift
    fi
    version="$(cargo pkgid gungraun)"
    version="${version##*@}"
    runner="$PWD/target/gungraun-runner"
    installed="$("$runner/bin/gungraun-runner" --version 2> /dev/null || true)"
    if [ "$installed" != "gungraun-runner $version" ]; then
        cargo install --locked --root "$runner" gungraun-runner --version "=$version"
    fi
    unset GUNGRAUN_RUNNER
    export PATH="$runner/bin:$PATH" RUSTFLAGS="" RUSTSTREAM_BENCH_MESSAGES="$messages" \
        GUNGRAUN_HOME="$PWD/target/gungraun/$messages"
    # A baseline named on the command line or in the environment brings the instruction limit.
    baseline="${GUNGRAUN_BASELINE:-}"
    for arg in "$@"; do
        case "$arg" in --baseline | --baseline=*) baseline="$arg" ;; esac
    done
    limits=()
    if [ -n "$baseline" ]; then
        limits=(--callgrind-limits='ir=2.0%')
    fi
    cargo bench {{ gated_benches }} {{ ungated_benches }} --bench wall_clock --no-run \
        --no-default-features --features {{ bench_features }}
    status=0
    cargo bench {{ gated_benches }} --no-fail-fast \
        --no-default-features --features {{ bench_features }} \
        -- --output-format=json "${limits[@]}" "$@" > target/bench-summary.json || status=$?
    cargo bench {{ ungated_benches }} --no-fail-fast \
        --no-default-features --features {{ bench_features }} \
        -- --output-format=json "$@" >> target/bench-summary.json || status=$?
    cargo bench --bench wall_clock --no-default-features --features {{ bench_features }} \
        || status=$?
    python3 scripts/bench_results.py --messages "$messages" target/bench-summary.json \
        docs/benchmarks/results.json
    exit "$status"

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

# Each `ruststream-*` repository next to the main checkout of this one runs
# `cargo test --workspace --all-features` against this working tree, on whatever branch it has
# checked out, so a core change and the broker adaptations it needs are tested together before
# either is committed. A worktree of the core finds the brokers through the main checkout. For the
# run, a broker's lock file takes `ruststream` from this tree instead of the release it pins, and
# is put back afterwards; a broker that would still build a release fails. `just brokers nats fred`
# runs the named ones and fails on one it cannot test; with no names it runs every broker it finds
# and lists the repositories it skips (no crate, no `ruststream` dependency). The run fails when it
# tests no broker. Needs jq.
# Every broker crate's test suite against this working tree of the core.
brokers *names:
    #!/usr/bin/env bash
    set -uo pipefail
    command -v jq > /dev/null || { echo "error: just brokers needs jq" >&2; exit 1; }
    # A broker's own warnings are for its own gates to judge.
    unset CARGO_BUILD_WARNINGS
    core="$(pwd)"
    main="$(git worktree list --porcelain | sed -n '1s/^worktree //p')"
    root="$(dirname "$main")"
    version="$(cargo metadata --format-version 1 --no-deps \
        | jq -r '.packages[] | select(.name == "ruststream") | .version')"
    patch="patch.crates-io.ruststream.path='$core'"
    # One broker's suite against this tree, in a subshell, so its lock file goes back on any exit.
    test_broker() (
        cd "$1" || exit 1
        saved="$(mktemp)" || exit 1
        if [ -f Cargo.lock ]; then
            cp Cargo.lock "$saved"
            trap 'cp "$saved" Cargo.lock; rm -f "$saved"' EXIT
        else
            trap 'rm -f Cargo.lock "$saved"' EXIT
        fi
        # The patch alone leaves the release the lock file pins in place.
        cargo update --config "$patch" -p ruststream || exit 1
        resolved="$(cargo metadata --format-version 1 --all-features --locked --config "$patch" \
            | jq -r '.packages[] | select(.name == "ruststream") | .manifest_path')" || exit 1
        if [ "$resolved" != "$core/Cargo.toml" ]; then
            echo "error: ruststream comes from ${resolved:-nowhere}, not from this tree" \
                "(ruststream $version): the patch is unused" >&2
            exit 1
        fi
        cargo test --workspace --all-features --locked --config "$patch"
    )
    if [ -n "{{ names }}" ]; then
        named=true
        repos=()
        for name in {{ names }}; do repos+=("$root/ruststream-$name"); done
    else
        named=false
        shopt -s nullglob
        repos=("$root"/ruststream-*)
    fi
    passed=()
    failed=()
    skipped=()
    for repo in "${repos[@]}"; do
        name="$(basename "$repo")"
        # The dashboard repository has no crate, and a crate without the core has nothing to test
        # against it.
        if [ ! -d "$repo" ]; then
            unfit="not found"
        elif [ ! -f "$repo/Cargo.toml" ]; then
            unfit="no crate"
        elif ! manifest="$(cd "$repo" && cargo metadata --format-version 1 --no-deps)"; then
            failed+=("$name")
            continue
        elif jq -e 'any(.packages[].dependencies[]; .name == "ruststream")' <<< "$manifest" \
            > /dev/null; then
            unfit=""
        else
            unfit="no ruststream dependency"
        fi
        if [ -n "$unfit" ]; then
            # A name on the command line is a broker the run was asked to test.
            if $named; then failed+=("$name ($unfit)"); else skipped+=("$name ($unfit)"); fi
            continue
        fi
        echo "==> $name ($(git -C "$repo" branch --show-current))"
        if test_broker "$repo"; then passed+=("$name"); else failed+=("$name"); fi
    done
    echo
    echo "passed: ${passed[*]:-none}"
    echo "failed: ${failed[*]:-none}"
    echo "skipped: ${skipped[*]:-none}"
    if [ $((${#passed[@]} + ${#failed[@]})) -eq 0 ]; then
        echo "error: no broker crate next to $main" >&2
        exit 1
    fi
    [ ${#failed[@]} -eq 0 ]
