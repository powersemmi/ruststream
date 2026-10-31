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
bench_features := "memory,macros,json,tokio"

# The two runtimes, `tokio` and `embassy`, exclude each other, so no build enables every feature.
# Each runtime has its set instead: the tokio set is every feature of the facade but `embassy`,
# and the embassy set is every feature that builds beside it.
tokio_set := "asyncapi,cbor,cli,conformance,json,logging,macros,memory,metrics,msgpack,otel,poll-diagnostics,testing,tokio"
embassy_set := "cbor,embassy,json,macros,memory,msgpack"

# What a workspace leg enables. A name without a package turns that feature on in every member
# that has it, so each member builds with every feature the tokio set reaches. The memory crate's
# local bus builds beside its tokio bus, so it joins the tokio legs and their coverage.
workspace_features := tokio_set + ",ruststream-memory/embassy"

# The members a workspace leg leaves out: the embedded runner's tests and doc examples build the
# facade with the embassy set, as a device service does, and the tokio set beside it is the
# two-runtime compile error. Its own legs below build it.
tokio_legs_exclude := "--exclude ruststream-runtime-embassy"

# The scenarios that count instructions and allocations, one benchmark file each. A run against a
# baseline holds the gated ones to an instruction limit; the ungated one is measured and reported,
# and held to no number. The wall-clock one is in neither list: it runs under a different harness,
# which takes none of the arguments below.
gated_benches := "--bench consume_json --bench consume_json_kilobyte --bench consume_lane --bench consume_pool --bench consume_threads --bench middleware --bench batch --bench reply --bench reply_lending --bench out_slot --bench typed_headers_write --bench typed_headers_read --bench request_reply --bench continuation"
ungated_benches := "--bench retry_copy --bench retry_copy_deferred"

default: check

check:
    cargo fmt --all -- --check
    # The benchmarks are left out of the workspace legs on purpose: they are built with the
    # production feature set, and the harness feature is a compile error in them. Their own leg
    # follows.
    cargo clippy --workspace {{ tokio_legs_exclude }} --lib --bins --tests --examples \
        --features {{ workspace_features }} \
        -- -D warnings
    # No clippy here: this feature combination has clippy lints of its own that no gate has ever
    # run, and cleaning them is not what a benchmark change is for.
    cargo check --benches --no-default-features --features {{ bench_features }}
    cargo check --workspace {{ tokio_legs_exclude }} --lib --bins --tests --examples \
        --features {{ workspace_features }}
    # The probe always asks for `macros`, `json` and `tokio`, and a workspace build unifies a
    # dependency's features across members, so the reduced legs leave it out and check it on its
    # own.
    cargo check --workspace --exclude ruststream-no-std-probe --no-default-features
    # The codec-free build. A codec is optional, so the self-carrying lanes
    # (`Serialized` / `Deserialized`) and the typed publish entry point over them must stand with
    # no codec feature at all; only encoding and decoding may demand one.
    cargo check --workspace --exclude ruststream-no-std-probe --no-default-features \
        --features testing,memory,macros
    cargo check -p ruststream-no-std-probe
    # The contract builds without `std`, for a Cortex-M4F with an allocator and for a Cortex-M0+,
    # which has no compare-and-swap: there `bytes` takes `extra-platforms`, and the library build
    # takes `portable-atomic`'s single-core option in place of the one a firmware binary chooses.
    # RUSTFLAGS is set so that a host's own flags (a target CPU, a linker) stay off these targets.
    rustup target add thumbv7em-none-eabihf thumbv6m-none-eabi
    RUSTFLAGS="" cargo build -p ruststream-core --no-default-features --features alloc \
        --target thumbv7em-none-eabihf
    RUSTFLAGS="--cfg portable_atomic_unsafe_assume_single_core" cargo build -p ruststream-core \
        --no-default-features --features alloc,bytes/extra-platforms --target thumbv6m-none-eabi
    # The shared delivery body builds without `std`: with a heap for a Cortex-M4F, and as the
    # runner traits alone for both targets. The embedded runner builds for the Cortex-M4F.
    RUSTFLAGS="" cargo build -p ruststream-runtime --no-default-features --features alloc \
        --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream-runtime --no-default-features --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream-runtime --no-default-features --target thumbv6m-none-eabi
    RUSTFLAGS="" cargo build -p ruststream-runtime-embassy --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream-memory --no-default-features --features embassy \
        --target thumbv7em-none-eabihf
    # The embassy runtime: the facade with the embassy set, where every target that needs tokio
    # names it and drops out, then the embedded runner and the local bus with their tests.
    cargo clippy -p ruststream --all-targets --no-default-features --features {{ embassy_set }} \
        -- -D warnings
    cargo clippy -p ruststream-runtime-embassy --all-targets -- -D warnings
    cargo clippy -p ruststream-memory --all-targets --no-default-features --features embassy \
        -- -D warnings
    # The harness of the embedded runtime, with its tests and doc examples on the facade's
    # embassy side.
    cargo clippy -p ruststream-testing --all-targets --features embassy,json -- -D warnings
    # The embassy set keeps tokio out of the facade's dependency graph.
    tree="$(cargo tree -p ruststream --no-default-features --features {{ embassy_set }} \
        -e normal -i tokio)"; \
    if [ -n "$tree" ]; then \
        echo "error: the embassy set pulls tokio into the facade:" >&2; echo "$tree" >&2; exit 1; \
    fi
    # Both runtimes at once are a compile error that names them.
    if out="$(cargo check -p ruststream --features embassy 2>&1)"; then \
        echo "error: the facade built with both runtimes" >&2; exit 1; \
    fi; \
    grep -qF 'the `tokio` and `embassy` features select the runtime and exclude each other; enable one' \
        <<< "$out" || { echo "$out" >&2; exit 1; }
    # Rustdoc sees what rustc cannot: broken intra-doc links and redundant targets. CI gates on
    # it, so a link to an item a refactor removed must fail here rather than three jobs later.
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --features {{ workspace_features }} --no-deps
    # The shared delivery body also documents without the tokio runner's feature, the way a crate
    # on another runtime builds it: a link to an item that feature gates must not break there.
    RUSTDOCFLAGS="-D warnings" cargo doc -p ruststream-runtime --no-deps \
        --features json,msgpack,cbor,asyncapi,metrics,otel,testing,poll-diagnostics
    # The types the public API of each published crate names from its dependencies, as CI checks
    # them: the facade with each runtime, and each crate behind it where its items are defined.
    cargo +nightly-2026-03-20 check-external-types --features {{ tokio_set }} \
        --manifest-path Cargo.toml --config external-types.toml
    cargo +nightly-2026-03-20 check-external-types --no-default-features \
        --features {{ embassy_set }} --manifest-path Cargo.toml --config external-types.toml
    for manifest in crates/ruststream-{core,runtime,runtime-tokio,memory,asyncapi,observability,testing,cli,conformance}/Cargo.toml; do \
        cargo +nightly-2026-03-20 check-external-types --all-features \
            --manifest-path "$manifest" --config external-types.toml || exit 1; \
    done
    # The compile-fail snapshots: rustc's exact wording on the stable toolchain this repository
    # selects, which is what CI records them with. REQUIRE_UI_TESTS turns a skip into a failure,
    # so a lost opt-in shows here. `ui_codec_free` holds the errors that exist only where no
    # codec resolves, so it builds without one.
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test --features {{ tokio_set }} --test suite ui::
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test --no-default-features --features macros,memory,testing,tokio --test suite ui_codec_free::
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test -p ruststream-runtime-embassy --test ui

test:
    # The examples' tests are modules of one test binary, so `--tests` builds none per example;
    # `check` compiles every example on its own. `--tests` leaves the doc examples out, so they
    # run on their own line.
    cargo test --workspace {{ tokio_legs_exclude }} --features {{ workspace_features }} --tests
    cargo test --workspace {{ tokio_legs_exclude }} --features {{ workspace_features }} --doc
    # The embassy runtime's host tests: the embedded runner and the local bus.
    cargo test -p ruststream-runtime-embassy
    cargo test -p ruststream-memory --no-default-features --features embassy
    cargo test -p ruststream-testing --features embassy,json
    # The reduced-feature legs CI runs, in CI's order. A module compiled only with a feature off
    # is invisible to the workspace run above, so an API a refactor removed can survive there
    # until CI says otherwise: `codec_free_lanes` and `ui_codec_free` build only where no codec
    # resolves, and `lane_traits_without_macros` only where the derives are gone. The UI
    # snapshots themselves run in `check`; the run here is what compiles the module.
    cargo test --no-default-features --lib
    cargo test --no-default-features --features macros,memory,testing,tokio --test suite -- \
        raw_subscriber:: codec_free_lanes:: ui_codec_free::
    cargo test --no-default-features --features memory,testing,tokio --test suite lane_traits_without_macros::
    # The manual examples teach the API with `macros` off; build and run them that way.
    cargo check --no-default-features --features testing,memory,json,tokio --examples
    cargo test --no-default-features --features testing,memory,json,tokio --test examples
    cargo test --no-default-features --features testing,memory,json,tokio --example manual_app_running
    # Plain `cargo test`: every target needing a non-default feature has to gate itself. The
    # facade alone, since the crates behind it build their doc examples against the facade with
    # the features those examples name, which a workspace run would turn on for the facade too.
    cargo test -p ruststream
    # Both feature edges, because a run with every feature hides a doc example that names a
    # feature-gated item without gating itself.
    cargo test -p ruststream --doc
    cargo test -p ruststream --doc --no-default-features

# What a change costs per message: instructions and allocations through valgrind, then wall-clock
# times printed for reference, then the document the benchmarks page publishes.
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
cov: cov-run
    cargo llvm-cov report --workspace --summary-only --fail-under-lines 95

# HTML coverage report at target/llvm-cov/html/index.html, for finding the
# uncovered lines the gate complains about.
cov-html: cov-run
    cargo llvm-cov report --workspace --html

# The suite under instrumentation, as CI runs it: the embassy runtime's legs build the facade
# with the other runtime, so they are runs of their own, and a report merges every run.
cov-run:
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --workspace {{ tokio_legs_exclude }} --features {{ workspace_features }} --tests
    cargo llvm-cov --no-report -p ruststream-runtime-embassy --tests
    cargo llvm-cov --no-report -p ruststream-memory --no-default-features --features embassy --tests
    cargo llvm-cov --no-report -p ruststream-testing --features embassy,json --tests

build:
    cargo build --workspace --release --features {{ workspace_features }}

# The facade's rustdoc as docs.rs renders it, with the tokio set, at target/doc: what the site's
# API links resolve against.
doc:
    cargo doc --no-deps -p ruststream --features {{ tokio_set }}

clean:
    cargo clean

ci: check test

# Each `ruststream-*` repository next to the main checkout of this one runs
# `cargo test --workspace --all-features` against this working tree, on whatever branch it has
# checked out, so a core change and the broker adaptations it needs are tested together before
# either is committed. A worktree of the core finds the brokers through the main checkout. For the
# run, a broker's lock file takes `ruststream` from this tree instead of the release it pins, and
# is put back afterwards from a copy beside it (`Cargo.lock.just-brokers`, kept and named when the
# restore fails); a broker that would still build a release fails. One run at a time owns a broker
# checkout, and a run that finds it owned fails that broker, naming the owner.
# `just brokers nats fred` runs the named ones and fails on one it cannot test; with no names it
# runs every broker it finds and lists the repositories it skips (no crate, no `ruststream`
# dependency). The run fails when it tests no broker. Needs jq.
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
        backup="$PWD/Cargo.lock.just-brokers"
        owner="$PWD/Cargo.lock.just-brokers.owner"
        # One run owns a broker checkout from its backup to its restore, so overlapping runs from
        # other worktrees never share a backup. The lock is on the directory itself: it leaves no
        # file behind and dies with the run. A second run refuses rather than waits, because a
        # suite takes minutes and the run it would wait on may be stuck.
        exec 8< "$PWD" || exit 1
        if ! flock -n 8; then
            echo "error: $PWD is in use by another just brokers run" \
                "($(cat "$owner" 2>/dev/null || echo "holder unknown"))" >&2
            exit 1
        fi
        printf 'core %s, pid %s, since %s\n' "$core" "$BASHPID" "$(date -Is)" > "$owner"
        trap 'rm -f "$owner"' EXIT
        # A backup left by a killed run may be the only good copy of the lock file.
        if [ -e "$backup" ]; then
            echo "error: $backup is left from an earlier run: move it over Cargo.lock" \
                "or delete it" >&2
            exit 1
        fi
        if [ -f Cargo.lock ]; then
            # The backup sits beside the lock file, on its filesystem, so a full /tmp cannot cut
            # it, and the run stops before touching the lock unless the copy is whole.
            if ! cp Cargo.lock "$backup" || ! cmp -s Cargo.lock "$backup"; then
                rm -f "$backup"
                echo "error: could not back up $PWD/Cargo.lock; it was left as it was" >&2
                exit 1
            fi
            trap 'rm -f "$owner"; mv -f "$backup" Cargo.lock || {
                echo "error: Cargo.lock was not restored; the original is at $backup" >&2
                exit 1
            }' EXIT
        else
            trap 'rm -f "$owner" Cargo.lock' EXIT
        fi
        # The patch alone leaves the release the lock file pins in place. Cargo and the suite get
        # no handle on the directory lock, so a process they leave behind cannot keep it.
        cargo update --config "$patch" -p ruststream 8<&- || exit 1
        resolved="$(cargo metadata --format-version 1 --all-features --locked --config "$patch" \
            8<&- | jq -r '.packages[] | select(.name == "ruststream") | .manifest_path')" || exit 1
        if [ "$resolved" != "$core/Cargo.toml" ]; then
            echo "error: ruststream comes from ${resolved:-nowhere}, not from this tree" \
                "(ruststream $version): the patch is unused" >&2
            exit 1
        fi
        cargo test --workspace --all-features --locked --config "$patch" 8<&-
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
