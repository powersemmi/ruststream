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

# A compio service's production surface: the compio runner over the local memory bus, no tokio.
compio_bench_features := "compio,memory,macros,json"

# The members a test or coverage leg leaves out: the embedded runner's own tests link embassy-time's
# mock driver, and the embedded harness is a time driver of its own, so one binary cannot hold both.
# Its legs below build it alone.
embassy_tests_exclude := "--exclude ruststream-runtime-embassy"

# The two runtimes, `tokio` and `embassy`, are additive features: every workspace leg builds with
# all of them. The embassy set is the facade of a device service, the one build that leaves tokio
# out of the dependency graph.
embassy_set := "cbor,embassy,json,macros,memory,msgpack"

# The scenarios that count instructions and allocations, one benchmark file each. A run against a
# baseline holds the gated ones to an instruction limit; the ungated one is measured and reported,
# and held to no number. The wall-clock one is in neither list: it runs under a different harness,
# which takes none of the arguments below.
gated_benches := "--bench consume_json --bench consume_json_kilobyte --bench consume_lane --bench consume_pool --bench consume_threads --bench consume_threads_by_key --bench middleware --bench batch --bench batch_pool --bench reply --bench reply_lending --bench out_slot --bench out_slot_lending --bench typed_headers_write --bench typed_headers_read --bench request_reply --bench continuation"
ungated_benches := "--bench retry_copy --bench retry_copy_deferred"

default: check

check: && check-firmware
    cargo fmt --all -- --check
    # The check crates are workspaces of their own, which `--all` above does not reach.
    for manifest in checks/{embedded-broker,firmware-noalloc,no-std-probe,noalloc}/Cargo.toml; do \
        cargo fmt --manifest-path "$manifest" --all -- --check || exit 1; \
    done
    # The site's excerpts: every `reference=` names a file and a region that exist, and the fences
    # follow the page rules. The site build fails on the same file, minutes later.
    python3 scripts/lint_doc_fences.py
    # Clippy lints every crate and feature set the gates build, so no build compiles code that no
    # lint has read: a build that only checks is a clippy leg, and a build that makes code (a
    # library for a device target, the firmware) is followed by a clippy leg of its configuration.
    # The workspace leg holds every member with every feature; the benchmarks are left out of it,
    # because the harness feature is a compile error in them, and have legs of their own on the
    # production feature set they are measured with.
    cargo clippy --workspace {{ embassy_tests_exclude }} --lib --bins --tests --examples \
        --all-features \
        -- -D warnings
    cargo clippy --benches --no-default-features --features {{ bench_features }} -- -D warnings
    cargo clippy --bench consume_compio --no-default-features \
        --features {{ compio_bench_features }} -- -D warnings
    cargo clippy --workspace --no-default-features -- -D warnings
    # The embedded runner on its own, in both variants (a workspace build above has the variant
    # its neighbours chose).
    cargo clippy -p ruststream-runtime-embassy --no-default-features -- -D warnings
    cargo clippy -p ruststream-runtime-embassy --features alloc -- -D warnings
    # The codec-free build. A codec is optional, so the self-carrying lanes
    # (`Serialized` / `Deserialized`) and the typed publish entry point over them must stand with
    # no codec feature at all; only encoding and decoding may demand one.
    cargo clippy --workspace --no-default-features \
        --features testing,memory,macros -- -D warnings
    # The default codec's fallbacks: with `json` off, `cbor` and then `msgpack` name the default
    # codec, and only a build with that codec alone compiles its alias.
    cargo clippy --no-default-features --features cbor,memory,macros -- -D warnings
    cargo clippy --no-default-features --features msgpack,memory,macros -- -D warnings
    # The core's `send` feature is additive: a build that enables it without the Tokio runner (any
    # crate may) takes the `Send` bounds in every boxed future of the runtime as well.
    cargo clippy -p ruststream-runtime --features ruststream-core/send -- -D warnings
    cargo clippy -p ruststream-runtime --no-default-features --features alloc,ruststream-core/send \
        -- -D warnings
    # The contract's `alloc` is a variant any crate of the graph may turn on (a broker, a helper),
    # so every crate of the family must build with it on and its own `alloc` off.
    cargo clippy -p ruststream-runtime --no-default-features --features ruststream-core/alloc \
        -- -D warnings
    cargo clippy -p ruststream-memory --no-default-features \
        --features embassy,ruststream-core/alloc -- -D warnings
    cargo clippy -p ruststream-testing --no-default-features \
        --features embassy,ruststream-core/alloc -- -D warnings
    cargo clippy -p ruststream-runtime-embassy --no-default-features \
        --features ruststream-core/alloc -- -D warnings
    cargo clippy -p ruststream-conformance --no-default-features --features ruststream-core/alloc \
        -- -D warnings
    cargo clippy -p ruststream --no-default-features \
        --features embassy,macros,memory,ruststream-core/alloc -- -D warnings
    # The firmware facade with no serde codec: an embedded handler's `Out` slot mounts, and only
    # an encoding publish asks for a codec. The benchmarks of a device app without `alloc` build
    # with the same set.
    cargo clippy -p ruststream --no-default-features --features embassy,macros,memory --tests \
        --benches -- -D warnings
    # The macros' output in a crate without the standard library prelude: the probe is a workspace
    # of its own, so no workspace build carries its features. A path dependency is no member of
    # the probe's workspace, so the facade it builds is linted from this one.
    cargo clippy --manifest-path checks/no-std-probe/Cargo.toml -- -D warnings
    cargo clippy -p ruststream --no-default-features --features json,macros,tokio -- -D warnings
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
    # runner traits alone for both targets. The embedded runner builds for the Cortex-M4F, and so
    # does a device service's facade: the embassy set with the cbor or json codec.
    RUSTFLAGS="" cargo build -p ruststream-runtime --no-default-features --features alloc \
        --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream-runtime --no-default-features --features alloc,json \
        --target thumbv7em-none-eabihf
    # The same on a Cortex-M4F: a firmware whose dependency needs a heap has an allocator, so the
    # embassy set builds with the contract's `alloc` on and the framework's own off.
    RUSTFLAGS="" cargo build -p ruststream-runtime-embassy --no-default-features \
        --features ruststream-core/alloc --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream --no-default-features \
        --features embassy,macros,memory,ruststream-core/alloc --target thumbv7em-none-eabihf
    # The same builds, linted on their targets.
    RUSTFLAGS="" cargo clippy -p ruststream-core --no-default-features --features alloc \
        --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="--cfg portable_atomic_unsafe_assume_single_core" cargo clippy -p ruststream-core \
        --no-default-features --features alloc,bytes/extra-platforms --target thumbv6m-none-eabi \
        -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream-runtime --no-default-features --features alloc \
        --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream-runtime --no-default-features --features alloc,json \
        --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream-runtime-embassy --no-default-features \
        --features ruststream-core/alloc --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream --no-default-features \
        --features embassy,macros,memory,ruststream-core/alloc --target thumbv7em-none-eabihf \
        -- -D warnings
    # The contract's own unit tests without `alloc` are held to the lints of its sources.
    cargo clippy --manifest-path checks/noalloc/core/Cargo.toml --lib --tests -- -D warnings
    # The delivery body without `alloc`, and the crate that runs its own unit tests, are held to the
    # same lints.
    cargo clippy -p ruststream-runtime --no-default-features --lib -- -D warnings
    cargo clippy -p ruststream-runtime --no-default-features --features postcard,json-core --lib \
        -- -D warnings
    cargo clippy --manifest-path checks/noalloc/runtime/Cargo.toml --lib --tests \
        -- -D warnings
    # The embedded runner without `alloc`, and the crate that runs its unit tests.
    cargo clippy -p ruststream-runtime-embassy --no-default-features --lib -- -D warnings
    cargo clippy --manifest-path checks/noalloc/runtime-embassy/Cargo.toml --lib --tests \
        -- -D warnings
    RUSTFLAGS="" cargo build -p ruststream-runtime --no-default-features --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream-runtime --no-default-features --target thumbv6m-none-eabi
    RUSTFLAGS="" cargo build -p ruststream-runtime-embassy --target thumbv7em-none-eabihf
    # The diagnostics of a build without `alloc`: through `defmt` on a Cortex-M, through both
    # sinks, and through `defmt` on the target that has no atomics `tracing` could use.
    RUSTFLAGS="" cargo build -p ruststream --no-default-features \
        --features embassy,macros,memory,defmt --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream --no-default-features \
        --features embassy,macros,memory,defmt,log --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream-runtime --no-default-features --features defmt \
        --target thumbv6m-none-eabi
    RUSTFLAGS="" cargo build -p ruststream-memory --no-default-features --features embassy \
        --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream --no-default-features \
        --features cbor,embassy,macros,memory --target thumbv7em-none-eabihf
    RUSTFLAGS="" cargo build -p ruststream --no-default-features \
        --features json,embassy,macros,memory --target thumbv7em-none-eabihf
    # The same builds, linted on their targets.
    RUSTFLAGS="" cargo clippy -p ruststream-runtime --no-default-features \
        --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream-runtime --no-default-features \
        --target thumbv6m-none-eabi -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream-runtime-embassy --target thumbv7em-none-eabihf \
        -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream --no-default-features \
        --features embassy,macros,memory,defmt --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream --no-default-features \
        --features embassy,macros,memory,defmt,log --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream-runtime --no-default-features --features defmt \
        --target thumbv6m-none-eabi -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream-memory --no-default-features --features embassy \
        --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream --no-default-features \
        --features cbor,embassy,macros,memory --target thumbv7em-none-eabihf -- -D warnings
    RUSTFLAGS="" cargo clippy -p ruststream --no-default-features \
        --features json,embassy,macros,memory --target thumbv7em-none-eabihf -- -D warnings
    # The embassy runtime alone, as a device service builds it: the facade with the embassy set,
    # where every target that needs tokio names it and drops out, then the embedded runner and
    # the local bus with their tests.
    cargo clippy -p ruststream --all-targets --no-default-features --features {{ embassy_set }} \
        -- -D warnings
    cargo clippy -p ruststream-runtime-embassy --all-targets -- -D warnings
    cargo clippy -p ruststream-memory --all-targets --no-default-features --features embassy \
        -- -D warnings
    # The harness of the embedded runtime, with its tests and doc examples on the facade's
    # embassy side.
    cargo clippy -p ruststream-testing --all-targets --features embassy,json -- -D warnings
    # A broker crate on the embedded runner, whose types are never `Send`, with its contract
    # checks: the facade's `conformance` feature without tokio, in both variants.
    cargo clippy --manifest-path checks/embedded-broker/Cargo.toml --all-targets -- -D warnings
    cargo clippy --manifest-path checks/embedded-broker/Cargo.toml --all-targets \
        --features ruststream/alloc -- -D warnings
    # The facade as that broker builds it: a path dependency is no member of its workspace.
    cargo clippy -p ruststream --no-default-features --features conformance,embassy,memory,testing \
        -- -D warnings
    cargo clippy -p ruststream --no-default-features \
        --features alloc,conformance,embassy,memory,testing -- -D warnings
    # The test builds of `just test` and of the compile-fail snapshots below, in the feature sets
    # no leg above has: the facade with its default features and with none, the suite without a
    # codec and without the macros, the manual examples (`cargo test --example` builds an example
    # as a test), the harness on the tokio runner alone, the local suites beside the embedded
    # runner, and the CLI's compile-fail suite.
    cargo clippy -p ruststream --lib --bins --tests --examples -- -D warnings
    cargo clippy --no-default-features --lib --tests -- -D warnings
    cargo clippy --no-default-features --features macros,memory,testing,tokio --test suite \
        -- -D warnings
    cargo clippy --no-default-features --features memory,testing,tokio --test suite -- -D warnings
    cargo clippy --no-default-features --features testing,memory,json,tokio --examples \
        --test examples -- -D warnings
    cargo clippy --no-default-features --features testing,memory,json,tokio \
        --example manual_app_running --profile test -- -D warnings
    cargo clippy -p ruststream-testing --all-targets --features tokio,json -- -D warnings
    cargo clippy --no-default-features --features memory,testing,json,embassy --test local_family \
        -- -D warnings
    cargo clippy -p ruststream-cli --all-targets -- -D warnings
    # The builds of `just test-no-alloc`: the facade's tests without `alloc`, one leg per feature
    # set, and the local bus and the delivery body as crates of their own.
    cargo clippy --no-default-features --features embassy,memory,macros,testing \
        --test embedded_no_alloc -- -D warnings
    cargo clippy --no-default-features --features embassy,memory,macros,testing,postcard \
        --test embedded_no_alloc_out --test embedded_no_alloc_out_retry \
        --test embedded_no_alloc_batch --test embedded_no_alloc_batch_input \
        --test embedded_no_alloc_batch_reply --test embedded_no_alloc_batch_out \
        --test embedded_no_alloc_name --test doc_device --test doc_device_manual \
        --test ui_no_alloc -- -D warnings
    cargo clippy --no-default-features --features embassy,memory,macros,testing,log \
        --test embedded_diagnostics -- -D warnings
    cargo clippy --manifest-path checks/noalloc/memory/Cargo.toml --all-targets --features testing \
        -- -D warnings
    cargo clippy --manifest-path checks/noalloc/memory/Cargo.toml --all-targets -- -D warnings
    cargo clippy --manifest-path checks/noalloc/runtime/Cargo.toml --lib --tests \
        --features postcard,json-core -- -D warnings
    # The embassy set keeps tokio out of the facade's dependency graph.
    tree="$(cargo tree -p ruststream --no-default-features --features {{ embassy_set }} \
        -e normal -i tokio)"; \
    if [ -n "$tree" ]; then \
        echo "error: the embassy set pulls tokio into the facade:" >&2; echo "$tree" >&2; exit 1; \
    fi
    # The compio set keeps tokio out of the facade's dependency graph.
    tree="$(cargo tree -p ruststream --no-default-features --features {{ compio_bench_features }} \
        -e normal -i tokio)"; \
    if [ -n "$tree" ]; then \
        echo "error: the compio set pulls tokio into the facade:" >&2; echo "$tree" >&2; exit 1; \
    fi
    # Rustdoc sees what rustc cannot: broken intra-doc links and redundant targets. CI gates on
    # it, so a link to an item a refactor removed must fail here rather than three jobs later.
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
    # The shared delivery body also documents without the tokio runner's feature, the way a crate
    # on another runtime builds it: a link to an item that feature gates must not break there.
    RUSTDOCFLAGS="-D warnings" cargo doc -p ruststream-runtime --no-deps \
        --features json,msgpack,cbor,asyncapi,metrics,otel,testing,poll-diagnostics
    # The types the public API of each published crate names from its dependencies, as CI checks
    # them: the facade with both runtimes and with embassy alone, and each crate behind it where its items are defined.
    cargo +nightly-2026-03-20 check-external-types --all-features \
        --manifest-path crates/ruststream/Cargo.toml --config external-types.toml
    cargo +nightly-2026-03-20 check-external-types --no-default-features \
        --features {{ embassy_set }} --manifest-path crates/ruststream/Cargo.toml --config external-types.toml
    for manifest in crates/{core,runtime,runtime-tokio,runtime-compio,memory,asyncapi,observability,testing,cli,conformance}/Cargo.toml; do \
        cargo +nightly-2026-03-20 check-external-types --all-features \
            --manifest-path "$manifest" --config external-types.toml || exit 1; \
    done
    # The compile-fail snapshots: rustc's exact wording on the stable toolchain this repository
    # selects, which is what CI records them with. REQUIRE_UI_TESTS turns a skip into a failure,
    # so a lost opt-in shows here. `ui_codec_free` holds the errors that exist only where no
    # codec resolves, so it builds without one.
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test --all-features --test suite ui::
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test --no-default-features --features macros,memory,testing,tokio --test suite ui_codec_free::
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test -p ruststream-runtime-embassy --test ui
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test -p ruststream-cli --test ui

# A firmware binary on the no-alloc build, linked as a board links it, with no global allocator.
check-firmware:
    rustup target add thumbv7em-none-eabihf thumbv6m-none-eabi
    # The bus check below reads the ELF with the toolchain's own `llvm-nm`.
    rustup component add llvm-tools
    # It fails to link while any crate of the build needs a heap. Once with no codec and once for
    # each slice codec.
    for target in thumbv7em-none-eabihf thumbv6m-none-eabi; do \
        for features in "" postcard json-core; do \
            RUSTFLAGS="" cargo build --locked --manifest-path checks/firmware-noalloc/Cargo.toml \
                --target $target ${features:+--features $features} || exit 1; \
        done; \
    done
    # The same binary with defmt: it links with `DefmtLogger` selected and with defmt compiled and
    # no logger selected, and the optimised ELF carries the framework's defmt code only in the first.
    for target in thumbv7em-none-eabihf thumbv6m-none-eabi; do \
        checks/firmware-noalloc/check-defmt.sh $target || exit 1; \
    done
    # The board's bus lies in `.bss`: its empty state is all zero bytes, so the flash keeps no
    # image of it.
    for target in thumbv7em-none-eabihf thumbv6m-none-eabi; do \
        checks/firmware-noalloc/check-bss.sh $target || exit 1; \
    done
    # The binary linted in each configuration above, and the facade as the binary builds it: a
    # path dependency is no member of the firmware's workspace. A Cortex-M0+ has no
    # compare-and-swap, so there the facade takes `portable-atomic`'s single-core option in place
    # of the critical section the firmware chooses.
    for target in thumbv7em-none-eabihf thumbv6m-none-eabi; do \
        for features in "" postcard json-core defmt defmt-unselected; do \
            RUSTFLAGS="" cargo clippy --locked --manifest-path checks/firmware-noalloc/Cargo.toml \
                --target $target ${features:+--features $features} -- -D warnings || exit 1; \
        done; \
        flags=""; \
        if [ $target = thumbv6m-none-eabi ]; then \
            flags="--cfg portable_atomic_unsafe_assume_single_core"; \
        fi; \
        for features in "" postcard json-core defmt; do \
            RUSTFLAGS="$flags" cargo clippy -p ruststream --no-default-features \
                --features embassy,macros,memory${features:+,$features} --target $target \
                -- -D warnings || exit 1; \
        done; \
    done

# The embassy runtime without a heap: the facade of a device service with no `alloc`, the facade
# a device broker's contract checks name, and the crates under them, each as its own invocation
# so a workspace neighbour cannot unify `alloc` on.
# The tree check comes first and fails the leg if the framework's `alloc` is in any graph; the
# tests run under an allocator that fails the framework's own allocations.
test-no-alloc:
    for graph in "-p ruststream --features embassy,memory,macros,testing" \
        "-p ruststream --features embassy,conformance" "-p ruststream-runtime-embassy"; do \
        tree="$(cargo tree $graph --no-default-features -e features,normal,build -i ruststream-core -i ruststream-runtime)"; \
        if printf '%s\n' "$tree" | grep -E 'ruststream-(core|runtime) feature "alloc"'; then \
            echo "error: the framework's alloc feature is on in: $graph" >&2; exit 1; \
        fi; \
    done
    tree="$(cargo tree --manifest-path checks/noalloc/memory/Cargo.toml --features testing --all-targets -e features -i ruststream-core -i ruststream-runtime)"; \
    if printf '%s\n' "$tree" | grep -E 'ruststream-(core|runtime) feature "alloc"'; then \
        echo "error: the framework's alloc feature is on in the local bus crate" >&2; exit 1; \
    fi
    cargo test --no-default-features --features embassy,memory,macros,testing --test embedded_no_alloc
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test embedded_no_alloc_out
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test embedded_no_alloc_out_retry
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test embedded_no_alloc_batch
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test embedded_no_alloc_batch_input
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test embedded_no_alloc_batch_reply
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test embedded_no_alloc_batch_out
    cargo test --no-default-features --features embassy,memory,macros,testing,log --test embedded_diagnostics
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test embedded_no_alloc_name
    # The device service the embedded runtime page teaches, under the failing allocator.
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test doc_device
    cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test doc_device_manual
    # The device build's own futures, without the harness that wraps them in the test above.
    cargo test --no-default-features --features embassy,memory,macros --test embedded_future_size
    # The no-alloc publish stand's scenarios deliver what they publish.
    cargo test --no-default-features --features embassy,memory,macros \
        --test embedded_publish_no_alloc_check
    RUN_UI_TESTS=1 REQUIRE_UI_TESTS=1 cargo test --no-default-features --features embassy,memory,macros,testing,postcard --test ui_no_alloc
    cargo test --manifest-path checks/noalloc/memory/Cargo.toml --features testing
    # Without the harness as well: the bus's claim that it allocates nothing is checked there.
    cargo test --manifest-path checks/noalloc/memory/Cargo.toml
    # The contract's and the runtime's own unit tests of the variant: their dev-dependencies turn
    # `alloc` on, so the sources build as crates of their own that have none.
    cargo test --manifest-path checks/noalloc/core/Cargo.toml --lib
    cargo test --manifest-path checks/noalloc/runtime/Cargo.toml --lib
    cargo test --manifest-path checks/noalloc/runtime/Cargo.toml --lib --features postcard,json-core
    # The same for the embedded runner.
    cargo test --manifest-path checks/noalloc/runtime-embassy/Cargo.toml --lib

test:
    # The examples' tests are modules of one test binary, so `--tests` builds none per example;
    # `check` compiles every example on its own. `--tests` leaves the doc examples out, so they
    # run on their own line. The all-features build has `defmt` compiled and no `DefmtLogger`
    # selected; `link_defmt_unselected` is the test that links and runs it.
    cargo test --workspace {{ embassy_tests_exclude }} --all-features --tests
    cargo test --workspace {{ embassy_tests_exclude }} --all-features --doc
    # The embassy runtime's host tests: the embedded runner and the local bus.
    cargo test -p ruststream-runtime-embassy --features alloc
    cargo test -p ruststream-memory --no-default-features --features embassy
    cargo test -p ruststream-testing --features embassy,json
    # The same crate on the tokio runner alone: every embedded target gates itself on embassy.
    cargo test -p ruststream-testing --features tokio,json
    # The Tokio runner off: the contract traits ask nothing of a broker's types, and a broker that
    # keeps its state in `Rc` runs a body written once for every broker.
    cargo test --no-default-features --features memory,testing,json,embassy --test local_family
    # The local conformance suites against such a broker, through the facade's `conformance`
    # feature beside the embedded runner: without a heap and with one.
    cargo test --manifest-path checks/embedded-broker/Cargo.toml
    cargo test --manifest-path checks/embedded-broker/Cargo.toml --features ruststream/alloc
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
    # The embassy runtime without a heap, under an allocator that fails the framework's own.
    just test-no-alloc

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
    cargo bench -p ruststream-runtime-embassy --bench embedded_idle --no-run
    cargo bench --no-default-features --features embassy,memory,macros --bench embedded_idle_no_alloc \
        --bench embedded_publish_no_alloc --no-run
    cargo test --no-default-features --features embassy,memory,macros \
        --test embedded_publish_no_alloc_check --no-run
    cargo bench --no-default-features --features {{ compio_bench_features }} --bench consume_compio \
        --no-run
    status=0
    cargo bench {{ gated_benches }} --no-fail-fast \
        --no-default-features --features {{ bench_features }} \
        -- --output-format=json "${limits[@]}" "$@" > target/bench-summary.json || status=$?
    cargo bench {{ ungated_benches }} --no-fail-fast \
        --no-default-features --features {{ bench_features }} \
        -- --output-format=json "$@" >> target/bench-summary.json || status=$?
    cargo bench --bench wall_clock --no-default-features --features {{ bench_features }} \
        || status=$?
    # The embedded app builds without tokio, so its scenario is a bench of the embedded runner's own
    # crate, and its allocation floor is absolute like the others; its summaries join the rest.
    cargo bench -p ruststream-runtime-embassy --bench embedded_idle --no-fail-fast \
        -- --output-format=json "${limits[@]}" "$@" >> target/bench-summary.json || status=$?
    # The same app without `alloc`: its floor is zero allocations, the start included.
    cargo bench --no-default-features --features embassy,memory,macros \
        --bench embedded_idle_no_alloc --no-fail-fast \
        -- --output-format=json "${limits[@]}" "$@" >> target/bench-summary.json || status=$?
    # The same app with a handler that publishes, measured only where its check finds every
    # message arriving: a broken path would measure as a faster one.
    if cargo test --no-default-features --features embassy,memory,macros \
        --test embedded_publish_no_alloc_check; then
        cargo bench --no-default-features --features embassy,memory,macros \
            --bench embedded_publish_no_alloc --no-fail-fast \
            -- --output-format=json "${limits[@]}" "$@" >> target/bench-summary.json || status=$?
    else
        status=1
    fi
    # The compio runner on one core, over the local bus: its floor holds a delivery to zero
    # allocations after the start.
    cargo bench --no-default-features --features {{ compio_bench_features }} \
        --bench consume_compio --no-fail-fast \
        -- --output-format=json "${limits[@]}" "$@" >> target/bench-summary.json || status=$?
    python3 scripts/bench_results.py --messages "$messages" target/bench-summary.json \
        docs/benchmarks/results.json
    exit "$status"

fmt:
    cargo fmt --all
    # The check crates are workspaces of their own, which `--all` above does not reach.
    for manifest in checks/{embedded-broker,firmware-noalloc,no-std-probe,noalloc}/Cargo.toml; do \
        cargo fmt --manifest-path "$manifest" --all || exit 1; \
    done

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

# The suite under instrumentation, as CI runs it: the embassy runtime alone is a run of its own,
# and a report merges every run.
cov-run:
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --workspace {{ embassy_tests_exclude }} --all-features --tests
    cargo llvm-cov --no-report -p ruststream-runtime-embassy --tests
    cargo llvm-cov --no-report -p ruststream-memory --no-default-features --features embassy --tests
    cargo llvm-cov --no-report -p ruststream-testing --features embassy,json --tests

build:
    cargo build --workspace --release --all-features

# The facade's rustdoc as docs.rs renders it, with every feature, at target/doc: what the site's
# API links resolve against.
doc:
    cargo doc --no-deps -p ruststream --all-features

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
    core="$(pwd)/crates/ruststream"
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
        printf 'core %s, pid %s, since %s\n' "$core" "${BASHPID:-$$}" "$(date -Is)" > "$owner"
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
