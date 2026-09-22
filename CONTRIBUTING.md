# Contributing to RustStream

RustStream is the core crate in this repository plus twelve broker crates, each in a repository
of its own. This page covers the environment, the checks a change passes before review, and how a
core change is tested on the broker crates.

## Repositories

The broker crates depend on the core through crates.io, and each repository builds on its own.
To work across them, clone them side by side in one directory:

```text
RustStream/
  ruststream/
  ruststream-amqp/
  ruststream-fred/
  ruststream-gcp-pubsub/
  ruststream-kinesis/
  ruststream-lapin/
  ruststream-nats/
  ruststream-pulsar/
  ruststream-rdkafka/
  ruststream-rumqttc/
  ruststream-sea-file/
  ruststream-sqs-sns/
  ruststream-zeromq/
```

```bash
mkdir RustStream && cd RustStream
for repo in ruststream ruststream-amqp ruststream-fred ruststream-gcp-pubsub ruststream-kinesis \
    ruststream-lapin ruststream-nats ruststream-pulsar ruststream-rdkafka ruststream-rumqttc \
    ruststream-sea-file ruststream-sqs-sns ruststream-zeromq; do
  git clone "https://github.com/powersemmi/$repo.git"
done
```

## Environment

- **Rust** through rustup. `rust-toolchain.toml` selects stable with rustfmt and clippy. The
  minimum supported version is 1.88: `rustup toolchain install 1.88` builds against it with
  `cargo +1.88 check --workspace --all-features`.
- **just**, which runs every recipe below.
- Per task:

| Task | Tool | Install |
| --- | --- | --- |
| `just deny` | cargo-deny | `cargo install cargo-deny --locked` |
| `just cov` | cargo-llvm-cov | `cargo install cargo-llvm-cov --locked` |
| `just bench` | valgrind and the benchmark runner | the system package manager, then `cargo install --locked gungraun-runner --version =0.19.4` |
| the documentation site | Python 3.12 | `pip install -r docs/requirements.txt`, then `properdocs serve` |
| a broker's live suite | Docker with Compose | the Docker documentation |

## Checking a change to the core

```bash
just check   # rustfmt, clippy, cargo check on the feature edges, rustdoc
just test    # the test suite, the reduced-feature legs, doctests on both feature edges
just ci      # both
```

The compile-fail snapshots record the exact wording of stable rustc and run on request:

```bash
RUN_UI_TESTS=1 cargo test --all-features --test ui
```

`just cov` holds line coverage at 95%, the same floor CI holds. `just bench --baseline=main`
compares instructions and allocations per message against `main`, and a pull request that changes
the cost cites its numbers.

## Testing a change on the broker crates

`just brokers` runs every broker crate's test suite against the core in this working tree:

```bash
just brokers            # every broker cloned next to the core
just brokers nats fred  # the named ones
```

Each broker runs `cargo test --workspace --all-features`, the same command as its own
`just test`, with its `ruststream` dependency patched to this checkout. The broker builds from
its own working tree, on the branch it has checked out and with its uncommitted edits. Its lock
file is restored after the run. The summary lists the brokers that passed and the ones that
failed, and the recipe fails when any did.

A change to the surface the brokers use goes like this:

1. Make the change in the core on a branch.
2. Adapt the affected brokers, each on a branch in its own repository.
3. Run `just brokers <name>...` from the core until every one passes.
4. Open the core pull request, then one pull request per broker. A broker's pull request raises
   its `ruststream` requirement to the core release that carries the change.

These suites run on each broker's in-process transport. A change to the broker contract is also
checked against a real broker. In the broker's repository, `just test-brokers` starts its
Compose stand and runs the live suite against the core from crates.io. Against the local core,
start the stand with `just brokers-up`, run the command `test-brokers` runs with
`--config "patch.crates-io.ruststream.path='../ruststream'"` added to `cargo test`, and stop the
stand with `just brokers-down`.

## Pull requests

- One logical change per pull request.
- Open it as a draft. CI runs when it is marked ready for review.
- Merging needs the `CI result` check, one approving review, linear history (rebase or squash),
  and signed commits.
- Documentation changes with the code. An item's rustdoc says what it is and does. A module
  overview says how a topic is done. The site holds the entry pages in English, Russian and
  Chinese, and an edit reaches all three.
