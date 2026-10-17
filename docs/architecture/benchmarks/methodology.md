---
description: "How every RustStream crate measures itself: the three loops, the run, the report and the code measurement."
---

# Methodology

How every RustStream crate measures itself, so a number published for one broker means the same
thing as a number published for another.

Every broker crate follows the procedure below. A broker that departs from it says so on its own
page.

## The three loops

A run measures the same scenario three times over, and the three differ in exactly one thing: what
carries the messages.

- **Raw client.** The broker's own client, driven directly.
- **Broker crate.** The crate's own consumer and publisher - its subscription source, the
  subscriber stream it yields, its acknowledgement, its publisher - driven by a loop in the
  benchmark, with no runtime above them.
- **RustStream.** The whole service a user writes: the handler, the app, the dispatch.

The first difference is what a broker crate costs over the client it wraps, which is the crate's
own responsibility. The second is what the runtime costs on top, over that broker in particular -
and it is published per broker because the crates meet the runtime differently: how a stream
yields, whether deliveries arrive in batches, how back-pressure reaches the consumer.

- **Same client, same client configuration.** Prefetch, ack mode, consumer group, durability,
  connection count and any broker-specific tuning are identical on both sides. The framework side
  configures the broker through RustStream, and the resulting client settings still have to match.
- **Same ack position.** RustStream acks after the handler returns, so the other two loops ack in
  the same place. Batching acks at the end of the raw run measures a different protocol, not a
  different framework.
- **Same decode into the same type.** The loops without a handler deserialize the payload into the
  same struct with the same codec and touches a field through `std::hint::black_box`. Skipping this
  is the easiest way to produce a wrong number: the optimizer removes a decode whose result is
  unused, and the raw side silently stops decoding.
- **Same payload, byte for byte.** One generator produces the bodies every loop consumes.
- **Same runtime.** The tokio flavor, the worker thread count and the number of messages processed
  at once are the same everywhere.
- **Same build.** Profile, `RUSTFLAGS` and the allocator match, and the observability features
  (`logging`, `metrics`, `otel`) are enabled for every loop or for none. A machine with
  `-C target-cpu=native` in the environment produces numbers another machine cannot reproduce. The
  flags are therefore published with the results.

## The run

- **The consumer is attached before the first message is published.** Otherwise one side works
  through a backlog the broker already holds, and the other receives messages as they are
  published. In most brokers those are two different paths.
- **Every run gets its own names.** A fresh subject, queue, stream or consumer group per run, so
  run N never sees what run N-1 left behind.
- **The window starts at the first message received and ends at the ack of the last one.** A
  calibration run precedes the measured ones and its result is discarded. Connection setup,
  consumer registration and the first allocations are startup cost, not per-message cost.
- **The message count makes a run last at least five seconds**, so startup transients and timer
  resolution stay inside the noise.
- **The loops are interleaved, not blocked.** Raw, crate, framework, raw, crate, framework and so
  on, for three rounds. Running one loop to the end and then the next attributes every drift of
  the machine (thermal, background load, page cache) to whichever ran last.

## The report

- **Every loop reports three rounds: its best, its median and its worst.** Noise on the machine
  only ever slows a run down, so the fastest round is the closest to the undisturbed cost, the
  median is the round a rerun typically repeats, and the slowest says how far from quiet the
  machine was. A single number from a single run is not a result.
- **The table prints the best round with the median in parentheses.** The worst round stays in the
  document and out of the cell, because what it is there for is the spread: the distance from the
  best round to the worst.
- **A difference smaller than that spread is published as `indistinguishable`,** never as a
  percentage: a figure below the run-to-run noise reads as precision that was never measured.
- **A saturated consumer is flagged.** When the raw side spends the run waiting on the broker, the
  row is marked `broker-bound`.
- **The environment is published with the numbers**: CPU and core count, kernel, how the broker was
  started (image, container, host), the rustc version, the crate versions, the build profile and
  the flags. Without them a number cannot be reproduced or declared out of date.

## The code measurement

Each crate produces its [cost chart](index.md#cost-of-the-code) with a benchmark recipe of its own,
which its benchmarks page names: `just bench` in `ruststream`. The recipe needs valgrind and the
benchmark runner pinned to the version the crate depends on. `just bench 5000` measures every
scenario of `ruststream` over five thousand deliveries instead of a thousand: a steadier number for
a longer run, while the published document stays at the default.

- **Every scenario is the service a user writes**, started through the real runtime with the test
  harness compiled out, so what is measured is the code that ships. The numbers are absolute, and
  in `ruststream` they are the framework's own cost. The comparison against a client someone else
  wrote is the [comparison chart](index.md#against-a-raw-client), and the broker crates produce
  it.
- **In `ruststream` the transport is in process.** The framework's own code is the subject, so the
  numbers must not move with a socket, a server's load or a network. Both halves pay the same
  transport cost anyway, and it cancels in the difference. A broker crate measures over its real
  transport, and its benchmarks page says what that adds to the number.
- **The queue is filled before the measured region opens.** What a scenario measures is
  steady-state delivery, never the connect, the subscription open or the first allocation behind
  them.
- **Collection covers the measured region and nothing around it.** Setup and teardown run in the
  same process and through the same framework code, so a measurement that counted them would report
  the queue being filled as the cost of draining it.
- **Three runs per scenario, and what they are for.** One delivery, then two longer runs, the second
  twice the first: a thousand and two thousand deliveries in `ruststream`, and the counts a broker
  crate's benchmarks page gives. The difference between the last two is what a message costs once
  the service is running; the single delivery is the cold start. Nothing has to be switched off
  part way through a run, which is what makes this work for the allocation counter, whose counting
  cannot be toggled at all.
- **Two numbers per scenario.** Instructions from callgrind, which is exact and compared;
  allocations from DHAT, which is exact and held to the floor the scenario declares. In
  `ruststream`, a separate benchmark measures wall time on scenarios of its own. `just bench`
  prints those times, which are noisy and informational.
- **A comparison, not a threshold.** Run with `--baseline=main`, the recipe measures a branch
  against the same benchmarks run on the branch it targets, and fails when a scenario's
  instructions grow past the crate's own limit: two percent in `ruststream`, and in a broker crate
  the limit its benchmarks page states. The limit is relative, so it applies only against a
  baseline. Allocations are held to what the scenario declares on every run, with a baseline or
  without. The author of a pull request runs the comparison and cites its numbers, and a change
  that lowers a scenario lowers its declared floor in the same pull request.
  The same rules apply to the cold start.
