# Benchmarks

A framework between the broker client and your handler costs time on every message: the
subscription stream, the decode, the dispatch, the ack. This page publishes that cost twice, as
two measurements that answer different questions.

The first is throughput against the raw client, on a real broker, doing the same work on the same
machine. It says what a deployed service pays.

The second is the cost of the framework's own code: instructions and allocations per message, with
no broker in the number. It says what changed when the framework changed, and it is precise enough
to fail a pull request that makes a message more expensive.

Each crate measures itself and publishes its own numbers. This page loads them and shows them
together. Nothing is copied here, so a crate that remeasures itself changes the tables below the
next time it publishes its documentation.

## Results

### Against a raw client

Medians over interleaved rounds, with the observed spread in parentheses. `Broker crate` is the
crate's own consumer and publisher driven without the runtime, so the two differences read apart:
what the crate costs over the client it wraps, and what the runtime costs on top of that.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading published results...", "broker": "Broker", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "measured": "measured", "details": "Full results and methodology", "pending": "No results published yet: {brokers}.", "crate": "Crate", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start"}'></div>

### Cost of the code

Instructions and allocations per message in the steady state, measured on the in-process
transport. These are absolute figures for the framework's own code, not a comparison: what the
framework costs over a broker's own client is the table above, where the client is a real one.

<div id="benchmark-code"></div>

`Cold start` is what starting the service and handling the first delivery cost together,
instructions and allocations, and a service pays it once rather than per message. The line under
the table is the machine the run was taken on, down to the memory, because an instruction count is
comparable across machines only once you know they ran the same code.

## What the numbers are

### The comparison against a raw client

Each row was measured by whoever maintains that broker crate, on their own machine, against a
broker on localhost. Rows are therefore not comparable with each other: the absolute throughput of
one says nothing about another. Only the two columns inside a row are comparable, and that is the
comparison this page exists for.

A broker on localhost is the harshest setting for the framework. There is no network latency here,
so the same absolute cost per message comes out as a larger percentage than it would against a
broker across a real network. Read the percentage as an upper bound on what a deployed service
pays, not as a typical figure.

The `broker-bound` mark means the raw client spent most of the run waiting on the socket. The
framework does its work inside that wait, and the measured difference comes out near zero. For that
workload it is a real result: this is what a saturated consumer looks like. But the number is a
lower bound on the cost of dispatch, not a measurement of it, and you cannot read it as "free".

### The cost of the code

An instruction count is exact. Two runs of the same binary give the same number, and a machine
twice as fast gives the same number too, so the rows of this table are comparable with each other
and with the same row measured on another machine. What it does not tell you is time: the same
count costs more where it misses the cache, which is what the wall-clock pair below the table is
for.

The number is this crate's own work and nothing else: the service a user writes, over the
in-process queue, decoding the payload into a type, reading a field and settling the delivery. No
broker is in it, so the figure moves when the framework's code moves and at no other time, which is
what lets two percent count as a defect rather than as noise.

Every per-message figure is the steady state. Starting a service costs what it costs once - the
connect, the subscription, the first allocations behind them - and dividing that over the messages
of a run would publish it as a price per message it is not. So a scenario is measured over a
thousand deliveries and over two thousand, and what a message costs is the difference between the
two runs; the cold start is measured on its own, over a single delivery.

Allocations are counted per message, and on the delivery path the figure is zero: once the service
is running, a message goes from the queue to the handler body without the framework asking the
allocator for anything. On the publish path the figure is what the broker takes to own the message
it is handed, and nothing above it.

## Methodology

Every broker crate follows the procedure below, so a number published for one broker means the same
thing as a number published for another. A broker that departs from the procedure says so on its own
page.

### The three loops

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
- **Same decode into the same type.** The loops without a handler deserialize the payload into the same struct
  with the same codec and touches a field through `std::hint::black_box`. Skipping this is the
  easiest way to produce a wrong number: the optimizer removes a decode whose result is unused, and
  the raw side silently stops decoding.
- **Same payload, byte for byte.** One generator produces the bodies every loop consumes.
- **Same runtime.** The tokio flavor, the worker thread count and the number of messages processed
  at once are the same everywhere.
- **Same build.** Profile, `RUSTFLAGS` and the allocator match, and the observability features
  (`logging`, `metrics`, `otel`) are enabled for every loop or for none. A machine with
  `-C target-cpu=native` in the environment produces numbers another machine cannot reproduce. The
  flags are therefore published with the results.

### The run

- **The consumer is attached before the first message is published.** Otherwise one side works
  through a backlog the broker already holds, and the other receives messages as they are
  published. In most brokers those are two different paths.
- **Every run gets its own names.** A fresh subject, queue, stream or consumer group per run, so
  run N never sees what run N-1 left behind.
- **The window starts at the first message received and ends at the ack of the last one.** A warm-up
  run precedes the measured ones and its result is discarded. Connection setup, consumer
  registration and the first allocations are startup cost, not per-message cost.
- **The message count makes a run last at least five seconds**, so startup transients and timer
  resolution stay inside the noise.
- **The loops are interleaved, not blocked.** Raw, crate, framework, raw, crate, framework and so
  on, for at least eleven rounds, discarding the first. Running one loop to the end and then the
  next attributes every drift of the machine (thermal, background load, page cache) to whichever
  ran last.

### The report

- **Every loop reports a median and a spread**, over the rounds that were kept. A single number from
  a single run is not a result.
- **A difference smaller than the spread is published as `indistinguishable`,** never as a
  percentage: a figure below the run-to-run noise reads as precision that was never measured.
- **A saturated consumer is flagged.** When the raw side spends the run waiting on the broker, the
  row is marked `broker-bound`.
- **The environment is published with the numbers**: CPU and core count, kernel, how the broker was
  started (image, container, host), the rustc version, the crate versions, the build profile and
  the flags. Without them a number cannot be reproduced or declared out of date.

### The code measurement

`just bench` in the crate's repository produces the second table. It needs valgrind and the
benchmark runner pinned to the version the crate depends on. `just bench 5000` measures
every scenario over five thousand deliveries instead of a thousand: a steadier number for a longer
run, while the published document and the CI gate stay at the default.

- **Every scenario is the service a user writes**, started through the real runtime with the test
  harness compiled out, so what is measured is the code that ships. Nothing is compared against a
  hand-written loop here: the queue such a loop would read is this crate's own in-memory broker,
  which makes the comparison a crate measuring itself. The comparison against a client someone
  else wrote is the first table, and it is the broker crates that produce it.
- **The transport is in process.** The framework's own code is the subject, so the numbers must not
  move with a socket, a server's load or a network. Both halves pay the same transport cost anyway,
  and it cancels in the difference.
- **The queue is filled before the measured region opens.** What a scenario measures is
  steady-state delivery, never the connect, the subscription open or the first allocation behind
  them.
- **Collection covers the measured region and nothing around it.** Setup and teardown run in the
  same process and through the same framework code, so a measurement that counted them would report
  the queue being filled as the cost of draining it.
- **Three runs per scenario, and what they are for.** One delivery, a thousand, and two thousand.
  The difference between the last two is what a message costs once the service is running; the
  single delivery is the cold start. Nothing has to be switched off part way through a run, which
  is what makes this work for the allocation counter, whose counting cannot be toggled at all.
- **Three numbers per scenario.** Instructions from callgrind, which is exact and the gate;
  allocations from DHAT, which is exact and the gate; wall time from a separate run, which is noisy
  and informational.
- **A gate on the change, not on the value.** A pull request carrying the `run-bench` label is
  measured against the same benchmarks run on the target branch: more than two percent of
  instructions in a gated scenario fails it, and so does an allocation above what the scenario
  declares. The table lands as a comment on the pull request. The cold start and the wall clock
  only print.

## Publishing results

A broker crate runs its own harness with `just bench` against the broker in its compose file. It
publishes the outcome on its documentation site: a page a reader can follow, and one JSON document
this page reads.

### The stable path

```text
https://powersemmi.github.io/<crate>/latest/benchmarks/results.json
```

The file lives at `docs/benchmarks/results.json` in the broker repository. The docs build copies it
verbatim, and publishing the site puts it under the `latest` alias next to the page that explains
it. The broker sites share this site's origin, so this page reads them directly.

### The document

```json
{
  "schema": 2,
  "crate": "ruststream-nats",
  "crate_version": "0.7.0",
  "core_version": "0.7.0",
  "measured_at": "2026-08-20",
  "environment": {
    "cpu": "AMD Ryzen 9 5950X 16-Core Processor",
    "architecture": "x86_64 (x86-64-v3)",
    "cpu_frequency": "base 3400 MHz, max 4900 MHz",
    "cores": "16 physical, 32 logical",
    "memory": "62.7 GiB",
    "memory_speed": "DDR4, 3600 MT/s",
    "os": "Linux 6.16.7",
    "broker": "nats:2.10-alpine in Docker on localhost",
    "rustc": "1.90.0",
    "valgrind": "3.25.1",
    "profile": "bench, inheriting release (opt-level = 3, lto = false, codegen-units = 16)",
    "features": "--no-default-features --features memory,macros,json",
    "rustflags": "-C target-cpu=native"
  },
  "scenarios": [
    {
      "name": "core NATS, 512 B JSON, ack each",
      "unit": "msg/s",
      "messages": 200000,
      "pairs": 11,
      "raw": { "median": 128412, "min": 126980, "max": 129604 },
      "adapter": { "median": 128090, "min": 126700, "max": 129310 },
      "framework": { "median": 127905, "min": 126100, "max": 129020 },
      "overhead_percent": 0.4,
      "adapter_overhead_percent": 0.3,
      "verdict": "indistinguishable",
      "broker_bound": true
    }
  ],
  "code": [
    {
      "name": "consume, JSON decode into a small struct",
      "messages": 1000,
      "framework": { "instructions": 2839.8, "allocations": 0.0 },
      "cold": { "instructions": 19219, "allocations": 26 },
      "gated": true
    }
  ]
}
```

`schema` is the version of this document. `unit` is a short label rendered next to every value in
the row, not a sentence. `verdict` is `measured` or `indistinguishable`, decided by the rule above;
`overhead_percent` is recorded either way and displayed only when the verdict is `measured`. It is
the framework against the raw client, end to end. `adapter` and `adapter_overhead_percent` are the
crate's own consumer and publisher against the same client, measured without the runtime; a crate
that publishes neither leaves the middle column empty.
`broker_bound` marks a run the broker paced rather than the consumer.

`environment` describes the machine and the build. `cpu`, `architecture`, `cpu_frequency`,
`cores`, `memory` and `memory_speed` are the machine; `profile` and `features` are what the
benchmarks were built with. A field the machine does not publish is written as `unknown` rather
than guessed: memory speed comes from the DMI tables, which most systems only let root read.
Everything but `cpu`, `os` and `rustc` is optional, so a schema 1 document stays readable.

`code` is the second table, one entry per scenario. `framework` is per message in the steady state;
`cold` is the whole cost of starting the service and taking the first delivery, not divided by
anything. `gated` says whether CI fails on a regression in it. A crate that publishes `scenarios`
alone declares `schema` 1 and keeps its row in the first table.

A document that does not load, or that declares a `schema` this page does not know, leaves its
broker in the "no results published yet" line. A broken publish is visible instead of silently
missing.
