# Benchmarks

A framework between the broker client and your handler costs time on every message: the
subscription stream, the decode, the dispatch, the ack. This page publishes that cost, measured
against the raw client doing the same work on the same machine.

Each broker crate measures itself and publishes its own numbers. This page loads them and shows
them together. Nothing is copied here, so a broker that remeasures itself changes the table below
the next time it publishes its documentation.

## Results

Medians over interleaved pairs, with the observed spread in parentheses.

<div id="benchmark-results" data-benchmark-labels='{"loading":"Loading published results...","broker":"Broker","scenario":"Scenario","raw":"Raw client","framework":"RustStream","overhead":"Overhead","indistinguishable":"indistinguishable","brokerBound":"broker-bound","measured":"measured","details":"Full results and methodology","pending":"No results published yet: {brokers}."}'></div>

## What the number is

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

## Methodology

Every broker crate follows the procedure below, so a number published for one broker means the same
thing as a number published for another. A broker that departs from the procedure says so on its own
page.

### The pair

A run is a pair of binaries that differ in exactly one thing: whether the messages arrive through
RustStream or straight through the broker client.

- **Same client, same client configuration.** Prefetch, ack mode, consumer group, durability,
  connection count and any broker-specific tuning are identical on both sides. The framework side
  configures the broker through RustStream, and the resulting client settings still have to match.
- **Same ack position.** RustStream acks after the handler returns, so the raw loop acks in the
  same place. Batching acks at the end of the raw run measures a different protocol, not a
  different framework.
- **Same decode into the same type.** The raw side deserializes the payload into the same struct
  with the same codec and touches a field through `std::hint::black_box`. Skipping this is the
  easiest way to produce a wrong number: the optimizer removes a decode whose result is unused, and
  the raw side silently stops decoding.
- **Same payload, byte for byte.** One generator produces the bodies both sides consume.
- **Same runtime.** The tokio flavor, the worker thread count and the number of messages processed
  at once are the same on both sides.
- **Same build.** Profile, `RUSTFLAGS` and the allocator match, and the observability features
  (`logging`, `metrics`, `otel`) are enabled on both sides or on neither. A machine with
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
- **Pairs are interleaved, not blocked.** Raw, framework, raw, framework and so on, for at least
  eleven pairs, discarding the first. Running one side to the end and then the other attributes
  every drift of the machine (thermal, background load, page cache) to whichever side ran second.

### The report

- **Both sides report a median and a spread**, over the pairs that were kept. A single number from
  a single run is not a result.
- **A difference smaller than the spread is published as `indistinguishable`,** never as a
  percentage: a figure below the run-to-run noise reads as precision that was never measured.
- **A saturated consumer is flagged.** When the raw side spends the run waiting on the broker, the
  row is marked `broker-bound`.
- **The environment is published with the numbers**: CPU and core count, kernel, how the broker was
  started (image, container, host), the rustc version, the crate versions, the build profile and
  the flags. Without them a number cannot be reproduced or declared out of date.

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
  "schema": 1,
  "crate": "ruststream-nats",
  "crate_version": "0.7.0",
  "core_version": "0.7.0",
  "measured_at": "2026-08-20",
  "environment": {
    "cpu": "AMD Ryzen 9 5950X, 16 cores",
    "os": "Linux 6.16.7",
    "broker": "nats:2.10-alpine in Docker on localhost",
    "rustc": "1.90.0",
    "profile": "release, lto = thin, codegen-units = 1",
    "rustflags": "-C target-cpu=native"
  },
  "scenarios": [
    {
      "name": "core NATS, 512 B JSON, ack each",
      "unit": "msg/s",
      "messages": 200000,
      "pairs": 11,
      "raw": { "median": 128412, "min": 126980, "max": 129604 },
      "framework": { "median": 127905, "min": 126100, "max": 129020 },
      "overhead_percent": 0.4,
      "verdict": "indistinguishable",
      "broker_bound": true
    }
  ]
}
```

`schema` is the version of this document. `unit` is a short label rendered next to every value in
the row, not a sentence. `verdict` is `measured` or `indistinguishable`, decided by the rule above;
`overhead_percent` is recorded either way and displayed only when the verdict is `measured`.
`broker_bound` marks a run the broker paced rather than the consumer.

A document that does not load, or that declares a `schema` this page does not know, leaves its
broker in the "no results published yet" line. A broken publish is visible instead of silently
missing.
