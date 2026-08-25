# Benchmarks

Putting a framework between the broker client and your handler costs something on every message:
the subscription stream, the decode, the dispatch, the ack. This page publishes what that costs,
measured against the raw client doing the same work on the same machine.

Each broker crate measures itself and publishes its own numbers; this page loads them and shows
them together. Nothing is copied here, so a broker that remeasures changes what you read below on
its next docs deploy.

## Results

Medians over interleaved pairs, with the observed spread in parentheses.

<div id="benchmark-results" data-benchmark-labels='{"loading":"Loading published results...","broker":"Broker","scenario":"Scenario","raw":"Raw client","framework":"RustStream","overhead":"Overhead","indistinguishable":"indistinguishable","brokerBound":"broker-bound","measured":"measured","details":"Full results and methodology","pending":"No results published yet: {brokers}."}'></div>

## What the number is

The measurement answers one question: what does the layer between the broker client and your
handler cost per message. It is not a broker comparison. Each row was measured on the machine of
whoever maintains that broker crate, against a broker on localhost, so the absolute throughput of
one row says nothing about another; only the two columns within a row are comparable, and that is
exactly the comparison the page is for.

A broker on localhost is the harshest setting for the framework's share of the work. There is no
network latency for a per-message cost to hide in, so the same absolute overhead is a larger
fraction here than it would be against a broker across a real network. Read the percentage as an
upper bound on what a deployed service pays, not as a typical figure.

Rows marked `broker-bound` mean the raw client was already waiting on the socket for most of the
run. The framework's work then happens inside time that was being spent waiting anyway, and the
measured difference collapses toward zero. That is a real result for that workload - it is what
a saturated consumer looks like - but it is a lower bound on the dispatch cost, not a measurement
of it, and it must not be read as "free".

## Methodology

Every broker crate follows the procedure below, so that a number published for one broker means
the same thing as a number published for another. A broker that deviates says so on its own page.

### The pair

A run is a pair of binaries that differ in exactly one thing: whether the messages arrive through
RustStream or through the broker client directly.

- **Same client, same client configuration.** Prefetch, ack mode, consumer group, durability,
  connection count and any broker-specific tuning are identical on both sides. The framework side
  configures the broker through RustStream; the resulting client settings still have to match.
- **Same ack position.** RustStream acks after the handler returns, so the raw loop acks in the
  same place. Batching acks at the end of the raw run measures a different protocol, not a
  different framework.
- **Same decode into the same type.** The raw side deserializes the payload into the same struct
  with the same codec and touches a field through `std::hint::black_box`. Skipping this is the
  easiest way to produce a wrong number: the optimizer removes a decode whose result is unused,
  and the comparison silently becomes decode against nothing.
- **Same payload, byte for byte.** One generator produces the bodies both sides consume.
- **Same runtime.** Tokio flavor, worker thread count and the number of messages in flight are
  fixed and equal.
- **Same build.** Profile, `RUSTFLAGS`, and the allocator match, and the observability features
  (`logging`, `metrics`, `otel`) are off on both sides or on on both sides. A machine with
  `-C target-cpu=native` in the environment produces numbers that another machine cannot
  reproduce; the flags are published with the results.

### The run

- **The consumer is attached before anything is published.** Otherwise one side drains a backlog
  the broker already has and the other receives live deliveries, which are different paths through
  most brokers.
- **Every run gets its own names.** A fresh subject, queue, stream or consumer group per run, so
  run N never observes what run N-1 left behind.
- **The window starts at the first message received and ends when the last one is acked.** A
  discarded warm-up run precedes the measured ones: connection setup, consumer registration and
  the first allocations are startup cost, not per-message cost.
- **The message count makes a run last at least five seconds**, so that startup transients and
  timer resolution stay in the noise.
- **Pairs are interleaved, not blocked.** Raw, framework, raw, framework, and so on for at least
  eleven pairs, discarding the first. Running all of one side and then all of the other attributes
  every drift of the machine - thermal, background load, page cache - to whichever side ran second.

### The report

- **Both sides report a median and a spread**, over the pairs that were kept. A single number from
  a single run is not a result.
- **A difference smaller than the spread is published as `indistinguishable`,** never as a
  percentage: a figure below the run-to-run noise reads as precision that was never measured.
- **A saturated consumer is flagged.** When the raw side spends the run waiting on the broker, the
  row carries `broker-bound` and its number is understood as a lower bound.
- **The environment is published with the numbers**: CPU and core count, kernel, how the broker was
  started (image, container, host), the rustc version, the crate versions, the build profile and
  the flags. Without those a number cannot be reproduced or aged out.

## Publishing results

A broker crate runs its own harness with `just bench` against the broker in its compose file, and
publishes the outcome as part of its documentation site: a page a reader can follow, and one JSON
document this page can read.

### The stable path

```text
https://powersemmi.github.io/<crate>/latest/benchmarks/results.json
```

The file lives at `docs/benchmarks/results.json` in the broker repository, so the docs build copies
it verbatim and the deploy puts it under the `latest` alias next to the page that explains it. The
broker sites share this site's origin, so this page reads them directly.

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

`schema` is the version of this document, and a reader that does not recognise it shows the broker
as unpublished rather than guessing. `unit` is a short label rendered next to every value in the
row, so `msg/s` rather than a sentence. `verdict` is `measured` or `indistinguishable`, decided by
the rule above; `overhead_percent` is recorded either way, and displayed only when the verdict is
`measured`. `broker_bound` marks a run the broker paced rather than the consumer.

A document that fails to load, or that carries an unknown `schema`, leaves its broker in the "no
results published yet" line, so a broken publish is visible rather than quietly absent.
