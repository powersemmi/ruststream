---
description: "How a crate publishes its benchmark results: one JSON document at a stable path that the Benchmarks page reads."
---

# Publishing

How a crate publishes its benchmark results: one JSON document at a stable path, which the
[Benchmarks](index.md) page reads.

A broker crate runs its own harness with `just bench` against the broker in its compose file. It
commits the outcome to its repository: a page a reader can follow, and one JSON document the
Benchmarks page reads.

## The stable path

The file lives at `docs/benchmarks/results.json` in the broker repository, next to the page that
explains it. The Benchmarks page reads it from `main`, so a measurement shows as soon as it is
merged.

```text
https://raw.githubusercontent.com/powersemmi/<crate>/main/docs/benchmarks/results.json
```

## The document

`schema` is the version of the document: 3 reports each loop as three rounds (`best`, `median` and
`worst`), 2 added the `code` section, and a schema 1 document carried a median with its extremes.
The table prints `best` with `median` in parentheses. `worst` is read rather than printed: the
verdict rule rests on the distance between the best round and the worst. A schema 3 document without
`median` keeps the worst round in parentheses, which is what it printed before the field existed.
`unit` is a short label rendered next to every value in the row, not a sentence. `verdict` is
`measured` or `indistinguishable`, decided by the [spread rule](methodology.md#the-report);
`overhead_percent` is recorded either way and displayed only when the verdict is `measured`. It is
the framework against the raw client, end to end. `adapter` and `adapter_overhead_percent` are the
crate's own consumer and publisher against the same client, measured without the runtime, and
`adapter_verdict` applies the spread rule to that difference the way `verdict` applies it to the
other - and where a crate leaves it out, the page applies the same rule to the spreads it did
publish, so the two columns of a row never disagree about what is visible. A crate that publishes
none of them leaves the middle column empty. `environment` may carry `round_trip`, the probe the
`broker_bound` arithmetic rests on, so a reader can redo the sum. `broker_bound` marks a run the
broker paced rather than the consumer.

`environment` describes the machine and the build. `cpu`, `architecture`, `cpu_frequency`,
`cores`, `memory` and `memory_speed` are the machine; `profile` and `features` are what the
benchmarks were built with. A field the machine does not publish is written as `unknown` rather
than guessed: memory speed comes from the DMI tables, which most systems only let root read.
Everything but `cpu`, `os` and `rustc` is optional, so a schema 1 document stays readable.

`code` feeds the cost chart, one entry per scenario. `framework` is per message in the steady state;
`cold` is the whole cost of starting the service and taking the first delivery, not divided by
anything. `gated` says whether a regression in it fails the crate's benchmark recipe. A crate that
publishes `scenarios` alone declares `schema` 1 and shows its comparison only. A crate that measures
no broker of its own leaves `scenarios` out instead of publishing it empty, and shows a cost chart
only.

A crate that measures its code apart from the comparison records that run in `code_measured`: its
`measured_at`, `crate_version` and `core_version`. The line above the cost chart takes its date
and versions from there, and from the top-level fields in a document without it.

A document that does not load, or that declares a `schema` the Benchmarks page does not know,
leaves its tab with the "no results published yet" line. A broken publish is visible instead of
silently missing.

```json
{
  "schema": 3,
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
      "pairs": 3,
      "raw": { "best": 129604, "median": 128470, "worst": 126980 },
      "adapter": { "best": 129310, "median": 128040, "worst": 126700 },
      "framework": { "best": 129020, "median": 127640, "worst": 126100 },
      "overhead_percent": 0.4,
      "adapter_overhead_percent": 0.3,
      "adapter_verdict": "indistinguishable",
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
