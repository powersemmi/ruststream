---
description: "What RustStream costs on every message: throughput against each broker's raw client, and instructions and allocations of its own code."
---

import {BenchmarkTabs, MovedSections} from '@site/src/components/Benchmarks';

export const labels = {"loading": "Loading published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "measured": "measured", "details": "Full results and methodology", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code"};

# Benchmarks

What RustStream costs on every message, measured against each broker's raw client and as the cost
of its own code.

A framework between the broker client and your handler costs time on every message: the
subscription stream, the decode, the dispatch, the ack. This page publishes that cost twice, as
two measurements that answer different questions.

The first is throughput against the raw client, on a real broker, doing the same work on the same
machine. It says what a deployed service pays.

The second is the cost of the code: instructions and allocations per message. In `ruststream` it is
the framework's own code, with no broker in the number. It says what changed when the code changed,
and it is precise enough to fail a pull request that makes a message more expensive.

Each crate measures itself by one [methodology](methodology.md) and
[publishes](publishing.md) its own numbers. This page loads them from the `main` branch of each
crate's repository and shows each crate in a tab of its own. A new measurement appears in its tab
as soon as it is merged. What the numbers allow you to conclude, and what they do not, is on the
[Numbers](numbers.md) page.

## Against a raw client

The first chart of a broker's tab is its comparison. Each scenario draws three loops of three
interleaved rounds against the raw client's best round, the zero of its axis. A loop is a bar from
its best round to its slowest, with a dot at the best round and a notch at the median. A dot right
of zero is slower than the raw client. The best round is the closest to the undisturbed cost, and
the median is the one a rerun typically repeats.

The shaded band around zero is what the runtime's `indistinguishable` verdict rests on: the spread
from the best round to the worst. A best round inside it cannot be told from the raw client. A
hollow dot marks every loop the crate judged indistinguishable, and the verdicts beside a row are
the ones the crate published. A row shows its numbers on hover or focus,
and the table under the chart lists them all: the best round, with the median in parentheses.

`Broker crate` is the crate's own consumer and publisher driven without the runtime, so the two
differences read apart: what the crate costs over the client it wraps, and what the runtime costs
on top of that.

## Cost of the code

The cost chart closes every tab, and it is the core's only chart. It shows
instructions and allocations per message in the steady state. These are absolute figures, not a
comparison: what the framework costs over a broker's own client is the comparison. `ruststream`
measures its own code on the in-process transport. A broker crate measures over its real
transport, and its benchmarks page says what its number covers.

Each crate has an axis of its own, because a broker crate counts its client's work too. `Cold
start` is what starting the service and handling the first delivery cost together, instructions
and allocations, and a service pays it once rather than per message. A row shows it on hover or
focus, and the table under the chart lists it. The line above a crate's chart is the machine the
run was taken on, down to the memory, then the versions of the crate
and of `ruststream` it counted and the date it ran: an instruction count is comparable across
machines only once you know they ran the same code.

## Results

<BenchmarkTabs labels={labels} />

<MovedSections />
