---
description: "What each published benchmark number measures, and which numbers can be compared with each other."
---

# Numbers

What each published number measures, and which numbers can be compared with each other.

## The comparison against a raw client

Each row was measured by whoever maintains that broker crate, on their own machine, against a broker
on localhost. Rows are therefore not comparable with each other: the absolute throughput of one says
nothing about another. Only the loops inside a row are comparable, and that is the comparison the
[results](index.md) exist for.

A broker on localhost is the harshest setting for the framework. There is no network latency here,
so the same absolute cost per message comes out as a larger percentage than it would against a
broker across a real network. Read the percentage as an upper bound on what a deployed service
pays, not as a typical figure.

The `broker-bound` mark means the raw client spent most of the run waiting on the socket. The
framework does its work inside that wait, and the measured difference comes out near zero. For that
workload it is a real result: this is what a saturated consumer looks like. But the number is a
lower bound on the cost of dispatch, not a measurement of it, and you cannot read it as "free".

## The cost of the code

An instruction count is exact. In `ruststream` two runs of the same binary give the same number,
and a machine twice as fast gives the same number too, so its rows are comparable with each other
and with the same row measured on another machine. What a count does not tell you is time: the
same count costs more where it misses the cache.

In `ruststream` the number is the crate's own work and nothing else: the service a user writes,
over the in-process queue, decoding the payload into a type, reading a field and settling the
delivery. No broker is in it, so the figure moves when the framework's code moves and at no other
time, which is what lets two percent count as a defect rather than as noise.

Every per-message figure is the steady state. Starting a service costs what it costs once - the
connect, the subscription, the first allocations behind them - and dividing that over the messages
of a run would publish it as a price per message it is not. So a scenario is measured over two
runs, the second twice as long as the first, and what a message costs is the difference between
them; the cold start is measured on its own, over a single delivery. In `ruststream` the two runs
are a thousand deliveries and two thousand.

Allocations are counted per message. In `ruststream` the figure on the delivery path is zero: once
the service is running, a message goes from the queue to the handler body without the framework
asking the allocator for anything. On its publish path the figure is what the broker takes to own
the message it is handed, and nothing above it.

A broker crate measures over its real transport, which in most crates is the broker in its compose
file, so its number also holds the client's work and moves a little from run to run. Its benchmarks
page says what the number covers, how far it moves, which delivery counts it uses and which limit it
is held to.
