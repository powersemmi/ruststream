---
sidebar_position: 3
title: Diagnostics
description: "The poll-diagnostics feature times the polls of every handler and names the handlers that compute on the app's runtime."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Diagnostics

The `poll-diagnostics` feature times the polls of every handler and names the handlers that compute on the app's runtime.

With the feature on, every subscription is sampled.
One poll of its handler in 64 is timed with two reads of the CPU's cycle counter.
The time measured is the time inside one poll, between two `.await` points.
A handler that waits on I/O or a timer is not slow here; only one that computes is.
Each subscription keeps an exponential moving average and a histogram of that time.
When the average crosses 100 microseconds, the runtime logs one warning naming the subscription, its average and p99, and the fix: [threads](./threads.md).
It warns after 16 samples, and once per crossing.
A subscription already on `threads(n)` is measured the same way, and no warning names it.
[`PollDiagnostics`](api:ruststream::runtime::PollDiagnostics) changes the [`threshold`](api:ruststream::runtime::PollDiagnostics::threshold) and the [`sample_every`](api:ruststream::runtime::PollDiagnostics::sample_every) interval, and reads the reports.
[`RustStream::poll_diagnostics`](api:ruststream::runtime::RustStream::poll_diagnostics) hands it to the app; a clone the caller keeps reads the same reports.
[`Metrics::observe_poll_diagnostics`](api:ruststream::metrics::Metrics::observe_poll_diagnostics) and [`Otel::observe_poll_diagnostics`](api:ruststream::otel::export::Otel::observe_poll_diagnostics) export them, labelled per subscription.
The cost is a countdown per poll and, on a timed poll, two counter reads and a few atomic updates.
The example runs with `cargo test --example poll_diagnostics --features testing,macros,memory,json,poll-diagnostics`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/poll_diagnostics.rs#diagnostics"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/poll_diagnostics.rs#diagnostics"
```

</TabItem>
</Tabs>

## Reading the reports

[`reports`](api:ruststream::runtime::PollDiagnostics::reports) returns a [`PollReport`](api:ruststream::runtime::PollReport) for every subscription that has opened, and [`report`](api:ruststream::runtime::PollDiagnostics::report) the one of a given name.
A report holds the number of samples, the average and the p99 of the time in one poll.

{/* single-form: reading the reports is the same code with and without the macros */}
```rust reference="examples/poll_diagnostics.rs#reports"
```
