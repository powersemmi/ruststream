---
sidebar_position: 1
title: Logging
description: "The app selects the logger the framework reports through; the logging feature prints tracing events on the console, colored, at the verbosity RUST_LOG sets."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Logging

The app selects the logger the framework reports through, and the `logging` feature prints the `tracing` events on the console, colored, at the verbosity `RUST_LOG` sets.

## The logger

The framework reports while it dispatches, publishes and starts or stops the service, and it reports what it loses or refuses, such as a message that failed to decode, a publish that was lost, or an ack the broker does not support.
Each report names the subscription and the message type.
Nothing is reported until the app selects a logger with [`logger`](api:ruststream::runtime::RustStream::logger), as the [example](#example) below does with `.logger(TracingLogger)`, imported from `ruststream::diagnostics`.

Three loggers exist, each compiled by a feature of the same name:

| Logger | Feature | Reports through |
| --- | --- | --- |
| [`TracingLogger`](api:ruststream::diagnostics::TracingLogger) | `tracing` | `tracing` events, filtered by the subscriber the service installed |
| [`LogLogger`](api:ruststream::diagnostics::LogLogger) | `log` | records for the `log` crate, filtered by `log::max_level()` and the installed logger |
| [`DefmtLogger`](api:ruststream::diagnostics::DefmtLogger) | `defmt` | defmt lines, filtered by defmt's level |

`tracing` needs `alloc`.
The tokio runner turns it on, and an embassy app with a heap turns it on itself.
A build without `alloc` selects `LogLogger` or `DefmtLogger`; see [Embedded runtime](../application/embedded-runtime.md).
The same `logger` step exists on [`EmbeddedApp`](api:ruststream::runtime::EmbeddedApp).

The level is the way to silence a report.
Every warning stays on, and the logger hands each event to its backend, so `RUST_LOG=ruststream=error`, `log::set_max_level` or defmt's level drops what the service does not want.
Decode failures and lost publishes are at `WARN`.

The selection is process-wide, like the `log` crate's.
Two apps in one process share one logger, and the app that starts last decides.
A diagnostic costs one atomic load, and a delivery that succeeds raises none.

## The console

`TracingLogger` emits events; a `tracing` subscriber shows them.
[`logging::init`](api:ruststream::logging::init) installs the bundled one.
It writes to stderr, so stdout stays clean for the output of a command such as `asyncapi gen`.
Levels are colored when stderr is a terminal.
`RUST_LOG` sets the filter in the usual directive syntax; without it, the filter is `info`.
The `main` that [`#[ruststream::app]`](../application/app-macro.md) generates calls `init` on `run`, so a service with the attribute needs no setup.
A hand-written `main` calls `init` itself, or builds a [`Logging`](api:ruststream::logging::Logging) to change the defaults: the fallback filter, colors, and whether each line names its target.
`try_init` installs it, and returns [`LoggingInitError`](api:ruststream::logging::LoggingInitError) when the filter does not parse or a subscriber is already installed.
A subscriber installed first stays in place: the generated `main` leaves a service's own stack alone, a JSON formatter or an OpenTelemetry bridge for one.
A handler emits its own events with `tracing::info!` and its siblings, which need no logger selection.
The events of each delivery and its outcome come from [`TracingLayer`](./tracing-layer.md).

## Example

The app selects `TracingLogger`, and the example runs with the console logger on:

```text
RUST_LOG=ruststream=debug,info cargo run --example logging --features testing,macros,memory,json,logging -- run
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/logging.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/logging.rs#app"
```

</TabItem>
</Tabs>
