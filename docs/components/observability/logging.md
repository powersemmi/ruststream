---
sidebar_position: 1
title: Logging
description: "The logging feature prints the service's tracing events on the console, colored, at the verbosity RUST_LOG sets."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Logging

The `logging` feature prints the service's `tracing` events on the console, colored, at the verbosity `RUST_LOG` sets.

RustStream emits `tracing` events while it dispatches, publishes and starts or stops the service, and a handler emits its own with `tracing::info!` and its siblings.
Nothing is printed until a `tracing` subscriber is installed.
[`logging::init`](api:ruststream::logging::init) installs the bundled one.
It writes to stderr, so stdout stays clean for the output of a command such as `asyncapi gen`.
Levels are colored when stderr is a terminal.
`RUST_LOG` sets the filter in the usual directive syntax; without it, the filter is `info`.
The `main` that [`#[ruststream::app]`](../application/app-macro.md) generates calls `init` on `run`, so a service with the attribute needs no setup.
A hand-written `main` calls `init` itself, or builds a [`Logging`](api:ruststream::logging::Logging) to change the defaults: the fallback filter, colors, and whether each line names its target.
`try_init` installs it, and returns [`LoggingInitError`](api:ruststream::logging::LoggingInitError) when the filter does not parse or a subscriber is already installed.
A subscriber installed first stays in place: the generated `main` leaves a service's own stack alone, a JSON formatter or an OpenTelemetry bridge for one.
The events of each delivery and its outcome come from [`TracingLayer`](./tracing-layer.md).

## Example

The example runs with the logger on:

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
