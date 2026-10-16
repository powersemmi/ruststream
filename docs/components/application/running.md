---
sidebar_position: 4
title: Running the service
description: "run owns the process until a signal, run_until stops on a future of yours, and start hands the service to another server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running the service

`run` owns the process until a signal, `run_until` stops on a future of yours, and `start` hands the service to another server.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/app_running.rs#start"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/app_running.rs#start"
```

</TabItem>
</Tabs>

## Beside an HTTP server

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/http_outbox.rs#healthz"
```

```rust reference="examples/http_outbox.rs#wiring"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/http_outbox.rs#healthz"
```

```rust reference="examples/manual/http_outbox.rs#wiring"
```

</TabItem>
</Tabs>

## How it works

`run` runs the service until `SIGINT` or `SIGTERM`, then shuts it down gracefully.
`run_until` does the same, but stops when the future it is given resolves.
The future starts once the service has started.
`start` connects the brokers, opens the subscriptions and runs the startup hooks, then returns a `RunningApp`.
A startup failure is the error `start` returns, before the caller accepts its own traffic.
`start` installs no signal handlers: the caller decides when to call `shutdown`.
`shutdown` runs `on_shutdown`, stops the subscriptions, waits for running handlers, shuts the brokers down in reverse order of registration and runs `after_shutdown`.
Dropping a `RunningApp` without `shutdown` detaches the service with no graceful shutdown.
`health` hands out a `HealthProbe`, a cheap clone that reports `Running`, `ShuttingDown`, `Stopped` or `Failed` and outlives `shutdown`.
`stopping` resolves when the service tears itself down after a fail-fast failure, so an HTTP server can stop with it.
`publisher` pairs a bound token with its broker, for publishing from a task beside the service.

## See also

- [`#[ruststream::app]`](./app-macro.md): a `main` that calls `run`.
- [Lifecycle hooks](./lifecycle.md): what runs at start and at shutdown.
- [The `ruststream` command](./cli.md): `run` from the command line.
- [`RustStream::run`](api:ruststream::runtime::RustStream::run), [`RustStream::run_until`](api:ruststream::runtime::RustStream::run_until), [`RustStream::start`](api:ruststream::runtime::RustStream::start), [`RunningApp`](api:ruststream::runtime::RunningApp), [`HealthProbe`](api:ruststream::runtime::HealthProbe), [`HealthState`](api:ruststream::runtime::HealthState)
