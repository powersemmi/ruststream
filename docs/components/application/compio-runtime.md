---
sidebar_position: 5
title: Compio runtime
description: "A service runs on compio with one runtime per core, io_uring on Linux, core 0 receiving, and workers placed across the cores."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Compio runtime

A service runs on compio with one single-threaded runtime per core.
On Linux the runtimes use `io_uring`, and a host that refuses a ring (a container profile that blocks it, an old kernel, no memory for it) is served by the polling driver. Elsewhere the platform's own completion or polling driver is used.
[`#[ruststream::app(runtime = compio, cores = N)]`](./app-macro.md) turns the builder function into the service's `main`, which starts the cores and runs the app on them.
The handlers are the ones a tokio service runs, written with `#[subscriber]`.

Core 0 runs on the calling thread, and every further core runs on a thread of its own, pinned to a CPU the process may use.
Core 0 owns the brokers and receives every subscription.
A sequential subscription is handled on core 0.
A subscription with `workers(n)` or `threads(n)` hands its deliveries to workers placed across the cores, and `by_key` keeps the deliveries of one key on one worker.

The feature is `compio`. The runner builds on compio 0.20.0-beta.1 and moves to compio 0.20 when it ships.

## Example

The service confirms every order with four workers and audits it on core 0.
A handler reads the core it runs on with the `Ctx<Core>` extractor (`Core::current()` in the manual form); the confirmation carries it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream/examples/compio_runtime.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream/examples/manual/compio_runtime.rs#app"
```

</TabItem>
</Tabs>

`cores = 4` runs four cores; without it the app runs on every core the process may use.
The binary reads its first argument as a command, as on tokio: `run`, or no argument, runs the service until `SIGINT` or `SIGTERM`, and `asyncapi gen` prints the AsyncAPI document.
The manual tab writes the `main` the attribute generates, a call to [`run_main`](api:ruststream::runtime::compio::run_main) with the core count.

## Enabling it

`tokio` can be enabled beside `compio`; a service that runs on compio alone turns the defaults off:

```toml
ruststream = { version = "0.8", default-features = false, features = ["compio", "memory", "macros", "json"] }
```

On `compio` alone the in-process bus is `LocalMemoryBroker<N>` from `ruststream::memory::local`, and the items a service names come from `ruststream::runtime::compio`.

## Placement across cores

The app places its mounts once at startup, before any thread starts:

- `threads(n)` holds `n` cores of its own, taken in mount order from core 1 up. Core 0 is never dedicated.
- `workers(n)` spreads its `n` workers over the shared cores, which are the cores no `threads` mount holds, core 0 included. Each mount continues from where the previous one stopped, so mounts do not pile on the first core.
- A sequential subscription stays on core 0.

`threads` mounts that ask for more cores than the app has besides core 0 are an error of the run, and it names each of them.
A hand-off from core 0 to a worker is one queue push and a wake, and it allocates nothing once the workers have started.

[`Core`](api:ruststream::runtime::compio::Core) tells a handler or a hook where it runs: `index()`, `count()` and `is_main()`.
A hook that does one-time work acts on core 0 and lets the other cores reuse the result.

## What the runner asks of a handler

A delivery can be handed to a worker on another core, so the compio runner asks what the tokio runner asks.
The `compio` feature turns the `send` bound on: handler state and the future of a handler body are `Send`.
A body that holds an `Rc` across an `.await` fails at the `include` that mounts it, and the error names the handler.
A broker's connection stays on core 0, where the subscription is received.

## Time

The clock the app reads is compio's.
A handler sleeps on the app's clock with [`time::sleep`](api:ruststream::runtime::compio::time::sleep), which the [test harness](./testing/compio-app.md) moves.
Retries, batch deadlines and `retry_after` ride the same clock.

## Shutdown and health

`SIGINT` and `SIGTERM` end the run.
The app stops receiving, lets the handlers in flight finish, runs the shutdown hooks and shuts every broker down once the workers of every core are gone.
[`shutdown_timeout`](api:ruststream::runtime::compio::RustStream::shutdown_timeout) bounds the wait for handlers still running.
A core that fails or panics stops the others, and `run` returns the failure of the lowest core. A worker that panics outside its handler stops the service the way a handler that panics does.

[`RustStream::run_until`](api:ruststream::runtime::compio::RustStream::run_until) ends the run on a future of the caller's instead of a signal.
[`RustStream::start`](api:ruststream::runtime::compio::RustStream::start) starts the app on the current runtime and returns a [`RunningApp`](api:ruststream::runtime::compio::RunningApp), whose `health()` is a [`HealthProbe`](api:ruststream::runtime::compio::HealthProbe) a sibling task reads.

## Diagnostics

The runner reports what happens to the service (startup, a lost reply, a handler that panicked, a core that failed) through the logger the app selects, and reports nothing until it selects one: the app above selects `TracingLogger` with `.logger(TracingLogger)`.

[`logger`](api:ruststream::runtime::compio::RustStream::logger) is applied when the app starts, before the brokers connect.
`TracingLogger` is always available on compio; `LogLogger` needs the `log` feature.
The level is the backend's: a `tracing` subscriber's filter, or `log::max_level()`, decides which events show.
The selection is process-wide, so two apps in one process share one logger.

## AsyncAPI

`asyncapi gen` of a compio binary prints the document of the app.
[`asyncapi::build_spec`](api:ruststream::asyncapi::build_spec) takes a compio app as it takes a tokio one, and the two describe the same document for the same mounts.

## Testing

[`testing::compio::TestApp`](api:ruststream::testing::compio::TestApp) runs the app on the test's own compio runtime.
See [Testing a compio app](./testing/compio-app.md).
