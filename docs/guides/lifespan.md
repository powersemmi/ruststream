# Lifespan and shared state

Most services need resources that are created once at startup and shared by every handler: a
database pool, an HTTP client, parsed configuration. RustStream gives you one typed shared-state
value plus lifecycle hooks that run at fixed points around the run loop.

## Shared state

The application state is a single typed value produced by the `on_startup` hook: the value the hook
returns becomes the state and fixes the app's state type. Any handler or middleware borrows it
through `ctx.state()`. The full state story - the compile-time mount rules, `State<T>` injection,
and the per-delivery context for message-scoped data - lives in
[Context and state](context.md#application-level-typed-state). The hooks that produce the state and
tear it down are below.

## Lifecycle hooks

Anything that needs `async` work (connecting that pool, closing it cleanly) goes in a hook. Four
hooks bracket the run loop:

```text
on_startup(prev) -> S            # before brokers connect; build async resources, produce the state
  -> brokers connect, subscriptions open
after_startup(Arc<S>)            # handlers are live; publish a first message, signal readiness
  ... running ...
  -> shutdown triggered (signal, or the run_until future resolves)
on_shutdown(Arc<S>)              # brokers still connected
  -> brokers shut down, in-flight handlers drained
after_shutdown(Arc<S>)           # final teardown
```

- **`on_startup`** receives the previous state **by value** (`()` on the first call) and returns the
  new state, so its future can own resources across awaits - connect a pool, build the state struct,
  return it. The returned type becomes the app's state type. A failing `on_startup` aborts startup.
  The later hooks receive the state as a shared `Arc<S>`. `on_startup` only exists before the first
  `with_broker`: handlers are registered against the state type it produces, so the reverse order
  does not compile. Register the other lifecycle hooks after it (an earlier hook would close over
  the wrong state type; `on_startup` panics if one exists).
- **`after_startup`** runs once subscriptions are open and handlers are live. For publishing an
  initial message, prefer the scope-level form `b.after_startup(policy, hook)`: it runs at the
  same point, but the hook receives an already-paired live publisher, so nothing is threaded out
  of the wiring closure. The app-level hook remains for readiness signalling and non-broker work
  (the [testing guide](testing.md) uses it as the "handlers are live" gate). A failure in either
  aborts startup. This is also the delivery-correct point for seeds the app itself consumes: a
  publish before subscriptions open has no subscribers to reach.
- **`on_shutdown`** runs when shutdown begins, while brokers are still connected.
- **`after_shutdown`** runs after brokers are down, for final async teardown.

Startup hooks abort the service on error; shutdown hooks only log their error, so shutdown always
runs to completion. Hooks of the same kind run in registration order.

## Passing a database connection

The common case: open a pool before serving, share it with every handler, close it on the way out.
The `Database` below is a stand-in for any async resource - a `sqlx::PgPool` or an HTTP client
slots in the same way, only its `connect` / `close` calls differ:

=== "Macros"

    ```rust
    --8<-- "examples/lifespan.rs:hooks"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/lifespan.rs:hooks"
    ```

The hook's error type is inferred from the returned `Result`; it only needs to implement
`std::error::Error + Send + Sync`. The resource is `Send + Sync`, so every concurrent handler borrows
the one shared instance through `ctx.state()` - no per-message connection setup:

=== "Macros"

    ```rust
    --8<-- "examples/lifespan.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/lifespan.rs:handler"
    ```

The runnable program is
[`examples/lifespan.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/lifespan.rs).

## Running beside another server

`run` owns the whole process: it installs the signal handlers and returns only when the service
has stopped. A service that shares its process with another foreground server (an HTTP framework,
typically) brings the messaging side up with `start` instead. It performs the same startup
sequence and resolves once subscriptions are open - so a startup failure surfaces before the host
starts accepting traffic - and installs no signal handlers: the host decides what stops the
service. The returned `RunningApp` handle drives the rest of the lifecycle:

```rust
--8<-- "tests/app_start.rs:handle"
```

- `stopping()` returns an owned future that resolves when the service tears itself down on a
  fail-fast failure; plug it into the host's graceful shutdown (axum's `with_graceful_shutdown`)
  so the process stops serving when the messaging side dies.
- `shutdown()` is the explicit graceful teardown: the `on_shutdown` hooks, a drain of in-flight
  handlers and post-settle continuations (bounded by the [shutdown timeout](#shutdown-timeout)),
  broker shutdown in reverse registration order, then the `after_shutdown` hooks. A fail-fast
  reason surfaces here as an error.

The handle is `#[must_use]`: dropping it without calling `shutdown` detaches the service, with no
graceful teardown. `run` and `run_until` are built on the same start/shutdown path, so all three
forms share one startup and teardown sequence.

## Shutdown timeout

By default `run` waits indefinitely for in-flight handlers to finish after shutdown is triggered.
Bound that wait with `shutdown_timeout`, as the example above does; handlers still running after it
are aborted:

<!-- inline-rust: isolates the shutdown_timeout call; the full chain is compiled in lifespan.rs:hooks, shown earlier on this page -->
```rust
use std::time::Duration;

RustStream::new(info)
    .shutdown_timeout(Duration::from_secs(10))
    .with_broker(broker, |b| b.include(handle));
```
