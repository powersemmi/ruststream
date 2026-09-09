# Lifespan and shared state

Most services need resources that are created once at startup and shared by every handler: a
database pool, an HTTP client, parsed configuration. RustStream gives you one typed shared state for
them, and runs lifecycle hooks at fixed points around the run loop.

## Shared state

The application state is a single typed value returned by the `on_startup` hook. Any handler or
middleware borrows it through `ctx.state()`.

[Context and state](context.md#application-level-typed-state) describes the mount rules checked at
compile time, `State<T>` injection, and the per-delivery context for message-scoped data. The hooks
that produce the state and release it are below.

## Lifecycle hooks

Anything that needs `async` work (connecting that pool, closing it cleanly) runs in a hook. Four
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
  new one, so its future can own resources across awaits: connect a pool, build the state struct,
  return it. The later hooks receive the state in shared ownership, as `Arc<S>`. You can call
  `on_startup` only before the first `with_broker`: handlers are registered against the state type
  it produces, so the reverse order does not compile. Register the other lifecycle hooks after it: a
  hook registered earlier would capture the wrong state type, and `on_startup` panics in that case.
- **`after_startup`** runs once subscriptions are open and handlers are live. Publish an initial
  message with the scope-level form `b.after_startup(policy, hook)`: it runs at the same point, and
  the hook receives a live publisher, which the policy instantiates on the connected broker. The
  app-level hook stays for readiness signalling and for work not tied to a broker; the
  [testing guide](testing.md) uses it that way. It is also the place for the first messages the app
  consumes itself: a message published before subscriptions open reaches no subscriber.
- **`on_shutdown`** runs when shutdown begins, while brokers are still connected.
- **`after_shutdown`** runs after brokers are down, for final async teardown.

An error returned by a startup hook aborts the service. A shutdown hook only logs its error, so
shutdown always runs to completion. Hooks of the same kind run in registration order.

## Passing a database connection

The common case: open a pool before serving, hand it to every handler, close it at shutdown. The
`Database` below is a stand-in for any async resource: a `sqlx::PgPool` or an HTTP client fits the
same way, only the `connect` / `close` calls differ:

=== "Macros"

    ```rust
    --8<-- "examples/lifespan.rs:hooks"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/lifespan.rs:hooks"
    ```

The hook's error type is inferred from the returned `Result`; it only has to implement
`std::error::Error + Send + Sync`. The resource is `Send + Sync`, so every concurrent handler borrows
the one shared instance through `ctx.state()`, with no connection setup per message:

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

`run` owns the whole process: it installs the signal handlers and returns only once the service has
stopped.

A service that shares its process with another foreground server (usually an HTTP framework) starts
the messaging side with `start`. `start` performs the same startup sequence and resolves once
subscriptions are open, so you get a startup error before the host begins accepting traffic.
`start` installs no signal handlers: the host decides what stops the service. The returned
`RunningApp` handle drives the rest of the lifecycle:

```rust
--8<-- "tests/app_start.rs:handle"
```

- `stopping()` returns an owned future that resolves when the service has stopped itself on a
  fail-fast failure. You can plug it into the host's graceful shutdown (axum's
  `with_graceful_shutdown`), so the process stops serving requests as soon as the messaging side
  stops.
- `shutdown()` is the explicit graceful teardown: the `on_shutdown` hooks, a wait for in-flight
  handlers and for the continuations that run after a delivery settles (bounded by the
  [shutdown timeout](#shutdown-timeout)), broker shutdown in reverse registration order, then the
  `after_shutdown` hooks. `shutdown()` returns a fail-fast reason as an error.

The handle is marked `#[must_use]`: dropping it without calling `shutdown` detaches the service and
leaves it without a graceful teardown. `run` and `run_until` are built on the same start/shutdown
path, so all three forms go through one startup and teardown sequence.

## Shutdown timeout

By default, once shutdown is triggered, `run` waits indefinitely for in-flight handlers to finish.
You can bound that wait with `shutdown_timeout`, as the example above does. Handlers still running
when it expires are aborted:

<!-- inline-rust: isolates the shutdown_timeout call; the full chain is compiled in lifespan.rs:hooks, shown earlier on this page -->
```rust
use std::time::Duration;

RustStream::new(info)
    .shutdown_timeout(Duration::from_secs(10))
    .with_broker(broker, |b| b.include(handle));
```
