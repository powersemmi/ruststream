# Lifespan and shared state

Most services need resources that are created once at startup and shared by every handler: a
database pool, an HTTP client, parsed configuration. RustStream gives you one typed shared-state
value plus lifecycle hooks that run at fixed points around the run loop.

## Shared state

The application state is a single typed value of your own choosing (a struct, or `()` when the
service needs none). It is produced by the `on_startup` hook - the value the hook returns becomes
the state and fixes the app's state type:

```rust
--8<-- "examples/lifespan.rs:hooks"
```

Read it from any handler or middleware through the `Context`, which borrows it directly:

```rust
--8<-- "examples/lifespan.rs:handler"
```

`ctx.state()` returns `&S`, the typed state itself - no lookup, no `Option`, no downcast. (For data
scoped to one message rather than the whole service, use the typed
[per-delivery context](context.md#per-delivery-context) instead.)

A `#[subscriber]` handler that reads state names it as the third `Context` generic
(`ctx: &mut Context<'_, (), S>`); the runtime only lets that handler mount on an app whose state
type matches, checked at compile time. A plain handler that names no state type is generic over it
and mounts on any app; a `publish(..)` handler instead pins its state to `()` when none is named, so
name the app's state explicitly to mount one on a stateful app. Everything else the context carries
(the headers working copy, named publishers) is covered in [Context and state](context.md).

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
  The later hooks receive the state as a shared `Arc<S>`.
- **`after_startup`** runs once subscriptions are open and handlers are live. Use it to publish an
  initial message or signal readiness (the [testing guide](testing.md) uses it as the "handlers are
  live" gate). A failure here also aborts startup.
- **`on_shutdown`** runs when shutdown begins, while brokers are still connected.
- **`after_shutdown`** runs after brokers are down, for final async teardown.

Startup hooks abort the service on error; shutdown hooks only log their error, so shutdown always
runs to completion. Hooks of the same kind run in registration order.

## Passing a database connection

The common case: open a pool before serving, share it with every handler, close it on the way out.
The `Database` below is a stand-in for any async resource - a `sqlx::PgPool` or an HTTP client
slots in the same way, only its `connect` / `close` calls differ:

```rust
--8<-- "examples/lifespan.rs:hooks"
```

The hook's error type is inferred from the returned `Result`; it only needs to implement
`std::error::Error + Send + Sync`. The resource is `Send + Sync`, so every concurrent handler borrows
the one shared instance through `ctx.state()` - no per-message connection setup. The runnable
program is
[`examples/lifespan.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/lifespan.rs).

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
