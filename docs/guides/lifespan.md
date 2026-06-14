# Lifespan and shared state

Most services need resources that are created once at startup and shared by every handler: a
database pool, an HTTP client, parsed configuration. RustStream gives you a shared `State` type-map
plus lifecycle hooks that run at fixed points around the run loop.

## Shared state

`State` is a type-map: one value per type. Put ready-made values in at build time with
`insert_state` (or from a [startup hook](#lifecycle-hooks), as the `Database` below is):

<!-- inline-rust: minimal insert_state fragment with placeholder Config; the startup-hook form is compiled in lifespan.rs:hooks, pulled in below -->
```rust
RustStream::new(info)
    .insert_state(Config::from_env())
    .with_broker(broker, |b| b.include(handle));
```

Read them from any handler or middleware through the `Context`:

```rust
--8<-- "examples/lifespan.rs:handler"
```

`ctx.state().get::<T>()` returns `Option<&T>`; it is `None` only if no value of that type was
inserted. Inserting the same type again replaces the previous value. (`ctx.get::<T>()` without
`state()` reads the per-delivery [extensions](context.md#per-delivery-extensions), a separate
type-map scoped to one message.)

A `#[subscriber]` handler opts into the context by taking a second parameter, `ctx: &mut Context`,
after the payload. Omit it when the handler does not need state. Everything else the context
carries (the headers working copy, named publishers) is covered in
[Context and state](context.md).

## Lifecycle hooks

Anything that needs `async` work (connecting that pool, closing it cleanly) goes in a hook. Four
hooks bracket the run loop:

```text
on_startup(State) -> State       # before brokers connect; build async resources
  -> brokers connect, subscriptions open
after_startup(Arc<State>)        # handlers are live; publish a first message, signal readiness
  ... running ...
  -> shutdown triggered (signal, or the run_until future resolves)
on_shutdown(Arc<State>)          # brokers still connected
  -> brokers shut down, in-flight handlers drained
after_shutdown(Arc<State>)       # final teardown
```

- **`on_startup`** receives the `State` **by value** and returns it, so its future can own the state
  across awaits - connect a resource, insert it, hand the state back. A failing `on_startup` aborts
  startup. The later hooks receive the state as a shared `Arc<State>`.
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

The hook's error type is inferred from the `Ok::<_, E>(..)` annotation; it only needs to implement
`std::error::Error + Send + Sync`. The resource is `Clone` and `Send + Sync`, so every concurrent
handler borrows the one instance from `State` - no per-message connection setup. The runnable
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
