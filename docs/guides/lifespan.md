# Lifespan and shared state

Most services need resources that are created once at startup and shared by every handler: a
database pool, an HTTP client, parsed configuration. RustStream gives you a shared `State` type-map
plus lifecycle hooks that run at fixed points around the run loop.

## Shared state

`State` is a type-map: one value per type. Put ready-made values in at build time with
`insert_state`, and read them from any handler or middleware through the `Context`:

```rust
RustStream::new(info)
    .insert_state(Config::from_env())
    .with_broker(broker, |b| b.include(handle));
```

```rust
use ruststream::runtime::{Context, HandlerResult};

#[subscriber("orders")]
async fn handle(order: &Order, ctx: &mut Context) -> HandlerResult {
    let config = ctx.get::<Config>().expect("config inserted at build time");
    // ... use config ...
    HandlerResult::Ack
}
```

`ctx.get::<T>()` returns `Option<&T>`; it is `None` only if no value of that type was inserted.
Inserting the same type again replaces the previous value.

A `#[subscriber]` handler opts into the context by taking a second parameter, `ctx: &mut Context`,
after the payload. Omit it when the handler does not need state.

## Lifecycle hooks

Anything that needs `async` work (connecting that pool, closing it cleanly) goes in a hook. Four
hooks bracket the run loop:

```text
on_startup(State) -> State     # before brokers connect; build async resources
  -> brokers connect, subscriptions open
after_startup(&State)          # handlers are live; publish a first message, signal readiness
  ... running ...
  -> shutdown triggered (signal, or the run_until future resolves)
on_shutdown(&State)            # brokers still connected
  -> brokers shut down, in-flight handlers drained
after_shutdown(&State)         # final teardown
```

- **`on_startup`** receives the `State` **by value** and returns it, so its future can own the state
  across awaits - connect a resource, insert it, hand the state back. A failing `on_startup` aborts
  startup.
- **`after_startup`** runs once handlers are spawned. Use it to publish an initial message or signal
  readiness. A failure here also aborts startup.
- **`on_shutdown`** runs when shutdown begins, while brokers are still connected.
- **`after_shutdown`** runs after brokers are down, for final async teardown.

Startup hooks abort the service on error; shutdown hooks only log their error, so shutdown always
runs to completion. Hooks of the same kind run in registration order.

## Passing a database connection

The common case: open a pool before serving, share it with every handler, close it on the way out.

```rust
use ruststream::runtime::{AppInfo, Context, HandlerResult, RustStream};
use sqlx::PgPool;

#[subscriber("orders")]
async fn handle(order: &Order, ctx: &mut Context) -> HandlerResult {
    let pool = ctx.get::<PgPool>().expect("pool inserted in on_startup");
    if sqlx::query("insert into orders (id) values ($1)")
        .bind(order.id as i64)
        .execute(pool)
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // before connect: open the pool and put it in shared state
        .on_startup(|mut state| async move {
            let pool = PgPool::connect("postgres://localhost/orders").await?;
            state.insert(pool);
            Ok::<_, sqlx::Error>(state)
        })
        // after shutdown: close it cleanly
        .after_shutdown(|state| async move {
            if let Some(pool) = state.get::<PgPool>() {
                pool.close().await;
            }
            Ok::<_, sqlx::Error>(())
        })
        .with_broker(broker, |b| b.include(handle))
}
```

The hook's error type is inferred from the `Ok::<_, E>(..)` annotation; it only needs to implement
`std::error::Error + Send + Sync`. The pool is `Clone` and `Send + Sync`, so every concurrent handler
borrows the one instance from `State` - no per-message connection setup.

## Shutdown timeout

By default `run` waits indefinitely for in-flight handlers to finish after shutdown is triggered.
Bound that wait with `shutdown_timeout`; handlers still running after it are aborted:

```rust
use std::time::Duration;

RustStream::new(info)
    .shutdown_timeout(Duration::from_secs(10))
    .with_broker(broker, |b| b.include(handle));
```
