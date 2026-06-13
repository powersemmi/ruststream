# Logging

RustStream emits structured [`tracing`](https://docs.rs/tracing) events throughout dispatch,
publishing, and the service lifecycle. Like any well-behaved library it installs no subscriber on its
own - that choice belongs to the application. The `logging` feature is the batteries-included answer:
a colored console subscriber driven by `RUST_LOG`.

This is separate from the [`TracingLayer`](middleware.md#built-in-layers) middleware. `TracingLayer`
*emits* an event per message; the `logging` feature installs a subscriber that *renders* events
(RustStream's own and yours) to the terminal. Use them together to see per-message logs.

## With the generated CLI

When the `logging` feature is enabled, the `#[ruststream::app]` CLI calls the logger for you on the
`run` command, so a scaffolded service logs out of the box:

```toml
ruststream = { version = "0.3", features = ["macros", "memory", "json", "logging"] }
```

```bash
RUST_LOG=ruststream=debug,info cargo run -- run
```

Output goes to **stderr** (keeping stdout clean for `asyncapi gen`), with colors enabled
automatically when stderr is a terminal.

## By hand

Install the default logger once, early in `main`:

<!-- inline-rust: manual logger-init fragment; the shipped logging example uses the automatic #[ruststream::app] installer, so there is no compiled call site for the by-hand path -->
```rust
ruststream::logging::init()?;
tracing::info!("service starting");
```

`init` reads the filter from `RUST_LOG`, falling back to `info`. Tune the defaults through the
`Logging` builder:

<!-- inline-rust: manual Logging-builder fragment; the by-hand init path has no compiled call site (the logging example uses the automatic installer) -->
```rust
use ruststream::logging::Logging;

Logging::new()
    .with_default_filter("ruststream=debug,info")  // used when RUST_LOG is unset
    .with_target(false)                            // hide the event target column
    .try_init()?;
```

`init` / `try_init` never replace an existing subscriber: a second call (or one after another crate
installed a subscriber) returns `LoggingInitError::AlreadyInitialized` rather than panicking.

## Bring your own subscriber

The `logging` feature is optional sugar. Because RustStream only emits `tracing` events, any
subscriber works - install `tracing-subscriber`, `tracing-bunyan-formatter`, an OpenTelemetry layer,
or whatever your stack uses, and the same events flow through it. Skip the `logging` feature when you
do.
