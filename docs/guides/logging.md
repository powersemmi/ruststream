# Logging

RustStream emits structured [`tracing`](https://docs.rs/tracing) events throughout dispatch,
publishing, and the service lifecycle. The application installs the subscriber for those events.
The `logging` feature ships a ready-made one: a colored console subscriber driven by `RUST_LOG`.

The [`TracingLayer`](middleware.md#built-in-layers) middleware emits the per-message event, and the
subscriber from the `logging` feature prints it.

## With the generated CLI

With the `logging` feature enabled, the CLI from `#[ruststream::app]` installs the subscriber
itself on the `run` command.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "logging"] }
```

```bash
RUST_LOG=ruststream=debug,info cargo run -- run
```

The subscriber writes to **stderr**, so stdout stays clean for `asyncapi gen`. Colors turn on when
stderr is a terminal.

## By hand

Install the default subscriber once, early in `main`:

<!-- inline-rust: manual logger-init fragment; the shipped logging example uses the automatic #[ruststream::app] installer, so there is no compiled call site for the by-hand path -->
```rust
ruststream::logging::init()?;
tracing::info!("service starting");
```

Without `RUST_LOG` the filter is `info`. You can change the defaults through the `Logging` builder:

<!-- inline-rust: manual Logging-builder fragment; the by-hand init path has no compiled call site (the logging example uses the automatic installer) -->
```rust
use ruststream::logging::Logging;

Logging::new()
    .with_default_filter("ruststream=debug,info")  // used when RUST_LOG is unset
    .with_target(false)                            // hide the event target column
    .try_init()?;
```

A subscriber that you or another crate already installed stays in place: `init` and `try_init`
return `LoggingInitError::AlreadyInitialized`.

## Bring your own subscriber

Instead of the `logging` feature you can install any subscriber for `tracing` events: one built on
the `tracing-subscriber` or `tracing-bunyan-formatter` crates, one with an OpenTelemetry layer, or
the one your stack already uses.
