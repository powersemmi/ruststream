---
title: Runtimes
description: "A service is written once and runs on tokio or on embassy: the handlers are the same, and the app picks its runner."
---

# Runtimes

A service is written once, with the same handlers and the same mount chain, and it runs on one of two runners: the multi-thread runner on tokio, or the one-task runner on embassy for a device.
The facade has a feature for each, `tokio` and `embassy`, and they add up: a build can enable both.

```toml
# A server: tokio is in the default features.
ruststream = "0.8"

# A device with a heap and no operating system: embassy alone, tokio never enters the build.
ruststream = { version = "0.8", default-features = false, features = ["embassy", "macros", "json"] }

# A broker crate on tokio, which turns the defaults off.
ruststream = { version = "0.8", default-features = false, features = ["tokio"] }
```

## The app picks the runner

`#[ruststream::app]` builds a `RustStream`, the app of the tokio runner.
`#[ruststream::app(runtime = embassy)]` builds an `EmbeddedApp`, the app of the embassy runner and one embassy task.
Any other runtime name is a compile error.
The handlers carry no runtime: `#[subscriber]` expands to the same code under both, and the runner states what it asks of a handler where it mounts it.

## What each runner asks of a handler

The tokio runner moves a handler between threads at each `.await`, so the future of a handler body must be `Send`.
A body that holds an `Rc` across an `.await` fails at the `include` that mounts it, and the error names the handler.
The embassy runner polls every handler on its one task and asks nothing of the future, so the same body mounts there.

## What each runner offers

The tokio runner offers `workers(n)`, `threads(n)`, the signals, metrics, OpenTelemetry and the CLI.
The embassy runner offers constant-count workers (`.pool::<N>()` and `.keyed::<N>()`), a retry store with a capacity declared at the mount site (`.retry_capacity::<CAP>()`), and shutdown and health through `Control` and `Health`.
An item one runner does not offer is a compile error on the other.

## Testing

`TestApp` runs a tokio app, and `testing::embedded::TestApp` runs an embedded app on the host with embassy-time's mock clock.
Both are in the facade when `testing` is on, beside each other.
With `testing` on, every broker an embedded app registers implements the in-process mode, `InProcess`.
