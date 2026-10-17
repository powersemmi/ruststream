---
sidebar_position: 1
title: Run
description: "run starts the service and serves until SIGINT or SIGTERM, then shuts it down gracefully."
---

# Run

[`run`](api:ruststream::runtime::RustStream::run) starts the service and serves until `SIGINT` or `SIGTERM`, then shuts it down gracefully.

`run` is the whole process: it connects the brokers, opens the subscriptions and runs the startup hooks, then waits for a signal.
A startup failure is the error it returns.
A signal starts the same [shutdown](./shutdown.md) a started service gets.
A subscriber that fails fast tears the service down, and `run` returns that failure as its error.
The `main` that [`#[ruststream::app]`](../app-macro.md) generates calls `run`.

## Example

{/* single-form: the call is the same with and without the macros; the Manual tab of the full file differs only in the handler */}
```rust reference="examples/app_running.rs#run"
```
