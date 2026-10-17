---
sidebar_position: 3
title: Start
description: "start brings the service up and returns a RunningApp, so the caller owns the process and decides when it stops."
---

# Start

[`start`](api:ruststream::runtime::RustStream::start) brings the service up and returns a [`RunningApp`](api:ruststream::runtime::RunningApp), so the caller owns the process and decides when it stops.

`start` connects the brokers, opens the subscriptions and runs the startup hooks, then returns.
A startup failure is the error `start` returns, before the caller accepts its own traffic.
`start` installs no signal handlers: the caller stops the service with [`shutdown`](./shutdown.md).
The `RunningApp` also hands out the service's [health](./health.md) and publishers for tasks beside it, as the [HTTP](./http.md) page shows.

## Example

{/* single-form: the call is the same with and without the macros; the Manual tab of the full file differs only in the handler */}
```rust reference="examples/app_running.rs#start"
```
