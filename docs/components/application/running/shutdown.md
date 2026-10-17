---
sidebar_position: 4
title: Shutdown
description: "shutdown stops a started service gracefully: hooks, subscriptions, running handlers, then the brokers."
---

# Shutdown

[`shutdown`](api:ruststream::runtime::RunningApp::shutdown) stops a started service gracefully: hooks, subscriptions, running handlers, then the brokers.

`shutdown` runs `on_shutdown`, stops the subscriptions and waits for the handlers still running.
It then shuts the brokers down in reverse order of registration and runs `after_shutdown`.
The [lifecycle](../lifecycle.md) page shows the hooks, and `shutdown_timeout` there bounds the wait for running handlers.
A service that already tore itself down after a fail-fast failure returns that failure from `shutdown`.
Dropping a `RunningApp` without `shutdown` detaches the service with no graceful shutdown.

## Example

{/* single-form: the call is the same with and without the macros; the Manual tab of the full file differs only in the handler */}
```rust reference="examples/app_running.rs#shutdown"
```
