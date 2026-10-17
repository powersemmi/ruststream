---
sidebar_position: 5
title: Health
description: "health hands out a HealthProbe that reports whether a started service is running, stopping, stopped or failed."
---

# Health

[`health`](api:ruststream::runtime::RunningApp::health) hands out a [`HealthProbe`](api:ruststream::runtime::HealthProbe) that reports whether a started service is running, stopping, stopped or failed.

The probe is a cheap clone, so each task that reports health holds its own.
Its state is a [`HealthState`](api:ruststream::runtime::HealthState): `Running`, `ShuttingDown`, `Stopped` or `Failed` with the fail-fast diagnostic.
The probe outlives `shutdown` and keeps reporting the final state.
[`stopping`](api:ruststream::runtime::RunningApp::stopping) resolves when the service tears itself down after a fail-fast failure, so a server beside it can stop with it, as the [HTTP](./http.md) page shows.

## Example

{/* single-form: a health endpoint is the same with and without the macros */}
```rust reference="examples/http_outbox.rs#healthz"
```
