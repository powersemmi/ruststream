---
sidebar_position: 5
title: Health
description: "health hands out a HealthProbe that reports whether a started service is running, stopping, stopped or failed, and how many of its subscriptions consume."
---

# Health

[`health`](api:ruststream::runtime::RunningApp::health) hands out a [`HealthProbe`](api:ruststream::runtime::HealthProbe) that reports whether a started service is running, stopping, stopped or failed, and how many of its subscriptions consume.

The probe is a cheap clone, so each task that reports health holds its own.
Its state is a [`HealthState`](api:ruststream::runtime::HealthState): `Running`, `ShuttingDown`, `Stopped` or `Failed` with the fail-fast diagnostic.
The probe outlives `shutdown` and keeps reporting the final state.
[`stopping`](api:ruststream::runtime::RunningApp::stopping) resolves when the service tears itself down after a fail-fast failure, so a server beside it can stop with it, as the [HTTP](./http.md) page shows.

[`subscriptions`](api:ruststream::runtime::HealthProbe::subscriptions) returns a [`SubscriptionCount`](api:ruststream::runtime::SubscriptionCount): the subscriptions that consume and the subscriptions the service started.
A subscription counts once, whatever its number of workers.
The count changes only when a subscription starts or stops, so a delivery pays nothing for it.

## Example

{/* single-form: a health endpoint is the same with and without the macros */}
```rust reference="examples/http_outbox.rs#healthz"
```

## A subscription that stops

A subscription's stream can end while the service runs: the server cancels a consumer when its queue is deleted, or the broker gives up reconnecting.
By default the subscription logs the end and stops, and the rest of the service runs on.
The probe still reports `Running` and counts one subscription fewer, and the service decides whether that is healthy.
[`on_subscription_end`](api:ruststream::runtime::RustStream::on_subscription_end) with [`SubscriptionEnd::FailFast`](api:ruststream::runtime::SubscriptionEnd) makes the end a fail-fast failure instead.
The service then shuts down gracefully, `run` returns an error that names the subscription, and the probe reports `Failed`.
An end during shutdown is part of the shutdown and is never reported.

{/* single-form: the setting is the same with and without the macros; the Manual tab of the full file differs only in the handler */}
```rust reference="examples/app_running.rs#subscription_end"
```
