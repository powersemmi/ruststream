---
sidebar_position: 3
title: Prometheus
description: "The metrics feature counts and times deliveries and counts publishes in a Prometheus registry that the service serves on /metrics."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prometheus

The `metrics` feature counts and times deliveries and counts publishes in a Prometheus registry that the service serves on `/metrics`.

A [`Metrics`](api:ruststream::metrics::Metrics) value owns the collectors and the registry they are registered in.
[`Metrics::new`](api:ruststream::metrics::Metrics::new) registers them in the process's default registry, so a process builds one.
[`Metrics::with_registry`](api:ruststream::metrics::Metrics::with_registry) registers them in a registry of your own, and [`registry`](api:ruststream::metrics::Metrics::registry) hands the registry out for collectors of your own.
[`consume_layer`](api:ruststream::metrics::Metrics::consume_layer) is a [layer](../subscribers/middleware/layer.md) for the app: it times every handler and counts its outcome.
[`publish_layer`](api:ruststream::metrics::Metrics::publish_layer) is a [publish layer](../subscribers/middleware/publish-layer.md): it counts every publish a handler makes, replies included, and the failed ones too.
`Metrics` is cheap to clone, and every clone writes into the same collectors.

| Metric | Type | Labels |
|---|---|---|
| `ruststream_messages_consumed_total` | counter | `name`, `status`: `ack` or `nack` |
| `ruststream_consume_duration_seconds` | histogram | `name` |
| `ruststream_messages_published_total` | counter | `name`, `status`: `ok` or `error` |

`name` is the subscription on the consume side and the destination on the publish side.
With the `poll-diagnostics` feature, [`observe_poll_diagnostics`](api:ruststream::metrics::Metrics::observe_poll_diagnostics) adds the time handlers spend in one poll, as described in [Diagnostics](../subscribers/dispatch/poll-diagnostics.md).
[`export`](api:ruststream::metrics::Metrics::export) renders the registry in the Prometheus text format.
Serving it is the service's part: a route of its HTTP server, or a push to a gateway.

## Example

The example serves `/metrics` on port 8080:

```text
cargo run --example metrics --features testing,macros,memory,json,metrics
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/metrics.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/metrics.rs#app"
```

</TabItem>
</Tabs>

The route that serves the registry is an ordinary axum handler.

{/* single-form: an axum route, the same code with and without the macros */}
```rust reference="examples/metrics.rs#scrape"
```
