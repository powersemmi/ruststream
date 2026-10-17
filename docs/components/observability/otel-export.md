---
sidebar_position: 5
title: Export
description: "Otel sends the service's spans and metrics to an OpenTelemetry collector over OTLP, the dispatch metrics included."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Export

[`Otel`](api:ruststream::otel::export::Otel) sends the service's spans and metrics to an OpenTelemetry collector over OTLP, the dispatch metrics included.

[`Otel::builder`](api:ruststream::otel::export::Otel::builder) starts an [`OtelBuilder`](api:ruststream::otel::export::OtelBuilder).
It takes the `service.name`, the OTLP endpoint, the `messaging.system` the dispatch metrics carry, and extra resource attributes such as the environment.
An endpoint left unset comes from the standard `OTEL_EXPORTER_OTLP_*` environment, `http://localhost:4317` by default.
[`init`](api:ruststream::otel::export::OtelBuilder::init) builds the exporters and installs the tracer and meter providers as the process's globals.
It also installs a `tracing` subscriber that bridges spans into the tracer, so the spans of [Propagation](./otel-propagation.md) are exported with no further wiring.
A service with a subscriber of its own turns the bridge off with [`tracing_bridge(false)`](api:ruststream::otel::export::OtelBuilder::tracing_bridge) and adds it to its own stack.
[`consume_layer`](api:ruststream::otel::export::Otel::consume_layer) and [`publish_layer`](api:ruststream::otel::export::Otel::publish_layer) record the dispatch metrics, named by the messaging semantic conventions and labelled per subscription or destination:

| Instrument | What it measures |
|---|---|
| `messaging.client.consumed.messages` | deliveries received |
| `messaging.process.duration` | handler time |
| `ruststream.messages.processed` | settlements, by `outcome`: `ack`, `nack_requeue`, `nack_drop`, `retry_after` |
| `ruststream.messages.in_flight` | deliveries inside handlers |
| `ruststream.message.queue_time` | time from publish to handler, from the publish-time header the publish layer stamps |
| `ruststream.messages.decode_failures`, `ruststream.messages.panics` | payloads the codec rejected, handlers that panicked |
| `messaging.client.sent.messages`, `messaging.client.operation.duration` | publishes and their time |
| `ruststream.message.payload.size` | published payload sizes |
| `ruststream.batch.size` | decoded batch sizes, recorded by batch dispatch once `init` has run |

The duration histograms use the buckets the conventions advise, 5 ms to 10 s; [`view`](api:ruststream::otel::export::OtelBuilder::view) changes them per instrument.
[`observe_health`](api:ruststream::otel::export::Otel::observe_health) adds the `ruststream.app.state` gauge over the app's [health](../application/running/health.md) probe.
The service's own instruments come from `opentelemetry::global::meter` and go out through the same pipeline; the example builds them once at startup and shares them as [state](../application/state/index.md).
Spans and metric points are batched in the background.
[`shutdown`](api:ruststream::otel::export::Otel::shutdown) flushes them, after the app has stopped; dropping `Otel` does not.
A hand-written `main` makes that call, which is why the example keeps one with and without the macros.
[`attach`](api:ruststream::otel::export::OtelBuilder::attach) takes providers built by the caller and installs no globals: an exporter of your own, or an in-memory one in a test.
A Grafana dashboard over these instruments is in [`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana).

## Example

The example exports to a collector on `localhost:4317`; without one, the exporter retries in the background and the service keeps working:

```text
cargo run --example otel_export --features testing,macros,memory,json,otel -- run
```

`main` installs the exporters, runs the app, and flushes at the end.

{/* single-form: a hand-written main, the same with and without the macros */}
```rust reference="examples/otel_export.rs#main"
```

The app takes both layers and builds the service's own instrument at startup.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/otel_export.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/otel_export.rs#app"
```

</TabItem>
</Tabs>

The handler records the instrument it reads from the state.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/otel_export.rs#business_metric"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/otel_export.rs#business_metric"
```

</TabItem>
</Tabs>
