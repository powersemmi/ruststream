---
sidebar_position: 4
title: Propagation
description: "The otel feature carries the W3C trace context from a delivery to its replies and opens a span for every delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Propagation

The `otel` feature carries the W3C trace context from a delivery to its replies and opens a span for every delivery.

[`OpenTelemetry`](api:ruststream::otel::propagation::OpenTelemetry) hands out two pieces.
[`consume_layer`](api:ruststream::otel::propagation::OpenTelemetry::consume_layer) is a [layer](../subscribers/middleware/layer.md) for the app.
For each delivery it reads the `traceparent` and `tracestate` headers and continues that trace with a new span id: the consumer's span.
A delivery without a valid `traceparent` starts a new sampled trace.
The handler runs inside an `INFO` span named `ruststream.consume`, which records the subscription and both ids.
The layer writes the consumer's context into the delivery's working headers, where a handler reads it: `ctx.headers().get_str("traceparent")`.
[`propagation`](api:ruststream::otel::propagation::OpenTelemetry::propagation) is a [publish transform](../subscribers/publishing/transforms/publish-transform.md) for a reply position.
It copies `traceparent` and `tracestate` from the working headers onto every reply, so the next service continues the same trace with the consumer span as the parent.
`for_batch(otel.propagation())` puts it on a [batch reply](../subscribers/publishing/transforms/batch-transform.md).
The headers are parsed and written by the OpenTelemetry SDK's W3C propagator, which follows the specification strictly.
Propagation works with any `tracing` subscriber, or none; [Export](./otel-export.md) sends the spans to a collector.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/otel_propagation.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/otel_propagation.rs#app"
```

</TabItem>
</Tabs>
