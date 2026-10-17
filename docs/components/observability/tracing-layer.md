---
sidebar_position: 2
title: TracingLayer
description: "TracingLayer logs every delivery and its outcome as tracing events, so handlers hold no logging code of their own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# TracingLayer

[`TracingLayer`](api:ruststream::runtime::layers::TracingLayer) logs every delivery and its outcome as `tracing` events, so handlers hold no logging code of their own.

It is a [layer](../subscribers/middleware/layer.md) bundled with the runtime.
Added to the app with `RustStream::layer`, it wraps every handler, those of routers included.
Each delivery gives a `DEBUG` event when it reaches the handler and one more event when the handler settles it.
An ack is logged at `INFO`.
A nack is logged at `WARN` with `requeue=true` or `requeue=false`, and a delayed retry at `WARN` with its delay.
The events go under the `ruststream::dispatch` target, so `RUST_LOG=ruststream::dispatch=debug` shows them alone.
The handler runs inside a `TRACE` span; [`TracingLayer::with_target`](api:ruststream::runtime::layers::TracingLayer::with_target) records a name on that span.
The layer never changes the outcome it logs.
The events need a `tracing` subscriber to show, such as the one [Logging](./logging.md) installs.
[Batch handlers](../subscribers/batches.md) are not wrapped by app layers, so their deliveries are not logged here.

## Example

The handler acks an order and drops an empty one; the layer logs both outcomes.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/tracing_layer.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/tracing_layer.rs#app"
```

</TabItem>
</Tabs>
