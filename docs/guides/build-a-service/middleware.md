---
title: Middleware and observability
description: "The app wraps every subscriber in two layers: one logs each delivery and its outcome, one of the shop's own warns about slow handlers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Middleware and observability

The app wraps every subscriber in two layers: one logs each delivery and its outcome, one of the shop's own warns about slow handlers.

A [`Layer`](../../components/subscribers/middleware/layer.md) wraps a handler in another handler.
It runs code before the body, after it, or instead of it, and passes the outcome on.
A layer on the app wraps every subscriber the app mounts; a layer on a router wraps that router's subscribers only.
RustStream emits its own events through `tracing`: the service starting, each subscription opening, the service stopping.
[`TracingLayer`](api:ruststream::runtime::layers::TracingLayer) adds an event for every delivery: `delivery received` at `DEBUG`, then its outcome, an ack at `INFO`, a retry or a drop at `WARN`.
The `logging` feature prints them, as chapter 1 showed; any other `tracing` subscriber works in its place.
[Observability](../../components/observability/index.md) covers Prometheus metrics and OpenTelemetry export, which mount as layers the same way.

## A layer of the shop's own

`SlowHandlers` times each handler and warns when one runs longer than its threshold.
The warning names the subscription, read off the delivery's [context](../../components/subscribers/context/index.md).
A layer has no macro form: it is the same plain type in both tabs.
[`BlanketLayer`](api:ruststream::runtime::BlanketLayer) is what lets the app apply it to handlers whose types it hides.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="observability.rs" reference="examples/guide/observability.rs#layer"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="observability.rs" reference="examples/manual/guide/observability.rs#layer"
```

</TabItem>
</Tabs>

## The app

`service.rs` assembles the shop.
[`layer`](api:ruststream::runtime::RustStream::layer) adds a layer to the app; the first one added is the outermost.
Each module's router mounts with [`include_router`](api:ruststream::runtime::BrokerScope::include_router), and the app's layers wrap its subscribers.
`on_startup` opens the shared state of chapter 7, and `info()` is the description of chapter 9.
The function takes the broker as a parameter, so the command-line entry point and the HTTP host of chapter 10 build the same shop.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="service.rs" reference="examples/guide/service.rs#shop"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="service.rs" reference="examples/manual/guide/service.rs#shop"
```

</TabItem>
</Tabs>

## What the log shows

An order placed with the request id `req-1` leaves one line per step:

```text
INFO guide_http::orders order placed request=req-1 order=1 total_cents=14400
INFO ruststream::dispatch handler ack
INFO ruststream::dispatch handler ack
INFO ruststream::dispatch handler ack
INFO guide_http::notifications mail sent server=smtp://mail.internal:25 to="ada@example.com" text="We have your order 1."
INFO ruststream::dispatch handler ack
INFO ruststream::dispatch handler ack
INFO ruststream::dispatch handler ack
INFO guide_http::notifications mail sent server=smtp://mail.internal:25 to="ada@example.com" text="Order 1 is on its way: TRK-1-2."
INFO ruststream::dispatch handler ack
```

`RUST_LOG` tunes what is printed, for example `RUST_LOG=ruststream=debug,info`.
