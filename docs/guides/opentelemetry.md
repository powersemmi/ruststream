# OpenTelemetry

The `otel` feature gives a service distributed tracing: one trace covers the whole
consume-transform-produce chain, from the incoming message to its replies.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "otel"] }
```

Propagation carries the [W3C Trace Context](https://www.w3.org/TR/trace-context/) and opens
`tracing` spans. It is broker-agnostic and works with no exporter at all.

## Wiring it up

Create an `OpenTelemetry`, add its consume layer app-wide, and put its propagation on the reply
wiring:

=== "Macros"

    ```rust
    --8<-- "tests/opentelemetry.rs:wiring"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_opentelemetry.rs:wiring"
    ```

- `OpenTelemetry::consume_layer()` is a consume-side [layer](middleware.md). For each delivery it
  reads the incoming `traceparent`, opens a `tracing` span for the handler, and records the
  *consumer's* trace context on the working headers. It applies to handlers mounted directly and
  through a [router](routing.md).
- `OpenTelemetry::propagation()` is a static [publish transform](publishing.md). It copies the
  working `traceparent` (and `tracestate`) onto every reply, so a downstream service sees the
  consumer span as the reply's parent. You can put the same transform on a batch publisher with
  `for_batch(otel.propagation())`.

## What gets propagated

A delivery whose `traceparent` reads `00-<trace-id>-<span-id>-01` continues that trace: the reply
keeps the same `trace-id` and gets a fresh `span-id`, the consumer's span. A delivery with no
`traceparent` starts a fresh root trace, marked as sampled. The consume layer emits its spans under
the `ruststream.consume` target with `trace_id` / `span_id` / `subscription` fields.

## Reading the trace in a handler

The consumer's trace context is on the working headers, so a handler reads it like any other
header, through the [context](context.md):

<!-- inline-rust: one-line read of the working traceparent inside a handler; the full traced app, including this access, is compiled in tests/opentelemetry.rs and embedded above -->
```rust
let traceparent = ctx.headers().get_str("traceparent");
```

You can parse the value with the OpenTelemetry SDK's `TraceContextPropagator`, the same parser the
consume layer uses. It returns an `opentelemetry::trace::SpanContext` with `trace_id()`,
`span_id()` and `is_sampled()`.

## Exporting to a collector

Propagation stops at the W3C context and `tracing` spans. Export ships with the same feature: a
single `Otel::builder().init()` installs the
[OpenTelemetry SDK and OTLP exporters](#the-otel-feature-sdk-otlp-and-the-metrics-inventory).
Start there.

Assembling [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) and an exporter
yourself, right in the binary, is the way for a service that already owns its subscriber stack. The
framework leaves the choice of subscriber to you, the same as in [logging](logging.md).

## The otel feature: SDK, OTLP, and the metrics inventory

`Otel::builder().init()` builds the OTLP exporters, installs the OpenTelemetry tracer and meter
providers as the process **globals**, and turns on the `tracing` span bridge. The spans that
propagation opens are exported with no further wiring:

=== "Macros"

    ```rust
    --8<-- "examples/otel_export.rs:init"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/otel_export.rs:init"
    ```

Two more middleware record the dispatch metrics: `Otel::consume_layer()` the per-delivery ones and
`Otel::publish_layer()` the per-publish ones. Spans come from a different layer,
`OpenTelemetry::consume_layer()` in the wiring above. The instruments are labeled per handler
(`messaging.destination.name`) and named by the messaging semantic conventions or in the
`ruststream.*` namespace:

| Instrument | Kind | What it measures |
|---|---|---|
| `messaging.client.consumed.messages` | counter | deliveries received |
| `messaging.process.duration` | histogram (semconv buckets) | handler processing time |
| `ruststream.messages.processed` | counter, `outcome` attribute | settlements: `ack`, `nack_requeue`, `nack_drop`, `retry_after` |
| `ruststream.messages.in_flight` | up-down counter | deliveries inside handlers (pool saturation vs `workers(n)`) |
| `ruststream.message.queue_time` | histogram | publish-to-handler-start lag, from the stamped publish-time header |
| `ruststream.messages.decode_failures` | counter | deliveries whose payload the codec rejected |
| `ruststream.messages.panics` | counter | handler invocations that panicked |
| `messaging.client.sent.messages` | counter, `error.type` on failure | publishes |
| `messaging.client.operation.duration` | histogram | the publish operation |
| `ruststream.message.payload.size` | histogram (`By`) | published payload sizes |
| `ruststream.batch.size` | histogram | decoded batch sizes handed to batch handlers |
| `ruststream.app.state` | observable gauge | the lifecycle state, from [`RunningApp::health`](http.md#a-healthz-endpoint) via `otel.observe_health(running.health())` |

Batch handlers bypass the per-message consume layer, the documented
[middleware](middleware.md) exception, so `ruststream.batch.size` is recorded by the batch dispatch
itself through the global meter. The metric is recorded once `init()` installs the global providers.
Under a bare `attach()` nothing is recorded until you install your provider globally yourself.

Business metrics need no exporter wiring of their own. Build the instruments once at startup into
one storage object and share it through the typed state (injected as `State<..>` via `FromRef`).
They export through the same OTLP pipeline:

=== "Macros"

    ```rust
    --8<-- "examples/otel_export.rs:business_metric"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/otel_export.rs:business_metric"
    ```

A ready-made Grafana dashboard over exactly this inventory lives in
[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana). Import
`dashboards/ruststream.json` and point it at any Prometheus-compatible backend receiving the OTLP
metrics: the panels fill in per handler. Its README doubles as the metrics contract.

Call `otel.shutdown()` at the end of `main`, after the app's graceful shutdown, to flush the last
spans and metric points. To put the span bridge into your own subscriber stack (alongside the
`logging` feature's fmt layer, for example), build the `Otel` with `.tracing_bridge(false)` and
install the bridge yourself. `.messaging_system("kafka")` stamps the semconv system attribute that
the broker-agnostic core cannot derive.
