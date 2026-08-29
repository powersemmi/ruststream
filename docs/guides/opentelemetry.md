# OpenTelemetry

The `otel` feature gives a service distributed tracing: a trace flows from an incoming
message onto the replies it produces, so one trace spans the whole consume-transform-produce chain.
It is built on the typed publish-path context - the same seam that lets a publish transform read the
delivery that produced a reply.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "otel"] }
```

The feature has two halves. Propagation carries the
[W3C Trace Context](https://www.w3.org/TR/trace-context/) and emits `tracing` spans; it is
broker-agnostic and works with no exporter at all. Export ships with the feature too:
`Otel::builder().init()` installs the
[OpenTelemetry SDK and OTLP exporters](#the-otel-feature-sdk-otlp-and-the-metrics-inventory) as the
process globals and bridges the spans into them. Start there. Assembling your own subscriber (for
example [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry)) stays open, exactly as the
[logging](logging.md) guide leaves the subscriber to you.

## Wiring it up

Create an `OpenTelemetry`, add its consume layer app-wide, and bake its propagation onto the reply
publisher:

=== "Macros"

    ```rust
    --8<-- "tests/opentelemetry.rs:wiring"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_opentelemetry.rs:wiring"
    ```

- `consume_layer()` is a consume-side [layer](middleware.md): per delivery it reads the incoming
  `traceparent`, opens a `tracing` span for the handler, and records the *consumer's* span on the
  working headers. It applies to handlers mounted directly and through a [router](routing.md).
- `propagation()` is a static [publish layer](publishing.md): it copies the working `traceparent`
  (and `tracestate`) onto every reply, so a downstream service sees the consumer span as the reply's
  parent. Reuse it on a batch publisher with `for_batch(otel.propagation())`.

## What gets propagated

A delivery carrying `00-<trace-id>-<span-id>-01` continues that trace: the reply keeps the same
`trace-id` and carries a fresh `span-id` (the consumer's span), so the trace is linked end to end. A
delivery with no `traceparent` starts a fresh, sampled root trace. The spans are emitted under the
`ruststream.consume` target with `trace_id` / `span_id` / `subscription` fields.

## Reading the trace in a handler

The consumer's trace context is on the working headers, so a handler reads it the same way any
header is read - through the [context](context.md):

<!-- inline-rust: one-line read of the working traceparent inside a handler; the full traced app, including this access, is compiled in tests/opentelemetry.rs and embedded above -->
```rust
let traceparent = ctx.headers().get_str("traceparent");
```

Parse it with the OpenTelemetry SDK's `TraceContextPropagator` (the same parser the consume layer
uses) into an `opentelemetry::trace::SpanContext` to read the `trace_id()` / `span_id()` or to
check `is_sampled()`.

## Exporting to a collector

The propagation module stops at the W3C context and `tracing` spans; two ways ship them to a
collector. `Otel::builder().init()` below does it for you, and is the one to reach for first.
Assembling `tracing-opentelemetry` and an exporter yourself in the binary (the same split as
[logging](logging.md)) is there for a service that already owns that stack.

## The otel feature: SDK, OTLP, and the metrics inventory

`Otel::builder().init()` builds the OTLP exporters, installs the OpenTelemetry tracer and meter
providers as the process **globals**, and bridges `tracing` spans into them - so the spans the
propagation layer already opens are exported with no further wiring:

=== "Macros"

    ```rust
    --8<-- "examples/otel_export.rs:init"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/otel_export.rs:init"
    ```

The two middleware carry the dispatch metrics, labeled per handler
(`messaging.destination.name`), following the messaging semantic conventions plus a
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

Batch handlers bypass the per-message consume layer (the documented
[middleware](middleware.md) exception), so `ruststream.batch.size` is recorded by the batch
dispatch itself through the global meter: it goes live once `init()` installs the global
providers, and stays silent under a bare `attach()` unless you install your provider globally
yourself.

Because `init()` installs the global providers, business metrics need no exporter plumbing:
build the instruments once at startup into one storage object, share it through the typed state
(injectable with `State<..>` via `FromRef`), and everything in it rides the same OTLP pipeline:

=== "Macros"

    ```rust
    --8<-- "examples/otel_export.rs:business_metric"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/otel_export.rs:business_metric"
    ```

A ready-made Grafana dashboard over exactly this inventory lives in
[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana): import
`dashboards/ruststream.json`, point it at any Prometheus-compatible backend receiving the OTLP
metrics, and the panels light up per handler; its README doubles as the metrics contract.

Call `otel.shutdown()` at the end of `main`, after the app's graceful shutdown, to flush the last
spans and points. To compose the span bridge into your own subscriber stack (for example with the
`logging` feature's fmt layer), build with `.tracing_bridge(false)` and install the bridge
yourself; `.messaging_system("kafka")` stamps the semconv system attribute the core cannot derive
broker-agnostically.
