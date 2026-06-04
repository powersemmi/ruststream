# Metrics

The `metrics` feature collects Prometheus metrics for consumed and published messages. It is built on
the `prometheus` crate directly and exposes the data in the Prometheus exposition format.

```toml
ruststream = { version = "0.2", features = ["macros", "memory", "metrics"] }
```

## Wiring it up

Create a `Metrics`, install its consume and publish layers, and keep the handle to export later:

```rust
use ruststream::metrics::Metrics;

let metrics = Metrics::new()?;

let app = RustStream::new(info)
    .layer(metrics.consume_layer())
    .publish_layer(metrics.publish_layer())
    .with_broker(broker, |b| b.include(handle, JsonCodec));
```

`consume_layer` records every handled message; `publish_layer` records every published message. To
collect into an existing registry instead of a fresh one, use `Metrics::with_registry(registry)`.

## Metrics emitted

| Metric | Type | Labels |
|---|---|---|
| `ruststream_messages_consumed_total` | counter | `name`, `status` |
| `ruststream_consume_duration_seconds` | histogram | `name` |
| `ruststream_messages_published_total` | counter | `name`, `status` |

`name` is the subscription or destination name; `status` is the outcome (`ack`, `requeue`, `drop`
for consume; `ok`, `error` for publish).

## Exporting

`export` renders the current values in the Prometheus exposition format:

```rust
let body = metrics.export()?;
```

Hosting is your responsibility, as with AsyncAPI: serve `export()` from a `/metrics` route in your
own HTTP stack, or push it to a gateway. `metrics.registry()` returns the underlying
`prometheus::Registry` if you want to register your own collectors alongside RustStream's or use an
existing exporter.
