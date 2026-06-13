# Metrics

The `metrics` feature collects Prometheus metrics for consumed and published messages. It is built on
the `prometheus` crate directly and exposes the data in the Prometheus exposition format.

```toml
ruststream = { version = "0.3", features = ["macros", "memory", "metrics"] }
```

## Wiring it up

Create a `Metrics`, install its consume and publish layers, and keep the handle to export later:

```rust
--8<-- "examples/metrics_http.rs:wiring"
```

`consume_layer` records every handled message; `publish_layer` records every published message. To
collect into an existing registry instead of a fresh one, use `Metrics::with_registry(registry)`.

## Metrics emitted

| Metric | Type | Labels |
|---|---|---|
| `ruststream_messages_consumed_total` | counter | `name`, `status` |
| `ruststream_consume_duration_seconds` | histogram | `name` |
| `ruststream_messages_published_total` | counter | `name`, `status` |

`name` is the subscription or destination name; `status` is the outcome (`ack` or `nack` for
consume; `ok` or `error` for publish).

## Exporting

`export` renders the current values in the Prometheus exposition format:

<!-- inline-rust: one-line export() API shape; the complete server, including this call, is compiled in metrics_http.rs and pulled in below -->
```rust
let body = metrics.export()?;
```

Hosting is your responsibility, as with AsyncAPI: serve `export()` from a `/metrics` route in your
own HTTP stack, or push it to a gateway. `metrics.registry()` returns the underlying
`prometheus::Registry` if you want to register your own collectors alongside RustStream's or use an
existing exporter.

## A complete server

The [`metrics_http`](https://github.com/powersemmi/ruststream/blob/main/examples/metrics_http.rs)
example serves `/metrics` with [axum](https://github.com/tokio-rs/axum) and publishes orders through
a `/orders` route, so an HTTP client drives the counters. Run it with
`cargo run --example metrics_http --features macros,memory,metrics`, then:

```bash
curl -X POST http://127.0.0.1:8080/orders -d '{"id":1,"quantity":3}'
curl http://127.0.0.1:8080/metrics
```

```rust
--8<-- "examples/metrics_http.rs"
```
