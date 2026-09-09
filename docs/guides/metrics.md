# Metrics

The `metrics` feature collects Prometheus metrics for consumed and published messages. It is built
directly on the `prometheus` crate.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "metrics"] }
```

## Wiring it up

Create a `Metrics`, install its consume and publish layers, and keep the handle so you can export
later:

=== "Macros"

    ```rust
    --8<-- "examples/metrics_http.rs:wiring"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/metrics_http.rs:wiring"
    ```

`consume_layer` records every handled message; `publish_layer` records every published message.
`Metrics::with_registry(registry)` collects into your own registry instead of the default one.

## Metrics emitted

| Metric | Type | Labels |
|---|---|---|
| `ruststream_messages_consumed_total` | counter | `name`, `status` |
| `ruststream_consume_duration_seconds` | histogram | `name` |
| `ruststream_messages_published_total` | counter | `name`, `status` |

`name` is the subscription or destination name. `status` is the outcome: `ack` or `nack` for
consume, `ok` or `error` for publish.

## Exporting

`export` returns the current values in the Prometheus exposition format:

<!-- inline-rust: one-line export() API shape; the complete server, including this call, is compiled in metrics_http.rs and pulled in below -->
```rust
let body = metrics.export()?;
```

Serve the result of `export()` from a `/metrics` route in your own HTTP stack, or push it to a
gateway.

`metrics.registry()` returns the `prometheus::Registry` itself. You can register your own collectors
in it alongside RustStream's, or pass it to an existing exporter.

## A complete server

The [`metrics_http`](https://github.com/powersemmi/ruststream/blob/main/examples/metrics_http.rs)
example serves `/metrics` with [axum](https://github.com/tokio-rs/axum) and publishes the orders
that arrive on a `/orders` route, so an ordinary HTTP client increments the counters. Run it with
`cargo run --example metrics_http --features macros,memory,metrics`, then:

```bash
curl -X POST http://127.0.0.1:8080/orders -d '{"id":1,"quantity":3}'
curl http://127.0.0.1:8080/metrics
```

=== "Macros"

    ```rust
    --8<-- "examples/metrics_http.rs"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/metrics_http.rs"
    ```

If your service exports through the `otel` feature instead, a ready-made Grafana dashboard over the
full metrics inventory lives in
[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana); see the
[OpenTelemetry guide](opentelemetry.md).
