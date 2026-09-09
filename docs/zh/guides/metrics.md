# 指标

`metrics` feature 会为消费和发布的消息收集 Prometheus 指标。它直接构建在 `prometheus` crate 之上。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "metrics"] }
```

## 接线

创建一个 `Metrics`，装上它的消费层和发布层，并保留句柄，以便之后导出指标：

=== "宏"

    ```rust
    --8<-- "examples/metrics_http.rs:wiring"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/metrics_http.rs:wiring"
    ```

`consume_layer` 记录每一条处理过的消息，`publish_layer` 记录每一次发布尝试，包括返回错误的那些。
`Metrics::with_registry(registry)` 把指标收集到你自己的 registry 中，而不是默认的 registry。

## 产生的指标

| 指标 | 类型 | 标签 |
|---|---|---|
| `ruststream_messages_consumed_total` | 计数器 | `name`、`status` |
| `ruststream_consume_duration_seconds` | 直方图 | `name` |
| `ruststream_messages_published_total` | 计数器 | `name`、`status` |

`name` 是订阅名或发布目标名。`status` 是结果：消费时是 `ack` 或 `nack`，发布时是 `ok` 或 `error`。

## 导出

`export` 会把当前的取值渲染成 Prometheus 的 exposition 格式：

<!-- inline-rust: one-line export() API shape; the complete server, including this call, is compiled in metrics_http.rs and pulled in below -->
```rust
let body = metrics.export()?;
```

在你自己的 HTTP 栈里用 `/metrics` 路由提供 `export()` 的结果，或者把它推送到 push-gateway。

`metrics.registry()` 返回底层的 `prometheus::Registry`。你可以在它里面注册自己的 collector，与
RustStream 的指标放在一起，也可以把它交给现成的 exporter。

## 一个完整的服务器

[`metrics_http`](https://github.com/powersemmi/ruststream/blob/main/examples/metrics_http.rs) 示例用
[axum](https://github.com/tokio-rs/axum) 提供 `/metrics`，并通过 `/orders` 路由发布订单，于是普通的
HTTP 客户端就能驱动这些计数器。用
`cargo run --example metrics_http --features macros,memory,metrics` 运行它，然后：

```bash
curl -X POST http://127.0.0.1:8080/orders -d '{"id":1,"quantity":3}'
curl http://127.0.0.1:8080/metrics
```

=== "宏"

    ```rust
    --8<-- "examples/metrics_http.rs"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/metrics_http.rs"
    ```

如果你的服务通过 `otel` feature 导出指标，
[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana) 里有一份现成的 Grafana
仪表盘，覆盖全部指标清单。另见 [OpenTelemetry 指南](opentelemetry.md)。
