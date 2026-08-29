# 指标

`metrics` feature 会为消费和发布的消息收集 Prometheus 指标。它直接构建在 `prometheus` crate 之上，
并以 Prometheus 的 exposition 格式暴露数据。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "metrics"] }
```

## 接线

创建一个 `Metrics`，装上它的消费层和发布层，并留住句柄以便之后导出：

=== "宏"

    ```rust
    --8<-- "examples/metrics_http.rs:wiring"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/metrics_http.rs:wiring"
    ```

`consume_layer` 记录每一条处理过的消息，`publish_layer` 记录每一条发布出去的消息。如果想收集到已有的
registry 而不是新建一个，用 `Metrics::with_registry(registry)`。

## 产生的指标

| 指标 | 类型 | 标签 |
|---|---|---|
| `ruststream_messages_consumed_total` | 计数器 | `name`、`status` |
| `ruststream_consume_duration_seconds` | 直方图 | `name` |
| `ruststream_messages_published_total` | 计数器 | `name`、`status` |

`name` 是订阅名或目标名；`status` 是结果（消费侧是 `ack` 或 `nack`，发布侧是 `ok` 或 `error`）。

## 导出

`export` 会把当前的取值渲染成 Prometheus 的 exposition 格式：

<!-- inline-rust: one-line export() API shape; the complete server, including this call, is compiled in metrics_http.rs and pulled in below -->
```rust
let body = metrics.export()?;
```

和 AsyncAPI 一样，托管这件事由你负责：在你自己的 HTTP 栈里用一个 `/metrics` 路由把 `export()` 的结果
提供出去，或者推送到某个 gateway。如果你想把自己的 collector 和 RustStream 的注册在一起，或者想用已有
的 exporter，`metrics.registry()` 会返回底层的 `prometheus::Registry`。

## 一个完整的服务器

[`metrics_http`](https://github.com/powersemmi/ruststream/blob/main/examples/metrics_http.rs) 示例用
[axum](https://github.com/tokio-rs/axum) 提供 `/metrics`，并通过一个 `/orders` 路由发布订单，于是一个
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

如果你的服务改用 `otel` feature 导出，
[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana) 里有一份覆盖全部指标清单的
现成 Grafana 仪表板；另见 [OpenTelemetry 指南](opentelemetry.md)。
