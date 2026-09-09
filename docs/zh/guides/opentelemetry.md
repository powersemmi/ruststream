# OpenTelemetry

`otel` feature 为服务提供分布式链路追踪。一条链路从进来的消息延续到它产生的回复，覆盖完整的
“消费-转换-生产”链条。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "otel"] }
```

上下文传播携带 [W3C Trace Context](https://www.w3.org/TR/trace-context/)，并开启 `tracing` span。
它与具体 Broker 无关，不配置 exporter 也能工作。

## 接线

创建一个 `OpenTelemetry`，在整个应用上装上它的消费层，再把它的传播加到回复的接线上：

=== "宏"

    ```rust
    --8<-- "tests/opentelemetry.rs:wiring"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_opentelemetry.rs:wiring"
    ```

- `OpenTelemetry::consume_layer()` 是消费侧的[层](middleware.md)。每次投递时，它读取传入的
  `traceparent`，为处理器开启一个 `tracing` span，并把*消费方*的 span 写进消息头的工作副本。它对
  直接挂载的处理器和通过[路由器](routing.md)挂载的处理器都生效。
- `propagation()` 是静态的[发布变换](publishing.md)。它把工作副本里的 `traceparent`（以及
  `tracestate`）复制到每一条回复上，下游服务因此把消费方的 span 看作回复的父 span。在批量发布者
  上，用 `for_batch(otel.propagation())` 复用同一个变换。

## 会传播什么

带有 `00-<trace-id>-<span-id>-01` 的投递会延续那条链路。回复保持同一个 `trace-id`，并拿到一个新的
`span-id`，也就是消费方的 span，链路因此首尾相连。没有 `traceparent` 的投递会开启一条新的根链路，
并标记为已采样。这些 span 在 `ruststream.consume` target 下发出，带 `trace_id` / `span_id` /
`subscription` 字段。

## 在处理器中读取链路追踪上下文

消费方的链路追踪上下文就在消息头的工作副本里。处理器读它和读任何一个消息头一样，都通过
[上下文](context.md)：

<!-- inline-rust: one-line read of the working traceparent inside a handler; the full traced app, including this access, is compiled in tests/opentelemetry.rs and embedded above -->
```rust
let traceparent = ctx.headers().get_str("traceparent");
```

这个值可以用 OpenTelemetry SDK 的 `TraceContextPropagator` 解析成
`opentelemetry::trace::SpanContext`，从中读取 `trace_id()` 和 `span_id()`，或者检查 `is_sampled()`。
`OpenTelemetry::consume_layer()` 用的也是这个解析器。

## 导出到 collector

传播只做到 W3C 上下文和 `tracing` span 为止。导出也在同一个 feature 里：一次
`Otel::builder().init()` 就装上
[OpenTelemetry SDK 与 OTLP exporter](#the-otel-feature-sdk-otlp-and-the-metrics-inventory)。建议
从它开始。

在二进制里自己组装 [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) 和一个
exporter，适合已经有一套订阅者栈的服务。订阅者由你自己选，这一点和[日志](logging.md)一样。

## otel feature：SDK、OTLP 与指标清单 { #the-otel-feature-sdk-otlp-and-the-metrics-inventory }

`Otel::builder().init()` 会构建 OTLP exporter，把 OpenTelemetry 的 tracer provider 和 meter
provider 装成进程**全局**的，并把 `tracing` span 桥接进去。传播开启的那些 span 不必再接线就会导出。
指标由另外两个中间件记录，装在应用上：

=== "宏"

    ```rust
    --8<-- "examples/otel_export.rs:init"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/otel_export.rs:init"
    ```

消费侧是 `Otel::consume_layer()`，发布侧是 `Otel::publish_layer()`；开启 span 的是“接线”一节里的
`OpenTelemetry::consume_layer()`。这两个指标中间件按处理器打标签（`messaging.destination.name`），
名字遵循 messaging 语义约定，另加一个 `ruststream.*` 命名空间：

| 指标项 | 类型 | 度量的内容 |
|---|---|---|
| `messaging.client.consumed.messages` | 计数器 | 收到的投递数 |
| `messaging.process.duration` | 直方图（semconv 分桶） | 处理器的处理耗时 |
| `ruststream.messages.processed` | 计数器，带 `outcome` 属性 | 结算结果：`ack`、`nack_requeue`、`nack_drop`、`retry_after` |
| `ruststream.messages.in_flight` | 上下计数器 | 正处于处理器内部的投递数（相对 `workers(n)` 的池饱和度） |
| `ruststream.message.queue_time` | 直方图 | 从发布到处理器开始处理之间的滞后，取自打上的发布时间消息头 |
| `ruststream.messages.decode_failures` | 计数器 | 编解码器拒绝载荷的投递数 |
| `ruststream.messages.panics` | 计数器 | 发生 panic 的处理器调用次数 |
| `messaging.client.sent.messages` | 计数器，失败时带 `error.type` | 发布次数 |
| `messaging.client.operation.duration` | 直方图 | 发布操作本身 |
| `ruststream.message.payload.size` | 直方图（`By`） | 已发布载荷的大小 |
| `ruststream.batch.size` | 直方图 | 交给批量处理器的、解码后的批大小 |
| `ruststream.app.state` | 可观测仪表 | 生命周期状态，经由 `otel.observe_health(running.health())` 取自 [`RunningApp::health`](http.md#a-healthz-endpoint) |

批量处理器绕过按消息生效的消费层，这是[中间件](middleware.md)里写明的例外。因此
`ruststream.batch.size` 由批量分发自己通过全局 meter 记录。`init()` 装上全局 provider 之后，这个
指标开始记录。只调用 `attach()` 时它不记录任何数据，除非你自己把 provider 装成全局的。

业务指标不需要另外搭一套导出管道。在启动时把这些指标项一次性构建到一个存储对象里，通过类型化状态
共享出去（用 `FromRef` 就能以 `State<..>` 注入）。里面的一切都走同一条 OTLP 管线：

=== "宏"

    ```rust
    --8<-- "examples/otel_export.rs:business_metric"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/otel_export.rs:business_metric"
    ```

[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana) 里有一个照着这份清单做的
Grafana 仪表盘。导入 `dashboards/ruststream.json`，把它指向任意一个接收 OTLP 指标、兼容 Prometheus
的后端，各个面板就会按处理器填上数据。它的 README 就是这份指标契约。

在 `main` 的末尾、应用正常停止之后调用 `otel.shutdown()`，把最后的 span 和指标点发出去。如果要把
span 桥接层放进自己的订阅者栈（例如和 `logging` feature 的 fmt 层一起用），可以用
`.tracing_bridge(false)` 构建 `Otel`，再自己装上桥接层。`.messaging_system("kafka")` 打上 semconv
的 system 属性，与 Broker 无关的核心推导不出这个属性。
