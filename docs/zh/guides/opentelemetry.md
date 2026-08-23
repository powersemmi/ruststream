# OpenTelemetry

`otel` feature 为服务带来分布式链路追踪：一条链路从进来的消息一路流向它所产生的回复，因此单条链路
就覆盖了完整的“消费-转换-生产”链条。它建立在类型化的发布路径上下文之上，也正是这条接缝让发布变换
能够读到产生某条回复的那次投递。

```toml
ruststream = { version = "0.6", features = ["macros", "memory", "json", "otel"] }
```

该 feature 分成两半。传播那一半负责携带 [W3C Trace Context](https://www.w3.org/TR/trace-context/)
并发出 `tracing` span；它与具体 Broker 无关，即使完全不配 exporter 也能工作。导出那一半随 feature
一起提供：[OpenTelemetry SDK 与 OTLP exporter](#the-otel-feature-sdk-otlp-and-the-metrics-inventory)
藏在 `Otel::builder().init()` 背后，它会装上全局的 provider，并把 span 桥接进去；也可以自己
组装一个订阅者（例如用
[`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry)），就像[日志](logging.md)指南同样
把订阅者留给你自己决定那样。

## 接线

创建一个 `OpenTelemetry`，把它的消费侧层加到整个应用上，再把它的传播能力固化到回复发布者上：

```rust
--8<-- "tests/opentelemetry.rs:wiring"
```

- `consume_layer()` 是消费侧的一个[层](middleware.md)：每次投递时，它读取传入的 `traceparent`，为
  处理器开一个 `tracing` span，并把*消费方*的 span 记录到工作消息头上。它对直接挂载的处理器，以及
  通过[路由器](routing.md)挂载的处理器都生效。
- `propagation()` 是一个静态的[发布层](publishing.md)：它把工作副本里的 `traceparent`（以及
  `tracestate`）复制到每一条回复上，于是下游服务会把消费方的 span 视为该回复的父 span。在批量
  发布者上，用 `for_batch(otel.propagation())` 复用它。

## 会传播什么

带着 `00-<trace-id>-<span-id>-01` 的一次投递会延续那条链路：回复保持同一个 `trace-id`，并带上一个
新的 `span-id`（消费方的 span），链路因此首尾相连。没有 `traceparent` 的投递则开启一条全新的、
已采样的根链路。这些 span 在 `ruststream.consume` target 下发出，带有 `trace_id` / `span_id` /
`subscription` 字段。

## 在处理器中读取链路追踪上下文

消费方的链路追踪上下文就放在工作消息头里，因此处理器读它的方式和读任何一个消息头一样，都是通过
[上下文](context.md)：

<!-- inline-rust: one-line read of the working traceparent inside a handler; the full traced app, including this access, is compiled in tests/opentelemetry.rs and embedded above -->
```rust
let traceparent = ctx.headers().get_str("traceparent");
```

用 OpenTelemetry SDK 的 `TraceContextPropagator`（消费侧层用的就是同一个解析器）把它解析成一个
`opentelemetry::trace::SpanContext`，就能读取 `trace_id()` / `span_id()`，或者检查 `is_sampled()`。

## 导出到 collector

传播模块只做到 W3C 上下文和 `tracing` span 为止；把它们送到 collector 有两条路。一是在二进制里自己
组装 `tracing-opentelemetry` 和一个 exporter（与[日志](logging.md)那边的分工相同），二是让下面的
`Otel::builder().init()` 替你完成。

## otel feature：SDK、OTLP 与指标清单 { #the-otel-feature-sdk-otlp-and-the-metrics-inventory }

`Otel::builder().init()` 会构建 OTLP exporter，把 OpenTelemetry 的 tracer provider 和 meter
provider 装成进程**全局**的，并把 `tracing` span 桥接进去，于是不必再接任何线，传播层已经开出来的
那些 span 就会随之导出：

```rust
--8<-- "examples/otel_export.rs:init"
```

这两个中间件承载了分发相关的指标，按处理器打标签（`messaging.destination.name`），遵循 messaging
语义约定，并额外加上一个 `ruststream.*` 命名空间：

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
| `ruststream.batch.size` | 直方图 | 交给批量处理器的、解码后的批量大小 |
| `ruststream.app.state` | 可观测仪表 | 生命周期状态，经由 `otel.observe_health(running.health())` 取自 [`RunningApp::health`](http.md#a-healthz-endpoint) |

批量处理器会绕过按消息计的消费侧层（这是文档里写明的[中间件](middleware.md)例外），因此
`ruststream.batch.size` 是由批量分发自己通过全局 meter 记录的：一旦 `init()` 装上全局 provider 它
就开始工作；而在只调用 `attach()` 的情况下它保持沉默，除非你自己把 provider 装成全局的。

正因为 `init()` 装上了全局 provider，业务指标不需要再铺一套 exporter 的管道：在启动时把这些指标项
一次性构建进一个存储对象，通过类型化状态共享出去（借助 `FromRef` 即可用 `State<..>` 注入），其中的
一切都会走同一条 OTLP 管线：

```rust
--8<-- "examples/otel_export.rs:business_metric"
```

针对这份清单，[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana) 提供了一个
现成的 Grafana 仪表盘：导入 `dashboards/ruststream.json`，把它指向任意一个接收 OTLP 指标、兼容
Prometheus 的后端，各个面板就会按处理器逐一亮起来；它的 README 同时也充当这份指标契约。

在 `main` 的末尾、应用优雅关闭之后调用 `otel.shutdown()`，把最后的 span 和数据点刷出去。若要把
span 桥接层组合进你自己的订阅者栈（例如配合 `logging` feature 的 fmt 层），就用
`.tracing_bridge(false)` 构建，并自行装上桥接层；`.messaging_system("kafka")` 会打上 semconv 的
system 属性，而该属性核心无法在与 Broker 无关的前提下推导出来。
