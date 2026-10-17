---
title: 中间件与可观测性
description: "应用用两层包裹每个订阅者：一层记录每次投递及其结果，另一层是商店自己的，对运行过慢的处理器发出警告。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 中间件与可观测性

应用用两层包裹每个订阅者：一层记录每次投递及其结果，另一层是商店自己的，对运行过慢的处理器发出警告。

[`Layer`](../../components/subscribers/middleware/layer.md) 把一个处理器包裹在另一个处理器里。
它在主体之前、之后或者代替主体运行代码，并把结果传递下去。
应用上的层包裹应用挂载的每个订阅者，路由器上的层只包裹该路由器的订阅者。
RustStream 通过 `tracing` 发出自己的事件：服务启动、每个订阅打开、服务停止。
[`TracingLayer`](api:ruststream::runtime::layers::TracingLayer) 为每次投递添加事件：先在 `DEBUG` 级别记录 `delivery received`，然后记录它的结果。ack 在 `INFO` 级别，重试或丢弃在 `WARN` 级别。
`logging` feature 会打印这些事件，第 1 章已经展示过。任何其他 `tracing` 订阅者都可以代替它。
[可观测性](../../components/observability/index.md)讲解 Prometheus 指标和 OpenTelemetry 导出，它们同样以层的形式挂载。

## 商店自己的层

`SlowHandlers` 为每个处理器计时，运行时间超过阈值时发出警告。
警告中指明订阅，订阅名取自投递的[上下文](../../components/subscribers/context/index.md)。
层没有宏形式：两个标签页中是同一个普通类型。
应用的处理器类型是隐藏的，[`BlanketLayer`](api:ruststream::runtime::BlanketLayer) 让应用能把这个层应用到它们上。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="observability.rs" reference="examples/guide/observability.rs#layer"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="observability.rs" reference="examples/manual/guide/observability.rs#layer"
```

</TabItem>
</Tabs>

## 应用

`service.rs` 组装商店。
[`layer`](api:ruststream::runtime::RustStream::layer) 给应用添加一个层，最先添加的层位于最外层。
每个模块的路由器用 [`include_router`](api:ruststream::runtime::BrokerScope::include_router) 挂载，应用的层包裹其中的订阅者。
`on_startup` 打开第 7 章的共享状态，`info()` 是第 9 章的描述。
函数以 Broker 作为参数，因此命令行入口和第 10 章的 HTTP 宿主构建的是同一个商店。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="service.rs" reference="examples/guide/service.rs#shop"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="service.rs" reference="examples/manual/guide/service.rs#shop"
```

</TabItem>
</Tabs>

## 日志的内容

用请求 id `req-1` 下单后，每一步留下一行日志：

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

`RUST_LOG` 调节打印哪些内容，例如 `RUST_LOG=ruststream=debug,info`。
