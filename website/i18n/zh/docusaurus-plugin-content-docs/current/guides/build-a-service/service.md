---
title: 服务和它的第一个订阅者
description: "商店从一个订阅者起步。它接收每一个已下的订单，由一个小应用挂载在内存 Broker 上。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 服务和它的第一个订阅者

商店从一个订阅者起步。它接收每一个已下的订单，由一个小应用挂载在内存 Broker 上。

RustStream 服务是一个[应用](../../components/application/index.md)：一个构建器，它列出自己的 Broker，并在上面挂载订阅者。
[订阅者](../../components/subscribers/index.md)是一个 `async fn`，第一个参数是消息，[`#[subscriber]`](api:ruststream::subscriber) 指明它读取的主题。
运行时在主体运行之前解码载荷，默认使用 JSON。
主体的返回值决定如何结算这次投递：[`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack) 告诉 Broker 这条消息已处理完毕。
[内存 Broker](../../components/memory/index.md) 在进程内部传递消息，所以商店不需要安装任何东西。
第 11 章把它换成联网的 Broker，订阅者保持不变。

## 订单

订单是一个 `serde` 类型。
[`Outgoing`](api:ruststream::Outgoing) derive 声明订单发布到哪里：`orders.placed`。
商店自己还不发布订单。现在由测试发布，第 10 章起由 HTTP 店面发布。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/guide/first.rs#order"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/guide/first.rs#order"
```

</TabItem>
</Tabs>

手写形式写出 derive 生成的两个 impl：目的地，以及订单携带的消息头（目前没有）。

## 第一个订阅者

`accept` 读取 `orders.placed`，并通过 `tracing` 记录每个订单。
手写形式是一个具名类型，带有 [`impl Handle<Order>`](api:ruststream::runtime::Handle)：`Ok(())` 表示 ack，`Err(outcome)` 按该结果结算。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/guide/first.rs#accept"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/guide/first.rs#accept"
```

</TabItem>
</Tabs>

## 应用

[`RustStream`](api:ruststream::runtime::RustStream) 就是应用。[`with_broker`](api:ruststream::runtime::RustStream::with_broker) 注册一个 Broker，并用 `include` 在上面挂载订阅者。
闭包返回 `include` 开启的链，每多一个订阅者，就在链上再写一次 `.include(..)`。
构建器只记录交给它的内容：Broker 在服务启动时才连接。
[`#[ruststream::app]`](../../components/application/app-macro.md) 属性把构建器变成 `main`，并带有 `run` 和 `asyncapi gen` 命令。
手写形式亲手写出这个 `main`：在构建器之上调用 [`run_main`](api:ruststream::runtime::cli::run_main)。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust reference="examples/guide/first.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust reference="examples/manual/guide/first.rs#app"
```

</TabItem>
</Tabs>

## 运行

```bash
cargo run --example guide_first --features testing,macros,memory,json,logging -- run
```

`logging` feature 把服务的日志打印到终端。
服务连接 Broker，打开订阅，然后一直运行到 <kbd>Ctrl</kbd>+<kbd>C</kbd>：

```text
INFO ruststream::lifecycle starting service service=shop version=0.1.0 brokers=1 subscribers=1
INFO ruststream::lifecycle broker connected broker="ruststream::memory::MemoryBroker"
INFO ruststream::dispatch subscriber started subscriber=orders.placed input="guide_first::Order"
INFO ruststream::lifecycle service running subscribers=1
```

本指南的其余部分把这个文件逐步扩展成 [`examples/guide`](https://github.com/powersemmi/ruststream/tree/main/examples/guide) 中的商店。
