---
title: 用 TestApp 测试服务
description: "TestApp 在进程内运行商店自己的应用：测试发布一个订单，然后检查每个订阅者做了什么、每个主题收到了什么。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 用 TestApp 测试服务

[`TestApp`](api:ruststream::testing::TestApp) 在进程内运行商店自己的应用：测试发布一个订单，然后检查每个订阅者做了什么、每个主题收到了什么。

测试用 [`TestApp::start`](../../components/application/testing/starting-and-stopping/start.md) 启动 `main` 运行的那个应用，不做任何改动。
Broker 在进程内连接，每个订阅都运行在测试的运行时上。
测试中的一次发布会等到它引发的一切都结算之后才返回，所以下一行就能检查结果。
断言读取两份记录：每个[订阅者](../../components/application/testing/subscriber-assertions/index.md)收到了什么、如何结算，以及每个主题上[发布](../../components/application/testing/published-assertions/index.md)了什么。
在暂停的时钟上，[`advance`](../../components/application/testing/driving/advance.md) 让时间前进，所以一秒或一小时的延迟都不花费任何时间。
测试需要 crate 的 `testing` feature，通常作为开发依赖。

## 一个订单从头到尾

测试带着请求消息头下一个订单，并一路跟踪到顾客。
支付请求以 CBOR 传递，所以断言用 [`with_codec`](api:ruststream::testing::PublishedAssertions::with_codec) 解码它。
`accept` 和 `dispatch` 各发送一条通知，`send` 对两条都 ack。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="main.rs" reference="examples/guide/main.rs#journey"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="main.rs" reference="examples/manual/guide/main.rs#journey"
```

</TabItem>
</Tabs>

## 止步的订单

没有订单行的订单停在 `accept`：不收款，也不通知顾客。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="main.rs" reference="examples/guide/main.rs#empty"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="main.rs" reference="examples/manual/guide/main.rs#empty"
```

</TabItem>
</Tabs>

## 不必等待的延迟

每秒只接受一笔收款的服务商会以 `Busy` 拒绝第二个请求。
[`with_state`](api:ruststream::testing::TestApp::with_state) 用这个服务商启动应用，代替 `on_startup` 打开的那个。
`service.rs` 中的 `test_broker` 返回测试构建商店所用的 Broker，所以测试只在一处写明 Broker。
时钟从暂停状态开始，`advance(BACKOFF)` 让被拒绝的请求回来。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="payments.rs" reference="examples/guide/payments.rs#busy"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#busy"
```

</TabItem>
</Tabs>

## 死信主题

从不应答的服务商让每次投递都失败。
第五次投递按到达时的原样进入 `payments.dead`。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="payments.rs" reference="examples/guide/payments.rs#dead_letter"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#dead_letter"
```

</TabItem>
</Tabs>

运行两种形式的测试：

```bash
cargo test --example guide --features testing,macros,memory,json,cbor,asyncapi
cargo test --example manual_guide --no-default-features --features testing,memory,json,cbor,asyncapi
```

两个目录中的测试相同，它们在商店的两种形式上都能通过。
