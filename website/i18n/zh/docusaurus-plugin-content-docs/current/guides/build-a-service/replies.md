---
title: 回复与发布到其他主题
description: "accept 用一个支付请求回复每个订单，并通过 Out 槽位通知顾客。路由器决定两者如何发出。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 回复与发布到其他主题

`accept` 用一个支付请求回复每个订单，并通过 `Out` 槽位通知顾客。路由器决定两者如何发出。

订阅者有两种发布方式。
[回复](../../components/subscribers/publishing/replies.md)是主体返回的值：主体成功之后，运行时对它编码并发布。
[`Out` 槽位](../../components/subscribers/publishing/out-slots.md)是一个发布者。主体把它作为参数接收，并自己调用，需要多少次就调用多少次。
消息发往哪里是消息类型的属性：[`#[outgoing(name = ..)]`](../../components/subscribers/publishing/outgoing.md) 固定主题，主体和挂载点都不必重复它。
槽位列出可以经由它发出的类型，发布其他任何类型都无法编译。
挂载点把每个发布位置绑定到一个发布策略，这里是 Broker 的普通 `Publish`。

## 消息

支付模块接收 `PaymentRequest`，`Paid` 是它在下一章给出的回复。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="payments.rs" reference="examples/guide/payments.rs#request"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#request"
```

</TabItem>
</Tabs>

`Notice` 是商店发给顾客的通知，[`OutSlot`](api:ruststream::OutSlot) 标记 `Notices` 是它发出所经由的槽位。
手写形式写出 `#[derive(OutSlot)]` 生成的内容：槽位的名字、它发布的类型，以及这些类型在 AsyncAPI 文档中的条目。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="notifications.rs" reference="examples/guide/notifications.rs#notice"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="notifications.rs" reference="examples/manual/guide/notifications.rs#notice"
```

</TabItem>
</Tabs>

## 订阅者

没有订单行的订单，`accept` 直接丢弃，因为没有可收款的内容；超出商店上限的订单也会丢弃，因为它没有总额。
它通过槽位告诉顾客订单已收到，如果这次发布失败，就请求重试。
然后它返回支付请求，运行时把它作为回复发布。
`Ok` 发布回复并 ack，`Err` 不发布任何内容，按结果结算。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="orders.rs" reference="examples/guide/orders.rs#accept"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#accept"
```

</TabItem>
</Tabs>

在手写形式中，槽位是 [`Outs`](api:ruststream::runtime::Outs) 中的一个条目，`outs.get(Notices)` 交出它的发布者。

## 路由器

[`Router`](api:ruststream::runtime::Router) 汇集一个模块的订阅者，以及它们各个发布位置的设置。
[`out_reply`](api:ruststream::runtime::Mounting::out_reply) 把回复绑定到一个策略，`.codec(INTERNAL)` 用 CBOR 对它编码。
[`out`](api:ruststream::runtime::Mounting::out) 绑定槽位。槽位沿用路由器的编解码器，这里是 JSON。
路由器在 [`RouterDef<MemoryBroker, Shop>`](api:ruststream::runtime::RouterDef) 中指明 Broker 和应用的状态类型，`Shop` 在第 7 章介绍。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="orders.rs" reference="examples/guide/orders.rs#routes"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#routes"
```

</TabItem>
</Tabs>

手写形式用 [`subscriber(..)`](api:ruststream::runtime::subscriber) 把每个订阅者绑定到它的主题，`.describe(..)` 给它描述，相当于宏从文档注释得到的描述。
[`include_router`](api:ruststream::runtime::BrokerScope::include_router) 把路由器挂载到 Broker 上，第 5 章展示完成这一步的应用。
