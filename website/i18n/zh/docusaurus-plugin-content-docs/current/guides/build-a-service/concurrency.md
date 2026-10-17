---
title: "并发：工作者与线程"
description: "打包是计算任务，所以它有两个专属线程；预约快递要等待网络，所以四个预约同时进行。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 并发：工作者与线程

打包是计算任务，所以它有两个专属线程；预约快递要等待网络，所以四个预约同时进行。

默认情况下，一个订阅一次处理一次投递，顺序与 Broker 交付的顺序相同。
[工作者](../../components/subscribers/dispatch/workers.md)让订阅在应用的运行时上同时处理多次投递，适合等待 I/O 的处理器。
[线程](../../components/subscribers/dispatch/threads.md)给订阅分配专属线程，适合做计算的处理器。否则这样的处理器会阻塞为其他所有订阅服务的运行时。
两者都是订阅的设置，处理器的主体不需要改动。
使用工作者或线程的订阅不再保持投递的顺序。`workers(n, by_key)` 保持每个键内的顺序。

## 在线程上打包

`pack` 把已付款的订单拆分成包裹，并以打包方案作为回复。
超出商店上限的已付款订单，在规划任何包裹之前就被丢弃。
`threads(2)` 随订阅启动两个线程，投递轮流分给它们。
解码、主体、回复和结算都在收到该投递的线程上运行。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#pack"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#pack"
```

</TabItem>
</Tabs>

## 在工作者上预约

`dispatch` 为已打包的订单预约快递，并把快递单号告诉顾客。
`workers(4)` 让最多四个预约同时进行，下一次投递等待空闲的工作者。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#dispatch"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#dispatch"
```

</TabItem>
</Tabs>

## 路由器

发货路由器读取商店内部的主题，所以它解码 CBOR。
回复使用默认编解码器，除非它的发布位置另行指定，所以 `pack` 的回复指定了 CBOR。
槽位使用路由器的编解码器，除非它另行指定，所以通知指定了 JSON。
在手写形式中，这两项设置是挂载链上的步骤：[`threads`](api:ruststream::runtime::SubscriberSettings::threads) 和 [`workers`](api:ruststream::runtime::SubscriberSettings::workers)。

计算步骤运行在自己的线程上，应用运行时只需等待 I/O，因此商店把它放在调用 `main` 的线程上运行：`#[ruststream::app(flavor = "current_thread")]`，手写形式为 [`run_main_on(AppRuntime::CurrentThread, app)`](api:ruststream::runtime::cli::run_main_on)。后面一章中带 HTTP 的入口用 `#[tokio::main(flavor = "current_thread")]` 做同样的事。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#routes"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#routes"
```

</TabItem>
</Tabs>

每次投递的计算量达到约十微秒时，线程才划算。处理器更轻时，线程的开销超过收益。
[轮询诊断](../../components/subscribers/dispatch/poll-diagnostics.md)能找出阻塞运行时的处理器。
