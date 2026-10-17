---
title: 在 HTTP 服务器旁运行，outbox
description: "店面通过 HTTP 接收订单，把订单连同事件一起存储；中继把事件交给 Broker，商店运行在服务器旁边。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在 HTTP 服务器旁运行，outbox

店面通过 HTTP 接收订单，把订单连同事件一起存储；中继把事件交给 Broker，商店运行在服务器旁边。

服务同时提供 HTTP 时，[进程归 HTTP 服务器所有](../../components/application/running/http.md)，商店运行在它旁边。
[`start`](../../components/application/running/start.md) 在后台启动商店，商店运行起来后才返回。因此，如果 Broker 拒绝连接，进程会在服务器接收请求之前停止。
HTTP 请求不应自己向 Broker 发布：Broker 故障会让请求失败，而存储了订单却没有事件，订单就永远到不了商店。
事务性 outbox 同时解决这两个问题。
请求在一个事务中写入订单和它的事件，中继稍后发布已存储的事件。
Broker 故障只会推迟事件，不会丢失任何事件。如果在发布和删除之间发生崩溃，事件会再发布一次，所以每个订单商店至少会看到一次。

## 存储

使用真实数据库时，订单和 outbox 是在一个事务中写入的两张表。
这里由一个保护这两者的互斥锁承担这个角色。
只有真实数据库能让事件在崩溃后保留下来：这里的行存放在内存中，一旦崩溃，尚未发布的行就会丢失。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="http.rs" reference="examples/guide/http.rs#store"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="http.rs" reference="examples/manual/guide/http.rs#store"
```

</TabItem>
</Tabs>

HTTP 部分和 outbox 都是普通函数，所以两个标签页中的代码相同。

## 端点

`POST /orders` 以 JSON 接收订单，从 `x-request-id` 消息头读取请求 id。
它只写入存储，所以不等待 Broker 就以 `202 Accepted` 应答。
超出商店上限的订单以 `400 Bad Request` 应答，不会被存储。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="http.rs" reference="examples/guide/http.rs#endpoint"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="http.rs" reference="examples/manual/guide/http.rs#endpoint"
```

</TabItem>
</Tabs>

## 中继

中继先发布最早的事件，并带上它的消息头契约，发布成功之后才删除它。
发布被拒绝时，这一行留到下一轮处理。
中继一直运行到进程让它停止，然后再执行最后一轮。
发布者可以是任意 [`Publisher`](../../components/subscribers/publishing/publishers/publisher.md)，所以更换 Broker 时中继不变。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="http.rs" reference="examples/guide/http.rs#relay"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="http.rs" reference="examples/manual/guide/http.rs#relay"
```

</TabItem>
</Tabs>

## 进程

`bindable` 让进程在商店启动之前拿到一个发布者令牌，Broker 连接之后，[`publisher`](api:ruststream::runtime::RunningApp::publisher) 把令牌变成可用的发布者。
服务器在 <kbd>Ctrl</kbd>+<kbd>C</kbd> 时停止，或者在 [`stopping`](api:ruststream::runtime::RunningApp::stopping) 报告商店自行停止时停止。
HTTP 处理完所有请求之后，进程停止中继，中继的最后一轮在商店仍在运行时交出剩余的行。
然后商店用 [`shutdown`](../../components/application/running/shutdown.md) 处理完剩余的工作。
手写的 `main` 自己用 `ruststream::logging::init()` 安装日志记录器。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="http.rs" reference="examples/guide/http.rs#main"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="http.rs" reference="examples/manual/guide/http.rs#main"
```

</TabItem>
</Tabs>

运行它，然后下一个订单：

```bash
cargo run --example guide_http --features testing,macros,memory,json,cbor,asyncapi,logging
curl -X POST http://127.0.0.1:8080/orders -H 'content-type: application/json' \
  -H 'x-request-id: req-1' \
  -d '{"id":1,"customer":"ada@example.com","lines":[{"sku":"mug","quantity":12,"price_cents":1200}]}'
```

第 5 章的日志会跟随这个订单走完整个商店。
[健康检查端点](../../components/application/running/health.md)在同一个服务器上向编排系统报告商店的状态。
