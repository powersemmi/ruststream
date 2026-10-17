---
title: AsyncAPI 文档
description: "asyncapi gen 打印商店的 AsyncAPI 文档：每个主题、谁读谁写、schema、重试上限，以及应用对自己的描述。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AsyncAPI 文档

`asyncapi gen` 打印商店的 AsyncAPI 文档：每个主题、谁读谁写、schema、重试上限，以及应用对自己的描述。

文档由应用本身构建，所以它不可能描述代码没有使用的主题。
每个订阅是一个 `receive` 操作，每个回复和槽位是一个 `send` 操作。
载荷的 schema 来自它的 [`JsonSchema`](../../components/asyncapi/index.md) derive，文档注释成为描述。
`max_attempts` 和 `dead_letter` 出现在操作上，读者能看到失败的消息最终去了哪里。
[AsyncAPI](../../components/asyncapi/index.md) 讲解服务器、安全方案，以及通过 HTTP 提供文档。
文档需要 crate 的 `asyncapi` feature。

## 入口

`main.rs` 在内存 Broker 上构建商店。
[`#[ruststream::app]`](api:ruststream::app) 给二进制程序提供命令：`run` 启动商店，`asyncapi gen` 打印文档。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="main.rs" reference="examples/guide/main.rs#main"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="main.rs" reference="examples/manual/guide/main.rs#main"
```

</TabItem>
</Tabs>

## 商店对自己的描述

[`AppInfo`](api:ruststream::runtime::AppInfo) 是文档的 `info` 块：标题、版本、描述、联系方式、许可证和标签。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="service.rs" reference="examples/guide/service.rs#info"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="service.rs" reference="examples/manual/guide/service.rs#info"
```

</TabItem>
</Tabs>

## 文档

```bash
cargo run --example guide --features testing,macros,memory,json,cbor,asyncapi -- asyncapi gen --yaml
```

`charge` 的操作读取主题，在 `payments.settled` 上回复，并声明上限：

```yaml
receive_payments_requested:
  action: receive
  channel:
    $ref: '#/channels/payments.requested'
  messages:
  - $ref: '#/channels/payments.requested/messages/PaymentRequest'
  description: Charges an order through the payment provider.
  reply:
    channel:
      $ref: '#/channels/payments.settled'
    messages:
    - $ref: '#/channels/payments.settled/messages/Paid'
  x-ruststream-retry:
    maxAttempts: 5
    deadLetter: payments.dead
```

商店的两种形式生成相同的文档。
两者各有一个测试，把文档与 [`examples/guide/asyncapi.json`](https://github.com/powersemmi/ruststream/blob/main/examples/guide/asyncapi.json) 比较，所以主题的改动会在评审中显现出来。
