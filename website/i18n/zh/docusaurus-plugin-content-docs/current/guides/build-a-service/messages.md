---
title: "消息：载荷、消息头、编解码器"
description: "订单扩展为带订单行的载荷，加上每次发布都必须携带的消息头契约，以及按主题选定的编解码器。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 消息：载荷、消息头、编解码器

订单扩展为带订单行的载荷，加上每次发布都必须携带的消息头契约，以及按主题选定的编解码器。

载荷可以是任意 `serde` 类型。编解码器把它转换成字节，也把字节转换回来。
消息头与载荷一起传递。[消息头契约](../../components/subscribers/typed-headers/index.md)是一个 `serde` 结构体，每个字段对应一个消息头。
如果消息类型指定了契约，不带契约就无法发布：这样的发布无法编译。
订阅者通过 [`Headers<T>`](api:ruststream::runtime::Headers) 参数读取契约，解析发生在主体运行之前。
消息头解析失败的投递不会到达主体，而是被丢弃，与无法解码的载荷相同。
[编解码器](../../components/subscribers/codecs/index.md)属于挂载点，而不属于类型。因此同一个类型可以在一个主题上以 JSON 传递，在另一个主题上以 CBOR 传递。

## 订单

从这里开始，商店的代码放在各个模块里。
`orders.rs` 存放订单：订单行、总额，以及随订单一起传递的契约。
上限规定一个订单最多能买多少，总额用带溢出检查的算术累加，所以超出上限的订单没有总额。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="orders.rs" reference="examples/guide/orders.rs#order"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#order"
```

</TabItem>
</Tabs>

`headers = RequestMeta` 让契约成为类型的一部分。
手写形式用 `type Contract = WithHeaders<RequestMeta>` 表达同样的意思，并通过一个 [`MessageInfo`](api:ruststream::MessageInfo) impl 为消息在 AsyncAPI 文档中命名。

## 消息头契约

契约的每个字段是一个消息头。
`request_id` 指明下单的那次店面请求，这样每一步都能记录它。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="orders.rs" reference="examples/guide/orders.rs#meta"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#meta"
```

</TabItem>
</Tabs>

## 读取消息头

`audit` 是 `orders.placed` 上的第二个订阅者：它把每个订单连同下单请求写进审计日志。
每个订阅都会收到其主题的每一条消息，因此 `audit` 和下一章的 `accept` 都能看到每个订单。
手写形式以 [`Message<RequestMeta, Order>`](api:ruststream::runtime::Message) 这一对作为输入：两部分都在主体运行之前解码，AsyncAPI 文档会展示消息头的 schema。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="orders.rs" reference="examples/guide/orders.rs#audit"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#audit"
```

</TabItem>
</Tabs>

## 商店内部主题的编解码器

JSON 是默认编解码器。`orders.placed` 和 `notices` 由其他团队读取，商店在这两个主题上保留 JSON。
商店各步之间的主题以 [CBOR](../../components/subscribers/codecs/cbor.md) 传递：体积更小，而且没有别人读取。
一个常量指定这个编解码器，这样后面几章的路由器不会在这一点上出现分歧。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="payments.rs" reference="examples/guide/payments.rs#codec"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#codec"
```

</TabItem>
</Tabs>

[路由器](../../components/subscribers/router.md)用 [`with_codec`](api:ruststream::runtime::Router::with_codec) 指定的编解码器解码，每个发布位置用 `.codec(..)` 指定自己的编解码器。
下一章会同时用到这两者。
CBOR 需要 crate 的 `cbor` feature。
