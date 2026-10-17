---
title: 共享状态与上下文
description: "支付客户端和邮件发送器在启动时打开一次，以 State 的形式到达每个订阅者；上下文告诉订阅者它拿到的是哪次投递。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 共享状态与上下文

支付客户端和邮件发送器在启动时打开一次，以 `State` 的形式到达每个订阅者；上下文告诉订阅者它拿到的是哪次投递。

服务在订阅者之间共享客户端、连接池和缓存。
应用在 [`on_startup`](api:ruststream::runtime::RustStream::on_startup) 中打开它们，时机在第一个订阅打开之前。这里出现错误会中止启动。
钩子的返回值是应用的[状态](../../components/application/state/any-struct.md)，整个服务只有这一个值。
订阅者以 [`State<T>`](api:ruststream::runtime::State) 参数的形式取用其中的一个字段。
[上下文](../../components/subscribers/context/delivery-context.md)是按投递划分的那一半：订阅的名字、消息头、Broker 为这次投递提供的字段。
订阅者以 `&mut Context` 参数的形式取用它，紧跟在消息之后。

## 状态

`Shop` 持有支付客户端和邮件发送器，两者各自放在 `Arc` 后面，所以克隆的开销很小。
[`FromRef`](api:ruststream::runtime::FromRef) 从状态中投影出每个字段：`State<Arc<Gateway>>` 就是通过它解析的。
`open` 是第 5 章中应用传给 `on_startup` 的钩子。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="state.rs" reference="examples/guide/state.rs#state"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="state.rs" reference="examples/manual/guide/state.rs#state"
```

</TabItem>
</Tabs>

手写形式写出 derive 生成的两个 impl，每个字段一个。

## 同一个订阅者中的状态与上下文

`send` 通过状态中的邮件发送器用电子邮件发出通知。
不存在的邮箱再试也还是不存在，所以被拒绝的通知直接丢弃，警告中指明取自上下文的订阅。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="notifications.rs" reference="examples/guide/notifications.rs#send"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="notifications.rs" reference="examples/manual/guide/notifications.rs#send"
```

</TabItem>
</Tabs>

在手写形式中，状态类型是 `Handle` 的最后一个参数，挂载时会把它与应用的状态类型核对。
主体用 [`from_context`](api:ruststream::runtime::FromContext) 读取字段，这正是宏对 `State` 参数所做的事。
[自定义提取器](../../components/subscribers/context/from-context.md)是你自己的类型，以同样的方式读取。
