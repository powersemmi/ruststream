---
title: "结算：retry、retry_after、死信"
description: "支付服务商每次拒绝，charge 都给出所需的结果：立即重试、稍后重试、丢弃，或者转入死信主题。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 结算：retry、retry_after、死信

支付服务商每次拒绝，`charge` 都给出所需的结果：立即重试、稍后重试、丢弃，或者转入死信主题。

每次投递都以一个[结果](../../components/subscribers/messages/handler-outcome.md)结束。
[`ack`](api:ruststream::runtime::HandlerOutcome::ack) 完成它，[`drop`](api:ruststream::runtime::HandlerOutcome::drop) 丢弃它，[`retry`](api:ruststream::runtime::HandlerOutcome::retry) 请求 Broker 再投递一次。
[`retry_after`](../../components/subscribers/messages/retry-after.md) 在一段延迟之后再投递一次，不会阻塞排在它后面的投递。
一直不成功的重试会无休止地回来，因此挂载点限定尝试次数。
超过[上限](../../components/subscribers/retry-cap.md)后，投递进入死信主题，运维人员或其他服务可以在那里查看它。

## 订阅者

支付服务商有三种拒绝方式，每种需要不同的结果。
`Busy` 表示它的速率限额暂时用完：立即重试仍会被拒绝，所以 `retry_after` 等待一秒。
`Unavailable` 表示它没有应答：`retry` 立即再试一次。
`Declined` 表示卡上余额不足，再试一次也是同样的答复，所以这笔支付被丢弃。
服务商的客户端来自应用的状态，状态在第 7 章打开。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="payments.rs" reference="examples/guide/payments.rs#charge"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#charge"
```

</TabItem>
</Tabs>

## 上限与死信主题

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) 统计一条消息的每次投递，第一次也算在内。
第五次投递不再重试：[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) 把它发布到 `payments.dead`，载荷和消息头保持到达时的样子。
路由器以 CBOR 解码请求，并以同样的方式编码回复。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="payments.rs" reference="examples/guide/payments.rs#routes"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#routes"
```

</TabItem>
</Tabs>

内存 Broker 自己完成重新投递，延迟正是 `retry_after` 要求的时长。
如果 Broker 无法延迟一次投递，就把一份副本发布回该订阅。[重试副本的去向](../../components/subscribers/retry-destination.md)讲解了这一点。
第 8 章测试全部三条路径，包括延迟，而且不必真的等待。
