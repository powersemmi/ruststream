---
sidebar_position: 1
title: Publisher
description: "Publisher is the capability every slot names; a publish policy at the mount site becomes one when startup pairs it with the connected broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publisher

[`Publisher`](api:ruststream::Publisher) is the capability every slot names; a [publish policy](api:ruststream::PublishPolicy) at the mount site becomes one when startup pairs it with the connected broker.

A handler bounds its [slot](../out-slots.md) on the capability, `Out<impl Publisher>`, and never on a broker's publisher type.
The same handler then mounts on a production broker and on its in-process test transport unchanged.
A policy holds no connection, so it is built anywhere: a broker's prelude names its plain one `Publish`.
Startup pairs each policy with the connected broker, so a handler never holds a publisher that is not connected.
A [reply](../replies.md) with no policy named leaves through the broker's default one, [`DefaultPublish`](api:ruststream::DefaultPublish).
A startup hook such as [`after_startup`](api:ruststream::runtime::BrokerScope::after_startup) takes a policy and gets its live publisher, for a publish outside any handler.
A bare publisher starts a publish with the same [builder](../publish-builder.md), under the default codec.

A publisher declares three types.
`Payload` says how its client takes the bytes: `Lend` when it only reads them, so the runtime lends one reused buffer, and `Take` when it keeps them.
`Options` are its [per-message settings](../publish-options.md), and `Error` is what a failed publish returns.
Its `base_headers` go on every message it sends, under the headers of the call site.
The narrower capabilities extend it: [`TransactionalPublisher`](./transactional-publisher.md), [`OwnedTransactions`](./owned-transactions.md) and [`RequestReply`](./request-reply.md).

## Example

The handler names the capability; the mount site and the startup hook name the policy.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/publisher.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/publisher.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/publisher.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/publisher.rs#app"
```

</TabItem>
</Tabs>
