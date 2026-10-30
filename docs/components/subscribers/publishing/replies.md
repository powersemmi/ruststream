---
sidebar_position: 1
title: Replies
description: "A handler answers a message by returning a value; the reply type or the mount site says where the answer goes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replies

A handler answers a message by returning a value; the reply type or the mount site says where the answer goes.

Every reply type derives [`Outgoing`](api:ruststream::Outgoing).
A type with `#[outgoing(name = "..")]` fixes its destination, and the handler carries the bare clause `#[subscriber("src", reply)]`.
A type that names nothing takes the destination from the clause, `reply("dest")`.
A destination declared on the type wins over a name at the mount site, and startup logs a warning for such a registration.
The forms of a destination are on the [Destinations](./outgoing.md) page.
`Result<Reply, HandlerOutcome>` keeps the settlement in the handler's hands: `Ok` publishes the reply and acks, `Err` publishes nothing and settles by the [outcome](../messages/handler-outcome.md).
A reply whose publish fails nacks the delivery with requeue, so a replying handler has to be safe to run twice.
A [batch handler](../batches.md) returns one `Vec` of replies per batch.

With nothing else said, the reply leaves through the broker's default policy under the default codec.
[`.out_reply(policy)`](api:ruststream::runtime::RouterWith::out_reply) names the [publish policy](./publishers/publisher.md), and the steps after it complete the reply's wiring.
[`.codec(..)`](api:ruststream::runtime::RouterWith::codec) names the codec of the reply, while the request still decodes with the scope's [codec](../codecs/index.md).
[`.transform(..)`](./transforms/publish-transform.md) adds a publish transform, and [`.transactional()`](./publishers/transactional-publisher.md) puts every reply of a batch in one broker transaction.
A broker that only reads the payload is lent the dispatch loop's own encode buffer, so a reply allocates nothing per delivery.

## Example

The reply type fixes its destination.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/replies.rs#declared"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/replies.rs#declared"
```

</TabItem>
</Tabs>

The clause names the destination of a type that declares none.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/replies.rs#named"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/replies.rs#named"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/replies.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/replies.rs#app"
```

</TabItem>
</Tabs>
