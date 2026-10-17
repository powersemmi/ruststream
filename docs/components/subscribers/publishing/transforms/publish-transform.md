---
sidebar_position: 1
title: PublishTransform
description: "A publish transform is a static step on one publish position: it reads the delivery or the slot, and changes the outgoing message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PublishTransform

A [publish transform](api:ruststream::runtime::PublishTransform) is a static step on one publish position: it reads the delivery or the slot, and changes the outgoing message.

`.transform(..)` adds one after the position it belongs to: after `.out_reply(policy)` for a [reply](../replies.md), after `.out(marker, policy)` for a [slot](../out-slots.md).
Its `apply` gets the outgoing [message](api:ruststream::runtime::Outgoing), the broker's [per-message settings](../publish-options.md) and a view of the position, and returns nothing: it cannot refuse a publish.
An impl declares three things.
The kind is what it reads.
[`ForReply<C>`](api:ruststream::runtime::ForReply) reads the delivery being answered through a [`PublishContext`](api:ruststream::runtime::PublishContext): its channel, headers and the broker's context fields.
[`ForSlot`](api:ruststream::runtime::ForSlot) reads a [`SlotContext`](api:ruststream::runtime::SlotContext), the slot's name and nothing else, because the body that publishes has already read its delivery.
The `Options` parameter is the settings it writes: `()`, or the broker's own type, which mounts it only over that broker's publisher.
`type Destination` is what it may do to the destination: [`Reads`](api:ruststream::runtime::Reads) leaves it alone, and [`Names`](./naming-transform.md) sets it.
A transform that reads and sets nothing is generic over both, `impl<K: ContextKind, Options>`, and mounts anywhere.

Transforms run in the order the chain writes them, then the app's [publish layers](../../middleware/publish-layer.md), then the send.
The chain is one type, built when the code compiles, so a transform costs only its own code.
A reply of a [batch handler](../../batches.md) takes [batch transforms](./batch-transform.md) instead.
A transform has no macro form: the same impl serves both forms, and only the mounting differs.

## Example

A reply transform carries the request's correlation id onto the reply.

{/* single-form: a PublishTransform impl has no macro form; the same code serves the macro and the manual service */}
```rust reference="examples/publish_transform.rs#reply"
```

A slot transform stamps a constant on what leaves the slot.

{/* single-form: a PublishTransform impl has no macro form; the same code serves the macro and the manual service */}
```rust reference="examples/publish_transform.rs#slot"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/publish_transform.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/publish_transform.rs#app"
```

</TabItem>
</Tabs>
