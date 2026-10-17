---
sidebar_position: 4
title: Builder
description: "One builder publishes every message: message(&value), the positions its type leaves open, then publish()."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Builder

One builder publishes every message: `message(&value)`, the positions its type leaves open, then `publish()`.

A slot, a [transaction](./publishers/transactional-publisher.md) and any bare [`Publisher`](./publishers/publisher.md) start a publish with `message(&value)` and return a [`PublishBuilder`](api:ruststream::runtime::PublishBuilder).
The positions the message type leaves open come next.
[`.to(name)`](api:ruststream::runtime::PublishBuilder::to) names the destination of a type that declares none, and a template opens `.to()` with its setters, as on the [Destinations](./outgoing.md) page.
A publish whose destination is still open does not compile.
[`.with_headers(..)`](api:ruststream::runtime::PublishBuilder::with_headers) takes the type's declared [header contract](../typed-headers/outgoing.md) by reference, or a [`HeaderMap`](api:ruststream::HeaderMap) built by hand.
The headers on the wire are the publisher's base headers with the call site's over them, key by key.
[`.with_codec(codec)`](api:ruststream::runtime::PublishBuilder::with_codec) names a codec for this one publish, over the slot's and the app's.
A [`Serialized`](api:ruststream::Serialized) value carries its own bytes and has no codec position.
A broker adds steps of its own for its [per-message settings](./publish-options.md).

`publish()` encodes the value and sends it, and returns [`PublishError`](api:ruststream::runtime::PublishError) on failure.
`Encode` and `Serialize` mean the value did not encode, and a retry of the same value fails the same way.
`Publish` carries the broker's own error.
On a slot the message passes the slot's [transforms](./transforms/publish-transform.md) before the send, and the app's [publish layers](../middleware/publish-layer.md) where they apply.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/publish_builder.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/publish_builder.rs#handler"
```

</TabItem>
</Tabs>
