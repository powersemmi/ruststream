---
sidebar_position: 4
title: RequestReply
description: "RequestReply sends a request from a handler and awaits the correlated answer, on a broker that correlates replies natively."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RequestReply

[`RequestReply`](api:ruststream::RequestReply) sends a request from a handler and awaits the correlated answer, on a broker that correlates replies natively.

NATS-style brokers correlate replies, and the memory broker does too; Kafka and classic queues do not offer this capability.
A handler bounds its slot `Out<impl RequestReply, Marker>`, and the mount site binds the broker's request policy, which its prelude names `Request`.
[`request(message, timeout)`](api:ruststream::RequestReply::request) publishes the message and resolves on the first answer, or fails once the time runs out.
The request is an [`OutgoingMessage`](api:ruststream::OutgoingMessage) of bytes the handler encoded, and the answer is the broker's incoming message, which the handler decodes.
The request goes to the broker directly, past the slot's transforms and the app's publish layers.
The broker names an inbox for the answer: the memory broker puts it in the `reply-to` header.
The responder is an ordinary [reply](../replies.md) handler whose reply goes to that inbox, through a [naming transform](../transforms/naming-transform.md).
The slot also publishes with the [builder](../publish-builder.md), what its `#[publishes(..)]` list admits.

## Example

The handler asks the quote service for a price, then publishes the priced order through the same slot.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/requester.rs#requester"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/requester.rs#requester"
```

</TabItem>
</Tabs>

The responder answers on the inbox the request named.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/requester.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/requester.rs#app"
```

</TabItem>
</Tabs>
