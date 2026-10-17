---
sidebar_position: 1
title: Replies
description: "A reply is the reply of the receive operation it answers: the document names its channel and its message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replies

A reply is the `reply` of the `receive` operation it answers: the document names its channel and its message.

A request-reply registration's `receive` operation carries a [`reply`](api:ruststream::asyncapi::OperationReply) object.
It points at the reply's channel and at the reply type's message component.
The reply gets no `send` operation of its own.
Its channel is the destination the reply resolves to: the reply type's `#[outgoing(name = "..")]`, or the name the mount site gives (see [Replies](../../subscribers/publishing/replies.md)).
A reply that a [naming transform](../../subscribers/publishing/transforms/naming-transform.md) addresses per delivery has no fixed address, and its channel's address is `null`.
The reply's [`address`](api:ruststream::asyncapi::ReplyAddress) then says where a client reads it, as a runtime expression such as `$message.header#/reply-to`.
The broker states that expression through [`PublishPolicy::reply_address_location`](api:ruststream::PublishPolicy::reply_address_location), and a broker that cannot state it leaves the address out.
The example runs with `cargo test --example asyncapi_reports --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_reports.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_reports.rs#reply"
```

</TabItem>
</Tabs>
