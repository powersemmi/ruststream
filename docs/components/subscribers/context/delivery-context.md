---
sidebar_position: 1
title: Context
description: "Context carries one delivery's channel, headers, state and broker fields to the middleware and the handler."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Context

[`Context`](api:ruststream::runtime::Context) carries one delivery's channel, headers, state and broker fields to the middleware and the handler.

The runtime builds one `Context` per delivery and lends it to the middleware and then to the handler.
A `#[subscriber]` function takes it as `&mut Context<'_>` after the message; a manual handler gets it as the last argument of `handle`.
`name()` is the channel the message arrived on.
`headers()` reads the delivery's headers; `headers_mut()` gives a working copy that middleware can enrich for the handler.
The working copy stays inside the delivery: a reply or a publish from the handler starts with fresh headers.
`state()` is the application [state](../../application/state/any-struct.md) that `on_startup` produced.
The context's first type parameter is the broker's per-delivery context: `()` for most subscriptions, [`MemoryContext`](api:ruststream::memory::MemoryContext) for a retaining memory broker.
The broker exports keys for its fields, and `ctx.context(key)` reads one; [`Ctx<K>`](./ctx.md) does the same read as a handler parameter.
A key implements [`ContextField`](api:ruststream::ContextField), which names the context it reads, and [`Field`](api:ruststream::Field), which reads the value out of it with no lookup at run time.
The memory broker's [seek handle](../replay/seek.md) is one such field.
The context also schedules work for after the settlement: the [post-settle hooks](../messages/hooks.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/context.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/context.rs#handler"
```

</TabItem>
</Tabs>
