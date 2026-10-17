---
sidebar_position: 1
title: Context and Ctx<K>
description: "Context carries one delivery's channel, headers and broker fields to the handler; Ctx<K> passes one broker field as a parameter."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `Context` and `Ctx<K>`

`Context` carries one delivery's channel, headers and broker fields to the handler; `Ctx<K>` passes one broker field as a parameter.

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

## One broker field as a parameter

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/context.rs#ctx"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/context.rs#ctx"
```

</TabItem>
</Tabs>

## How it works

The runtime builds one `Context` per delivery and lends it to the middleware and then to the handler.
A `#[subscriber]` function takes it as `&mut Context<'_>` after the message; a manual handler gets it as the last argument of `handle`.
`name()` is the channel the message arrived on.
`headers()` reads the delivery's headers; `headers_mut()` gives a working copy that middleware can enrich for the handler.
The working copy stays inside the delivery: a reply or a publish from the handler starts with fresh headers.
`state()` is the application state that `on_startup` produced.

The context's first type parameter is the broker's per-delivery context: `()` for most subscriptions, `MemoryContext` for a retaining memory broker.
The broker exports keys for its fields, and `ctx.context(key)` reads one.
`Ctx<K>` does the same read as a handler parameter, so the body needs no `&mut Context`.
The `#[subscriber]` attribute infers the context type from the key.
A key only reads a context that the broker provides: `Ctx<Position>` compiles on a retaining memory broker, not on a plain one.
`Ctx<MainRuntime>` is the one key that reads no broker field: it binds the application's runtime for a handler on dedicated threads.

## See also

- [Custom extractors](./from-context.md): a parameter type of your own, resolved from the context.
- [Any struct as state](../../application/state/any-struct.md): the value `state()` returns, and `State<T>`.
- [`and_after` and post-settle hooks](../messages/and-after.md): work the context schedules after the settlement.
- [Start position and seeking](../start-at.md): the seek handle, another field of the memory broker's context.
- [`Context`](api:ruststream::runtime::Context), [`Ctx`](api:ruststream::runtime::Ctx), [`CtxKey`](api:ruststream::runtime::CtxKey), [`ContextField`](api:ruststream::ContextField), [`Field`](api:ruststream::Field), [`Position`](api:ruststream::memory::Position), [`MemoryContext`](api:ruststream::memory::MemoryContext), [`MainRuntime`](api:ruststream::runtime::MainRuntime)
