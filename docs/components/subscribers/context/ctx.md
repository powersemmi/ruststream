---
sidebar_position: 2
title: Ctx
description: "Ctx<K> passes one field of the broker's per-delivery context to the handler as a parameter."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Ctx

[`Ctx<K>`](api:ruststream::runtime::Ctx) passes one field of the broker's per-delivery [context](./delivery-context.md) to the handler as a parameter.

`K` is the broker's key for the field, the same key `ctx.context(key)` reads, so the body needs no `&mut Context`.
`Ctx` takes any [`CtxKey`](api:ruststream::runtime::CtxKey): a broker's context key, or `MainRuntime`.
The `#[subscriber]` attribute infers the context type from the key.
A key only reads a context that the broker provides: `Ctx<`[`Position`](api:ruststream::memory::Position)`>` compiles on a retaining memory broker, not on a plain one.
`Ctx<`[`MainRuntime`](api:ruststream::runtime::MainRuntime)`>` is the one key that reads no broker field: it binds the application's runtime for a handler on dedicated threads.
`Ctx<K>` is an [extractor](./from-context.md), resolved before the body runs.

## Example

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
