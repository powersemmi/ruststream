---
sidebar_position: 1
title: Struct
description: "on_startup builds one value of any type; a handler takes any of its fields as a State<T> parameter."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Struct

[`on_startup`](api:ruststream::runtime::RustStream::on_startup) builds one value of any type; a handler takes any of its fields as a [`State<T>`](api:ruststream::runtime::State) parameter.

`on_startup` runs once, before any broker connects, and its value becomes the application state.
Its future can await: open a pool, connect a client, read a file.
`on_startup` comes before `with_broker`, because it fixes the state type every handler is checked against.
The [lifecycle](../lifecycle.md) hooks after it receive the same state.
`#[derive(FromRef)]` writes one [`FromRef`](api:ruststream::runtime::FromRef) impl per field, each cloning that field.
A handler parameter `State(prices): State<Prices>` resolves through that impl before the body runs.
A field that is expensive to clone goes behind an `Arc`, so a delivery pays one reference count.
A handler can also borrow the whole state with [`ctx.state()`](api:ruststream::runtime::Context::state) from its `&mut Context<'_, (), AppState>` parameter, with no clone.
A hand-written `FromRef` impl builds a value from several fields.
A dependency-injection container fills the same role: [froodi](./froodi.md) with RustStream support, or [shaku](./shaku.md) wired by hand.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/app_state.rs#state"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/app_state.rs#state"
```

</TabItem>
</Tabs>
