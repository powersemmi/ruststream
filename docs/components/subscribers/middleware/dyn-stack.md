---
sidebar_position: 3
title: DynStack
description: "A DynStack is a handler middleware chain assembled when the app starts, mounted on one registration as one layer."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DynStack

A `DynStack` is a handler middleware chain assembled when the app starts, mounted on one registration as one layer.

A static [layer](./layer.md) chain is fixed when the code compiles.
A [`DynStack`](api:ruststream::runtime::DynStack) holds a list chosen when the app is built: from configuration, a loop or a feature flag.
Each element implements [`DynMiddleware`](api:ruststream::runtime::DynMiddleware).
It gets the input, the context and a [`Next`](api:ruststream::runtime::Next); `next.run(input, ctx)` runs the rest of the chain and the handler.
It may return an outcome without that call.
`DynStack::new(list)` freezes the list, and the first element runs outermost.
An empty list leaves the handler's behaviour as it is.
The stack is one ordinary `Layer`, fixed to one input type, so it wraps one registration: `.layer(stack)` right after an `include` on a [router](../router.md).
The app and a router take only layers that wrap any input, which a stack fixed to one type is not.
A registration's layer runs outside its decode step, so a stack over the broker's message type, `MemoryMessage` here, sees the raw delivery.
Each element costs one boxed future and one dynamic call per delivery, and the stack one more of each for the handler it wraps; the static layers around it stay free.
The middleware has no macro form: the same impl serves both forms, and only the mounting differs.

## Example

The middleware drops the deliveries of one tenant.

{/* single-form: a DynMiddleware impl has no macro form; the same code serves the macro and the manual service */}
```rust reference="examples/dyn_stack.rs#middleware"
```

The app builds one per tenant the configuration blocks.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/dyn_stack.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/dyn_stack.rs#app"
```

</TabItem>
</Tabs>
