---
sidebar_position: 4
title: PublishDynStack
description: "A PublishDynStack is a publish middleware chain assembled when the app starts, added to the app as one publish layer."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PublishDynStack

A `PublishDynStack` is a publish middleware chain assembled when the app starts, added to the app as one publish layer.

A [`PublishDynStack`](api:ruststream::runtime::PublishDynStack) holds a list chosen when the app is built: from configuration, a loop or a feature flag.
Each element implements [`PublishDynLayer`](api:ruststream::runtime::PublishDynLayer).
It gets the outgoing message and a [`PublishDynNext`](api:ruststream::runtime::PublishDynNext); `next.run(out)` runs the rest of the list and then the static chain around the stack.
`PublishDynStack::new(list)` freezes the list, and the first element runs outermost.
The stack is a [`PublishLayer`](./publish-layer.md) itself, added with `RustStream::publish_layer`, so it wraps the same publishes as any other.
Each element costs one boxed future and one dynamic call per publish, and the stack two boxes more: its continuation and the future of the chain after it.
The static layers around it stay free.
The layer has no macro form: the same impl serves both forms, and only the handler differs.

## Example

The layer writes one header on every message.

{/* single-form: a PublishDynLayer impl has no macro form; the same code serves the macro and the manual service */}
```rust reference="examples/publish_dyn_stack.rs#layer"
```

The app builds one per header the configuration lists.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/publish_dyn_stack.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/publish_dyn_stack.rs#app"
```

</TabItem>
</Tabs>
