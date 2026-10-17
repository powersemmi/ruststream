---
sidebar_position: 2
title: PublishLayer
description: "A publish layer wraps every message the app's handlers publish: it changes the message, observes the send, or refuses it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PublishLayer

A publish layer wraps every message the app's handlers publish: it changes the message, observes the send, or refuses it.

A [`PublishLayer`](api:ruststream::runtime::PublishLayer) gets the [`Outgoing`](api:ruststream::runtime::Outgoing) message and a [`PublishNext`](api:ruststream::runtime::PublishNext).
`next.run(out)` runs the layers after it and then the broker send.
Before that call the layer may change the message, its headers for one; after it, the layer sees the send's result.
An error returned without the call refuses the publish.
[`RustStream::publish_layer`](api:ruststream::runtime::RustStream::publish_layer) adds a layer to the app, before the first `with_broker`; the layer is `Clone`.
It wraps a reply and every message sent through an `Out` slot.
A message passes its position's [transforms](../publishing/transforms/index.md) first, then the app's publish layers, then the send.
The last publish layer added runs outermost, the reverse of the order of handler [layers](./layer.md).
A handler mounted through a router is the exception on the slot side: its slots take their own transforms, not the app's publish layers; its replies pass through them.
The chain is one type, with no dynamic call.
A layer has no macro form: the same impl serves both forms, and only the handler differs.
A chain chosen when the app starts goes into a [`PublishDynStack`](./publish-dyn-stack.md).

## Example

The layer names the service on every message it publishes.

{/* single-form: a PublishLayer impl has no macro form; the same code serves the macro and the manual service */}
```rust reference="examples/publish_layer.rs#layer"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/publish_layer.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/publish_layer.rs#app"
```

</TabItem>
</Tabs>
