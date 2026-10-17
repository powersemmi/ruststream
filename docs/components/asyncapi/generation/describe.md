---
sidebar_position: 8
title: describe
description: ".describe(text) gives a manual registration the operation description the attribute takes from the handler's doc comment."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# describe

`.describe(text)` gives a manual registration the operation description the attribute takes from the handler's doc comment.

[`describe`](api:ruststream::runtime::SubscriberBuilder::describe) is a step of the `subscriber(..)` chain, before `.build()`; after `.reply()` it is still available.
The text becomes the description of the registration's `receive` operation.
A message's description comes from its own type.
A message whose type has no description takes the handler's.
On the attribute path the doc comment of the handler function is the description, and the step has no use.
The example runs with `cargo test --example asyncapi_generation --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_generation.rs#describe"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_generation.rs#describe"
```

</TabItem>
</Tabs>
