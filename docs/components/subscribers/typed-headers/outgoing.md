---
sidebar_position: 1
title: Outgoing
description: "A message type names its header contract, so publishing it does not compile without the headers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Outgoing

A message type names its header contract, so publishing it does not compile without the headers.

`#[outgoing(headers = Meta)]` makes the contract part of the message type, the type's [`MessageHeaders`](api:ruststream::MessageHeaders) contract.
Its publish does not compile without [`.with_headers(&meta)`](api:ruststream::runtime::PublishBuilder::with_headers).
The manual form implements `MessageHeaders` by hand and names the contract as its `Contract`; a type with none names `NoHeaders`.
An `Out` slot lists the types it may publish in `#[publishes(..)]`, and each keeps its own contract, as on the [Slots](../publishing/out-slots.md) page.
The handler on the [Headers](./index.md) page publishes a `ChunkDone` through such a slot.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#messages"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#dictionary"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#dictionary"
```

</TabItem>
</Tabs>
