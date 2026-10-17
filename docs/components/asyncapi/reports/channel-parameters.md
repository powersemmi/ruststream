---
sidebar_position: 2
title: Parameters
description: "A destination declared as a name template becomes a channel whose address keeps its placeholders, each one declared as a parameter."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Parameters

A destination declared as a name template becomes a channel whose address keeps its placeholders, each one declared as a parameter.

`#[outgoing(name = "orders.{tenant}.accepted")]` declares a [name template](../../subscribers/publishing/outgoing.md).
The template is the channel's id and its address.
Each `{placeholder}` is an entry of the channel's [`parameters`](api:ruststream::asyncapi::Parameter).
The document names the segments; what a service puts in them is run-time data and is not listed.
A type reaches the document where it is published: each type of an `Out` slot's `#[publishes(..)]` list gets a `send` operation on its channel.
A type with no declared destination contributes no channel, since each call site names its own.
Without the macros the slot's [`OutSlot::outgoing`](api:ruststream::runtime::OutSlot::outgoing) returns the list, and [`OutgoingMessageMetadata::with_parameters`](api:ruststream::runtime::OutgoingMessageMetadata::with_parameters) names the placeholders.
The example runs with `cargo test --example asyncapi_reports --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_reports.rs#parameters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_reports.rs#parameters"
```

</TabItem>
</Tabs>
