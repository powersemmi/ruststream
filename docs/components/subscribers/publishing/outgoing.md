---
sidebar_position: 3
title: Destinations
description: "A message type declares where it goes: a fixed name, a name template with a setter per placeholder, or no name, left to the call."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Destinations

A message type declares where it goes: a fixed name, a name template with a setter per placeholder, or no name, left to the call.

`#[derive(Outgoing)]` makes a type publishable, and `#[outgoing(name = "..")]` declares its destination.
A fixed name, `"orders.confirmed"`, resolves the destination for every call site, so the publish needs no `.to(..)`.
A name template, `"orders.{tenant}.placed"`, opens `.to()` with one setter per placeholder, and the publish exists only once every placeholder is bound.
A template renders its name on each publish; a fixed name costs nothing per publish.
A type without a name takes `.to("dest")` at every call site.
A reply type declares a fixed name or none: the runtime publishes the reply and has nothing to bind a template's placeholders with, so a [reply](./replies.md) to a template does not compile.
The generated AsyncAPI document reports each declared destination, a template as a channel with parameters.

Without the macros, the type implements [`OutgoingDestination`](api:ruststream::OutgoingDestination) by hand: `FixedName`, `NameTemplate` or `CallerName` as its `Form`.
A template there also names the builder of its setters through [`TemplateAddress`](api:ruststream::runtime::TemplateAddress).
The same attribute declares the message's header contract, `#[outgoing(headers = Meta)]`, described on the [Outgoing](../typed-headers/outgoing.md) page of typed headers.

## Example

The three forms of a destination.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/destinations.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/destinations.rs#messages"
```

</TabItem>
</Tabs>

Each publish fills only what its type leaves open.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/destinations.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/destinations.rs#handler"
```

</TabItem>
</Tabs>
