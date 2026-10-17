---
sidebar_position: 2
title: Naming
description: "A naming transform sets the destination of each message, on a position where nothing else declared it: a reply to the inbox a request named."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Naming

A naming transform sets the destination of each message, on a position where nothing else declared it: a reply to the inbox a request named.

A transform that declares `type Destination = Names` may call [`set_name`](api:ruststream::runtime::Outgoing::set_name) on the outgoing message.
A position offers that right only where nothing declared the destination: a reply type with no `#[outgoing(name = ..)]`, or a slot whose whole `#[publishes(..)]` list leaves it open.
It offers it once, so a second naming transform on one position does not compile, and neither does one on a declared reply or on a batch reply.
[`DefaultSlot`](api:ruststream::runtime::DefaultSlot) and a slot that lists a declared destination never offer it.
The name at the mount site stays the declared fallback: the generated document reports it, and a delivery the transform leaves alone is answered there.
A name read off the delivery costs no copy: [`get_shared`](api:ruststream::HeaderMap::get_shared) hands over the header's own buffer, and [`Str`](api:ruststream::Str) checks it is UTF-8.
The reply side of [`RequestReply`](../publishers/request-reply.md) is the usual case.
A naming transform is a [publish transform](./publish-transform.md) in every other way: it has no macro form, and only the mounting differs.

## Example

{/* single-form: a PublishTransform impl has no macro form; the same code serves the macro and the manual service */}
```rust reference="examples/naming_transform.rs#transform"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/naming_transform.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/naming_transform.rs#app"
```

</TabItem>
</Tabs>
