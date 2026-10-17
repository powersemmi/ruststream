---
sidebar_position: 6
title: Bytes
description: "A handler that reads the delivery's bytes as they arrived, and a reply that leaves as the bytes it holds, skip the codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Bytes

A handler that reads the delivery's bytes as they arrived, and a reply that leaves as the bytes it holds, skip the codec.

A newtype over `&'a [u8]` deriving [`Deserialized`](api:ruststream::Deserialized) is a view of the payload, borrowed from the broker's buffer.
A handler over `&Frame<'_>` receives it with no copy and no decode step, and `&[Frame<'_>]` is the [batch](../batches.md) form.
A bare `&[u8]` parameter does not compile: the newtype names what the bytes are.
A newtype over `Vec<u8>` deriving [`Serialized`](api:ruststream::Serialized) publishes its bytes as they are, as a reply, an `Out` slot or a publish from the builder.
The two lanes mix freely with codecs: a handler can decode its input with the scope codec and reply in bytes, or the other way round.
A service whose messages are all on the lanes builds with no codec feature.
A type that holds fields and writes its own format uses the same derives with `#[wire(..)]`, as [Protobuf](./protobuf.md) does.
In the manual form the impls the derives write are spelled out.

## Example

The example builds with no codec feature:

```text
cargo test --example byte_lanes --no-default-features --features testing,macros,memory
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/byte_lanes.rs#lanes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/byte_lanes.rs#lanes"
```

</TabItem>
</Tabs>
