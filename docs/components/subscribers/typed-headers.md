---
sidebar_position: 10
title: Typed headers
description: "A serde struct is the header contract a handler reads and a publish writes, checked in both directions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Typed headers

A serde struct is the header contract a handler reads and a publish writes, checked in both directions.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#contracts"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#contracts"
```

</TabItem>
</Tabs>

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

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#handler"
```

</TabItem>
</Tabs>

## The payload and its headers as one input

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#message"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#message"
```

</TabItem>
</Tabs>

## A contract written by a transform

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#stamp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#stamp"
```

</TabItem>
</Tabs>

## How it works

A contract is a flat struct: each field names a header, and its value travels as a string.
Fields are numbers, booleans, strings, bytes, unit-only enums, or `Option`s of those.
An `Option` field reads as `None` when its header is absent; any other missing field fails the contract.
Header names are matched without regard to case; `#[serde(rename = "..")]` gives a wire name that is not a Rust identifier.
`Headers<T>` parses the contract before the body runs, and `Message<H, T>` pairs it with the decoded payload; a batch takes `&[Message<H, T>]`.
A contract that does not parse is a decode failure, settled by the subscriber's `on_failure(decode = ..)` policy.
In a manual body `Headers::<T>::from_context(ctx)` parses it, and a failure there drops the delivery.
`#[outgoing(headers = Meta)]` makes the contract part of the message type: its publish does not compile without `.with_headers(&meta)`.
A reply typed `Message<H, T>` carries its headers the same way.
`HeaderMap::insert_typed` writes a contract into a header map, as a transform does; a `None` field writes nothing.

## See also

- [Failure policy](./failure-policy.md): what a contract that does not parse does.
- [Context](./context/context-and-ctx.md): the raw `HeaderMap` of a delivery.
- [Publishing](./publishing/index.md): `Out` slots and publish transforms.
- [`Headers`](api:ruststream::runtime::Headers), [`Message`](api:ruststream::runtime::Message), [`MessageHeaders`](api:ruststream::MessageHeaders), [`PublishBuilder::with_headers`](api:ruststream::runtime::PublishBuilder::with_headers), [`HeaderMap::insert_typed`](api:ruststream::HeaderMap::insert_typed)
