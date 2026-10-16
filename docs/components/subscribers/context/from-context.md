---
sidebar_position: 2
title: Custom extractors
description: "A type that implements FromContext becomes a handler parameter, resolved from the delivery before the body runs."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Custom extractors

A type that implements `FromContext` becomes a handler parameter, resolved from the delivery before the body runs.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/from_context.rs#extractor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/from_context.rs#extractor"
```

</TabItem>
</Tabs>

## The rejection settles the delivery

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/from_context.rs#test"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/from_context.rs#test"
```

</TabItem>
</Tabs>

## How it works

Every parameter of a `#[subscriber]` function after the message and the optional `&mut Context` is an extractor.
The generated handler resolves the extractors in the order they are declared, then runs the body.
`from_context` reads the context mutably, so an extractor can read headers, broker fields and the state.
It is async, so it may also do I/O, such as a lookup.
An error is the `Rejection`, which converts into a `HandlerOutcome`: the delivery settles with that outcome, and the body does not run.
`HandlerOutcome` itself is a valid rejection; an extractor that cannot fail uses `Infallible`.
A manual handler calls `from_context` at the top of `handle` and returns a rejection as its error.
The impl is generic over the context type `C` and the state `S`, so the extractor works on any subscription.
An extractor that reads a broker field or a typed state implements the trait for those types instead.
`State<T>` and `Ctx<K>` are extractors of this kind that the crate provides.

## See also

- [`Context` and `Ctx<K>`](./context.md): what the context carries, and one broker field as a parameter.
- [Any struct as state](../../application/state/any-struct.md): `State<T>`, the extractor for the application state.
- [Ack, retry, drop](../messages/handler-outcome.md): the outcomes a rejection can settle with.
- [`FromContext`](api:ruststream::runtime::FromContext), [`State`](api:ruststream::runtime::State), [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome)
