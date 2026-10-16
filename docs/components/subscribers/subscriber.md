---
sidebar_position: 1
title: "#[subscriber]"
description: "#[subscriber] turns an async fn over a reference to the message into a handler that include mounts on a broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `#[subscriber]`

`#[subscriber]` turns an `async fn` over a reference to the message into a handler that `include` mounts on a broker.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/subscriber_handler.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/subscriber_handler.rs#handler"
```

</TabItem>
</Tabs>

## How it works

The first parameter is the message: `&T` handles one message, `&[T]` a whole batch.
The runtime decodes the payload with the scope's codec before the body runs; a `serde` type is all the input needs.
A payload that does not decode never reaches the body: the decode failure policy settles it, and drops it by default.
An optional `&mut Context` comes second, and any further parameter is an extractor resolved before the body runs.
What the body returns settles the delivery: `HandlerOutcome`, `()` (ack), `Result<(), E>` (ack on `Ok`, drop on `Err`) or `Result<HandlerOutcome, E>`.
The attribute expands into a definition value named after the function, and `include` on a broker scope or a router mounts it.
The attribute's first argument is the subscription source; settings it leaves out are chained at the mount site.
The manual form writes the body as an `impl Handle<T>` of a named type, binds it with `subscriber(source, body)` and closes the chain with `.build()`.
There `Ok(())` acks and `Err(outcome)` settles by that outcome.

## See also

- [Subscription source](./subscription-source.md): by name or by the broker's descriptor.
- [Mount-site settings](./mount-settings.md): what the attribute leaves to `include`.
- [Batch handlers](./batches.md): a `&[T]` parameter.
- [Ack, retry, drop](./messages/handler-outcome.md): what the body returns.
- [Context and `Ctx<K>`](./context/context.md) and [Custom extractors](./context/from-context.md): the parameters after the message.
- [Application](../application/app.md): the broker scope `include` belongs to.
- [`#[subscriber]`](api:ruststream::subscriber), [`subscriber`](api:ruststream::runtime::subscriber), [`Handle`](api:ruststream::runtime::Handle), [`BrokerScope::include`](api:ruststream::runtime::BrokerScope::include), [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome)
