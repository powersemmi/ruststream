---
title: Subscribers
description: "#[subscriber] turns an async fn over a reference to the message into a handler that include mounts on a broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscribers

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
- [Context and `Ctx<K>`](./context/context-and-ctx.md) and [Custom extractors](./context/from-context.md): the parameters after the message.
- [Application](../application/index.md): the broker scope `include` belongs to.
- [`#[subscriber]`](api:ruststream::subscriber), [`subscriber`](api:ruststream::runtime::subscriber), [`Handle`](api:ruststream::runtime::Handle), [`BrokerScope::include`](api:ruststream::runtime::BrokerScope::include), [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome)

## Pages in this category

- [Subscription source](./subscription-source.md): by name or by the broker's descriptor.
- [Mount-site settings](./mount-settings.md): the settings the attribute left open.
- [Batch handlers](./batches.md): one call per batch of messages.
- [Start position and seeking](./start-at.md): where a replayable subscription opens, and how the body repositions it.
- [Routers](./router.md): one module's handlers, mounted together.
- [Attempt cap and dead letter](./retry-cap.md): the end of a message that keeps coming back.
- [Where a retry copy goes](./retry-destination.md): the `Retry` slot.
- [Failure policy](./failure-policy.md): what a panic or an undecodable payload does.
- [Typed headers](./typed-headers.md): a serde struct as the header contract.
- [Messages](./messages/index.md): what a handler returns.
- [Context and extractors](./context/index.md): the handler's parameters after the message.
- [Codecs](./codecs/index.md), [Dispatch](./dispatch/index.md), [Middleware](./middleware/index.md), [Publishing](./publishing/index.md).
