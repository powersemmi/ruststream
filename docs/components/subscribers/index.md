---
title: Subscribers
description: "#[subscriber] turns an async fn over a reference to the message into a handler that include mounts on a broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscribers

[`#[subscriber]`](api:ruststream::subscriber) turns an `async fn` over a reference to the message into a handler that `include` mounts on a broker.

The first parameter is the message: `&T` handles one message, `&[T]` a whole [batch](./batches.md).
The runtime decodes the payload with the scope's codec before the body runs; a `serde` type is all the input needs.
A payload that does not decode never reaches the body: the decode [failure policy](./failure-policy.md) settles it, and drops it by default.
An optional `&mut Context` comes second, and any further parameter is an extractor resolved before the body runs: the [context](./context/index.md) page covers both.
What the body returns settles the delivery: [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome), `()` (ack), `Result<(), E>` (ack on `Ok`, drop on `Err`) or `Result<HandlerOutcome, E>`, as the [outcomes](./messages/handler-outcome.md) page shows.
The attribute expands into a definition value named after the function, and [`include`](api:ruststream::runtime::BrokerScope::include) on a broker scope of the [application](../application/index.md) or a router mounts it.
The attribute's first argument is the subscription [source](./subscription-source.md); [settings](./mount-settings.md) it leaves out are chained at the mount site.
The manual form writes the body as an [`impl Handle<T>`](api:ruststream::runtime::Handle) of a named type, binds it with [`subscriber(source, body)`](api:ruststream::runtime::subscriber) and closes the chain with `.build()`.
There `Ok(())` acks and `Err(outcome)` settles by that outcome.

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

## Pages in this category

- [Source](./subscription-source.md): by name or by the broker's descriptor.
- [Settings](./mount-settings.md): the settings the attribute left open.
- [Batches](./batches.md): one call per batch of messages.
- [Replay](./replay/index.md): where a replayable subscription opens, and how the body repositions it.
- [Routers](./router.md): one module's handlers, mounted together.
- [Attempts](./retry-cap.md): the end of a message that keeps coming back.
- [Retry](./retry-destination.md): the slot a retry copy leaves through.
- [Failures](./failure-policy.md): what a panic or an undecodable payload does.
- [Headers](./typed-headers/index.md): a serde struct as the header contract.
- [Messages](./messages/index.md): what a handler returns.
- [Context and extractors](./context/index.md): the handler's parameters after the message.
- [Codecs](./codecs/index.md), [Dispatch](./dispatch/index.md), [Middleware](./middleware/index.md), [Publishing](./publishing/index.md).
