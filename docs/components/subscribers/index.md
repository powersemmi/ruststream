---
description: "A subscriber is an async fn that handles the messages of one subscription, declared with [#[subscriber]](api:ruststream::subscriber) and mounted on a broker."
---

# Subscribers

A subscriber is an `async fn` that handles the messages of one subscription, declared with [`#[subscriber]`](api:ruststream::subscriber) and mounted on a broker.

- [`#[subscriber]`](./subscriber.md): an `async fn` that becomes a mountable handler.
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
