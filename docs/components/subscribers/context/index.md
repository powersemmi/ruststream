---
description: "A handler's parameters after the message: the per-delivery Context and the values extracted from it."
---

# Context and extractors

A handler's parameters after the message: the per-delivery [`Context`](api:ruststream::runtime::Context) and the values extracted from it.

- [Context](./delivery-context.md): the delivery's channel, headers, state and broker fields.
- [Ctx](./ctx.md): one broker field as a parameter.
- [Extractors](./from-context.md): a parameter type of your own, resolved before the body runs.
