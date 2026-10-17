---
description: "A handler's parameters after the message: the per-delivery Context and the values extracted from it."
---

# Context and extractors

A handler's parameters after the message: the per-delivery [`Context`](api:ruststream::runtime::Context) and the values extracted from it.

- [`Context` and `Ctx<K>`](./context-and-ctx.md): the delivery's channel, headers and broker fields, and one field as a parameter.
- [Custom extractors](./from-context.md): a parameter type of your own, resolved before the body runs.
