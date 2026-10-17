---
description: "Layers wrap handlers and publishes: app-wide, per router or per registration."
---

# Middleware

Layers wrap handlers and publishes: app-wide, per router or per registration.

A [layer](./layer.md) wraps a handler: it runs code of its own around each delivery, with the delivery's context.
A [publish layer](./publish-layer.md) wraps a publish: it runs around each message a handler sends.
Both are static: the chain is one type built when the code compiles, and a layer costs only its own code.
A chain chosen when the app starts, from configuration, goes into a dynamic stack, which boxes each step.
