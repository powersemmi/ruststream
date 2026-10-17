---
description: "How a handler answers a message and publishes to other destinations."
---

# Publishing

How a handler answers a message and publishes to other destinations.

A handler publishes in two ways.
Returning a value is a [reply](./replies.md), the default when the handler answers on one destination.
Taking a publisher as an [`Out` slot](./out-slots.md) is for a destination chosen at run time, or several at once, another broker's included.
Either way the handler never holds a publisher that is not connected: the mount site names a [publish policy](./publishers/publisher.md), and startup pairs it with the connected broker.
A message type declares its own [destination](./outgoing.md), and one [builder](./publish-builder.md) publishes every message.
