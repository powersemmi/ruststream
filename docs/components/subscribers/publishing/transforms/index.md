---
description: "A publish transform changes an outgoing message at one publish position, before it reaches the publisher."
---

# Transforms

A [publish transform](api:ruststream::runtime::PublishTransform) changes an outgoing message at one publish position, before it reaches the publisher.

A transform belongs to one position: a reply, one slot, or the replies of a batch.
It is static, so the chain of transforms is one type and costs only its own code.
What it reads, which settings it writes and whether it names the destination are part of its type, so a transform mounted where it does not fit does not compile.
The app-wide counterpart, which wraps every publish of the app, is the [publish layer](../../middleware/publish-layer.md).
