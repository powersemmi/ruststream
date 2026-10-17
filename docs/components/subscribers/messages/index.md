---
description: "What a handler returns settles its delivery: ack, retry, retry after a delay, or drop, as a HandlerOutcome."
---

# Messages

What a handler returns settles its delivery: ack, retry, retry after a delay, or drop, as a [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome).

- [Outcomes](./handler-outcome.md): what the body returns settles the delivery.
- [retry_after](./retry-after.md): the delivery comes back after a delay.
- [and_after](./and-after.md): a continuation that runs once the settlement is through.
- [Hooks](./hooks.md): work on the context, gated by the settlement.
