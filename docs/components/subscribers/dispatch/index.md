---
description: "How many deliveries of one subscription run at once, and on which threads."
---

# Dispatch

How many deliveries of one subscription run at once, and on which threads.

A subscription handles one delivery at a time by default.
The next delivery is read only after the previous one has settled, so the handler sees the stream in order.
The handler runs on the runtime the app runs on, beside the broker clients' I/O, the timers and every other subscription.
[Workers](./workers.md) run several deliveries of one subscription at once, for a handler that waits on I/O.
[Threads](./threads.md) give a subscription threads of its own, for a handler that computes.
Both are a setting of the subscription, and a subscription takes one of them.
