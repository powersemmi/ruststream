---
title: Cost
description: "What the framework's own code costs per message, measured in instructions and allocations, and where the cost goes."
---

import {CoreCost} from '@site/src/components/Benchmarks';

export const labels = {"code": "Instructions and allocations per message", "table": "Show the numbers as a table", "scenario": "Scenario", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "measured": "measured"};

# Cost

What the framework's own code costs per message, measured in instructions and allocations, and
where the cost goes.

<CoreCost labels={labels} />

The numbers are the steady state per message, on the in-process broker, so no transport is in
them. A row shows its cold start on hover or focus: what starting the service and handling the
first delivery cost together, paid once.

Taking a delivery allocates nothing. The loop keeps one slot for the delivery and one encode
buffer, and every stage is a generic type resolved at compile time, with no dynamic call on the
way. A [`DynStack`](../components/subscribers/middleware/dyn-stack.md) of layers chosen at run
time costs one boxed future per layer per message. An
[`and_after`](../components/subscribers/messages/and-after.md) continuation is one boxed future,
paid only by the delivery that sets it.

A reply allocates nothing when the broker's client only reads the bytes: it is lent the loop's
buffer. The in-process broker keeps what it is handed, so its reply costs the buffer the codec
wrote. The other publish rows allocate what the in-process broker keeps of each message, and what
the scenario builds itself: a header value, or a request's single-use inbox.

The payload's size is the expensive part of a decode: compare the 1 KB body with the small
struct. The [benchmarks](benchmarks/index.md) publish every scenario with its method, against each
broker's raw client too.
