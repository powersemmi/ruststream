---
sidebar_position: 7
title: Attempt cap and dead letter
description: "max_attempts(n) ends a message that keeps coming back, and dead_letter(name) is where it goes then."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Attempt cap and dead letter

`max_attempts(n)` ends a message that keeps coming back, and `dead_letter(name)` is where it goes then.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/retry.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/retry.rs#declaration"
```

</TabItem>
</Tabs>

## How it works

`max_attempts(n)` counts the deliveries one message gets, the first one included.
The delivery that reaches the cap is not retried: it is published to the `dead_letter` destination with its payload and headers.
A cap without a destination rejects the delivery instead, and a warning names the subscription.
The count is the broker's own where the transport keeps one, and the framework's `RETRY_COUNT_HEADER` where it keeps none.
The in-memory broker counts its deliveries, so the cap there counts its redeliveries.
A `retry()` and a `retry_after(delay)` are both capped.
Where the broker moves a spent delivery itself (a queue with a delivery limit and a dead-letter exchange), the declaration reaches the subscription descriptor at startup, and the broker applies it.
The two steps come right after `include`, before `.out_retry(..)`, and each is written once.

## See also

- [Ack, retry, drop](./messages/handler-outcome.md): the `retry()` the cap counts.
- [`retry_after`](./messages/retry-after.md): a retry after a delay, capped the same way.
- [Where a retry copy goes](./retry-destination.md): the publisher the dead-letter copy leaves through.
- [`RouterWith::max_attempts`](api:ruststream::runtime::RouterWith::max_attempts), [`RouterWith::dead_letter`](api:ruststream::runtime::RouterWith::dead_letter), [`IncomingMessage::redelivery_count`](api:ruststream::IncomingMessage::redelivery_count), [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)
