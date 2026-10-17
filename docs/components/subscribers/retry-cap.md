---
sidebar_position: 7
title: Attempts
description: "max_attempts(n) ends a message that keeps coming back, and dead_letter(name) is where it goes then."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Attempts

[`max_attempts(n)`](api:ruststream::runtime::RouterWith::max_attempts) ends a message that keeps coming back, and [`dead_letter(name)`](api:ruststream::runtime::RouterWith::dead_letter) is where it goes then.

`max_attempts(n)` counts the deliveries one message gets, the first one included.
The delivery that reaches the cap is not retried: it is published to the `dead_letter` destination with its payload and headers.
A cap without a destination rejects the delivery instead, and a warning names the subscription.
The count is the broker's own where the transport keeps one, read through [`redelivery_count`](api:ruststream::IncomingMessage::redelivery_count), and the framework's [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) where it keeps none.
The in-memory broker counts its deliveries, so the cap there counts its redeliveries.
A [`retry()`](./messages/handler-outcome.md) and a [`retry_after(delay)`](./messages/retry-after.md) are both capped.
Where the broker moves a spent delivery itself (a queue with a delivery limit and a dead-letter exchange), the declaration reaches the subscription descriptor at startup, and the broker applies it.
The two steps come right after `include`, before [`.out_retry(..)`](./retry-destination.md), the slot the dead-letter copy leaves through, and each is written once.

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
