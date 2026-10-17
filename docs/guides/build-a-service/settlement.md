---
title: "Settlement: retry, retry_after, dead letters"
description: "charge answers each refusal of the payment provider with the outcome it needs: a retry now, a retry later, a drop, or the dead-letter topic."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Settlement: retry, retry_after, dead letters

`charge` answers each refusal of the payment provider with the outcome it needs: a retry now, a retry later, a drop, or the dead-letter topic.

Every delivery ends in one [outcome](../../components/subscribers/messages/handler-outcome.md).
[`ack`](api:ruststream::runtime::HandlerOutcome::ack) finishes it, [`drop`](api:ruststream::runtime::HandlerOutcome::drop) discards it, and [`retry`](api:ruststream::runtime::HandlerOutcome::retry) asks the broker to deliver it again.
[`retry_after`](../../components/subscribers/messages/retry-after.md) delivers it again after a delay, without holding up the deliveries behind it.
A retry that never succeeds would come back forever, so the mount site caps the attempts.
Past the [cap](../../components/subscribers/retry-cap.md), the delivery goes to a dead-letter topic, where an operator or another service can look at it.

## The subscriber

The payment provider refuses a charge in three ways, and each needs a different outcome.
`Busy` means its rate limit is used up for now: an immediate retry would be refused again, so `retry_after` waits a second.
`Unavailable` means it did not answer: `retry` tries again at once.
`Declined` means the card does not cover the amount, and another try gets the same answer, so the payment is dropped.
The provider's client comes from the app's state, which chapter 7 opens.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="payments.rs" reference="examples/guide/payments.rs#charge"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#charge"
```

</TabItem>
</Tabs>

## The cap and the dead-letter topic

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) counts every delivery of one message, the first included.
The fifth delivery is not retried: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) publishes it to `payments.dead`, payload and headers as they arrived.
The router decodes the requests as CBOR and encodes the reply the same way.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="payments.rs" reference="examples/guide/payments.rs#routes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#routes"
```

</TabItem>
</Tabs>

The in-memory broker redelivers by itself, after the delay `retry_after` asks for.
A broker that cannot delay a delivery gets a copy published back to the subscription; [where a retry copy goes](../../components/subscribers/retry-destination.md) covers it.
Chapter 8 tests all three paths, the delay included, without waiting for it.
