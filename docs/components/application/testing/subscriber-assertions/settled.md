---
sidebar_position: 13
title: settled
description: "settled(outcome) checks how the latest handler call settled, against the HandlerOutcome a handler returns."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# settled

[`settled(outcome)`](api:ruststream::testing::SubscriberAssertions::settled) checks how the latest handler call settled, against the [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome) a handler returns.

It takes the value a handler answers with: `ack()`, `retry()`, `drop()`, `retry_after(..)`.
It compares the settlement alone, so a hook attached with `and_after` plays no part.
For a batch it checks every element of the latest call, which is what one answer for the whole slice produces.
The record holds what the handler answered: on a transport that cannot requeue, a retry still reads as a retry, and the missing redelivery shows in `assert_called`.
A fail-fast panic leaves the delivery unsettled, which matches no outcome.
[`assert_outcome`](./assert-outcome.md) is the same check by `Outcome`, failures included, and [`outcomes`](./outcomes.md) returns every settlement in order.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_calls.rs#settled"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_calls.rs#settled"
```

</TabItem>
</Tabs>
