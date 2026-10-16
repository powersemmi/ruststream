---
sidebar_position: 13
title: settled
description: "settled(outcome) checks how the latest handler call settled, against the HandlerOutcome a handler returns."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `settled`

`settled(outcome)` checks how the latest handler call settled, against the `HandlerOutcome` a handler returns.

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

## How it works

It takes the value a handler answers with: `ack()`, `retry()`, `drop()`, `retry_after(..)`.
It compares the settlement alone, so a hook attached with `and_after` plays no part.
For a batch it checks every element of the latest call, which is what one answer for the whole slice produces.
The record holds what the handler answered: on a transport that cannot requeue, a retry still reads as a retry, and the missing redelivery shows in `assert_called`.
A fail-fast panic leaves the delivery unsettled, which matches no outcome.

## See also

- [`assert_outcome`](./assert-outcome.md): the same check by `Outcome`, failures included.
- [`outcomes`](./outcomes.md): every settlement in order.
- [`SubscriberAssertions::settled`](api:ruststream::testing::SubscriberAssertions::settled), [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome)
