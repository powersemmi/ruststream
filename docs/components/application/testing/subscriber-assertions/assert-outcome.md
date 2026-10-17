---
sidebar_position: 14
title: assert_outcome
description: "assert_outcome(..) checks the latest handler call by its classified Outcome, failures included."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# assert_outcome

[`assert_outcome(..)`](api:ruststream::testing::SubscriberAssertions::assert_outcome) checks the latest handler call by its classified [`Outcome`](api:ruststream::testing::Outcome), failures included.

`Outcome` folds a call into five cases: `Ack`, `Nack`, `Drop`, `DecodeFailed` and `Panicked`.
`Nack` covers both `retry()` and `retry_after(..)`, so the delay plays no part.
A panic or a decode failure wins over the settlement: such a call reads as `Panicked` or `DecodeFailed`, which [`panicked`](./panicked.md) and [`assert_last_failed_to_decode`](./assert-last-failed-to-decode.md) check on their own.
Here the delayed copy waits for [`tb.advance(..)`](../driving/advance.md) on the paused clock, so the record holds one call.
[`settled`](./settled.md) checks by `HandlerOutcome` instead, and [`outcomes`](./outcomes.md) returns the outcome of every call.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_calls.rs#assert_outcome"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_calls.rs#assert_outcome"
```

</TabItem>
</Tabs>
