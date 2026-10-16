---
sidebar_position: 12
title: outcomes
description: "outcomes() returns how every handler call settled, in call order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `outcomes`

`outcomes()` returns how every handler call settled, in call order.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_calls.rs#outcomes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_calls.rs#outcomes"
```

</TabItem>
</Tabs>

## How it works

Each entry is an `Outcome`: `Ack`, `Nack`, `Drop`, `DecodeFailed` or `Panicked`.
`settled` and `assert_outcome` read the latest call only; `outcomes` pins a sequence, such as a nack followed by the ack of its redelivery.
A batch call has one entry, classified by its first delivery.
`Outcome` is `#[non_exhaustive]`, so a `match` on it needs a wildcard arm.

## See also

- [`assert_outcome`](./assert-outcome.md): the latest entry only.
- [`settled`](./settled.md): the latest settlement by `HandlerOutcome`.
- [`SubscriberAssertions::outcomes`](api:ruststream::testing::SubscriberAssertions::outcomes), [`Outcome`](api:ruststream::testing::Outcome)
