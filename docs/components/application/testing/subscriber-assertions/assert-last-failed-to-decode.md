---
sidebar_position: 17
title: assert_last_failed_to_decode
description: "assert_last_failed_to_decode() checks that the latest delivery did not decode into the handler's input type."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_last_failed_to_decode`

`assert_last_failed_to_decode()` checks that the latest delivery did not decode into the handler's input type.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_calls.rs#assert_last_failed_to_decode"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_calls.rs#assert_last_failed_to_decode"
```

</TabItem>
</Tabs>

## How it works

The verdict comes from the codec, or from the broker when its client reports a decode error itself.
The record holds the delivery as a call although the body never ran, so `assert_called_once` counts it.
The decode policy settles it: the default drops the message and keeps the service running.
The test injects the bad bytes through a `#[derive(Outgoing, Serialized)]` newtype, which publishes them as they are.
It returns nothing, so it ends a chain.

## See also

- [`assert_outcome`](./assert-outcome.md): the same verdict as `Outcome::DecodeFailed`.
- [`panicked`](./panicked.md): the other failure a call can end in.
- [`SubscriberAssertions::assert_last_failed_to_decode`](api:ruststream::testing::SubscriberAssertions::assert_last_failed_to_decode)
