---
sidebar_position: 5
title: with_raw
description: "with_raw(bytes) checks the exact bytes of the latest handler call, before any decoding."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# with_raw

[`with_raw(bytes)`](api:ruststream::testing::SubscriberAssertions::with_raw) checks the exact bytes of the latest handler call, before any decoding.

It compares the payload as the broker delivered it, so it pins the wire format: here the JSON the default codec wrote.
It reads the most recent call only, and that call must hold one delivery.
It is the check for a handler that reads bytes itself, where no codec has a value to compare.
[`with`](./with.md) checks the same call decoded, and [`received_raw`](./received-raw.md) returns the bytes of every call.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_payloads.rs#with_raw"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_payloads.rs#with_raw"
```

</TabItem>
</Tabs>
