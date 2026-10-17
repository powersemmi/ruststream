---
sidebar_position: 6
title: with_value
description: "with_value(&value) checks the value the latest delivery lent its handler, on a broker whose deliveries already hold one."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# with_value

[`with_value(&value)`](api:ruststream::testing::SubscriberAssertions::with_value) checks the value the latest delivery lent its handler, on a broker whose deliveries already hold one.

Some brokers decode a delivery in their client and lend the handler that value, such as a database row; the delivery implements [`Carries<T>`](api:ruststream::Carries).
That value never was bytes, so [`with`](./with.md) has nothing to decode; the harness keeps a clone of the lent value instead.
The value type needs `Clone`, `PartialEq` and `Debug`.
It panics when the latest delivery lent no `T`: the handler takes another type, or the delivery carried nothing.
The row subscription here stands in for such a broker crate: it reads `id:name` payloads of the in-memory broker as rows (`examples/support/carried_rows.rs`).
[`received_values`](./received-values.md) returns every lent value, not only the latest.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_carried.rs#with_value"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_carried.rs#with_value"
```

</TabItem>
</Tabs>
