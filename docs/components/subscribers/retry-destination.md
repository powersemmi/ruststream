---
sidebar_position: 8
title: Where a retry copy goes
description: "The Retry slot is the publisher a copy of a delivery leaves through; out_retry replaces it and to(name) names its destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Where a retry copy goes

The `Retry` slot is the publisher a copy of a delivery leaves through; `.out_retry(policy)` replaces it and `.to(name)` names its destination.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/settle_retry_destination.rs#mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/settle_retry_destination.rs#mount"
```

</TabItem>
</Tabs>

## A subscription that addresses nothing

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/settle_retry_destination.rs#filter"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/settle_retry_destination.rs#filter"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/settle_retry_destination.rs#named"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/settle_retry_destination.rs#named"
```

</TabItem>
</Tabs>

## How it works

The runtime publishes a copy of a delivery in two cases: a delayed retry on a broker without native delayed redelivery, and a delivery sent to its `dead_letter` destination.
Every registration has a publisher for those copies, paired from the broker's default publish policy.
`.out_retry(policy)` replaces it; it is the `Out` slot `Retry`, so `.codec(..)` and `.transform(..)` follow it as on any slot.
A copy carries the delivery's own bytes, so no codec encodes it, and its transforms read the delivery being retried.
A copy sent back to the subscription carries the `RETRY_COUNT_HEADER`, incremented.
On the in-memory broker the delay is native, so only the dead-letter copy leaves through this slot.
A subscription whose descriptor declares `AddressedCopies` reports where it is reached again, and copies go there.
One that declares `NamedCopies` reads many destinations and addresses none: a wildcard, a filter, a pattern.
On it the mount site names the destination with `.to(name)`, or a transform names one per delivery.
Without either, a router chain does not compile, and a mount in a broker scope refuses to start.
A name given with `.to(name)` is a channel the registration sends to, and the generated document reports it.
A broker that moves a spent delivery itself declares `BrokerMoves`, and `.out_retry(..)` does not compile there.

## See also

- [Attempt cap and dead letter](./retry-cap.md): when a copy goes to the dead-letter destination.
- [`retry_after`](./messages/retry-after.md): the delayed retry a copy carries.
- [Subscription source](./subscription-source.md): the descriptor that declares its copy path.
- [Publishing](./publishing/index.md): slots and publish transforms.
- [`Retry`](api:ruststream::runtime::Retry), [`RouterWith::out_retry`](api:ruststream::runtime::RouterWith::out_retry), [`RouterWith::to`](api:ruststream::runtime::RouterWith::to), [`NamedCopies`](api:ruststream::NamedCopies), [`AddressedCopies`](api:ruststream::AddressedCopies), [`PublishTransform`](api:ruststream::runtime::PublishTransform)
