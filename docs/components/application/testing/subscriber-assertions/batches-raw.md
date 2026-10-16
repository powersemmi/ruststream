---
sidebar_position: 11
title: batches_raw
description: "batches_raw() returns the bytes a batch handler received, one list per call."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `batches_raw`

`batches_raw()` returns the bytes a batch handler received, one list per call.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_payloads.rs#batches_raw"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_payloads.rs#batches_raw"
```

</TabItem>
</Tabs>

## How it works

It is `received_raw` with the batch boundaries kept.
A single-message handler yields lists of one.
Here the handler takes raw frames, `&[Frame<'_>]`, so no codec runs on the way in or in the check.
The batches come from a replayed log, as on the `batches` page.

## See also

- [`batches`](./batches.md): the same batches decoded.
- [`received_raw`](./received-raw.md): the same bytes as one flat list.
- [`settle`](../driving/settle.md): drive a batch built from a bare publisher.
- [`SubscriberAssertions::batches_raw`](api:ruststream::testing::SubscriberAssertions::batches_raw)
