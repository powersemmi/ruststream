---
title: with_raw (published assertions)
sidebar_label: with_raw
sidebar_position: 8
description: "with_raw checks the bytes of the latest payload published to the destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# with_raw

[`with_raw`](api:ruststream::testing::PublishedAssertions::with_raw) checks the bytes of the latest payload published to the destination.

The assertion compares the payload byte for byte, with no codec involved.
It pins the wire form: field names, field order and the codec's formatting.
It also reads payloads no codec produced, such as a type that serializes itself.
It panics when nothing was published or the bytes differ.
[`with`](./with.md) checks the latest payload as a value, and [`messages`](./messages.md) returns every payload.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published_payloads.rs#with-raw"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published_payloads.rs#with-raw"
```

</TabItem>
</Tabs>
