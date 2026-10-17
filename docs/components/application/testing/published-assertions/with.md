---
title: with (published assertions)
sidebar_label: with
sidebar_position: 7
description: "with checks that the latest payload published to the destination decodes to a value; with_codec names the codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# with

[`with`](api:ruststream::testing::PublishedAssertions::with) checks that the latest payload published to the destination decodes to a value; [`with_codec`](api:ruststream::testing::PublishedAssertions::with_codec) names the codec.

`with(&value)` decodes the latest payload with the default codec and compares it with `value`.
It exists when a codec feature is enabled.
`with_codec(&codec, &value)` decodes with the codec the mount site named.
Both panic when nothing was published, when the payload does not decode, or when the value differs.
Both return the record, so further assertions chain.
[`decoded`](./decoded.md) returns every payload, not only the latest, and [`with_raw`](./with-raw.md) checks the latest payload as bytes.
A subscriber's [`with`](../subscriber-assertions/with.md) checks the value a handler received.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published_payloads.rs#with"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published_payloads.rs#with"
```

</TabItem>
</Tabs>
