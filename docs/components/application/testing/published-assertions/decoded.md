---
sidebar_position: 5
title: decoded
description: "decoded returns every payload published to the destination as a value; decoded_with names the codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `decoded`

`decoded` returns every payload published to the destination as a value; `decoded_with` names the codec.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published_payloads.rs#decoded"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published_payloads.rs#decoded"
```

</TabItem>
</Tabs>

## How it works

The values come in publish order, decoded as the record's type parameter `T`.
`decoded` uses the default codec and exists when a codec feature is enabled.
`decoded_with(&codec)` decodes with the codec the mount site named, here a `.codec(..)` on the reply.
Either one panics, naming the destination and the type, when a payload does not decode.

## See also

- [`with`](./with.md): the latest payload against one value.
- [`decoded_as`](./decoded-as.md): decode as another type.
- [`PublishedAssertions::decoded`](api:ruststream::testing::PublishedAssertions::decoded), [`PublishedAssertions::decoded_with`](api:ruststream::testing::PublishedAssertions::decoded_with)
