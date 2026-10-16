---
sidebar_position: 9
title: with_header
description: "with_header checks one header of the latest message published to the destination, such as one a transform or a publish layer added."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `with_header`

`with_header` checks one header of the latest message published to the destination, such as one a transform or a publish layer added.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published.rs#with-header"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published.rs#with-header"
```

</TabItem>
</Tabs>

## How it works

The record holds the message as it left the publish pipeline, so every header a transform or a publish layer added is there.
The example stamps one header with a transform on the reply and one with an app-wide `publish_layer`.
The header name is matched without regard to case; the value is compared as bytes, so a `&str` and a `&[u8]` both work.
The assertion panics when nothing was published, when the header is absent, or when its value differs.

## See also

- [`messages`](./messages.md): the headers of every message.
- [`PublishedAssertions::with_header`](api:ruststream::testing::PublishedAssertions::with_header), [`PublishTransform`](api:ruststream::runtime::PublishTransform), [`PublishLayer`](api:ruststream::runtime::PublishLayer)
