---
sidebar_position: 4
title: messages
description: "messages returns every message published to the destination, with its name, headers and payload, for checks of your own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `messages`

`messages` returns every message published to the destination, with its name, headers and payload, for checks of your own.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published.rs#messages"
```

</TabItem>
</Tabs>

## How it works

The slice is in publish order, one `RawMessage` per publish.
Each message gives its destination through `name()`, its headers through `headers()` and its bytes through `payload()`.
The built-in assertions read only the latest message; `messages` is the way to check every one.
The slice borrows the record, so bind the record to a variable first.

## See also

- [`decoded`](./decoded.md): every payload as a value.
- [`with_header`](./with-header.md): one header of the latest message.
- [`PublishedAssertions::messages`](api:ruststream::testing::PublishedAssertions::messages), [`RawMessage`](api:ruststream::RawMessage)
