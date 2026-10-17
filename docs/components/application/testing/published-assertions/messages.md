---
sidebar_position: 4
title: messages
description: "messages returns every message published to the destination, with its name, headers and payload, for checks of your own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# messages

[`messages`](api:ruststream::testing::PublishedAssertions::messages) returns every message published to the destination, with its name, headers and payload, for checks of your own.

The slice is in publish order, one [`RawMessage`](api:ruststream::RawMessage) per publish.
Each message gives its destination through `name()`, its headers through `headers()` and its bytes through `payload()`.
The built-in assertions, such as [`with_header`](./with-header.md), read only the latest message; `messages` is the way to check every one.
[`decoded`](./decoded.md) returns every payload as a value.
The slice borrows the record, so bind the record to a variable first.

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
