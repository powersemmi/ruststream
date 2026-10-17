---
sidebar_position: 2
title: Message
description: "Message<H, T> hands a handler the decoded payload and its parsed header contract as one input."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Message

[`Message<H, T>`](api:ruststream::runtime::Message) hands a handler the decoded payload and its parsed header contract as one input.

`chunk.headers` is the contract and `chunk.body` the payload.
The contract parses the way [`Headers<T>`](./index.md) parses it, and a contract that does not parse is a decode failure.
A batch handler takes `&[Message<H, T>]`, so each element keeps its own headers.
A reply typed `Message<H, T>` carries its headers the same way.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#message"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#message"
```

</TabItem>
</Tabs>
