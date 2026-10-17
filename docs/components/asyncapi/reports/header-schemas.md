---
title: Headers in AsyncAPI
sidebar_label: Headers
sidebar_position: 3
description: "A header contract becomes the headers schema of its message, from a Headers<T> parameter or from #[outgoing(headers = ..)]."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

A header contract becomes the headers schema of its message, from a `Headers<T>` parameter or from `#[outgoing(headers = ..)]`.

The contract struct derives `JsonSchema`, as a payload does.
The schema describes the logical contract; on the wire each header value is a string (see [Headers](../../subscribers/typed-headers/index.md)).
On the receive side the contract is a [`Headers<T>`](api:ruststream::runtime::Headers) parameter, or the header half of a [`Message<H, P>`](api:ruststream::runtime::Message) input.
On the publish side it is the type's [`#[outgoing(headers = Meta)]`](../../subscribers/typed-headers/outgoing.md) declaration.
A message component carries one contract.
Two handlers that read one type with different contracts keep the first, and generation logs a warning.
Without the macros a `Message<H, P>` input reports its contract.
The example runs with `cargo test --example asyncapi_reports --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_reports.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_reports.rs#headers"
```

</TabItem>
</Tabs>
