---
title: Headers
description: "A serde struct is the header contract a handler reads and a publish writes, checked in both directions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

A serde struct is the header contract a handler reads and a publish writes, checked in both directions.

A contract is a flat struct: each field names a header, and its value travels as a string.
Fields are numbers, booleans, strings, bytes, unit-only enums, or `Option`s of those.
An `Option` field reads as `None` when its header is absent; any other missing field fails the contract.
Header names are matched without regard to case; `#[serde(rename = "..")]` gives a wire name that is not a Rust identifier.
[`Headers<T>`](api:ruststream::runtime::Headers) parses the contract before the body runs.
A contract that does not parse is a decode failure, settled by the subscriber's `on_failure(decode = ..)` [policy](../failure-policy.md).
In a manual body `Headers::<T>::from_context(ctx)` parses it, and a failure there drops the delivery.
The raw `HeaderMap` of a delivery stays on its [context](../context/delivery-context.md).
What the handler publishes carries a contract of its own, declared on the [outgoing](./outgoing.md) type.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#contracts"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#contracts"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#handler"
```

</TabItem>
</Tabs>
