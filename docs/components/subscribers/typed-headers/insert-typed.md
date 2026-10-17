---
sidebar_position: 3
title: insert_typed
description: "insert_typed writes a header contract into a header map, the way a publish transform stamps every message of a slot."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# insert_typed

[`HeaderMap::insert_typed`](api:ruststream::HeaderMap::insert_typed) writes a header contract into a header map, the way a publish transform stamps every message of a slot.

A field becomes one header named after it, its value written as a string.
A `None` field writes nothing, and a header of the same name is overwritten.
A field that holds a nested struct, a sequence or a map is an error.
The example's transform is a [publish transform](../publishing/transforms/index.md) on an `Out` slot, so every message the slot sends carries the contract.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/typed_headers.rs#stamp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/typed_headers.rs#stamp"
```

</TabItem>
</Tabs>
