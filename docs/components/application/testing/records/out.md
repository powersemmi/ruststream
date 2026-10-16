---
sidebar_position: 3
title: out::<M>()
description: "TestApp::out opens the record of one Out slot: every message the handler sent through it, on whichever broker it went."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `out::<M>()`

`out::<M>()` opens the record of one `Out` slot: every message the handler sent through it, on whichever broker it went.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_records.rs#out"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_records.rs#out"
```

</TabItem>
</Tabs>

## How it works

`M` is the slot's marker type, the one the handler names in `Out<impl Publisher, M>`.
The record is attributed by slot, not by destination, so it shows which slot sent a message when two slots share a destination.
Each entry keeps the destination name, the headers and the per-message settings of the publish.
The record is untyped: `decoded_as::<U>()` turns it into one that decodes to `U`.
A publish made outside the handler task, such as a spawned task, is not attributed to the slot.

## See also

- [`published::<T>(name)`](./published.md): the record of one destination on one broker.
- [`decoded_as`](../published-assertions/decoded-as.md), [`messages`](../published-assertions/messages.md), [`with_options`](../published-assertions/with-options.md)
- [`TestApp::out`](api:ruststream::testing::TestApp::out), [`OutSlot`](api:ruststream::OutSlot)
