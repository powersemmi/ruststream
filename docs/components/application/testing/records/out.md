---
sidebar_position: 3
title: out
description: "TestApp::out opens the record of one Out slot: every message the handler sent through it, on whichever broker it went."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# out

[`out::<M>()`](api:ruststream::testing::TestApp::out) opens the record of one `Out` slot: every message the handler sent through it, on whichever broker it went.

`M` is the slot's marker type, the one the handler names in `Out<impl Publisher, M>`, and it implements [`OutSlot`](api:ruststream::OutSlot).
The record is attributed by slot, not by destination, so it shows which slot sent a message when two slots share a destination.
[`published::<T>(name)`](./published.md) is the record of one destination on one broker.
Each entry keeps the destination name, the headers and the per-message settings of the publish, which [`with_options`](../published-assertions/with-options.md) checks.
The record is untyped: [`decoded_as::<U>()`](../published-assertions/decoded-as.md) turns it into one that decodes to `U`.
[`messages`](../published-assertions/messages.md) returns every entry for checks of your own.
A publish made outside the handler task, such as a spawned task, is not attributed to the slot.

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
