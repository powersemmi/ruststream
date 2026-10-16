---
sidebar_position: 6
title: decoded_as
description: "decoded_as::<U>() re-types a record, so its payload assertions decode to U."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `decoded_as`

`decoded_as::<U>()` re-types a record, so its payload assertions decode to `U`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published_payloads.rs#decoded-as"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published_payloads.rs#decoded-as"
```

</TabItem>
</Tabs>

## How it works

`out::<M>()` returns an untyped record, because a slot may publish several types; `decoded_as` names the one to read.
The same call reads a destination record as a type other than the one it was opened with.
It consumes the record and keeps its messages and their recorded settings.
After it, `with`, `with_codec`, `decoded` and `decoded_with` decode to `U`.

## See also

- [`out::<M>()`](../records/out.md): the slot record that needs it.
- [`with`](./with.md), [`decoded`](./decoded.md)
- [`PublishedAssertions::decoded_as`](api:ruststream::testing::PublishedAssertions::decoded_as)
