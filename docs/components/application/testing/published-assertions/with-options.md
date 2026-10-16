---
sidebar_position: 10
title: with_options
description: "with_options checks the per-message settings the latest recorded publish carried, the value a builder step set."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `with_options`

`with_options` checks the per-message settings the latest recorded publish carried, the value a builder step set.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published_options.rs#with-options"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published_options.rs#with-options"
```

</TabItem>
</Tabs>

## How it works

Per-message settings are the broker's `Publisher::Options`; the memory broker's publisher has none, so the example wraps it with a priority, as a networked broker ships one.
A slot record, `out::<M>()`, carries the settings of every publish through the slot.
A destination record, `published::<T>(name)`, carries the settings of the replies the runtime published there.
A message the test publishes itself carries none.
The assertion names the broker's own type and panics when the latest publish carried no settings, carried another type, or carried a different value.

## See also

- [`assert_options_default`](./assert-options-default.md): no step ran.
- [`out::<M>()`](../records/out.md): the record that carries every slot publish's settings.
- [`PublishedAssertions::with_options`](api:ruststream::testing::PublishedAssertions::with_options), [`Publisher`](api:ruststream::Publisher)
