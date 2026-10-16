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
The harness records the settings with every publish the runtime makes (a reply, a slot publish, a retry copy), on the broker it went to.
Both records read them: a slot record, `out::<M>()`, and a destination record, `published::<T>(name)`.
A message the test publishes itself carries no recorded settings.
The assertion names the broker's own type and panics when the latest publish carried no settings, carried another type, or carried a different value.
It also panics when it cannot tell which settings the latest publish carried: two publishes to the destination had the same payload and their order is unknown.

## See also

- [`assert_options_default`](./assert-options-default.md): no step ran.
- [`out::<M>()`](../records/out.md): the record that carries every slot publish's settings.
- [`PublishedAssertions::with_options`](api:ruststream::testing::PublishedAssertions::with_options), [`Publisher`](api:ruststream::Publisher)
