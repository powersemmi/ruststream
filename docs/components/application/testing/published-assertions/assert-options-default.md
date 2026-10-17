---
sidebar_position: 11
title: assert_options_default
description: "assert_options_default checks that the latest recorded publish carried no per-message settings, so the publish policy's defaults applied."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# assert_options_default

[`assert_options_default`](api:ruststream::testing::PublishedAssertions::assert_options_default) checks that the latest recorded publish carried no per-message settings, so the publish policy's defaults applied.

A publish carries settings only when a builder step or a transform set them; otherwise the policy named at the mount site decides.
The records that carry settings are the same as for [`with_options`](./with-options.md): every publish the runtime makes, in a slot record and in a destination record.
The assertion panics when the latest publish carried settings, naming their type.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published_options.rs#assert-options-default"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published_options.rs#assert-options-default"
```

</TabItem>
</Tabs>
