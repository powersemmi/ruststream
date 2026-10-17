---
sidebar_position: 3
title: BatchPublishTransform
description: "A batch publish transform is a step on the replies of a batch handler: each reply passes it on its own, with the batch's context."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# BatchPublishTransform

A [batch publish transform](api:ruststream::runtime::BatchPublishTransform) is a step on the replies of a batch handler: each reply passes it on its own, with the batch's context.

A [batch handler](../../batches.md) with a reply returns one `Vec` of replies per batch, and those replies pass their own steps.
[`.batch_transform(..)`](api:ruststream::runtime::Mounting::batch_transform) adds one after `.out_reply(policy)`, and a single-message mount rejects it at compile time.
The per-message [publish transforms](./publish-transform.md) do not run on batch replies, and batch transforms do not run on single replies.
[`for_batch`](api:ruststream::runtime::for_batch) lifts a per-message transform onto the batch path, so one impl serves both.
All replies of a batch see one [`PublishContext`](api:ruststream::runtime::PublishContext), the batch's: its name is the subscription, its headers are empty, and its context fields are the broker's batch context.
A transform that reads the message it answers belongs on a single-message handler.
A batch reply never offers the naming right.
A batch transform has no macro form: the same impl serves both forms, and only the mounting differs.

## Example

{/* single-form: a BatchPublishTransform impl has no macro form; the same code serves the macro and the manual service */}
```rust reference="examples/batch_transform.rs#transform"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/batch_transform.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/batch_transform.rs#app"
```

</TabItem>
</Tabs>
