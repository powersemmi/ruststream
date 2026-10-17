---
sidebar_position: 4
title: Hooks
description: "A post-settle hook registered on the context runs once the delivery is settled, only for the settlement it names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Hooks

A post-settle hook registered on the [context](../context/delivery-context.md) runs once the delivery is settled, only for the settlement it names.

[`ctx.after_ack(fut)`](api:ruststream::runtime::Context::after_ack) registers a hook that runs only when the delivery is acked.
[`ctx.after(outcome)`](api:ruststream::runtime::Context::after) returns an [`After`](api:ruststream::runtime::After), and its [`then(fut)`](api:ruststream::runtime::After::then) runs only when the settlement matches that outcome by kind; `retry_after` matches any delay.
[`ctx.after_settle(fut)`](api:ruststream::runtime::Context::after_settle) runs whatever the settlement.
Hooks accumulate, and every matching one runs.
On a batch the context belongs to the whole batch: only `after_settle` hooks run there, once, after every element is settled.
A hook is at most once, like a continuation of [`and_after`](./and-after.md): a lost or panicking hook never brings the delivery back.
A graceful shutdown waits for running hooks within the app's `shutdown_timeout`, and a test waits for them with [`drain`](../../application/testing/driving/drain.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/post_settle.rs#hooks"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/post_settle.rs#hooks"
```

</TabItem>
</Tabs>
