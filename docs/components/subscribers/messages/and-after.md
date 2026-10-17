---
sidebar_position: 3
title: and_after
description: "and_after attaches a continuation to an outcome: work that runs on a task of its own once the delivery is settled."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# and_after

[`and_after`](api:ruststream::runtime::HandlerOutcome::and_after) attaches a continuation to an outcome: work that runs on a task of its own once the delivery is settled.

`HandlerOutcome::ack().and_after(fut)` settles the delivery first, then runs `fut`.
A continuation attaches to any of the [outcomes](./handler-outcome.md), `drop()` and `retry()` included; a second `and_after` replaces the first.
On a batch each element carries its own continuation, so only the elements that asked for one run it.
Post-settle work is at most once: the delivery is already settled, so a lost or panicking continuation never brings it back.
Work whose loss must redeliver the message belongs in the body, before the settlement.
A graceful shutdown waits for running continuations within the app's `shutdown_timeout`.
A test waits for them with [`drain`](../../application/testing/driving/drain.md).
A [hook](./hooks.md) on the context runs after the settlement too, gated by which settlement it was.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/post_settle.rs#single"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/post_settle.rs#single"
```

</TabItem>
</Tabs>

## One continuation per element

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/post_settle.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/post_settle.rs#batch"
```

</TabItem>
</Tabs>
