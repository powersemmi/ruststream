---
sidebar_position: 3
title: and_after and post-settle hooks
description: "Work that runs once the delivery is settled: a continuation on the outcome, or a hook on the context gated by the settlement."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `and_after` and post-settle hooks

Work that runs once the delivery is settled: a continuation on the outcome, or a hook on the context gated by the settlement.

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

## Hooks on the context

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

## How it works

`HandlerOutcome::ack().and_after(fut)` settles the delivery first, then runs `fut` on a task of its own.
A continuation attaches to any outcome, `drop()` and `retry()` included; a second `and_after` replaces the first.
On a batch each element carries its own continuation, so only the elements that asked for one run it.
`ctx.after_ack(fut)` registers a hook that runs only when the delivery is acked.
`ctx.after(outcome).then(fut)` runs only when the settlement matches that outcome by kind; `retry_after` matches any delay.
`ctx.after_settle(fut)` runs whatever the settlement.
Hooks accumulate, and every matching one runs.
On a batch the context belongs to the whole batch: only `after_settle` hooks run there, once, after every element is settled.
Post-settle work is at most once: the delivery is already settled, so a lost or panicking continuation never brings it back.
Work whose loss must redeliver the message belongs in the body, before the settlement.
A graceful shutdown waits for running continuations within the app's `shutdown_timeout`.
A test waits for them with `tb.drain()`.

## See also

- [Ack, retry, drop](./handler-outcome.md): the settlements a continuation follows.
- [Context](../context/context-and-ctx.md): the per-delivery context the hooks register on.
- [`drain`](../../application/testing/driving/drain.md): wait for continuations in a test.
- [`HandlerOutcome::and_after`](api:ruststream::runtime::HandlerOutcome::and_after), [`Context::after_ack`](api:ruststream::runtime::Context::after_ack), [`Context::after`](api:ruststream::runtime::Context::after), [`After::then`](api:ruststream::runtime::After::then), [`Context::after_settle`](api:ruststream::runtime::Context::after_settle), [`After`](api:ruststream::runtime::After)
