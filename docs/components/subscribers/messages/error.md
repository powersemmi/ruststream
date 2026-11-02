---
sidebar_position: 5
title: error
description: "error tells the handler when a publish the runtime makes for its outcome fails: a retry copy, a dead-letter copy or a reply."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# error

[`error`](api:ruststream::runtime::HandlerOutcome::error) tells the handler when a publish the runtime makes for its outcome fails.

Some publishes are the runtime's, not the handler's: the delayed copy of a [`retry_after`](./retry-after.md), the copy that goes to a declared [dead letter](../retry-cap.md), and the reply of an [`out_reply`](../publishing/replies.md).
A publish in the handler body returns a `Result`, and the handler decides.
A publish the runtime makes on its behalf gives the same control through `.error`.

The continuation receives [`Failures`](api:ruststream::runtime::Failures), the publishes that failed.
Each [`Failed`](api:ruststream::runtime::Failed) holds the cause, `error()`, and the very [message](api:ruststream::runtime::FailedMessage) the runtime tried to send, `message()`: its destination, its bytes and its headers.
The message is the one that was sent: the headers and bytes the publish transforms and the publisher's base headers produced, so a reply arrives as it left.
The bytes are `None` only when the publisher took the payload buffer and did not give it back.
The message is a [`Serialized`](api:ruststream::Serialized) value, so another `Out` republishes it as it is, and nothing is decoded again.
A message with no bytes cannot be republished: the publish returns [`MissingPayloadError`](api:ruststream::runtime::MissingPayloadError).
It returns nothing, like [`and_after`](./and-after.md), and runs once the delivery is settled.
Whatever the reaction needs (a client, state, a counter, an `Out` publisher) the handler moves into the closure.
An `Out` publisher is cloned for that: declare it `Out<impl Publisher>` and move `alerts.clone()` in, then publish through it in the continuation as in the handler body.
The closure runs at most once, like `and_after`, so it may own what it captures.

The handler never runs again because a runtime publish failed.
The delivery is settled before the copy is published, so a failed copy cannot make the handler repeat its side effects or run past its `max_attempts` cap.
A failed reply settles the same way: the delivery is acknowledged, and the `.error` registered from the context runs.

`.error` runs for every cause: the network, a broker that is down, full or overloaded, a broker that rejected the message, a codec or a publish transform that failed, or, on the embedded runner, a store of retry copies with no free record ([`NoFreeRecordError`](api:ruststream::runtime::NoFreeRecordError)).
`Failed::error` is the cause itself, so the continuation can tell a transient failure from a permanent one.

Without `.error` the failure is logged with the subscription, the destination, the message type and the error, and the copy is lost.

## Example

### On the outcome

`HandlerOutcome::error` covers the publishes the runtime makes for that outcome: the delayed retry copy, and the dead-letter copy when the mount's `max_attempts(..).dead_letter(..)` turns this retry into a dead letter at the cap.
The handler below retries once at its cap, so the retry becomes a dead letter, and the broker refuses the dead-letter copy.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/retry_error.rs#error"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/retry_error.rs#error"
```

</TabItem>
</Tabs>

On a subscription whose broker retries and dead-letters natively the runtime publishes nothing, so the continuation never runs.
Each outcome that carries `.error` there logs a warning naming the subscription.
A batch handler has two forms.
A `Vec<HandlerOutcome>` attaches a one-shot `.error` to the outcome of the element it concerns; its `Failures` holds that element's failure.
A single `HandlerOutcome` returned for the whole batch runs its `.error` once, with the failure of every element whose copy failed, in the batch's order.
The list is allocated only when a copy fails.
The [`and_after`](./and-after.md) beside a uniform outcome runs once, after the batch, when at least one copy was published; when every copy fails it does not run.
Both continuations run beside each other, so neither waits for the other.

### From the context

The context form covers a delivery whose handler returns no outcome of its own, and the reply of `out_reply`:
[`ctx.after(outcome).error(..)`](api:ruststream::runtime::After::error) names the settlement it stands behind, as [`then`](./hooks.md) does.
A batch handler that returns replies registers it the same way; when the replies of a batch fail to publish, the continuation receives every reply of the batch.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/retry_error.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/retry_error.rs#reply"
```

</TabItem>
</Tabs>

### With a continuation that follows the publish

`.then` and `.error` combine on both forms, in either order: `.and_after(..).error(..)` on the outcome, `.then(..).error(..)` or `.error(..).then(..)` on the context.
With both registered, `.then` waits for the publish: it runs once the publish succeeded and never when it failed, and `.error` runs when it failed and never when it succeeded.
Each runs once.
Where the runtime makes no publish for the delivery, the `.then` runs once the delivery is settled.

### The closure and the runtime

With the tokio runtime on, the closure and its future are `Send`.
On the embedded runtime alone they carry no `Send`, so they may hold the local bus's publisher or an `Rc`.
A closure registered on an outcome that never fails costs one allocation, its box; an outcome without `.error` stores and runs nothing for it.
The runtime copies the failed message for the continuation only when one is registered.
