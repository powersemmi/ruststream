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

The continuation receives the cause and the [`FailedMessage`](api:ruststream::runtime::FailedMessage): its destination, its bytes and its headers.
It returns nothing, like [`and_after`](./and-after.md), and runs once the delivery is settled.
Whatever the reaction needs (an `Out` publisher, state, a counter) the handler moves into the closure.

The handler never runs again because a runtime publish failed.
The delivery is settled before the copy is published, so a failed copy cannot make the handler repeat its side effects or run past its `max_attempts` cap.
A failed reply no longer requeues the delivery either.

`.error` runs for every cause: the network, a broker that is down, full or overloaded, a broker that rejected the message, a codec or a publish transform that failed, or, on the embedded runner, a store of retry copies with no free record ([`NoFreeRecordError`](api:ruststream::runtime::NoFreeRecordError)).
The first argument is the cause itself, so the continuation can tell a transient failure from a permanent one.

Without `.error` the failure is logged with the subscription, the destination, the message type and the error, and the copy is lost.

## On the outcome

`HandlerOutcome::error` covers the publishes the runtime makes for that outcome: the delayed retry copy, and the dead-letter copy when the mount's `max_attempts(..).dead_letter(..)` turns this retry into a dead letter at the cap.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

The handler below retries once at its cap, so the retry becomes a dead letter, and the broker refuses the dead-letter copy.

```rust reference="examples/retry_error.rs#error"
```

</TabItem>
</Tabs>

On a subscription whose broker retries and dead-letters natively the runtime publishes nothing, so the continuation never runs.
Each outcome that carries `.error` there logs a warning naming the subscription.

## From the context

The context form covers a delivery whose handler returns no outcome of its own, and the reply of `out_reply`:
[`ctx.after(outcome).error(..)`](api:ruststream::runtime::After::error) names the settlement it stands behind, as [`then`](./hooks.md) does.

```rust
# #[cfg(feature = "json")]
# mod demo {
use ruststream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct QuoteRequest {
    id: u64,
}

#[derive(Outgoing, Serialize)]
#[outgoing(name = "quotes")]
struct Quote {
    id: u64,
}

#[subscriber("prices", reply)]
async fn quote(req: &QuoteRequest, ctx: &mut Context<'_>) -> Quote {
    let id = req.id;
    ctx.after(HandlerOutcome::ack())
        .error(async move |err, _reply| tracing::warn!(id, %err, "the quote was not sent"));
    Quote { id }
}
# }
```

## With a continuation that follows the publish

`.then` and `.error` combine on both forms, in either order: `.and_after(..).error(..)` on the outcome, `.then(..).error(..)` or `.error(..).then(..)` on the context.
With both registered, `.then` waits for the publish: it runs once the publish succeeded and never when it failed, and `.error` runs when it failed and never when it succeeded.
Each runs once.
Where the runtime makes no publish for the delivery, the `.then` runs once the delivery is settled.

## The closure and the runtime

With the tokio runtime on, the closure and its future are `Send`.
On the embedded runtime alone they carry no `Send`, so they may hold the local bus's publisher or an `Rc`.
A closure registered on an outcome that never fails costs one allocation, its box; an outcome without `.error` stores and runs nothing for it.
The runtime copies the failed message for the continuation only when one is registered.
