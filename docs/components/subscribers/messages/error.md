---
sidebar_position: 5
title: error
description: "error tells the handler when a publish the runtime makes for it fails: a retry copy, a dead-letter copy or a reply."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# error

[`error`](api:ruststream::runtime::HandlerOutcome::error) tells the handler when a publish the runtime makes on its behalf fails.

Three publishes are the runtime's: the delayed copy of a [`retry_after`](./retry-after.md), the copy that goes to a declared [dead letter](../retry-cap.md), and the reply of an [`out_reply`](../publishing/replies.md).
A publish in the handler body returns a `Result`; for these three, `.error` gives the handler the same control.

The continuation receives [`Failures`](api:ruststream::runtime::Failures), one [`Failed`](api:ruststream::runtime::Failed) per message that did not go out.
`error()` is the cause, the publisher's own error: downcast it to the broker's error type to tell a transient failure, such as a refused connection, from a permanent one, such as a message the broker rejected.
`message()` is the message the runtime tried to send, with its destination, bytes and headers.
It is a [`Serialized`](api:ruststream::Serialized) value, so another `Out` republishes it as it was.

The continuation runs once, after the delivery is settled.
The handler never runs again because a runtime publish failed.

Without `.error`, the failure is logged with the subscription, the destination, the message type and the error, the delivery stays acknowledged, and the message is lost.

On a subscription whose broker moves deliveries itself ([`BrokerMoves`](api:ruststream::BrokerMoves)), the runtime publishes no copy, so the continuation never runs.
Each outcome that carries `.error` there logs a warning naming the subscription.

`.error` combines with [`and_after`](./and-after.md) on an outcome and with [`then`](./hooks.md) on the context: the success half runs once the publish succeeded, `.error` once it failed.
The embedded runtime's forms, rooms and costs are in the rustdoc of [`HandlerOutcome::error`](api:ruststream::runtime::HandlerOutcome::error) and [`After::error`](api:ruststream::runtime::After::error).

## Example

### On the outcome

The handler retries once at its cap, so the retry becomes a dead letter, and the broker refuses the dead-letter copy.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream/examples/retry_error.rs#error"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream/examples/manual/retry_error.rs#error"
```

</TabItem>
</Tabs>

### A batch

A batch handler that returns one outcome per element attaches `.error` to each; its `Failures` holds that element's failure.
A `#[subscriber]` batch body that returns a single `HandlerOutcome` runs its `.error` once, with the failure of every element whose copy failed, in the batch's order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream/examples/retry_error.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream/examples/manual/retry_error.rs#batch"
```

</TabItem>
</Tabs>

### A reply

A reply has no outcome to carry `.error`, so the handler registers it on the context: [`ctx.after(outcome).error(..)`](api:ruststream::runtime::After::error).
When the replies of a batch fail to publish, the continuation receives every one of them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream/examples/retry_error.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream/examples/manual/retry_error.rs#reply"
```

</TabItem>
</Tabs>
