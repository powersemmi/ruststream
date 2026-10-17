---
sidebar_position: 9
title: Failures
description: "What a subscriber does when its handler panics or a payload cannot decode, set per subscriber with on_failure."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Failures

What a subscriber does when its handler panics or a payload cannot decode, set per subscriber with [`on_failure`](api:ruststream::runtime::SubscriberSettings::on_failure).

A subscriber carries two policies: one for a handler panic, one for a delivery that cannot decode.
A panic fails fast by default, and a test reads it with [`panicked`](../application/testing/subscriber-assertions/panicked.md): a loud error names the subscription, the service shuts down gracefully, and `run` returns an error, so an orchestrator restarts it.
Fail fast takes effect under an unwinding panic profile; with `panic = "abort"` the process is already gone.
A decode failure drops the delivery by default: one malformed payload does not stop the consumer, and [`assert_last_failed_to_decode`](../application/testing/subscriber-assertions/assert-last-failed-to-decode.md) checks it in a test.
A decode failure is a payload the codec rejects, a [header contract](./typed-headers/index.md) that does not parse, a missing carried value, or a delivery the broker reports unreadable.
Each policy is a [`FailurePolicy`](api:ruststream::runtime::FailurePolicy): `fail_fast`, `drop`, `retry`, `retry_after(delay)` or `skip`.
`drop`, `retry` and `retry_after(delay)` settle the way the handler [outcomes](./messages/handler-outcome.md) of the same name do.
`skip` acks the delivery to move past it; the message is gone and was never handled.
`retry` and `retry_after` bring the delivery back, so a payload that never decodes keeps coming back.
The attribute takes `on_failure(panic = .., decode = ..)`, with either key alone; the manual form takes a [`FailurePolicies`](api:ruststream::runtime::FailurePolicies) value.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/failure_policy.rs#defaults"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/failure_policy.rs#defaults"
```

</TabItem>
</Tabs>

## Move past every failure

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/failure_policy.rs#skip"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/failure_policy.rs#skip"
```

</TabItem>
</Tabs>

## A payload to read later

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/failure_policy.rs#tuned"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/failure_policy.rs#tuned"
```

</TabItem>
</Tabs>
