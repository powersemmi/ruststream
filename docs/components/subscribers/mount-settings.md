---
sidebar_position: 3
title: Settings
description: "Settings the attribute leaves open are chained where the handler is mounted: name, workers, failure policy, batch size and start position."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Settings

Settings the attribute leaves open are chained where the handler is mounted: name, workers, failure policy, batch size and start position.

Each setting has two spellings: a clause in the attribute, or a method of [`SubscriberSettings`](api:ruststream::runtime::SubscriberSettings) at the mount site.

| At the mount site | In the attribute | What it sets |
|---|---|---|
| `.name(name)` | `#[subscriber("name")]` | the subscription a [source](./subscription-source.md) left open |
| `.workers(n)`, `.workers_by_key(n)` | `workers(n)`, `workers(n, by_key)` | how many deliveries run at once ([dispatch](./dispatch/index.md)) |
| `.threads(n)`, `.threads_by_key(n)` | `threads(n)`, `threads(n, by_key)` | dedicated threads for a handler that computes |
| `.on_failure(policies)` | `on_failure(panic = .., decode = ..)` | what a panic or an undecodable payload does ([failures](./failure-policy.md)) |
| `.batch(n)` | none | the batch size of a `&[T]` handler ([batches](./batches.md)) |
| `.start_at(position)` | `start_at(position)` | where a replayable subscription opens ([`start_at`](./replay/start-at.md)) |

A setting the attribute named is fixed in the definition's type, so naming it again at the mount site does not compile.
The mount site is where a value known only at run time goes: a name from configuration, a pool size per deployment.
Counts are `NonZeroUsize`, and the [`nonzero!`](api:ruststream::nonzero) macro writes one: `nonzero!(4)` compiles and `nonzero!(0)` does not.
`on_failure` takes a [`FailurePolicies`](api:ruststream::runtime::FailurePolicies) value: one [`FailurePolicy`](api:ruststream::runtime::FailurePolicy) for a panic and one for a decode failure.
A subscription takes `workers` or `threads`, not both: they fill one position.
`batch` exists only on a batch handler, and a batch handler does not mount without it.
`start_at` compiles only against a broker whose subscriptions are replayable.
A broker crate adds steps of its own to the same chain, such as a consumer group, in the broker's vocabulary.
In the manual form the same methods chain on `subscriber(source, body)` before `.build()`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/subscriber_settings.rs#settings"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/subscriber_settings.rs#settings"
```

</TabItem>
</Tabs>
