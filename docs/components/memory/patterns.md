---
sidebar_position: 2
title: Patterns
description: "A MemoryPattern subscription reads every name its pattern matches, and the broker's Routing decides who receives a shared match."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Patterns

A [`MemoryPattern`](api:ruststream::memory::MemoryPattern) subscription reads every name its pattern matches, and the broker's [`Routing`](api:ruststream::memory::Routing) decides who receives a shared match.

Patterns use the NATS subject syntax.
A name splits into tokens at `.`.
`*` matches exactly one token, and `>` as the last token matches one or more.
So `orders.*` reads `orders.eu` and `orders.us`, and `orders.>` also reads `orders.eu.created`.
The broker checks a pattern when the subscription opens; a bad one stops the service at startup with `MemoryError::InvalidPattern`, which names it.
A subscription by name reads one name, and a wildcard token in it stops the service with `MemoryError::WildcardName`.
[`MemoryBroker::routing`](api:ruststream::memory::MemoryBroker::routing) sets the rule for a publish that several subscriptions match.
`Routing::EveryMatch`, the default, delivers it to every match, as NATS does: a pattern reads beside the handlers of single names, as an audit trail or a metrics tap.
`Routing::MostSpecific` delivers it to the subscriptions of the exact name, or, where there are none, to the most specific matching pattern: a pattern is the fallback for names without a handler of their own.
Patterns are compared token by token from the left, and at the first difference a literal beats `*`, and `*` beats `>`.
For `orders.eu.created`, `orders.eu.*` beats `orders.*.created`, which beats `orders.>`, which beats `*.eu.created`.
A broker with no pattern subscription routes by the exact name alone and pays nothing for patterns.
A pattern subscription does not [replay](./retention.md), even on a retaining broker.

## Example

`orders.eu` goes to its own handler alone, and the pattern takes every other region.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/memory_patterns.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/memory_patterns.rs#app"
```

</TabItem>
</Tabs>
