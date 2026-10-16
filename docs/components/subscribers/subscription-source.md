---
sidebar_position: 2
title: Subscription source
description: "A subscriber reads a subscription named by a string or by the broker's descriptor, fixed in the attribute or left to the mount site."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscription source

A subscriber reads a subscription named by a string or by the broker's descriptor, fixed in the attribute or left to the mount site.

## Example

The bare attribute leaves the name to the mount site.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/subscriber_source.rs#name_at_mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/subscriber_source.rs#name_at_mount"
```

</TabItem>
</Tabs>

## The broker's descriptor

A descriptor is fixed in the attribute, or only its kind is, and the name comes at the mount site.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/subscriber_source.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/subscriber_source.rs#descriptor"
```

</TabItem>
</Tabs>

## How it works

The attribute fixes the kind of subscription and may leave its value to the mount site:

| Attribute | Kind | Value |
|---|---|---|
| `#[subscriber("orders")]` | by name | fixed here |
| `#[subscriber]` | by name | `.name(..)` at the mount site |
| `#[subscriber(MemorySource::new("orders"))]` | the broker's descriptor | fixed here |
| `#[subscriber(MemorySource)]` | the broker's descriptor | `.name(..)` at the mount site |

A name works on any broker that implements `Subscribe`, which opens its default subscription kind for that name.
A descriptor is a type of the broker crate that carries the broker's own settings, such as a consumer group or a durable name.
The macro reads the descriptor's type off the constructor call, so the expression ends in `Type::new(..)` and every step after it returns `Self`.
A source left open must be named: a mount without `.name(..)` does not compile.
`.name(..)` takes any string, so the name may come from configuration read before the app is built.
A descriptor kind that needs more than a name, such as a Pulsar topic with its subscription, is written out in full in the attribute.
In the manual form the first argument of `subscriber` is the source: a string, a descriptor, or `Unnamed::<Kind>::new()` for a source the mount site names.

## See also

- [`#[subscriber]`](./index.md): the handler the source belongs to.
- [Mount-site settings](./mount-settings.md): the other settings `include` can chain.
- [Routers](./router.md): mounting a module's handlers as one group.
- [`Subscribe`](api:ruststream::Subscribe), [`Name`](api:ruststream::Name), [`Unnamed`](api:ruststream::Unnamed), [`FromName`](api:ruststream::FromName), [`SubscriptionSource`](api:ruststream::SubscriptionSource), [`MemorySource`](api:ruststream::memory::MemorySource), [`SubscriberSettings::name`](api:ruststream::runtime::SubscriberSettings::name)
