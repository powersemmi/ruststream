---
sidebar_position: 6
title: Bindable
description: "A handler consumes from one broker and publishes through another, named by a token the target broker mints before registration."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Bindable

A handler consumes from one broker and publishes through another, named by a token the target broker mints before registration.

[`bindable()`](api:ruststream::Broker::bindable) wraps the target broker in a [`Bindable`](api:ruststream::runtime::Bindable).
[`bind(policy)`](api:ruststream::runtime::Bindable::bind) mints a [`Bound`](api:ruststream::runtime::Bound) token, before any scope is built.
The token is a publish policy: it goes where a policy goes, `.out_reply(token)` for a [reply](./replies.md) and `.out(marker, token)` for a [slot](./out-slots.md).
The wrapper is what `with_broker` registers for the target, and startup pairs the token with that broker once it connects.
The example labels its two scopes with `with_broker_labeled`, so a test tells them apart with `broker_named`.
A task beside the service gets a live publisher from a token with [`RunningApp::publisher`](api:ruststream::runtime::RunningApp::publisher), as on the [HTTP](../../application/running/http.md) page.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/cross_broker.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/cross_broker.rs#app"
```

</TabItem>
</Tabs>
