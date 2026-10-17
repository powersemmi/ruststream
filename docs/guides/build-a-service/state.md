---
title: Shared state and context
description: "The payment client and the mailer open once at startup and reach every subscriber as State; the context tells a subscriber which delivery it holds."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shared state and context

The payment client and the mailer open once at startup and reach every subscriber as `State`; the context tells a subscriber which delivery it holds.

A service shares clients, pools and caches between its subscribers.
The app opens them in [`on_startup`](api:ruststream::runtime::RustStream::on_startup), before the first subscription opens, and an error there stops the startup.
What the hook returns is the app's [state](../../components/application/state/any-struct.md), one value for the whole service.
A subscriber takes one field of it as a [`State<T>`](api:ruststream::runtime::State) parameter.
The [context](../../components/subscribers/context/delivery-context.md) is the per-delivery half: the subscription's name, the headers, the broker's fields of this delivery.
A subscriber takes it as a `&mut Context` parameter, right after the message.

## The state

`Shop` holds the payment client and the mailer, each behind an `Arc` so a clone is cheap.
[`FromRef`](api:ruststream::runtime::FromRef) projects each field out of the state: `State<Arc<Gateway>>` resolves through it.
`open` is the hook the app passes to `on_startup` in chapter 5.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="state.rs" reference="examples/guide/state.rs#state"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="state.rs" reference="examples/manual/guide/state.rs#state"
```

</TabItem>
</Tabs>

The manual form writes the two impls the derive generates, one per field.

## The state and the context in one subscriber

`send` e-mails a notice through the mailer from the state.
A mailbox that does not exist stays that way, so a refused notice is dropped, and the warning names the subscription from the context.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="notifications.rs" reference="examples/guide/notifications.rs#send"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="notifications.rs" reference="examples/manual/guide/notifications.rs#send"
```

</TabItem>
</Tabs>

In the manual form the state type is the last parameter of `Handle`, and the mount checks it against the app's.
The body reads the field with [`from_context`](api:ruststream::runtime::FromContext), which is what the macro does for a `State` parameter.
[Custom extractors](../../components/subscribers/context/from-context.md) are types of your own read the same way.
