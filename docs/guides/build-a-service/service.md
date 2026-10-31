---
title: The service and its first subscriber
description: "The shop starts as one subscriber that takes every placed order, mounted on the in-memory broker by a small app."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# The service and its first subscriber

The shop starts as one subscriber that takes every placed order, mounted on the in-memory broker by a small app.

A RustStream service is an [application](../../components/application/index.md): a builder that names its brokers and mounts subscribers on them.
A [subscriber](../../components/subscribers/index.md) is an `async fn` whose first parameter is the message, and [`#[subscriber]`](api:ruststream::subscriber) names the topic it reads.
The runtime decodes the payload before the body runs, with JSON by default.
What the body returns settles the delivery: [`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack) tells the broker the message is done.
The [in-memory broker](../../components/memory/index.md) carries messages inside the process, so the shop needs nothing installed.
Chapter 11 swaps it for a networked broker, and the subscribers stay as they are.

## The order

An order is a `serde` type.
The [`Outgoing`](api:ruststream::Outgoing) derive says where an order is published: `orders.placed`.
The shop does not publish orders yet; a test does, and in chapter 10 the HTTP storefront does.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/guide/first.rs#order"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/guide/first.rs#order"
```

</TabItem>
</Tabs>

The manual form writes the two impls the derive generates: the destination, and the headers an order carries (none yet).

## The first subscriber

`accept` reads `orders.placed` and logs each order through `tracing`.
The manual form is a named type with an [`impl Handle<Order>`](api:ruststream::runtime::Handle): `Ok(())` acks, and `Err(outcome)` settles by that outcome.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/guide/first.rs#accept"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/guide/first.rs#accept"
```

</TabItem>
</Tabs>

## The app

[`RustStream`](api:ruststream::runtime::RustStream) is the app; [`with_broker`](api:ruststream::runtime::RustStream::with_broker) registers a broker and mounts subscribers on it with `include`.
The closure returns the chain `include` opens, and each further subscriber is one more `.include(..)` on it.
The builder only records what it is given: the broker connects when the service starts.
The [`#[ruststream::app]`](../../components/application/app-macro.md) attribute turns the builder into `main`, with the `run` and `asyncapi gen` commands.
The manual form writes that `main` by hand: [`run_main`](api:ruststream::runtime::cli::run_main) over the builder.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/guide/first.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/guide/first.rs#app"
```

</TabItem>
</Tabs>

## Run it

```bash
cargo run --example guide_first --features testing,macros,memory,json,logging -- run
```

The `logging` feature prints the service's log to the terminal.
The service connects the broker, opens the subscription and runs until <kbd>Ctrl</kbd>+<kbd>C</kbd>:

```text
INFO ruststream::lifecycle starting service service=shop version=0.1.0 brokers=1 subscribers=1
INFO ruststream::lifecycle broker connected broker="ruststream::memory::MemoryBroker"
INFO ruststream::dispatch subscriber started subscriber=orders.placed input="guide_first::Order"
INFO ruststream::lifecycle service running subscribers=1
```

The rest of the guide grows this file into the shop of [`examples/guide`](https://github.com/powersemmi/ruststream/tree/main/examples/guide).
