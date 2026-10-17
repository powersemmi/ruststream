---
title: Testing the service with TestApp
description: "TestApp runs the shop's own app in process: a test publishes an order and checks what each subscriber did and what each topic received."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing the service with TestApp

[`TestApp`](api:ruststream::testing::TestApp) runs the shop's own app in process: a test publishes an order and checks what each subscriber did and what each topic received.

A test starts the app that `main` runs, unchanged, with [`TestApp::start`](../../components/application/testing/starting-and-stopping/start.md).
The broker connects in process, and every subscription runs on the test's runtime.
A publish from the test returns once everything it set off has settled, so the next line can check the result.
Assertions read two records: what each [subscriber](../../components/application/testing/subscriber-assertions/index.md) received and how it settled, and what was [published](../../components/application/testing/published-assertions/index.md) to each topic.
On a paused clock, [`advance`](../../components/application/testing/driving/advance.md) moves time forward, so a delay of a second or an hour costs nothing.
Testing needs the crate's `testing` feature, usually as a dev-dependency.

## An order from start to finish

The test places an order with its request headers and follows it to the customer.
The payment request travels as CBOR, so the assertion decodes it with [`with_codec`](api:ruststream::testing::PublishedAssertions::with_codec).
`accept` and `dispatch` each send a notice, and `send` acks both.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="main.rs" reference="examples/guide/main.rs#journey"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="main.rs" reference="examples/manual/guide/main.rs#journey"
```

</TabItem>
</Tabs>

## An order that goes no further

An order with no lines stops at `accept`: nothing is charged and no customer is told.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="main.rs" reference="examples/guide/main.rs#empty"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="main.rs" reference="examples/manual/guide/main.rs#empty"
```

</TabItem>
</Tabs>

## A delay, without waiting for it

A provider that takes one charge a second refuses the second request with `Busy`.
[`with_state`](api:ruststream::testing::TestApp::with_state) starts the app with that provider instead of the one `on_startup` opens.
The clock starts paused, and `advance(BACKOFF)` brings the refused request back.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="payments.rs" reference="examples/guide/payments.rs#busy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#busy"
```

</TabItem>
</Tabs>

## The dead-letter topic

A provider that never answers makes every delivery fail.
The fifth delivery goes to `payments.dead`, as it arrived.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="payments.rs" reference="examples/guide/payments.rs#dead_letter"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#dead_letter"
```

</TabItem>
</Tabs>

Run the tests of both forms:

```bash
cargo test --example guide --features testing,macros,memory,json,cbor,asyncapi
cargo test --example manual_guide --no-default-features --features testing,memory,json,cbor,asyncapi
```

The tests in both directories are the same, and they pass against both forms of the shop.
