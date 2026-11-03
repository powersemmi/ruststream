---
sidebar_position: 3
title: Local memory broker
description: "LocalMemoryBroker is the in-process bus of the embassy runtime: a queue between the parts of one firmware, with types that are not Send."
---

# Local memory broker

`LocalMemoryBroker` is the in-process bus of the embassy runtime, in `ruststream::memory::local`: a complete broker for a queue between the parts of one firmware, and the reference implementation of the contract for brokers whose types are not `Send`.

It comes with the `memory` and `embassy` features, runs on any executor and reads no clock.
Its constructor does no I/O, like every broker constructor, and a subscription receives what is published after it opened.
`LocalMemorySource` is its subscription descriptor and `LocalMemoryPublish` its publish policy.
A publish to a subscription that holds its capacity is refused with `LocalMemoryError::Full`, and any use of a bus that was shut down returns an error.
The first const parameter of the broker is how many messages one subscription holds, waiting or in its handler's hands; a message in hand keeps its place, so a requeue always finds room.

The bus has a form for each build of the framework.
With the `alloc` feature the bus lives on the heap and the broker is built with `LocalMemoryBroker::new()`.
Without it the bus is a `LocalMemoryBus` of fixed size, which the board declares as a `static`, and the broker is built over it with `LocalMemoryBroker::new(&BUS)`.
`LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>` fixes the messages each subscription holds, the largest payload in bytes (64 unless named), and how many subscriptions are open at once (8 unless named).
A bus that holds no message, no byte or no subscription does not build.
The bus sits behind a critical section, so the binary supplies the `critical-section` implementation.

## Example

The bus of a device with no heap, and a service mounted on it:

```rust reference="tests/doc_device.rs#bus"
```

```rust reference="tests/doc_device.rs#service"
```

The service itself is taught on the [Embedded runtime](../application/embedded-runtime.md) page, and a test drives it with the [embedded `TestApp`](../application/testing/embedded-app.md), whose broker is the same `LocalMemoryBroker`.
