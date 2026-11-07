---
title: A device service on the local memory broker
description: "A thermometer board runs a RustStream service on the embassy runtime with no heap, and its tests on the host fail on any allocation the framework makes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# A device service on the local memory broker

A thermometer board runs a RustStream service on the embassy runtime with no heap, and tests it on the host under an allocator that fails on any allocation the framework makes.

The shop of the earlier chapters runs on `MemoryBroker` and the tokio runtime.
A firmware has its own pair: the [embedded runtime](../../components/application/embedded-runtime.md) and the [local memory broker](../../brokers/local-memory.md), a queue between the parts of one board.
The subscribers read the same, and the mounts have the same chain form.
This chapter builds a small board: a sensor publishes readings, the board reports them in degrees and raises an alert when one is hot.

## The dependency

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard"] }
serde = { version = "1", default-features = false, features = ["derive"] }
```

`default-features = false` leaves out `tokio` and `json`, and `embassy` brings the embedded runner.
`postcard` is the codec: it encodes into a slice and decodes from one, so the board keeps no buffer on a heap.
Nothing in this list turns the framework's `alloc` feature on, so every part whose size a heap would decide has a fixed form.

## The bus

The board declares the bus as a `static`, and the app function receives a broker built over it.
Each subscription holds four messages, counting the one a handler has in its hands.
The binary supplies the `critical-section` implementation the bus sits behind.

```rust reference="crates/ruststream/tests/doc_device.rs#bus"
```

## A handler and its reply

A reading and its report are plain `serde` types that name where they go.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#messages"
```

</TabItem>
</Tabs>

The handler takes a reading and returns its report.
The reply goes to the destination the report names, so the handler publishes nothing itself.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#reading-handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#reading-handler"
```

</TabItem>
</Tabs>

The app mounts the handler on the board's bus.
`out_reply` publishes the report through the bus, `codec` encodes it with postcard, and `encode_capacity` gives each worker the bytes to encode a reply into, 256 by default.
A reply that does not fit is refused with `CodecFailure::BufferFull`, never cut: the delivery is acknowledged and the failure reaches the handler's `.error` continuation.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#reply-mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#reply-mount"
```

</TabItem>
</Tabs>

## Publishing to another topic

A second handler listens to the reports and raises an alert when one passes fifty degrees.
The alert leaves through an `Out` slot, as in [the shop's notices](./replies.md), and carries the sensor's number as one byte.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#alarm"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#alarm"
```

</TabItem>
</Tabs>

The mount adds the handler and names the publisher behind the slot.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#alarm-mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#alarm-mount"
```

</TabItem>
</Tabs>

On the device, `#[ruststream::app(runtime = embassy)]` puts this function in front of the board's executor.
The [embedded runtime](../../components/application/embedded-runtime.md) page shows the rest of the device surface: retries on the board's clock, continuations and batches.

## Testing it on the host

The tests run on the host, with `std`, through the embedded [`TestApp`](../../components/application/testing/embedded-app.md).
It starts the app that the board runs, unchanged, and the bus is its own in-process mode, so no stand-in broker is needed.
The harness keeps its own bookkeeping (the record of what each subscription received and what each slot published) on the host's heap.
The framework under it stays in the form of the device: the test build does not turn `alloc` on, and the app is polled in its fixed form.

A test binary can prove the service allocates nothing.
It installs `assert_no_alloc::AllocDisabler` as its global allocator, and every allocation the framework makes while the app is polled fails the test.
The harness's own bookkeeping runs where the allocator lets it through.
The guard works in a debug build, which is what `cargo test` builds.

```toml title="Cargo.toml"
[dev-dependencies]
ruststream = { version = "0.8", default-features = false, features = ["testing"] }
assert_no_alloc = "1.1"
futures = "0.3"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device.rs#allocator"
```

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device.rs#device-test"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#allocator"
```

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#device-test"
```

</TabItem>
</Tabs>

The test publishes a reading of 1200 millivolts, which the handler reports as 70 degrees.
It checks that the report reached `reports` and that the alert for sensor 2 left through its slot.
A publish from the test returns once everything it set off has settled, so the checks follow it directly.
The board's clock moves only when the test calls `advance`, as on the paused clock of [the shop's tests](./testing.md).

## Keeping the test in the device's form

Cargo builds a crate once per build, with the union of the features every crate in that build asks for.
A dev-dependency, or any other crate in the test build, that turns the framework's `alloc` feature on turns it on for the service under test too.
Then the test checks the heap form of the framework, and the allocator proves nothing about the device.

The usual cause is a dependency line that keeps the default features, `ruststream = { version = "0.8", features = ["testing"] }`, which brings `json` and `tokio`, and both imply `alloc`.
`alloc`, `tokio`, `compio`, `tracing`, `json`, `msgpack`, `cbor` and `conformance` do the same.
So every `ruststream` line of the service, dev-dependencies included, carries `default-features = false`, and no other crate of the test build asks for one of these features.

The core keeps its own no-alloc builds apart in [`checks/noalloc`](https://github.com/powersemmi/ruststream/tree/main/checks/noalloc), because the dev-dependencies of a crate's own unit tests turn `alloc` on.
Its `just test-no-alloc` recipe lists the feature tree of each build with `cargo tree -e features` and fails if `alloc` appears in it.
The same check fits a service:

```bash
cargo tree -e features,normal,build,dev -i ruststream-core | grep 'feature "alloc"'
```

The command prints nothing while the service stays in the device's form.
