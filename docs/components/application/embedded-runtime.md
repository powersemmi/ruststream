---
sidebar_position: 4
title: Embedded runtime
description: "A device service runs on embassy as one task, and without alloc it keeps headers, codecs, continuations and batches in fixed storage."
---

# Embedded runtime

A device service runs on embassy's executor and clock as one task.
[`#[ruststream::app(runtime = embassy)]`](./app-macro.md) turns the builder function into an [`EmbeddedApp`](api:ruststream::runtime::EmbeddedApp) and an embassy task, `app::task`, that the board's `main` spawns.
The handlers are the ones a server runs, written with `#[subscriber]`.

A device with no heap builds the framework without the `alloc` feature.
That build is a form of its own: it allocates nothing at startup or per message, and it keeps in storage of a fixed size what the heap form keeps on the heap.
The selection is by features: `embassy` picks the runner, `memory` the local bus, `macros` the attributes, `postcard` or `json-core` the codec, and `log` or `defmt` for the logger the app selects.
Cargo features add up, so one crate of the dependency graph that turns `alloc` on turns it on for the whole build.
`tokio`, `json`, `cbor` and `msgpack` turn `alloc` on.

## Example

The dependency line of a device with no heap:

```toml
[dependencies]
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard", "defmt"] }
```

The service reads a sensor's readings from the board's bus, reports them in degrees, and updates a display once each report is acknowledged.

```rust reference="crates/ruststream/tests/doc_device.rs#service"
```

The board declares the bus as a `static` and the app function receives a broker built over it.
The binary supplies the `critical-section` implementation the bus needs.

```rust reference="crates/ruststream/tests/doc_device.rs#bus"
```

## What the build holds

Each part whose size the heap would decide has a fixed form.

| Part | Form without `alloc` |
|---|---|
| Headers of a message | `FixedHeaders` of 8 entries and 256 bytes; `insert` returns an error when the map is full |
| Codec | `SliceCodec`, which encodes into a slice and decodes from one: `PostcardCodec` (`postcard`) and `JsonCoreCodec` (`json-core`) |
| Continuation | held in a room of 128 bytes, aligned to 8 bytes |
| Failures read by `.error` | borrowed from the subscription's fixed buffer: `async move \|failures: &Failures\| ..` |
| `ctx.after(..)`, `ctx.after_ack(..)`, `ctx.after_settle(..)` | a delivery holds four registrations; one more returns an error, and nothing registered is dropped |
| Batch | up to 16 messages, named at the mount with `.batch::<N>()`, of messages that deserialize themselves or that the mount's codec decodes; the outcomes are `Outcomes<HandlerOutcome, N>` |
| Local bus | a `LocalMemoryBus` of fixed size that the board declares as a `static` |
| State | hooks and handlers borrow it: `async \|state: &State\| ..` |

A codec is named at the mount site: `b.with_codec(PostcardCodec)` for the handlers and `.out_reply(policy).codec(PostcardCodec)` for a reply.

## Capacities

A capacity is a const generic or a crate constant, and each is declared at one place.

| Capacity | Where | Value |
|---|---|---|
| Messages, payload bytes and subscriptions of the bus | `LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>` | `PAYLOAD` is 64 and `SUBSCRIPTIONS` is 8 unless named |
| Delayed retries waiting at once | `.retry_capacity::<CAP>()` | a count above zero; on a batch mount at least `N`, and at most 1024 |
| Bytes a reply is encoded into | `.encode_capacity::<N>()` | 256 unless named |
| Workers of a subscription | `.pool::<N>()` or `.keyed::<N>()` | a count above zero |
| Messages of a batch | `.batch::<N>()` | 1 to 16 |
| Continuation | crate constant | 128 bytes |
| Registrations of a delivery | crate constant | 4 |
| Payload kept in a copy of a delivery | crate constant | 256 bytes |

The compiler holds the capacities that a mount names:

- A continuation that captures more than 128 bytes does not build. The error is an overflow in `ROOM - size_of::<Work>()` that prints both numbers. It appears at `cargo build`, not at `cargo check`.
- A continuation aligned to more than 8 bytes does not build either, and the error names the limit. A future that awaits an `embassy-time` timer is aligned to 8 bytes and fits on every core.
- A batch size outside 1 to 16 is an error that names `.batch::<N>()` and the limit.
- An `.encode_capacity::<N>()` past what the reply publisher can carry is an error that names both numbers. A publisher that keeps the payload (`Take`) is handed a buffer of 256 bytes; a publisher that only reads the bytes has no such limit.
- A retry store, a pool or a worker count of zero does not build.
- A batch mount whose `.retry_capacity::<CAP>()` is below its batch size `N` is an error that names both numbers: a batch that retries whole needs one record per element.

A capacity that a run exceeds is a failure the handler hears of:

- A reply that does not fit its buffer is a publish error, `CodecFailure::BufferFull`, and reaches the handler's [`.error`](../subscribers/messages/error.md) like any failed reply.
- A delayed retry that finds the retry store full is the "no free record" error of the same kind.
- A retry or dead-letter copy of a delivery whose payload is larger than 256 bytes cannot be kept, delayed or sent at once, and is a failed publish as well.

## Continuations and retries

A continuation attaches to an outcome with [`and_after`](../subscribers/messages/and-after.md), or to the context with `ctx.after_ack(..)`.
In this build the context calls return a `Result`, and the handler decides what a refusal means.

```rust reference="crates/ruststream/tests/doc_device.rs#retry"
```

The retry store sits in the app's type, so its capacity is part of the registration.
The `.error` closure borrows the failure, so it does not copy the message into the continuation's room.

## Batches

A batch handler takes a slice, and the mount names the size.
The slice holds messages that deserialize themselves, or messages the mount's codec decodes.

```rust reference="crates/ruststream/tests/doc_device.rs#batch"
```

A batch handler that returns one `HandlerOutcome` settles the whole batch with it.
To settle each message on its own, it returns `Outcomes<HandlerOutcome, N>`, one entry per message in the order of the slice, with `N` the size the mount names.

```rust reference="crates/ruststream/tests/doc_device.rs#batch-each"
```

### Retries and dead letters

A batch mount takes the steps of a single-message one: `.retry_capacity::<CAP>()`, `.max_attempts(n).dead_letter(..)` and `.out_retry(..)`.
`retry_after` and dead letters work for each element, whether the handler returns one outcome for the batch or one per message.
The store keeps one record for each element a batch retries at once, so `CAP` is at least `N`.

```rust reference="crates/ruststream/tests/doc_device.rs#batch-retry"
```

### Replies

A batch handler mounted with `reply` publishes one reply for each element, in the order of the slice, and returns them as `Outcomes<Reply, N>`.
A reply the bus refuses is lost together with the replies behind it, and the `.error` continuation receives them; there are at most `N`.

```rust reference="crates/ruststream/tests/doc_device.rs#batch-reply"
```

### Continuations

An outcome that follows the whole batch has one `.error`, which reads every failed copy once, after the last copy is reported.
Its `.then` runs once when at least one element succeeded.
The failures of a batch gather in one place for the mount, so a second batch outcome with an `.error` waits until the first has run its closure.
With `retry_after`, the wait lasts the delay of the first batch's copies.
Outcomes without an `.error`, one outcome per element, and copies that go out at once do not wait.

An outcome for one element carries its own `.then` and `.error`.
Each worker of the mount has one continuation slot, so the continuations of the elements run one after another, in the order of the batch.

### Out slots

A batch handler may take `Out` slots and publish through them as a single-message handler does.
A handler that both replies and takes `Out` is refused, as on a single delivery.

An app with a batch handler that takes `Out` beside a retry store or several other batch routes may pass the compiler's default recursion limit.
Such an app sets `#![recursion_limit = "256"]` in its crate root; every other combination builds on the default limit.

## Diagnostics

The app selects the logger with `.logger(..)` on the `EmbeddedApp`, and nothing is reported until it does.
`tracing` needs the heap, so this build selects `LogLogger` or `DefmtLogger`, whichever feature the build enables; a heap-backed app selects `TracingLogger`.
The reports are the same ones the tokio runner makes: a reply the bus did not take, a continuation that never runs, a retry that found no room.
Each names the subscription and the message type, and the level of the logger's backend decides which show.

```toml
# A host or a board with a logger.
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard", "log"] }
```

## Testing

The harness follows the build: without `alloc` the embedded `TestApp` drives the fixed form, with the mock clock and the same assertions as with a heap.
See [Testing an embedded app](./testing/embedded-app.md).
