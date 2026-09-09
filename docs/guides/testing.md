# Testing

A RustStream service is tested at two levels:

1. **In-process unit tests** drive your real handlers, middleware and codecs with the
   [`TestApp`](#unit-testing-a-service-with-testapp) harness: no server, no docker, no network.
   This is the default path, and it covers handler logic end to end: decode, dispatch, the
   outcome (ack / nack / drop / panic / decode failure), and everything the handler publishes
   downstream.
2. **Integration tests** run against a real broker, gated behind an environment variable, and cover
   the semantics only a real server has: durable consumers, redelivery timers, partitions.

!!! warning "What the in-process transport covers"
    The harness drives a broker's **in-process transport**: it delivers a message to every
    subscriber whose subject matches, runs your handler through the real dispatch path, and records
    the outcome together with any downstream publishes. Storage and redelivery semantics are the
    real server's; check them in the
    [integration suite](#integration-tests-against-a-real-broker).

    `MemoryBroker` has its own page: [the memory broker](../brokers/memory.md).

## Unit-testing a service with `TestApp`

`TestApp` takes a built `RustStream` application, connects its brokers, mounts the handlers, and
records every delivery. Connecting the in-process bus does no I/O. You publish an input. The
publish drives the whole reaction to completion before it returns: the handler, its downstream
publishes, any cross-broker cascade. Then you assert.

The handler under test (in a real service it lives in your handler module and the test imports it):

=== "Macros"

    ```rust
    --8<-- "tests/doc_testing_memory.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_doc_testing_memory.rs:handler"
    ```

The test:

=== "Macros"

    ```rust
    --8<-- "tests/doc_testing_memory.rs:test"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_doc_testing_memory.rs:test"
    ```

!!! info "This test runs in this repository's CI"
    The code above comes from
    [`tests/doc_testing_memory.rs`](https://github.com/powersemmi/ruststream/blob/main/tests/doc_testing_memory.rs),
    and `cargo test --all-features` runs it on every change, so the example cannot rot unnoticed.

Enable the `testing` feature in your dev-dependencies:

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["testing", "memory", "macros", "json"] }
```

### Addressing brokers

`tb.broker::<MemoryBroker>()` addresses the broker by type. `tb.broker_named("ingress")` addresses
it by the label from [`with_broker_labeled`](asyncapi.md), for a service that mounts several
brokers with colliding subjects. In a single-broker app you can leave the broker unnamed:
`tb.message(&value).to(name)` works without it, and returns `TestError::Ambiguous` when more than
one broker is registered.

Input goes in through the same publish builder the service publishes through. `message(&value)`
publishes a `#[derive(Outgoing)]` value to the destination its type declares, `with_headers(&meta)`
attaches a typed header contract, and `to(name)` names the subject when the value's type does not.

Bytes that are not a model go through that same entry, wrapped in a
`#[derive(Outgoing, Serialized)]` newtype. That is how you inject an undecodable payload for a
decode policy, or the input of a handler that
[deserializes the bytes itself](subscribers.md#raw-subscribers): the test names what it injects.

### Asserting on a handler

`tb.broker::<B>().subscriber(name)` returns an assertion builder over what that handler received:

| Method | Asserts |
|---|---|
| `assert_called_once()` / `assert_called(n)` / `assert_not_called()` | the call count |
| `with(&value)` | the most recent call's sole delivery decodes to `value` (with the default codec) |
| `with_raw(bytes)` | the most recent call's sole raw payload |
| `settled(HandlerOutcome::ack())` | how everything the most recent call carried settled |
| `assert_batch_sizes(&[2, 1])` | the batches the body was handed, in arrival order |
| `assert_outcome(Outcome::Drop)` | the classified outcome (ack / nack / drop / decode-failure / panic) |
| `panicked()` | the handler panicked on the most recent call |
| `assert_last_failed_to_decode()` | the payload did not decode |

These assertions count the handler CALL, not the message. A single-message handler is called on
every delivery, so a call and a message are one and the same. A batch handler is called once per
batch: `assert_called_once()` means one batch of any size, `settled(..)` covers every element in
it, and `received_raw()` lists the elements one by one.

`with` and `with_raw` name a single expected payload, so on a batch the assertion does not hold and
reports the batch size. An element the decode policy rejected before the body ran is settled by
that policy, and it is not in the batch the handler saw.

The broker decides where the batches fall, in answer to the
[`batch(n)`](subscribers.md#batch-subscribers) the mount named, and `assert_batch_sizes` is what
shows them: a log of three replayed under `batch(2)` reaches the body as `[2, 1]`. The same run on
a single-message handler reports `[1, 1, 1]`.

!!! note "Filling a batch with more than one element"
    `tb.message(&value).publish()` drives the whole reaction to completion before it returns, and
    a settled reaction closes the batch of a broker that assembles its batches on the client.
    Publishing one message per call therefore produces one batch per message, each holding a single
    element, whatever size the mount named. Take a publisher handle off the broker before the app
    is built and publish the whole run through it: nothing settles on the way. Then drive the
    reaction to completion once, with `tb.settle()`. A broker that batches natively is unaffected:
    there the broker decides where a batch ends.

`tb.broker::<B>().published::<T>(name)` reads the broker's publish log and asserts on what the
handler published downstream: `.assert_called_once()` / `.assert_called(n)` /
`.assert_not_called()` pin the count, `.with(&Receipt { id: 1 })` / `.with_raw(bytes)` the most
recent payload, and `.with_header("x-app", b"1")` a header that a publish middleware or a
[`PublishTransform`](publishing.md) added.

The messages themselves are available too, when you want a check of your own:
`subscriber(name).received::<T>()` / `.received_raw()` returns what the handler received, and
`published::<T>(name).decoded()` / `.messages()` returns every message published to the channel.
Both lists are in arrival order.

Two more views keep what a flat list drops. `subscriber(name).batches::<T>()` / `.batches_raw()`
group the deliveries by CALL, one inner vector per call: the test sees how the stream was cut into
batches, a boundary `received::<T>()` erases. `subscriber(name).outcomes()` returns the classified
outcome of every call in order, which is what you compare a redelivery sequence against (a nack,
then the ack on the redelivery), while `settled(..)` and `assert_outcome(..)` read the most recent
call only.

The decoding methods (`with`, `received`, `decoded`) use the default codec. If a handler or
publisher was mounted with a different codec (`with_broker_codec`, `Router::with_codec`), pass it
explicitly through the `_with` / `with_codec` variants:
`subscriber(name).with_codec(&CborCodec, &expected)`, `.received_with(&CborCodec)`,
`published::<T>(name).with_codec(&CborCodec, &expected)`, `.decoded_with(&CborCodec)`.
`with_raw` / `received_raw` / `messages` use no codec.

### A message that serializes itself

A value on a [byte lane](codecs.md#binary-protocols-are-not-codecs) is published without a codec,
and every typed assertion above uses one: `with(&value)`, `received::<T>()` and their
`_with(codec)` variants all decode, and a `Serialized` / `Deserialized` type resolves no codec at
all. A test on this lane rests on the two codec-free assertions: `with_raw(bytes)` for the payload,
`received_raw()` for reading a delivery back. The type's own format supplies the rest:

```rust
--8<-- "tests/self_serialising.rs:assertions"
```

The expected bytes come from the format, not from the harness: a hand-rolled frame is short enough
to write out in full, and a generated message produces its own bytes, so a `prost` message is
`with_raw(&order.encode_to_vec())`. You read a delivery back with `Deserialized::from_payload` over
the owned `Bytes` that `received_raw()` returns, the same reader that parsed the input, so the
assertion is against the model type and still without a codec. The publish side is the same:
`published::<T>(name).with_raw(bytes)` and `.messages()` use no codec, `.with(&value)` and
`.decoded()` do.

### Asserting on Out slots

A handler's [`Out` slot](publishing.md#named-slots) identifies it in a test as well.
`tb.out::<Marker>()` returns exactly the messages published through that injected publisher,
destinations and headers included, across every broker. The assertions are the same as on
`published`: `assert_called_once`, `with_raw`, `messages`; for the typed `with`, chain
`.decoded_as::<T>()`. The slot only adds attribution: the broker's per-channel publish log sees the
same messages.

```rust
--8<-- "tests/out_slots.rs:slot_capture"
```

Publishes that leave the handler task (a spawned sibling task, the buffer of a settled owned
transaction) are not attributed to the slot: assert on the broker's publish log for those.

### Failure policy, panic, and shutdown

The harness runs dispatch under the application's real `FailurePolicy`, so a negative test is a
full scenario here. Under the default `panic = fail_fast`, a handler panic shuts the service down
exactly as it does in production:

```rust
--8<-- "tests/testing_harness.rs:panic"
```

Under `on_failure(panic = skip)` the panic settles with an ack, consumption continues, and
`tb.assert_running()` holds. `run_result()` returns what the real [`run`](lifespan.md) would
return: `Ok` while the service is running, and an error once a fail-fast failure has stopped it.

!!! note "Panic catching needs unwinding"
    The harness relies on the runtime's `catch_unwind`, so a deliberate panic does not kill the
    test thread. A build compiled with `panic = "abort"` cannot catch a handler panic.

### Delayed redelivery (`retry_after`)

A handler that returns `retry_after(delay)` schedules a delayed redelivery. `publish` records the
immediate `NackAfter` outcome and returns; you drive the redelivery separately, by advancing a
paused clock:

=== "Macros"

    ```rust
    --8<-- "tests/testing_harness.rs:retry_after"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_testing_harness.rs:retry_after"
    ```

## Integration tests against a real broker

Put behaviour that depends on real broker semantics in a separate suite, and gate it behind an
environment variable. The plain `cargo test` then stays fast and needs no network:

<!-- inline-rust: integration-test skeleton with a pseudocode body; it drives a real NatsBroker (external crate) behind an env gate, so it has no compiled home here -->
```rust title="tests/integration_nats.rs"
fn test_url() -> Option<String> {
    std::env::var("NATS_TEST_URL").ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_consumer_resumes_after_restart() {
    let Some(url) = test_url() else {
        eprintln!("skipping: set NATS_TEST_URL to run");
        return;
    };
    // connect NatsBroker::new(url), drive the real JetStream consumer ...
}
```

Run it explicitly against a live server:

```bash
docker run -d -p 4222:4222 nats:latest -js
NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --test integration_nats
```

Handler logic is checked on the in-process path, broker semantics on the real server. Keep both
suites over the same handler modules, so the production code has a single source of truth.

!!! note "Writing a broker crate?"
    The in-process transport and the `TestableBroker` contract that let `TestApp` work against a
    broker are the broker author's job. They are described in
    [Broker authors: test support](../broker-authors/index.md#test-support) and
    [Conformance](../broker-authors/conformance.md).
