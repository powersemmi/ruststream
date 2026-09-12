# Memory

`MemoryBroker`, behind the `memory` feature, is a complete broker that runs inside your process. It
suits a queue that belongs to a single application rather than to a network. The default `cargo
generate` template (`templates/memory`) uses it, so a fresh project runs with no external
dependencies.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
```

<!-- inline-rust: two-line constructor sketch; the broker in context is exercised by every memory-feature example (e.g. quickstart.rs:app) -->
```rust
use ruststream::memory::MemoryBroker;

let broker = MemoryBroker::new();
```

## How much it keeps { #retention }

A broker built with `MemoryBroker::new()` keeps nothing. A message lives from the publish until the
last subscriber has read it, so the memory a long-running service holds is the work its handlers
have yet to do, whatever the message count.

A service that replays needs history, and says how much of it to keep:

```rust
--8<-- "examples/seek.rs:retaining"
```

`Retention` bounds one topic: `Messages(n)` keeps the newest `n` messages of every topic,
`Bytes(n)` keeps the newest payloads that fit in `n` bytes, and `MessagesAndBytes { .. }` applies
both at once. A broker publishing under a thousand topics therefore holds up to that much for each
of them. The newest message always stays, so a payload wider than a byte bound is kept alone rather
than dropped when it arrives.

The two forms are different types: `MemoryBroker::retaining(..)` gives the one whose subscriptions
are `Seekable`, and a mount that opens at a position or reads a seek handle only compiles against
it. Replaying on a broker that keeps nothing is a compile error, not a replay that quietly finds
nothing.

## The prelude a mount site imports { #prelude }

`ruststream::memory::prelude` is this broker's glob, built like the prelude of every broker crate.
It re-exports the core prelude, then the broker's own surface (`MemoryBroker`, `MemorySource`,
`MemoryError`, `MemoryPosition`, `Retention` with the log modes `Discarding` / `Retaining`, and the
context keys `MemoryContext` / `MemoryBatchContext` / `Position` / `SeekHandle`), then the publish
policies under the uniform names `Publish`,
`TransactionalPublish` and `Request`. All three are aliases of `MemoryPublish` and `MemoryRequest`.
This broker's publisher implements both transaction kinds, so `TransactionalPublish` here is the
same policy as `Publish`; on a broker with a separate transactional configuration that name points
to a different policy.

<!-- inline-rust: the import shape; every memory-feature example under examples/ mounts through it -->
```rust
use ruststream::memory::prelude::*;
```

The same glob brings in the capability traits this broker implements (`TransactionalPublisher`,
`OwnedTransactions`, `Transaction`, `RequestReply`, `Positioned`, `Seeker`), so their operations are
in scope wherever the policies are. `Partitioned` stays out: in scope it makes
`msg.partition_key()` ambiguous with the method of the same name on `IncomingMessage`. A service
that reads partition keys imports `Partitioned` itself.

A handler body keeps `use ruststream::prelude::*;`: it names capabilities, never policies, and does
not know which broker runs it. A file holding both a body and its mount site needs the broker glob
alone.

## Semantics

- **Topic names match in full.** A subscription to `orders` receives the messages published to
  `orders`.
- **Fan-out.** Every subscriber of a topic receives every message published to it after the
  subscription.
- **Ack is a no-op; `nack(requeue: true)` redelivers** the same payload to the same subscriber.
- **Shared ownership.** `MemoryBroker` is a reference-counted handle: all its owners work with one
  state, so a clone held by a test sees everything the application publishes.

A handler, its middleware and its decoding behave here as they do against a networked broker: the
runtime dispatches messages through the same path.

## Capabilities

Every capability trait is implemented over this broker's own in-process semantics:

- **Request / reply.** `broker.requester()` gives you a `MemoryRequester`: its `request` publishes
  the message and names a unique in-process reply topic in the `reply-to` header, and completes
  with the first message delivered there. The responder reads `reply-to` from the request and
  publishes its reply to that topic. A request nobody answers returns `RequestError::Timeout`.
  `MemoryRequest` is the policy that constructs `MemoryRequester`, so you bind a slot bound with
  `Out<impl RequestReply, ..>` to `MemoryRequest`.
- **Batches.** `MemorySubscriber` implements `BatchSubscriber`: a batch is the first delivery to
  arrive plus everything already buffered, capped at the size the handler registration named with
  `batch(n)`. A partial batch is delivered immediately.
- **Transactions.** `MemoryPublish` is the policy that constructs `MemoryPublisher`, which
  implements both transaction kinds, so you bind a slot or a wiring bound with
  `TransactionalPublisher` or `OwnedTransactions` to `MemoryPublish`. Publishes inside a transaction
  scope are buffered: `commit` delivers them to every subscriber at once in publish order, `abort`
  discards them. Every owned transaction buffers on its own, and clones of a publisher handle do not
  share its transaction. Out-of-order calls on the publisher itself return `MemoryError`: a second
  `begin_transaction` while one is open returns `TransactionBusy` and leaves the open transaction
  untouched, and a `commit` or `abort` without one returns `NoTransaction`.
- **Partition keys.** `MemoryMessage` implements `Partitioned` and reads the key from the
  `partition-key` header (`memory::PARTITION_KEY_HEADER`).
- **Seeking.** On a [retaining broker](#retention), `MemorySubscriber` implements `Seekable` over
  the per-topic log: get a `MemorySeeker` before reading starts, then call `seek` with a
  `MemoryPosition`, taken from a delivered message with `Positioned::position` (which delivers that
  same message again) or constructed (`MemoryPosition::start()` / `sequence(n)` / `end()`).
  Sequence numbers are absolute and keep naming the same message as the retention bound evicts
  older ones. `start()` is the oldest message still kept and `end()` is the tip, past everything
  published so far. Seeking forward skips the deliveries queued before the target. A sequence the
  bound has already evicted returns `MemoryError::PositionEvicted`, which reports the oldest
  position left. A seek acts on one subscriber instance. Through a handle to a bus that has already
  shut down it returns `MemoryError::ShutDown`. Inside an application, `MemoryContext` holds the position
  of the message and the `MemorySeeker`, and a handler reads them under the `Position` and
  `SeekHandle` keys (see [Seeking](../guides/subscribers.md#seeking)). A batch handler reads
  `MemoryBatchContext`: it holds `SeekHandle` but no `Position`, because a batch spans many
  deliveries.
- **Shutdown.** `MemoryBroker::connect(self)` gives `ConnectedMemoryBroker`, and its `shutdown`
  consumes `self` and returns `ClosedMemoryBroker`, which reports how many subscriber registrations
  the shutdown dropped. After that, a publish, a transaction commit or a request through a handle
  handed out earlier returns `MemoryError::ShutDown` or `RequestError::ShutDown`.

## Subscription source

`ConnectedMemoryBroker` implements `Subscribe`, so `#[subscriber("orders")]` works directly. The
`MemorySource` descriptor names the same subscription, in the form every broker uses. From the
[`routed_service`](https://github.com/powersemmi/ruststream/tree/main/examples/routed_service)
example:

=== "Macros"

    ```rust
    use ruststream::memory::prelude::*;

    --8<-- "examples/routed_service/orders.rs:descriptor"
    ```

=== "Manual"

    ```rust
    use ruststream::memory::prelude::*;

    --8<-- "examples/manual/routed_service_orders.rs:descriptor"
    ```

## For testing

You test an application built on `MemoryBroker` with the [`TestApp`](../guides/testing.md) harness:
build the app, hand it to `TestApp::start`, publish messages, and assert on what the handlers
received and published. [Testing](../guides/testing.md#unit-testing-a-service-with-testapp) walks
through the full pattern.

The harness records what the service publishes for the length of a run, so `published::<T>(..)`
assertions read the same list whichever form of the broker the application was built on. Outside
the harness, reading a broker's log back through `TestableBroker::published` shows what that broker
keeps: everything on a retaining one within its bound, nothing on the default one.
