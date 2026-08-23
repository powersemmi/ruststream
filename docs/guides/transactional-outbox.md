# Transactional outbox

Publishing an event and writing the row it describes are two operations, and a crash between them
leaves the system inconsistent: an order recorded with no event, or an event for an order that
rolled back. The outbox closes that gap by making the event part of the write, and moving it to the
broker afterwards.

The pattern is not specific to HTTP; it applies wherever a publish has to agree with a database
write. The example below drives it from an axum endpoint, which is the common case, and the full
compiled source lives at
[`examples/http_outbox.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/http_outbox.rs):

```text
cargo run --example http_outbox --features macros,memory,json
```

## Recording the event beside the write

Instead of publishing on the request path, the endpoint records the event next to the business
write, atomically. A relay then moves recorded events to the broker:

```rust
--8<-- "examples/http_outbox.rs:event"
```

```rust
--8<-- "examples/http_outbox.rs:store"
```

The endpoint only writes to the store. Recording the order and queueing its event is one atomic
step, and no broker I/O can fail or stall the response:

```rust
--8<-- "examples/http_outbox.rs:endpoint"
```

## Draining the outbox

A background task drains the outbox into the broker. A row is removed only after its publish
succeeds, so a broker outage delays events instead of losing them; a crash between the publish and
the removal re-publishes the row on restart. Consumers therefore see at-least-once delivery, the
usual contract of an outbox, and handle duplicates the same way they handle redeliveries from the
broker itself:

```rust
--8<-- "examples/http_outbox.rs:relay"
```

With a real database the `Store` is a table plus an `outbox` table written in one SQL
transaction, and the relay reads `outbox` rows in insertion order, publishes, and deletes them.
Everything else stays as shown: the broker, the publisher, and the subscriber do not know the
outbox exists.
