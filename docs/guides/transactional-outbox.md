# Transactional outbox

Publishing an event and writing the row it describes are two operations. A crash of the process
between them leaves the system inconsistent: an order recorded with no event, or an event for an
order that rolled back. The outbox removes that inconsistency: the event becomes part of the write
and is published to the broker later.

The pattern is not tied to HTTP. You need it wherever a publish has to agree with a database write.
The example below shows it on an axum endpoint, the common case. The full compiled source is
[`examples/http_outbox.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/http_outbox.rs):

```text
cargo run --example http_outbox --features macros,memory,json
```

## Recording the event beside the write

Instead of publishing on the request path, the endpoint records the event next to the business
write. A relay moves the recorded events to the broker:

=== "Macros"

    ```rust
    --8<-- "examples/http_outbox.rs:event"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/http_outbox.rs:event"
    ```

```rust
--8<-- "examples/http_outbox.rs:store"
```

The endpoint writes only to the store. The order and its event are saved in one atomic step. The
request path does no broker I/O, so a broker outage neither delays the response nor turns it into
an error:

```rust
--8<-- "examples/http_outbox.rs:endpoint"
```

## Draining the outbox

The relay runs as a background task and drains the outbox into the broker. It removes a row only
after the publish succeeds, so a broker outage delays events instead of losing them. If the process
crashes between the publish and the removal, the row is published again after restart. Consumers
get at-least-once delivery, the usual contract of an outbox. They handle duplicates the same way
they handle redeliveries from the broker itself:

```rust
--8<-- "examples/http_outbox.rs:relay"
```

With a real database the `Store` is a business table and an `outbox` table written in one SQL
transaction. The relay reads `outbox` rows in insertion order, publishes them, and deletes them.
Nothing else changes: the broker, the publisher, and the subscriber do not know about the outbox.
