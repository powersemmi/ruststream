# NATS

[`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) is the NATS broker. It covers
Core NATS subjects and JetStream durable consumers, and ships an in-memory test broker under its
`testing` feature.

```toml
ruststream = { version = "0.3", features = ["macros"] }
ruststream-nats = "0.3"
serde = { version = "1", features = ["derive"] }
```

`NatsBroker::new` is synchronous and does no I/O, so a NATS service is assembled with the same
`#[ruststream::app]` macro as any other broker. The runtime connects the broker once at startup,
before opening subscriptions.

## Core subscription

A `#[subscriber("subject")]` handler binds straight to a NATS subject:

```rust
--8<-- "examples/nats_core.rs:handler"
```

Wire it onto the broker; the `with_broker` / `include` part is identical to the in-memory broker.

```rust
--8<-- "examples/nats_core.rs:app"
```

## JetStream durable consumer

To consume from JetStream instead, override the handler's by-name source with `SubscribeOptions`,
naming the stream and a durable consumer so progress survives restarts. The handler's
`HandlerResult::Ack` acks back to JetStream. `include_on` is the source-override form; the codec
resolves the same way as for `include`.

```rust
--8<-- "examples/nats_jetstream.rs:include_on"
```

The `SubscribeOptions` builder also sits directly in the `#[subscriber(..)]` decorator (the macro
follows the chain), which is what the `nats-js` CLI scaffold generates:

```rust
--8<-- "examples/nats_jetstream.rs:decorator"
```

Beyond `jetstream` and `durable`, the builder carries `queue_group` (Core NATS load balancing),
`filter_subject`, `ack_wait`, `max_ack_pending`, and `deliver_policy`. Incompatible combinations
(for example `queue_group` together with `jetstream`) are rejected with a clear error before any
I/O. See the crate's documentation for connection options and authentication.

## Request-reply

NATS supports request-reply natively, so `NatsPublisher` implements the `RequestReply` capability:
`request(msg, timeout)` publishes with a reply inbox and resolves with the reply message, or fails
with a timeout error when nothing answers in time:

```rust
use std::time::Duration;

use ruststream::{IncomingMessage, OutgoingMessage, RequestReply};

--8<-- "examples/nats_request_reply.rs:request"
```

Any NATS responder answers it: another service, or `nats reply questions 'pong'` from the CLI. The
runnable program is
[`examples/nats_request_reply.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/nats_request_reply.rs) -
it sends the request from an `after_startup` hook, through a publisher built before `connect` (the
lazy-startup contract resolves the connection on first use).

The in-memory test broker implements `RequestReply` too, and additionally hands the request's
reply inbox to handlers as the `reply-to` header, so a responder is testable in-process: read
`ctx.headers().reply_to()` and publish the answer to that subject. Against a real server the
requester side above is fully supported; the reply inbox of an *incoming* request is not yet
exposed to handlers, so the responder end of an RPC pair is today another NATS service.

## Testing

The `testing` feature ships `NatsTestBroker` / `NatsTestClient`: an in-process broker with real
NATS subject matching (`*` and `>` wildcards), header propagation, and request-reply - no
`nats-server`, no docker. See
[Testing against in-memory NATS](../guides/testing.md#testing-handlers-against-in-memory-nats).

JetStream edge cases (durable resume, `ack_wait` redelivery, retention) are not simulated; test
them against a real server, gated behind `NATS_TEST_URL`.

For how this broker implements the contract from the inside, read the
[worked example](../broker-authors/example-nats.md).
