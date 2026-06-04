# A worked example: a NATS broker

This page implements the contract for Core NATS on top of the
[`async-nats`](https://docs.rs/async-nats) client. It is a complete, realistic broker crate in
miniature: a `Broker`, by-name subscriptions, a publisher, and a native request-reply capability.

!!! note
    Item names track the `async-nats` version you depend on; adapt the few spots noted below if the
    crate's API has moved.

```toml title="Cargo.toml"
[package]
name = "ruststream-nats"
version = "0.1.0"
edition = "2024"

[dependencies]
ruststream = { version = "0.2", default-features = false }
async-nats = "0.38"
bytes = "1"
futures = "0.3"
thiserror = "2"

[dependencies.tokio]
version = "1"
features = ["time"]
```

## Errors

One crate-level error enum, variants by source, `#[non_exhaustive]` so new variants are not a
breaking change.

```rust
use async_nats::{ConnectError, PublishError, SubscribeError};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NatsError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Subscribe(#[from] SubscribeError),
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error("request to {subject} failed or timed out")]
    Request { subject: String },
    #[error("broker is not connected")]
    NotConnected,
}
```

## The broker

The client only exists after `connect`, but a publisher is built while the application is being
assembled, before `connect` runs. Share the connection through an `Arc<OnceCell<Client>>` so a
publisher captured early reads the live client once it is set.

```rust
use std::sync::Arc;

use async_nats::Client;
use ruststream::Broker;
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct NatsBroker {
    url: String,
    client: Arc<OnceCell<Client>>,
}

impl NatsBroker {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: Arc::new(OnceCell::new()),
        }
    }

    /// Returns a publisher sharing this broker's connection. Resolvable before `connect`; the
    /// client is read lazily on the first publish.
    #[must_use]
    pub fn publisher(&self) -> NatsPublisher {
        NatsPublisher {
            client: Arc::clone(&self.client),
        }
    }

    fn client(&self) -> Result<&Client, NatsError> {
        self.client.get().ok_or(NatsError::NotConnected)
    }
}

impl Broker for NatsBroker {
    type Error = NatsError;

    async fn connect(&self) -> Result<(), Self::Error> {
        self.client
            .get_or_try_init(|| async_nats::connect(&self.url))
            .await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        if let Some(client) = self.client.get() {
            // Best-effort flush; the connection closes once the last client clone drops.
            let _ = client.flush().await;
        }
        Ok(())
    }
}
```

`connect` is idempotent: `get_or_try_init` dials only the first time. `shutdown` never blocks or
panics, as the contract requires.

## Receiving

By-name subscriptions come from the `Subscribe` capability. The subscriber wraps the `async-nats`
stream; each delivered message becomes a `NatsMessage`.

```rust
use async_nats::Subscriber as Subscription;
use futures::{Stream, StreamExt};
use ruststream::{Subscribe, Subscriber};

impl Subscribe for NatsBroker {
    type Subscriber = NatsSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let inner = self.client()?.subscribe(name.to_owned()).await?;
        Ok(NatsSubscriber(inner))
    }
}

pub struct NatsSubscriber(Subscription);

impl Subscriber for NatsSubscriber {
    type Message = NatsMessage;
    // The async-nats stream ends rather than yielding per-item errors, so this never fails.
    type Error = std::convert::Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        (&mut self.0).map(|message| Ok(NatsMessage::new(message)))
    }
}
```

A delivered message exposes its payload and headers. Core NATS is fire-and-forget, so ack and nack
are no-ops; see [Core NATS versus JetStream](#core-nats-versus-jetstream).

```rust
use async_nats::{HeaderMap, Message};
use bytes::Bytes;
use ruststream::{AckError, Headers, IncomingMessage};

pub struct NatsMessage {
    message: Message,
    headers: Headers,
}

impl NatsMessage {
    fn new(message: Message) -> Self {
        let headers = convert_headers(message.headers.as_ref());
        Self { message, headers }
    }
}

impl IncomingMessage for NatsMessage {
    fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        Ok(())
    }

    async fn nack(self, _requeue: bool) -> Result<(), AckError> {
        Ok(())
    }
}

/// Copies NATS headers into the framework's `Headers`. Header iteration is the one spot that tracks
/// the async-nats version; take the first value of each name.
fn convert_headers(map: Option<&HeaderMap>) -> Headers {
    let mut headers = Headers::default();
    let Some(map) = map else {
        return headers;
    };
    for (name, values) in map.iter() {
        if let Some(value) = values.iter().next() {
            headers.insert(name.to_string(), Bytes::copy_from_slice(value.as_str().as_bytes()));
        }
    }
    headers
}
```

## Publishing

The publisher holds the shared connection cell and reads the client on each publish.

```rust
use ruststream::{OutgoingMessage, Publisher};

pub struct NatsPublisher {
    client: Arc<OnceCell<Client>>,
}

impl NatsPublisher {
    fn client(&self) -> Result<&Client, NatsError> {
        self.client.get().ok_or(NatsError::NotConnected)
    }
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.client()?
            .publish(msg.name().to_owned(), Bytes::copy_from_slice(msg.payload()))
            .await?;
        Ok(())
    }
}
```

To forward outgoing headers, build an `async_nats::HeaderMap` from `msg.headers()` and call
`publish_with_headers` instead.

## Wiring it into an app

With the broker in hand, an application looks exactly like any other; nothing about the handlers or
codecs is NATS-specific.

```rust
use ruststream::codec::JsonCodec;
use ruststream::runtime::{AppInfo, RustStream, TypedPublisher};

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        let replies = TypedPublisher::new(b.broker().publisher(), JsonCodec);
        b.include_publishing(confirm, JsonCodec, replies);
    });
```

## Capabilities

NATS supports request-reply natively, so implement `RequestReply` on the publisher. Bound the wait
with the caller's timeout.

```rust
use std::time::Duration;

use ruststream::RequestReply;

impl RequestReply for NatsPublisher {
    type Reply = NatsMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let subject = msg.name().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        let request = self.client()?.request(subject.clone(), payload);
        let reply = tokio::time::timeout(timeout, request)
            .await
            .map_err(|_| NatsError::Request { subject: subject.clone() })?
            .map_err(|_| NatsError::Request { subject })?;
        Ok(NatsMessage::new(reply))
    }
}
```

Report the server for AsyncAPI by implementing `DescribeServer`:

```rust
use ruststream::{DescribeServer, ServerSpec};

impl DescribeServer for NatsBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(self.url.clone(), "nats")
    }
}
```

Do not implement capabilities the transport lacks. Core NATS has no batch subscribe or transactional
publish, so `BatchSubscriber` and `TransactionalPublisher` are left out.

## Core NATS versus JetStream

The ack and nack above are no-ops because Core NATS does not acknowledge delivery. A JetStream
variant would carry the acknowledgement: `ack` would call the message's `ack`, and `nack` would map
to `nak` (with the redelivery delay when `requeue` is set). Model JetStream as its own
`SubscriptionSource` and message type, so durable-consumer config lives in the descriptor rather
than in the core.

## Proving it

Ship a `TestClient` under a `testing` feature that does core routing only (a subject matcher fanning
published messages out to subscribers), then run the conformance suite against it. The test client
must not simulate JetStream cursors, redelivery timers, or retention; those are checked end to end
against a real `nats-server`. See [Conformance](conformance.md).
