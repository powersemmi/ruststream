# A worked example: a NATS broker

This page follows how the real [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats)
crate implements the contract on top of the [`async-nats`](https://docs.rs/async-nats) client. It is
a complete broker in miniature: a `Broker`, one subscription type that serves both Core NATS and
JetStream behind a single `SubscribeOptions` descriptor, a publisher that forwards headers, and a
native request-reply capability.

!!! note
    Item names track the `async-nats` version you depend on (0.46 here); adapt the few spots noted
    below if the crate's API has moved.

```toml title="Cargo.toml"
[package]
name = "ruststream-nats"
version = "0.1.0"
edition = "2024"

[features]
default = []
testing = ["ruststream/conformance"]

[dependencies]
ruststream = { version = "0.3", default-features = false }
async-nats = "0.46"
bytes = "1"
futures = "0.3"
thiserror = "2"
tokio = { version = "1", features = ["sync", "time"] }
tokio-stream = "0.1"
tracing = "0.1"
```

## Errors

One crate-level enum, variants by source, `#[non_exhaustive]` so new variants are not breaking. The
sources are boxed `std` errors, so the public API does not leak the `async-nats` error types.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::error::Error as StdError;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NatsError {
    #[error("nats connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats publish error: {0}")]
    Publish(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats subscribe error: {0}")]
    Subscribe(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats jetstream error: {0}")]
    JetStream(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats request timed out")]
    RequestTimeout,
    #[error("nats broker is not connected")]
    NotConnected,
    #[error("invalid subscribe options: {0}")]
    InvalidOptions(String),
}
```

## The broker

The client only exists after `connect`, but a publisher is built while the application is being
assembled, before `connect` runs. Share the connection through an `Arc<OnceCell<Client>>` so a
publisher captured early reads the live client once it is set. `new` is synchronous and records only
the address - that is what lets a NATS service compose with the synchronous `#[ruststream::app]`
builder.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::sync::Arc;

use ruststream::Broker;
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct NatsBroker {
    client: Arc<OnceCell<async_nats::Client>>,
    addrs: Option<String>,
}

impl NatsBroker {
    /// Records the address; connects lazily when `Broker::connect` runs. No I/O.
    #[must_use]
    pub fn new(addrs: impl Into<String>) -> Self {
        Self {
            client: Arc::new(OnceCell::new()),
            addrs: Some(addrs.into()),
        }
    }

    /// Eager constructor for callers that already drive their own runtime.
    pub async fn connect(addrs: impl async_nats::ToServerAddrs) -> Result<Self, NatsError> {
        let client = async_nats::connect(addrs)
            .await
            .map_err(|e| NatsError::Connect(Box::new(e)))?;
        Ok(Self::from_client(client))
    }

    /// Wraps an already-connected client (TLS, credentials, custom options).
    #[must_use]
    pub fn from_client(client: async_nats::Client) -> Self {
        Self {
            client: Arc::new(OnceCell::new_with(Some(client))),
            addrs: None,
        }
    }

    /// A publisher sharing this broker's connection cell; resolvable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> NatsPublisher {
        NatsPublisher::new(Arc::clone(&self.client))
    }

    /// The connected client, or `NotConnected` before `connect` ran.
    fn connected(&self) -> Result<async_nats::Client, NatsError> {
        self.client.get().cloned().ok_or(NatsError::NotConnected)
    }
}

impl Broker for NatsBroker {
    type Error = NatsError;

    async fn connect(&self) -> Result<(), Self::Error> {
        self.client
            .get_or_try_init(|| async {
                let addrs = self.addrs.as_deref().ok_or(NatsError::NotConnected)?;
                async_nats::connect(addrs)
                    .await
                    .map_err(|e| NatsError::Connect(Box::new(e)))
            })
            .await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        if let Some(client) = self.client.get() {
            let _ = client.drain().await; // best-effort; never blocks or panics
        }
        Ok(())
    }
}
```

`connect` is idempotent: `get_or_try_init` dials only the first time, and a broker built with
`from_client` finds the cell already filled. `shutdown` does all fallible teardown and never panics,
as the contract requires.

## One subscription for Core and JetStream

Core NATS is fire-and-forget; JetStream is persisted and acknowledged. Rather than two subscriber
types, model both behind a single `SubscribeOptions` descriptor and a single `NatsSubscriber`.
`SubscribeOptions` is the `SubscriptionSource`; the broker dispatches on whether `jetstream(..)` was
called. Each builder method maps onto one keyword of the `#[subscriber(..)]` decorator.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::time::Duration;

pub use async_nats::jetstream::consumer::DeliverPolicy;
use ruststream::SubscriptionSource;

#[derive(Debug, Clone)]
#[must_use]
pub struct SubscribeOptions {
    subject: String,
    queue_group: Option<String>,
    stream: Option<String>, // Some(..) => JetStream
    durable: Option<String>,
    // JetStream tuning, elided here: filter_subject, ack_wait, max_ack_pending, deliver_policy
}

impl SubscribeOptions {
    pub fn new(subject: impl Into<String>) -> Self {
        Self { subject: subject.into(), queue_group: None, stream: None, durable: None }
    }

    /// Core-only load balancing. Rejected together with `jetstream`.
    pub fn queue_group(mut self, name: impl Into<String>) -> Self {
        self.queue_group = Some(name.into());
        self
    }

    /// Switch to a JetStream pull consumer on `stream`.
    pub fn jetstream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Durable consumer name (JetStream only). Without it the consumer is ephemeral.
    pub fn durable(mut self, name: impl Into<String>) -> Self {
        self.durable = Some(name.into());
        self
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub const fn is_jetstream(&self) -> bool {
        self.stream.is_some()
    }

    /// Reject incompatible combinations before any I/O.
    pub fn validate(&self) -> Result<(), NatsError> {
        if self.subject.is_empty() {
            return Err(NatsError::InvalidOptions("subject must be non-empty".into()));
        }
        if self.stream.is_some() && self.queue_group.is_some() {
            return Err(NatsError::InvalidOptions(
                "queue_group is Core NATS only and cannot be combined with jetstream(_)".into(),
            ));
        }
        // ...and reject the JetStream-only fields (durable, ack_wait, ...) when jetstream is unset.
        Ok(())
    }
}

impl SubscriptionSource<NatsBroker> for SubscribeOptions {
    type Subscriber = NatsSubscriber;

    fn name(&self) -> &str {
        self.subject()
    }

    async fn subscribe(self, broker: &NatsBroker) -> Result<NatsSubscriber, NatsError> {
        broker.subscribe(self).await
    }
}
```

Because the `#[subscriber(..)]` macro accepts a builder chain, the whole descriptor sits inline in
the decorator:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("worker"))]
async fn handle(order: &Order) -> HandlerResult {
    HandlerResult::Ack
}
```

By-name subscriptions reuse the same path: implement `Subscribe` by delegating to
`SubscribeOptions::new(name)`, so `#[subscriber("orders")]` works too.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::Subscribe;

impl Subscribe for NatsBroker {
    type Subscriber = NatsSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        NatsBroker::subscribe(self, SubscribeOptions::new(name)).await
    }
}
```

The broker's own `subscribe` validates the options and branches once (`queue_group_ref`,
`stream_ref`, and `durable_ref` are small `pub(crate)` getters returning `Option<&str>`):

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::{self, consumer::pull::Config as PullConfig};

impl NatsBroker {
    pub async fn subscribe(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        opts.validate()?;
        if opts.is_jetstream() {
            self.subscribe_jetstream(opts).await
        } else {
            self.subscribe_core(opts).await
        }
    }

    async fn subscribe_core(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let client = self.connected()?;
        let subject = opts.subject().to_owned();
        let inner = match opts.queue_group_ref() {
            Some(group) => client.queue_subscribe(subject.clone(), group.to_owned()).await,
            None => client.subscribe(subject.clone()).await,
        }
        .map_err(|e| NatsError::Subscribe(Box::new(e)))?;
        Ok(NatsSubscriber::from_core(subject, inner))
    }

    async fn subscribe_jetstream(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let ctx = jetstream::new(self.connected()?);
        let stream_name = opts.stream_ref().expect("validated").to_owned();
        let stream = ctx
            .get_stream(&stream_name)
            .await
            .map_err(|e| NatsError::JetStream(Box::new(e)))?;
        let consumer = stream
            .create_consumer(PullConfig {
                durable_name: opts.durable_ref().map(str::to_owned),
                ..Default::default() // filter_subject, ack_wait, max_ack_pending, deliver_policy
            })
            .await
            .map_err(|e| NatsError::JetStream(Box::new(e)))?;
        let messages = consumer
            .messages()
            .await
            .map_err(|e| NatsError::JetStream(Box::new(e)))?;
        Ok(NatsSubscriber::from_jetstream(opts.subject().to_owned(), stream_name, messages))
    }
}
```

## The subscriber

`NatsSubscriber` wraps either an `async-nats` core subscription or a JetStream pull stream, behind
one `Message` type. `stream` branches with `futures::future::Either` and takes the inner stream out
on first poll, so it is single-use (the contract allows one `stream` call).

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::consumer::pull::Stream as PullStream;
use futures::{Stream, future::Either};
use ruststream::Subscriber;
use tokio_stream::StreamExt;

pub struct NatsSubscriber {
    subject: String,
    kind: SubscriberKind,
}

enum SubscriberKind {
    Core { inner: Option<async_nats::Subscriber> },
    JetStream { inner: Option<Box<PullStream>>, stream_name: String },
}

impl Subscriber for NatsSubscriber {
    type Message = NatsMessage;
    type Error = NatsError;

    fn stream(&mut self) -> impl Stream<Item = Result<NatsMessage, NatsError>> + Send + '_ {
        match &mut self.kind {
            SubscriberKind::Core { inner } => {
                let inner = inner.take().expect("stream called more than once");
                Either::Left(inner.map(|m| Ok(NatsMessage::Core(Box::new(CoreMessage::new(m))))))
            }
            SubscriberKind::JetStream { inner, .. } => {
                let inner = *inner.take().expect("stream called more than once");
                Either::Right(inner.map(|item| match item {
                    Ok(m) => Ok(NatsMessage::JetStream(Box::new(JetStreamMessage::new(m)))),
                    Err(e) => Err(NatsError::JetStream(Box::new(e))),
                }))
            }
        }
    }
}
```

## The message

`NatsMessage` is an enum: a core delivery (no ack) or a JetStream delivery (real ack). Both are
boxed because the wrapped `async-nats` messages are large. `ack`/`nack` on a core delivery return
`AckError::Unsupported` - a non-error the runtime accepts; on JetStream they confirm, with `nack`
mapping to `nak` (redeliver) when the handler asks for it and to `term` (drop a poison message)
when it does not.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::AckKind;
use ruststream::{AckError, Headers, IncomingMessage};

pub enum NatsMessage {
    Core(Box<CoreMessage>),
    JetStream(Box<JetStreamMessage>),
}

impl IncomingMessage for NatsMessage {
    fn payload(&self) -> &[u8] {
        match self {
            Self::Core(m) => &m.inner.payload,
            Self::JetStream(m) => &m.inner.message.payload,
        }
    }

    fn headers(&self) -> &Headers {
        match self {
            Self::Core(m) => &m.headers,
            Self::JetStream(m) => &m.headers,
        }
    }

    async fn ack(self) -> Result<(), AckError> {
        match self {
            Self::Core(_) => Err(AckError::Unsupported),
            Self::JetStream(m) => m.inner.ack().await.map_err(|e| AckError::Broker(box_err(e))),
        }
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match self {
            Self::Core(_) => Err(AckError::Unsupported),
            Self::JetStream(m) => {
                let kind = if requeue { AckKind::Nak(None) } else { AckKind::Term };
                m.inner.ack_with(kind).await.map_err(|e| AckError::Broker(box_err(e)))
            }
        }
    }
}
```

Returning `AckError::Unsupported` (rather than a real error) for core deliveries is exactly what lets
the conformance lifecycle check pass for Core NATS. Each message converts its headers once at
construction; the two converters are the one spot that tracks the `async-nats` version:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use bytes::Bytes;

fn headers_from_nats(map: Option<&async_nats::HeaderMap>) -> Headers {
    let mut headers = Headers::new();
    if let Some(map) = map {
        for (name, values) in map.iter() {
            if let Some(first) = values.iter().next() {
                headers.insert(name.to_string(), Bytes::copy_from_slice(first.as_ref()));
            }
        }
    }
    headers
}

fn headers_to_nats(headers: &Headers) -> Option<async_nats::HeaderMap> {
    if headers.is_empty() {
        return None;
    }
    let mut map = async_nats::HeaderMap::new();
    for (name, value) in headers.iter() {
        if let Ok(text) = std::str::from_utf8(value) {
            map.insert(name, text);
        }
    }
    Some(map)
}
```

## Publishing

The publisher holds the shared connection cell and reads the client on each publish, forwarding
headers when present.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{OutgoingMessage, Publisher};

#[derive(Clone)]
pub struct NatsPublisher {
    client: Arc<OnceCell<async_nats::Client>>,
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let client = self.client.get().cloned().ok_or(NatsError::NotConnected)?;
        let subject = msg.name().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        match headers_to_nats(msg.headers()) {
            Some(headers) => client.publish_with_headers(subject, headers, payload).await,
            None => client.publish(subject, payload).await,
        }
        .map_err(|e| NatsError::Publish(Box::new(e)))
    }
}
```

## Capabilities

NATS supports request-reply natively, so implement `RequestReply` on the publisher and bound the wait
with the caller's timeout, mapping an elapsed timer to `RequestTimeout`.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
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
        let client = self.client.get().cloned().ok_or(NatsError::NotConnected)?;
        let subject = msg.name().to_owned();
        let request = async_nats::Request::new().payload(Bytes::copy_from_slice(msg.payload()));
        let send = async {
            client
                .send_request(subject, request)
                .await
                .map_err(|e| NatsError::Publish(Box::new(e)))
        };
        let reply = tokio::time::timeout(timeout, send)
            .await
            .map_err(|_| NatsError::RequestTimeout)??;
        Ok(NatsMessage::Core(Box::new(CoreMessage::new(reply))))
    }
}
```

Implement only the capabilities the transport supports: Core NATS has no batch subscribe or
transactional publish, so `BatchSubscriber` and `TransactionalPublisher` are left out.
`ruststream-nats` also does not currently implement `DescribeServer`; add it if you want the broker
reported as a server in the AsyncAPI document.

## Wiring it into an app

With the broker in hand, an application looks exactly like any other; nothing about the handlers or
codecs is NATS-specific.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::runtime::{AppInfo, RustStream, TypedPublisher};

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        let replies = TypedPublisher::new(b.broker().publisher());
        b.include_publishing(confirm, replies);
    });
```

## Proving it

Ship a `TestClient` under a `testing` feature that does core routing only (a subject matcher fanning
published messages out to subscribers), then run the conformance suite against it. The test client
must not simulate JetStream cursors, redelivery timers, or retention; those are checked end to end
against a real `nats-server`. See [Conformance](conformance.md).
