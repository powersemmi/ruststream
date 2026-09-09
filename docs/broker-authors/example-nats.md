# A worked example: a NATS broker

This page shows how the real [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats)
crate implements the contract on top of the [`async-nats`](https://docs.rs/async-nats) client. It is
a complete broker in miniature: the `Broker` -> `ConnectedBroker` -> `Closed` ladder, one
subscription type for Core NATS and JetStream behind a single `SubscribeOptions` descriptor, a
publisher that forwards headers, and the capabilities the transport has.

Read the page as an illustration of the contract, not as the crate's source: the code below is
trimmed to what each rule of [the contract](index.md) asks for, and the crate itself also has the
options, the tuning and the typed delivery context of a real broker. Item names come from the
`async-nats` API, which changes between releases. A broker crate chooses the client version itself,
and its documentation states it.

```toml title="Cargo.toml"
[features]
default = []
# The in-process test broker users get. The conformance harness is a broker-author tool and stays
# a dev-dependency, not a feature users can turn on.
testing = ["ruststream/testing"]

[dependencies]
ruststream = { version = "0.7", default-features = false }
```

Everything else is the client and its support: `async-nats`, plus `bytes`, `futures`, `thiserror`,
`tokio` and `tracing`.

## Errors

One enum for the whole crate, variants by source, `#[non_exhaustive]` so that new variants are not a
breaking change. Each source is stored as a boxed `std` error, so the `async-nats` error types do
not appear in the public API.

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
    #[error("nats shutdown error: {0}")]
    Shutdown(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats request timed out")]
    RequestTimeout,
    /// A publisher aliasing the connection was used after the broker shut down.
    #[error("nats connection is closed; cannot reach {subject}")]
    Closed { subject: String },
    #[error("invalid subscribe options: {0}")]
    InvalidOptions(String),
}
```

`Closed` names the subject, not just the fact that the connection is gone: an error a service reads
at three in the morning says what it could not reach.

## The broker ladder

`new` is synchronous and records only the address. `connect` consumes `self`, establishes the
connection and returns the connected form: it holds the live client directly, and there is no
"maybe connected" state for its own operations to check. The connected form hands out publishers,
and nothing else does, so a publisher without a connection is not representable.

A publisher can outlive the connection, and types do not rule that out: the question is not the
order of calls, but that several handles refer to one connection. The connection therefore has a
closed flag: `shutdown` sets it before calling `drain`, and every such handle reads the client
through it.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_nats::{Client, ConnectOptions};
use ruststream::{Broker, ConnectedBroker};

/// The live connection, shared by the connected broker and every publisher paired off it.
struct NatsConnection {
    client: Client,
    closed: AtomicBool,
}

impl NatsConnection {
    /// The client, or `Closed` once the broker has shut down. A runtime check because the force
    /// is external: aliased handles outlive the connection, and the ladder can only rule out
    /// misuse through the owner's handle.
    fn live_client(&self, subject: &str) -> Result<&Client, NatsError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(NatsError::Closed { subject: subject.to_owned() });
        }
        Ok(&self.client)
    }
}

#[derive(Debug, Clone)]
#[must_use]
pub struct NatsBroker {
    addrs: String,
    options: ConnectOptions,
}

impl NatsBroker {
    /// Records the address; dials when `Broker::connect` runs. No I/O.
    pub fn new(addrs: impl Into<String>) -> Self {
        Self { addrs: addrs.into(), options: ConnectOptions::default() }
    }

    /// Credentials, TLS, reconnect behaviour: still pure configuration, still no I/O.
    pub fn with_options(mut self, options: ConnectOptions) -> Self {
        self.options = options;
        self
    }
}

impl Broker for NatsBroker {
    type Error = NatsError;
    type Connected = ConnectedNatsBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let client = self
            .options
            .connect(self.addrs.as_str())
            .await
            .map_err(|e| NatsError::Connect(Box::new(e)))?;
        Ok(ConnectedNatsBroker::from_client(client))
    }
}

/// The typed witness that `connect` succeeded: the only value with a publish or subscribe surface.
#[derive(Debug)]
pub struct ConnectedNatsBroker {
    connection: Arc<NatsConnection>,
}

impl ConnectedNatsBroker {
    /// Adopts an already-connected client: the escape hatch for a connection built outside the
    /// framework. Only the plain `NatsBroker` slots into the synchronous app builder.
    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            connection: Arc::new(NatsConnection { client, closed: AtomicBool::new(false) }),
        }
    }
}

impl ConnectedBroker for ConnectedNatsBroker {
    type Error = NatsError;
    type Closed = ClosedNatsBroker;

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        // Marked closed before draining: a publisher aliasing the connection must not slip a
        // message into a connection that is already going away.
        self.connection.closed.store(true, Ordering::Release);
        let client = &self.connection.client;
        let stats = client.statistics();
        client.drain().await.map_err(|e| NatsError::Shutdown(Box::new(e)))?;
        Ok(ClosedNatsBroker {
            messages_sent: stats.out_messages.load(Ordering::Relaxed),
            messages_received: stats.in_messages.load(Ordering::Relaxed),
        })
    }
}

/// The terminal witness: no publish or subscribe surface, just the drained connection's counters,
/// for a shutdown log line or a teardown assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedNatsBroker {
    messages_sent: u64,
    messages_received: u64,
}
```

On the owner's path, consuming `self` rules out a second `connect` and a publish or subscribe after
shutdown. `shutdown` does all the teardown that can return an error, returns the witness, and never
panics. A publisher created earlier returns `Closed` after shutdown instead of publishing
successfully into a closed connection: that is the contract for such handles, and the `lifecycle`
check verifies it.

## One subscription for Core and JetStream

Core NATS is fire-and-forget; JetStream stores messages and requires acknowledgement. One
`SubscribeOptions` descriptor and one `NatsSubscriber` cover both. `SubscribeOptions` is the
`SubscriptionSource`, and the broker picks the branch by whether `jetstream(..)` was called. Each
builder method corresponds to one named parameter of the `#[subscriber(..)]` attribute.

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

impl SubscriptionSource<ConnectedNatsBroker> for SubscribeOptions {
    type Subscriber = NatsSubscriber;

    fn name(&self) -> &str {
        self.subject()
    }

    async fn subscribe(self, connected: &ConnectedNatsBroker) -> Result<NatsSubscriber, NatsError> {
        connected.subscribe(self).await
    }
}
```

The `#[subscriber(..)]` macro accepts a builder chain, so the whole descriptor fits inside the
attribute:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("worker"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

Subscriptions by subject name reuse the same path: you can implement `Subscribe` by delegating to
`SubscribeOptions::new(name)`, and then the `#[subscriber("orders")]` form works too.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::Subscribe;

impl Subscribe for ConnectedNatsBroker {
    type Subscriber = NatsSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_with(SubscribeOptions::new(name)).await
    }
}
```

The connected form's own `subscribe_with` validates the options and branches exactly once
(`queue_group_ref`, `stream_ref` and `durable_ref` are small `pub(crate)` getters returning
`Option<&str>`); it takes the client from the connection, where the closed check runs:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::{self, consumer::pull::Config as PullConfig};

impl ConnectedNatsBroker {
    pub async fn subscribe_with(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        opts.validate()?;
        if opts.is_jetstream() {
            self.subscribe_jetstream(opts).await
        } else {
            self.subscribe_core(opts).await
        }
    }

    async fn subscribe_core(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let client = self.connection.live_client(opts.subject())?;
        let subject = opts.subject().to_owned();
        let inner = match opts.queue_group_ref() {
            Some(group) => client.queue_subscribe(subject.clone(), group.to_owned()).await,
            None => client.subscribe(subject.clone()).await,
        }
        .map_err(|e| NatsError::Subscribe(Box::new(e)))?;
        // Core SUB is written without waiting for the server, so without this round trip a
        // producer on another connection can publish into a subscription the server has not
        // registered yet, and the message is simply lost.
        client.flush().await.map_err(|e| NatsError::Subscribe(Box::new(e)))?;
        Ok(NatsSubscriber::from_core(subject, inner))
    }

    async fn subscribe_jetstream(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let ctx = jetstream::new(self.connection.live_client(opts.subject())?.clone());
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

`NatsSubscriber` wraps either an `async-nats` Core subscription or a JetStream pull stream and hides
both behind one `Message` type. `stream` branches with `futures::future::Either` and takes the inner
stream out on the first poll, so it is single-use: the contract allows exactly one `stream` call.

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

`NatsMessage` is an enum: a Core delivery (no ack) or a JetStream delivery (a real ack). Both
variants are boxed because the wrapped `async-nats` messages are large. `ack` and `nack` on a Core
delivery return `AckError::Unsupported`. That is not an error, and the runtime accepts it. On
JetStream they confirm the delivery, and `nack` maps to `nak` (redeliver) when the handler asks for
it and to `term` (drop a poison message) when it does not.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::AckKind;
use ruststream::{AckError, HeaderMap, IncomingMessage};

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

    fn headers(&self) -> &HeaderMap {
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

The `lifecycle` check from the conformance suite accepts `AckError::Unsupported`, so Core NATS
passes it. Each message converts its headers once, at construction; these two functions are the only
place that depends on the `async-nats` version:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use bytes::Bytes;

fn headers_from_nats(map: Option<&async_nats::HeaderMap>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(map) = map {
        for (name, values) in map.iter() {
            if let Some(first) = values.iter().next() {
                headers.insert(name.to_string(), Bytes::copy_from_slice(first.as_ref()));
            }
        }
    }
    headers
}

fn headers_to_nats(headers: &HeaderMap) -> Option<async_nats::HeaderMap> {
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

The publisher and the connected broker on which the policy instantiated it share ownership of the
connection. On every publish the publisher reads the client through the closed check and forwards
the headers when they are present.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{OutgoingMessage, Publisher};

#[derive(Clone)]
pub struct NatsPublisher {
    connection: Arc<NatsConnection>,
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    /// # Cancel safety
    ///
    /// Core NATS publishing is fire-and-forget: the message is handed to the connection's writer
    /// without waiting for the server. Dropping the future may leave it either sent or unsent.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let client = self.connection.live_client(msg.name())?.clone();
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

NATS has request-reply at the transport level, so you can implement `RequestReply` on the
publisher. The wait is bounded by the caller's timeout, and an elapsed timer maps to
`RequestTimeout`.

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
        let client = self.connection.live_client(msg.name())?.clone();
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

A JetStream pull consumer fetches messages in batches at the protocol level, so `BatchSubscriber`
delivers the transport's own batches instead of emulating them. One stream item is one fetch,
bounded by a batch size and an expiry. An empty fetch is retried, so a batch never arrives empty.
The Core arm of the same subscriber has no batching at the protocol level, so a batch there is
whatever the client has already buffered locally, bounded only by the batch size. A broker that
has neither leaves the capability unimplemented, and its users batch with the client-side
[`buffered`](../guides/subscribers.md#batch-subscribers) adapter instead.

`DescribeServer` puts the broker in the generated AsyncAPI document. It is implemented on the
**unconnected** broker, because the document is generated from a service that has not connected to
anything: the trait reports the configured address. The coordinates the server itself announces (a
cluster route, a discovered peer) are known only after connecting, so they belong on an accessor
of the connected form, not on this trait.

Everything else is left unimplemented, because the transport does not have it. NATS has no
transactions, so there is no `TransactionalPublisher` and no `OwnedTransactions`. There is no
`Seekable` either: a NATS `Seekable` would be built on a JetStream consumer, whose stream is a
replayable log.

## The publish policy

`NatsPublish` is the policy that constructs the publisher `NatsPublisher`. You specify the policy
when registering the handler, and it instantiates the publisher at startup, on the connected
broker. Core NATS publishing has no per-publisher options, because the subject and the headers are
set on every message. The policy here is an empty structure, and `pair` only copies the connection
handle, so it cannot return an error. A broker whose publisher creation can return an error (a
transactional producer) wraps that error with `PairError::new`. Because the plain policy is usable
as is, the connected form also implements `DefaultPublish` (see
[the contract](index.md#publishpolicy)), and a handler with `publish(..)` then compiles without an
explicit publisher.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{DefaultPublish, PairError, PublishPolicy};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct NatsPublish;

impl PublishPolicy<ConnectedNatsBroker> for NatsPublish {
    type Live = NatsPublisher;

    async fn pair(self, connected: &ConnectedNatsBroker) -> Result<Self::Live, PairError> {
        Ok(NatsPublisher { connection: Arc::clone(connected.connection()) })
    }
}
```

## The prelude

A mount site imports the crate's prelude in full: the core prelude, then the broker and its
descriptor, then the policies under the uniform names ([the contract](index.md#broker-prelude)).

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
pub use ruststream::prelude::*;

pub use crate::{NatsBroker, NatsError, NatsSource};
pub use crate::NatsPublish as Publish;

// The capabilities this broker implements on its live values.
pub use ruststream::{Positioned, RequestReply, Seekable, Seeker};
```

## Wiring it into an app

Once the broker is ready, an application looks like any other: nothing in the handlers or the
codecs is specific to NATS.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream_nats::prelude::*;

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        // `Publish` is this crate's publish policy; the runtime pairs it after connect.
        b.include(confirm).out(Reply, Publish::default());
    });
```

## Proving it

Ship an in-process transport under a `testing` feature that does basic routing only: a subject
matcher that delivers a published message to every subscriber of the subject at once. It
implements `TestableBroker` on its connected form, and that type is registered with
`register_testable_broker!`. Run the conformance suite against it. Such a transport must not
simulate JetStream cursors, redelivery timers or retention: those are checked by the end-to-end
suite against a real `nats-server`. See [Conformance](conformance.md).
