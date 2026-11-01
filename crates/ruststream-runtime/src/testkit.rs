//! Fixtures shared by the crate's own unit tests.
//!
//! Some scenarios need the same scaffolding in several modules: a tracing subscriber that records
//! the fields of the events a handler emits, and a few helpers that drive the in-memory broker to
//! produce a batch. They live here once so the copies cannot drift apart.
//!
//! This module is `#[cfg(test)]` and everything in it is `pub(crate)`: nothing here is part of the
//! published surface. The user-facing test harness is [`crate::testing`].

use alloc::format;
use alloc::string::String;
use core::time::Duration;
use std::sync::Mutex;

use tokio::time;
use tokio_util::task::TaskTracker;

use alloc::sync::Arc;

use crate::Publisher;
use crate::host::{Copying, Detach, Halt};
use crate::publish::{PublishIdentity, PublishTransformIdentity};
use crate::redelivery::{RetryLeaf, RetryRoute, bare_retry_publisher};

/// The runner a delivery built by a unit test hands its leftovers to: tasks on the test's own
/// runtime, tracked by `0`, so a test can wait for them or count them.
pub(crate) struct Tracked(pub(crate) TaskTracker);

impl Detach for Tracked {
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        drop(self.0.spawn(work));
    }

    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        drop(self.0.spawn(async move {
            time::sleep(delay).await;
            work.await;
        }));
    }
}

/// `host` carrying the retry route of a registration: copies leave through `publisher`, a bare
/// retry slot, to `destination`.
pub(crate) fn copying<Host, P>(
    host: Host,
    publisher: P,
    destination: &str,
) -> Copying<Host, RetryLeaf<P, PublishTransformIdentity, PublishIdentity>>
where
    P: Publisher + Send + Sync + 'static,
{
    Copying::new(
        host,
        Some(Arc::new(RetryRoute::to(
            bare_retry_publisher(publisher),
            destination,
        ))),
    )
}

/// The shutdown a unit test hands the body: it records the failure a fail-fast raised it with, the
/// first one winning, as the runner's shutdown does.
#[derive(Debug, Default)]
pub(crate) struct Shutdown(Mutex<Option<String>>);

impl Shutdown {
    /// A shutdown nothing has raised yet.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Whether a fail-fast raised it.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.peek_failure().is_some()
    }

    /// The first failure recorded, naming its subscription.
    pub(crate) fn peek_failure(&self) -> Option<String> {
        self.0
            .lock()
            .expect("the failure slot is never poisoned")
            .clone()
    }
}

impl Halt for Shutdown {
    fn fail(&self, subscription: &str, reason: &str) {
        self.0
            .lock()
            .expect("the failure slot is never poisoned")
            .get_or_insert_with(|| format!("{subscription}: {reason}"));
    }

    fn stream_ended(&self, _subscription: &str) {}
}

/// Records the fields of every tracing event emitted while the guard is alive, so a test can
/// assert on a diagnostic's content rather than on its mere existence.
///
/// It compiles where its callers do: with a tracing subscriber and the in-memory broker their
/// modules need.
pub(crate) mod log_capture {
    use alloc::borrow::ToOwned;
    use alloc::format;
    use alloc::string::String;
    use alloc::string::ToString;
    use alloc::vec::Vec;
    use core::fmt::Debug;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, Once};

    use tracing::field::{Field, Visit};
    use tracing::subscriber::DefaultGuard;
    use tracing::{Subscriber, subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt as _};

    /// The captured events, each one a map from field name to its rendered value.
    pub(crate) type Events = Arc<Mutex<Vec<HashMap<String, String>>>>;

    #[derive(Default)]
    struct FieldGrab(HashMap<String, String>);

    impl Visit for FieldGrab {
        // Without this, a string field arrives through `record_debug` and is captured in its
        // quoted Debug form, which no assertion is written against.
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().to_owned(), value.to_string());
        }

        // First writer wins, so a value already recorded in its typed form is not overwritten by
        // a later Debug rendering of the same field.
        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.0
                .entry(field.name().to_owned())
                .or_insert_with(|| format!("{value:?}"));
        }
    }

    struct Capture(Events);

    impl<S: Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut grab = FieldGrab::default();
            event.record(&mut grab);
            self.0.lock().unwrap().push(grab.0);
        }
    }

    /// Installs the capturing subscriber for the current thread. Events are collected until the
    /// returned guard is dropped.
    ///
    /// The process-wide subscriber is an empty registry, interested in every callsite. Without it
    /// the only global dispatcher is the no-op one, and a callsite a test on another thread hits
    /// first, while no capture is installed anywhere, can be cached as never interested: a capture
    /// installed a moment later then misses that event.
    pub(crate) fn start() -> (Events, DefaultGuard) {
        static INTERESTED: Once = Once::new();
        INTERESTED.call_once(|| {
            // Another test binary's global subscriber is not this process's concern: a failure
            // here means one is already installed, which keeps the interest open just as well.
            let _ = subscriber::set_global_default(tracing_subscriber::registry());
        });
        let events: Events = Arc::new(Mutex::new(Vec::new()));
        let guard = subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&events))),
        );
        (events, guard)
    }

    /// Returns the fields of the first captured event whose message is `message`.
    ///
    /// # Panics
    ///
    /// Panics when no such event was captured.
    pub(crate) fn find(events: &Events, message: &str) -> HashMap<String, String> {
        let captured = events.lock().unwrap();
        captured
            .iter()
            .find(|fields| fields.get("message").is_some_and(|m| m == message))
            .cloned()
            .unwrap_or_else(|| panic!("no `{message}` event was emitted"))
    }
}

/// A delivery its broker could not read, for the unit tests of the places that ask
/// [`IncomingMessage::decode_error`](crate::IncomingMessage::decode_error): its payload is
/// whatever the test hands it, and it reports [`REASON`](unreadable::REASON) as its error.
///
/// Its callers read the warning it causes, so it compiles where they do: with a tracing subscriber
/// and the in-memory broker and codec their modules need.
#[cfg(feature = "json")]
pub(crate) mod unreadable {
    use alloc::vec::Vec;
    use core::future::ready;

    use crate::codec::CodecError;
    use crate::{AckError, HeaderMap, IncomingMessage};

    /// What the delivery reports, as the inner error of a [`CodecError::Decode`].
    pub(crate) const REASON: &str = "the column `amount` holds text";

    pub(crate) struct Unreadable {
        payload: Vec<u8>,
        headers: HeaderMap,
        error: CodecError,
    }

    impl Unreadable {
        pub(crate) fn new(payload: &[u8]) -> Self {
            Self {
                payload: payload.to_vec(),
                headers: HeaderMap::new(),
                error: CodecError::Decode(REASON.into()),
            }
        }
    }

    impl IncomingMessage for Unreadable {
        fn payload(&self) -> &[u8] {
            &self.payload
        }

        fn headers(&self) -> &HeaderMap {
            &self.headers
        }

        fn decode_error(&self) -> Option<&CodecError> {
            Some(&self.error)
        }

        fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
            ready(Ok(()))
        }

        fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
            ready(Ok(()))
        }
    }
}

/// Hands a unit test one real delivery of the in-memory broker, for the handler adapters that are
/// driven with a message directly rather than through a dispatch loop.
///
/// It stays below `TestApp` on purpose: its callers test an adapter's handling of one delivery,
/// which `TestApp` never exposes, since it drives the whole dispatch around the adapter.
/// Behaviour a service can observe is tested through the harness.
#[cfg(feature = "json")]
pub(crate) mod delivery {
    use futures::StreamExt;

    use crate::{OutgoingMessage, Publisher, Subscriber};
    use ruststream::memory::{MemoryBroker, MemoryMessage};

    /// Publishes `payload` to `name` and pulls the delivery back off the bus. It drives the
    /// transport itself, so it publishes through the broker SPI rather than the typed builder.
    pub(crate) async fn one_delivery(
        broker: &MemoryBroker,
        name: &str,
        payload: &[u8],
    ) -> MemoryMessage {
        let mut subscriber = broker.subscribe(name);
        broker
            .publisher()
            .publish(OutgoingMessage::new(name, payload), None)
            .await
            .expect("publish failed");
        let mut stream = core::pin::pin!(subscriber.stream());
        stream
            .next()
            .await
            .expect("delivery missing")
            .expect("memory subscriber never errors")
    }
}

/// Drives the in-memory broker for the batch handler tests: publish a few deliveries, then pull
/// them back as one batch.
#[cfg(feature = "json")]
pub(crate) mod batch {
    use alloc::vec::Vec;
    use core::num::NonZeroUsize;

    use futures::StreamExt;

    use crate::{BatchSubscriber, OutgoingMessage, Publisher};
    use ruststream::memory::{LogMode, MemoryBroker, MemoryMessage, MemorySubscriber};

    /// Publishes each number as its JSON encoding, so a `Decoded<u32>` handler sees them.
    pub(crate) async fn publish_numbers<Log: LogMode>(
        broker: &MemoryBroker<Log>,
        name: &str,
        numbers: &[u32],
    ) {
        for n in numbers {
            publish_payloads(broker, name, &[&serde_json::to_vec(n).unwrap()]).await;
        }
    }

    /// Puts payloads on the bus as they are. These tests drive the transport itself, so they
    /// publish through the broker SPI (`Publisher::publish`) rather than the typed builder a
    /// service uses.
    pub(crate) async fn publish_payloads<Log: LogMode>(
        broker: &MemoryBroker<Log>,
        name: &str,
        payloads: &[&[u8]],
    ) {
        let publisher = broker.publisher();
        for payload in payloads {
            publisher
                .publish(OutgoingMessage::new(name, payload), None)
                .await
                .unwrap();
        }
    }

    /// Pulls the next batch off the subscriber, opened wide enough that only what is published
    /// bounds it.
    pub(crate) async fn pull_batch<Log: LogMode>(
        sub: &mut MemorySubscriber<Log>,
    ) -> Vec<MemoryMessage<Log>> {
        let mut stream =
            core::pin::pin!(sub.batches(NonZeroUsize::new(64).expect("64 is nonzero")));
        stream.next().await.unwrap().unwrap()
    }
}
