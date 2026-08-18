use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;

use super::super::dispatch::Delivery;
use super::super::publish::TypedPublisher;
use super::*;
use crate::codec::JsonCodec;
use crate::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublisher,
    MemorySubscriber,
};
use crate::runtime::Decoded;
use crate::{
    BatchSubscriber, Headers, Name, OutgoingMessage, Publisher, Subscriber, SubscriptionSource,
};

struct Confirm {
    reply_to: &'static str,
    fail_with: Option<HandlerResult>,
    calls: Arc<AtomicUsize>,
}

impl Confirm {
    fn new(reply_to: &'static str) -> Self {
        Self {
            reply_to,
            fail_with: None,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn failing(reply_to: &'static str, result: HandlerResult) -> Self {
        Self {
            fail_with: Some(result),
            ..Self::new(reply_to)
        }
    }

    fn calls(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.calls)
    }
}

impl BatchPublishingDef for Confirm {
    type Input = Decoded<u32>;
    type Injections = ();
    type Reply = u32;
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("orders")
    }

    fn reply_name(&self) -> &str {
        self.reply_to
    }
}

// Ignores the app state, so it is generic over it (mounts on any app).
impl<S: Send + Sync> BatchPublishingCall<S> for Confirm {
    async fn call(
        &self,
        batch: &[u32],
        (): &(),
        _ctx: &mut Context<'_, (), S>,
    ) -> Result<Vec<u32>, HandlerResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(result) = self.fail_with {
            return Err(result);
        }
        Ok(batch.iter().map(|n| n * 10).collect())
    }
}

async fn publish_numbers(broker: &MemoryBroker, name: &str, numbers: &[u32]) {
    let publisher = broker.publisher();
    for n in numbers {
        publisher
            .publish(OutgoingMessage::new(name, &serde_json::to_vec(n).unwrap()))
            .await
            .unwrap();
    }
}

async fn publish_payloads(broker: &MemoryBroker, name: &str, payloads: &[&[u8]]) {
    let publisher = broker.publisher();
    for payload in payloads {
        publisher
            .publish(OutgoingMessage::new(name, payload))
            .await
            .unwrap();
    }
}

async fn pull_batch(sub: &mut MemorySubscriber) -> Vec<MemoryMessage> {
    let mut stream = std::pin::pin!(sub.batches());
    stream.next().await.unwrap().unwrap()
}

#[tokio::test]
async fn transactional_replies_publish_atomically_then_ack() {
    let broker = MemoryBroker::new();
    let mut input = broker.subscribe("orders");
    let mut replies = broker.subscribe("confirmations");

    let handler = BatchPublishingHandler {
        def: Confirm::new("confirmations"),
        codec: JsonCodec,
        publisher: TypedPublisher::with_codec(broker.publisher(), JsonCodec).transactional(),
        pipeline: PublishIdentity,
        injections: (),
        decode: FailurePolicy::Drop,
    };

    publish_numbers(&broker, "orders", &[1, 2]).await;
    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut input).await;
    handler.handle_batch(batch, &mut ctx).await;

    // Both replies are visible after the commit, in order.
    let confirmed = pull_batch(&mut replies).await;
    let payloads: Vec<&[u8]> = confirmed.iter().map(IncomingMessage::payload).collect();
    assert_eq!(payloads, [b"10", b"20"]);
    for msg in confirmed {
        msg.ack().await.unwrap();
    }

    // The acked input batch is not redelivered.
    let mut stream = std::pin::pin!(input.stream());
    assert!(futures::poll!(stream.next()).is_pending());
}

#[tokio::test]
async fn handler_error_publishes_nothing_and_settles_the_batch() {
    let broker = MemoryBroker::new();
    let mut input = broker.subscribe("orders");
    let mut replies = broker.subscribe("confirmations");

    let handler = BatchPublishingHandler {
        def: Confirm::failing("confirmations", HandlerResult::retry()),
        codec: JsonCodec,
        publisher: TypedPublisher::with_codec(broker.publisher(), JsonCodec).transactional(),
        pipeline: PublishIdentity,
        injections: (),
        decode: FailurePolicy::Drop,
    };

    publish_numbers(&broker, "orders", &[1, 2]).await;
    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut input).await;
    handler.handle_batch(batch, &mut ctx).await;

    // Nothing was published, and the whole input batch is back for redelivery.
    let mut reply_stream = std::pin::pin!(replies.stream());
    assert!(futures::poll!(reply_stream.next()).is_pending());
    let redelivered = pull_batch(&mut input).await;
    assert_eq!(redelivered.len(), 2);
    for msg in redelivered {
        msg.ack().await.unwrap();
    }
}

/// A definition filling in nothing but the required items registers without documentation: the
/// trait defaults contribute no description, no schema and no declared outgoing message.
#[test]
fn batch_publishing_def_defaults_register_without_documentation() {
    let def = Confirm::new("confirmations");
    let source = def.source();
    let name = SubscriptionSource::<ConnectedMemoryBroker>::name(&source).to_owned();
    let meta = batch_publishing_metadata(name, &def);

    assert_eq!(meta.name, "orders");
    assert_eq!(meta.input_type, "u32");
    assert_eq!(meta.output_type, Some("u32"));
    assert!(meta.description.is_none());
    assert!(meta.payload_schema.is_none());
    assert!(meta.headers_schema.is_none());
    assert!(meta.message_name.is_none());
    assert!(meta.message_description.is_none());
    assert!(meta.outgoing.is_empty());
    assert_eq!(def.workers(), Workers::sequential());
    assert_eq!(def.failure_policies(), FailurePolicies::default());
}

#[test]
fn handler_debug_hides_the_wiring() {
    let handler = BatchPublishingHandler {
        def: Confirm::new("confirmations"),
        codec: JsonCodec,
        publisher: TypedPublisher::with_codec(MemoryBroker::new().publisher(), JsonCodec),
        pipeline: PublishIdentity,
        injections: (),
        decode: FailurePolicy::Drop,
    };
    assert!(format!("{handler:?}").contains("BatchPublishingHandler"));
}

/// Every element of the batch failing to decode short-circuits: the handler is not invoked at
/// all (an empty slice is not a batch), and no reply is published for it.
#[tokio::test]
async fn a_fully_undecodable_batch_never_reaches_the_handler() {
    let broker = MemoryBroker::new();
    let mut input = broker.subscribe("orders");
    let mut replies = broker.subscribe("confirmations");

    let def = Confirm::new("confirmations");
    let calls = def.calls();
    let handler = BatchPublishingHandler {
        def,
        codec: JsonCodec,
        publisher: TypedPublisher::with_codec(broker.publisher(), JsonCodec),
        pipeline: PublishIdentity,
        injections: (),
        decode: FailurePolicy::Drop,
    };

    publish_payloads(&broker, "orders", &[b"not json", b"also not json"]).await;
    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut input).await;
    assert_eq!(batch.len(), 2);
    handler.handle_batch(batch, &mut ctx).await;

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut reply_stream = std::pin::pin!(replies.stream());
    assert!(futures::poll!(reply_stream.next()).is_pending());
    // Both elements were dropped by the decode policy, so nothing comes back either.
    let mut input_stream = std::pin::pin!(input.stream());
    assert!(futures::poll!(input_stream.next()).is_pending());
}

/// A publisher that fails every publish after the first `succeed_first`, to model a reply
/// publish failing part-way through a batch.
struct HalfwayPublisher {
    inner: MemoryPublisher,
    succeed_first: usize,
    published: AtomicUsize,
}

impl HalfwayPublisher {
    fn new(inner: MemoryPublisher, succeed_first: usize) -> Self {
        Self {
            inner,
            succeed_first,
            published: AtomicUsize::new(0),
        }
    }
}

impl Publisher for HalfwayPublisher {
    type Error = MemoryError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), MemoryError> {
        if self.published.fetch_add(1, Ordering::SeqCst) >= self.succeed_first {
            return Err(MemoryError::ShutDown);
        }
        self.inner.publish(msg).await
    }
}

/// A failed reply publish retries the whole batch instead of losing the replies. With a plain
/// (non-transactional) publisher the replies published before the failure stay visible, so the
/// redelivery republishes them: at-least-once, as documented.
#[tokio::test]
async fn a_failed_reply_publish_retries_the_whole_batch() {
    let broker = MemoryBroker::new();
    let mut input = broker.subscribe("orders");
    let mut replies = broker.subscribe("confirmations");

    let handler = BatchPublishingHandler {
        def: Confirm::new("confirmations"),
        codec: JsonCodec,
        publisher: TypedPublisher::with_codec(
            HalfwayPublisher::new(broker.publisher(), 1),
            JsonCodec,
        ),
        pipeline: PublishIdentity,
        injections: (),
        decode: FailurePolicy::Drop,
    };

    publish_numbers(&broker, "orders", &[1, 2]).await;
    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut input).await;
    handler.handle_batch(batch, &mut ctx).await;

    let published = pull_batch(&mut replies).await;
    let payloads: Vec<&[u8]> = published.iter().map(IncomingMessage::payload).collect();
    assert_eq!(payloads, [b"10"]);
    for msg in published {
        msg.ack().await.unwrap();
    }

    let redelivered = pull_batch(&mut input).await;
    assert_eq!(redelivered.len(), 2);
    for msg in redelivered {
        msg.ack().await.unwrap();
    }
}

/// Captures the fields of the events emitted while the guard is alive (needs a tracing
/// subscriber, hence the `logging` feature gate).
#[cfg(feature = "logging")]
mod log_capture {
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex};

    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt as _};

    pub(super) type Events = Arc<Mutex<Vec<HashMap<String, String>>>>;

    #[derive(Default)]
    struct FieldGrab(HashMap<String, String>);

    impl Visit for FieldGrab {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

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

    pub(super) fn start() -> (Events, DefaultGuard) {
        let events: Events = Arc::new(Mutex::new(Vec::new()));
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&events))),
        );
        (events, guard)
    }

    pub(super) fn find(events: &Events, message: &str) -> HashMap<String, String> {
        let captured = events.lock().unwrap();
        captured
            .iter()
            .find(|fields| fields.get("message").is_some_and(|m| m == message))
            .cloned()
            .unwrap_or_else(|| panic!("no `{message}` event was emitted"))
    }
}

/// The publish-failure diagnostic names the subscription, the reply channel, the reply type and
/// the broker error, so the retry is attributable from the logs alone.
#[cfg(feature = "logging")]
#[tokio::test]
async fn a_failed_reply_publish_is_logged_with_its_reply_channel() {
    let broker = MemoryBroker::new();
    let mut input = broker.subscribe("orders");

    let handler = BatchPublishingHandler {
        def: Confirm::new("confirmations"),
        codec: JsonCodec,
        publisher: TypedPublisher::with_codec(
            HalfwayPublisher::new(broker.publisher(), 0),
            JsonCodec,
        ),
        pipeline: PublishIdentity,
        injections: (),
        decode: FailurePolicy::Drop,
    };

    publish_numbers(&broker, "orders", &[1]).await;
    let state = ();
    let delivery = Delivery::empty();
    let headers = Headers::new();
    let mut ctx = Context::new("orders", &headers, &state, (), &delivery);
    let batch = pull_batch(&mut input).await;

    let (events, guard) = log_capture::start();
    handler.handle_batch(batch, &mut ctx).await;
    drop(guard);

    let failure = log_capture::find(&events, "batch reply publish failed");
    assert_eq!(
        failure.get("subscription").map(String::as_str),
        Some("orders")
    );
    assert_eq!(
        failure.get("reply").map(String::as_str),
        Some("confirmations")
    );
    assert_eq!(failure.get("reply_type").map(String::as_str), Some("u32"));
    assert!(
        failure
            .get("error")
            .is_some_and(|e| e.contains("shut down")),
        "{failure:?}"
    );

    let redelivered = pull_batch(&mut input).await;
    assert_eq!(redelivered.len(), 1);
    for msg in redelivered {
        msg.ack().await.unwrap();
    }
}
