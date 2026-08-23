use std::fmt;
use std::sync::Arc;

use crate::codec::Codec;
use crate::{PublishPolicy, Publisher};

use super::*;

/// Fixtures the in-memory broker cannot express: a value the codec cannot encode, a
/// transactional publisher rigged to fail one step of the protocol, and a policy that
/// refuses to pair.
#[cfg(feature = "json")]
mod fixtures {
    use std::collections::HashMap;
    use std::future::ready;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use thiserror::Error;

    #[cfg(feature = "memory")]
    use crate::memory::{ConnectedMemoryBroker, MemoryTransaction};
    use crate::{OutgoingMessage, Publisher, TransactionalPublisher};
    #[cfg(feature = "memory")]
    use crate::{OwnedTransactions, PairError, PublishPolicy};

    /// A value JSON cannot encode: an object key must be a string, and this map's are
    /// tuples. Encoding one is a real codec failure, not a stubbed one.
    pub(super) fn unencodable() -> HashMap<(u8, u8), u8> {
        HashMap::from([((1, 2), 3)])
    }

    /// The failure the rigged publisher reports.
    #[derive(Debug, Error)]
    #[error("the rigged publisher refused")]
    pub(super) struct RiggedError;

    /// A transactional publisher whose protocol steps fail on demand: the memory broker's
    /// transactions always succeed, so the batch path's error handling needs this.
    #[derive(Debug, Default)]
    pub(super) struct Rigged {
        pub(super) fail_begin: bool,
        pub(super) fail_commit: bool,
        pub(super) fail_abort: bool,
        pub(super) published: AtomicUsize,
        pub(super) aborted: AtomicUsize,
    }

    impl Rigged {
        /// One protocol step, rigged or not.
        fn step(fail: bool) -> Result<(), RiggedError> {
            if fail { Err(RiggedError) } else { Ok(()) }
        }
    }

    impl Publisher for Rigged {
        type Error = RiggedError;

        fn publish(
            &self,
            _msg: OutgoingMessage<'_>,
        ) -> impl Future<Output = Result<(), Self::Error>> {
            self.published.fetch_add(1, Ordering::SeqCst);
            ready(Ok(()))
        }
    }

    impl TransactionalPublisher for Rigged {
        fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
            ready(Self::step(self.fail_begin))
        }

        fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> {
            ready(Self::step(self.fail_commit))
        }

        fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
            self.aborted.fetch_add(1, Ordering::SeqCst);
            ready(Self::step(self.fail_abort))
        }
    }

    // The publisher a broker hands out is opened per call; refusing to open one is how a
    // broker reports that the handle is unusable.
    #[cfg(feature = "memory")]
    impl OwnedTransactions for Rigged {
        type Transaction = MemoryTransaction;

        fn transaction(&self) -> impl Future<Output = Result<Self::Transaction, Self::Error>> {
            ready(Err(RiggedError))
        }
    }

    /// A policy that fails to pair, standing in for a broker whose publisher needs real work
    /// to come alive (a transactional producer initializing).
    #[cfg(feature = "memory")]
    pub(super) struct RefusePairing;

    #[cfg(feature = "memory")]
    impl PublishPolicy<ConnectedMemoryBroker> for RefusePairing {
        type Live = Rigged;

        fn pair(
            self,
            _connected: &ConnectedMemoryBroker,
        ) -> impl Future<Output = Result<Self::Live, PairError>> {
            ready(Err(PairError::from_boxed(Box::from(
                "the policy refused to pair",
            ))))
        }
    }
}

/// Collects the `tracing` messages emitted on this thread: a warning's field values are only
/// evaluated while a subscriber listens, so asserting on one needs a capture in place.
#[cfg(feature = "logging")]
fn capture_events() -> (
    Arc<std::sync::Mutex<Vec<String>>>,
    tracing::subscriber::DefaultGuard,
) {
    use std::sync::Mutex;

    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};

    struct Capture(Arc<Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: LayerContext<'_, S>) {
            struct Grab(Vec<String>);
            impl tracing::field::Visit for Grab {
                fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
                    self.0.push(format!("{}={value:?}", field.name()));
                }
            }
            let mut grab = Grab(Vec::new());
            event.record(&mut grab);
            self.0.lock().unwrap().push(grab.0.join(" "));
        }
    }

    let events = Arc::new(Mutex::new(Vec::new()));
    let guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(Capture(Arc::clone(&events))),
    );
    (events, guard)
}

// A cancelled commit leaves the broker transaction genuinely unsettled, so the scope's
// drop warning must still fire (needs a tracing subscriber, hence the `logging` gate; the
// stub's pending commit is the cancellation window the single-poll memory broker lacks).
#[cfg(all(feature = "json", feature = "logging"))]
#[tokio::test]
async fn cancelled_commit_keeps_the_unsettled_drop_warning() {
    use std::future::{pending, ready};

    use crate::{OutgoingMessage, Publisher, TransactionalPublisher};

    struct PendingCommit;

    impl Publisher for PendingCommit {
        type Error = std::convert::Infallible;

        fn publish(
            &self,
            _msg: OutgoingMessage<'_>,
        ) -> impl Future<Output = Result<(), Self::Error>> {
            ready(Ok(()))
        }
    }

    impl TransactionalPublisher for PendingCommit {
        fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
            ready(Ok(()))
        }

        async fn commit(&self) -> Result<(), Self::Error> {
            pending().await
        }

        fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
            ready(Ok(()))
        }
    }

    let (events, guard) = capture_events();

    let wrapper = TypedPublisher::new(PendingCommit).transactional();
    let scope = wrapper.begin().await.expect("begin failed");
    {
        let mut commit = std::pin::pin!(scope.commit());
        assert!(
            futures::poll!(commit.as_mut()).is_pending(),
            "the stub commit must hold the cancellation window open",
        );
    }
    drop(guard);

    let warned = {
        let captured = events.lock().unwrap();
        captured
            .iter()
            .any(|message| message.contains("transaction scope dropped without commit"))
    };
    assert!(
        warned,
        "a commit cancelled mid-flight leaves the broker transaction unsettled and must \
             keep the drop warning",
    );
}

#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dyn_stack_walks_its_layers_then_the_static_tail() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::StreamExt;

    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;
    use crate::{IncomingMessage, Subscriber};

    /// Adds its weight on every pass, proving order and that each layer ran exactly once.
    struct Mark(Arc<AtomicUsize>, usize);
    impl PublishDynLayer for Mark {
        fn on_publish<'a>(
            &'a self,
            out: &'a mut Outgoing<'a>,
            next: PublishDynNext<'a>,
        ) -> PublishFut<'a> {
            self.0.fetch_add(self.1, Ordering::SeqCst);
            next.run(out)
        }
    }

    let hits = Arc::new(AtomicUsize::new(0));
    let stack = PublishDynStack::new([
        Arc::new(Mark(Arc::clone(&hits), 1)) as Arc<dyn PublishDynLayer>,
        Arc::new(Mark(Arc::clone(&hits), 10)),
    ]);
    assert!(format!("{stack:?}").contains("middleware"));
    let pipeline = PublishStack::new(stack.clone(), PublishIdentity);

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("dyn");
    let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
    let headers = Headers::new();
    let cx = PublishContext::new("dyn", &headers, &());
    publisher
        .publish("dyn", &5_u32, &pipeline, &cx)
        .await
        .expect("publish through the dynamic stack failed");

    assert_eq!(hits.load(Ordering::SeqCst), 11, "each layer must run once");
    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = stream
        .next()
        .await
        .expect("delivery missing")
        .expect("memory subscriber never errors");
    assert_eq!(msg.payload(), b"5", "the static tail must still send");
    msg.ack().await.expect("ack failed");
}

#[test]
fn borrowed_name_is_not_owned() {
    // The macro-reply hot path passes a string literal: it must stay borrowed (no alloc),
    // which is the whole point of the Cow.
    let out = Outgoing::new("orders.created", b"payload".as_slice());
    assert!(matches!(out.name, Cow::Borrowed(_)));
    assert_eq!(out.name(), "orders.created");
    assert_eq!(out.payload(), b"payload");
}

#[test]
fn owned_name_moves_in() {
    let computed = format!("orders.{}", 42);
    let out = Outgoing::new(computed, BytesMut::from(&b"x"[..]));
    assert!(matches!(out.name, Cow::Owned(_)));
    assert_eq!(out.name(), "orders.42");
}

#[test]
fn payload_mutates_in_place() {
    let mut out = Outgoing::new("t", BytesMut::from(&b"body"[..]));
    out.payload_mut().extend_from_slice(b"!");
    assert_eq!(out.payload(), b"body!");

    out.set_payload(b"fresh".as_slice());
    assert_eq!(out.payload(), b"fresh");
}

#[test]
fn set_name_and_headers() {
    let mut out = Outgoing::new("a", b"".as_slice());
    out.set_name("b");
    out.headers_mut().insert("k", "v");
    assert_eq!(out.name(), "b");
    assert_eq!(out.headers().get_str("k"), Some("v"));
}

/// Both cursors report where in the chain a middleware sits: the static one is opaque, the
/// dynamic one counts the middleware still ahead of it.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn the_pipeline_cursors_render_their_position() {
    use std::sync::Mutex;

    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;

    /// Records how the cursor it is handed renders, then continues the chain.
    struct Record(Arc<Mutex<Vec<String>>>);

    impl PublishLayer for Record {
        fn on_publish<'a, N: PublishPipeline, P: Publisher>(
            &'a self,
            out: &'a mut Outgoing<'a>,
            next: PublishNext<'a, N, P>,
        ) -> impl Future<Output = Result<(), BoxError>> + Send + 'a {
            self.0.lock().unwrap().push(format!("{next:?}"));
            next.run(out)
        }
    }

    impl PublishDynLayer for Record {
        fn on_publish<'a>(
            &'a self,
            out: &'a mut Outgoing<'a>,
            next: PublishDynNext<'a>,
        ) -> PublishFut<'a> {
            self.0.lock().unwrap().push(format!("{next:?}"));
            next.run(out)
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let dynamic = PublishDynStack::new([
        Arc::new(Record(Arc::clone(&seen))) as Arc<dyn PublishDynLayer>,
        Arc::new(Record(Arc::clone(&seen))),
    ]);
    let pipeline = PublishStack::new(
        Record(Arc::clone(&seen)),
        PublishStack::new(dynamic, PublishIdentity),
    );

    let broker = MemoryBroker::new();
    let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
    let headers = Headers::new();
    let cx = PublishContext::new("cursors", &headers, &());
    publisher
        .publish("cursors", &1_u32, &pipeline, &cx)
        .await
        .expect("publish through the recorded pipeline failed");

    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        3,
        "every middleware must see a cursor: {seen:?}"
    );
    let (static_cursor, first_dynamic, last_dynamic) = (&seen[0], &seen[1], &seen[2]);
    assert!(
        static_cursor.starts_with("PublishNext"),
        "the static cursor names itself: {static_cursor}",
    );
    assert!(
        first_dynamic.contains("remaining: 1"),
        "the first dynamic middleware still has one ahead of it: {first_dynamic}",
    );
    assert!(
        last_dynamic.contains("remaining: 0"),
        "the last dynamic middleware hands back to the static tail: {last_dynamic}",
    );
}

/// The view a static transform gets: the delivery's channel and headers, plus a Debug form
/// naming the channel (what a user sees when they log the context).
#[test]
fn publish_context_reads_the_originating_delivery() {
    let mut headers = Headers::new();
    headers.insert("correlation-id", "abc");
    let cx = PublishContext::new("orders.created", &headers, &());

    assert_eq!(cx.name(), "orders.created");
    assert_eq!(cx.headers().get_str("correlation-id"), Some("abc"));
    assert!(
        format!("{cx:?}").contains("orders.created"),
        "the delivery channel is what identifies the context in a log line",
    );
}

/// A codec failure is reported by the publish paths instead of reaching the broker, on both
/// the single-message and the batch reply route.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn an_unencodable_reply_stops_both_reply_paths() {
    use fixtures::unencodable;
    use futures::StreamExt;

    use crate::Subscriber;
    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("out");
    let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
    let headers = Headers::new();
    let cx = PublishContext::new("in", &headers, &());

    let single = publisher
        .publish("out", &unencodable(), &PublishIdentity, &cx)
        .await
        .expect_err("the codec cannot encode this reply");
    assert!(
        single.to_string().contains("encode failed"),
        "the codec error must reach the caller: {single}",
    );

    let batched = publisher
        .publish_batch(
            "out",
            &[unencodable(), unencodable()],
            &PublishIdentity,
            &cx,
        )
        .await
        .expect_err("the batch path encodes per reply");
    assert!(
        batched.to_string().contains("encode failed"),
        "the codec error must reach the caller: {batched}",
    );

    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(
        futures::poll!(stream.next()).is_pending(),
        "nothing may reach the broker when encoding fails",
    );
}

/// A plain typed publisher sends the batch's replies one by one, and exposes the codec the
/// batch mounts reuse for decoding.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn a_plain_wiring_publishes_each_reply_and_names_its_codec() {
    use futures::StreamExt;

    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;
    use crate::{IncomingMessage, Subscriber};

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("out");
    let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);
    let headers = Headers::new();
    let cx = PublishContext::new("in", &headers, &());

    publisher
        .publish_batch("out", &[1_u32, 2, 3], &PublishIdentity, &cx)
        .await
        .expect("publishing a batch of replies failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    let mut sent = Vec::new();
    for _ in 0..3 {
        let msg = stream
            .next()
            .await
            .expect("delivery missing")
            .expect("memory subscriber never errors");
        sent.push(msg.payload().to_vec());
        msg.ack().await.expect("ack failed");
    }
    assert_eq!(
        sent,
        vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
        "each reply is published independently, in order",
    );

    let decoded: u32 = ReplyPublisher::<()>::reply_codec(&publisher)
        .decode(b"7")
        .expect("the reply codec decodes what it encodes");
    assert_eq!(decoded, 7);
}

/// The transactional wiring reports the same reply codec as the stack it wraps: the batch
/// mounts read it off either shape to decode the incoming batch.
#[cfg(all(feature = "memory", feature = "json"))]
#[test]
fn the_transactional_wiring_reports_the_reply_codec() {
    use crate::codec::{Codec, JsonCodec};
    use crate::memory::MemoryBroker;

    let broker = MemoryBroker::new();
    let wiring = TypedPublisher::with_codec(broker.publisher(), JsonCodec).transactional();

    let from_wiring: u32 = ReplyWiring::decode_codec(&wiring)
        .decode(b"7")
        .expect("decode through the wiring codec");
    let from_reply: u32 = ReplyPublisher::<()>::reply_codec(&wiring)
        .decode(b"7")
        .expect("decode through the reply codec");
    assert_eq!((from_wiring, from_reply), (7, 7));
}

/// The wiring stacks hide their leaf and codec from Debug: a publisher is not a data type,
/// and its connection must not print.
#[cfg(all(feature = "memory", feature = "json"))]
#[test]
fn the_wiring_stacks_render_without_their_leaf() {
    use crate::codec::JsonCodec;
    use crate::memory::MemoryPublish;

    let typed = TypedPublisher::with_codec(MemoryPublish, JsonCodec);
    assert_eq!(format!("{typed:?}"), "TypedPublisher { .. }");
    assert_eq!(
        format!("{:?}", typed.transactional()),
        "Transactional { .. }",
    );
}

/// A typed publisher over a policy leaf is itself a policy: pairing swaps the leaf for its
/// live form and the codec travels along, so the paired stack publishes.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn a_typed_publisher_over_a_policy_pairs_into_its_live_form() {
    use futures::StreamExt;

    use crate::codec::JsonCodec;
    use crate::memory::{MemoryBroker, MemoryPublish};
    use crate::{Broker, IncomingMessage, Subscriber};

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("paired");
    let connected = broker.clone().connect().await.expect("connect failed");

    let live = TypedPublisher::with_codec(MemoryPublish, JsonCodec)
        .pair(&connected)
        .await
        .expect("pairing a typed publisher over a policy failed");

    let headers = Headers::new();
    let cx = PublishContext::new("in", &headers, &());
    live.publish("paired", &9_u32, &PublishIdentity, &cx)
        .await
        .expect("the paired stack must publish");

    let mut stream = std::pin::pin!(subscriber.stream());
    let msg = stream
        .next()
        .await
        .expect("delivery missing")
        .expect("memory subscriber never errors");
    assert_eq!(msg.payload(), b"9");
    msg.ack().await.expect("ack failed");
}

/// The transactional wiring pairs like the plain one, and its scope holds the publishes back
/// until the commit.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn a_paired_transactional_wiring_scopes_its_publishes() {
    use futures::StreamExt;

    use crate::codec::JsonCodec;
    use crate::memory::{MemoryBroker, MemoryPublish};
    use crate::{Broker, IncomingMessage, Subscriber};

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("scoped");
    let connected = broker.clone().connect().await.expect("connect failed");

    let live = TypedPublisher::with_codec(MemoryPublish, JsonCodec)
        .transactional()
        .pair(&connected)
        .await
        .expect("pairing a transactional wiring failed");

    let mut scope = live.begin().await.expect("begin failed");
    scope
        .publish("scoped", &4_u32)
        .await
        .expect("publishing inside the scope failed");
    assert!(
        format!("{scope:?}").contains("open: true"),
        "an unsettled scope reports itself open",
    );

    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(
        futures::poll!(stream.next()).is_pending(),
        "a scoped publish stays invisible until the commit",
    );
    scope.commit().await.expect("commit failed");

    let msg = stream
        .next()
        .await
        .expect("delivery missing")
        .expect("memory subscriber never errors");
    assert_eq!(msg.payload(), b"4");
    msg.ack().await.expect("ack failed");
}

/// A broker that refuses to open the transaction fails the batch before any reply is sent.
#[cfg(feature = "json")]
#[tokio::test]
async fn a_refused_begin_fails_the_batch_before_the_first_reply() {
    use std::sync::atomic::Ordering;

    use fixtures::Rigged;

    use crate::codec::JsonCodec;

    let wiring = TypedPublisher::with_codec(
        Rigged {
            fail_begin: true,
            ..Rigged::default()
        },
        JsonCodec,
    )
    .transactional();
    let headers = Headers::new();
    let cx = PublishContext::new("in", &headers, &());

    let err = wiring
        .publish_batch("out", &[1_u32, 2], &PublishIdentity, &cx)
        .await
        .expect_err("a refused begin fails the batch");
    assert!(err.to_string().contains("rigged"), "reported: {err}");
    assert_eq!(
        wiring.inner.publisher.published.load(Ordering::SeqCst),
        0,
        "no reply may be sent without an open transaction",
    );
}

/// A reply that cannot be produced aborts the whole batch, so no half-published batch stays
/// visible.
#[cfg(all(feature = "json", feature = "memory"))]
#[tokio::test]
async fn a_failed_reply_aborts_the_whole_batch() {
    use std::sync::atomic::Ordering;

    use fixtures::{Rigged, unencodable};

    use crate::codec::JsonCodec;

    let wiring = TypedPublisher::with_codec(Rigged::default(), JsonCodec).transactional();
    let headers = Headers::new();
    let cx = PublishContext::new("in", &headers, &());

    let err = wiring
        .publish_batch(
            "out",
            &[unencodable(), unencodable()],
            &PublishIdentity,
            &cx,
        )
        .await
        .expect_err("a reply that cannot be encoded fails the batch");

    assert!(
        err.to_string().contains("encode failed"),
        "the caller acts on the failure that broke the batch: {err}",
    );
    assert_eq!(
        wiring.inner.publisher.aborted.load(Ordering::SeqCst),
        1,
        "the open transaction must be aborted exactly once",
    );
}

/// When the abort itself fails, the original error still travels and the abort failure is
/// only logged: the caller acts on the failure that broke the batch.
#[cfg(all(feature = "json", feature = "memory", feature = "logging"))]
#[tokio::test]
async fn a_failed_abort_is_logged_rather_than_propagated() {
    use std::sync::atomic::Ordering;

    use fixtures::{Rigged, unencodable};

    use crate::codec::JsonCodec;

    let (events, guard) = capture_events();

    let wiring = TypedPublisher::with_codec(
        Rigged {
            fail_abort: true,
            ..Rigged::default()
        },
        JsonCodec,
    )
    .transactional();
    let headers = Headers::new();
    let cx = PublishContext::new("in", &headers, &());

    let err = wiring
        .publish_batch("out", &[unencodable()], &PublishIdentity, &cx)
        .await
        .expect_err("a reply that cannot be encoded fails the batch");
    drop(guard);

    assert!(
        err.to_string().contains("encode failed"),
        "the caller acts on the original failure, not on the abort: {err}",
    );
    assert_eq!(
        wiring.inner.publisher.aborted.load(Ordering::SeqCst),
        1,
        "the open transaction must be aborted exactly once",
    );
    let warned = {
        let captured = events.lock().unwrap();
        captured
            .iter()
            .any(|event| event.contains("transaction abort failed"))
    };
    assert!(warned, "a failed abort is logged, never propagated");
}

/// A commit the broker rejects fails the batch, so the incoming batch is retried rather
/// than acked over half-published replies.
#[cfg(feature = "json")]
#[tokio::test]
async fn a_refused_commit_fails_the_batch() {
    use std::sync::atomic::Ordering;

    use fixtures::Rigged;

    use crate::codec::JsonCodec;

    let wiring = TypedPublisher::with_codec(
        Rigged {
            fail_commit: true,
            ..Rigged::default()
        },
        JsonCodec,
    )
    .transactional();
    let headers = Headers::new();
    let cx = PublishContext::new("in", &headers, &());

    let err = wiring
        .publish_batch("out", &[1_u32, 2], &PublishIdentity, &cx)
        .await
        .expect_err("a refused commit fails the batch");
    assert!(err.to_string().contains("rigged"), "reported: {err}");
    assert_eq!(
        wiring.inner.publisher.published.load(Ordering::SeqCst),
        2,
        "the replies were sent; only the commit failed",
    );
    assert_eq!(
        wiring.inner.publisher.aborted.load(Ordering::SeqCst),
        0,
        "a failed commit closes the transaction, so there is nothing to abort",
    );
}

/// The scope's publish surfaces the encode failure as its own variant, and does not settle:
/// the caller still chooses between a retry and an abort.
#[cfg(feature = "json")]
#[tokio::test]
async fn a_scoped_publish_reports_an_encode_failure_without_settling() {
    use fixtures::{Rigged, unencodable};

    use crate::codec::JsonCodec;

    let wiring = TypedPublisher::with_codec(Rigged::default(), JsonCodec).transactional();
    let mut scope = wiring.begin().await.expect("begin failed");

    let err = scope
        .publish("out", &unencodable())
        .await
        .expect_err("the codec cannot encode this value");
    assert!(
        matches!(err, TransactionPublishError::Encode(_)),
        "the encode arm must be distinguishable from a broker rejection: {err:?}",
    );
    assert_eq!(
        err.to_string(),
        "failed to encode the value for a transactional publish",
    );
    scope.abort().await.expect("abort failed");
}

/// An owned transaction encodes with the publisher's codec, keeps its buffer private, and
/// discards it on abort.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn an_owned_transaction_encodes_and_discards_on_abort() {
    use fixtures::unencodable;
    use futures::StreamExt;

    use crate::Subscriber;
    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;

    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("owned");
    let publisher = TypedPublisher::with_codec(broker.publisher(), JsonCodec);

    let mut txn = publisher.transaction().await.expect("open failed");
    assert_eq!(format!("{txn:?}"), "TypedTransaction { .. }");
    txn.publish("owned", &3_u32).await.expect("publish failed");
    txn.abort().await.expect("abort failed");

    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(
        futures::poll!(stream.next()).is_pending(),
        "an aborted buffer never reaches the bus",
    );

    let mut txn = publisher.transaction().await.expect("open failed");
    let err = txn
        .publish("owned", &unencodable())
        .await
        .expect_err("the codec cannot encode this value");
    assert!(
        matches!(err, TransactionPublishError::Encode(_)),
        "the encode arm must be distinguishable from a broker rejection: {err:?}",
    );
    txn.abort().await.expect("abort failed");
}

/// A policy that fails to pair fails the whole wiring stack: neither shape hands out a
/// half-built publisher, and the error travels to the startup that asked for it.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn a_refused_pairing_fails_the_whole_wiring() {
    use fixtures::RefusePairing;

    use crate::Broker;
    use crate::codec::JsonCodec;
    use crate::memory::MemoryBroker;

    let connected = MemoryBroker::new().connect().await.expect("connect failed");

    let plain = TypedPublisher::with_codec(RefusePairing, JsonCodec)
        .pair(&connected)
        .await
        .expect_err("the policy refuses to pair");
    assert!(
        plain.to_string().contains("refused to pair"),
        "the policy's reason must survive the stack: {plain}",
    );

    let transactional = TypedPublisher::with_codec(RefusePairing, JsonCodec)
        .transactional()
        .pair(&connected)
        .await
        .expect_err("the policy refuses to pair");
    assert!(
        transactional.to_string().contains("refused to pair"),
        "the transactional wrapper must not swallow it: {transactional}",
    );
}

/// A broker that refuses to open the handle's transaction surfaces its error from `begin`,
/// so no scope exists over a transaction the broker does not have.
#[cfg(feature = "json")]
#[tokio::test]
async fn a_refused_begin_reports_the_publisher_error() {
    use fixtures::Rigged;

    use crate::codec::JsonCodec;

    let wiring = TypedPublisher::with_codec(
        Rigged {
            fail_begin: true,
            ..Rigged::default()
        },
        JsonCodec,
    )
    .transactional();

    let err = wiring
        .begin()
        .await
        .expect_err("the publisher refuses to begin");
    assert_eq!(err.to_string(), "the rigged publisher refused");
}

/// Likewise for the owned kind: a refused open is reported, not papered over with an empty
/// buffer that would silently drop everything published into it.
#[cfg(all(feature = "memory", feature = "json"))]
#[tokio::test]
async fn a_refused_owned_transaction_reports_the_publisher_error() {
    use fixtures::Rigged;

    use crate::codec::JsonCodec;

    let publisher = TypedPublisher::with_codec(Rigged::default(), JsonCodec);

    let err = publisher
        .transaction()
        .await
        .expect_err("the publisher refuses to open a transaction");
    assert_eq!(err.to_string(), "the rigged publisher refused");
}
