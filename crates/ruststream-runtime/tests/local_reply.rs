//! A reply definition on a router of the local family, opened against the local in-memory
//! broker and run in one future: the reply leaves through the codec to the broker.

#![cfg(feature = "json")]
// The run holds the bus's `!Send` subscriber on purpose: that is the family under test.
#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::future::ready;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::future::join;
use ruststream_core::{
    BytesMut, LocalBroker, LocalIncomingMessage, LocalPublishPolicy, LocalPublisher,
    LocalSubscriber, LocalSubscriptionSource, OutgoingMessage,
};
use ruststream_memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream_runtime::{
    Context, ContextKind, Deserialized, Detach, Halt, Handle, HandlerOutcome, Input, OpenRoutes,
    Outgoing, PublishTransform, Reads, Router, RunRoutes, ShutdownWait, SoloDeserialized,
    subscriber,
};
use tokio::sync::Notify;
use tokio::sync::futures::Notified;
use tokio::time::timeout;

/// The app's shutdown: a flag and the waiters it wakes.
#[derive(Default)]
struct Stop {
    flag: AtomicBool,
    notify: Notify,
}

impl Stop {
    fn stop(&self) {
        self.flag.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}

impl ShutdownWait for &Stop {
    type Wait<'a>
        = Notified<'a>
    where
        Self: 'a;

    fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    fn cancelled(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

impl Halt for &Stop {
    fn fail(&self, _subscription: &str, _reason: &str) {
        self.stop();
    }

    fn stream_ended(&self, _subscription: &str) {}
}

/// The runner's host: counts the work a delivery left behind, which this scenario leaves none
/// of.
#[derive(Default)]
struct CountDetached(AtomicUsize);

impl Detach for CountDetached {
    fn detach<Work>(&self, _work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn detach_after<Work>(&self, _delay: Duration, _work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// A request as its bytes, viewed in place.
struct Request<'a>(&'a [u8]);

impl Deserialized for Request<'_> {
    type Output<'a> = Request<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Request<'_>, Infallible> {
        Ok(Request(payload))
    }
}

impl Input for Request<'_> {
    type Axis = SoloDeserialized<Request<'static>>;
}

/// Answers a request with its bytes as text, which the reply codec encodes.
struct Echo;

impl Handle<Request<'_>, String> for Echo {
    fn handle(
        &self,
        request: &Request<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<String, HandlerOutcome>> + Send {
        ready(Ok(String::from_utf8_lossy(request.0).into_owned()))
    }
}

/// A reply transform: stamps every reply it sees with the header `stamped`.
struct Stamp;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Stamp {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        out.headers_mut().insert("stamped", "yes");
    }
}

#[tokio::test]
async fn an_encoded_reply_leaves_through_the_codec_to_a_local_broker() {
    let connected = LocalMemoryBroker::<4>::new()
        .connect()
        .await
        .expect("the bus connects");
    let mut answers = LocalMemorySource::new("answers")
        .subscribe(&connected)
        .await
        .expect("the answers subscribe");
    let opened = Router::<LocalMemoryBroker<4>, _, _, _, _>::local()
        .include(
            subscriber(LocalMemorySource::new("requests"), Echo)
                .reply()
                .to("answers")
                .build(),
        )
        .out_reply(LocalMemoryPublish)
        .transform(Stamp)
        .build()
        .open(&connected)
        .await
        .expect("the route opens");
    let publisher = LocalMemoryPublish
        .pair(&connected)
        .await
        .expect("the publisher pairs");
    let stop = Stop::default();
    let host = CountDetached::default();

    let ((), answer) = join(opened.run(&stop, &Arc::new(()), &host), async {
        let request = OutgoingMessage::with_payload("requests", BytesMut::from(&b"ping"[..]));
        publisher
            .publish(request, None)
            .await
            .expect("the bus takes it");
        // A guard, not a wait: the answer is published within the same few polls, and the
        // timeout only ends a run whose reply never left.
        let answer = timeout(Duration::from_secs(5), answers.stream().next())
            .await
            .ok()
            .flatten();
        stop.stop();
        answer
    })
    .await;

    let answer = answer.expect("an answer").expect("a delivery");
    assert_eq!(LocalIncomingMessage::payload(&answer), b"\"ping\"");
    assert_eq!(
        LocalIncomingMessage::headers(&answer).get_str("stamped"),
        Some("yes"),
        "the reply transform's header"
    );
    assert_eq!(
        host.0.load(Ordering::Relaxed),
        0,
        "work detached by the delivery"
    );
}
