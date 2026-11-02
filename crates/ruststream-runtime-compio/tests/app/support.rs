//! The doubles the run tests share: a line the test posts deliveries into, the handler that reports
//! them, and the runtime the tests start services on.

use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::future::ready;
use std::panic::panic_any;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use compio::time::timeout;
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::future::{Either, select};
use futures::{Stream, StreamExt};
use ruststream_core::{
    AckError, BrokerMoves, HeaderMap, IncomingMessage, Settle, Subscriber, SubscriptionSource,
};
use ruststream_core::{BytesMut, OutgoingMessage, Publisher};
use ruststream_memory::local::{
    ConnectedLocalMemoryBroker, LocalMemoryError, LocalMemoryPublisher,
};
use ruststream_runtime::{
    Context, Deserialized, Handle, HandlerOutcome, Input, IntoSource, SoloDeserialized,
};
use ruststream_runtime_compio::test_runtime;
use ruststream_runtime_compio::time::sleep;
use std::pin::pin;

/// What happened, in order, as the services under test report it.
#[derive(Clone, Default)]
pub(crate) struct Log {
    entries: Arc<Mutex<Vec<String>>>,
    // A publisher made while the subscription opened, kept to ask the bus whether it is still up.
    bus: Arc<Mutex<Option<LocalMemoryPublisher<8>>>>,
}

impl Log {
    pub(crate) fn add(&self, entry: impl Into<String>) {
        self.entries.lock().expect("the log").push(entry.into());
    }

    pub(crate) fn entries(&self) -> Vec<String> {
        self.entries.lock().expect("the log").clone()
    }

    /// Whether the broker the subscription was opened on has been shut down.
    pub(crate) async fn bus_is_shut_down(&self) -> bool {
        let publisher = self.bus.lock().expect("the bus").clone();
        let publisher = publisher.expect("a subscription was opened on the bus");
        publisher
            .publish(OutgoingMessage::produced("probe", BytesMut::new()), None)
            .await
            .is_err()
    }
}

/// A delivery numbered by the test; settling it reports its number to the test.
#[derive(Debug)]
pub(crate) struct Mail {
    payload: Bytes,
    headers: HeaderMap,
    settled: UnboundedSender<usize>,
}

impl Mail {
    fn position(&self) -> usize {
        usize::from_be_bytes(self.payload.as_ref().try_into().expect("a numbered mail"))
    }
}

impl IncomingMessage for Mail {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl Settle for Mail {
    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        let _ = self.settled.unbounded_send(self.position());
        ready(Ok(()))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }
}

/// A subscription handed to the mount ready-made, which a router or `handle` takes, and the test's
/// end of it.
pub(crate) fn direct() -> (Direct, Line) {
    let (post, inbox) = mpsc::unbounded();
    let (settled, acked) = mpsc::unbounded();
    (
        Direct(Some(inbox)),
        Line {
            post: Some(post),
            settled,
            acked,
        },
    )
}

/// An open subscription that reads what the test posts.
pub(crate) struct Direct(Option<UnboundedReceiver<Mail>>);

impl Subscriber for Direct {
    type Message = Mail;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Mail, Infallible>> + Send + '_ {
        self.0.take().expect("the stream is opened once").map(Ok)
    }
}

/// The test's end of a subscription: what it posts, and the settlements it gets back.
pub(crate) struct Line {
    post: Option<UnboundedSender<Mail>>,
    settled: UnboundedSender<usize>,
    pub(crate) acked: UnboundedReceiver<usize>,
}

impl Line {
    /// Posts delivery number `position`.
    pub(crate) fn post(&self, position: usize) {
        let mail = Mail {
            payload: Bytes::copy_from_slice(&position.to_be_bytes()),
            headers: HeaderMap::new(),
            settled: self.settled.clone(),
        };
        self.post
            .as_ref()
            .expect("the line is open")
            .unbounded_send(mail)
            .expect("the subscription reads the line");
    }

    /// Ends the subscription's stream, as a server that deletes the queue does.
    pub(crate) fn end(&mut self) {
        self.post = None;
    }

    /// A cloneable sender of deliveries, for a hook that posts.
    pub(crate) fn poster(&self) -> Poster {
        Poster {
            post: self.post.clone().expect("the line is open"),
            settled: self.settled.clone(),
        }
    }
}

/// A [`Line`]'s posting end, which a lifecycle hook can own.
#[derive(Clone)]
pub(crate) struct Poster {
    post: UnboundedSender<Mail>,
    settled: UnboundedSender<usize>,
}

impl Poster {
    pub(crate) fn post(&self, position: usize) {
        let mail = Mail {
            payload: Bytes::copy_from_slice(&position.to_be_bytes()),
            headers: HeaderMap::new(),
            settled: self.settled.clone(),
        };
        self.post.unbounded_send(mail).expect("the line is read");
    }
}

/// The subscription a service mounts: it reads what the test posts on its [`Line`].
#[derive(Clone)]
pub(crate) struct Wire {
    name: &'static str,
    // The descriptor is cloned for each mount; the one subscription that opens takes the inbox.
    inbox: Arc<Mutex<Option<UnboundedReceiver<Mail>>>>,
    log: Log,
    refuse: bool,
    explode: bool,
    mute_explosion: bool,
}

/// A wire named `name`, and the test's end of it. `log` hears when the subscription opens and
/// closes.
pub(crate) fn wire(name: &'static str, log: &Log) -> (Wire, Line) {
    let (post, inbox) = mpsc::unbounded();
    let (settled, acked) = mpsc::unbounded();
    (
        Wire {
            name,
            inbox: Arc::new(Mutex::new(Some(inbox))),
            log: log.clone(),
            refuse: false,
            explode: false,
            mute_explosion: false,
        },
        Line {
            post: Some(post),
            settled,
            acked,
        },
    )
}

impl Wire {
    /// The stream of the wire panics when the loop opens it, as a client library with a bug does.
    pub(crate) fn exploding(mut self) -> Self {
        self.explode = true;
        self
    }

    /// The stream of the wire panics with a payload that carries no text.
    pub(crate) fn exploding_without_a_word(mut self) -> Self {
        self.explode = true;
        self.mute_explosion = true;
        self
    }

    /// The wire refuses to open, as a server that has no such queue does.
    pub(crate) fn refusing(mut self) -> Self {
        self.refuse = true;
        self
    }
}

impl IntoSource for Wire {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedLocalMemoryBroker<8>> for Wire {
    type Subscriber = Open;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.name
    }

    fn subscribe(
        self,
        connected: &ConnectedLocalMemoryBroker<8>,
    ) -> impl Future<Output = Result<Open, LocalMemoryError>> {
        *self.log.bus.lock().expect("the bus") = Some(connected.publisher());
        ready(if self.refuse {
            Err(LocalMemoryError::ShutDown)
        } else {
            self.log.add(format!("opened {}", self.name));
            Ok(Open {
                name: self.name,
                inbox: self.inbox.lock().expect("the inbox").take(),
                explode: self.explode,
                mute_explosion: self.mute_explosion,
                log: self.log,
            })
        })
    }
}

/// An open subscription; dropping it, when its loop ends, is the close.
pub(crate) struct Open {
    name: &'static str,
    inbox: Option<UnboundedReceiver<Mail>>,
    explode: bool,
    mute_explosion: bool,
    log: Log,
}

impl Drop for Open {
    fn drop(&mut self) {
        self.log.add(format!("closed {}", self.name));
    }
}

impl Subscriber for Open {
    type Message = Mail;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Mail, Infallible>> + Send + '_ {
        if self.explode && self.mute_explosion {
            panic_any(7_u8);
        }
        assert!(!self.explode, "the client library broke");
        self.inbox
            .take()
            .expect("the stream is opened once")
            .map(Ok)
    }
}

/// The text of a delivery, as a handler reads it.
pub(crate) struct Reading<'a>(pub(crate) &'a [u8]);

#[derive(Debug)]
pub(crate) struct Unreadable;

impl fmt::Display for Unreadable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unreadable")
    }
}

impl Error for Unreadable {}

impl Deserialized for Reading<'_> {
    type Output<'a> = Reading<'a>;
    type Error = Unreadable;

    fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Unreadable> {
        Ok(Reading(payload))
    }
}

impl Input for Reading<'_> {
    type Axis = SoloDeserialized<Reading<'static>>;
}

/// A handler that runs `work(position)` for each delivery and answers what it returns.
pub(crate) struct On<Work>(pub(crate) Work);

impl<Work, Then> Handle<Reading<'_>> for On<Work>
where
    Work: Fn(usize) -> Then + Send + Sync,
    Then: Future<Output = Result<(), HandlerOutcome>> + Send,
{
    fn handle(
        &self,
        reading: &Reading<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> + Send {
        let position = usize::from_be_bytes(reading.0.try_into().expect("a numbered mail"));
        (self.0)(position)
    }
}

/// Runs `test` on a compio runtime of its own, and fails it if it takes longer than ten seconds.
pub(crate) fn on_runtime<Out>(test: impl Future<Output = Out>) -> Out {
    test_runtime().expect("a runtime").block_on(async {
        timeout(Duration::from_secs(10), test)
            .await
            .expect("the scenario finished in time")
    })
}

/// What `signal` yields, or `None` once ten seconds pass, so a delivery that never comes ends a
/// run's signal instead of hanging it. The future is `Send`, which a run's signal must be.
pub(crate) async fn within<Out>(signal: impl Future<Output = Out> + Send) -> Option<Out> {
    let late = sleep(Duration::from_secs(10));
    match select(pin!(signal), pin!(late)).await {
        Either::Left((out, _)) => Some(out),
        Either::Right(_) => None,
    }
}
