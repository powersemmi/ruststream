//! Each check against the in-memory broker, which passes, and against a broken double of it,
//! which the check must fail: a check no broker can fail proves nothing.

use std::{
    future::{Future, pending},
    mem,
    sync::{
        Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, stream};
use tokio::spawn;
use tokio::time::sleep;

use super::{ORDERED, OptionCases, keyed_order, publish_options, publisher_carries};
use crate::conformance::helpers::unique_subject;
use crate::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublish,
    MemoryPublisher, MemorySource, MemorySubscriber, PARTITION_KEY_HEADER,
};
use crate::{
    AckError, AddressedCopies, HeaderMap, IncomingMessage, OutgoingMessage, PairError,
    PublishPolicy, Publisher, Subscribe, Subscriber, SubscriptionSource, Take,
};

/// What a double reports when it refuses, alongside what the memory broker reports.
#[derive(Debug, thiserror::Error)]
enum DoubleError {
    #[error(transparent)]
    Memory(#[from] MemoryError),
    #[error("the double refuses this publish: {0}")]
    Refused(&'static str),
}

/// A message a double holds back, owned.
struct Held {
    name: String,
    payload: BytesMut,
    headers: HeaderMap,
}

/// How a double breaks the message on its way to the memory broker.
#[derive(Clone, Copy)]
enum Fault {
    /// Holds messages back and releases each run of `window` in reverse.
    Reverses { window: usize },
    /// Publishes every message with no headers, and reports success.
    DropsHeaders,
    /// Rewrites every header value as lossy UTF-8 text, and reports success.
    RewritesAsText,
    /// Refuses every publish that carries headers. The honest answer of a headerless transport.
    RefusesHeaders,
    /// Reports a publish with headers as refused, and delivers it anyway once `after` more
    /// publishes went through.
    RefusesAndDelivers { after: usize },
    /// Drops the partition key header.
    DropsKey,
    /// Appends a byte to every payload.
    CorruptsPayload,
    /// Refuses every publish.
    RefusesAll,
    /// Reports every publish with no headers as done, and delivers none of them.
    SwallowsPlain,
    /// Never returns from a publish.
    Hangs,
    /// Delivers every message twice, the way an at-least-once transport may.
    Duplicates,
    /// Loses the first message with no headers, and redelivers the second one every few seconds
    /// for as long as the broker stands.
    RepeatsWhileLosing,
    /// Delivers each message one [`TRICKLE`] after the one published before it, so the whole run
    /// takes longer than one delivery wait.
    Trickles,
}

/// How far apart [`Fault::Trickles`] delivers its messages.
const TRICKLE: Duration = Duration::from_secs(1);

struct Faulty {
    inner: MemoryPublisher,
    fault: Fault,
    held: Mutex<Vec<Held>>,
    /// How many publishes with no headers went through, for the faults that pick one by order.
    plain: AtomicUsize,
    /// Refused messages still to deliver, each with the publishes left before it goes.
    refused: Mutex<Vec<(Held, usize)>>,
}

impl Faulty {
    fn new(inner: MemoryPublisher, fault: Fault) -> Self {
        Self {
            inner,
            fault,
            held: Mutex::new(Vec::new()),
            plain: AtomicUsize::new(0),
            refused: Mutex::new(Vec::new()),
        }
    }

    async fn forward(&self, held: Held) -> Result<(), DoubleError> {
        let msg = OutgoingMessage::produced(&held.name, held.payload).with_headers(held.headers);
        Ok(self.inner.publish(msg, None).await?)
    }

    /// Loses the first message with no headers and keeps redelivering the second one.
    async fn lose_first_and_repeat_second(&self, held: Held) -> Result<(), DoubleError> {
        match self.plain.fetch_add(1, Ordering::SeqCst) {
            0 => Ok(()),
            1 => {
                let publisher = self.inner.clone();
                let (name, payload) = (held.name.clone(), held.payload.clone());
                self.forward(held).await?;
                drop(spawn(async move {
                    loop {
                        sleep(Duration::from_secs(3)).await;
                        let copy = OutgoingMessage::produced(&name, payload.clone());
                        if publisher.publish(copy, None).await.is_err() {
                            break;
                        }
                    }
                }));
                Ok(())
            }
            _ => self.forward(held).await,
        }
    }

    /// Forwards `held`, then every refused message whose count of publishes ran out.
    async fn forward_then_release(&self, held: Held) -> Result<(), DoubleError> {
        self.forward(held).await?;
        let due = {
            let mut refused = self.refused.lock().unwrap_or_else(PoisonError::into_inner);
            for (_, left) in refused.iter_mut() {
                *left = left.saturating_sub(1);
            }
            let (due, waiting) = mem::take(&mut *refused)
                .into_iter()
                .partition::<Vec<_>, _>(|(_, left)| *left == 0);
            *refused = waiting;
            due
        };
        for (held, _) in due {
            self.forward(held).await?;
        }
        Ok(())
    }
}

impl Publisher for Faulty {
    type Payload = Take;
    type Error = DoubleError;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), DoubleError>> + Send {
        let (name, payload, headers) = msg.into_parts();
        let held = Held {
            name: name.to_owned(),
            payload,
            headers,
        };
        async move {
            match self.fault {
                Fault::Reverses { window } => {
                    let run = {
                        let mut queue = self.held.lock().unwrap_or_else(PoisonError::into_inner);
                        queue.push(held);
                        if queue.len() < window {
                            return Ok(());
                        }
                        mem::take(&mut *queue)
                    };
                    for held in run.into_iter().rev() {
                        self.forward(held).await?;
                    }
                    Ok(())
                }
                Fault::DropsHeaders => {
                    self.forward(Held {
                        headers: HeaderMap::new(),
                        ..held
                    })
                    .await
                }
                Fault::RewritesAsText => {
                    let mut text = HeaderMap::new();
                    for (key, value) in held.headers.iter() {
                        text.insert(key.to_owned(), String::from_utf8_lossy(value).into_owned());
                    }
                    self.forward(Held {
                        headers: text,
                        ..held
                    })
                    .await
                }
                Fault::RefusesHeaders if !held.headers.is_empty() => {
                    Err(DoubleError::Refused("this transport carries no headers"))
                }
                Fault::RefusesAndDelivers { after: 0 } if !held.headers.is_empty() => {
                    self.forward(held).await?;
                    Err(DoubleError::Refused("reported refused, delivered anyway"))
                }
                Fault::RefusesAndDelivers { after } if !held.headers.is_empty() => {
                    self.refused
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push((held, after));
                    Err(DoubleError::Refused("reported refused, delivered later"))
                }
                Fault::RefusesAndDelivers { .. } => self.forward_then_release(held).await,
                Fault::DropsKey => {
                    let mut held = held;
                    held.headers.remove(PARTITION_KEY_HEADER);
                    self.forward(held).await
                }
                Fault::CorruptsPayload => {
                    let mut held = held;
                    held.payload.extend_from_slice(b"!");
                    self.forward(held).await
                }
                Fault::RefusesAll => Err(DoubleError::Refused("this transport refuses everything")),
                Fault::SwallowsPlain if held.headers.is_empty() => Ok(()),
                Fault::Hangs => pending().await,
                Fault::Duplicates => {
                    let copy = Held {
                        name: held.name.clone(),
                        payload: held.payload.clone(),
                        headers: held.headers.clone(),
                    };
                    self.forward(held).await?;
                    self.forward(copy).await
                }
                Fault::Trickles => {
                    let turn = self.plain.fetch_add(1, Ordering::SeqCst) + 1;
                    let delay = TRICKLE.saturating_mul(u32::try_from(turn).unwrap_or(u32::MAX));
                    let publisher = self.inner.clone();
                    drop(spawn(async move {
                        sleep(delay).await;
                        let msg = OutgoingMessage::produced(&held.name, held.payload)
                            .with_headers(held.headers);
                        let _delivered = publisher.publish(msg, None).await;
                    }));
                    Ok(())
                }
                Fault::RepeatsWhileLosing if held.headers.is_empty() => {
                    self.lose_first_and_repeat_second(held).await
                }
                Fault::RefusesHeaders | Fault::SwallowsPlain | Fault::RepeatsWhileLosing => {
                    self.forward(held).await
                }
            }
        }
    }
}

// `make_source` / `make_publisher` stay closures: their bounds are higher-ranked, so a method
// path, which binds one lifetime, does not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
async fn carries(fault: Fault) {
    let _factories = publisher_carries(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        move |connected| Faulty::new(connected.publisher(), fault),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_carries_every_message_and_its_headers() {
    let _factories = publisher_carries(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_refuses_headers_passes() {
    carries(Fault::RefusesHeaders).await;
}

/// A partitioned transport delivers unkeyed records in no total order; the check holds it to
/// delivering every one, not to an order it does not promise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_reorders_unkeyed_messages_passes() {
    carries(Fault::Reverses { window: 2 }).await;
}

/// A transport that keeps redelivering one accepted message while another is lost still fails:
/// the redeliveries do not buy the lost one a fresh wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "unkeyed run: waiting for unkeyed:0000: no delivery within")]
async fn a_transport_that_repeats_one_message_while_losing_another_fails() {
    carries(Fault::RepeatsWhileLosing).await;
}

/// A transport that delivers each message in time, while the whole run takes longer than one
/// delivery wait, passes. The clock is paused, so the run costs no real seconds.
#[tokio::test(start_paused = true)]
async fn a_transport_that_delivers_gradually_passes() {
    carries(Fault::Trickles).await;
}

/// A redelivered accepted message is the transport's right, during the run and after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_delivers_twice_passes() {
    carries(Fault::Duplicates).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must arrive byte for byte")]
async fn a_publisher_that_drops_headers_fails() {
    carries(Fault::DropsHeaders).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "header round trip (a non-UTF-8 value)")]
async fn a_publisher_that_rewrites_binary_values_as_text_fails() {
    carries(Fault::RewritesAsText).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "header round trip: the publish carrying many entries was refused, \
                           and the message arrived anyway"
)]
async fn a_publisher_that_reports_refused_and_delivers_fails() {
    carries(Fault::RefusesAndDelivers { after: 0 }).await;
}

/// The refused messages arrive after every accepted one, so only the quiet period sees them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "header round trip: the publish carrying many entries was refused, \
                           and the message arrived anyway"
)]
async fn a_publisher_that_delivers_a_refused_message_late_fails() {
    carries(Fault::RefusesAndDelivers { after: ORDERED }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a delivery nobody published arrived")]
async fn a_transport_that_corrupts_the_payload_fails() {
    carries(Fault::CorruptsPayload).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "unkeyed run: publish 0 through the broker's publisher failed")]
async fn a_publisher_that_refuses_a_plain_message_fails() {
    carries(Fault::RefusesAll).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "unkeyed run: waiting for unkeyed:0000: no delivery within")]
async fn a_publisher_that_loses_a_message_fails() {
    carries(Fault::SwallowsPlain).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "header round trip: the publish carrying many entries hung for")]
async fn a_publish_that_hangs_fails() {
    carries(Fault::Hangs).await;
}

// Subscriptions whose stream or settlement is broken.

/// How a subscription double breaks what it hands the check.
#[derive(Clone, Copy)]
enum StreamFault {
    /// Ends the stream before its first delivery.
    Ends,
    /// Yields an error in place of its first delivery.
    Errors,
    /// Answers every acknowledgement with a timeout.
    AckFails,
}

struct ShapeSource {
    name: String,
    fault: StreamFault,
}

impl SubscriptionSource<ConnectedMemoryBroker> for ShapeSource {
    type Subscriber = ShapeSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<ShapeSubscriber, MemoryError> {
        Ok(ShapeSubscriber {
            inner: Subscribe::subscribe(connected, &self.name).await?,
            fault: self.fault,
        })
    }
}

struct ShapeSubscriber {
    inner: MemorySubscriber,
    fault: StreamFault,
}

impl Subscriber for ShapeSubscriber {
    type Message = ShapeMessage;
    type Error = DoubleError;

    fn stream(&mut self) -> impl Stream<Item = Result<ShapeMessage, DoubleError>> + Send + '_ {
        let fault = self.fault;
        let delivered = match fault {
            StreamFault::AckFails => usize::MAX,
            StreamFault::Ends | StreamFault::Errors => 0,
        };
        let failure = matches!(fault, StreamFault::Errors)
            .then_some(Err(DoubleError::Refused("the subscription fails")));
        self.inner
            .stream()
            .take(delivered)
            .map(move |item| match item {
                Ok(inner) => Ok(ShapeMessage { inner, fault }),
                Err(never) => match never {},
            })
            .chain(stream::iter(failure))
    }
}

struct ShapeMessage {
    inner: MemoryMessage,
    fault: StreamFault,
}

impl IncomingMessage for ShapeMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    async fn ack(self) -> Result<(), AckError> {
        match self.fault {
            StreamFault::AckFails => Err(AckError::Timeout),
            StreamFault::Ends | StreamFault::Errors => self.inner.ack().await,
        }
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.inner.nack(requeue).await
    }
}

#[allow(clippy::redundant_closure_for_method_calls)]
async fn carries_through(fault: StreamFault) {
    let _factories = publisher_carries(
        MemoryBroker::new,
        move |name| ShapeSource {
            name: name.to_owned(),
            fault,
        },
        |connected| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the subscription ended before the expected delivery")]
async fn a_subscription_that_ends_fails() {
    carries_through(StreamFault::Ends).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the subscription yielded an error")]
async fn a_subscription_that_fails_fails() {
    carries_through(StreamFault::Errors).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "message shape: ack must succeed or be unsupported, got: Timeout")]
async fn an_acknowledgement_that_fails_fails() {
    carries_through(StreamFault::AckFails).await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
async fn keyed(fault: Fault) {
    keyed_order(
        MemoryBroker::new,
        &unique_subject("conformance.keyed"),
        |name| MemorySource::new(name),
        move |connected| Faulty::new(connected.publisher(), fault),
        |key, headers| {
            headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
            None
        },
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_keeps_keys_and_their_order() {
    keyed_order(
        MemoryBroker::new,
        &unique_subject("conformance.keyed"),
        |name| MemorySource::new(name),
        |connected| connected.publisher(),
        |key, headers| {
            headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
            None
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must report that key")]
async fn a_publisher_that_drops_the_key_fails() {
    keyed(Fault::DropsKey).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must arrive in publish order")]
async fn a_publisher_that_reorders_one_key_fails() {
    // Six is two messages of each of the three keys, so every key comes out reversed.
    keyed(Fault::Reverses { window: 6 }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "keyed order: publishing")]
async fn a_transport_that_refuses_the_key_header_fails() {
    keyed(Fault::RefusesHeaders).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "keyed order: a delivery nobody published")]
async fn a_transport_that_corrupts_a_keyed_payload_fails() {
    keyed(Fault::CorruptsPayload).await;
}

/// A priority, the one per-message setting of the options double.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Priority {
    priority: Option<u8>,
}

/// The highest priority the options double's transport honours.
const MAX_PRIORITY: u8 = 9;

/// How the options double resolves a priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolve {
    /// Call over policy, and a value past the range refused.
    Honest,
    /// The transport's own default whenever the call names nothing.
    IgnoresPolicy,
    /// The policy's value, whatever the call names.
    IgnoresCall,
    /// The last call's value keeps applying to publishes that name nothing.
    Sticky,
    /// A value past the range becomes the highest one, and the publish succeeds.
    Clamps,
    /// Every publish whose call names a priority is refused.
    RefusesCalls,
    /// A value past the range is refused and delivered all the same, at once.
    DeliversRefused,
    /// A value past the range is refused, and delivered after the next publish.
    DeliversRefusedLate,
}

#[derive(Debug, Clone, Copy)]
struct PriorityPublish {
    priority: u8,
    resolve: Resolve,
}

struct PriorityPublisher {
    inner: MemoryPublisher,
    policy: u8,
    resolve: Resolve,
    last: Mutex<Option<u8>>,
    /// A refused message held back until the next publish.
    late: Mutex<Option<Held>>,
}

impl PublishPolicy<ConnectedMemoryBroker> for PriorityPublish {
    type Live = PriorityPublisher;

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
        Ok(PriorityPublisher {
            inner: MemoryPublish.pair(connected).await?,
            policy: self.priority,
            resolve: self.resolve,
            last: Mutex::new(None),
            late: Mutex::new(None),
        })
    }
}

impl PriorityPublisher {
    fn resolve(&self, call: Option<u8>) -> Result<u8, DoubleError> {
        let last = {
            let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
            let before = *last;
            if call.is_some() {
                *last = call;
            }
            before
        };
        let resolved = match (self.resolve, call) {
            (Resolve::RefusesCalls, Some(_)) => {
                return Err(DoubleError::Refused(
                    "this transport refuses every priority",
                ));
            }
            (Resolve::IgnoresPolicy, None) => 0,
            (Resolve::Sticky, None) => last.unwrap_or(self.policy),
            (Resolve::IgnoresCall, _) | (_, None) => self.policy,
            (_, Some(value)) => value,
        };
        match resolved {
            value if value <= MAX_PRIORITY => Ok(value),
            _ if self.resolve == Resolve::Clamps => Ok(MAX_PRIORITY),
            _ => Err(DoubleError::Refused("priority past the transport's range")),
        }
    }

    async fn forward(&self, held: Held) -> Result<(), DoubleError> {
        let msg = OutgoingMessage::produced(&held.name, held.payload).with_headers(held.headers);
        Ok(self.inner.publish(msg, None).await?)
    }
}

impl Publisher for PriorityPublisher {
    type Payload = Take;
    type Error = DoubleError;
    type Options = Priority;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Priority>,
    ) -> Result<(), DoubleError> {
        let resolved = self.resolve(options.and_then(|options| options.priority));
        let (name, payload, headers) = msg.into_parts();
        let held = Held {
            name: name.to_owned(),
            payload,
            headers,
        };
        let priority = match (resolved, self.resolve) {
            (Ok(priority), _) => priority,
            (Err(refused), Resolve::DeliversRefused) => {
                self.forward(held).await?;
                return Err(refused);
            }
            (Err(refused), Resolve::DeliversRefusedLate) => {
                *self.late.lock().unwrap_or_else(PoisonError::into_inner) = Some(held);
                return Err(refused);
            }
            (Err(refused), _) => return Err(refused),
        };
        let mut held = held;
        held.headers.insert("priority", priority.to_string());
        self.forward(held).await?;
        let late = self
            .late
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(late) = late {
            self.forward(late).await?;
        }
        Ok(())
    }
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
async fn options(resolve: Resolve) {
    publish_options(
        MemoryBroker::new,
        &unique_subject("conformance.options"),
        |name| MemorySource::new(name),
        PriorityPublish {
            priority: 5,
            resolve,
        },
        OptionCases::new(Some(5))
            .overrides(Priority { priority: Some(1) }, Some(1))
            .overrides(Priority { priority: None }, Some(5))
            .refuses(Priority { priority: Some(42) }),
        |delivery| {
            delivery
                .headers()
                .get_str("priority")
                .and_then(|value| value.parse::<u8>().ok())
        },
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_broker_resolves_its_options() {
    publish_options(
        MemoryBroker::new,
        &unique_subject("conformance.options"),
        |name| MemorySource::new(name),
        MemoryPublish,
        OptionCases::new(()).overrides((), ()),
        |_delivery| (),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn options_resolved_over_the_policy_pass() {
    options(Resolve::Honest).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a publish with no options takes the policy's")]
async fn a_publisher_that_ignores_the_policy_fails() {
    options(Resolve::IgnoresPolicy).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "override 0 must win over the policy: the delivery shows another")]
async fn a_publisher_that_ignores_the_call_fails() {
    options(Resolve::IgnoresCall).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the publish after override 0 takes the policy's setting again")]
async fn a_publisher_whose_call_sticks_fails() {
    options(Resolve::Sticky).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "refused value 0 was accepted")]
async fn a_publisher_that_clamps_an_unhonourable_value_fails() {
    options(Resolve::Clamps).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "override 0 must win over the policy: the publish failed")]
async fn a_publisher_that_refuses_an_honourable_value_fails() {
    options(Resolve::RefusesCalls).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a message arrived out of order, or one that was refused")]
async fn a_publisher_that_delivers_a_refused_value_fails() {
    options(Resolve::DeliversRefused).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "publish options: refused value 0 was refused, and the message arrived")]
async fn a_publisher_that_delivers_a_refused_value_late_fails() {
    options(Resolve::DeliversRefusedLate).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "pass at least one override")]
async fn options_with_no_override_are_rejected() {
    publish_options(
        MemoryBroker::new,
        &unique_subject("conformance.options"),
        |name: &str| MemorySource::new(name),
        MemoryPublish,
        OptionCases::new(()),
        |_delivery| (),
    )
    .await;
}

#[cfg(feature = "asyncapi")]
mod credentials {
    use std::future::Future;

    use serde_json::json;

    use super::super::{describes_addresses_without_credentials, publishes_without_credentials};
    use crate::asyncapi::{Binding, Bindings};
    use crate::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPublish};
    use crate::{Broker, DescribeServer, PairError, PublishPolicy, ServerSpec};

    /// A policy that puts its connection URL, password and all, into its operation binding.
    struct LeakyPublish {
        url: &'static str,
    }

    impl PublishPolicy<ConnectedMemoryBroker> for LeakyPublish {
        type Live = crate::memory::MemoryPublisher;

        async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
            MemoryPublish.pair(connected).await
        }

        fn operation_bindings(&self, _channel: &str) -> Bindings {
            let body = json!({ "port": 5672, "tls": false, "urls": [self.url] });
            Binding::extension("x-leaky", &body)
                .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
        }
    }

    #[test]
    fn a_clean_policy_passes() {
        publishes_without_credentials::<ConnectedMemoryBroker, _>(&MemoryPublish, "hunter2");
    }

    #[test]
    #[should_panic(expected = "the publish policy's bindings carry the password")]
    fn a_policy_that_binds_its_password_fails() {
        publishes_without_credentials::<ConnectedMemoryBroker, _>(
            &LeakyPublish {
                url: "amqp://svc:hunter2@broker:5672",
            },
            "hunter2",
        );
    }

    /// A password with a quote in it, which the serialized document escapes.
    #[test]
    #[should_panic(expected = "the publish policy's bindings carry the password")]
    fn a_policy_that_binds_an_escaped_password_fails() {
        publishes_without_credentials::<ConnectedMemoryBroker, _>(
            &LeakyPublish {
                url: "amqp://svc:hun\"ter2@broker:5672",
            },
            "hun\"ter2",
        );
    }

    /// The failure must not print the password it found.
    #[test]
    fn the_failure_does_not_print_the_password() {
        let failure = std::panic::catch_unwind(|| {
            publishes_without_credentials::<ConnectedMemoryBroker, _>(
                &LeakyPublish {
                    url: "amqp://svc:hunter2@broker:5672",
                },
                "hunter2",
            );
        })
        .expect_err("a policy that binds its password fails the check");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(message.contains("carry the password"), "{message}");
        assert!(!message.contains("hunter2"), "{message}");
    }

    /// How the cluster double describes its addresses.
    #[derive(Clone, Copy)]
    enum Describe {
        EveryHost,
        FirstStrippedOnly,
        FirstOnly,
        Nothing,
    }

    struct Cluster {
        addrs: Vec<String>,
        describe: Describe,
    }

    impl Broker for Cluster {
        type Error = MemoryError;
        type Connected = ConnectedMemoryBroker;

        fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
            MemoryBroker::new().connect()
        }
    }

    impl DescribeServer for Cluster {
        fn describe_server(&self) -> ServerSpec {
            let hosts: Vec<String> = match self.describe {
                Describe::EveryHost => self
                    .addrs
                    .iter()
                    .map(|addr| ServerSpec::host_from_url(addr))
                    .collect(),
                Describe::FirstStrippedOnly => self
                    .addrs
                    .iter()
                    .enumerate()
                    .map(|(index, addr)| match index {
                        0 => ServerSpec::host_from_url(addr),
                        _ => addr.clone(),
                    })
                    .collect(),
                Describe::FirstOnly => self
                    .addrs
                    .first()
                    .map(|addr| ServerSpec::host_from_url(addr))
                    .into_iter()
                    .collect(),
                Describe::Nothing => Vec::new(),
            };
            ServerSpec::new(hosts.join(","), "nats")
        }
    }

    fn cluster(describe: Describe) {
        describes_addresses_without_credentials(
            |addrs| Cluster {
                addrs: addrs.iter().map(|addr| (*addr).to_owned()).collect(),
                describe,
            },
            "nats",
        );
    }

    #[test]
    fn a_cluster_described_host_by_host_passes() {
        cluster(Describe::EveryHost);
    }

    #[test]
    #[should_panic(expected = "carries their userinfo")]
    fn a_cluster_that_strips_only_the_first_address_fails() {
        cluster(Describe::FirstStrippedOnly);
    }

    #[test]
    fn a_cluster_described_by_its_first_address_passes() {
        cluster(Describe::FirstOnly);
    }

    #[test]
    #[should_panic(expected = "names none of the configured addresses")]
    fn a_cluster_that_describes_no_address_fails() {
        cluster(Describe::Nothing);
    }
}
