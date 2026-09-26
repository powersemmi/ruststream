//! Each check above, proved against a broker that breaks it.
//!
//! [`Faulty`] is the memory broker with one [`Fault`] of an in-process transport: an honest one
//! passes every check, and each fault fails the check written for it, with that check's message.

use std::{
    collections::HashMap,
    future::{Future, ready},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use futures::{Stream, StreamExt};

use super::{Refusal, backlog_matches_server, refuses_like_the_server};
use crate::conformance::harness::run_suite;
use crate::memory::{
    ClosedMemoryBroker, ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage,
    MemoryPosition, MemoryPublisher, MemorySource, MemorySubscriber, Retaining, Retention,
};
use crate::testing::{Backlog, Coordinator, InProcess, TestableBroker};
use crate::{
    AckError, AddressedCopies, Broker, BytesMut, ConnectedBroker, DefaultPublish, HeaderMap,
    IncomingMessage, Name, OutgoingFor, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Seekable, Seeker, Subscribe, Subscriber, Take, nonzero,
};

/// What is wrong with a [`Faulty`] broker's transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Nothing: the memory broker as it is.
    Honest,
    /// The publish log holds what was injected and nothing the broker's own publisher sent.
    LogsInjectedOnly,
    /// The coordinator is never told of a delivery.
    Uncounted,
    /// A publish to a name nobody subscribed is counted in flight.
    CountsUnread,
    /// A delayed nack requeues at once.
    ImmediateRetry,
    /// A delayed nack reports success and never redelivers.
    LostRetry,
    /// The routing answer names one subscription of a name, while both receive every message.
    FansOutCompeting,
    /// The routing answer names the first subscription of a name for every message, while the
    /// two subscriptions of the name split the messages between them the other way round.
    SplitsAgainstItsAnswer,
    /// Against the server a subscription starts from the beginning of the log, in process it
    /// does not, and the declaration is the in-process behaviour.
    ServerKeepsBacklog,
    /// The server refuses a payload over this many bytes; the in-process transport accepts it.
    ServerRefusesOver(usize),
    /// The in-process transport refuses a payload over this many bytes; the server accepts it.
    InProcessRefusesOver(usize),
    /// Both transports refuse a publish to a name nothing subscribed.
    RefusesUnread,
    /// The server refuses a publish to a name nothing subscribed; the in-process transport
    /// takes it.
    ServerRefusesUnread,
    /// The server refuses a publish to a name nothing subscribed; the in-process transport refuses
    /// every publish.
    InProcessRefusesEverything,
    /// The server refuses a wildcard name; the in-process transport opens it.
    InProcessOpensWildcards,
    /// The broker's own publisher takes the first publish and refuses the next one.
    RefusesSecond,
    /// The broker's own publisher refuses every publish, and the log records none of them.
    RefusesOwnPublishes,
    /// The broker's own publisher refuses every publish, and the log records each all the same.
    LogsRefusedPublishes,
    /// The routing answer names every subscription, whatever the destination.
    RoutesToEveryone,
    /// A publish to a name nothing subscribed reaches every subscription, and the routing answer
    /// says so: the model of a bound queue socket, which routes by connection.
    RoutesByConnection,
    /// Every acknowledgement fails once the transport reports to a coordinator, which the
    /// in-process scenarios install and the routing scenarios do not.
    AckFailsWhenCounted,
    /// Every nack without requeue fails once the transport reports to a coordinator.
    NackFailsWhenCounted,
    /// Every requeue fails once the transport reports to a coordinator.
    RequeueFailsWhenCounted,
    /// Every delayed nack fails.
    DelayedNackFails,
    /// A delivery reports a delayed nack, and the nack answers `Unsupported` after releasing it.
    DelayedNackUnsupported,
    /// A delivery dropped unsettled is released and never comes back.
    DropForgets,
    /// The in-process transport refuses a publish to a name nothing subscribed; the server takes
    /// it.
    InProcessRefusesUnread,
    /// Both transports refuse every subscription.
    RefusesEverySubscription,
    /// Both transports keep what was published before a subscription opened, and the
    /// declaration says so.
    KeepsBacklog,
    /// Both transports refuse a publish to a name nothing subscribed after storing it, and a
    /// subscription starts from the beginning of the log.
    RefusedPublishLeaks,
    /// Both transports admit one subscription per name.
    ExclusiveNames,
    /// The in-process transport admits one subscription per name; the server admits several.
    InProcessExclusiveNames,
}

/// The memory broker with `fault` in its transport.
struct Faulty {
    bus: MemoryBroker<Retaining>,
    fault: Fault,
}

impl Faulty {
    fn new(fault: Fault) -> Self {
        Self {
            bus: MemoryBroker::retaining(Retention::Messages(nonzero!(64))),
            fault,
        }
    }

    async fn open(self, server: bool) -> Result<FaultyConnected, MemoryError> {
        Ok(FaultyConnected {
            inner: self.bus.connect().await?,
            fault: self.fault,
            server,
            coordinator: Arc::default(),
            injected: Mutex::new(Vec::new()),
            opened: Arc::default(),
        })
    }
}

impl Broker for Faulty {
    type Error = MemoryError;
    type Connected = FaultyConnected;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.open(true)
    }
}

impl InProcess for Faulty {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.open(false)
    }
}

crate::register_testable_broker!(Faulty);

/// How many subscriptions each name has, shared with the subscriptions.
type Opened = Arc<Mutex<HashMap<String, usize>>>;

struct FaultyConnected {
    inner: ConnectedMemoryBroker<Retaining>,
    fault: Fault,
    /// Connected with `connect`, which stands for the server.
    server: bool,
    coordinator: Arc<OnceLock<Coordinator>>,
    injected: Mutex<Vec<RawMessage>>,
    /// How many subscriptions each name has.
    opened: Opened,
}

impl FaultyConnected {
    fn faulty(&self, fault: Fault) -> bool {
        self.fault == fault
    }

    fn publisher(&self) -> FaultyPublisher {
        FaultyPublisher {
            inner: self.inner.publisher(),
            counts_unread: self
                .faulty(Fault::CountsUnread)
                .then(|| (Arc::clone(&self.opened), Arc::clone(&self.coordinator))),
            unread: match self.fault {
                Fault::RefusesUnread | Fault::RefusedPublishLeaks => Some(Arc::clone(&self.opened)),
                Fault::InProcessRefusesUnread if !self.server => Some(Arc::clone(&self.opened)),
                Fault::ServerRefusesUnread | Fault::InProcessRefusesEverything if self.server => {
                    Some(Arc::clone(&self.opened))
                }
                // A map of its own that no subscription enters: every name stays unread.
                Fault::InProcessRefusesEverything => Some(Opened::default()),
                _ => None,
            },
            refuses_over: match self.fault {
                Fault::ServerRefusesOver(limit) if self.server => Some(limit),
                Fault::InProcessRefusesOver(limit) if !self.server => Some(limit),
                _ => None,
            },
            refuses: match self.fault {
                Fault::RefusesSecond => Refuses::Payload(b"second"),
                Fault::RefusesOwnPublishes | Fault::LogsRefusedPublishes => Refuses::Everything,
                _ => Refuses::Nothing,
            },
            stores_refused: matches!(
                self.fault,
                Fault::LogsRefusedPublishes | Fault::RefusedPublishLeaks
            ),
            by_connection: self
                .faulty(Fault::RoutesByConnection)
                .then(|| Arc::clone(&self.opened)),
        }
    }
}

impl ConnectedBroker for FaultyConnected {
    type Error = MemoryError;
    type Closed = ClosedMemoryBroker;

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send {
        self.inner.shutdown()
    }
}

impl TestableBroker for FaultyConnected {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if !self.faulty(Fault::Uncounted) {
            self.inner.install_coordinator(coordinator.clone());
        }
        let _ = self.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let unread = !self
            .opened
            .lock()
            .expect("opened")
            .contains_key(message.name());
        if self.faulty(Fault::CountsUnread)
            && unread
            && let Some(coordinator) = self.coordinator.get()
        {
            coordinator.enqueued();
        }
        self.injected.lock().expect("injected").push(
            RawMessage::new(message.name().to_owned(), message.payload().to_vec())
                .with_headers(message.headers().clone()),
        );
        self.inner.inject(message);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        if self.faulty(Fault::LogsInjectedOnly) {
            return self
                .injected
                .lock()
                .expect("injected")
                .iter()
                .filter(|raw| raw.name() == name)
                .cloned()
                .collect();
        }
        self.inner.published(name)
    }

    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let unread = !subscriptions.contains(&destination);
        if self.faulty(Fault::RoutesToEveryone)
            || (self.faulty(Fault::RoutesByConnection) && unread)
        {
            return (0..subscriptions.len()).collect();
        }
        let routed = self.inner.routes(destination, subscriptions);
        if self.faulty(Fault::FansOutCompeting) || self.faulty(Fault::SplitsAgainstItsAnswer) {
            // Only the first subscription of each name.
            return routed
                .into_iter()
                .filter(|&position| {
                    subscriptions[..position]
                        .iter()
                        .all(|earlier| *earlier != subscriptions[position])
                })
                .collect();
        }
        routed
    }

    fn backlog(&self) -> Backlog {
        if self.faulty(Fault::KeepsBacklog) {
            Backlog::Delivered
        } else {
            self.inner.backlog()
        }
    }
}

impl Subscribe for FaultyConnected {
    type Subscriber = FaultySubscriber;
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let exclusive = (self.faulty(Fault::ExclusiveNames)
            || (self.faulty(Fault::InProcessExclusiveNames) && !self.server))
            && self.opened.lock().expect("opened").contains_key(name);
        if self.faulty(Fault::RefusesEverySubscription) || exclusive {
            return Err(MemoryError::ShutDown);
        }
        let opened = if self.faulty(Fault::InProcessOpensWildcards) && !self.server {
            name.replace('*', "any")
        } else {
            name.to_owned()
        };
        let inner = Subscribe::subscribe(&self.inner, &opened).await?;
        let from_start = match self.fault {
            Fault::ServerKeepsBacklog => self.server,
            Fault::KeepsBacklog | Fault::RefusedPublishLeaks => true,
            _ => false,
        };
        if from_start {
            inner.seeker().seek(MemoryPosition::start()).await?;
        }
        let mut opened = self.opened.lock().expect("opened");
        let count = opened.entry(name.to_owned()).or_insert(0);
        let earlier = *count;
        *count += 1;
        drop(opened);
        Ok(FaultySubscriber {
            inner,
            fault: self.fault,
            coordinator: Arc::clone(&self.coordinator),
            // The first of a name drops the even messages, the second the odd ones, once the name
            // has two.
            split: (self.faulty(Fault::SplitsAgainstItsAnswer) && earlier < 2)
                .then(|| (Arc::clone(&self.opened), name.to_owned(), earlier == 1)),
        })
    }
}

struct FaultySubscriber {
    inner: MemorySubscriber<Retaining>,
    fault: Fault,
    /// The coordinator the transport reports to, once one is installed.
    coordinator: Arc<OnceLock<Coordinator>>,
    /// Consumes the even numbered deliveries (the odd ones where the flag is set) instead of
    /// yielding them while its name has more than one subscription.
    split: Option<(Opened, String, bool)>,
}

impl Subscriber for FaultySubscriber {
    type Message = FaultyMessage;
    type Error = <MemorySubscriber<Retaining> as Subscriber>::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let fault = self.fault;
        let split = self.split.clone();
        let coordinator = Arc::clone(&self.coordinator);
        self.inner.stream().filter_map(move |delivery| {
            let Ok(inner) = delivery;
            let number = <[u8; 4]>::try_from(inner.payload()).map(u32::from_be_bytes);
            let dropped = split.as_ref().is_some_and(|(opened, name, drops_odd)| {
                opened.lock().expect("opened").get(name).copied() > Some(1)
                    && number.is_ok_and(|number| number % 2 == u32::from(*drops_odd))
            });
            if dropped {
                // Taken and never yielded: consumed, since an unsettled memory delivery that is
                // merely dropped goes back to its subscription.
                let _consumed = inner.nack(false);
                return ready(None);
            }
            ready(Some(Ok(FaultyMessage {
                inner: Some(inner),
                fault,
                counted: coordinator.get().is_some(),
            })))
        })
    }
}

struct FaultyMessage {
    /// Always present until the message is settled or dropped; an `Option` so `Drop` can take it.
    inner: Option<MemoryMessage<Retaining>>,
    fault: Fault,
    /// Delivered while the transport reported to a coordinator.
    counted: bool,
}

impl FaultyMessage {
    fn inner(&self) -> &MemoryMessage<Retaining> {
        self.inner.as_ref().expect("a live delivery")
    }

    fn into_inner(mut self) -> MemoryMessage<Retaining> {
        self.inner.take().expect("a live delivery")
    }
}

impl Drop for FaultyMessage {
    fn drop(&mut self) {
        if self.fault == Fault::DropForgets
            && let Some(inner) = self.inner.take()
        {
            let _released = inner.nack(false);
        }
    }
}

/// The error a failing settlement of a [`Faulty`] broker answers.
fn settlement_lost() -> AckError {
    AckError::Broker("the settlement was lost".into())
}

impl IncomingMessage for FaultyMessage {
    fn payload(&self) -> &[u8] {
        self.inner().payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner().headers()
    }

    async fn ack(self) -> Result<(), AckError> {
        if self.fault == Fault::AckFailsWhenCounted && self.counted {
            return Err(settlement_lost());
        }
        self.into_inner().ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match (self.fault, requeue) {
            (Fault::NackFailsWhenCounted, false) | (Fault::RequeueFailsWhenCounted, true)
                if self.counted =>
            {
                Err(settlement_lost())
            }
            _ => self.into_inner().nack(requeue).await,
        }
    }

    fn supports_nack_after(&self) -> bool {
        self.inner().supports_nack_after()
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        match self.fault {
            Fault::ImmediateRetry => self.into_inner().nack(true).await,
            Fault::LostRetry => self.into_inner().nack(false).await,
            Fault::DelayedNackFails => Err(settlement_lost()),
            Fault::DelayedNackUnsupported => {
                self.into_inner().nack(false).await?;
                Err(AckError::Unsupported)
            }
            _ => self.into_inner().nack_after(delay).await,
        }
    }
}

#[derive(Debug, Default)]
struct FaultyPublish;

impl PublishPolicy<FaultyConnected> for FaultyPublish {
    type Live = FaultyPublisher;

    fn pair(
        self,
        connected: &FaultyConnected,
    ) -> impl Future<Output = Result<Self::Live, PairError>> + Send {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for FaultyConnected {
    type Policy = FaultyPublish;
}

#[derive(Debug, thiserror::Error)]
enum FaultyError {
    #[error(transparent)]
    Memory(#[from] MemoryError),
    #[error("the payload is over the limit")]
    TooLarge,
    #[error("no subscription reads this name")]
    Unread,
    #[error("the publisher refused the publish")]
    Refused,
}

/// Which publishes a [`FaultyPublisher`] refuses whatever their destination.
#[derive(Debug, Clone, Copy)]
enum Refuses {
    Nothing,
    Everything,
    Payload(&'static [u8]),
}

struct FaultyPublisher {
    inner: MemoryPublisher,
    /// Refuses a publish to a name with no subscription in this map.
    unread: Option<Opened>,
    /// Counts a publish to a name with no subscription in flight.
    counts_unread: Option<(Opened, Arc<OnceLock<Coordinator>>)>,
    refuses_over: Option<usize>,
    refuses: Refuses,
    /// Stores a publish on the bus before it reports the refusal.
    stores_refused: bool,
    /// Sends a publish to a name with no subscription in this map to every name in it instead.
    by_connection: Option<Opened>,
}

impl Publisher for FaultyPublisher {
    type Payload = Take;
    type Error = FaultyError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let unread = |opened: &Opened| !opened.lock().expect("opened").contains_key(msg.name());
        let refusal = if self
            .refuses_over
            .is_some_and(|limit| msg.payload().len() > limit)
        {
            Some(FaultyError::TooLarge)
        } else if self.unread.as_ref().is_some_and(unread) {
            Some(FaultyError::Unread)
        } else {
            match self.refuses {
                Refuses::Everything => Some(FaultyError::Refused),
                Refuses::Payload(payload) if msg.payload() == payload => Some(FaultyError::Refused),
                Refuses::Nothing | Refuses::Payload(_) => None,
            }
        };
        if let Some(refusal) = refusal {
            if self.stores_refused {
                self.inner.publish(msg, None).await?;
            }
            return Err(refusal);
        }
        if let Some((opened, coordinator)) = &self.counts_unread
            && unread(opened)
            && let Some(coordinator) = coordinator.get()
        {
            coordinator.enqueued();
        }
        if let Some(opened) = self.by_connection.as_ref().filter(|opened| unread(opened)) {
            let names: Vec<String> = opened.lock().expect("opened").keys().cloned().collect();
            for name in names {
                let copy = OutgoingMessage::produced(&name, BytesMut::from(msg.payload()));
                self.inner.publish(copy, None).await?;
            }
            return Ok(());
        }
        Ok(self.inner.publish(msg, None).await?)
    }
}

fn faulty(fault: Fault) -> impl Fn() -> Faulty {
    move || Faulty::new(fault)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_honest_transport_passes_the_routing_suite() {
    run_suite(faulty(Fault::Honest)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the publish log must record what the broker's own publisher sent")]
async fn a_log_without_the_brokers_own_publishes_fails() {
    run_suite(faulty(Fault::LogsInjectedOnly)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must be counted in flight (`Coordinator::enqueued`) before `inject`")]
async fn a_transport_that_counts_nothing_fails() {
    run_suite(faulty(Fault::Uncounted)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a publish no subscription receives must not be counted")]
async fn a_transport_that_counts_an_unread_publish_fails() {
    run_suite(faulty(Fault::CountsUnread)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "after `nack_after` the delivery must be released")]
async fn a_delayed_nack_that_requeues_at_once_fails() {
    run_suite(faulty(Fault::ImmediateRetry)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a delayed redelivery must be counted in flight once its delay ran out")]
async fn a_delayed_nack_that_never_redelivers_fails() {
    run_suite(faulty(Fault::LostRetry)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "so exactly those subscriptions must receive it, once each")]
async fn a_routing_answer_that_differs_from_the_deliveries_fails() {
    run_suite(faulty(Fault::FansOutCompeting)).await;
}

/// Each name receives each message as often as the answer says, and yet not on the subscription
/// it names: a live `TestApp` waits on the named one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "`routes` answers [0], so exactly those subscriptions must receive it")]
async fn a_delivery_to_the_other_subscription_of_the_name_fails() {
    run_suite(faulty(Fault::SplitsAgainstItsAnswer)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_memory_broker_backlog_matches_its_server() {
    backlog_matches_server(MemoryBroker::new, ConnectedMemoryBroker::publisher).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_honest_backlog_declaration_passes() {
    backlog_matches_server(faulty(Fault::Honest), FaultyConnected::publisher).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "backlog_matches_server against the server: the in-process transport \
                           declares `Backlog::Missed`"
)]
async fn a_backlog_the_server_keeps_and_the_declaration_misses_fails() {
    backlog_matches_server(
        faulty(Fault::ServerKeepsBacklog),
        FaultyConnected::publisher,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_memory_broker_refuses_like_its_server() {
    refuses_like_the_server(
        MemoryBroker::new,
        ConnectedMemoryBroker::publisher,
        [Refusal::Subscription {
            source: MemorySource::new("orders.*"),
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn honest_refusals_pass() {
    refuses_like_the_server(
        faulty(Fault::ServerRefusesOver(8)),
        FaultyConnected::publisher,
        [Refusal::Subscription {
            source: Name::new("orders.*"),
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "refuses_like_the_server: a payload of 9 bytes to \"orders\", one over the limit: \
                the server refused it and the in-process transport accepted it"
)]
async fn an_in_process_transport_accepting_an_oversized_payload_fails() {
    refuses_like_the_server(
        faulty(Fault::ServerRefusesOver(8)),
        FaultyConnected::publisher,
        [Refusal::<Name>::PayloadOver {
            name: "orders".to_owned(),
            limit: 8,
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "refuses_like_the_server: the subscription \"orders.*\": the server refused it \
                and the in-process transport accepted it"
)]
async fn an_in_process_transport_opening_a_refused_subscription_fails() {
    refuses_like_the_server(
        faulty(Fault::InProcessOpensWildcards),
        FaultyConnected::publisher,
        [Refusal::Subscription {
            source: Name::new("orders.*"),
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "refuses_like_the_server: a payload of 9 bytes to \"orders\", one over the limit: \
                the server accepted it, so it proves nothing"
)]
async fn a_probe_the_server_accepts_fails_as_a_wrong_probe() {
    refuses_like_the_server(
        faulty(Fault::Honest),
        FaultyConnected::publisher,
        [Refusal::<Name>::PayloadOver {
            name: "orders".to_owned(),
            limit: 8,
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "pass at least one probe the server refuses")]
async fn an_empty_probe_list_fails() {
    refuses_like_the_server(
        faulty(Fault::Honest),
        FaultyConnected::publisher,
        Vec::<Refusal<Name>>::new(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "refuses_like_the_server: a payload of 9 bytes to \"orders\", one over \
                           the limit: the in-process transport refused it and the server accepted \
                           it"
)]
async fn an_in_process_transport_refusing_what_the_server_accepts_fails() {
    refuses_like_the_server(
        faulty(Fault::InProcessRefusesOver(8)),
        FaultyConnected::publisher,
        [Refusal::<Name>::PayloadOver {
            name: "orders".to_owned(),
            limit: 8,
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_both_transports_refuse_before_a_subscription_passes() {
    backlog_matches_server(faulty(Fault::RefusesUnread), FaultyConnected::publisher).await;
}

/// Both transports refuse the first publish, and the check still reads what the subscription
/// receives afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "backlog_matches_server in process: a publish to")]
async fn an_in_process_transport_refusing_every_publish_fails() {
    backlog_matches_server(
        faulty(Fault::InProcessRefusesEverything),
        FaultyConnected::publisher,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "backlog_matches_server: the server refused a publish to a name no \
                           subscription had opened"
)]
async fn a_publish_only_the_server_refuses_before_a_subscription_fails() {
    backlog_matches_server(
        faulty(Fault::ServerRefusesUnread),
        FaultyConnected::publisher,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "through the broker's own default publisher failed")]
async fn an_own_publisher_that_refuses_its_second_publish_fails() {
    run_suite(faulty(Fault::RefusesSecond)).await;
}

/// A publisher that sends nowhere from where the suite calls it refuses, and a refused publish is
/// neither logged nor counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_own_publisher_that_refuses_everything_passes() {
    run_suite(faulty(Fault::RefusesOwnPublishes)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the publish log recorded a publish the broker's own publisher refused")]
async fn a_log_of_refused_publishes_fails() {
    run_suite(faulty(Fault::LogsRefusedPublishes)).await;
}

/// The subscription of the other name is owed every message and receives none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "`routes` answers [0, 1, 2], so exactly those subscriptions must receive"
)]
async fn a_routing_answer_naming_a_subscription_that_receives_nothing_fails() {
    run_suite(faulty(Fault::RoutesToEveryone)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_routes_by_connection_passes() {
    run_suite(faulty(Fault::RoutesByConnection)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "ack must succeed or be unsupported, got: Broker")]
async fn a_failing_ack_fails() {
    run_suite(faulty(Fault::AckFailsWhenCounted)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nack must succeed or be unsupported, got: Broker")]
async fn a_failing_nack_fails() {
    run_suite(faulty(Fault::NackFailsWhenCounted)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nack must succeed or be unsupported, got: Broker")]
async fn a_failing_requeue_fails() {
    run_suite(faulty(Fault::RequeueFailsWhenCounted)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nack_after must succeed or be unsupported, got: Broker")]
async fn a_failing_delayed_nack_fails() {
    run_suite(faulty(Fault::DelayedNackFails)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_nack_answering_unsupported_after_release_passes() {
    run_suite(faulty(Fault::DelayedNackUnsupported)).await;
}

/// Dropping an unsettled delivery may release it for good; nothing is then owed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_forgets_a_dropped_delivery_passes() {
    run_suite(faulty(Fault::DropForgets)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "was refused (no subscription reads this name), and the server accepted it"
)]
async fn a_publish_only_the_in_process_transport_refuses_before_a_subscription_fails() {
    backlog_matches_server(
        faulty(Fault::InProcessRefusesUnread),
        FaultyConnected::publisher,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "backlog_matches_server against the server: subscribing to")]
async fn a_backlog_probe_whose_subscription_is_refused_fails() {
    backlog_matches_server(
        faulty(Fault::RefusesEverySubscription),
        FaultyConnected::publisher,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backlog_both_transports_keep_passes() {
    backlog_matches_server(faulty(Fault::KeepsBacklog), FaultyConnected::publisher).await;
}

/// A refused publish must not reach the subscription opened after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "the publish before the subscription opened was refused, so a subscription opened \
                after a publish must first receive \"after-subscribe\""
)]
async fn a_refused_publish_that_is_delivered_anyway_fails() {
    backlog_matches_server(
        faulty(Fault::RefusedPublishLeaks),
        FaultyConnected::publisher,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_publishes_and_conflicting_subscriptions_pass() {
    refuses_like_the_server(
        faulty(Fault::RefusesUnread),
        FaultyConnected::publisher,
        [Refusal::<Name>::Publish {
            name: "nobody.reads".to_owned(),
        }],
    )
    .await;
    refuses_like_the_server(
        faulty(Fault::ExclusiveNames),
        FaultyConnected::publisher,
        [Refusal::Conflicting {
            open: Name::new("orders"),
            refused: Name::new("orders"),
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "refuses_like_the_server against the server: subscribing to \"orders\"")]
async fn a_payload_probe_whose_subscription_is_refused_fails() {
    refuses_like_the_server(
        faulty(Fault::RefusesEverySubscription),
        FaultyConnected::publisher,
        [Refusal::<Name>::PayloadOver {
            name: "orders".to_owned(),
            limit: 8,
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "refuses_like_the_server against the server: a payload of exactly 8 bytes to \
                \"orders\" was refused"
)]
async fn a_transport_refusing_one_byte_early_fails() {
    refuses_like_the_server(
        faulty(Fault::ServerRefusesOver(7)),
        FaultyConnected::publisher,
        [Refusal::<Name>::PayloadOver {
            name: "orders".to_owned(),
            limit: 8,
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "refuses_like_the_server against the server: the subscription \"orders\" opened \
                first was refused"
)]
async fn a_conflict_probe_whose_first_subscription_is_refused_fails() {
    refuses_like_the_server(
        faulty(Fault::RefusesEverySubscription),
        FaultyConnected::publisher,
        [Refusal::Conflicting {
            open: Name::new("orders"),
            refused: Name::new("orders.eu"),
        }],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "refuses_like_the_server: a subscription to a name, a second one to it and one to \
                another name are admitted as [true, false, true] in process and as [true, true, \
                true] by the server"
)]
async fn an_in_process_transport_refusing_a_second_subscription_the_server_admits_fails() {
    refuses_like_the_server(
        faulty(Fault::InProcessExclusiveNames),
        FaultyConnected::publisher,
        [Refusal::Subscription {
            source: Name::new("orders.*"),
        }],
    )
    .await;
}
