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
use crate::testing::{Coordinator, InProcess, TestableBroker};
use crate::{
    AckError, AddressedCopies, Broker, ConnectedBroker, DefaultPublish, HeaderMap, IncomingMessage,
    Name, OutgoingFor, OutgoingMessage, PairError, PublishPolicy, Publisher, RawMessage, Seekable,
    Seeker, Subscribe, Subscriber, Take, nonzero,
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
    /// The routing answer names the first subscription of a name, which never receives anything.
    OneMemberTakesAll,
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
    /// The server refuses a wildcard name; the in-process transport opens it.
    InProcessOpensWildcards,
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
                Fault::RefusesUnread => Some(Arc::clone(&self.opened)),
                Fault::ServerRefusesUnread if self.server => Some(Arc::clone(&self.opened)),
                _ => None,
            },
            refuses_over: match self.fault {
                Fault::ServerRefusesOver(limit) if self.server => Some(limit),
                Fault::InProcessRefusesOver(limit) if !self.server => Some(limit),
                _ => None,
            },
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
        let routed = self.inner.routes(destination, subscriptions);
        if self.faulty(Fault::FansOutCompeting) || self.faulty(Fault::OneMemberTakesAll) {
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
}

impl Subscribe for FaultyConnected {
    type Subscriber = FaultySubscriber;
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let opened = if self.faulty(Fault::InProcessOpensWildcards) && !self.server {
            name.replace('*', "any")
        } else {
            name.to_owned()
        };
        let inner = Subscribe::subscribe(&self.inner, &opened).await?;
        if self.faulty(Fault::ServerKeepsBacklog) && self.server {
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
            // The first of a name, once it has a second beside it.
            silent: (self.faulty(Fault::OneMemberTakesAll) && earlier == 0)
                .then(|| (Arc::clone(&self.opened), name.to_owned())),
        })
    }
}

struct FaultySubscriber {
    inner: MemorySubscriber<Retaining>,
    fault: Fault,
    /// Consumes every delivery instead of yielding it while its name has more than one
    /// subscription.
    silent: Option<(Opened, String)>,
}

impl Subscriber for FaultySubscriber {
    type Message = FaultyMessage;
    type Error = <MemorySubscriber<Retaining> as Subscriber>::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let fault = self.fault;
        let silent = self.silent.clone();
        self.inner.stream().filter_map(move |delivery| {
            let dropped = silent.as_ref().is_some_and(|(opened, name)| {
                opened.lock().expect("opened").get(name).copied() > Some(1)
            });
            if dropped {
                // Taken and never yielded: consumed, since an unsettled memory delivery that is
                // merely dropped goes back to its subscription.
                let Ok(inner) = delivery;
                let _consumed = inner.nack(false);
                return ready(None);
            }
            ready(Some(delivery.map(|inner| FaultyMessage { inner, fault })))
        })
    }
}

struct FaultyMessage {
    inner: MemoryMessage<Retaining>,
    fault: Fault,
}

impl IncomingMessage for FaultyMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.ack()
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.nack(requeue)
    }

    fn supports_nack_after(&self) -> bool {
        self.inner.supports_nack_after()
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        match self.fault {
            Fault::ImmediateRetry => self.inner.nack(true).await,
            Fault::LostRetry => self.inner.nack(false).await,
            _ => self.inner.nack_after(delay).await,
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
}

struct FaultyPublisher {
    inner: MemoryPublisher,
    /// Refuses a publish to a name with no subscription in this map.
    unread: Option<Opened>,
    /// Counts a publish to a name with no subscription in flight.
    counts_unread: Option<(Opened, Arc<OnceLock<Coordinator>>)>,
    refuses_over: Option<usize>,
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
        if self
            .refuses_over
            .is_some_and(|limit| msg.payload().len() > limit)
        {
            return Err(FaultyError::TooLarge);
        }
        if self
            .unread
            .as_ref()
            .is_some_and(|opened| !opened.lock().expect("opened").contains_key(msg.name()))
        {
            return Err(FaultyError::Unread);
        }
        if let Some((opened, coordinator)) = &self.counts_unread
            && !opened.lock().expect("opened").contains_key(msg.name())
            && let Some(coordinator) = coordinator.get()
        {
            coordinator.enqueued();
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
#[should_panic(expected = "so each name must receive it that many times")]
async fn a_routing_answer_that_differs_from_the_deliveries_fails() {
    run_suite(faulty(Fault::FansOutCompeting)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "a competing transport shares the messages between its consumers")]
async fn competing_consumers_where_the_unnamed_one_takes_all_fail() {
    run_suite(faulty(Fault::OneMemberTakesAll)).await;
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
