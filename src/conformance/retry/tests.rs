//! The checks against the in-memory broker, and against doubles broken the way a broker's retry
//! path breaks, so each check stays proven to fail where it should.

use std::{
    convert::Infallible,
    future::{Future, ready},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{Stream, StreamExt, stream};
use tokio::{runtime::Handle, sync::Mutex as AsyncMutex};

use super::{CONTROL, EXTRA_DELIVERIES, broker_moves, redelivery_address};
use crate::{
    AckError, AddressedCopies, BrokerMoves, BytesMut, DeclareRetryError, HeaderMap,
    IncomingMessage, Name, OutgoingMessage, Publisher, RedeliveryAddress, RedeliveryAddressed,
    RetryDeclaration, Subscribe, Subscriber, SubscriptionSource, Take,
    memory::{
        ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublisher,
        MemorySource, MemorySubscriber,
    },
    nonzero,
    runtime::RETRY_COUNT_HEADER,
};

// The factories stay closures throughout: their bounds are higher-ranked, which a method path
// does not satisfy.

// --8<-- [start:redelivery_address]
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_memory_descriptor_passes() {
    redelivery_address(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_memory_bare_name_passes() {
    redelivery_address(
        MemoryBroker::new,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}
// --8<-- [end:redelivery_address]

/// Two subscriptions sharing their deliveries are a group, and a copy reaching one of them is
/// what the check expects of one.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_that_shares_its_deliveries_passes() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Shared, Counting::Honest),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the copy never arrived")]
async fn an_address_that_reaches_nothing_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::Honest).misaddressed(),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "exactly one consumer of a group gets a retry copy")]
async fn a_copy_that_reaches_every_member_of_a_group_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::SharedWithCopiesToAll, Counting::Honest),
        |connected| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lost the header x-conformance-copy")]
async fn a_publisher_that_drops_headers_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |connected| StripsHeaders(connected.publisher(), None),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "lost x-ruststream-retry-count")]
async fn a_publisher_that_drops_the_retry_count_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |connected| StripsHeaders(connected.publisher(), Some(RETRY_COUNT_HEADER)),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the copy never arrived")]
async fn a_publisher_that_sends_on_the_callers_runtime_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |connected| SpawnsOnCaller(connected.publisher()),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "the control message published to the descriptor's subject never arrived"
)]
async fn a_publisher_bound_to_its_first_callers_runtime_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |connected| AttachesOnFirstUse(connected.publisher(), OnceLock::new()),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "redelivery_count on the copy's first delivery must be 1")]
async fn a_count_from_zero_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::FromZero),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "redelivery_count on a redelivery after nack(true) must be 2")]
async fn a_count_that_does_not_grow_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::Stuck),
        |connected| connected.publisher(),
    )
    .await;
}

/// A requeue that loses the headers leaves the runtime nothing to count the attempts with where the
/// transport reports no delivery count.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "the copy redelivered after nack(true) lost the header x-conformance-copy"
)]
async fn a_requeue_that_drops_the_headers_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| {
            Probe::new(name, Reading::Own, Counting::Silent).settling(Settling::RequeueDropsHeaders)
        },
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_that_moves_at_the_cap_passes() {
    for attempts in [nonzero!(1u32), nonzero!(2u32), nonzero!(3u32)] {
        broker_moves(
            MemoryBroker::new,
            |name| Moves::new(name, Moving::AtCap),
            |connected| connected.publisher(),
            attempts,
        )
        .await;
    }
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the message was delivered 3 times")]
async fn a_broker_that_ignores_the_declaration_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::Never),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the message was delivered 3 times")]
async fn a_broker_that_moves_one_delivery_late_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::PastCap),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "opened its subscription")]
async fn a_broker_that_accepts_half_a_declaration_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::AcceptsHalves),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

/// A transport with no requeue answers `Unsupported`, and has no attempt to count.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_without_requeue_passes() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::NoRequeue),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "must succeed or be unsupported, got: Broker")]
async fn a_broker_whose_requeue_fails_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::RequeueFails),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "redelivery_count on a delivery the broker counts against the cap must be 1"
)]
async fn a_broker_that_counts_from_zero_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::CountsFromZero),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

/// A transport that admits one reader of a subscription is checked with the one.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descriptor_admitting_one_reader_passes() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Exclusive, Counting::Honest),
        |connected| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "publishing the retry copy to the reported address")]
async fn a_publisher_that_refuses_the_copy_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |_connected| Refuses,
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "neither the control message nor the copy arrived")]
async fn a_descriptor_that_reads_nothing_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::Honest).deaf(),
        |connected| connected.publisher(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "each is published once")]
async fn a_publisher_that_sends_twice_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        |connected| Duplicates(connected.publisher()),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_whose_requeue_is_unsupported_passes() {
    redelivery_address(
        MemoryBroker::new,
        |name| {
            Probe::new(name, Reading::Own, Counting::Honest).settling(Settling::RequeueUnsupported)
        },
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nack(requeue = true) must succeed or be unsupported, got: Broker")]
async fn a_copy_whose_requeue_fails_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::Honest).settling(Settling::RequeueFails),
        |connected| connected.publisher(),
    )
    .await;
}

/// Waits the check's whole deadline for the redelivery, on a paused clock that moves at once.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(start_paused = true)]
#[should_panic(expected = "a copy requeued with nack(true) did not come back")]
async fn a_requeue_that_loses_the_copy_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::Honest).settling(Settling::RequeueLoses),
        |connected| connected.publisher(),
    )
    .await;
}

/// A log that rewinds to the requeued copy delivers the settled control message again first.
#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeue_that_rewinds_past_the_control_passes() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::Honest).settling(Settling::RequeueRewinds),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "deliveries of the control message arrived and none of the requeued copy"
)]
async fn a_requeue_that_redelivers_only_the_control_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::Honest).settling(Settling::RequeueFloods),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = ": ack must succeed or be unsupported, got: Broker")]
async fn a_failing_ack_fails() {
    redelivery_address(
        MemoryBroker::new,
        |name| Probe::new(name, Reading::Own, Counting::Honest).settling(Settling::AckFails),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "was refused")]
async fn a_broker_that_refuses_the_full_declaration_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::RefusesDeclaration),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "did not open")]
async fn a_broker_that_refuses_the_declared_subscription_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::RefusesSubscription),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the message reached the dead-letter destination after 1 deliveries")]
async fn a_broker_that_moves_one_delivery_early_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::BeforeCap),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "it arrived again on the dead-letter destination")]
async fn a_broker_that_moves_twice_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::MovesTwice),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

#[allow(clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "it arrived again on the subscription")]
async fn a_broker_that_delivers_a_moved_message_again_fails() {
    broker_moves(
        MemoryBroker::new,
        |name| Moves::new(name, Moving::MovesAndRedelivers),
        |connected| connected.publisher(),
        nonzero!(2u32),
    )
    .await;
}

/// How a [`Probe`]'s subscriptions read, and where its copies go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reading {
    /// Each subscription reads the name on its own, as the in-memory broker does.
    Own,
    /// The subscriptions of one descriptor share one reader, as a queue's consumers do.
    Shared,
    /// A group as above, whose reported address reaches every member: the defect of a stand-in
    /// that fans a publish out to competing consumers.
    SharedWithCopiesToAll,
    /// One subscription reads the name, and the descriptor refuses a second one, as a socket the
    /// first one bound does.
    Exclusive,
}

/// What a [`Probe`]'s deliveries report as their count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Counting {
    Honest,
    /// One short on every delivery.
    FromZero,
    /// `1` on every delivery.
    Stuck,
    /// No count at all, the honest answer of a transport that counts nothing.
    Silent,
}

/// An addressed descriptor over the in-memory bus, broken on request.
#[derive(Clone)]
struct Probe {
    name: String,
    reading: Reading,
    counting: Counting,
    misaddressed: bool,
    /// Its subscriptions read a name nothing is published to.
    deaf: bool,
    settling: Settling,
    /// Whether an [`Reading::Exclusive`] descriptor's one subscription is taken.
    taken: Arc<AtomicBool>,
    /// The one reader every subscription of a sharing descriptor takes from, opened by the first.
    group: Arc<Mutex<Option<Arc<AsyncMutex<MemorySubscriber>>>>>,
}

impl Probe {
    fn new(name: &str, reading: Reading, counting: Counting) -> Self {
        Self {
            name: name.to_owned(),
            reading,
            counting,
            misaddressed: false,
            deaf: false,
            settling: Settling::Honest,
            taken: Arc::default(),
            group: Arc::default(),
        }
    }

    /// Reports an address no subscription reads.
    fn misaddressed(mut self) -> Self {
        self.misaddressed = true;
        self
    }

    /// Reads a name nothing is published to.
    fn deaf(mut self) -> Self {
        self.deaf = true;
        self
    }

    /// Settles its deliveries as `settling` says.
    fn settling(mut self, settling: Settling) -> Self {
        self.settling = settling;
        self
    }

    /// The name its subscriptions read.
    fn read(&self) -> String {
        if self.deaf {
            format!("{}.deaf", self.name)
        } else {
            self.name.clone()
        }
    }

    fn copies(&self) -> String {
        format!("{}.copies", self.name)
    }

    async fn group_reader(
        &self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<Arc<AsyncMutex<MemorySubscriber>>, MemoryError> {
        let existing = self.group.lock().expect("probe group lock").clone();
        if let Some(existing) = existing {
            return Ok(existing);
        }
        let opened = Arc::new(AsyncMutex::new(
            Subscribe::subscribe(connected, &self.name).await?,
        ));
        let mut group = self.group.lock().expect("probe group lock");
        Ok(Arc::clone(group.get_or_insert(opened)))
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for Probe {
    type Subscriber = ProbeSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<ProbeSubscriber, MemoryError> {
        let (shared, own) = match self.reading {
            Reading::Own => (
                None,
                Some(Subscribe::subscribe(connected, &self.read()).await?),
            ),
            Reading::Exclusive => {
                if self.taken.swap(true, Ordering::SeqCst) {
                    return Err(MemoryError::ShutDown);
                }
                (
                    None,
                    Some(Subscribe::subscribe(connected, &self.name).await?),
                )
            }
            Reading::Shared => (Some(self.group_reader(connected).await?), None),
            Reading::SharedWithCopiesToAll => (
                Some(self.group_reader(connected).await?),
                Some(Subscribe::subscribe(connected, &self.copies()).await?),
            ),
        };
        Ok(ProbeSubscriber {
            shared,
            own,
            counting: self.counting,
            settling: self.settling,
            name: self.name,
            publisher: connected.publisher(),
        })
    }
}

impl RedeliveryAddressed<ConnectedMemoryBroker> for Probe {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        let address = if self.misaddressed {
            format!("{}.elsewhere", self.name)
        } else if self.reading == Reading::SharedWithCopiesToAll {
            self.copies()
        } else {
            self.name.clone()
        };
        ready(Ok(RedeliveryAddress::new(address)))
    }
}

struct ProbeSubscriber {
    shared: Option<Arc<AsyncMutex<MemorySubscriber>>>,
    own: Option<MemorySubscriber>,
    counting: Counting,
    settling: Settling,
    /// What the descriptor was built from, where a rewinding requeue publishes the control again.
    name: String,
    publisher: MemoryPublisher,
}

impl Subscriber for ProbeSubscriber {
    type Message = ProbeMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<ProbeMessage, Infallible>> + Send + '_ {
        let counting = self.counting;
        let settling = self.settling;
        let name = self.name.clone();
        let publisher = self.publisher.clone();
        let shared = stream::unfold(self.shared.clone(), async move |shared| {
            let group = shared.as_ref()?;
            let item = group.lock().await.stream().next().await;
            item.map(|item| (item, shared))
        });
        let own = self.own.as_mut().map_or_else(
            || stream::pending().right_stream(),
            |own| own.stream().left_stream(),
        );
        stream::select(shared, own).map(move |item| {
            item.map(|inner| ProbeMessage {
                inner,
                counting,
                settling,
                name: name.clone(),
                publisher: publisher.clone(),
            })
        })
    }
}

struct ProbeMessage {
    inner: MemoryMessage,
    counting: Counting,
    settling: Settling,
    name: String,
    publisher: MemoryPublisher,
}

/// What a [`Probe`] that forgets headers hands out on a redelivery.
static NO_HEADERS: HeaderMap = HeaderMap::new();

impl IncomingMessage for ProbeMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        if self.settling == Settling::RequeueDropsHeaders && self.inner.redelivery_count() > Some(1)
        {
            return &NO_HEADERS;
        }
        self.inner.headers()
    }

    fn redelivery_count(&self) -> Option<u64> {
        let count = self.inner.redelivery_count();
        match self.counting {
            Counting::Honest => count,
            Counting::FromZero => count.map(|count| count - 1),
            Counting::Stuck => count.map(|_| 1),
            Counting::Silent => None,
        }
    }

    async fn ack(self) -> Result<(), AckError> {
        if self.settling == Settling::AckFails {
            return Err(AckError::Broker("the ack was lost".into()));
        }
        self.inner.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if !requeue {
            return self.inner.nack(false).await;
        }
        let control = |times: usize| async move {
            for _ in 0..times {
                self.publisher
                    .publish(OutgoingMessage::new(&self.name, CONTROL), None)
                    .await
                    .map_err(|err| AckError::Broker(Box::new(err)))?;
            }
            Ok::<(), AckError>(())
        };
        match self.settling {
            Settling::Honest | Settling::RequeueDropsHeaders | Settling::AckFails => {
                self.inner.nack(true).await
            }
            Settling::RequeueUnsupported => {
                self.inner.ack().await?;
                Err(AckError::Unsupported)
            }
            Settling::RequeueFails => Err(AckError::Broker("the requeue was lost".into())),
            Settling::RequeueLoses => self.inner.ack().await,
            Settling::RequeueRewinds => {
                control(1).await?;
                self.inner.nack(true).await
            }
            Settling::RequeueFloods => {
                control(EXTRA_DELIVERIES).await?;
                self.inner.ack().await
            }
        }
    }
}

/// How a [`Probe`]'s deliveries settle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settling {
    Honest,
    /// A requeue answers `Unsupported`, as a transport with no requeue does.
    RequeueUnsupported,
    /// Every requeue fails.
    RequeueFails,
    /// A redelivery after a requeue arrives with no headers.
    RequeueDropsHeaders,
    /// A requeue answers `Ok` and the message never comes back.
    RequeueLoses,
    /// A requeue delivers the settled control message again before the copy, as a log that
    /// rewinds to a requeued record does.
    RequeueRewinds,
    /// A requeue delivers the control message again and again, and never the copy.
    RequeueFloods,
    /// Every acknowledgement fails.
    AckFails,
}

/// A publisher that fails every publish.
struct Refuses;

impl Publisher for Refuses {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    fn publish(
        &self,
        _msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), MemoryError>> + Send {
        ready(Err(MemoryError::ShutDown))
    }
}

/// A publisher that sends every message twice.
struct Duplicates(MemoryPublisher);

impl Publisher for Duplicates {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&()>,
    ) -> Result<(), MemoryError> {
        let (name, payload, headers) = msg.into_parts();
        let twin = OutgoingMessage::produced(name, payload.clone()).with_headers(headers.clone());
        self.0.publish(twin, options).await?;
        self.0
            .publish(
                OutgoingMessage::produced(name, payload).with_headers(headers),
                options,
            )
            .await
    }
}

/// A publisher that drops one header, or every header, on the way to the bus.
struct StripsHeaders(MemoryPublisher, Option<&'static str>);

impl Publisher for StripsHeaders {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&()>,
    ) -> Result<(), MemoryError> {
        let (name, payload, mut headers) = msg.into_parts();
        match self.1 {
            Some(dropped) => {
                headers.remove(dropped);
            }
            None => headers = HeaderMap::new(),
        }
        self.0
            .publish(
                OutgoingMessage::produced(name, payload).with_headers(headers),
                options,
            )
            .await
    }
}

/// A publisher that leaves the send to a task on whichever runtime called it, and reports success
/// at once: the defect a handler on a dedicated thread loses its retry copy to.
struct SpawnsOnCaller(MemoryPublisher);

impl Publisher for SpawnsOnCaller {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), MemoryError>> + Send {
        let (name, payload, headers) = msg.into_parts();
        let name = name.to_owned();
        let publisher = self.0.clone();
        tokio::spawn(async move {
            // The task has not run by the time the caller's runtime stops.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let copy = OutgoingMessage::produced(&name, payload).with_headers(headers);
            let _ = publisher.publish(copy, None).await;
        });
        ready(Ok(()))
    }
}

/// A publisher that attaches to the runtime of its first caller and sends from there afterwards:
/// the lazily attached socket that dies with the handler thread which first used it.
struct AttachesOnFirstUse(MemoryPublisher, OnceLock<Handle>);

impl Publisher for AttachesOnFirstUse {
    type Payload = Take;
    type Error = MemoryError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&()>,
    ) -> Result<(), MemoryError> {
        let Some(attached) = self.1.get() else {
            let _ = self.1.set(Handle::current());
            return self.0.publish(msg, options).await;
        };
        let (name, payload, headers) = msg.into_parts();
        let name = name.to_owned();
        let publisher = self.0.clone();
        // Handed to the attached runtime, which runs it only while it is alive.
        drop(attached.spawn(async move {
            let later = OutgoingMessage::produced(&name, payload).with_headers(headers);
            let _ = publisher.publish(later, None).await;
        }));
        Ok(())
    }
}

/// How a [`Moves`] broker applies a declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Moving {
    /// Moves the delivery that spends the cap, and refuses half a declaration.
    AtCap,
    /// Takes the declaration and applies none of it.
    Never,
    /// Moves one delivery after the cap.
    PastCap,
    /// Opens a subscription on half a declaration.
    AcceptsHalves,
    /// Moves at the cap, and counts one short.
    CountsFromZero,
    /// Answers a requeue with `Unsupported`, as a transport with no requeue does.
    NoRequeue,
    /// Fails every requeue.
    RequeueFails,
    /// Refuses the full declaration as well as its halves.
    RefusesDeclaration,
    /// Takes the full declaration and then refuses to open the subscription.
    RefusesSubscription,
    /// Moves one delivery before the cap.
    BeforeCap,
    /// Moves at the cap, into the dead-letter destination twice.
    MovesTwice,
    /// Moves at the cap, and delivers the message on the subscription again soon after.
    MovesAndRedelivers,
}

/// A descriptor over the in-memory bus whose broker moves a spent delivery itself.
#[derive(Clone)]
struct Moves {
    name: String,
    declared: RetryDeclaration,
    moving: Moving,
}

impl Moves {
    fn new(name: &str, moving: Moving) -> Self {
        Self {
            name: name.to_owned(),
            declared: RetryDeclaration::new(),
            moving,
        }
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for Moves {
    type Subscriber = MovesSubscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<MovesSubscriber, MemoryError> {
        let full = self.declared.max_attempts().is_some() && self.declared.dead_letter().is_some();
        if full && self.moving == Moving::RefusesSubscription {
            return Err(MemoryError::ShutDown);
        }
        let inner = Subscribe::subscribe(connected, &self.name).await?;
        let rule = match (self.declared.max_attempts(), self.declared.dead_letter()) {
            (Some(cap), Some(dead)) if self.moving != Moving::Never => {
                Some((u64::from(cap.get()), dead.to_owned()))
            }
            _ => None,
        };
        Ok(MovesSubscriber {
            inner,
            rule,
            publisher: connected.publisher(),
            moving: self.moving,
        })
    }

    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.declared = declaration.clone();
        self
    }

    fn declare_retry_on(
        &self,
        _connected: &ConnectedMemoryBroker,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        let half = declaration.max_attempts().is_some() != declaration.dead_letter().is_some();
        if (half && self.moving != Moving::AcceptsHalves)
            || self.moving == Moving::RefusesDeclaration
        {
            return Err(DeclareRetryError::Broker(
                "a dead-letter policy needs the cap and the destination together".into(),
            ));
        }
        Ok(())
    }
}

struct MovesSubscriber {
    inner: MemorySubscriber,
    rule: Option<(u64, String)>,
    publisher: MemoryPublisher,
    moving: Moving,
}

impl Subscriber for MovesSubscriber {
    type Message = MovesMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<MovesMessage, Infallible>> + Send + '_ {
        let Self {
            inner,
            rule,
            publisher,
            moving,
        } = self;
        let moving = *moving;
        inner.stream().map(move |item| {
            item.map(|inner| MovesMessage {
                inner,
                rule: rule.clone(),
                publisher: publisher.clone(),
                moving,
            })
        })
    }
}

struct MovesMessage {
    inner: MemoryMessage,
    rule: Option<(u64, String)>,
    publisher: MemoryPublisher,
    moving: Moving,
}

impl IncomingMessage for MovesMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    fn redelivery_count(&self) -> Option<u64> {
        let count = self.inner.redelivery_count();
        if self.moving == Moving::CountsFromZero {
            count.map(|count| count - 1)
        } else {
            count
        }
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
        self.inner.ack()
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match self.moving {
            Moving::NoRequeue if requeue => return Err(AckError::Unsupported),
            Moving::RequeueFails if requeue => {
                return Err(AckError::Broker("the requeue was lost".into()));
            }
            _ => {}
        }
        if requeue && let Some((cap, dead)) = &self.rule {
            let count = self.inner.redelivery_count().unwrap_or(1);
            let spent = match self.moving {
                Moving::PastCap => count > *cap,
                Moving::BeforeCap => count + 1 >= *cap,
                _ => count >= *cap,
            };
            if spent {
                let moves = if self.moving == Moving::MovesTwice {
                    2
                } else {
                    1
                };
                for _ in 0..moves {
                    self.publisher
                        .publish(OutgoingMessage::new(dead, self.inner.payload()), None)
                        .await
                        .map_err(|err| AckError::Broker(Box::new(err)))?;
                }
                if self.moving == Moving::MovesAndRedelivers {
                    // Later than the move, so the dead letter is read first, and well within the
                    // quiet window the check reads after it.
                    return self.inner.nack_after(Duration::from_millis(100)).await;
                }
                return self.inner.ack().await;
            }
        }
        self.inner.nack(requeue).await
    }
}
