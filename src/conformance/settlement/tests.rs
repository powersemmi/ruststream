//! The settlement checks against the reference broker, and against broken doubles of it that
//! each check must catch.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::{Stream, StreamExt};
use tokio::{runtime::Handle, time::sleep};

use super::{Answer, Answers, matches_in_process, suite};
use crate::{
    AckError, AddressedCopies, Broker, ConnectedBroker, HeaderMap, IncomingMessage,
    OutgoingMessage, RawMessage, Subscriber, SubscriptionSource,
    conformance::harness::{self, InProcessBroker},
    memory::{
        ClosedMemoryBroker, ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage,
        MemoryPublisher, MemorySource, MemorySubscriber,
    },
    testing::{Coordinator, InProcess, TestableBroker},
};

/// The one defect a double carries, each the shape of one a broker crate shipped or could ship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flaw {
    /// None: the double behaves as the memory broker does.
    Honest,
    /// `ack` answers `Ok` and hands the delivery back.
    AckRequeues,
    /// `nack(false)` answers `Ok` and hands the delivery back.
    RejectRequeues,
    /// `nack(true)` answers `Ok` and drops the delivery.
    RequeueDrops,
    /// A delivery dropped unsettled is consumed, as if acknowledged on receive.
    DropConsumes,
    /// A delivery dropped unsettled goes back through a task on the dropping caller's runtime.
    DropSpawnsOnCaller,
    /// Acknowledging a delivery consumes every earlier one of its subscription, the way a
    /// cumulative offset commit does.
    CumulativeCommit,
    /// `nack_after` redelivers at once, the way a delay rounded down to zero seconds does.
    RoundsDelayDown,
    /// The server settles nothing, and the in-process transport claims `ack` and `nack(false)`.
    ClaimsInProcess,
}

/// A broker whose deliveries carry `flaw` over the memory broker's transport.
#[derive(Debug, Clone)]
struct Flawed {
    inner: MemoryBroker,
    flaw: Flaw,
}

impl Flawed {
    fn new(flaw: Flaw) -> Self {
        Self {
            inner: MemoryBroker::new(),
            flaw,
        }
    }

    async fn connect_as(self, in_process: bool) -> Result<FlawedConnected, MemoryError> {
        let inner = self.inner.connect().await?;
        Ok(FlawedConnected {
            inner,
            flaw: self.flaw,
            in_process,
        })
    }
}

impl Broker for Flawed {
    type Error = MemoryError;
    type Connected = FlawedConnected;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.connect_as(false)
    }
}

impl InProcess for Flawed {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.connect_as(true)
    }
}

struct FlawedConnected {
    inner: ConnectedMemoryBroker,
    flaw: Flaw,
    in_process: bool,
}

impl FlawedConnected {
    fn publisher(&self) -> MemoryPublisher {
        self.inner.publisher()
    }
}

impl ConnectedBroker for FlawedConnected {
    type Error = MemoryError;
    type Closed = ClosedMemoryBroker;

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send {
        self.inner.shutdown()
    }
}

impl TestableBroker for FlawedConnected {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.inner.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.inner.inject(message);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.inner.published(name)
    }
}

#[derive(Debug, Clone)]
struct FlawedSource(MemorySource);

impl SubscriptionSource<FlawedConnected> for FlawedSource {
    type Subscriber = FlawedSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        SubscriptionSource::<ConnectedMemoryBroker>::name(&self.0)
    }

    async fn subscribe(self, connected: &FlawedConnected) -> Result<FlawedSubscriber, MemoryError> {
        let inner = self.0.subscribe(&connected.inner).await?;
        Ok(FlawedSubscriber {
            inner,
            flaw: connected.flaw,
            in_process: connected.in_process,
            next: 0,
            committed: Arc::new(AtomicU64::new(0)),
        })
    }
}

struct FlawedSubscriber {
    inner: MemorySubscriber,
    flaw: Flaw,
    in_process: bool,
    /// The sequence number the next delivery gets.
    next: u64,
    /// Under [`Flaw::CumulativeCommit`], every delivery numbered below this one is committed.
    committed: Arc<AtomicU64>,
}

impl Subscriber for FlawedSubscriber {
    type Message = FlawedMessage;
    type Error = <MemorySubscriber as Subscriber>::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let (flaw, in_process) = (self.flaw, self.in_process);
        let committed = Arc::clone(&self.committed);
        let next = &mut self.next;
        self.inner.stream().map(move |delivery| {
            delivery.map(|inner| {
                let seq = *next;
                *next += 1;
                FlawedMessage {
                    inner: Some(inner),
                    flaw,
                    in_process,
                    seq,
                    committed: Arc::clone(&committed),
                }
            })
        })
    }
}

struct FlawedMessage {
    inner: Option<MemoryMessage>,
    flaw: Flaw,
    in_process: bool,
    seq: u64,
    committed: Arc<AtomicU64>,
}

impl FlawedMessage {
    fn take(&mut self) -> MemoryMessage {
        self.inner
            .take()
            .expect("a delivery is present until it is settled")
    }

    fn present(&self) -> &MemoryMessage {
        self.inner
            .as_ref()
            .expect("a delivery is present until it is settled")
    }
}

/// Consumes a memory delivery without handing it back.
fn consume(inner: MemoryMessage) {
    let _ = inner.into_raw();
}

impl IncomingMessage for FlawedMessage {
    fn payload(&self) -> &[u8] {
        self.present().payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.present().headers()
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> + Send {
        let inner = self.take();
        let (flaw, in_process) = (self.flaw, self.in_process);
        if flaw == Flaw::CumulativeCommit {
            self.committed.fetch_max(self.seq, Ordering::SeqCst);
        }
        async move {
            match flaw {
                Flaw::AckRequeues => inner.nack(true).await,
                Flaw::ClaimsInProcess => {
                    consume(inner);
                    if in_process {
                        Ok(())
                    } else {
                        Err(AckError::Unsupported)
                    }
                }
                _ => inner.ack().await,
            }
        }
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        let inner = self.take();
        let (flaw, in_process) = (self.flaw, self.in_process);
        async move {
            match flaw {
                Flaw::RejectRequeues if !requeue => inner.nack(true).await,
                Flaw::RequeueDrops if requeue => inner.nack(false).await,
                Flaw::ClaimsInProcess => {
                    consume(inner);
                    if in_process && !requeue {
                        Ok(())
                    } else {
                        Err(AckError::Unsupported)
                    }
                }
                _ => inner.nack(requeue).await,
            }
        }
    }

    fn supports_nack_after(&self) -> bool {
        self.flaw != Flaw::ClaimsInProcess && self.present().supports_nack_after()
    }

    fn nack_after(mut self, delay: Duration) -> impl Future<Output = Result<(), AckError>> + Send {
        let inner = self.take();
        let flaw = self.flaw;
        async move {
            if flaw == Flaw::RoundsDelayDown {
                inner.nack(true).await
            } else {
                inner.nack_after(delay).await
            }
        }
    }
}

impl Drop for FlawedMessage {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        match self.flaw {
            Flaw::DropConsumes | Flaw::ClaimsInProcess => consume(inner),
            Flaw::CumulativeCommit if self.seq < self.committed.load(Ordering::SeqCst) => {
                consume(inner);
            }
            Flaw::DropSpawnsOnCaller => {
                let pending = HandedBackLater(Some(inner));
                match Handle::try_current() {
                    Ok(caller) => drop(caller.spawn(async move {
                        sleep(Duration::from_millis(20)).await;
                        pending.hand_back();
                    })),
                    Err(_) => drop(pending),
                }
            }
            // The memory broker hands an unsettled delivery back itself.
            _ => drop(inner),
        }
    }
}

/// A delivery on its way back through a task: lost when the task never gets to hand it back.
struct HandedBackLater(Option<MemoryMessage>);

impl HandedBackLater {
    fn hand_back(mut self) {
        drop(self.0.take());
    }
}

impl Drop for HandedBackLater {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            consume(inner);
        }
    }
}

async fn run_suite_on(flaw: Flaw) -> Answers {
    let broker = Flawed::new(flaw);
    suite(
        || broker.clone(),
        |name| FlawedSource(MemorySource::new(name)),
        FlawedConnected::publisher,
        Duration::ZERO,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_memory_broker_passes_and_settles_everything() {
    let broker = MemoryBroker::new();
    let answers = suite(
        || broker.clone(),
        |name| MemorySource::new(name),
        ConnectedMemoryBroker::publisher,
        Duration::ZERO,
    )
    .await;
    assert_eq!(
        answers,
        Answers {
            ack: Answer::Settled,
            reject: Answer::Settled,
            requeue: Answer::Settled,
            delay: true,
        },
    );
}

// --8<-- [start:matches_in_process]
// `make_source` stays a closure: its bound is higher-ranked, so a bare constructor path would bind
// one concrete lifetime and fail to type-check.
#[allow(clippy::redundant_closure)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_memory_broker_answers_alike_in_process() {
    // Clones of one broker: the second connection of a check reaches what the first one left.
    let broker = MemoryBroker::new();
    matches_in_process(
        || broker.clone(),
        |name| MemorySource::new(name),
        ConnectedMemoryBroker::publisher,
        // The bus hands an unsettled delivery back at once.
        Duration::ZERO,
    )
    .await;
}
// --8<-- [end:matches_in_process]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_memory_broker_per_connection_passes_too() {
    // Each connection a world of its own: nothing survives the reconnect, which is what a bus
    // without durability does, and the unsettled deliveries come back before it.
    suite(
        MemoryBroker::new,
        |name| MemorySource::new(name),
        ConnectedMemoryBroker::publisher,
        Duration::ZERO,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_honest_double_passes() {
    run_suite_on(Flaw::Honest).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "settlement ack consumes: the settled delivery came back on its \
                           subscription"
)]
async fn an_ack_that_hands_the_delivery_back_fails() {
    run_suite_on(Flaw::AckRequeues).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "settlement nack(false) drops: the settled delivery came back on its \
                           subscription"
)]
async fn a_reject_that_hands_the_delivery_back_fails() {
    run_suite_on(Flaw::RejectRequeues).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "settlement nack(true) returns: no delivery arrived")]
async fn a_requeue_that_drops_the_delivery_fails() {
    run_suite_on(Flaw::RequeueDrops).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "settlement out of order: deliveries released without a settlement \
                           never came back"
)]
async fn a_cumulative_commit_fails() {
    run_suite_on(Flaw::CumulativeCommit).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "settlement out of order: deliveries released without a settlement \
                           never came back"
)]
async fn a_drop_that_consumes_fails() {
    run_suite_on(Flaw::DropConsumes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "settlement unsettled drop: deliveries released without a settlement \
                           never came back"
)]
async fn a_drop_that_hands_back_on_the_callers_runtime_fails() {
    run_suite_on(Flaw::DropSpawnsOnCaller).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "the in-process transport answers settlements differently from the \
                           server"
)]
async fn an_in_process_transport_claiming_settlements_fails() {
    let broker = Flawed::new(Flaw::ClaimsInProcess);
    matches_in_process(
        || broker.clone(),
        |name| FlawedSource(MemorySource::new(name)),
        FlawedConnected::publisher,
        Duration::ZERO,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_settles_nothing_passes_on_its_own() {
    // Honest on the server: every settlement unsupported, and nothing to redeliver.
    let broker = Flawed::new(Flaw::ClaimsInProcess);
    let answers = suite(
        || broker.clone(),
        |name| FlawedSource(MemorySource::new(name)),
        FlawedConnected::publisher,
        Duration::ZERO,
    )
    .await;
    assert_eq!(answers.requeue, Answer::Unsupported);
    // The same double in process claims what the server refuses; the suite alone reads that as
    // a transport with acknowledgement and no requeue, which is why the comparison exists.
    let claimed = suite(
        || InProcessBroker::new(broker.clone()),
        |name| FlawedSource(MemorySource::new(name)),
        FlawedConnected::publisher,
        Duration::ZERO,
    )
    .await;
    assert_ne!(claimed, answers);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_honest_double_passes_the_delay_floor() {
    harness::lifecycle(
        || Flawed::new(Flaw::Honest),
        |name| FlawedSource(MemorySource::new(name)),
        FlawedConnected::publisher,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(
    expected = "a broker that ignores the delay or rounds it down redelivers before it runs out"
)]
async fn a_delay_rounded_down_fails_the_lifecycle() {
    harness::lifecycle(
        || Flawed::new(Flaw::RoundsDelayDown),
        |name| FlawedSource(MemorySource::new(name)),
        FlawedConnected::publisher,
    )
    .await;
}
