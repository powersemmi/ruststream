//! The checks of a broker that moves a delivery itself, against the in-memory broker and against
//! a [`Moves`] double broken the way such a broker breaks.

use std::{convert::Infallible, num::NonZeroU64, time::Duration};

use futures::{Stream, StreamExt};

use crate::retry::broker_moves;
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryMessage, MemoryPublisher,
    MemorySubscriber,
};
use ruststream_core::{
    AckError, BrokerMoves, DeclareRetryError, HeaderMap, IncomingMessage, OutgoingMessage,
    Publisher, RetryDeclaration, Settle, Subscribe, Subscriber, SubscriptionSource, nonzero,
};

// The factories stay closures throughout: their bounds are higher-ranked, which a method path
// does not satisfy.

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
                Some((NonZeroU64::from(cap).get(), dead.to_owned()))
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
}

impl Settle for MovesMessage {
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
