//! A batch subscription collects every batch into storage the runtime allocated once per worker:
//! a worker's slot and its taken batch trade places for each batch, so the broker is lent one of
//! the same two buffers for every fill, whatever the number of batches.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]

use std::convert::Infallible;
use std::future::ready;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryMessage, MemorySubscriber};
use ruststream::testing::TestApp;
use ruststream::{
    AddressedCopies, BatchStream, BatchSubscriber, RedeliveryAddress, RedeliveryAddressed,
    Seekable, Subscribe, Subscriber, SubscriptionSource,
};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

/// The message the batches carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Outgoing)]
struct Job {
    id: u32,
}

/// Where in memory each filled slot lay: one entry per batch the broker collected.
type Storage = Arc<Mutex<Vec<usize>>>;

/// The in-memory subscription, which notes the storage of every slot it is lent.
struct Probed {
    inner: MemorySubscriber<Retaining>,
    storage: Storage,
}

impl Subscriber for Probed {
    type Message = <MemorySubscriber<Retaining> as Subscriber>::Message;
    type Error = <MemorySubscriber<Retaining> as Subscriber>::Error;

    fn stream(
        &mut self,
    ) -> impl futures::Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream()
    }
}

impl BatchSubscriber for Probed {
    type Batch = Vec<MemoryMessage<Retaining>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl BatchStream<Self::Batch, Error = Self::Error> + Send + '_ {
        Probing {
            inner: Box::pin(self.inner.batches(size)),
            storage: Arc::clone(&self.storage),
        }
    }
}

impl Seekable for Probed {
    type Seeker = <MemorySubscriber<Retaining> as Seekable>::Seeker;

    fn seeker(&self) -> Self::Seeker {
        self.inner.seeker()
    }
}

struct Probing<Inner> {
    inner: Pin<Box<Inner>>,
    storage: Storage,
}

impl<Inner> Unpin for Probing<Inner> {}

impl<Inner> BatchStream<Vec<MemoryMessage<Retaining>>> for Probing<Inner>
where
    Inner: BatchStream<Vec<MemoryMessage<Retaining>>>,
{
    type Error = Inner::Error;

    fn poll_fill(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Vec<MemoryMessage<Retaining>>,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        let step = ready!(self.inner.as_mut().poll_fill(cx, slot));
        if matches!(step, Some(Ok(()))) {
            let at = slot.as_ptr() as usize;
            self.storage.lock().expect("storage").push(at);
        }
        Poll::Ready(step)
    }
}

/// The descriptor that opens it.
#[derive(Clone)]
struct Probe {
    name: &'static str,
    storage: Storage,
}

impl SubscriptionSource<ConnectedMemoryBroker<Retaining>> for Probe {
    type Subscriber = Probed;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker<Retaining>,
    ) -> Result<Probed, MemoryError> {
        Ok(Probed {
            inner: Subscribe::subscribe(connected, self.name).await?,
            storage: self.storage,
        })
    }
}

impl RedeliveryAddressed<ConnectedMemoryBroker<Retaining>> for Probe {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker<Retaining>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> {
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}

#[subscriber(Probe { name: "sequential", storage: Storage::default() })]
async fn sequential(_jobs: &[Job]) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber(Probe { name: "pooled", storage: Storage::default() }, workers(2))]
async fn pooled(_jobs: &[Job]) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber(Probe { name: "threaded", storage: Storage::default() }, threads(2))]
async fn threaded(_jobs: &[Job]) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Six jobs in the log before the subscription opens, which the opening replay delivers in batches
/// of the size the mount asks for.
async fn replayed() -> MemoryBroker<Retaining> {
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(32)));
    let publisher = broker.publisher();
    for id in 1..=6u32 {
        publisher
            .message(&Job { id })
            .to("sequential")
            .publish()
            .await
            .expect("publish");
        publisher
            .message(&Job { id })
            .to("pooled")
            .publish()
            .await
            .expect("publish");
        publisher
            .message(&Job { id })
            .to("threaded")
            .publish()
            .await
            .expect("publish");
    }
    broker
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sequential_loop_fills_two_buffers_for_every_batch() {
    let storage = Storage::default();
    let app = RustStream::new(AppInfo::new("slots", "0.1.0")).with_broker(replayed().await, |b| {
        b.include(
            sequential
                .batch(nonzero!(1))
                .map_source(|probe| Probe {
                    storage: Arc::clone(&storage),
                    ..probe
                })
                .start_at(MemoryPosition::start()),
        )
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batches settle");

    tb.broker::<MemoryBroker<Retaining>>()
        .subscriber("sequential")
        .assert_batch_sizes(&[1, 1, 1, 1, 1, 1])
        .settled(HandlerOutcome::ack());
    let mut storage = storage.lock().expect("storage").clone();
    assert_eq!(storage.len(), 6);
    storage.sort_unstable();
    storage.dedup();
    assert!(
        storage.len() <= 2,
        "six batches on one worker used {} buffers",
        storage.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_fills_two_buffers_per_worker() {
    let storage = Storage::default();
    let app = RustStream::new(AppInfo::new("slots", "0.1.0")).with_broker(replayed().await, |b| {
        b.include(
            pooled
                .batch(nonzero!(1))
                .map_source(|probe| Probe {
                    storage: Arc::clone(&storage),
                    ..probe
                })
                .start_at(MemoryPosition::start()),
        )
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batches settle");

    tb.broker::<MemoryBroker<Retaining>>()
        .subscriber("pooled")
        .assert_called(6)
        .settled(HandlerOutcome::ack());
    let mut storage = storage.lock().expect("storage").clone();
    assert_eq!(storage.len(), 6);
    storage.sort_unstable();
    storage.dedup();
    assert!(
        storage.len() <= 4,
        "six batches on a crew of two used {} buffers",
        storage.len()
    );
}

/// Slots travel to a thread with their batch and come back with its settlement: the loop collects
/// more batches than it ever allocates slots for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn threads_settle_every_batch_and_return_its_slot() {
    let storage = Storage::default();
    let app = RustStream::new(AppInfo::new("slots", "0.1.0")).with_broker(replayed().await, |b| {
        b.include(
            threaded
                .batch(nonzero!(2))
                .map_source(|probe| Probe {
                    storage: Arc::clone(&storage),
                    ..probe
                })
                .start_at(MemoryPosition::start()),
        )
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed batches settle");

    tb.broker::<MemoryBroker<Retaining>>()
        .subscriber("threaded")
        .assert_called(3)
        .assert_batch_sizes(&[2, 2, 2])
        .settled(HandlerOutcome::ack());
    assert_eq!(storage.lock().expect("storage").len(), 3);
}

/// Every batch the handler saw, as the ids it carried.
#[derive(Clone, Default, FromRef)]
struct Calls {
    seen: Arc<Mutex<Vec<Vec<u32>>>>,
}

/// Panics on its first batch, which `panic = drop` logs and skips, and acks every later one.
#[subscriber("panicky", on_failure(panic = drop))]
async fn panicky(jobs: &[Job], ctx: &mut Context<'_, (), Calls>) -> HandlerOutcome {
    let first = {
        let mut seen = ctx.state().seen.lock().expect("calls");
        seen.push(jobs.iter().map(|job| job.id).collect());
        seen.len() == 1
    };
    assert!(!first, "the first batch panics");
    HandlerOutcome::ack()
}

/// A batch that panics leaves its worker's storage empty: the delivery that went back to the
/// subscription arrives alone in the next batch, with nothing of the first one beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_after_a_panicked_one_holds_only_its_own_deliveries() {
    let calls = Calls::default();
    let app = RustStream::new(AppInfo::new("slots", "0.1.0"))
        .on_startup({
            let calls = calls.clone();
            async move |()| Ok::<_, Infallible>(calls.clone())
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(panicky.batch(nonzero!(4)))
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    timeout(Duration::from_secs(5), async {
        tb.message(&Job { id: 1 })
            .to("panicky")
            .publish()
            .await
            .expect("publish");
        tb.settle().await.expect("settle");
    })
    .await
    .expect("the batch after a panicked one settles");

    assert_eq!(*calls.seen.lock().expect("calls"), [vec![1], vec![1]]);
}
