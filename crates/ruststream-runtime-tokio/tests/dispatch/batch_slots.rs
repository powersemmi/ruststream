//! A batch subscription collects every batch into a slot the runtime allocated once per worker:
//! the broker is lent the same storage for each fill, whatever the number of batches.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]

use std::future::ready;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryMessage, MemorySubscriber};
use ruststream::testing::TestApp;
use ruststream::{
    AddressedCopies, BatchStream, BatchSubscriber, RedeliveryAddress, RedeliveryAddressed,
    Seekable, Subscribe, Subscriber, SubscriptionSource,
};
use serde::{Deserialize, Serialize};

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

/// Six jobs in the log before the subscription opens, so the opening replay delivers them as three
/// batches of two.
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
async fn a_sequential_loop_fills_one_slot_for_every_batch() {
    let storage = Storage::default();
    let app = RustStream::new(AppInfo::new("slots", "0.1.0")).with_broker(replayed().await, |b| {
        b.include(
            sequential
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
        .subscriber("sequential")
        .assert_batch_sizes(&[2, 2, 2])
        .settled(HandlerOutcome::ack());
    let storage = storage.lock().expect("storage").clone();
    assert_eq!(storage.len(), 3);
    assert!(
        storage.iter().all(|at| *at == storage[0]),
        "each batch was collected into storage of its own: {storage:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_fills_one_slot_per_worker() {
    let storage = Storage::default();
    let app = RustStream::new(AppInfo::new("slots", "0.1.0")).with_broker(replayed().await, |b| {
        b.include(
            pooled
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
        .subscriber("pooled")
        .assert_called(3)
        .settled(HandlerOutcome::ack());
    let mut storage = storage.lock().expect("storage").clone();
    assert_eq!(storage.len(), 3);
    storage.sort_unstable();
    storage.dedup();
    assert!(
        storage.len() <= 2,
        "three batches on a crew of two used {} slots",
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
