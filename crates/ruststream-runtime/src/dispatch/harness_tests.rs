//! What the dispatch copies of a batch for the test harness: nothing while no harness watches,
//! and one clone of each value a batch lends while one does.

use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::convert::Infallible;
use core::future::ready;
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;
use futures::{Stream, stream};
use tokio_util::task::TaskTracker;

use super::{Delivery, DispatchFailure, run_batch};
use crate::batch::CarriedBatch;
use crate::context::Context;
use crate::failure::FailurePolicies;
use crate::handler::HandlerOutcome;
use crate::testing::coordinator::Coordinator;
use crate::testkit::Shutdown;
use crate::testkit::Tracked;
use crate::{
    AckError, BatchSubscriber, CarriesBatch, HeaderMap, IncomingMessage, NamedCopies, Subscriber,
    SubscriptionSource,
};
use ruststream::memory::{ConnectedMemoryBroker, MemoryError};

/// A value that counts the clones made of it.
#[derive(Debug)]
struct Counted(Arc<AtomicUsize>);

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.0.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(&self.0))
    }
}

/// A delivery that settles at once.
struct Claim(HeaderMap);

impl IncomingMessage for Claim {
    fn payload(&self) -> &[u8] {
        &[]
    }

    fn headers(&self) -> &HeaderMap {
        &self.0
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send {
        ready(Ok(()))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send {
        ready(Ok(()))
    }
}

/// A batch that lends one counted value per delivery.
struct Page {
    values: Vec<Counted>,
    claims: Vec<Claim>,
}

impl CarriesBatch<Counted> for Page {
    fn carried(&self) -> &[Counted] {
        &self.values
    }
}

impl IntoIterator for Page {
    type Item = Claim;
    type IntoIter = vec::IntoIter<Claim>;

    fn into_iter(self) -> Self::IntoIter {
        self.claims.into_iter()
    }
}

/// The subscription such pages come from. The tests hand the dispatch a page directly, so its
/// streams stay empty.
struct Pages;

impl SubscriptionSource<ConnectedMemoryBroker> for Pages {
    type Subscriber = PageSubscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &'static str {
        "pages"
    }

    fn subscribe(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<PageSubscriber, MemoryError>> + Send {
        ready(Ok(PageSubscriber))
    }
}

struct PageSubscriber;

impl Subscriber for PageSubscriber {
    type Message = Claim;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Claim, Infallible>> + Send + '_ {
        stream::empty()
    }
}

impl BatchSubscriber for PageSubscriber {
    type Batch = Page;

    fn batches(
        &mut self,
        _size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Page, Infallible>> + Send + '_ {
        stream::empty()
    }
}

/// Dispatches one page of two counted values to a body that reads the slice and acks it, and
/// reports how many clones of the values the dispatch made.
async fn clones_made_dispatching(delivery: &Delivery<()>) -> usize {
    let clones = Arc::new(AtomicUsize::new(0));
    let page = Page {
        values: vec![Counted(Arc::clone(&clones)), Counted(Arc::clone(&clones))],
        claims: vec![Claim(HeaderMap::new()), Claim(HeaderMap::new())],
    };
    let handler = CarriedBatch::<Pages, ConnectedMemoryBroker, Counted, _>::over(
        |_values: &[Counted], _ctx: &mut Context| ready(HandlerOutcome::ack()),
    );
    run_batch(
        &Arc::new(handler),
        page,
        &mut (),
        &mut BytesMut::new(),
        "pages",
        &(),
        delivery,
        &Tracked(TaskTracker::new()),
        &DispatchFailure::new(FailurePolicies::default(), Shutdown::new()),
    )
    .await;
    clones.load(Ordering::SeqCst)
}

#[tokio::test]
async fn a_batch_dispatched_with_no_harness_watching_is_not_copied() {
    assert_eq!(clones_made_dispatching(&Delivery::empty()).await, 0);
}

#[tokio::test]
async fn the_harness_keeps_one_clone_of_each_value_a_batch_lends() {
    let delivery = Delivery::empty();
    delivery.hooks.install(Coordinator::new(16));
    assert_eq!(clones_made_dispatching(&delivery).await, 2);
}
