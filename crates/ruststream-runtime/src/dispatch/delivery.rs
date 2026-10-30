//! What one subscription dispatches under, and the slot one delivery waits in on its way to
//! the settle.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use tokio_util::task::TaskTracker;

#[cfg(doc)]
use crate::IncomingMessage;
use crate::RetryDeclaration;
#[cfg(doc)]
use crate::context::Context;
use crate::host::Detach;
use crate::lifecycle::BoxFuture;
#[cfg(all(test, feature = "poll-diagnostics"))]
use crate::poll_diagnostics::PollDiagnostics;
#[cfg(feature = "poll-diagnostics")]
use crate::poll_diagnostics::PollSampler;
#[cfg(test)]
use crate::redelivery::ErasedRetryPublisher;
use crate::redelivery::{DeferredRetry, ScopeDelivery};
#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;
#[cfg(test)]
use crate::testkit::Tracked;

/// Per-subscription publish context threaded into every delivery's [`Context`]: the
/// broker-agnostic `retry_after` fallback of this registration and the runner that runs what a
/// delivery leaves behind. An `and_after` continuation is handed to the runner, which tracks it
/// so a graceful shutdown drains it.
#[doc(hidden)]
pub struct Delivery<C = ()> {
    /// The retry path of this subscription: the publisher a copy of a delivery leaves through,
    /// with the destination the mount site or the descriptor named. `None` where the broker moves
    /// the delivery itself, in which case a `NackAfter` on a transport with no native delayed
    /// redelivery has nothing left to do but requeue.
    pub(crate) retry: Option<DeferredRetry<C>>,
    /// What the mount site declared with `max_attempts(..)` and `dead_letter(..)`. Read only
    /// where the runtime is the one moving the delivery.
    pub(crate) declaration: RetryDeclaration,
    /// The runner that runs what a delivery leaves behind (a continuation, a post-settle hook,
    /// the timer of a deferred retry copy), where the delivery runs: the subscription's own, or
    /// under `threads(n)` the dedicated thread's, which waits for it before it ends.
    pub(crate) host: Arc<dyn Detach>,
    /// The harness's recording-and-quiescence hooks for this scope. Empty (uninstalled) outside a
    /// [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) run, so the per-delivery read is a single atomic load.
    #[cfg(feature = "testing")]
    pub(crate) hooks: Arc<TestHooks>,
    /// This broker's registration index, used to scope recorded deliveries per broker.
    #[cfg(feature = "testing")]
    pub(crate) scope_id: usize,
    /// The subscription of the app this delivery comes through, identifying its records.
    #[cfg(feature = "testing")]
    pub(crate) subscription: usize,
    /// Times every Nth poll of this subscription's handler.
    #[cfg(feature = "poll-diagnostics")]
    pub(crate) poll: PollSampler,
}

impl<C> Delivery<C> {
    /// The context one subscription dispatches under: its own retry path over what the whole
    /// scope shares.
    #[cfg_attr(not(feature = "poll-diagnostics"), allow(unused_variables))]
    pub(crate) fn for_subscription(
        scope: &ScopeDelivery,
        subscription: &str,
        retry: Option<DeferredRetry<C>>,
        declaration: RetryDeclaration,
    ) -> Self {
        Self {
            retry,
            declaration,
            host: Arc::clone(scope.host()),
            #[cfg(feature = "testing")]
            hooks: Arc::clone(scope.hooks()),
            #[cfg(feature = "testing")]
            scope_id: scope.scope_id(),
            #[cfg(feature = "testing")]
            subscription: scope.subscription(),
            #[cfg(feature = "poll-diagnostics")]
            poll: scope.poll().register(subscription),
        }
    }

    /// A delivery context outside any scope, for tests that drive the dispatch functions
    /// directly.
    #[cfg(test)]
    pub(crate) fn detached(retry: Option<DeferredRetry<C>>, tasks: TaskTracker) -> Self {
        Self {
            retry,
            declaration: RetryDeclaration::new(),
            host: Arc::new(Tracked(tasks)),
            #[cfg(feature = "testing")]
            hooks: Arc::new(TestHooks::detached()),
            #[cfg(feature = "testing")]
            scope_id: 0,
            #[cfg(feature = "testing")]
            subscription: 0,
            #[cfg(feature = "poll-diagnostics")]
            poll: PollDiagnostics::new().register("detached"),
        }
    }

    /// A delivery context publishing its retry copies through `publisher` at `destination`. For
    /// tests.
    #[cfg(test)]
    pub(crate) fn deferring_to(
        publisher: Arc<dyn ErasedRetryPublisher<C>>,
        destination: &str,
        tasks: TaskTracker,
    ) -> Self {
        Self::detached(
            Some(DeferredRetry {
                publisher,
                destination: Some(Arc::from(destination)),
            }),
            tasks,
        )
    }

    /// The same context with a declaration on it, as a mount site's `max_attempts(..)` and
    /// `dead_letter(..)` leave it. For tests.
    #[cfg(test)]
    pub(crate) fn declaring(mut self, declaration: RetryDeclaration) -> Self {
        self.declaration = declaration;
        self
    }

    /// An empty delivery context: no deferred retry, a fresh continuation tracker. For tests.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::with_tasks(TaskTracker::new())
    }

    /// An empty delivery context carrying a caller-owned continuation tracker, so a test can
    /// observe the post-settle continuations spawned through it.
    #[cfg(test)]
    pub(crate) fn with_tasks(tasks: TaskTracker) -> Self {
        Self::detached(None, tasks)
    }
}

impl<C> Delivery<C> {
    /// Hands work the delivery leaves behind (a continuation, a post-settle hook) to the runner,
    /// which runs it where the delivery runs, tracked so a graceful shutdown waits for it.
    pub(crate) fn spawn_after(&self, work: BoxFuture<'static, ()>) {
        self.host.detach(work);
    }

    /// Hands the runner work to run once `delay` has passed: the timer of a deferred copy.
    pub(crate) fn spawn_later(&self, delay: Duration, work: BoxFuture<'static, ()>) {
        self.host.detach_after(delay, work);
    }

    /// The same context for one dedicated thread, whose runner `host` runs what the thread's
    /// deliveries leave behind.
    #[doc(hidden)]
    #[must_use]
    pub fn on_thread(&self, host: Arc<dyn Detach>) -> Self {
        Self {
            retry: self.retry.as_ref().map(|retry| DeferredRetry {
                publisher: Arc::clone(&retry.publisher),
                destination: retry.destination.clone(),
            }),
            declaration: self.declaration.clone(),
            host,
            #[cfg(feature = "testing")]
            hooks: Arc::clone(&self.hooks),
            #[cfg(feature = "testing")]
            scope_id: self.scope_id,
            #[cfg(feature = "testing")]
            subscription: self.subscription,
            #[cfg(feature = "poll-diagnostics")]
            poll: self.poll.for_thread(),
        }
    }
}

impl<C> fmt::Debug for Delivery<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field(
                "retry_destination",
                &self.retry.as_ref().map(|retry| &retry.destination),
            )
            .field("declaration", &self.declaration)
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

/// One delivery, between the stream that yielded it and the settle that consumes it.
///
/// Everything on the path borrows the delivery out of the slot and the settle takes it, so a
/// delivery is copied twice whatever its size: once out of the stream, once into
/// [`ack(self)`](IncomingMessage::ack). The futures in between hold a pointer to the slot rather
/// than the value, which is what keeps a pinned dispatch the same size for a 16-byte delivery and
/// a 16-kilobyte one.
///
/// The inside is an `Option` because Rust cannot move a value out of a place its caller owns. A
/// slot is never empty on the dispatch path: the sequential loop's one slot is filled by the poll
/// that takes a delivery off the stream and emptied by the settle, and a pooled worker's is filled
/// where it is declared; taking the delivery is the last thing that happens to it.
#[doc(hidden)]
#[derive(Debug)]
pub struct Slot<M>(Option<M>);

/// What a slot says when it is read after the settle has taken the delivery.
const SETTLED: &str = "the delivery has already been settled";

impl<M> Slot<M> {
    /// The slot holding `msg`.
    #[doc(hidden)]
    pub const fn new(msg: M) -> Self {
        Self(Some(msg))
    }

    /// A slot with nothing in it yet: the sequential loop's own, which the stream fills in place.
    pub(super) const fn empty() -> Self {
        Self(None)
    }

    /// Puts the next delivery in the slot.
    pub(super) fn fill(&mut self, msg: M) {
        self.0 = Some(msg);
    }

    /// The delivery, for everything that only reads it.
    ///
    /// # Panics
    ///
    /// Panics once the settle has taken the delivery, which is the last thing the dispatch path
    /// does with a slot.
    pub(crate) fn get(&self) -> &M {
        self.0.as_ref().expect(SETTLED)
    }

    /// The delivery, for the settle that consumes it.
    ///
    /// # Panics
    ///
    /// Panics if called twice; see [`Slot::get`].
    pub(crate) fn take(&mut self) -> M {
        self.0.take().expect(SETTLED)
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::RejectingPublisher;
    use super::*;
    use crate::redelivery::bare_retry_publisher;

    #[test]
    fn the_delivery_debug_form_reports_wiring_without_leaking_the_publisher() {
        let empty = format!("{:?}", Delivery::<()>::empty());
        assert!(empty.contains("retry_destination: None"), "{empty}");
        assert!(empty.contains("pending_continuations: 0"), "{empty}");

        let wired: Delivery = Delivery::deferring_to(
            bare_retry_publisher(RejectingPublisher),
            "orders",
            TaskTracker::new(),
        );
        assert!(format!("{wired:?}").contains("retry_destination: Some(Some(\"orders\"))"));
    }
}
