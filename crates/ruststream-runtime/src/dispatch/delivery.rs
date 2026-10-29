//! What one subscription dispatches under, and the slot one delivery waits in on its way to
//! the settle.

use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::LazyLock;

#[cfg(test)]
use tokio::runtime::Builder;
#[cfg(test)]
use tokio::runtime::Handle;
#[cfg(test)]
use tokio::runtime::Runtime;
use tokio_util::task::TaskTracker;

#[cfg(doc)]
use crate::IncomingMessage;
use crate::RetryDeclaration;
#[cfg(doc)]
use crate::context::Context;
use crate::main_runtime::MainRuntime;
#[cfg(all(test, feature = "poll-diagnostics"))]
use crate::poll_diagnostics::PollDiagnostics;
#[cfg(feature = "poll-diagnostics")]
use crate::poll_diagnostics::PollSampler;
#[cfg(test)]
use crate::redelivery::ErasedRetryPublisher;
use crate::redelivery::{DeferredRetry, ScopeDelivery};
#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;

/// Per-subscription publish context threaded into every delivery's [`Context`]: the
/// broker-agnostic `retry_after` fallback of this registration and the app-wide tracker for
/// post-settle continuations.
/// An `and_after` continuation is spawned onto `tasks` so a graceful shutdown drains it.
pub(crate) struct Delivery<C = ()> {
    /// The retry path of this subscription: the publisher a copy of a delivery leaves through,
    /// with the destination the mount site or the descriptor named. `None` where the broker moves
    /// the delivery itself, in which case a `NackAfter` on a transport with no native delayed
    /// redelivery has nothing left to do but requeue.
    pub(crate) retry: Option<DeferredRetry<C>>,
    /// What the mount site declared with `max_attempts(..)` and `dead_letter(..)`. Read only
    /// where the runtime is the one moving the delivery.
    pub(crate) declaration: RetryDeclaration,
    /// Per-scope task tracker for post-settle `and_after` continuations. The
    /// dispatcher spawns each element's continuation onto it after settling, so a graceful
    /// shutdown drains them.
    pub(crate) tasks: TaskTracker,
    /// The runtime the app connected its brokers on, captured when the subscription opens. A
    /// handler reaches it through [`Context::main_runtime`](crate::context::Context::main_runtime)
    /// to send work there. What the delivery leaves behind (a continuation, the timer of a
    /// delayed retry copy) does not run here: it runs where the delivery runs, which under
    /// `threads(n)` is the dedicated thread's runtime (see [`Delivery::spawn_after`]).
    pub(crate) runtime: MainRuntime,
    /// On a dedicated thread (`threads(n)`), the thread's own tracker: the thread waits for what
    /// its deliveries left behind before it ends, so a shutdown does not drop it with the
    /// thread's runtime. `None` everywhere else.
    pub(crate) thread_tasks: Option<TaskTracker>,
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
            tasks: scope.tasks().clone(),
            // A subscription opens inside the app's startup, on the runtime it connected on.
            runtime: MainRuntime::current(),
            thread_tasks: None,
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
            tasks,
            runtime: MainRuntime::new(test_runtime()),
            thread_tasks: None,
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
    /// Spawns work the delivery leaves behind (a continuation, a post-settle hook, the timer of a
    /// deferred copy) on the runtime the delivery runs on, tracked so a graceful shutdown waits
    /// for it.
    pub(crate) fn spawn_after<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        match &self.thread_tasks {
            None => drop(self.tasks.spawn(work)),
            Some(own) => drop(self.tasks.spawn(own.track_future(work))),
        }
    }

    /// The same context for one dedicated thread, whose leftovers `own` tracks as well.
    pub(super) fn on_thread(&self, own: TaskTracker) -> Self {
        Self {
            retry: self.retry.as_ref().map(|retry| DeferredRetry {
                publisher: Arc::clone(&retry.publisher),
                destination: retry.destination.clone(),
            }),
            declaration: self.declaration.clone(),
            tasks: self.tasks.clone(),
            runtime: self.runtime.clone(),
            thread_tasks: Some(own),
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

/// The runtime a delivery context built by a unit test answers `main_runtime` with: the test's
/// own where it runs inside one, and a shared idle one where a synchronous test builds a context.
#[cfg(test)]
fn test_runtime() -> Handle {
    static IDLE: LazyLock<Runtime> = LazyLock::new(|| {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds")
    });
    Handle::try_current().unwrap_or_else(|_| IDLE.handle().clone())
}

impl<C> fmt::Debug for Delivery<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field(
                "retry_destination",
                &self.retry.as_ref().map(|retry| &retry.destination),
            )
            .field("declaration", &self.declaration)
            .field("pending_continuations", &self.tasks.len())
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
pub(crate) struct Slot<M>(Option<M>);

/// What a slot says when it is read after the settle has taken the delivery.
const SETTLED: &str = "the delivery has already been settled";

impl<M> Slot<M> {
    /// The slot holding `msg`.
    pub(crate) const fn new(msg: M) -> Self {
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
