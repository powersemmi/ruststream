//! What one subscription dispatches under, and the slot one delivery waits in on its way to
//! the settle.

use std::fmt;
use std::sync::Arc;

#[cfg(doc)]
use crate::IncomingMessage;
use crate::RetryDeclaration;
#[cfg(doc)]
use crate::context::Context;
#[cfg(feature = "tokio")]
use crate::main_runtime::MainRuntime;
#[cfg(feature = "poll-diagnostics")]
use crate::poll_diagnostics::PollDiagnostics;
#[cfg(feature = "poll-diagnostics")]
use crate::poll_diagnostics::PollSampler;
#[cfg(test)]
use crate::redelivery::ErasedRetryPublisher;
use crate::redelivery::{DeferredRetry, ScopeDelivery};
#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;

/// Per-subscription publish context threaded into every delivery's [`Context`]: the
/// broker-agnostic `retry_after` fallback of this registration and what the mount site declared
/// about it. What a delivery leaves behind goes to the runner's
/// [`Detach`](crate::host::Detach), which the loop carries beside it.
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
    /// The runtime the app runs on, captured where the subscription opens, inside the app's
    /// startup: a handler reaches it through
    /// [`Context::main_runtime`](crate::context::Context::main_runtime) to send work there.
    /// `None` for a subscription a runner opened outside any Tokio runtime.
    #[cfg(feature = "tokio")]
    pub(crate) runtime: Option<MainRuntime>,
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
            #[cfg(feature = "tokio")]
            runtime: MainRuntime::try_current(),
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

    /// A delivery context outside any scope, with no retry path: what a runner's own tests drive
    /// the dispatch functions under.
    #[doc(hidden)]
    #[must_use]
    pub fn unscoped() -> Self {
        Self {
            retry: None,
            declaration: RetryDeclaration::new(),
            #[cfg(feature = "tokio")]
            runtime: MainRuntime::try_current(),
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

    /// A delivery context outside any scope, for tests that drive the dispatch functions
    /// directly.
    #[cfg(test)]
    pub(crate) fn detached(retry: Option<DeferredRetry<C>>) -> Self {
        Self {
            retry,
            ..Self::unscoped()
        }
    }

    /// A delivery context publishing its retry copies through `publisher` at `destination`. For
    /// tests.
    #[cfg(test)]
    pub(crate) fn deferring_to(
        publisher: Arc<dyn ErasedRetryPublisher<C>>,
        destination: &str,
    ) -> Self {
        Self::detached(Some(DeferredRetry {
            publisher,
            destination: Some(Arc::from(destination)),
        }))
    }

    /// The same context with a declaration on it, as a mount site's `max_attempts(..)` and
    /// `dead_letter(..)` leave it. For tests.
    #[cfg(test)]
    pub(crate) fn declaring(mut self, declaration: RetryDeclaration) -> Self {
        self.declaration = declaration;
        self
    }

    /// An empty delivery context: no deferred retry. For tests.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::detached(None)
    }
}

impl<C> Delivery<C> {
    /// The same context for one dedicated thread: its poll diagnostics sample the thread's
    /// handler on their own.
    #[doc(hidden)]
    #[must_use]
    pub fn on_thread(&self) -> Self {
        Self {
            retry: self.retry.as_ref().map(|retry| DeferredRetry {
                publisher: Arc::clone(&retry.publisher),
                destination: retry.destination.clone(),
            }),
            declaration: self.declaration.clone(),
            #[cfg(feature = "tokio")]
            runtime: self.runtime.clone(),
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
    use std::future::ready;

    use bytes::BytesMut;

    use super::super::dispatch;
    use super::super::fixtures::RejectingPublisher;
    use super::*;
    use crate::context::Context;
    use crate::failure::{DispatchFailure, FailurePolicies};
    use crate::handler::{Handler, HandlerOutcome};
    use crate::redelivery::bare_retry_publisher;
    use crate::testkit::{Shutdown, Tracked};
    use crate::{AckError, HeaderMap, IncomingMessage};
    use tokio_util::task::TaskTracker;

    #[test]
    fn the_delivery_debug_form_reports_wiring_without_leaking_the_publisher() {
        let empty = format!("{:?}", Delivery::<()>::empty());
        assert!(empty.contains("retry_destination: None"), "{empty}");

        let wired: Delivery =
            Delivery::deferring_to(bare_retry_publisher(RejectingPublisher), "orders");
        assert!(format!("{wired:?}").contains("retry_destination: Some(Some(\"orders\"))"));
    }

    /// A delivery whose size is its own: `BULK` is what makes one message type bigger than another,
    /// and both settle through a ready future, so what a dispatch future holds of the delivery is the
    /// only thing that can make it grow.
    struct SizedMessage<const BULK: usize> {
        headers: HeaderMap,
        /// The delivery's bytes, held inline the way a broker's message holds its record: this is
        /// what makes one message type bigger than another.
        bulk: [u8; BULK],
    }

    impl<const BULK: usize> SizedMessage<BULK> {
        fn new() -> Self {
            Self {
                headers: HeaderMap::new(),
                bulk: [0; BULK],
            }
        }
    }

    impl<const BULK: usize> IncomingMessage for SizedMessage<BULK> {
        fn payload(&self) -> &[u8] {
            &self.bulk
        }

        fn headers(&self) -> &HeaderMap {
            &self.headers
        }

        // The delivery is dropped here rather than carried into the settle future: what a settle
        // costs is the broker's business, and this one makes it nothing, so the only thing that can
        // make a dispatch future grow with `BULK` is the dispatch itself.
        fn ack(self) -> impl Future<Output = Result<(), AckError>> {
            ready(Ok(()))
        }

        fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
            ready(Ok(()))
        }
    }

    /// Acks whatever arrives, for any delivery size.
    struct AckAnything;

    impl<C, St, const BULK: usize> Handler<SizedMessage<BULK>, C, St> for AckAnything
    where
        C: Send,
        St: Send + Sync,
    {
        fn handle(
            &self,
            _msg: &SizedMessage<BULK>,
            _ctx: &mut Context<'_, C, St>,
        ) -> impl Future<Output = HandlerOutcome> + Send {
            ready(HandlerOutcome::ack())
        }
    }

    /// The size of the future one delivery of `SizedMessage<BULK>` is dispatched through.
    fn dispatch_future_size<const BULK: usize>() -> usize {
        let state = ();
        let delivery: Delivery<()> = Delivery::empty();
        let failure = DispatchFailure::new(FailurePolicies::default(), Shutdown::new());
        let mut encode = BytesMut::new();
        let mut slot = Slot::new(SizedMessage::<BULK>::new());
        let host = Tracked(TaskTracker::new());
        let dispatching = dispatch(
            &AckAnything,
            &mut slot,
            &mut encode,
            "orders",
            &state,
            &delivery,
            &host,
            &failure,
        );
        size_of_val(&dispatching)
    }

    #[test]
    fn the_dispatch_future_holds_a_pointer_to_the_delivery_rather_than_the_delivery() {
        let small = dispatch_future_size::<0>();
        let large = dispatch_future_size::<4096>();

        assert_eq!(
            small, large,
            "the dispatch future grew with the delivery, so it carries the value",
        );
        assert!(
            large < size_of::<SizedMessage<4096>>(),
            "the dispatch future is at least as large as the delivery it dispatches: {large} bytes",
        );
    }
}
