//! The per-broker handler registration scope.

use std::{error::Error as StdError, fmt, future::Future, sync::Arc};

use crate::{Broker, Connected, Publisher, Subscriber};

use crate::PublishPolicy;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::lifecycle::ConnectedSlot;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{BlanketLayer, Identity};
use crate::runtime::publish::{PublishIdentity, PublishPipeline};
use crate::runtime::publisher_registry::ErasedPublisher;
use crate::runtime::router::{RouterDef, RouterSink};

use super::{LifecycleHook, lifecycle_hooks::box_startup_publish};

/// A handler-registration scope bound to one broker.
///
/// Handed to the [`RustStream::with_broker`](crate::runtime::RustStream::with_broker) closure. It
/// drives the same registration chain a [`Router`](crate::runtime::Router) does - `include`
/// returns a guard over one - plus the broker it is bound to and the app's global middleware
/// stack `Layers`; registrations are collected and started later, in
/// [`RustStream::run`](crate::runtime::RustStream::run).
pub struct BrokerScope<B: Broker, Layers = Identity, C = (), State = (), Pipeline = PublishIdentity>
{
    pub(super) broker: B,
    /// The slot the runtime fills with this broker's connected form at startup; shared with
    /// every starter of this scope and with the [`Bound`] tokens minted here.
    pub(super) slot: ConnectedSlot<B>,
    /// Startup publishes registered on this scope: paired against the broker and run with the
    /// app-level `after_startup` hooks, in registration order.
    pub(super) startup_hooks: Vec<LifecycleHook<State>>,
    pub(super) sink: RouterSink<B, State>,
    pub(super) pipeline: Pipeline,
    pub(super) retry_publisher: Option<Arc<dyn ErasedPublisher>>,
    pub(super) global: Layers,
    pub(super) codec: C,
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Returns the broker, for creating subscribers or publishers with its own API.
    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
    }

    /// Registers a startup publish: once every broker is connected and the subscriptions are
    /// open, `source` is paired against this scope's broker and `hook` runs with the live
    /// publisher. The scope-side home of the first message (seeding reference data, announcing
    /// readiness): the pairing happens inside, so no token leaves the closure. A failing hook
    /// aborts startup, exactly like the app-level
    /// [`after_startup`](crate::runtime::RustStream::after_startup).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # fn demo() {
    /// use ruststream::memory::{MemoryBroker, MemoryPublish};
    /// use ruststream::runtime::{AppInfo, RustStream};
    /// use ruststream::{OutgoingMessage, Publisher};
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| {
    ///         b.after_startup(MemoryPublish, async move |publisher| {
    ///             let msg = OutgoingMessage::new("announcements", b"up".as_slice());
    ///             publisher.publish(msg).await
    ///         });
    ///     });
    /// # let _ = app;
    /// # }
    /// ```
    pub fn after_startup<Source, Hook, Fut, E>(&mut self, source: Source, hook: Hook)
    where
        Source: PublishPolicy<Connected<B>> + Send + 'static,
        Source::Live: Send,
        Hook: FnOnce(Source::Live) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
        B: 'static,
    {
        self.startup_hooks
            .push(box_startup_publish::<B, State, Source, Hook, Fut, E>(
                Arc::clone(&self.slot),
                source,
                hook,
            ));
    }

    /// Wires a publisher for the broker-agnostic `retry_after` fallback on this scope.
    ///
    /// When a handler returns [`HandlerOutcome::retry_after`](crate::runtime::HandlerOutcome::retry_after)
    /// (or a delivery is `nack_after`-ed) on a broker that does not natively support delayed
    /// redelivery, the runtime re-publishes the message to its own source subject after the delay,
    /// through `publisher`, with the
    /// [`RETRY_COUNT_HEADER`](crate::runtime::RETRY_COUNT_HEADER) incremented. Pass a publisher
    /// bound to the same broker (`b.broker().publisher()`); a publish to the source subject then
    /// reaches this scope's own subscriptions.
    ///
    /// Brokers with native delayed redelivery do not need this: the runtime uses their
    /// [`nack_after`](crate::IncomingMessage::nack_after) instead. Without it, a `retry_after` on a
    /// non-native broker degrades to an immediate requeue (with a warning).
    ///
    /// # Cancel safety
    ///
    /// The fallback's deferred re-publish is at-most-once over the delay window: see
    /// [`HandlerOutcome::retry_after`](crate::runtime::HandlerOutcome::retry_after).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::BrokerScope;
    /// use ruststream::{Broker, Publisher};
    ///
    /// // Wire a deferred-retry publisher bound to the same broker as the scope.
    /// fn configure<B, P>(scope: &mut BrokerScope<B>, retry_publisher: P)
    /// where
    ///     B: Broker + 'static,
    ///     P: Publisher + 'static,
    /// {
    ///     scope.retry_via(retry_publisher);
    /// }
    /// ```
    pub fn retry_via<P>(&mut self, publisher: P)
    where
        P: Publisher + 'static,
    {
        self.retry_publisher = Some(Arc::new(publisher));
    }

    /// Attaches `handler` (wrapped with the app's global stack) to an already-created
    /// `subscriber`.
    ///
    /// Machinery, not the user path - see [`Router::handle`](crate::runtime::Router::handle);
    /// a service mounts definitions with [`include`](Self::include) and the value constructors.
    pub fn handle<S, H>(&mut self, subscriber: S, handler: H, meta: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        S::Message: Send + Sync + 'static,
        State: Send + Sync + 'static,
        H: Handler<S::Message, (), State> + 'static,
        Layers: BlanketLayer + Clone + Send + Sync + 'static,
    {
        let handler = self.global.apply::<S::Message, (), State, H>(handler);
        self.sink
            .push_handle(subscriber, handler, meta, FailurePolicies::default());
    }

    /// Mounts every registration from `router` onto this broker, wrapping each handler with the
    /// app's global middleware stack.
    ///
    /// The app's global stack must be a [`BlanketLayer`] (it applies to handlers whose concrete
    /// types the router hides), which every bundled layer and any
    /// [`Stack`](crate::runtime::Stack) of them satisfies.
    pub fn include_router<R>(&mut self, router: R)
    where
        R: RouterDef<B, State>,
        State: Send + Sync + 'static,
        Layers: BlanketLayer + Clone + Send + Sync + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
    {
        router.mount(&self.global, &self.pipeline, &mut self.sink);
    }
}

impl<B: Broker, Layers, C, State, Pipeline> fmt::Debug
    for BrokerScope<B, Layers, C, State, Pipeline>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerScope")
            .field("sink", &self.sink)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::pin::pin;

    use futures::StreamExt as _;

    use crate::memory::MemoryBroker;
    use crate::runtime::publisher_registry::ErasedPublisher;
    use crate::runtime::{AppInfo, RustStream};
    use crate::{IncomingMessage, OutgoingMessage, Subscriber};

    use super::Arc;

    /// The deferred-retry fallback is only reachable through a broker without native delayed
    /// redelivery (the in-memory one has it), so what the scope owes is the wiring: the publisher
    /// handed to `retry_via` is held erased and still reaches the broker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_via_holds_a_live_erased_publisher() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("retry.fallback");
        let publisher = broker.publisher();

        let mut fallback: Option<Arc<dyn ErasedPublisher>> = None;
        let _app = RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(broker, |b| {
            assert!(
                b.retry_publisher.is_none(),
                "a fresh scope has no fallback publisher",
            );
            b.retry_via(publisher);
            fallback = b.retry_publisher.clone();
        });

        let fallback = fallback.expect("retry_via must wire the deferred-retry publisher");
        fallback
            .publish_erased(OutgoingMessage::new("retry.fallback", b"deferred"))
            .await
            .expect("the erased fallback publish failed");

        let mut stream = pin!(subscriber.stream());
        let msg = stream
            .next()
            .await
            .expect("the fallback publish must reach the broker")
            .expect("delivery");
        assert_eq!(msg.payload(), b"deferred");
    }

    #[test]
    fn scope_debug_reports_its_registrations() {
        let _app =
            RustStream::new(AppInfo::new("dbg", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
                let rendered = format!("{b:?}");
                assert!(rendered.starts_with("BrokerScope"), "{rendered}");
                assert!(rendered.contains("sink"), "{rendered}");
            });
    }
}
