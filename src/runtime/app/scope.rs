//! The per-broker handler registration scope.

use std::{error::Error as StdError, fmt, future::Future, sync::Arc};

use crate::{Broker, Connected, Subscriber};

use crate::PublishPolicy;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::lifecycle::ConnectedSlot;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{BlanketLayer, Identity};
use crate::runtime::publish::{PublishIdentity, PublishPipeline};
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
    ///             publisher.publish(msg, None).await
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

    /// Attaches `handler` (wrapped with the app's global stack) to an already-created
    /// `subscriber`.
    ///
    /// Machinery, not the user path - see [`Router::handle`](crate::runtime::Router::handle);
    /// a service mounts definitions with [`include`](Self::include) and the value constructors.
    pub fn handle<S, H, Cx>(&mut self, subscriber: S, handler: H, meta: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        S::Message: Send + Sync + 'static,
        State: Send + Sync + 'static,
        Cx: crate::BuildContext<S::Message> + Send + 'static,
        H: Handler<S::Message, Cx, State> + 'static,
        Layers: BlanketLayer + Clone + Send + Sync + 'static,
    {
        let handler = self.global.apply::<S::Message, Cx, State, H>(handler);
        // A directly mounted subscriber names no source, so no deferred-retry position can be
        // bound on it: nothing reports where a redelivery of it would be published.
        self.sink
            .push_handle(subscriber, handler, meta, FailurePolicies::default(), None);
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
    use crate::memory::MemoryBroker;
    use crate::runtime::{AppInfo, RustStream};

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
