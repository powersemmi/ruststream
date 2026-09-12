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
use crate::runtime::redelivery::RetryPairing;
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
    /// The deferred-retry policy wired with [`retry_via`](Self::retry_via), as the pairing the
    /// runtime takes once the broker is connected.
    pub(super) retry: Option<RetryPairing>,
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

    /// Wires the publish policy the broker-agnostic `retry_after` fallback publishes through on
    /// this scope.
    ///
    /// When a handler returns [`HandlerOutcome::retry_after`](crate::runtime::HandlerOutcome::retry_after)
    /// (or a delivery is `nack_after`-ed) on a broker that does not natively support delayed
    /// redelivery, the runtime re-publishes the message after the delay through the publisher
    /// `policy` pairs into, with the [`RETRY_COUNT_HEADER`](crate::runtime::RETRY_COUNT_HEADER)
    /// incremented. The policy is this broker's own (`Publish::default()` from its prelude): it is
    /// paired with the connected broker at startup, before the scope's first subscription opens,
    /// so a broker that hands out no publisher before `connect` wires the fallback like any
    /// other. A policy that fails to pair aborts startup, like a reply policy that does.
    ///
    /// Where that copy goes is the subscription's own answer, read once at startup from
    /// [`SubscriptionSource::redelivery_address`](crate::SubscriptionSource::redelivery_address).
    /// A subscription name and a publish destination are one string on a subject or a topic, and
    /// separate resources on Google Pub/Sub. So a scope wired here whose subscriptions cannot
    /// report an address fails to start, naming the subscription and its source, instead of
    /// publishing copies into nothing once a handler asks for a delay.
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
    /// # #[cfg(feature = "memory")]
    /// # fn demo() {
    /// use ruststream::memory::{MemoryBroker, MemoryPublish};
    /// use ruststream::runtime::{AppInfo, RustStream};
    ///
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| {
    ///         // The broker's own publish policy; paired once the broker is connected.
    ///         b.retry_via(MemoryPublish);
    ///     });
    /// # let _ = app;
    /// # }
    /// ```
    pub fn retry_via<Policy>(&mut self, policy: Policy)
    where
        Policy: PublishPolicy<Connected<B>> + Send + 'static,
        Policy::Live: Publisher + 'static,
    {
        // The slot's type projects through `Broker::Connected`, which inference cannot walk back
        // to `B`, so the pairing names both parameters.
        self.retry = Some(RetryPairing::new::<B, Policy>(
            Arc::clone(&self.slot),
            policy,
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
    use crate::memory::{MemoryBroker, MemoryPublish};
    use crate::runtime::{AppInfo, RustStream};

    /// The deferred-retry fallback is only reachable through a broker without native delayed
    /// redelivery (the in-memory one has it), so what the scope owes at build time is the wiring:
    /// the policy handed to `retry_via` is held as a pairing the runtime takes at startup. That
    /// the pairing yields a publisher which reaches the broker is proven where the pairing lives.
    #[test]
    fn retry_via_holds_the_policy_as_a_pairing() {
        let mut wired = false;
        let _app =
            RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
                assert!(b.retry.is_none(), "a fresh scope defers nothing");
                b.retry_via(MemoryPublish);
                wired = b.retry.is_some();
            });
        assert!(wired, "retry_via must wire the deferred-retry pairing");
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
