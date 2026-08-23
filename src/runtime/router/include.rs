//! The `include` family on [`Router`]: mounting macro-generated definitions.
//!
//! `include` is the one entry point for every definition form, single-message and batch alike;
//! which machinery runs is picked by the definition's form token ([`IncludeDef::Form`]), exactly
//! as on a [`BrokerScope`](crate::runtime::BrokerScope). Forms that take an attachment hand back
//! a registration builder; because a router is a consuming builder, the builder commits through
//! an explicit terminal (`.publisher(policy)`, `.mount()`, `.out(marker, policy)` per slot) and
//! returns the grown router.
//!
//! The subscription source always comes from the definition: `#[subscriber(..)]` takes the
//! broker's own source expression, builder chain included, so there is nothing to override from
//! the mount site.

use serde::de::DeserializeOwned;

use crate::{BatchSubscriber, Broker, Connected, SubscriptionSource};

use crate::runtime::batch::SliceHandler;
use crate::runtime::metadata::HandlerMetadata;

use super::SubscribedBatchRouter;
use super::builder::Router;
use super::mount::{MountCodec, RouterMount};
use crate::runtime::settings::Declared;

impl<B: Broker + 'static, Routes, RouteCodec, RouteLayers>
    Router<B, Routes, RouteCodec, RouteLayers>
{
    /// Mounts a `#[subscriber]` definition of any form, on the source the definition names: a
    /// plain or batch handler grows the router directly, a `publish("dest")` or `Out`-taking one
    /// hands back a registration builder to finish with `.publisher(policy)`, `.mount()`, or
    /// `.out(marker, policy)` per slot.
    ///
    /// A batch definition's subscriber must implement [`BatchSubscriber`] - natively, or through
    /// the [`Buffered`](crate::Buffered) adapter; router and app middleware wrap per-message
    /// handlers and do not apply to batch registrations.
    ///
    /// Decoding uses the chain's codec when one was set with
    /// [`with_codec`](Router::with_codec), else the
    /// [`DefaultCodec`](crate::codec::DefaultCodec). The router-level counterpart of
    /// [`BrokerScope::include`](crate::runtime::BrokerScope::include).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # mod demo {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{HandlerResult, Router, RouterDef};
    /// use ruststream::subscriber;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: u64 }
    ///
    /// #[subscriber("orders")]
    /// async fn handle(order: &Order) -> HandlerResult {
    ///     let _ = order.id;
    ///     HandlerResult::Ack
    /// }
    ///
    /// fn routes() -> impl RouterDef<MemoryBroker> {
    ///     Router::<MemoryBroker>::new().include(handle)
    /// }
    /// # }
    /// ```
    pub fn include<D>(
        self,
        def: D,
    ) -> <D::Form as RouterMount<B, Routes, RouteCodec, RouteLayers, D::Settings>>::Out
    where
        D: Declared,
        D::Form: RouterMount<B, Routes, RouteCodec, RouteLayers, D::Settings>,
    {
        <D::Form as RouterMount<B, Routes, RouteCodec, RouteLayers, D::Settings>>::begin(
            def.declare(),
            self,
        )
    }

    /// Attaches a slice handler to a batch subscription described by `source`, decoding each
    /// element with the chain's codec (or the [`DefaultCodec`](crate::codec::DefaultCodec)).
    ///
    /// The functional-path counterpart of mounting a `batch(..)` definition with
    /// [`include`](Self::include): `handler` is
    /// any [`SliceHandler`](crate::runtime::SliceHandler), typically a closure
    /// `|batch: &[T], ctx: &mut Context| async { .. }`. The source's subscriber must implement
    /// [`BatchSubscriber`] - natively, or through the [`Buffered`](crate::Buffered) adapter.
    /// Set the dispatch concurrency with [`workers`](Router::workers) on the returned router.
    pub fn subscribe_batch<Source, T, H>(
        self,
        source: Source,
        handler: H,
        meta: HandlerMetadata,
    ) -> SubscribedBatchRouter<B, Source, T, RouteCodec::Codec, H, RouteCodec, RouteLayers, Routes>
    where
        RouteCodec: MountCodec,
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        T: DeserializeOwned + Send + Sync + 'static,
        H: SliceHandler<T> + 'static,
    {
        let codec = self.codec.mount_codec();
        self.push_batch_route(source, handler, codec, meta)
    }
}
