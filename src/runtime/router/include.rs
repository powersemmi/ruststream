//! The `include` family on [`Router`]: mounting macro-generated definitions.
//!
//! `include` is one entry point for every single-message definition form and `include_batch` for
//! the batch ones; which machinery runs is picked by the definition's form token
//! ([`IncludeDef::Form`]), exactly as on a [`BrokerScope`](crate::runtime::BrokerScope). Forms
//! that take an attachment hand back a registration builder; because a router is a consuming
//! builder, the builder commits through an explicit terminal (`.publisher(policy)`, `.mount()`,
//! `.out(marker, policy)` per slot) and returns the grown router.
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
use super::mount::{IncludeDef, MountCodec, RouterMount};

impl<B: Broker + 'static, Routes, RouteCodec, RouteLayers>
    Router<B, Routes, RouteCodec, RouteLayers>
{
    /// Mounts a single-message `#[subscriber]` definition on its own source: a plain handler
    /// grows the router directly, a `publish("dest")` or `Out`-taking one hands back a
    /// registration builder to finish with `.publisher(policy)`, `.mount()`, or
    /// `.out(marker, policy)` per slot.
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
    pub fn include<Def>(
        self,
        def: Def,
    ) -> <Def::Form as RouterMount<B, Routes, RouteCodec, RouteLayers, Def>>::Out
    where
        Def: IncludeDef,
        Def::Form: RouterMount<B, Routes, RouteCodec, RouteLayers, Def>,
    {
        <Def::Form as RouterMount<B, Routes, RouteCodec, RouteLayers, Def>>::begin(def, self)
    }

    /// Mounts a `#[subscriber(batch(..))]` definition on its own source; the `publish("dest")`
    /// and `Out`-taking shapes hand back a registration builder, exactly like
    /// [`include`](Self::include).
    ///
    /// The source's subscriber must implement [`BatchSubscriber`] - natively, or through the
    /// [`Buffered`](crate::Buffered) adapter. Router and app middleware wrap per-message
    /// handlers and do not apply to batch registrations.
    pub fn include_batch<Def>(
        self,
        def: Def,
    ) -> <Def::Form as RouterMount<B, Routes, RouteCodec, RouteLayers, Def>>::Out
    where
        Def: IncludeDef,
        Def::Form: RouterMount<B, Routes, RouteCodec, RouteLayers, Def>,
    {
        <Def::Form as RouterMount<B, Routes, RouteCodec, RouteLayers, Def>>::begin(def, self)
    }

    /// Attaches a slice handler to a batch subscription described by `source`, decoding each
    /// element with the chain's codec (or the [`DefaultCodec`](crate::codec::DefaultCodec)).
    ///
    /// The functional-path counterpart of [`include_batch`](Self::include_batch): `handler` is
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
