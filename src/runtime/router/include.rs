//! `include` on [`Router`]: the one mounting entry point.
//!
//! `include` mounts every definition form, single-message and batch alike - an attribute
//! definition and a value one (`subscriber(..)`, `batch(..)`, ...) the same way; which machinery
//! runs is picked by the definition's form token ([`IncludeDef::Form`]), exactly as on a
//! [`BrokerScope`](crate::runtime::BrokerScope). Forms that take an attachment hand back a
//! registration builder; because a router is a consuming builder, the builder commits through
//! an explicit terminal (`.publisher(policy)`, `.mount()`, `.out(marker, policy)` per slot) and
//! returns the grown router.
//!
//! The subscription source always comes from the definition: `#[subscriber(..)]` takes the
//! broker's own source expression, builder chain included, and a value constructor takes it as
//! its first argument - so there is nothing to override from the mount site.

use crate::Broker;

use super::builder::Router;
use super::mount::RouterMount;
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
}
