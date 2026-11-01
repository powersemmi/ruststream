//! `include` on [`BrokerScope`]: the scope opens the one mount chain, and the `with_broker`
//! closure returns it.
//!
//! Both registration surfaces run the same builder. `b.include(def)` consumes the scope and hands
//! back the chain a [`Router`] would: every step, every typestate slot and every diagnostic comes
//! from [`RouterWith`](crate::RouterWith), and a step is followed by the next `include` directly. The closure returns
//! the chain, and the app mounts it once the closure is done ([`MountChain`], then [`MountRoutes`]).
//!
//! Because `include` consumes the scope, every registration of a broker is one expression: a
//! second `b.include(..)` beside the first is a use of a moved value, not a mount that silently
//! went nowhere.

// Every spelling, mounted on this surface and on the router's. The JSON codec and the document
// schemas it mounts with come with the facade the tests reach the in-memory broker through.
#[cfg(test)]
mod parity_tests;

use crate::SendBroker;
use crate::{BlanketLayer, Broker, Declared, IntoDeclared, PublishIdentity, PublishPipeline};
use crate::{BodyFits, ChainHead, Identity, PoolFits, SplitHead};
use crate::{MountChain, Poisoned, Router, RouterDef, RouterMount};

use super::LifecycleHook;
use super::scope::{BrokerScope, ScopeRegistrations};
use crate::sink::RouterSink;

/// The chain a scope opens: what it registered so far as the head, its codec and publish
/// pipeline, no router-scope layers of its own (the app's stack wraps at the mount, as it does
/// for any router).
pub(crate) type ScopeRouter<B, C, Pipeline, Pre> = Router<B, ChainHead<Pre>, C, Identity, Pipeline>;

/// Mounts the finished routes of a `with_broker` closure into the app: registers the head the
/// scope carries and every route after it into `sink`, and the startup publishes into `hooks`,
/// each handler wrapped with `global`, in the order the chain was written.
///
/// The closure returns a [`MountChain`]; the app finishes it and mounts its
/// [`Routes`](MountChain::Routes) through this trait. Machinery; you never implement it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "these routes cannot be mounted on the tokio runner",
    label = "`{Self}` is not mountable here",
    note = "the chain's broker, layers and publish pipeline must be the ones the app registers; \
            a handler whose bounds fail is reported at the `include` that wrote it"
)]
pub trait MountRoutes<B: Broker, Layers = Identity, State = (), Pipeline = PublishIdentity> {
    /// Registers every mount of the routes into `sink` and its startup publishes into `hooks`.
    fn mount_routes(
        self,
        global: &Layers,
        pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        hooks: &mut Vec<LifecycleHook<State>>,
    );
}

// The scope is the chain of what it registered: `|b| b` mounts nothing.
impl<B, Layers, C, State, Pipeline, Pre> MountChain
    for BrokerScope<B, Layers, C, State, Pipeline, Pre>
where
    B: SendBroker,
{
    type Routes = ScopeRouter<B, C, Pipeline, Pre>;

    fn finish(self) -> Self::Routes {
        Router::for_scope_with(self.pre, self.codec, self.pipeline)
    }
}

// A finished chain is the scope's head under a router: the head registers first, then the routes
// mount the way `include_router` mounts any router.
impl<B, Layers, State, Pipeline, Routes, RC, RL, RP> MountRoutes<B, Layers, State, Pipeline>
    for Router<B, Routes, RC, RL, RP>
where
    B: SendBroker,
    Routes: SplitHead,
    Routes::Pre: ScopeRegistrations<B, Layers, State, Pipeline>,
    Routes::Rest: RouterDef<B, State, RP>,
    RL: BlanketLayer + Clone + Send + Sync + 'static,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn mount_routes(
        self,
        global: &Layers,
        pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        hooks: &mut Vec<LifecycleHook<State>>,
    ) {
        let (pre, routes) = self.split_head();
        pre.register(global, pipeline, sink, hooks);
        // The chain's router carries the scope's own publish path, so the fallback here is never
        // the one a registration takes.
        RouterDef::mount(routes, global, pipeline, &PublishIdentity, sink);
    }
}

impl<B: SendBroker, Layers, C, State, Pipeline, Pre>
    BrokerScope<B, Layers, C, State, Pipeline, Pre>
{
    /// Mounts a definition of any form on this broker and returns the mount chain, which the
    /// `with_broker` closure returns.
    ///
    /// The chain takes the per-handler steps (`.out(marker, policy)`, `.out_reply(policy)`,
    /// `.out_retry(policy)`, `.transform(..)`) and goes on to the next registration with
    /// `.include(def)`, which exists once every position the handler declares is bound.
    ///
    /// Decoding uses the scope codec when one was set
    /// ([`with_broker_codec`](crate::RustStream::with_broker_codec)), else the
    /// [`DefaultCodec`](ruststream_runtime::codec::DefaultCodec).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// struct Receipt {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders", reply("receipts"))]
    /// async fn confirm(order: &Order) -> Receipt {
    ///     Receipt { id: order.id }
    /// }
    ///
    /// #[subscriber("audit")]
    /// async fn audit(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(confirm).out_reply(Publish).include(audit)
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[allow(clippy::type_complexity)] // the mount's own output; an alias would hide it
    pub fn include<D, Mk>(
        self,
        def: D,
    ) -> <<D as BodyFits<Pipeline, Mk>>::Fits as Poisoned<
        <<D::Decl as Declared>::Form as RouterMount<
            ScopeRouter<B, C, Pipeline, Pre>,
            <D::Decl as Declared>::Settings,
        >>::Out,
    >>::Out
    where
        D: IntoDeclared<Mk> + BodyFits<Pipeline, Mk>,
        <D::Decl as Declared>::Settings: PoolFits<Pipeline>,
        <D::Decl as Declared>::Form:
            RouterMount<ScopeRouter<B, C, Pipeline, Pre>, <D::Decl as Declared>::Settings>,
    {
        Router::for_scope_with(self.pre, self.codec, self.pipeline).include(def)
    }
}
