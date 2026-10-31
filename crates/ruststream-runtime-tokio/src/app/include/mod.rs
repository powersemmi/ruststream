//! `include` on [`BrokerScope`]: the scope opens the one mount chain, and the `with_broker`
//! closure returns it.
//!
//! Both registration surfaces run the same builder. `b.include(def)` consumes the scope and hands
//! back the chain a [`Router`] would: every step, every typestate slot and every diagnostic comes
//! from [`RouterWith`], and a step is followed by the next `include` directly. The closure returns
//! the chain, and the app mounts it once the closure is done ([`MountChain`]).
//!
//! Because `include` consumes the scope, every registration of a broker is one expression: a
//! second `b.include(..)` beside the first is a use of a moved value, not a mount that silently
//! went nowhere.

// Every spelling, mounted on this surface and on the router's. The JSON codec and the document
// schemas it mounts with come with the facade the tests reach the in-memory broker through.
#[cfg(test)]
mod parity_tests;

use crate::SendBroker;
use crate::{BlanketLayer, Broker, Declared, PublishIdentity, PublishPipeline};
use crate::{ChainHead, Identity, PoolFits, RetryDestinationsDeclared, SplitHead};
use crate::{Router, RouterCommit, RouterDef, RouterMount, RouterWith};

use super::LifecycleHook;
use super::scope::{BrokerScope, ScopeRegistrations};
use crate::sink::RouterSink;

/// The chain a scope opens: what it registered so far as the head, its codec and publish
/// pipeline, no router-scope layers of its own (the app's stack wraps at the mount, as it does
/// for any router).
pub(crate) type ScopeRouter<B, C, Pipeline, Pre> = Router<B, ChainHead<Pre>, C, Identity, Pipeline>;

/// What a `with_broker` closure returns: the mount chain the app registers once the closure is
/// done.
///
/// Implemented by the scope itself (the chain of what it registered, empty for `|b| b`) and by the
/// chain its `include` opens (a [`Router`] over the scope's head, or a [`RouterWith`] waiting for
/// its next step). `()` is not a chain, so a closure that dropped what `include` returned does not
/// compile. Machinery; you never implement it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "the `with_broker` closure must return the mount chain",
    label = "`{Self}` is not a mount chain",
    note = "return the scope or the chain its `include` opens: `|b| b.include(a).include(b)`, \
            every `Out` slot bound with `.out(marker, policy)`; a broker that mounts nothing is \
            `|b| b`"
)]
pub trait MountChain<B: Broker, Layers = Identity, State = (), Pipeline = PublishIdentity> {
    /// Registers every mount of the chain into `sink` and its startup publishes into `hooks`,
    /// each handler wrapped with `global`, in the order the chain was written.
    fn mount_chain(
        self,
        global: &Layers,
        pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        hooks: &mut Vec<LifecycleHook<State>>,
    );
}

// The scope is the chain of what it registered: `|b| b` mounts nothing.
impl<B, Layers, C, State, Pipeline, Pre> MountChain<B, Layers, State, Pipeline>
    for BrokerScope<B, Layers, C, State, Pipeline, Pre>
where
    B: SendBroker,
    Pre: ScopeRegistrations<B, Layers, State, Pipeline>,
{
    fn mount_chain(
        self,
        global: &Layers,
        pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        hooks: &mut Vec<LifecycleHook<State>>,
    ) {
        self.pre.register(global, pipeline, sink, hooks);
    }
}

// A finished chain is the scope's head under a router: the head registers first, then the routes
// mount the way `include_router` mounts any router.
impl<B, Layers, State, Pipeline, Routes, RC, RL, RP> MountChain<B, Layers, State, Pipeline>
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
    fn mount_chain(
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

// A chain whose last registration still takes steps commits through the router's own terminal,
// bounded exactly as `build` and the next `include` are.
impl<B, Layers, State, Pipeline, Mount, R, Def, Attach, Last> MountChain<B, Layers, State, Pipeline>
    for RouterWith<Mount, R, Def, Attach, Last>
where
    B: SendBroker,
    Attach: RouterCommit<Mount, R, Def, Out: RetryDestinationsDeclared>,
    Attach::Out: MountChain<B, Layers, State, Pipeline>,
{
    fn mount_chain(
        self,
        global: &Layers,
        pipeline: &Pipeline,
        sink: &mut RouterSink<B, State>,
        hooks: &mut Vec<LifecycleHook<State>>,
    ) {
        self.build().mount_chain(global, pipeline, sink, hooks);
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
    pub fn include<D>(
        self,
        def: D,
    ) -> <D::Form as RouterMount<ScopeRouter<B, C, Pipeline, Pre>, D::Settings>>::Out
    where
        D: Declared,
        D::Settings: PoolFits<Pipeline>,
        D::Form: RouterMount<ScopeRouter<B, C, Pipeline, Pre>, D::Settings>,
    {
        Router::for_scope_with(self.pre, self.codec, self.pipeline).include(def)
    }
}
