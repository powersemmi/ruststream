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

use crate::{BlanketLayer, Broker, Declared, PublishIdentity, PublishPipeline};
use crate::{Identity, RetryDestinationsDeclared};
use crate::{Router, RouterCommit, RouterDef, RouterMount, RouterWith};

use super::scope::BrokerScope;
use crate::sink::RouterSink;

/// The empty chain a scope opens: its codec and publish pipeline, no router-scope layers of its
/// own (the app's stack wraps at the mount, as it does for any router).
pub(crate) type ScopeRouter<B, C, Pipeline> = Router<B, (), C, Identity, Pipeline>;

/// What a `with_broker` closure returns: the mount chain the app registers once the closure is
/// done.
///
/// Implemented by the chain `b.include(..)` opens (a [`Router`] or a [`RouterWith`] waiting for
/// its next step); [`BrokerScope::end`] and every registration the scope makes at once return the
/// empty one. `()` is not a chain, so a closure that dropped what `include` returned does not
/// compile. Machinery; you never implement it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "the `with_broker` closure must return the mount chain",
    label = "`{Self}` is not a mount chain",
    note = "return the chain `include` opens: `|b| b.include(a).include(b)`, every `Out` slot \
            bound with `.out(marker, policy)`; a scope that mounts nothing returns `b.end()`"
)]
pub trait MountChain<B: Broker, Layers = Identity, State = (), Pipeline = PublishIdentity> {
    /// Registers every mount of the chain into `sink`, each handler wrapped with `global`.
    fn mount_chain(self, global: &Layers, pipeline: &Pipeline, sink: &mut RouterSink<B, State>);
}

// A finished chain is a router: mounting it is the operation `include_router` performs, so a
// scope's chain and a router reach the sink through one path.
impl<B, Layers, State, Pipeline, Routes, RC, RL, RP> MountChain<B, Layers, State, Pipeline>
    for Router<B, Routes, RC, RL, RP>
where
    B: Broker + 'static,
    Routes: RouterDef<B, State, RP>,
    RL: BlanketLayer + Clone + Send + Sync + 'static,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn mount_chain(self, global: &Layers, pipeline: &Pipeline, sink: &mut RouterSink<B, State>) {
        // The chain's router carries the scope's own publish path, so the fallback here is never
        // the one a registration takes.
        RouterDef::mount(self, global, pipeline, &PublishIdentity, sink);
    }
}

// A chain whose last registration still takes steps commits through the router's own terminal,
// bounded exactly as `build` and the next `include` are.
impl<B, Layers, State, Pipeline, Mount, R, Def, Attach, Last> MountChain<B, Layers, State, Pipeline>
    for RouterWith<Mount, R, Def, Attach, Last>
where
    B: Broker + 'static,
    Attach: RouterCommit<Mount, R, Def, Out: RetryDestinationsDeclared>,
    Attach::Out: MountChain<B, Layers, State, Pipeline>,
{
    fn mount_chain(self, global: &Layers, pipeline: &Pipeline, sink: &mut RouterSink<B, State>) {
        self.build().mount_chain(global, pipeline, sink);
    }
}

impl<B: Broker + 'static, Layers, C, State, Pipeline>
    BrokerScope<'_, B, Layers, C, State, Pipeline>
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
    ) -> <D::Form as RouterMount<ScopeRouter<B, C, Pipeline>, D::Settings>>::Out
    where
        D: Declared,
        D::Form: RouterMount<ScopeRouter<B, C, Pipeline>, D::Settings>,
    {
        Router::for_scope(self.codec, self.pipeline).include(def)
    }

    /// Ends a scope that mounts no handler of its own, returning the empty mount chain.
    ///
    /// For a broker the service only publishes to.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn handle(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .with_broker_labeled("ingress", MemoryBroker::new(), |b| b.include(handle))
    ///         // The service publishes to the second broker and consumes nothing from it.
    ///         .with_broker_labeled("audit", MemoryBroker::new(), |b| b.end())
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn end(&self) -> ScopeRouter<B, C, Pipeline>
    where
        C: Clone,
        Pipeline: Clone,
    {
        self.chain()
    }
}
