//! `include` on [`BrokerScope`]: the scope's window onto the one mount chain.
//!
//! Both registration surfaces run the same builder. A [`Router`] is a consuming builder, so its
//! chain ends in an explicit `.build()`; a scope is a `&mut` borrow inside the `with_broker`
//! closure, so `include` hands back a [`Mounting`] guard that owns a router chain of its own and
//! drains it into the scope's sink when the statement ends. The guard is an adapter and nothing
//! more: every step, every typestate slot and every diagnostic comes from
//! [`RouterWith`](crate::runtime::RouterWith).
//!
//! Which guard a registration uses follows its form. A plain or batch handler and a
//! reply-publishing one commit when the [`Mounting`] guard drops at the end of the statement, so
//! `b.include(handle);` and `b.include(respond).out_reply(Publish);` are each a whole
//! registration. One carrying [`Out`](crate::runtime::Out) slots gets a [`MountingSlots`] guard
//! and commits with `.build()`: a chain that still has an unbound slot has nothing to commit, so
//! its terminal has to be a call - and the type is `#[must_use]`, so forgetting it is a warning
//! where the mount site is.
//!
//! Because every form hands back a guard, a registration is a statement: the guard borrows the
//! scope, so a `with_broker` closure whose body is the bare call does not compile. Write the body
//! as statements, `|b| { b.include(handle); }`.

mod guard;

use crate::Broker;
use crate::runtime::middleware::Identity;
use crate::runtime::router::{Router, RouterMount, forms};

use super::scope::BrokerScope;
pub use guard::{Mounting, MountingSlots, ScopeCommit};

/// The empty chain a scope drives one registration through: its codec and publish pipeline, no
/// router-scope layers of its own.
pub(crate) type ScopeRouter<B, C, Pipeline> = Router<B, (), C, Identity, Pipeline>;

/// The chain a form produces on a scope, before the guard wraps it.
type ScopeChain<Form, B, C, Pipeline, Def> =
    <Form as RouterMount<ScopeRouter<B, C, Pipeline>, Def>>::Out;

/// Form-token dispatch for [`BrokerScope::include`]: implemented by the tokens in
/// [`forms`](crate::runtime::forms), it picks the chain the form opens and the guard that
/// commits it. Machinery; you never implement or name it.
#[doc(hidden)]
pub trait IncludeMount<'s, B: Broker, Layers, C, State, Pipeline, Def> {
    /// What `include` hands back: a guard over the form's own mount chain.
    type Out;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out;
}

/// Implements [`IncludeMount`] for a form whose registration is complete as it stands: open the
/// router chain over a scope-shaped router, then wrap it in the guard that commits on drop.
macro_rules! scope_mount {
    ($($form:ty),+ $(,)?) => {$(
        impl<'s, B, Layers, C, State, Pipeline, Def>
            IncludeMount<'s, B, Layers, C, State, Pipeline, Def> for $form
        where
            B: Broker + 'static,
            C: Clone + 's,
            Layers: 's,
            State: 's,
            Pipeline: Clone + 's,
            Self: RouterMount<ScopeRouter<B, C, Pipeline>, Def>,
            ScopeChain<Self, B, C, Pipeline, Def>: ScopeCommit<B, Layers, C, State, Pipeline>,
        {
            type Out = Mounting<
                's,
                B,
                Layers,
                C,
                State,
                Pipeline,
                ScopeChain<Self, B, C, Pipeline, Def>,
            >;

            fn begin(
                def: Def,
                scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
            ) -> Self::Out {
                let router = Router::for_scope(scope.codec.clone(), scope.pipeline.clone());
                Mounting::new(<Self as RouterMount<_, Def>>::begin(def, router), scope)
            }
        }
    )+};
}

scope_mount! {
    forms::Publishing,
    forms::RawReply,
    forms::BatchPublishing,
}

/// Implements [`IncludeMount`] for a form carrying [`Out`](crate::runtime::Out) slots: the same
/// chain, in the guard whose terminal is `.build()`. The chain is not committable until every
/// slot is bound, so no commit bound is stated here - `build` is where it is checked.
macro_rules! scope_mount_slots {
    ($($form:ty),+ $(,)?) => {$(
        impl<'s, B, Layers, C, State, Pipeline, Def>
            IncludeMount<'s, B, Layers, C, State, Pipeline, Def> for $form
        where
            B: Broker + 'static,
            C: Clone + 's,
            Layers: 's,
            State: 's,
            Pipeline: Clone + 's,
            Self: RouterMount<ScopeRouter<B, C, Pipeline>, Def>,
        {
            type Out = MountingSlots<
                's,
                B,
                Layers,
                C,
                State,
                Pipeline,
                ScopeChain<Self, B, C, Pipeline, Def>,
            >;

            fn begin(
                def: Def,
                scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
            ) -> Self::Out {
                let router = Router::for_scope(scope.codec.clone(), scope.pipeline.clone());
                MountingSlots::new(<Self as RouterMount<_, Def>>::begin(def, router), scope)
            }
        }
    )+};
}

scope_mount_slots! {
    forms::Out,
    forms::BatchOut,
    forms::PublishingOut,
    forms::RawReplyOut,
    forms::BatchPublishingOut,
}

/// Implements [`IncludeMount`] for a form that attaches no publish of the handler's own: its
/// chain is a finished router, in the guard that drains it when the statement ends. The guard is
/// what carries the one position such a registration can still bind, the deferred retry.
macro_rules! eager_mount {
    ($($form:ty),+ $(,)?) => {$(
        impl<'s, B, Layers, C, State, Pipeline, Def>
            IncludeMount<'s, B, Layers, C, State, Pipeline, Def> for $form
        where
            B: Broker + 'static,
            C: Clone + 's,
            Layers: 's,
            State: 's,
            Pipeline: Clone + 's,
            Self: RouterMount<ScopeRouter<B, C, Pipeline>, Def>,
            ScopeChain<Self, B, C, Pipeline, Def>: ScopeCommit<B, Layers, C, State, Pipeline>,
        {
            type Out = Mounting<
                's,
                B,
                Layers,
                C,
                State,
                Pipeline,
                ScopeChain<Self, B, C, Pipeline, Def>,
            >;

            fn begin(
                def: Def,
                scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
            ) -> Self::Out {
                let router = Router::for_scope(scope.codec.clone(), scope.pipeline.clone());
                Mounting::new(<Self as RouterMount<_, Def>>::begin(def, router), scope)
            }
        }
    )+};
}

eager_mount! {
    forms::Subscribing,
    forms::RawSubscribing,
    forms::Batch,
    forms::RawBatch,
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a definition of any form on this broker.
    ///
    /// A plain or batch handler and a `publish("dest")` one register when the statement ends, so
    /// `b.include(handle);` and `b.include(respond).out_reply(Publish);` are both complete; a
    /// handler carrying [`Out`](crate::runtime::Out) slots binds each with `.out(marker, policy)`
    /// and finishes with `.build()`. Every form takes `.out_retry(policy)`, the deferred
    /// `retry_after` publisher of that one registration.
    ///
    /// Decoding uses the scope codec when one was set
    /// ([`with_broker_codec`](crate::runtime::RustStream::with_broker_codec)), else the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    pub fn include<'s, D>(
        &'s mut self,
        def: D,
    ) -> <D::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, D::Settings>>::Out
    where
        D: crate::runtime::Declared,
        D::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, D::Settings>,
    {
        <D::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, D::Settings>>::begin(
            def.declare(),
            self,
        )
    }
}
