//! The `include` family on [`BrokerScope`]: mounting macro-generated definitions.
//!
//! `include` is the one entry point for every definition form, single-message and batch alike;
//! which machinery runs is picked by the definition's form token ([`IncludeDef::Form`]), so
//! `b.include(handle)`, `b.include(bulk)`, `b.include(respond).publisher(..)` and
//! `b.include(forward).publisher(..)` all read the same. Publisher-producing forms return a
//! registration builder that commits when the statement ends; `.publisher(..)` attaches the
//! publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker target).
//!
//! The vocabulary itself (the form tokens, the mount tokens, the codec resolution) belongs to
//! the router and is imported from there, so both surfaces dispatch on one set of tokens.

use crate::Broker;

use super::scope::BrokerScope;

// The form vocabulary lives in the router: routing is its responsibility, and the scope mounts
// through the same tokens.
pub(crate) use crate::runtime::router::{
    BatchInjectMount, BatchPublishInjectMount, BatchPublishMount, DefaultBareReply, DefaultReply,
    InjectMount, MountCodec, PublishInjectMount, PublishMount, forms,
};

/// Form-token dispatch for [`BrokerScope::include`]: implemented by the tokens in
/// [`forms`](crate::runtime::forms), generic over the definition and the scope. Machinery; you
/// never implement or name it.
#[doc(hidden)]
pub trait IncludeMount<'s, B: Broker, Layers, C, State, Pipeline, Def> {
    /// What `include` hands back: `()` for eager forms, a registration builder for the
    /// publisher-producing ones.
    type Out;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out;
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a `#[subscriber]` definition of any form: a plain or batch handler mounts eagerly,
    /// a `publish("dest")` or `Out`-taking one returns a registration builder that commits at
    /// the end of the statement; chain [`publisher`](IncludePublishing::publisher) on it to
    /// attach the publish policy.
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

mod builder;
mod commit;
mod forms_batch;
mod forms_eager;
mod forms_out;
mod forms_handle;
mod forms_publish;
mod slot_builder;
mod slot_reply_builder;

pub use builder::{
    IncludeBatchOut, IncludeBatchPublishing, IncludeOut, IncludePublishing, IncludeWith,
};
// The mount tokens and the commit trait are machinery: reachable across the include
// modules, never re-exported from the crate root.
pub(crate) use commit::CommitVia;
pub use slot_builder::{IncludeSlots, SlotCommit};
pub use slot_reply_builder::{
    IncludeBatchPublishingOut, IncludePublishingOut, IncludeSlotsWithReply,
};
