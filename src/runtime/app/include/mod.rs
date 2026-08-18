//! The `include` family on [`BrokerScope`]: mounting macro-generated definitions.
//!
//! `include` is one entry point for every single-message definition form and `include_batch` for
//! both batch forms; which machinery runs is picked by the definition's form token
//! ([`IncludeDef::Form`]), so `b.include(handle)`, `b.include(respond).publisher(..)` and
//! `b.include(forward).publisher(..)` all read the same. Publisher-producing forms return a
//! registration builder that commits when the statement ends; `.publisher(..)` attaches the
//! publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker target).

use crate::codec::Codec;
// The typed default-reply commits need a default codec, so that import is gated the same way;
// the raw default-reply commit publishes bare bytes and needs only `DefaultPublish`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
// The default-reply commits build a `TypedPublisher` over the broker's default policy, so those
// imports are gated with the default codec they require.
use crate::Broker;

use super::scope::BrokerScope;

/// Ties a definition type to its form token.
///
/// One `include` entry point then dispatches to the right mounting machinery at compile time.
/// Implemented by the `#[subscriber]` macro; a hand-written definition adds it next to its def
/// trait impl.
pub trait IncludeDef {
    /// The form token: one of the markers in [`forms`].
    type Form;
}

/// Form tokens for [`IncludeDef`]: which mounting machinery a definition uses.
pub mod forms {
    /// A plain subscriber (`#[subscriber("in")]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Subscribing;
    /// A raw-bytes subscriber (`#[subscriber("in", raw)]`): no decode, no codec.
    #[derive(Debug, Clone, Copy)]
    pub struct RawSubscribing;
    /// A byte-reply subscriber (`#[subscriber("in", publish_raw("out"))]`, with or without
    /// `raw` on the input side): the reply bytes go out as-is through a bare publisher.
    #[derive(Debug, Clone, Copy)]
    pub struct RawReply;
    /// A reply-publishing subscriber (`#[subscriber("in", publish("out"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Publishing;
    /// A subscriber whose startup injections need publisher attachments.
    ///
    /// The signature carries `Out(out): Out<impl Publisher[, Marker]>` parameters (optionally
    /// next to a `Seek` one), so the include site chains `.publisher(..)` (single slot) or
    /// `.out(marker, ..)` per named slot.
    #[derive(Debug, Clone, Copy)]
    pub struct Out;
    /// A subscriber whose startup injections need nothing from the include site.
    ///
    /// The signature carries a `Seek(seeker): Seek<K>` parameter (and no `Out`).
    #[derive(Debug, Clone, Copy)]
    pub struct Seek;
    /// A reply-publishing subscriber whose handler also takes `Out` parameters, so the
    /// include site chains `.out(marker, ..)` per slot next to the (optional)
    /// `.publisher(..)`.
    #[derive(Debug, Clone, Copy)]
    pub struct PublishingOut;
    /// A byte-reply subscriber whose handler also takes an `Out` parameter.
    #[derive(Debug, Clone, Copy)]
    pub struct RawReplyOut;
    /// A batch subscriber (`#[subscriber(batch("in"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Batch;
    /// A batch reply-publishing subscriber (`#[subscriber(batch("in"), publish("out"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchPublishing;
    /// A batch subscriber whose startup injections need a publisher attachment (an `Out`
    /// parameter, optionally next to a `Seek` one).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchOut;
    /// A batch subscriber whose startup injections need nothing from the include site (a
    /// `Seek` parameter and no `Out`).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchSeek;
    /// A batch reply-publishing subscriber whose handler also takes `Out` parameters, so the
    /// include site chains `.out(marker, ..)` per slot next to the (optional)
    /// `.publisher(..)`.
    #[derive(Debug, Clone, Copy)]
    pub struct BatchPublishingOut;
}

/// Form-token dispatch for [`BrokerScope::include`]: implemented by the tokens in [`forms`],
/// generic over the definition and the scope. Machinery; you never implement or name it.
#[doc(hidden)]
pub trait IncludeMount<'s, B: Broker, Layers, C, State, Pipeline, Def> {
    /// What `include` hands back: `()` for eager forms, a registration builder for the
    /// publisher-producing ones.
    type Out;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out;
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a single-message `#[subscriber]` definition: a plain handler mounts eagerly, a
    /// `publish("dest")` or `Out`-taking handler returns a registration builder that commits
    /// at the end of the statement; chain [`publisher`](IncludePublishing::publisher) on it to
    /// attach the publish policy.
    ///
    /// Decoding uses the scope codec when one was set
    /// ([`with_broker_codec`](crate::runtime::RustStream::with_broker_codec)), else the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    pub fn include<'s, Def>(
        &'s mut self,
        def: Def,
    ) -> <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::Out
    where
        Def: IncludeDef,
        Def::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, Def>,
    {
        <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::begin(def, self)
    }

    /// Mounts a batch `#[subscriber(batch(..))]` definition; the `publish("dest")` form returns
    /// a registration builder, exactly like [`include`](Self::include).
    pub fn include_batch<'s, Def>(
        &'s mut self,
        def: Def,
    ) -> <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::Out
    where
        Def: IncludeDef,
        Def::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, Def>,
    {
        <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::begin(def, self)
    }
}

/// The codec a scope decodes with: the scope's own codec when one was set, else the default.
/// Machinery behind `include`; the two impls mirror the two `with_broker` forms.
#[doc(hidden)]
pub trait ScopeCodec {
    type Codec: Codec + Clone + Send + Sync + 'static;
    fn scope_codec(&self) -> Self::Codec;
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl ScopeCodec for () {
    type Codec = DefaultCodec;
    fn scope_codec(&self) -> Self::Codec {
        DefaultCodec::default()
    }
}

impl<C: Codec + Clone + Send + Sync + 'static> ScopeCodec for C {
    type Codec = C;
    fn scope_codec(&self) -> Self::Codec {
        self.clone()
    }
}

/// The default reply commit: the broker's default publish policy under the default codec.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultReply;

/// The default commit of the byte-reply form: the broker's plain publish policy taken bare.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultBareReply;

/// The mount tokens keying [`CommitVia`]: which mount a committed attachment drives.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct InjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishInjectMount;

mod builder;
mod commit;
mod forms_batch;
mod forms_eager;
mod forms_out;
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
