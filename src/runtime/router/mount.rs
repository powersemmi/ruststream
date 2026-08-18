//! Form-token dispatch: the vocabulary both registration surfaces mount through.
//!
//! A definition ties itself to a form token ([`IncludeDef::Form`]), and one entry point per
//! shape - [`Router::include`](super::Router::include) /
//! [`include_batch`](super::Router::include_batch) and their `_on` source-override variants,
//! [`BrokerScope::include`](crate::runtime::BrokerScope::include) /
//! [`include_batch`](crate::runtime::BrokerScope::include_batch) - resolves the token to the
//! machinery that form needs, at compile time.

use crate::Broker;
use crate::codec::Codec;
// The default-codec resolution exists only when a codec feature supplies a default.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;

use super::builder::Router;
use super::forms;

/// Ties a definition type to its form token.
///
/// One `include` entry point then dispatches to the right mounting machinery at compile time.
/// Implemented by the `#[subscriber]` macro; a hand-written definition adds it next to its def
/// trait impl.
pub trait IncludeDef {
    /// The form token: one of the markers in [`forms`].
    type Form;
}

/// The codec a registration surface decodes with: its own codec when one was named, else the
/// default. Machinery behind `include`; the `()` impl is the "nothing named" case, which every
/// surface starts in.
#[doc(hidden)]
pub trait MountCodec {
    /// The resolved codec.
    type Codec: Codec + Clone + Send + Sync + 'static;
    /// Produces it, fresh per registration.
    fn mount_codec(&self) -> Self::Codec;
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl MountCodec for () {
    type Codec = DefaultCodec;
    fn mount_codec(&self) -> Self::Codec {
        DefaultCodec::default()
    }
}

impl<C: Codec + Clone + Send + Sync + 'static> MountCodec for C {
    type Codec = C;
    fn mount_codec(&self) -> Self::Codec {
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

/// The mount tokens keying the commit traits: which mount a committed attachment drives.
///
/// Strategies of different form families are impls on the same attachment types with different
/// concrete tokens, so they never overlap without negative reasoning.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishMount;
/// See [`PublishMount`]. The byte-reply form keeps its own token because its reply travels a
/// bare publisher rather than the encoded stack, which is a different route on a router.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct RawReplyMount;
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
/// See [`RawReplyMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct RawReplyInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishInjectMount;

/// Form-token dispatch for [`Router::include`](super::Router::include) and
/// [`include_batch`](super::Router::include_batch): implemented by the tokens in [`forms`],
/// generic over the definition and the router chain. Machinery; you never implement or name it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this definition's form cannot be mounted on a router",
    label = "the form token `{Self}` has no router mount",
    note = "check that the definition's shape matches the entry point: batch definitions mount \
            with .include_batch(..), single-message ones with .include(..)"
)]
pub trait RouterMount<B: Broker, Routes, RouteCodec, RouteLayers, Def> {
    /// What `include` hands back: the grown router for eager forms, a registration builder for
    /// the ones that take an attachment.
    type Out;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out;
}

/// The source-override counterpart of [`RouterMount`], behind
/// [`Router::include_on`](super::Router::include_on) and
/// [`include_batch_on`](super::Router::include_batch_on).
///
/// Implemented for the forms whose subscription source can be replaced from the outside. A
/// handler with [`Out`](crate::runtime::Out) slots has none: its definition is only
/// instantiated once the slots are bound, so its source is not known at the call.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this definition's form cannot be mounted on an explicit source",
    label = "the form token `{Self}` has no source-override router mount",
    note = "a handler with Out slots takes its source from the definition the bound slots \
            instantiate; mount it with .include(..) and bind the slots there"
)]
pub trait RouterMountOn<B: Broker, Routes, RouteCodec, RouteLayers, Source, Def> {
    /// See [`RouterMount::Out`].
    type Out;

    fn begin_on(
        source: Source,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self::Out;
}

/// Implements [`RouterMount`] for a form whose source comes from the definition: it resolves the
/// source and hands over to the source-override mount, so the two entry points share one body.
macro_rules! mount_via_own_source {
    ($($form:ty => $def_trait:path),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Def>
            RouterMount<B, Routes, RouteCodec, RouteLayers, Def> for $form
        where
            B: Broker + 'static,
            Def: $def_trait,
            Self: RouterMountOn<B, Routes, RouteCodec, RouteLayers, Def::Source, Def>,
        {
            type Out =
                <Self as RouterMountOn<B, Routes, RouteCodec, RouteLayers, Def::Source, Def>>::Out;

            fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
                let source = def.source();
                Self::begin_on(source, def, router)
            }
        }
    )+};
}

mount_via_own_source! {
    forms::Subscribing => crate::runtime::subscriber_def::SubscriberDef,
    forms::RawSubscribing => crate::runtime::subscriber_def::SubscriberDef,
    forms::Seek => crate::runtime::inject::InjectDef,
    forms::Publishing => crate::runtime::publishing::PublishingDef,
    forms::RawReply => crate::runtime::publishing::PublishingDef,
    forms::Batch => crate::runtime::batch::BatchDef,
    forms::BatchWithHeaders => crate::runtime::batch::BatchWithHeadersDef,
    forms::BatchSeek => crate::runtime::batch_inject::BatchInjectDef,
    forms::BatchPublishing => crate::runtime::batch_publishing::BatchPublishingDef,
}
