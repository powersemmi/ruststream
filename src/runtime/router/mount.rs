//! Form-token dispatch: the vocabulary both registration surfaces mount through.
//!
//! A definition ties itself to a form token ([`IncludeDef::Form`]), and one entry point per
//! surface - [`Router::include`](super::Router::include) and
//! [`BrokerScope::include`](crate::runtime::BrokerScope::include) - resolves the token to the
//! machinery that form needs, at compile time. The subscription source is never named at a
//! mount site: it belongs to the definition, which takes the broker's own source expression.

use crate::Broker;
use crate::codec::Codec;
use crate::runtime::input::{Decoded, DecodedPair, Provided};
// The default-codec resolution exists only when a codec feature supplies a default.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;

use super::builder::Router;

/// Ties a definition type to its form token.
///
/// One `include` entry point then dispatches to the right mounting machinery at compile time.
/// Implemented by the `#[subscriber]` macro; a hand-written definition adds it next to its def
/// trait impl.
pub trait IncludeDef {
    /// The form token: one of the markers in [`forms`](super::forms).
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

/// The codec a mount resolves for one input kind: the surface's own for a decoded input, none
/// at all for a byte one.
///
/// [`MountCodec`] answers "which codec did this surface name", which every decoding mount
/// needs. A byte input decodes with `()` and asks nothing of the codec, so a mount that
/// resolved [`MountCodec`] regardless would demand a default codec the build may not have -
/// shutting the byte path out of a service compiled with no codec feature at all. Resolving
/// against the input kind keeps that demand where it is real.
///
/// Machinery behind `include`; you never implement or name it.
#[doc(hidden)]
pub trait InputCodec<Input> {
    /// The resolved codec: the surface's own, or `()` where the input needs none.
    type Codec: Clone + Send + Sync + 'static;
    /// Produces it, fresh per registration.
    fn input_codec(&self) -> Self::Codec;
}

impl<C: MountCodec, T> InputCodec<Decoded<T>> for C {
    type Codec = <C as MountCodec>::Codec;

    fn input_codec(&self) -> Self::Codec {
        self.mount_codec()
    }
}

// A pair input decodes its payload with the surface codec, exactly like a plain decoded one
// (the headers side needs no codec at all).
impl<C: MountCodec, H, P> InputCodec<DecodedPair<H, P>> for C {
    type Codec = <C as MountCodec>::Codec;

    fn input_codec(&self) -> Self::Codec {
        self.mount_codec()
    }
}

impl<C, F> InputCodec<Provided<F>> for C {
    type Codec = ();

    fn input_codec(&self) {}
}

/// The default reply commit: the broker's default publish policy, wrapped per the reply's wire
/// (under the default codec for an encoded reply, bare for a `Serialized` one - the mount token
/// carries the wire, so one marker serves both).
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultReply;

/// The mount tokens keying the commit traits: which mount a committed attachment drives.
///
/// Form families are impls on the same attachment types with different tokens, so they never
/// overlap.
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

/// Form-token dispatch for [`Router::include`](super::Router::include): implemented by the
/// tokens in [`forms`](super::forms), generic over the definition and the router chain.
/// Machinery; you never implement or name it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this definition's form cannot be mounted on a router",
    label = "the form token `{Self}` has no router mount",
    note = "every `#[subscriber]` form mounts with .include(..); check that the definition is a \
            generated one and that its broker matches the router's"
)]
pub trait RouterMount<B: Broker, Routes, RouteCodec, RouteLayers, Def> {
    /// What `include` hands back: the grown router for eager forms, a registration builder for
    /// the ones that take an attachment.
    type Out;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out;
}
