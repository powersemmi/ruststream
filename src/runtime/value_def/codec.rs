//! The per-definition codec override: the top rung of the codec ladder, on the value path.
//!
//! `subscriber(..).codec(JsonCodec)` wraps the definition in [`CodecValue`], whose form token
//! ([`forms::WithCodec`]) routes the mount through the wrapped codec instead of the surface's.
//! The other rungs are untouched: without the call, the surface's
//! [`with_codec`](crate::runtime::Router::with_codec) codec (or the
//! [`DefaultCodec`](crate::codec::DefaultCodec)) applies.

use std::fmt;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, BuildContext, Connected, SubscriptionSource};

use crate::runtime::app::{BrokerScope, IncludeMount};
use crate::runtime::batch::{BatchDef, SliceHandler};
use crate::runtime::handler::Handler;
use crate::runtime::input::{DecodeWith, InputKind};
use crate::runtime::middleware::Layer;
use crate::runtime::router::{
    IncludeDef, IncludedBatchRouter, IncludedRouter, Router, RouterMount, forms,
};
use crate::runtime::settings::SubscriberBuilder;
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;
use crate::runtime::{SourceMessage, SourceSubscriber};

use super::subscribing::{BatchValue, SubscriberValue};

/// A value definition with its own decode codec: what
/// [`codec`](SubscriberBuilder::codec) wraps the definition in. You never name this type.
pub struct CodecValue<D, C> {
    pub(crate) inner: D,
    pub(crate) codec: C,
}

impl<D, C> fmt::Debug for CodecValue<D, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodecValue").finish_non_exhaustive()
    }
}

impl<D: IncludeDef, C> IncludeDef for CodecValue<D, C> {
    type Form = forms::WithCodec<D::Form>;
}

/// Splits a codec-carrying definition into the wrapped definition and its codec. Machinery
/// behind the [`forms::WithCodec`] mounts; never named directly.
#[doc(hidden)]
pub trait SplitCodec: Sized {
    /// The wrapped definition.
    type Inner;
    /// The codec the chain named.
    type Codec;

    fn split_codec(self) -> (Self::Inner, Self::Codec);
}

impl<D, C> SplitCodec for CodecValue<D, C> {
    type Inner = D;
    type Codec = C;

    fn split_codec(self) -> (D, C) {
        (self.inner, self.codec)
    }
}

// The settings builder splits through to the wrapped definition, keeping the source and the
// collected settings on the outside - so the mount reads them off the builder as usual.
impl<D: SplitCodec, Src, State> SplitCodec for SubscriberBuilder<D, Src, State> {
    type Inner = SubscriberBuilder<D::Inner, Src, State>;
    type Codec = D::Codec;

    fn split_codec(self) -> (Self::Inner, Self::Codec) {
        self.split_def(SplitCodec::split_codec)
    }
}

impl<T, H, Src, State> SubscriberBuilder<SubscriberValue<T, H>, Src, State> {
    /// Decodes this subscriber with `codec`, overriding the surface's codec for this
    /// registration only.
    ///
    /// Chain it after the documentation opt-ins (`describe`, `documented`): the override wraps
    /// the definition, and those methods bind to the unwrapped form.
    #[must_use]
    pub fn codec<C: Codec>(
        self,
        codec: C,
    ) -> SubscriberBuilder<CodecValue<SubscriberValue<T, H>, C>, Src, State> {
        self.map_def(|inner| CodecValue { inner, codec })
    }
}

impl<T, H, Src, State> SubscriberBuilder<BatchValue<T, H>, Src, State> {
    /// Decodes this batch subscriber's elements with `codec`, overriding the surface's codec
    /// for this registration only. See [`codec`](SubscriberBuilder::codec) on the plain form.
    #[must_use]
    pub fn codec<C: Codec>(
        self,
        codec: C,
    ) -> SubscriberBuilder<CodecValue<BatchValue<T, H>, C>, Src, State> {
        self.map_def(|inner| CodecValue { inner, codec })
    }
}

// ---------------------------------------------------------------------------------------------
// Router mounts: the two decoded eager forms, with the codec taken from the definition.

impl<B, Routes, RouteCodec, RouteLayers, Def, Inner>
    RouterMount<B, Routes, RouteCodec, RouteLayers, Def> for forms::WithCodec<forms::Subscribing>
where
    B: Broker + 'static,
    Def: SplitCodec<Inner = Inner>,
    Def::Codec: Codec + Send + Sync + 'static,
    Inner: SubscriberDef,
    Inner::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Inner::Source>: Send + 'static,
    Inner::Input: DecodeWith<Def::Codec>,
    Inner::Handler: 'static,
{
    type Out = IncludedRouter<B, Inner::Source, Inner, Def::Codec, RouteCodec, RouteLayers, Routes>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let (inner, codec) = def.split_codec();
        let source = inner.source();
        router.mount_subscriber(source, inner, codec)
    }
}

impl<B, Routes, RouteCodec, RouteLayers, Def, Inner>
    RouterMount<B, Routes, RouteCodec, RouteLayers, Def> for forms::WithCodec<forms::Batch>
where
    B: Broker + 'static,
    Def: SplitCodec<Inner = Inner>,
    Def::Codec: Codec + Send + Sync + 'static,
    Inner: BatchDef,
    Inner::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Inner::Source>: BatchSubscriber + Send + 'static,
    Inner::Input: DecodeWith<Def::Codec>,
    Inner::Handler: 'static,
{
    type Out =
        IncludedBatchRouter<B, Inner::Source, Inner, Def::Codec, RouteCodec, RouteLayers, Routes>;

    fn begin(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out {
        let (inner, codec) = def.split_codec();
        let source = inner.source();
        router.mount_batch(source, inner, codec)
    }
}

// ---------------------------------------------------------------------------------------------
// Scope mounts: the same two forms on a `BrokerScope`.

impl<'s, B, Layers, C, State, Pipeline, Def, Inner>
    IncludeMount<'s, B, Layers, C, State, Pipeline, Def> for forms::WithCodec<forms::Subscribing>
where
    B: Broker + 'static,
    Def: SplitCodec<Inner = Inner>,
    Def::Codec: Codec + Send + Sync + 'static,
    Inner: SubscriberDef,
    Inner::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    SourceSubscriber<B, Inner::Source>: Send + 'static,
    SourceMessage<B, Inner::Source>: 'static,
    Inner::Input: DecodeWith<Def::Codec>,
    Inner::Handler: 'static,
    Inner::Context: BuildContext<SourceMessage<B, Inner::Source>> + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<Typed<SourceMessage<B, Inner::Source>, Inner::Input, Def::Codec, Inner::Handler>>,
    Layers::Handler: Handler<SourceMessage<B, Inner::Source>, Inner::Context, State> + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let (inner, codec) = def.split_codec();
        let source = inner.source();
        scope.mount_subscriber(source, inner, codec);
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def, Inner>
    IncludeMount<'s, B, Layers, C, State, Pipeline, Def> for forms::WithCodec<forms::Batch>
where
    B: Broker + 'static,
    Def: SplitCodec<Inner = Inner>,
    Def::Codec: Codec + Send + Sync + 'static,
    Inner: BatchDef,
    Inner::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Inner::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Send + 'static,
    Inner::Input: DecodeWith<Def::Codec>,
    Inner::Handler: SliceHandler<<Inner::Input as InputKind>::Owned, State> + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let (inner, codec) = def.split_codec();
        let source = inner.source();
        scope.mount_batch(source, inner, codec);
    }
}
