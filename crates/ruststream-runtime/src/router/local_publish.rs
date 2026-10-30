//! A local router's mounts for the reply-publishing forms, and the commits their chains resolve
//! through: the [`form_publish`](super::form_publish) twins for a chain started with
//! [`Router::local`].
//!
//! The chain is the same `RouterWith` the multi-thread runner's router hands back, so
//! `.out_reply(policy)`, `.codec(..)` and `.transform(..)` read as they do there; the commit
//! builds the same route, which the fold opens against a broker of the local family.

use crate::input::Materialize;
use crate::publish::RawReplyWiring;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::publish::ReplyWiring;
use crate::publishing::PublishingDef;
use crate::settings::{DefMountCodec, MountsWith};
use crate::slot::{IntoSlotSource, WithSource};
use crate::{
    LocalBroker, LocalConnected, LocalDefaultPublish, LocalSourceMessage, LocalSubscriptionSource,
};

use super::builder::Router;
use super::builders::{RouterCommit, RouterPublishing, RouterWith};
use super::local::LocalPipeline;
use super::mount::{DefaultReply, PublishMount, RawReplyMount, RouterMount};
use super::{PublishingRouter, RawReplyRouter, forms};

/// What a reply definition's single-delivery input decodes with when it mounts on a local router
/// of broker `B` with the surface codec `RouteCodec`.
type ReplyDecoder<Def, B, RouteCodec> =
    DefMountCodec<Def, <Def as PublishingDef>::Input, RouteCodec, LocalConnected<B>>;

/// The connected form's default publish policy: what a reply with no `.out_reply(..)` pairs.
type DefaultPolicy<B> = <LocalConnected<B> as LocalDefaultPublish>::Policy;

/// Implements [`RouterMount`] for a reply-only form on a local router: the chain starts with the
/// reply position on the broker's default and no slots beside it.
macro_rules! local_reply_form {
    ($($form:ty => $mount:ty),+ $(,)?) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def>
            RouterMount<Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def>
            for $form
        where
            B: LocalBroker + 'static,
        {
            type Out = RouterPublishing<
                $mount,
                Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
                Def,
            >;

            fn begin(
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
            ) -> Self::Out {
                RouterWith::new(def, (DefaultReply, ()), router)
            }
        }
    )+};
}

local_reply_form! {
    forms::Publishing => PublishMount,
    forms::RawReply => RawReplyMount,
}

/// Implements the named-policy commit of a reply form on a local router: the route the
/// multi-thread runner's commit builds, over the local twins of the source and the message.
macro_rules! local_reply_commit {
    ($($mount:ty => $router:ident, $mount_fn:ident;)+) => {$(
        impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def, Policy>
            RouterCommit<$mount, Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def>
            for (WithSource<Policy>, ())
        where
            B: LocalBroker + 'static,
            Def: PublishingDef
                + MountsWith<<Def as PublishingDef>::Input, RouteCodec, LocalConnected<B>>
                + 'static,
            Def::Source: LocalSubscriptionSource<LocalConnected<B>> + 'static,
            Def::Input:
                Materialize<ReplyDecoder<Def, B, RouteCodec>, LocalSourceMessage<B, Def::Source>>,
            Policy: 'static,
        {
            type Out = $router<
                B,
                Def::Source,
                Def,
                ReplyDecoder<Def, B, RouteCodec>,
                Policy,
                ((),),
                RouteCodec,
                RouteLayers,
                LocalPipeline<Pipe>,
                Routes,
            >;

            fn commit(
                self,
                def: Def,
                router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
            ) -> Self::Out {
                let codec = def.mounted_codec(&router.codec);
                let source = def.source();
                // No slot attachment on this form: the injections resolve against the padding.
                router.$mount_fn(source, def, codec, self.0.into_source(), ((),))
            }
        }
    )+};
}

local_reply_commit! {
    PublishMount => PublishingRouter, mount_publishing_source;
    RawReplyMount => RawReplyRouter, mount_raw_reply_source;
}

// The typed default reply: the broker's plain policy under the default codec, committed as if
// `.out_reply(<policy>)` had been chained.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def>
    RouterCommit<PublishMount, Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def>
    for (DefaultReply, ())
where
    B: LocalBroker + 'static,
    LocalConnected<B>: LocalDefaultPublish,
    (WithSource<ReplyWiring<DefaultPolicy<B>>>, ()): RouterCommit<
            PublishMount,
            Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
            Def,
        >,
{
    type Out = <(WithSource<ReplyWiring<DefaultPolicy<B>>>, ()) as RouterCommit<
        PublishMount,
        Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
        Def,
    >>::Out;

    fn commit(
        self,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
    ) -> Self::Out {
        (
            WithSource::new(ReplyWiring::new(DefaultPolicy::<B>::default())),
            (),
        )
            .commit(def, router)
    }
}

// The serialized wire's default: the broker's plain policy taken bare, so no codec is demanded.
impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def>
    RouterCommit<
        RawReplyMount,
        Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
        Def,
    > for (DefaultReply, ())
where
    B: LocalBroker + 'static,
    LocalConnected<B>: LocalDefaultPublish,
    (WithSource<RawReplyWiring<DefaultPolicy<B>>>, ()): RouterCommit<
            RawReplyMount,
            Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
            Def,
        >,
{
    type Out = <(WithSource<RawReplyWiring<DefaultPolicy<B>>>, ()) as RouterCommit<
        RawReplyMount,
        Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
        Def,
    >>::Out;

    fn commit(
        self,
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
    ) -> Self::Out {
        (
            WithSource::new(RawReplyWiring::new(DefaultPolicy::<B>::default())),
            (),
        )
            .commit(def, router)
    }
}
