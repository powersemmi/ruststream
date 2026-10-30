//! The router of a runner whose deliveries stay on the thread that polls them.
//!
//! [`Router::local`] starts a chain whose publish pipeline is a [`LocalPipeline`]. The marker is
//! what selects the local family's mounts: a definition included there mounts on a
//! [`LocalBroker`], whose subscriber, messages and handler futures need not be `Send`. The chain
//! [`Router::new`] starts keeps the mounts of the multi-thread runner, which ask for `Send` and
//! name a source of the wrong broker in their errors.

use core::marker::PhantomData;

use crate::batch::BatchDef;
use crate::input::{Materialize, Provided};
use crate::lifecycle::BoxError;
use crate::middleware::Identity;
use crate::publish::{Outgoing, PublishIdentity, SendThrough};
use crate::settings::{BatchSized, DefMountCodec, MountsWith};
use crate::subscriber_def::SubscriberDef;
use crate::{
    LocalBatchSubscriber, LocalBroker, LocalConnected, LocalPublisher, LocalSourceMessage,
    LocalSourceSubscriber, LocalSubscriptionSource, PayloadForm,
};

use super::builder::Router;
use super::mount::RouterMount;
use super::{IncludedRawBatchRouter, IncludedRouter, forms};

/// The publish pipeline of a [`Router::local`] chain: the `Pipe` its `Out` slots send through,
/// marked as the pipeline of a runner that polls every delivery on one thread.
///
/// The router reads the marker to pick the mounts of the local family, so a chain carries it
/// from its first step. A runner that hosts the whole app in one task (embassy's executor)
/// builds its scope's chain over it.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::Router;
/// use ruststream_memory::local::LocalMemoryBroker;
/// use ruststream_runtime::LocalPipeline;
///
/// // The chain a device app's routes are written on.
/// let routes: Router<LocalMemoryBroker<4>, (), (), _, LocalPipeline> = Router::local();
/// # let _ = routes;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalPipeline<Pipe = PublishIdentity>(pub(crate) Pipe);

impl<B: LocalBroker + 'static> Router<B, (), (), Identity, LocalPipeline> {
    /// Creates an empty router for a broker of the local family, decoding with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Its `include` mounts a definition on a broker whose subscriber, messages and handler
    /// futures need not be `Send`; a runner that hosts the app on one thread opens the finished
    /// chain against the connected broker.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::convert::Infallible;
    ///
    /// use ruststream::runtime::{Context, Handle, HandlerOutcome, Router, subscriber};
    /// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
    /// use ruststream_runtime::{Deserialized, Input, SoloDeserialized};
    ///
    /// /// A reading as the device's sensor bus frames it, viewed in place.
    /// struct Reading<'a>(&'a [u8]);
    ///
    /// impl Deserialized for Reading<'_> {
    ///     type Output<'a> = Reading<'a>;
    ///     type Error = Infallible;
    ///
    ///     fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Infallible> {
    ///         Ok(Reading(payload))
    ///     }
    /// }
    ///
    /// impl Input for Reading<'_> {
    ///     type Axis = SoloDeserialized<Reading<'static>>;
    /// }
    ///
    /// struct Log;
    ///
    /// impl Handle<Reading<'_>> for Log {
    ///     async fn handle(
    ///         &self,
    ///         reading: &Reading<'_>,
    ///         _outs: &(),
    ///         _ctx: &mut Context<'_>,
    ///     ) -> Result<(), HandlerOutcome> {
    ///         let _ = reading.0.len();
    ///         Ok(())
    ///     }
    /// }
    ///
    /// let routes = Router::<LocalMemoryBroker<4>, _, _, _, _>::local()
    ///     .include(subscriber(LocalMemorySource::new("readings"), Log).build());
    /// # let _ = routes;
    /// ```
    #[must_use]
    pub const fn local() -> Self {
        Self {
            routes: (),
            codec: (),
            layers: Identity,
            pipeline: LocalPipeline(PublishIdentity),
            _broker: PhantomData,
        }
    }
}

// A local chain's replies and slot publishes go straight to the broker: the publish layers of
// the multi-thread runner's app do not reach it.
impl<P: LocalPublisher> SendThrough<P> for LocalPipeline<PublishIdentity> {
    // The publisher's own future, which is `Send` exactly where the publisher's is.
    #[allow(clippy::future_not_send)]
    async fn send_through<'a>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        send: &'a P,
        options: Option<&'a P::Options>,
    ) -> Result<(), BoxError> {
        // The broker is the last reader of this message, as in `PublishIdentity`.
        let msg = <P::Payload as PayloadForm>::leaving(out);
        send.publish(msg, options).await.map_err(BoxError::from)
    }
}

/// What a definition's single-delivery input decodes with when it mounts on a local router of
/// broker `B` with the surface codec `RouteCodec`.
type SoloDecoder<Def, B, RouteCodec> =
    DefMountCodec<Def, <Def as SubscriberDef>::Input, RouteCodec, LocalConnected<B>>;

impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def>
    for forms::Subscribing
where
    B: LocalBroker + 'static,
    Def: SubscriberDef + MountsWith<<Def as SubscriberDef>::Input, RouteCodec, LocalConnected<B>>,
    Def::Source: LocalSubscriptionSource<LocalConnected<B>> + 'static,
    Def::Input: Materialize<SoloDecoder<Def, B, RouteCodec>, LocalSourceMessage<B, Def::Source>>,
    Def::Handler: 'static,
{
    type Out = IncludedRouter<
        B,
        Def::Source,
        Def,
        SoloDecoder<Def, B, RouteCodec>,
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
        Routes,
    >;

    fn begin(
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
    ) -> Self::Out {
        let codec = def.mounted_codec(&router.codec);
        let source = def.source();
        router.mount_subscriber(source, def, codec)
    }
}

// The self-deserializing input kind decodes with `()`, so the chain's codec parameter is left
// unconstrained and the mount works without any codec feature enabled.
impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def, F>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def>
    for forms::RawSubscribing
where
    B: LocalBroker + 'static,
    Def: SubscriberDef<Input = Provided<F>>,
    Def::Source: LocalSubscriptionSource<LocalConnected<B>> + 'static,
    Def::Handler: 'static,
    F: Send + Sync + 'static,
{
    type Out = IncludedRouter<
        B,
        Def::Source,
        Def,
        (),
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
        Routes,
    >;

    fn begin(
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
    ) -> Self::Out {
        let source = def.source();
        router.mount_subscriber(source, def, ())
    }
}

// A self-deserializing batch decodes nothing, so the chain's codec parameter stays
// unconstrained here too.
impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def, F>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def>
    for forms::RawBatch
where
    B: LocalBroker + 'static,
    Def: BatchDef<Input = Provided<F>> + BatchSized,
    Def::Source: LocalSubscriptionSource<LocalConnected<B>> + 'static,
    LocalSourceSubscriber<B, Def::Source>: LocalBatchSubscriber,
    Def::Handler: 'static,
    F: Send + Sync + 'static,
{
    type Out = IncludedRawBatchRouter<
        B,
        Def::Source,
        Def,
        F,
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
        Routes,
    >;

    fn begin(
        def: Def,
        router: Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>,
    ) -> Self::Out {
        let source = def.source();
        router.mount_raw_batch(source, def)
    }
}
