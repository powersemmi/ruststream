//! The router of a runner whose deliveries stay on the thread that polls them.
//!
//! [`Router::local`] starts a chain whose publish pipeline is a [`LocalPipeline`]. The marker is
//! what selects the local family's mounts: a definition included there mounts on a
//! [`Broker`], whose subscriber, messages and handler futures need not be `Send`. The chain
//! [`Router::new`] starts keeps the mounts of the multi-thread runner, which ask for `Send` and
//! name a source of the wrong broker in their errors.

use core::marker::PhantomData;

use crate::batch::BatchDef;
use crate::handle::{AsBodyFn, AsDeclared, BodyTag};
use crate::input::{Materialize, Provided};
use crate::lifecycle::BoxError;
use crate::middleware::Identity;
use crate::middleware::LocalBlanketLayer;
use crate::publish::{LocalOutPipeline, Outgoing, PublishIdentity, PublishPipeline, SendThrough};
use crate::settings::{BatchSized, Declared, DefMountCodec, MountsWith, Sequential};
use crate::subscriber_def::SubscriberDef;
use crate::{
    Broker, Connected, LocalBatchSubscriber, LocalPublisher, LocalSourceMessage, OutgoingFor,
    PayloadForm, SourceSubscriber, SubscriptionSource,
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

impl<B: Broker + 'static> Router<B, (), (), Identity, LocalPipeline> {
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
    /// use ruststream::runtime::{Context, HandlerOutcome, LocalHandle, Router, local_subscriber};
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
    /// impl LocalHandle<Reading<'_>> for Log {
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
    ///     .include(local_subscriber(LocalMemorySource::new("readings"), Log).build());
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

/// A definition's settings that fit the runner the chain's pipeline `Pipe` belongs to: every
/// setting on the multi-thread runner, and settings that name no worker count of their own on a
/// local chain, whose runner fixes its pools at its own mount site. Machinery; never named
/// directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` sets a worker count, which the embedded runner fixes at the mount site",
    note = "on EmbeddedApp write `.include(def).pool::<N>()` or `.keyed::<N>()`; `threads(n)` runs \
            only on the tokio runtime"
)]
pub trait PoolFits<Pipe> {}

impl<Settings, Pipe: PublishPipeline> PoolFits<Pipe> for Settings {}

impl<Settings: Sequential, Pipe> PoolFits<LocalPipeline<Pipe>> for Settings {}

/// Whether a definition's body runs where the chain mounts it: a body function on a multi-thread
/// chain returns a `Send` future; a local chain asks nothing of it; a definition that is its own
/// body (`subscriber(..)`, a `Handle` impl) carries its own bound. Machinery; never named
/// directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this handler cannot run on the multi-thread runner: its future is not `Send`",
    note = "the multi-thread runner moves a handler between threads at each `.await`; keep only \
            `Send` values across an `.await` in the handler (an `Arc` where it holds an `Rc`), \
            or mount it on an `EmbeddedApp`, which polls it on one thread"
)]
pub trait BodyFits<Pipe, Mk> {
    /// What a scope's chain reads the mount's output through: when the body does not fit, the
    /// output has no type and the compiler reports the failure once, at the `include`.
    type Fits;
}

// Bounded by `Declared` so a body function matches the other impl alone, and the marker is inferred
// from the body even where the chain reads its output through `Fits`.
impl<D: Declared, Pipe> BodyFits<Pipe, AsDeclared> for D {
    type Fits = ();
}

impl<F, M, I, O, X, Fut, Pipe> BodyFits<Pipe, AsBodyFn<M, I, O, X>> for F
where
    Pipe: PublishPipeline,
    F: Fn(BodyTag<M>, I, O, X) -> Fut,
    Fut: Future + Send,
{
    type Fits = ();
}

impl<F, M, I, O, X, Pipe> BodyFits<LocalPipeline<Pipe>, AsBodyFn<M, I, O, X>> for F {
    type Fits = ();
}

/// The publish pipeline a router's chain carries, read off the router type. Machinery; never
/// named directly.
#[doc(hidden)]
pub trait RouterPipe {
    /// The chain's pipeline: a [`LocalPipeline`] on a local chain.
    type Pipe;
}

impl<B, Routes, RouteCodec, RouteLayers, Pipe> RouterPipe
    for Router<B, Routes, RouteCodec, RouteLayers, Pipe>
{
    type Pipe = Pipe;
}

/// A chain whose registrations take a worker count at run time: the multi-thread runner's.
/// Machinery; never named directly.
///
/// # Examples
///
/// [`Router::workers`] does not exist on a local chain:
///
/// ```compile_fail
/// # use std::convert::Infallible;
/// # use std::future::ready;
/// use ruststream::nonzero;
/// use ruststream::runtime::{Context, HandlerOutcome, LocalHandle, Router, Workers, local_subscriber};
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream_runtime::{Deserialized, Input, SoloDeserialized};
/// # struct Reading<'a>(&'a [u8]);
/// # impl Deserialized for Reading<'_> {
/// #     type Output<'a> = Reading<'a>;
/// #     type Error = Infallible;
/// #     fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Infallible> { Ok(Reading(payload)) }
/// # }
/// # impl Input for Reading<'_> { type Axis = SoloDeserialized<Reading<'static>>; }
/// # struct Log;
/// # impl LocalHandle<Reading<'_>> for Log {
/// #     fn handle(&self, r: &Reading<'_>, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { let _ = r.0; ready(Ok(())) }
/// # }
/// let routes = Router::<LocalMemoryBroker<4>, _, _, _, _>::local()
///     .include(local_subscriber(LocalMemorySource::new("readings"), Log).build())
///     .workers(Workers::pool(nonzero!(4)));
/// ```
///
/// Neither does the attribute's `workers(..)` clause:
///
/// ```compile_fail
/// use ruststream::runtime::{HandlerOutcome, Router, SubscriberSettings};
/// use ruststream::{Deserialized, subscriber};
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
///
/// #[derive(Deserialized)]
/// struct Reading<'a>(&'a [u8]);
///
/// #[subscriber(LocalMemorySource, workers(4))]
/// async fn log(reading: &Reading<'_>) -> HandlerOutcome {
///     let _ = reading.0.len();
///     HandlerOutcome::ack()
/// }
///
/// let routes = Router::<LocalMemoryBroker<4>, _, _, _, _>::local().include(log.name("readings"));
/// ```
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "a registration on a local router takes no worker count at run time",
    note = "the embedded runner fixes the count at its mount site: on EmbeddedApp write \
            `.include(def).pool::<N>()` or `.keyed::<N>()`"
)]
pub trait TakesWorkers {}

// The help that would list the pipelines this holds for names the multi-thread runner's machinery,
// which a local chain's author has no use for.
#[diagnostic::do_not_recommend]
impl<Pipe: PublishPipeline> TakesWorkers for Pipe {}

/// A router-scope layer that fits the chain's runner: any layer on the multi-thread runner's
/// chain, and a [`LocalBlanketLayer`] on a local chain. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` runs on the tokio runtime",
    label = "this layer does not wrap a local router's handlers",
    note = "a layer on a router started with `Router::local()` implements `LocalBlanketLayer`"
)]
pub trait LayerFits<Pipe> {}

#[diagnostic::do_not_recommend]
impl<Layer, Pipe: PublishPipeline> LayerFits<Pipe> for Layer {}

#[diagnostic::do_not_recommend]
impl<Layer: LocalBlanketLayer, Pipe> LayerFits<LocalPipeline<Pipe>> for Layer {}

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

// The bottom of a local slot's publish path: the leaf call, lent the message as it arrives, with
// nothing rebuilt or copied and the leaf's own error.
impl<W> LocalOutPipeline<W> for LocalPipeline<PublishIdentity> {
    // The leaf's own future, which is `Send` exactly where the leaf's is.
    #[allow(clippy::future_not_send)]
    fn send<P>(
        &self,
        leaf: &P,
        msg: OutgoingFor<'_, W::Payload>,
        options: Option<&P::Options>,
    ) -> impl Future<Output = Result<(), P::Error>>
    where
        W: LocalPublisher,
        P: LocalPublisher<Options = W::Options, Payload = W::Payload>,
    {
        leaf.publish(msg, options)
    }
}

/// What a definition's single-delivery input decodes with when it mounts on a local router of
/// broker `B` with the surface codec `RouteCodec`.
type SoloDecoder<Def, B, RouteCodec> =
    DefMountCodec<Def, <Def as SubscriberDef>::Input, RouteCodec, Connected<B>>;

impl<B, Routes, RouteCodec, RouteLayers, Pipe, Def>
    RouterMount<Router<B, Routes, RouteCodec, RouteLayers, LocalPipeline<Pipe>>, Def>
    for forms::Subscribing
where
    B: Broker + 'static,
    Def: SubscriberDef + MountsWith<<Def as SubscriberDef>::Input, RouteCodec, Connected<B>>,
    Def::Source: SubscriptionSource<Connected<B>> + 'static,
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
    B: Broker + 'static,
    Def: SubscriberDef<Input = Provided<F>>,
    Def::Source: SubscriptionSource<Connected<B>> + 'static,
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
    B: Broker + 'static,
    Def: BatchDef<Input = Provided<F>> + BatchSized,
    Def::Source: SubscriptionSource<Connected<B>> + 'static,
    SourceSubscriber<B, Def::Source>: LocalBatchSubscriber,
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
