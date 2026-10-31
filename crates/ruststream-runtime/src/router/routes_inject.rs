//! The deferred startup-injection routes: a handler whose `Out` parameters are only live once
//! the subscription is open.
//!
//! Like the publishing routes these store the pieces rather than a built handler: the injections
//! resolve right after the subscription opens and before the first delivery, so every injected
//! handle is live by construction and a "not ready" state is never representable inside the
//! handler.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;
use core::num::NonZeroUsize;

use crate::{
    BatchSubscriber, Broker, BuildBatchContext, BuildContext, Connected, SendSource, Settle,
    SubscriptionSource,
};

use crate::batch_inject::{BatchInjectCall, BatchInjectDef, BatchInjectHandler};
use crate::dispatch::Workers;
use crate::failure::FailurePolicies;
use crate::inject::{FromStartup, InjectCall, InjectDef, InjectHandler};
use crate::input::{DecodeWith, Materialize};
use crate::lifecycle::BoxError;
use crate::metadata::HandlerMetadata;
use crate::middleware::BlanketLayer;
use crate::publish::PublishPipeline;
use crate::redelivery::{
    CopyPathAddress, CopyPathPairing, RetrySetup, RetrySlot, ScopeDelivery, open_subscription,
};
use crate::retry::RetryOpen;

use super::SourceMessage;
use super::mount_sink::MountSink;
use super::routes::{
    MountRoute, RouteMeta, RouteMetadata, RouteSubscription, impl_destination_declared,
};

/// One registration whose handler takes startup injections: an attached publish policy pairing
/// into an [`Out`](crate::Out) parameter. An implementation detail of
/// [`Router`](crate::Router)'s registration list.
///
/// `Extra` is the include-site attachment the injections resolve against, one element per
/// injected parameter.
#[doc(hidden)]
pub struct InjectRoute<Source, Def, DecodeCodec, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) extra: Extra,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// The batch counterpart of [`InjectRoute`]. An implementation detail of
/// [`Router`](crate::Router)'s registration list.
#[doc(hidden)]
pub struct BatchInjectRoute<Source, Def, DecodeCodec, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) extra: Extra,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
    /// The size the subscription opens its batches at, from the registration's `batch(n)`.
    pub(super) batch_size: NonZeroUsize,
}

/// See the publishing routes: a deferred route holds no built handler, so its `Debug` and its
/// metadata collection both go through the registration metadata.
macro_rules! debug_by_metadata {
    ($($route:ident<$($param:ident),+>),+ $(,)?) => {$(
        impl<$($param),+> fmt::Debug for $route<$($param),+> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($route))
                    .field("meta", &self.meta)
                    .finish_non_exhaustive()
            }
        }

        impl<$($param),+> RouteMeta for $route<$($param),+> {
            fn collect(&self, out: &mut Vec<HandlerMetadata>) {
                out.push(self.meta.clone());
            }
        }
    )+};
}

debug_by_metadata!(
    InjectRoute<Source, Def, DecodeCodec, Extra>,
    BatchInjectRoute<Source, Def, DecodeCodec, Extra>,
);

/// Implements the three per-route marker impls the mount chain reads: the retry position is
/// unbound, the metadata is reachable, and the descriptor's copy path is the route's.
macro_rules! impl_route_markers {
    ($($route:ident<$($param:ident),+>),+ $(,)?) => {$(
        impl<$($param),+> RetryOpen for $route<$($param),+> {}

        impl<$($param),+> RouteMetadata for $route<$($param),+> {
            fn metadata_mut(&mut self) -> &mut HandlerMetadata {
                &mut self.meta
            }
        }

    )+};
}

impl_route_markers!(
    InjectRoute<Source, Def, DecodeCodec, Extra>,
    BatchInjectRoute<Source, Def, DecodeCodec, Extra>,
);

impl_destination_declared!(
    [Source, Def, DecodeCodec, Extra] InjectRoute<Source, Def, DecodeCodec, Extra>,
    [Source, Def, DecodeCodec, Extra] BatchInjectRoute<Source, Def, DecodeCodec, Extra>,
);

impl<B, Source, Def, DecodeCodec, Extra> RouteSubscription<B>
    for InjectRoute<Source, Def, DecodeCodec, Extra>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Def: InjectDef,
{
    type Source = Source;
    type Copies = <Source as SubscriptionSource<Connected<B>>>::Copies;
    type Context = Def::Context;
}

impl<B, Source, Def, DecodeCodec, Extra> RouteSubscription<B>
    for BatchInjectRoute<Source, Def, DecodeCodec, Extra>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Def: BatchInjectDef,
{
    type Source = Source;
    type Copies = <Source as SubscriptionSource<Connected<B>>>::Copies;
    type Context = Def::Context;
}

impl<B, Source, Def, DecodeCodec, Extra, State, RetryPipeline> MountRoute<B, State, RetryPipeline>
    for InjectRoute<Source, Def, DecodeCodec, Extra>
where
    B: Broker + 'static,
    Source: SendSource<Connected<B>> + Send + 'static,
    Source::Copies:
        CopyPathPairing<B, Def::Context, RetryPipeline> + CopyPathAddress<Connected<B>, Source>,
    Def::Context: Send + Sync + 'static,
    RetryPipeline: Clone,
    Source::Subscriber: Sync + Send + 'static,
    SourceMessage<B, Source>: Settle + 'static,
    State: Send + Sync + 'static,
    Def: InjectCall<State> + 'static,
    Def::Input: Materialize<DecodeCodec, SourceMessage<B, Source>>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
    Def::Context: BuildContext<SourceMessage<B, Source>> + Send + Sync + 'static,
    // Not `Codec`: `Materialize` already carries what the input asks of it, and a byte input
    // asks for nothing - its route is built with `()` there.
    DecodeCodec: Send + Sync + 'static,
    Extra: Send + Sync + 'static,
{
    type Context = Def::Context;
    type Copies = Source::Copies;

    fn mount_one<G, PP, Sink, Slot>(
        self,
        global: &G,
        _pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut Sink,
        setup: RetrySetup<B, Def::Context, Slot>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
        Sink: MountSink<B, State>,
        Slot: RetrySlot<B, Def::Context, Source::Copies, RetryPipeline>,
    {
        let Self {
            source,
            def,
            codec,
            extra,
            mut meta,
            policies,
            workers,
        } = self;
        // The handler only exists once the injections resolve, and the wrapped handler's type
        // borrows the layer it was applied through, so the runner applies `global` where it
        // spawns the loop.
        let global = global.clone();
        let setup = setup.resolve::<Source::Copies, _>(retry_pipeline, &mut meta);
        sink.push_workers(
            meta,
            policies,
            workers,
            global,
            async move |connected: Arc<Connected<B>>, scope: Arc<ScopeDelivery>, name: Arc<str>| {
                let (subscriber, delivery, retry) = open_subscription::<B, _, Def::Context, _>(
                    source,
                    connected.as_ref(),
                    &scope,
                    &name,
                    setup,
                )
                .await?;
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(BoxError::from)?;
                let handler = InjectHandler {
                    def,
                    codec,
                    injections,
                    decode: policies.decode,
                };
                Ok((subscriber, handler, delivery, retry))
            },
        );
    }
}

impl<B, Source, Def, DecodeCodec, Extra, State, RetryPipeline> MountRoute<B, State, RetryPipeline>
    for BatchInjectRoute<Source, Def, DecodeCodec, Extra>
where
    B: Broker + 'static,
    Source: SendSource<Connected<B>> + Send + 'static,
    Source::Copies:
        CopyPathPairing<B, Def::Context, RetryPipeline> + CopyPathAddress<Connected<B>, Source>,
    Def::Context: Send + Sync + 'static,
    RetryPipeline: Clone,
    Source::Subscriber: BatchSubscriber + Sync + Send + 'static,
    SourceMessage<B, Source>: Settle + 'static,
    State: Send + Sync + 'static,
    Def: BatchInjectCall<State> + 'static,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
    Def::Context: BuildBatchContext<SourceMessage<B, Source>> + Send + Sync + 'static,
    DecodeCodec: Send + Sync + 'static,
    Extra: Send + Sync + 'static,
{
    type Context = Def::Context;
    type Copies = Source::Copies;

    fn mount_one<G, PP, Sink, Slot>(
        self,
        _global: &G,
        _pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut Sink,
        setup: RetrySetup<B, Def::Context, Slot>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
        Sink: MountSink<B, State>,
        Slot: RetrySlot<B, Def::Context, Source::Copies, RetryPipeline>,
    {
        // Per-message layers cannot wrap a whole-batch handler, so no layer applies here.
        let Self {
            source,
            def,
            codec,
            extra,
            mut meta,
            policies,
            workers,
            batch_size,
        } = self;
        let setup = setup.resolve::<Source::Copies, _>(retry_pipeline, &mut meta);
        sink.push_batch::<_, _, _, _, Def::Context, _>(
            meta,
            policies,
            workers,
            batch_size,
            async move |connected: Arc<Connected<B>>, scope: Arc<ScopeDelivery>, name: Arc<str>| {
                let (subscriber, delivery, retry) = open_subscription::<B, _, Def::Context, _>(
                    source,
                    connected.as_ref(),
                    &scope,
                    &name,
                    setup,
                )
                .await?;
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(BoxError::from)?;
                let handler = BatchInjectHandler {
                    def,
                    codec,
                    injections,
                    decode: policies.decode,
                };
                Ok((subscriber, handler, delivery, retry))
            },
        );
    }
}

#[cfg(all(test, feature = "json"))]
mod tests {
    use super::*;
    use crate::Name;
    use crate::codec::JsonCodec;
    use alloc::format;

    /// The injection routes hold no built handler to print, so their `Debug` has to identify the
    /// registration by its metadata; without that a router's registration list is anonymous.
    #[test]
    fn an_injection_route_debug_names_the_registration() {
        let single = InjectRoute {
            source: Name::new("jobs"),
            def: (),
            codec: JsonCodec,
            extra: ((),),
            meta: HandlerMetadata::raw("jobs"),
            policies: FailurePolicies::default(),
            workers: Workers::sequential(),
        };
        let rendered = format!("{single:?}");
        assert!(rendered.contains("InjectRoute"), "{rendered}");
        assert!(rendered.contains("jobs"), "{rendered}");

        let batched = BatchInjectRoute {
            source: Name::new("bulk-jobs"),
            def: (),
            codec: JsonCodec,
            extra: ((),),
            meta: HandlerMetadata::raw("bulk-jobs"),
            policies: FailurePolicies::default(),
            workers: Workers::sequential(),
            batch_size: crate::nonzero!(8),
        };
        let rendered = format!("{batched:?}");
        assert!(rendered.contains("BatchInjectRoute"), "{rendered}");
        assert!(rendered.contains("bulk-jobs"), "{rendered}");
    }
}
