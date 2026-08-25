//! The deferred startup-injection routes: a handler whose `Out` or `Seek` parameters are only
//! live once the subscription is open.
//!
//! Like the publishing routes these store the pieces rather than a built handler: the injections
//! resolve right after the subscription opens and before the first delivery, so every injected
//! handle is live by construction and a "not ready" state is never representable inside the
//! handler.

use std::fmt;
use std::sync::Arc;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, BuildContext, Connected, SubscriptionSource};

use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectHandler};
use crate::runtime::dispatch::{Workers, spawn_dispatch_workers};
use crate::runtime::failure::{DispatchFailure, FailurePolicies};
use crate::runtime::inject::{FromStartup, InjectCall, InjectHandler};
use crate::runtime::input::DecodeWith;
use crate::runtime::lifecycle::BoxError;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::BlanketLayer;
use crate::runtime::publish::PublishPipeline;

use super::SourceMessage;
use super::routes::{MountRoute, RouteMeta};
use super::sink::RouterSink;

/// One registration whose handler takes startup injections: an attached publish policy pairing
/// into an [`Out`](crate::runtime::Out) parameter, the subscription's own seeker for a
/// [`Seek`](crate::runtime::Seek) one. An implementation detail of
/// [`Router`](crate::runtime::Router)'s registration list.
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
/// [`Router`](crate::runtime::Router)'s registration list.
#[doc(hidden)]
pub struct BatchInjectRoute<Source, Def, DecodeCodec, Extra> {
    pub(super) source: Source,
    pub(super) def: Def,
    pub(super) codec: DecodeCodec,
    pub(super) extra: Extra,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// See the publishing routes: a deferred route holds no built handler, so its `Debug` and its
/// metadata collection both go through the registration metadata.
macro_rules! debug_by_metadata {
    ($($route:ident),+ $(,)?) => {$(
        impl<Source, Def, DecodeCodec, Extra> fmt::Debug for $route<Source, Def, DecodeCodec, Extra> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($route))
                    .field("meta", &self.meta)
                    .finish_non_exhaustive()
            }
        }

        impl<Source, Def, DecodeCodec, Extra> RouteMeta for $route<Source, Def, DecodeCodec, Extra> {
            fn collect(&self, out: &mut Vec<HandlerMetadata>) {
                out.push(self.meta.clone());
            }
        }
    )+};
}

debug_by_metadata!(InjectRoute, BatchInjectRoute);

impl<B, Source, Def, DecodeCodec, Extra, State> MountRoute<B, State>
    for InjectRoute<Source, Def, DecodeCodec, Extra>
where
    B: Broker + 'static,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Sync + Send + 'static,
    SourceMessage<B, Source>: Send + Sync + 'static,
    State: Send + Sync + 'static,
    Def: InjectCall<State> + 'static,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
    Def::Context: BuildContext<SourceMessage<B, Source>> + Send + Sync + 'static,
    DecodeCodec: Codec + Send + 'static,
    Extra: Send + Sync + 'static,
{
    fn mount_one<G, PP>(self, global: &G, _pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        let Self {
            source,
            def,
            codec,
            extra,
            meta,
            policies,
            workers,
        } = self;
        // The apply-and-push tail: the handler only exists once the injections resolve, and
        // `BlanketLayer::apply` returns an unnameable `impl Handler<..>`, so the wrapped handler
        // cannot be returned out of a factory closure; apply and spawn stay in one block.
        let global = global.clone();
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        sink.push_raw(
            Box::new(move |connected, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let injections =
                        Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                            .await
                            .map_err(|e| Box::new(e) as BoxError)?;
                    let handler = global.apply::<SourceMessage<B, Source>, Def::Context, State, _>(
                        InjectHandler {
                            def,
                            codec,
                            injections,
                            decode: policies.decode,
                        },
                    );
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_dispatch_workers(
                        subscriber,
                        Arc::new(handler),
                        token,
                        name,
                        state,
                        delivery,
                        failure,
                        workers,
                    ))
                })
            }),
            meta,
        );
    }
}

impl<B, Source, Def, DecodeCodec, Extra, State> MountRoute<B, State>
    for BatchInjectRoute<Source, Def, DecodeCodec, Extra>
where
    B: Broker + 'static,
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: BatchSubscriber + Sync + Send + 'static,
    SourceMessage<B, Source>: Send + 'static,
    State: Send + Sync + 'static,
    Def: BatchInjectCall<State> + 'static,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + Send + Sync + 'static,
    DecodeCodec: Send + Sync + 'static,
    Extra: Send + Sync + 'static,
{
    fn mount_one<G, PP>(self, _global: &G, _pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // Per-message layers cannot wrap a whole-batch handler, so no layer applies here.
        let Self {
            source,
            def,
            codec,
            extra,
            meta,
            policies,
            workers,
        } = self;
        sink.push_injected_batch(
            source,
            async move |connected: Arc<Connected<B>>, subscriber| {
                let injections = Def::Injections::resolve(extra, connected.as_ref(), &subscriber)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                let handler = BatchInjectHandler {
                    def,
                    codec,
                    injections,
                    decode: policies.decode,
                };
                Ok((subscriber, handler))
            },
            meta,
            policies,
            workers,
        );
    }
}

#[cfg(all(test, feature = "memory", feature = "json"))]
mod tests {
    use super::*;
    use crate::Name;
    use crate::codec::JsonCodec;

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
        };
        let rendered = format!("{batched:?}");
        assert!(rendered.contains("BatchInjectRoute"), "{rendered}");
        assert!(rendered.contains("bulk-jobs"), "{rendered}");
    }
}
