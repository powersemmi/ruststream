//! The fold's opener for the injection routes: a definition with `Out` slots mounted on a local
//! router, its slots paired against the connected broker of the local family when the
//! subscription opens and before its first delivery.

// Written over the local twins of the contract for a runner that polls on one thread: the
// futures are `Send` exactly where the types they run are, and nothing here spawns them.
#![allow(clippy::future_not_send)]

use alloc::boxed::Box;
use alloc::sync::Arc;

use crate::inject::{InjectHandler, LocalFromStartup, LocalInject, LocalInjectCall};
use crate::input::Materialize;
use crate::lifecycle::BoxError;
use crate::redelivery::{ScopeDelivery, open_local_route};
use crate::{
    BuildContext, LocalBroker, LocalConnected, LocalIncomingMessage, LocalSubscriber,
    LocalSubscriptionSource,
};

use super::fold_plan::{LocalRoute, OpenLocalRoute};
use super::routes_inject::InjectRoute;

impl<B, State, Source, M, Def, DecodeCodec, Extra> OpenLocalRoute<B, State>
    for InjectRoute<Source, Def, DecodeCodec, Extra>
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Subscriber: LocalSubscriber<Message = M>,
    M: LocalIncomingMessage,
    Def: LocalInjectCall<State>,
    Def::Input: Materialize<DecodeCodec, M>,
    Def::Injections: LocalFromStartup<B, Source::Subscriber, Extra>,
    Def::Context: BuildContext<M> + Send + Sync + 'static,
{
    type Source = Source;
    type Sub = Source::Subscriber;
    type Body = LocalInject<InjectHandler<Def, DecodeCodec>>;
    type Cx = Def::Context;

    fn source(&self) -> &Source {
        &self.source
    }

    async fn open_route(
        self,
        connected: &LocalConnected<B>,
        scope: &ScopeDelivery,
    ) -> Result<LocalRoute<Self::Sub, Self::Body, Self::Cx>, BoxError> {
        let Self {
            source,
            def,
            codec,
            extra,
            policies,
            ..
        } = self;
        let name: Arc<str> = Arc::from(source.name());
        let delivery = open_local_route(scope, &name);
        let subscriber = source
            .subscribe(connected)
            .await
            .map_err(|err| -> BoxError { Box::new(err) })?;
        // Resolved before the loop starts, so every slot the body reaches is live.
        let injections = Def::Injections::resolve_local(extra, connected, &subscriber)
            .await
            .map_err(BoxError::from)?;
        let handler = LocalInject(InjectHandler {
            def,
            codec,
            injections,
            decode: policies.decode,
        });
        Ok(LocalRoute {
            subscriber,
            handler,
            name,
            delivery,
            policies,
        })
    }
}
