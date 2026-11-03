//! The fold's opener for the injection routes: a definition with `Out` slots mounted on a local
//! router, its slots paired against the connected broker of the local family when the
//! subscription opens and before its first delivery.

// Written over the contract traits for a runner that polls on one thread: the
// futures are `Send` exactly where the types they run are, and nothing here spawns them.
#![allow(clippy::future_not_send)]

use crate::batch_inject::{BatchInjectCall, BatchInjectHandler};
use crate::inject::{FromStartup, InjectCall, InjectHandler};
use crate::input::{DecodeWith, InputKind, Materialize};
use crate::lifecycle::BoxError;
use crate::redelivery::{ScopeDelivery, Undeclared, open_local_subscription, startup_failure};
use crate::{
    BatchSubscriber, Broker, BuildBatchContext, BuildContext, Connected, RetryDeclaration, Settle,
    Subscriber, SubscriptionSource,
};
use ruststream_core::MaybeSendSync;

use super::fold_batch::{OneTaskBatchRoute, OpenBatchRoute};
use super::fold_plan::{OneTaskRoute, OpenOneTaskRoute};
use super::routes_inject::{BatchInjectRoute, InjectRoute};

impl<B, State, Source, M, Def, DecodeCodec, Extra> OpenOneTaskRoute<B, State>
    for InjectRoute<Source, Def, DecodeCodec, Extra>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Subscriber: Subscriber<Message = M>,
    M: Settle,
    Def: InjectCall<State>,
    Def::Input: Materialize<DecodeCodec, M>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + MaybeSendSync,
    DecodeCodec: MaybeSendSync,
    State: MaybeSendSync,
    Def::Context: BuildContext<M> + MaybeSendSync + 'static,
{
    type Source = Source;
    type Declaration = Undeclared;
    type Sub = Source::Subscriber;
    type Body = InjectHandler<Def, DecodeCodec>;
    type Cx = Def::Context;

    fn source(&self) -> &Source {
        &self.source
    }

    async fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        declaration: RetryDeclaration,
    ) -> Result<OneTaskRoute<Self::Sub, Self::Body, Self::Cx>, BoxError> {
        let Self {
            source,
            def,
            codec,
            extra,
            policies,
            ..
        } = self;
        let (name, subscriber, delivery) =
            open_local_subscription::<B, _, _>(source, connected, scope, declaration).await?;
        // Resolved before the loop starts, so every slot the body reaches is live.
        let injections = Def::Injections::resolve(extra, connected, &subscriber)
            .await
            .map_err(|err| startup_failure("injections", &name, err))?;
        let handler = InjectHandler {
            def,
            codec,
            injections,
            decode: policies.decode,
        };
        Ok(OneTaskRoute {
            subscriber,
            handler,
            name,
            delivery,
            policies,
            input: <Def::Input as InputKind>::input_label(),
        })
    }
}

impl<B, State, Source, M, Def, DecodeCodec, Extra, Size> OpenBatchRoute<B, State>
    for BatchInjectRoute<Source, Def, DecodeCodec, Extra, Size>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Subscriber: BatchSubscriber<Message = M>,
    M: Settle,
    Def: BatchInjectCall<State>,
    Def::Input: DecodeWith<DecodeCodec>,
    Def::Injections: FromStartup<B, Source::Subscriber, Extra> + MaybeSendSync,
    DecodeCodec: MaybeSendSync,
    State: MaybeSendSync,
    Def::Context: BuildBatchContext<M> + MaybeSendSync + 'static,
{
    type Source = Source;
    type Declaration = Undeclared;
    type Sub = Source::Subscriber;
    type Body = BatchInjectHandler<Def, DecodeCodec>;
    type Cx = Def::Context;

    fn source(&self) -> &Source {
        &self.source
    }

    async fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        declaration: RetryDeclaration,
    ) -> Result<OneTaskBatchRoute<Self::Sub, Self::Body, Self::Cx>, BoxError> {
        let Self {
            source,
            def,
            codec,
            extra,
            policies,
            batch_size,
            ..
        } = self;
        let (name, subscriber, delivery) =
            open_local_subscription::<B, _, _>(source, connected, scope, declaration).await?;
        // Resolved before the loop starts, so every slot the body reaches is live.
        let injections = Def::Injections::resolve(extra, connected, &subscriber)
            .await
            .map_err(|err| startup_failure("injections", &name, err))?;
        let handler = BatchInjectHandler {
            def,
            codec,
            injections,
            decode: policies.decode,
        };
        Ok(OneTaskBatchRoute {
            subscriber,
            handler,
            name,
            batch_size,
            delivery,
            policies,
            input: <Def::Input as InputKind>::input_label(),
        })
    }
}
