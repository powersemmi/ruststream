//! The reply-and-slots cells of the matrix: a body that answers and fans out through the arena
//! in one signature. The chain's reply attach seeds the include-site binder, which still takes
//! one `.out(marker, policy)` per slot.

use std::any::type_name;

use crate::runtime::batch::BatchResult;
use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingDef};
use crate::runtime::context::Context;
use crate::runtime::handler::HandlerOutcome;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publishing::{PublishingCall, PublishingDef};
use crate::runtime::router::IncludeDef;
use crate::runtime::slot::{BindSlots, HasSlots, OutSlot};
use crate::{ConnectedBroker, Name, PublishPolicy, Unnamed};

use super::Handle;
use super::axis::{
    Axis, AxisDocs, Batch, BatchPair, BatchedAxis, Deserialized, Input, Message, Solo, SoloAxis,
    SoloDeserialized, SoloPair,
};
use super::eager::construct;
use super::outs::{EntryMarkers, Outs, Slot};
use super::reply::{ReplyDest, ReplyRoute, ReplyShape, WireDocs, batch_reply_verdict};
use super::value::{HandleValue, ReplyValue, Sealed};

impl<A, R, E, C, H, Doc, Dest> IncludeDef
    for Sealed<ReplyValue<HandleValue<A, R, Outs<E>, C, H, Doc>, Dest>>
where
    A: Axis,
    R: ReplyRoute<A::Family>,
{
    type Form = R::SlotForm;
}

impl<A, R, E, C, H, Doc, Dest> HasSlots
    for Sealed<ReplyValue<HandleValue<A, R, Outs<E>, C, H, Doc>, Dest>>
where
    E: EntryMarkers,
{
    type Markers = E::Markers;
}

/// See the plain arena's `BindSlots`: the declared entries unify with their markers' paired
/// live values, so the definition is its own bound form.
macro_rules! impl_reply_bind_slots {
    ($(($($m:ident / $p:ident: $e:ident / $pipe:ident),+))+) => {$(
        impl<Conn, A, R, C, H, Doc, Dest, $($m, $p, $e, $pipe),+>
            BindSlots<Conn, ($(($p, $e, $pipe),)+)>
            for Sealed<
                ReplyValue<
                    HandleValue<
                        A,
                        R,
                        Outs<($(Slot<$m, <$p as PublishPolicy<Conn>>::Live, $e, $pipe>,)+)>,
                        C,
                        H,
                        Doc,
                    >,
                    Dest,
                >,
            >
        where
            Conn: ConnectedBroker,
            $(
                $m: OutSlot,
                $p: PublishPolicy<Conn>,
            )+
        {
            type Bound = Self;
            type Extra = ($(($p, $e, $pipe),)+);

            fn bind(self, sources: ($(($p, $e, $pipe),)+)) -> (Self, Self::Extra) {
                (self, sources)
            }
        }
    )+};
}

impl_reply_bind_slots! {
    (M0 / P0: E0 / Pipe0)
    (M0 / P0: E0 / Pipe0, M1 / P1: E1 / Pipe1)
    (M0 / P0: E0 / Pipe0, M1 / P1: E1 / Pipe1, M2 / P2: E2 / Pipe2)
}
impl<A, R, E, C, H, Doc, Dest> PublishingDef
    for Sealed<ReplyValue<HandleValue<A, R, Outs<E>, C, H, Doc>, Dest>>
where
    A: SoloAxis,
    R: ReplyShape<Wire: WireDocs<R, Doc>>,
    E: EntryMarkers + Send + Sync,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    Dest: ReplyDest<R>,
{
    type Input = A::Kind;
    type Injections = Outs<E>;
    type Reply = R;
    type Context = C;
    // See the eager cells: the settings builder carries the real source.
    type Source = Unnamed<Name>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn reply_name(&self) -> &str {
        self.0.dest.name()
    }

    fn description(&self) -> Option<&str> {
        self.0.value.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .value
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .value
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.value.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.value.docs.message_description
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        if let Some(declared) = &self.0.value.docs.outgoing {
            return declared.clone();
        }
        let mut declared = vec![
            OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R::Body>())
                .with_payload_schema(<R::Wire as WireDocs<R, Doc>>::payload_schema())
                .with_headers_schema(<R::Wire as WireDocs<R, Doc>>::headers_schema())
                .with_serialized(<R::Wire as WireDocs<R, Doc>>::SERIALIZED),
        ];
        declared.extend(E::outgoing());
        declared
    }
}

impl<T, R, E, C, S, H, Doc, Dest> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<Solo<T>, R, Outs<E>, C, H, Doc>, Dest>>
where
    Self: PublishingDef<Input = <Solo<T> as Axis>::Kind, Injections = Outs<E>, Reply = R, Context = C>,
    T: Input<Axis = Solo<T>> + Send + Sync + 'static,
    R: ReplyShape,
    E: Send + Sync,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<T, R, Outs<E>, C, S>,
{
    async fn call(
        &self,
        input: &T,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> Result<R, HandlerOutcome> {
        self.0.value.body.handle(input, injections, ctx).await
    }
}

impl<F, R, E, C, S, H, Doc, Dest> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<SoloDeserialized<F>, R, Outs<E>, C, H, Doc>, Dest>>
where
    Self: PublishingDef<
            Input = <SoloDeserialized<F> as Axis>::Kind,
            Injections = Outs<E>,
            Reply = R,
            Context = C,
        >,
    F: Deserialized + Send + Sync + 'static,
    for<'p> F::Output<'p>: Input<Axis = SoloDeserialized<F>>,
    R: ReplyShape,
    E: Send + Sync,
    C: Send + Sync,
    S: Send + Sync,
    H: for<'p> Handle<F::Output<'p>, R, Outs<E>, C, S>,
{
    async fn call(
        &self,
        input: &[u8],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> Result<R, HandlerOutcome> {
        let input = construct::<F, C, S>(input, ctx)?;
        self.0.value.body.handle(&input, injections, ctx).await
    }
}

impl<Hd, P, R, E, C, S, H, Doc, Dest> PublishingCall<S>
    for Sealed<ReplyValue<HandleValue<SoloPair<Hd, P>, R, Outs<E>, C, H, Doc>, Dest>>
where
    Self: PublishingDef<
            Input = <SoloPair<Hd, P> as Axis>::Kind,
            Injections = Outs<E>,
            Reply = R,
            Context = C,
        >,
    Message<Hd, P>: Input<Axis = SoloPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    R: ReplyShape,
    E: Send + Sync,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<Message<Hd, P>, R, Outs<E>, C, S>,
{
    async fn call(
        &self,
        input: &Message<Hd, P>,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> Result<R, HandlerOutcome> {
        self.0.value.body.handle(input, injections, ctx).await
    }
}
impl<A, R, E, C, H, Doc, Dest> BatchPublishingDef
    for Sealed<ReplyValue<HandleValue<A, Vec<R>, Outs<E>, C, H, Doc>, Dest>>
where
    A: BatchedAxis,
    R: ReplyShape<Wire: WireDocs<R, Doc>>,
    E: EntryMarkers + Send + Sync,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    Dest: ReplyDest<R>,
{
    type Input = A::Kind;
    type Injections = Outs<E>;
    type Context = C;
    type Reply = R;
    type Source = Unnamed<Name>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn reply_name(&self) -> &str {
        self.0.dest.name()
    }

    fn description(&self) -> Option<&str> {
        self.0.value.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .value
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .value
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.value.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.value.docs.message_description
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        if let Some(declared) = &self.0.value.docs.outgoing {
            return declared.clone();
        }
        let mut declared = vec![
            OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R::Body>())
                .with_payload_schema(<R::Wire as WireDocs<R, Doc>>::payload_schema())
                .with_headers_schema(<R::Wire as WireDocs<R, Doc>>::headers_schema())
                .with_serialized(<R::Wire as WireDocs<R, Doc>>::SERIALIZED),
        ];
        declared.extend(E::outgoing());
        declared
    }
}

impl<T, R, E, C, S, H, Doc, Dest> BatchPublishingCall<S>
    for Sealed<ReplyValue<HandleValue<Batch<T>, Vec<R>, Outs<E>, C, H, Doc>, Dest>>
where
    Self: BatchPublishingDef<
            Input = <Batch<T> as Axis>::Kind,
            Injections = Outs<E>,
            Context = C,
            Reply = R,
        >,
    [T]: Input<Axis = Batch<T>>,
    T: Send + Sync + 'static,
    R: ReplyShape,
    E: Send + Sync,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[T], Vec<R>, Outs<E>, C, S>,
{
    async fn call(
        &self,
        batch: &[T],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> Result<Vec<R>, BatchResult> {
        let verdict = self.0.value.body.handle(batch, injections, ctx).await;
        batch_reply_verdict(verdict, batch.len(), ctx.name())
    }
}

impl<Hd, P, R, E, C, S, H, Doc, Dest> BatchPublishingCall<S>
    for Sealed<ReplyValue<HandleValue<BatchPair<Hd, P>, Vec<R>, Outs<E>, C, H, Doc>, Dest>>
where
    Self: BatchPublishingDef<
            Input = <BatchPair<Hd, P> as Axis>::Kind,
            Injections = Outs<E>,
            Context = C,
            Reply = R,
        >,
    [Message<Hd, P>]: Input<Axis = BatchPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    R: ReplyShape,
    E: Send + Sync,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[Message<Hd, P>], Vec<R>, Outs<E>, C, S>,
{
    async fn call(
        &self,
        batch: &[Message<Hd, P>],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> Result<Vec<R>, BatchResult> {
        let verdict = self.0.value.body.handle(batch, injections, ctx).await;
        batch_reply_verdict(verdict, batch.len(), ctx.name())
    }
}
