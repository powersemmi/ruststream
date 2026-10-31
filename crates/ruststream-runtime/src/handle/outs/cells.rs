//! The slot-carrying cells of the form matrix: a sealed definition whose body declares an arena
//! mounts through the inject machinery, bound to the include site's policies, with the body
//! adapted to the dispatch contracts here.

use crate::batch::BatchResult;
use crate::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::context::Context;
use crate::handler::HandlerOutcome;
use crate::inject::{InjectCall, InjectDef, LocalInjectCall};
use crate::metadata::OutgoingMessageMetadata;
use crate::router::IncludeDef;
use crate::slot::{BindSlots, HasSlots, LocalBindSlots, LocalHasSlots, OutSlot};
use crate::{
    ConnectedBroker, LocalConnectedBroker, LocalPublishPolicy, Name, PublishPolicy, Unnamed,
};
use alloc::string::String;
use alloc::vec::Vec;

use super::arena::{EntryMarkers, Outs, SlotDictionary};
use super::entry::Slot;
use super::local_entry::{LocalEntryMarkers, LocalSlot};
use crate::handle::axis::{
    Axis, AxisDocs, Batch, BatchPair, BatchSet, BatchedAxis, Deserialized, Input, Message, Solo,
    SoloAxis, SoloCarried, SoloDeserialized, SoloPair, SoloSet,
};
use crate::handle::eager::{construct, run_batch, settle_solo};
use crate::handle::value::{HandleValue, Sealed};
use crate::handle::{Handle, LocalHandle};

impl<A, E, C, H, Doc> IncludeDef for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    A: Axis,
{
    type Form = A::SlotForm;
}

#[doc(hidden)]
impl<A, E, C, H, Doc> HasSlots for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    E: EntryMarkers,
{
    type Markers = E::Markers;
}

#[doc(hidden)]
impl<A, E, C, H, Doc> LocalHasSlots for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    E: LocalEntryMarkers,
{
    type Markers = E::Markers;
}

/// Ties the declared arena to the bound policies: the entry a body left generic unifies with
/// its marker's paired live value, so the definition is its own bound form and the body's
/// capability bounds are checked right here.
macro_rules! impl_bind_slots {
    ($(($($m:ident / $p:ident: $e:ident / $pipe:ident / $b:ident),+))+) => {$(
        #[doc(hidden)]
        impl<Conn, A, C, H, Doc, $($m, $p, $e, $pipe, $b),+>
            BindSlots<Conn, ($(($p, $e, $pipe),)+)>
            for Sealed<
                HandleValue<
                    A,
                    (),
                    Outs<($(Slot<$m, <$p as PublishPolicy<Conn>>::Live, $e, $pipe, $b>,)+)>,
                    C,
                    H,
                    Doc,
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

impl_bind_slots! {
    (M0 / P0: E0 / Pipe0 / B0)
    (M0 / P0: E0 / Pipe0 / B0, M1 / P1: E1 / Pipe1 / B1)
    (M0 / P0: E0 / Pipe0 / B0, M1 / P1: E1 / Pipe1 / B1, M2 / P2: E2 / Pipe2 / B2)
}

/// The local family's binding: each entry a body left generic unifies with its marker's
/// [`LocalSlot`] over the paired live value of the local family.
macro_rules! impl_local_bind_slots {
    ($(($($m:ident / $p:ident: $e:ident / $pipe:ident / $b:ident),+))+) => {$(
        #[doc(hidden)]
        impl<Conn, A, C, H, Doc, $($m, $p, $e, $pipe, $b),+>
            LocalBindSlots<Conn, ($(($p, $e, $pipe),)+)>
            for Sealed<
                HandleValue<
                    A,
                    (),
                    Outs<($(LocalSlot<$m, <$p as LocalPublishPolicy<Conn>>::Live, $e, $pipe, $b>,)+)>,
                    C,
                    H,
                    Doc,
                >,
            >
        where
            Conn: LocalConnectedBroker,
            $(
                $m: OutSlot,
                $p: LocalPublishPolicy<Conn>,
            )+
        {
            type Bound = Self;
            type Extra = ($(($p, $e, $pipe),)+);

            fn bind_local(self, sources: ($(($p, $e, $pipe),)+)) -> (Self, Self::Extra) {
                (self, sources)
            }
        }
    )+};
}

impl_local_bind_slots! {
    (M0 / P0: E0 / Pipe0 / B0)
    (M0 / P0: E0 / Pipe0 / B0, M1 / P1: E1 / Pipe1 / B1)
    (M0 / P0: E0 / Pipe0 / B0, M1 / P1: E1 / Pipe1 / B1, M2 / P2: E2 / Pipe2 / B2)
}

impl<A, C, H, Doc, E> InjectDef for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    A: SoloAxis,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    // The arena is a type only here: the definition holds no entry, so a local family's entries,
    // which are not `Send`, describe a definition all the same.
    E: SlotDictionary,
{
    type Input = A::Kind;
    type Context = C;
    // See the eager cells: the settings builder carries the real source.
    type Source = Unnamed<Name>;
    type Injections = Outs<E>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.0.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.docs.message_description
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        self.0.docs.outgoing.clone().unwrap_or_else(E::outgoing)
    }
}

impl<T, C, S, H, Doc, E> InjectCall<S> for Sealed<HandleValue<Solo<T>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <Solo<T> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    T: Input<Axis = Solo<T>> + Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<T, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &T,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        settle_solo(self.0.body.handle(input, injections, ctx).await)
    }
}

impl<T, C, S, H, Doc, E> InjectCall<S> for Sealed<HandleValue<SoloSet<T>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <SoloSet<T> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    T: Input<Axis = SoloSet<T>> + Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<T, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &T,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        settle_solo(self.0.body.handle(input, injections, ctx).await)
    }
}

impl<T, C, S, H, Doc, E> InjectCall<S>
    for Sealed<HandleValue<SoloCarried<T>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <SoloCarried<T> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    T: Input<Axis = SoloCarried<T>> + Clone + Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<T, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &T,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        settle_solo(self.0.body.handle(input, injections, ctx).await)
    }
}

impl<F, C, S, H, Doc, E> InjectCall<S>
    for Sealed<HandleValue<SoloDeserialized<F>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <SoloDeserialized<F> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    F: Deserialized + Send + Sync + 'static,
    for<'p> F::Output<'p>: Input<Axis = SoloDeserialized<F>>,
    C: Send + Sync,
    S: Send + Sync,
    H: for<'p> Handle<F::Output<'p>, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &[u8],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        let input = match construct::<F, C, S>(input, ctx) {
            Ok(input) => input,
            Err(outcome) => return outcome,
        };
        settle_solo(self.0.body.handle(&input, injections, ctx).await)
    }
}

impl<Hd, P, C, S, H, Doc, E> InjectCall<S>
    for Sealed<HandleValue<SoloPair<Hd, P>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <SoloPair<Hd, P> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    Message<Hd, P>: Input<Axis = SoloPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<Message<Hd, P>, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &Message<Hd, P>,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        settle_solo(self.0.body.handle(input, injections, ctx).await)
    }
}

/// The local family's single-message calls: the body is a [`LocalHandle`] and its future is
/// awaited where the delivery is polled.
macro_rules! impl_local_inject_call {
    ($($axis:ident $(+ $extra:ident)?),+ $(,)?) => {$(
        // Polled on the one thread that hosts the app: `Send` exactly where the types it runs are.
        #[allow(clippy::future_not_send)]
        impl<T, C, S, H, Doc, E> LocalInjectCall<S>
            for Sealed<HandleValue<$axis<T>, (), Outs<E>, C, H, Doc>>
        where
            Self: InjectDef<Input = <$axis<T> as Axis>::Kind, Context = C, Injections = Outs<E>>,
            // An input is shared with the worker tasks of the multi-thread runner, so the axes ask it
            // of every input; the local family keeps the same axes.
            T: Input<Axis = $axis<T>> $(+ $extra)? + Send + Sync + 'static,
            H: LocalHandle<T, (), Outs<E>, C, S>,
        {
            async fn call_local(
                &self,
                input: &T,
                injections: &Outs<E>,
                ctx: &mut Context<'_, C, S>,
            ) -> HandlerOutcome {
                settle_solo(self.0.body.handle(input, injections, ctx).await)
            }
        }
    )+};
}

impl_local_inject_call!(Solo, SoloSet, SoloCarried + Clone);

// Polled on the one thread that hosts the app: `Send` exactly where the types it runs are.
#[allow(clippy::future_not_send)]
impl<F, C, S, H, Doc, E> LocalInjectCall<S>
    for Sealed<HandleValue<SoloDeserialized<F>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <SoloDeserialized<F> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    F: Deserialized + Send + Sync + 'static,
    for<'p> F::Output<'p>: Input<Axis = SoloDeserialized<F>>,
    H: for<'p> LocalHandle<F::Output<'p>, (), Outs<E>, C, S>,
{
    async fn call_local(
        &self,
        input: &[u8],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        let input = match construct::<F, C, S>(input, ctx) {
            Ok(input) => input,
            Err(outcome) => return outcome,
        };
        settle_solo(self.0.body.handle(&input, injections, ctx).await)
    }
}

// Polled on the one thread that hosts the app: `Send` exactly where the types it runs are.
#[allow(clippy::future_not_send)]
impl<Hd, P, C, S, H, Doc, E> LocalInjectCall<S>
    for Sealed<HandleValue<SoloPair<Hd, P>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <SoloPair<Hd, P> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    Message<Hd, P>: Input<Axis = SoloPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    H: LocalHandle<Message<Hd, P>, (), Outs<E>, C, S>,
{
    async fn call_local(
        &self,
        input: &Message<Hd, P>,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        settle_solo(self.0.body.handle(input, injections, ctx).await)
    }
}

impl<A, C, H, Doc, E> BatchInjectDef for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    A: BatchedAxis,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    E: EntryMarkers + Send + Sync,
{
    type Input = A::Kind;
    type Source = Unnamed<Name>;
    type Injections = Outs<E>;
    type Context = C;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.0.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.docs.message_description
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        self.0.docs.outgoing.clone().unwrap_or_else(E::outgoing)
    }
}

impl<T, C, S, H, Doc, E> BatchInjectCall<S>
    for Sealed<HandleValue<Batch<T>, (), Outs<E>, C, H, Doc>>
where
    Self: BatchInjectDef<Input = <Batch<T> as Axis>::Kind, Injections = Outs<E>, Context = C>,
    [T]: Input<Axis = Batch<T>>,
    T: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[T], (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        batch: &[T],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> BatchResult {
        run_batch(&self.0.body, injections, batch, ctx).await
    }
}

impl<T, C, S, H, Doc, E> BatchInjectCall<S>
    for Sealed<HandleValue<BatchSet<T>, (), Outs<E>, C, H, Doc>>
where
    Self: BatchInjectDef<Input = <BatchSet<T> as Axis>::Kind, Injections = Outs<E>, Context = C>,
    [T]: Input<Axis = BatchSet<T>>,
    T: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[T], (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        batch: &[T],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> BatchResult {
        run_batch(&self.0.body, injections, batch, ctx).await
    }
}

impl<Hd, P, C, S, H, Doc, E> BatchInjectCall<S>
    for Sealed<HandleValue<BatchPair<Hd, P>, (), Outs<E>, C, H, Doc>>
where
    Self:
        BatchInjectDef<Input = <BatchPair<Hd, P> as Axis>::Kind, Injections = Outs<E>, Context = C>,
    [Message<Hd, P>]: Input<Axis = BatchPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[Message<Hd, P>], (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        batch: &[Message<Hd, P>],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> BatchResult {
        run_batch(&self.0.body, injections, batch, ctx).await
    }
}
