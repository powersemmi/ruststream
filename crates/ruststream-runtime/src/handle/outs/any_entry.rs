//! One arena entry as a runtime-neutral body sees it: either runner's slot, picked by marker.
//!
//! A `#[subscriber]` body that publishes is one function for both runners. It is generic over
//! each of its entries and bounds it with [`SlotEntry`], whose capability is the local family's
//! (`Wire: LocalPublisher`), which every `Publisher` satisfies. The multi-thread runner's mount
//! then asks the body's future to be `Send` at the concrete entries it wired, where the compiler
//! sees through every publish the body makes.

use core::marker::PhantomData;
use core::ops::Deref;

use crate::codec::Codec;
use crate::publish::{Admits, TransactionScope, TypedTransaction};
use crate::publish::{
    HeadersUnset, LocalOutPipeline, LocalPublishSink, MessageBody, OutPipeline, PublishBuilder,
};
use crate::slot::{ContainsMessage, OutSlot, PublishedThrough, SlotPublisher};
use crate::{
    HeaderMap, LocalPublisher, OutgoingDestination, OutgoingFor, OwnedTransactions, Publisher,
    TransactionalPublisher,
};

use super::arena::Outs;
use super::entry::Slot;
use super::local_entry::{LocalSlot, SlotEncoding};

/// An arena entry of either runner, as a runtime-neutral body holds it: a window onto the live
/// value the slot's policy paired.
pub trait SlotEntry<M, Body = ()>: Deref<Target = Self::Wire> {
    /// The live value the slot's policy paired.
    type Wire;
}

impl<M, W, E, Pipe, Body> SlotEntry<M, Body> for Slot<M, W, E, Pipe, Body> {
    type Wire = W;
}

impl<M, W, E, Pipe, Body> SlotEntry<M, Body> for LocalSlot<M, W, E, Pipe, Body> {
    type Wire = W;
}

/// An entry whose live value publishes: the slot itself publishes through its path, and
/// [`message`](Self::message) starts a typed publish over the slot's codec and dictionary.
pub trait SlotPublish<M, Body = ()>:
    SlotEntry<M, Body, Wire: LocalPublisher> + LocalPublisher
{
    /// The codec typed publishes through the slot encode with.
    type Enc: SlotEncoding;
    /// The sink a publish through the slot travels.
    type Sink<'a>: LocalPublishSink<Options = <Self::Wire as LocalPublisher>::Options>
    where
        Self: 'a;

    /// Starts a typed publish through the slot.
    fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<Self::Sink<'a>, MessageBody<'a, T>, &'a Self::Enc, HeadersUnset, T::Form>
    where
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination + PublishedThrough<M>;
}

impl<M, W, E, Pipe, Body> SlotPublish<M, Body> for Slot<M, W, E, Pipe, Body>
where
    M: OutSlot,
    W: Publisher,
    E: Codec + Send + Sync,
    Pipe: OutPipeline<W>,
{
    type Enc = E;
    type Sink<'a>
        = &'a Self
    where
        Self: 'a;

    fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a Self, MessageBody<'a, T>, &'a E, HeadersUnset, T::Form>
    where
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination + PublishedThrough<M>,
    {
        Self::message(self, value)
    }
}

impl<M, W, E, Pipe, Body> SlotPublish<M, Body> for LocalSlot<M, W, E, Pipe, Body>
where
    M: OutSlot,
    W: LocalPublisher,
    E: SlotEncoding,
    Pipe: LocalOutPipeline<W>,
{
    type Enc = E;
    type Sink<'a>
        = &'a Self
    where
        Self: 'a;

    fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a Self, MessageBody<'a, T>, &'a E, HeadersUnset, T::Form>
    where
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination + PublishedThrough<M>,
    {
        Self::message(self, value)
    }
}

/// The position of an entry in the arena tuple, for [`Outs::slot`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct AnyOutPos<const N: usize>;

/// Picks the entry of marker `M` out of an arena tuple, whichever runner built it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this arena has no `{M}` slot",
    note = "the body's `Out` parameters list the slots it may publish through; check the marker"
)]
pub trait SelectEntry<M, I> {
    /// The picked entry.
    type Picked;
    /// Borrows the picked entry.
    fn pick_entry(&self) -> &Self::Picked;
}

macro_rules! impl_select_entry {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<M, Body, Entry $(, $before)* $(, $after)*> SelectEntry<M, (AnyOutPos<$pos>, Body)>
            for ($($before,)* Entry, $($after,)*)
        where
            Entry: SlotEntry<M, Body>,
        {
            type Picked = Entry;

            fn pick_entry(&self) -> &Entry {
                #[allow(non_snake_case)]
                let ($($before,)* picked, $($after,)*) = self;
                $(let _ = $before;)*
                $(let _ = $after;)*
                picked
            }
        }
    )+};
}

impl_select_entry! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
}

impl<E> Outs<E> {
    /// The entry of `marker`, as a runtime-neutral body reaches it.
    #[doc(hidden)]
    #[allow(clippy::needless_pass_by_value)]
    pub fn slot<M, I>(&self, marker: M) -> &<E as SelectEntry<M, I>>::Picked
    where
        E: SelectEntry<M, I>,
    {
        let _ = marker;
        self.entries().pick_entry()
    }
}

// The slot publishes through its own path, so a direct `publish` keeps the slot's attribution,
// as the multi-thread runner's slot does.
#[allow(clippy::future_not_send)]
impl<M, W, E, Pipe, Body> LocalPublisher for LocalSlot<M, W, E, Pipe, Body>
where
    M: OutSlot,
    W: LocalPublisher,
    Pipe: LocalOutPipeline<W>,
{
    type Payload = W::Payload;
    type Error = W::Error;
    type Options = W::Options;

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let mut sink = self;
        LocalPublishSink::send(&mut sink, msg, options).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        (**self).base_headers()
    }
}

/// The transaction openers of a slot whose wire is transactional: the multi-thread runner's.
#[doc(hidden)]
pub trait SlotTransactions<M: 'static, Body = ()>:
    SlotPublish<M, Body, Wire: 'static, Enc: 'static>
{
    /// Opens a broker-side transaction scope over the slot's wire.
    #[allow(clippy::type_complexity)] // the scope's own type, which the body never names
    fn begin(
        &self,
    ) -> impl Future<
        Output = Result<
            TransactionScope<'_, SlotPublisher<Self::Wire, M>, &'_ Self::Enc, SlotAdmit<M, Body>>,
            <Self::Wire as Publisher>::Error,
        >,
    >
    where
        Self::Wire: TransactionalPublisher;

    /// Opens an owned transaction over the slot's wire, typed by the slot's dictionary.
    #[allow(clippy::type_complexity)]
    fn transaction(
        &self,
    ) -> impl Future<
        Output = Result<
            TypedTransaction<
                <Self::Wire as OwnedTransactions>::Transaction,
                &'_ Self::Enc,
                SlotAdmit<M, Body>,
            >,
            <Self::Wire as Publisher>::Error,
        >,
    >
    where
        Self::Wire: OwnedTransactions;
}

impl<M, W, E, Pipe, Body> SlotTransactions<M, Body> for Slot<M, W, E, Pipe, Body>
where
    M: OutSlot + 'static,
    W: Publisher + 'static,
    E: Codec + Send + Sync + 'static,
    Pipe: OutPipeline<W>,
{
    fn begin(
        &self,
    ) -> impl Future<
        Output = Result<
            TransactionScope<'_, SlotPublisher<W, M>, &'_ E, SlotAdmit<M, Body>>,
            W::Error,
        >,
    >
    where
        W: TransactionalPublisher,
    {
        let (wired, codec) = self.parts();
        TransactionScope::open(wired, codec)
    }

    fn transaction(
        &self,
    ) -> impl Future<
        Output = Result<TypedTransaction<W::Transaction, &'_ E, SlotAdmit<M, Body>>, W::Error>,
    >
    where
        W: OwnedTransactions,
    {
        let (wired, codec) = self.parts();
        TypedTransaction::open(wired, codec)
    }
}

/// What a transaction opened through [`SlotTransactions`] admits: the marker's dictionary,
/// narrowed by the parameter's declared set, as the slot's own typed publish.
#[doc(hidden)]
#[derive(Debug)]
pub struct SlotAdmit<M, Body>(PhantomData<fn() -> (M, Body)>);

impl<M, Body, T, Index> Admits<T, Index> for SlotAdmit<M, Body>
where
    T: PublishedThrough<M>,
    Body: ContainsMessage<T, Index>,
{
}
