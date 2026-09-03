//! The single-attachment registration builder that commits when the statement ends.

use std::fmt;
use std::marker::PhantomData;

use crate::Broker;

use crate::runtime::publish::{
    AddBatchReplyTransform, AddReplyTransform, CodecSlotOpen, NameReplyCodec, PublishingDirectly,
    TransactionalReply,
};
use crate::runtime::slot::WithSource;

use super::{
    BatchInjectMount, BatchPublishMount, CommitVia, IncludeSlots, InjectMount, PublishMount,
    RawReplyMount, ReplyAttachment,
};
use crate::runtime::app::scope::BrokerScope;

/// A registration builder over one attachment, generic over its mount token.
///
/// Commits when dropped (the end of the `b.include(..)` statement);
/// [`publisher`](Self::publisher) names the reply's publish policy (the broker's default when
/// the call is omitted), and the steps after it - [`codec`](Self::codec),
/// [`transform`](Self::transform), [`batch_transform`](Self::batch_transform),
/// [`transactional`](Self::transactional) - fill the rest of the reply wiring. The per-form
/// names are aliases: [`IncludePublishing`], [`IncludeBatchPublishing`].
pub struct IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    // Options only so `publisher` can move the pieces into the replacement builder out of a
    // Drop type; both stay `Some` until the commit or that replacement.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Attachment)>,
    _mount: PhantomData<Mount>,
}

/// The builder [`BrokerScope::include`] returns for a `publish("dest")` definition: the
/// attachment is the reply source, defaulting to the broker's default publish policy under
/// the default codec.
pub type IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, PublishMount, B, Layers, C, State, Pipeline, Def, Source>;

/// The builder [`BrokerScope::include`](crate::runtime::BrokerScope::include) returns for a
/// `publish("dest")` definition whose reply type is [`Serialized`](crate::runtime::Serialized).
///
/// The reply bytes leave as they are through a bare publisher.
pub type IncludeRawReply<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, RawReplyMount, B, Layers, C, State, Pipeline, Def, Source>;

/// The builder [`BrokerScope::include`] returns for a handler with
/// [`Out`](crate::runtime::Out) parameters: the attachment is the slot tuple, with no
/// defaults.
pub type IncludeOut<'s, B, Layers, C, State, Pipeline, Def, Slots> =
    IncludeSlots<'s, InjectMount, B, Layers, C, State, Pipeline, Def, Slots>;

/// The builder [`BrokerScope::include`] returns for a batch publishing (`&[T]` +
/// `publish("dest")`) definition.
///
/// The attachment is the page's reply wiring, which [`transactional`](Self::transactional)
/// switches to one transaction per page.
pub type IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, BatchPublishMount, B, Layers, C, State, Pipeline, Def, Source>;

/// The builder [`BrokerScope::include`] returns for a batch handler with
/// [`Out`](crate::runtime::Out) parameters.
pub type IncludeBatchOut<'s, B, Layers, C, State, Pipeline, Def, Slots> =
    IncludeSlots<'s, BatchInjectMount, B, Layers, C, State, Pipeline, Def, Slots>;

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
    IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    pub(super) fn new(
        def: Def,
        attachment: Attachment,
        scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) -> Self {
        Self {
            scope: Some(scope),
            parts: Some((def, attachment)),
            _mount: PhantomData,
        }
    }

    /// Names the reply's publish policy: one of the broker prelude's (`Publish`,
    /// `TransactionalPublish`, ...), or a [`Bound`](crate::runtime::Bound) token wrapping one for
    /// a cross-broker target. The runtime pairs it after the brokers connect.
    ///
    /// On an encoded reply the call opens the reply wiring, and [`codec`](Self::codec),
    /// [`transform`](Self::transform), [`batch_transform`](Self::batch_transform) and
    /// [`transactional`](Self::transactional) chain onto it; a byte-for-byte
    /// ([`Serialized`](crate::runtime::Serialized)) reply takes the policy and nothing else,
    /// because its bytes leave unencoded.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<Mount::Wiring>>
    where
        Mount: ReplyAttachment<Policy>,
        WithSource<Mount::Wiring>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, scope) = self.take();
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, WithSource::new(Mount::wire(policy)))),
            _mount: PhantomData,
        }
    }

    /// The definition and the scope, moved out of the builder without running its commit.
    ///
    /// # Panics
    ///
    /// Never in practice: both stay present until the commit or a replacement takes them.
    fn take(mut self) -> (Def, &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let (def, _attachment) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        (def, scope)
    }
}

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, W>
    IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<W>>
where
    B: Broker + 'static,
    WithSource<W>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    /// Rebuilds the builder over a grown wiring, keeping the definition and the scope.
    ///
    /// # Panics
    ///
    /// Never in practice: see [`take`](Self::take).
    fn map_wiring<W2>(
        mut self,
        f: impl FnOnce(W) -> W2,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<W2>>
    where
        WithSource<W2>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, wiring) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, wiring.map(f))),
            _mount: PhantomData,
        }
    }

    /// Encodes the reply with `codec` instead of the [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Named once per registration: the wiring's codec slot is filled by this call, so a second
    /// one does not compile.
    // No `must_use`: dropping the builder at the end of the statement IS the commit.
    pub fn codec<Cd>(
        self,
        codec: Cd,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<W::Out>>
    where
        W: NameReplyCodec<Cd, Slot: CodecSlotOpen>,
        WithSource<W::Out>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        self.map_wiring(|wiring| wiring.name_codec(codec))
    }

    /// Composes a static [`PublishTransform`](crate::runtime::PublishTransform) onto every reply
    /// of this registration. The first one added runs first (closest to the encoded value).
    // No `must_use`: see `codec`.
    pub fn transform<N>(
        self,
        transform: N,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<W::Out>>
    where
        W: AddReplyTransform<N>,
        WithSource<W::Out>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        self.map_wiring(|wiring| wiring.add_transform(transform))
    }

    /// Composes a [`BatchPublishTransform`](crate::runtime::BatchPublishTransform) onto every
    /// reply of a page (`&[T]` plus `publish(..)`), after the per-message stack. Wrap a
    /// per-message transform with [`for_batch`](crate::runtime::for_batch) to reuse it here.
    // No `must_use`: see `codec`.
    pub fn batch_transform<N>(
        self,
        transform: N,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<W::Out>>
    where
        W: AddBatchReplyTransform<N>,
        WithSource<W::Out>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        self.map_wiring(|wiring| wiring.add_batch_transform(transform))
    }

    /// Publishes a page's replies inside one broker transaction: they all become visible
    /// atomically on commit, or none of them do.
    ///
    /// The policy's live publisher has to be a
    /// [`TransactionalPublisher`](crate::TransactionalPublisher), which the pairing checks
    /// against this scope's broker; a one-message reply has no page to make atomic, so this
    /// wiring only mounts on the page forms.
    // No `must_use`: see `codec`.
    pub fn transactional(
        self,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<W::Out>>
    where
        W: TransactionalReply<State: PublishingDirectly>,
        WithSource<W::Out>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        self.map_wiring(TransactionalReply::into_transactional)
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Attachment> fmt::Debug
    for IncludeWith<'_, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncludeWith").finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Attachment> Drop
    for IncludeWith<'_, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
    }
}
