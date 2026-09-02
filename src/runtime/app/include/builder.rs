//! The single-attachment registration builder that commits when the statement ends.

use std::fmt;
use std::marker::PhantomData;

use crate::Broker;

use crate::runtime::slot::WithSource;

use super::{
    BatchInjectMount, BatchPublishMount, CommitVia, IncludeSlots, InjectMount, PublishMount,
    RawReplyMount,
};
use crate::runtime::app::scope::BrokerScope;

/// A registration builder over one attachment, generic over its mount token.
///
/// Commits when dropped (the end of the `b.include(..)` statement);
/// [`publisher`](Self::publisher) replaces the reply source (defaulted when the call is
/// omitted). The per-form names are aliases: [`IncludePublishing`],
/// [`IncludeBatchPublishing`].
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
/// The attachment is the batch reply source: a typed stack, or its
/// [`transactional`](crate::runtime::TypedPublisher::transactional) form for one transaction per batch.
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

    /// Attaches the reply source: a [`TypedPublisher`](crate::runtime::TypedPublisher) stack naming the reply codec and
    /// transforms, a bare policy on the byte-reply form, or a [`Bound`](crate::runtime::Bound)
    /// token wrapping one for a cross-broker target. The runtime pairs it after the brokers
    /// connect.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<NewSource>>
    where
        WithSource<NewSource>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _default) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, WithSource::new(source))),
            _mount: PhantomData,
        }
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
