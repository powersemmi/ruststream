//! Outgoing message and the publish middleware pipeline.
//!
//! Every publish a handler makes - the reply of a `#[subscriber(.., publish(..))]` form, and every
//! message that leaves through an injected [`Out`](super::Out) slot - flows through a chain of
//! [`PublishLayer`] before reaching the broker publisher. Middleware transform the
//! payload (for example, wrap it in a Confluent / Avro envelope) and enrich the headers
//! (content-type, schema id), or observe it (publish metrics). The chain is symmetric to the
//! consume-side static [`Stack`](super::Stack).

use std::{borrow::Cow, future::Future, pin::Pin};

use bytes::BytesMut;

use crate::HeaderMap;
use crate::runtime::lifecycle::BoxError;

// The boxed future of the DYNAMIC middleware path only (PublishDynLayer / PublishDynNext).
// The static pipeline returns unboxed RPITIT futures; only the opt-in runtime-composed list
// pays this allocation.
pub(super) type PublishFut<'a> = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>>;

/// A mutable outgoing message flowing through the publish pipeline.
///
/// The [`name`](Self::name) is a [`Cow`]: the macro reply path borrows a string literal
/// (`reply_name(&self) -> &str`), so the common case carries the destination without an
/// allocation; a computed name moves in owned. The [`payload`](Self::payload_mut) is a
/// [`BytesMut`]: codec output moves in directly (no copy), and middleware can still mutate it in
/// place (for example wrapping it in an envelope). Middleware may change the name, transform the
/// payload, and enrich the [`headers`](Self::headers_mut) before the message is sent.
#[derive(Debug, Clone)]
pub struct Outgoing<'a> {
    name: Cow<'a, str>,
    payload: BytesMut,
    headers: HeaderMap,
}

impl<'a> Outgoing<'a> {
    /// Creates an outgoing message with no headers.
    ///
    /// Pass a `&str` (a borrowed destination, the no-allocation case) or a `String` (a computed
    /// owned one) for `name`; pass a [`BytesMut`] (codec output moves in) or a `&[u8]` for the
    /// payload.
    #[must_use]
    pub fn new(name: impl Into<Cow<'a, str>>, payload: impl Into<BytesMut>) -> Self {
        Self {
            name: name.into(),
            payload: payload.into(),
            headers: HeaderMap::new(),
        }
    }

    /// The destination name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Sets the destination name.
    pub fn set_name(&mut self, name: impl Into<Cow<'a, str>>) {
        self.name = name.into();
    }

    /// The payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// The payload bytes, mutably (for envelope wrapping).
    pub fn payload_mut(&mut self) -> &mut BytesMut {
        &mut self.payload
    }

    /// Replaces the payload.
    pub fn set_payload(&mut self, payload: impl Into<BytesMut>) {
        self.payload = payload.into();
    }

    /// The outgoing headers.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The outgoing headers, mutably.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }
}

mod sealed {
    /// Seals [`ReplyPublisher`](super::ReplyPublisher): the reply-publishing strategies are the
    /// two live sinks above, not an extension point.
    pub trait Sealed {}

    impl<P, C, PL, BL> Sealed for super::TypedPublisher<P, C, PL, BL> {}
    impl<P, C, PL, BL> Sealed for super::Transactional<P, C, PL, BL> {}
}

mod builder;
mod ext;
mod out;
mod pipeline;
mod publisher;
mod reply;
mod sink;
mod transaction;
mod transform;
mod wiring;

pub use builder::{
    BoundSegment, EncodeOutcome, EncodedWire, HeaderSource, HeadersUnset, MapHeaders, MessageBody,
    MessageWire, MissingSegment, PayloadError, PublishAt, PublishBuilder, PublishError,
    PublishHeaders, ResolvedName, SatisfiesContract, SerializePayloadError, Serialized,
    SerializedWire, SuppliedName, TemplateAddress, TypedHeaders, WirePayload,
};
pub(crate) use builder::{message_of, raw_of};
pub use ext::PublishExt;
pub use out::{
    LowerOutTransforms, OutPipeline, OutTransform, OutTransformIdentity, OutTransformStack,
    PipelinePublishError,
};
pub use pipeline::{
    PublishDynLayer, PublishDynNext, PublishDynStack, PublishIdentity, PublishLayer, PublishNext,
    PublishPipeline, PublishStack,
};
pub use publisher::{Transactional, TypedPublisher};
pub use reply::ReplyPublisher;
pub use sink::{CallCodec, PublishCodec, PublishSink, UnnamedCodec};
pub use transaction::{
    Admits, AnyDeclared, TransactionPublishError, TransactionScope, TypedTransaction,
};
pub use transform::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, ForBatch,
    PublishContext, PublishTransform, PublishTransformIdentity, PublishTransformStack, for_batch,
};
pub use wiring::{
    AddBatchReplyTransform, AddReplyTransform, CodecSlotOpen, Direct, InTransaction,
    MapReplyPolicy, NameReplyCodec, PublishingDirectly, RawReplyWiring, ReplyWiring,
    TransactionalReply,
};

#[cfg(test)]
mod tests;
