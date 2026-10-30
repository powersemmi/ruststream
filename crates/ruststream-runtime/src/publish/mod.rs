//! Outgoing message and the publish middleware pipeline.
//!
//! Every publish a handler makes - the reply of a `#[subscriber(.., reply(..))]` form, and every
//! message that leaves through an injected [`Out`](super::Out) slot - flows through a chain of
//! [`PublishLayer`] before reaching the broker publisher. Middleware transform the
//! payload (for example, wrap it in a Confluent / Avro envelope) and enrich the headers
//! (content-type, schema id), or observe it (publish metrics). The chain is symmetric to the
//! consume-side static [`Stack`](super::Stack).

use std::pin::Pin;

use crate::lifecycle::BoxError;
pub use crate::outgoing::{Outgoing, OutgoingName};
pub use crate::wire::{Serialized, WireBytes};

// The boxed future of the DYNAMIC middleware path only (PublishDynLayer / PublishDynNext).
// The static pipeline returns unboxed RPITIT futures; only the opt-in runtime-composed list
// pays this allocation.
pub(super) type PublishFut<'a> = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>>;

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

pub use builder::message_of;
#[cfg(test)]
pub(crate) use builder::raw_of;
pub use builder::{
    BoundSegment, EncodeOutcome, EncodedWire, HeaderSource, HeadersUnset, MapHeaders, MessageBody,
    MessageWire, MissingSegment, PayloadError, PublishAt, PublishBuilder, PublishError,
    PublishHeaders, ResolvedName, SatisfiesContract, SerializePayloadError, SerializedWire,
    SuppliedName, TemplateAddress, TypedHeaders, WirePayload,
};
pub use ext::PublishExt;
pub use out::{
    LowerOutTransforms, NamedDestinationSend, NarrowToUse, OutPipeline, PipelinePublishError,
    SendOnlyPolicy, SlotStackUse, SlotTransforms,
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
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, ContextKind,
    DestinationSettled, DestinationUse, Either, FitsOffer, FitsTaken, ForBatch, ForReply, ForSlot,
    Names, NamesDestination, NamingOffered, NamingUntaken, PublishContext, PublishTransform,
    PublishTransformIdentity, PublishTransformStack, Reads, SlotContext, for_batch,
};
pub use wiring::{
    AddBatchReplyTransform, AddReplyTransform, CodecSlotOpen, Direct, InTransaction,
    MapReplyPolicy, NameReplyCodec, PublishingDirectly, RawReplyWiring, ReplyWiring,
    TransactionalReply,
};

#[cfg(test)]
mod tests;
