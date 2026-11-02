//! Outgoing message and the publish middleware pipeline.
//!
//! Every publish a handler makes - the reply of a `#[subscriber(.., reply(..))]` form, and every
//! message that leaves through an injected [`Out`](super::Out) slot - flows through a chain of
//! [`PublishLayer`] before reaching the broker publisher. Middleware transform the
//! payload (for example, wrap it in a Confluent / Avro envelope) and enrich the headers
//! (content-type, schema id), or observe it (publish metrics). The chain is symmetric to the
//! consume-side static [`Stack`](super::Stack).

#[cfg(ruststream_alloc)]
use alloc::boxed::Box;
#[cfg(ruststream_alloc)]
use core::pin::Pin;

#[cfg(ruststream_alloc)]
use crate::lifecycle::BoxError;
pub use crate::outgoing::{Outgoing, OutgoingName};
pub use crate::wire::{Serialized, WireBytes};
#[cfg(ruststream_alloc)]
use ruststream_core::IfSend;

// The boxed future of the DYNAMIC middleware path only (PublishDynLayer / PublishDynNext).
// The static pipeline returns unboxed RPITIT futures; only the opt-in runtime-composed list
// pays this allocation, and it exists only where there is a heap to box on.
#[cfg(ruststream_alloc)]
pub(super) type PublishFut<'a> = IfSend<
    Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>>,
    Pin<Box<dyn Future<Output = Result<(), BoxError>> + 'a>>,
>;

mod sealed {
    /// Seals [`ReplyPublisher`](super::ReplyPublisher): the reply-publishing strategies are the
    /// two live sinks above, not an extension point.
    pub trait Sealed {}

    impl<P, C, PL, BL> Sealed for super::TypedPublisher<P, C, PL, BL> {}
    impl<P, C, PL, BL> Sealed for super::Transactional<P, C, PL, BL> {}
}

mod builder;
#[cfg(ruststream_alloc)]
mod dynamic;
mod ext;
mod out;
mod pipeline;
mod publisher;
mod reply;
mod scratch;
mod sink;
mod transaction;
mod transform;
mod wiring;

pub use builder::message_of;
#[cfg(test)]
#[cfg(ruststream_alloc)]
pub(crate) use builder::raw_of;
pub use builder::{
    BoundSegment, EncodedWire, HeaderSource, HeadersUnset, MapHeaders, MessageBody, MessageWire,
    MissingSegment, PayloadError, PublishAt, PublishBuilder, PublishError, PublishHeaders,
    ResolvedName, SatisfiesContract, SerializedWire, SuppliedName, TemplateAddress, TypedHeaders,
    WirePayload,
};
pub use builder::{EncodeOutcome, SerializePayloadError};
#[cfg(ruststream_alloc)]
pub use dynamic::{PublishDynLayer, PublishDynNext, PublishDynStack};
pub use ext::PublishExt;
pub use out::{
    LowerOutTransforms, NamedDestinationSend, NarrowToUse, OutPipeline, PipelinePublishError,
    SendOnlyPolicy, SlotStackUse, SlotTransforms,
};
pub(crate) use pipeline::SendThrough;
pub use pipeline::{PublishIdentity, PublishLayer, PublishNext, PublishPipeline, PublishStack};
pub use publisher::{Transactional, TypedPublisher};
pub use reply::{BatchRefusal, ReplyPublisher};
pub use sink::{CallCodec, PublishCodec, PublishSink, SinkOutcome, SinkRefusal, UnnamedCodec};
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
#[cfg(ruststream_alloc)]
mod tests;
