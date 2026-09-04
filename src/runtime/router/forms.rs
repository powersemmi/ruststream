//! Form tokens for [`IncludeDef`](super::IncludeDef): which mounting machinery a definition uses.
//!
//! A definition names its form once, and both registration surfaces (a
//! [`Router`](super::Router) chain and a [`BrokerScope`](crate::runtime::BrokerScope)) dispatch
//! on that token, so `include(def)` reads the same wherever it is written.

/// A plain subscriber (`#[subscriber("in")]`).
#[derive(Debug, Clone, Copy)]
pub struct Subscribing;
/// A self-deserializing subscriber (a handler taking a
/// [`Deserialized`](crate::runtime::Deserialized) input): no decode, no codec.
#[derive(Debug, Clone, Copy)]
pub struct RawSubscribing;
/// A byte-reply subscriber (a `publish("out")` handler whose reply type is
/// [`Serialized`](crate::runtime::Serialized), on any input): the reply bytes go out as-is
/// through a bare publisher.
#[derive(Debug, Clone, Copy)]
pub struct RawReply;
/// A reply-publishing subscriber (`#[subscriber("in", publish("out"))]`).
#[derive(Debug, Clone, Copy)]
pub struct Publishing;
/// A subscriber whose startup injections need publisher attachments.
///
/// The signature carries `Out(out): Out<impl Publisher[, Marker]>` parameters, so the include
/// site chains `.out(marker, ..)` per slot (the implicit `DefaultSlot` for a single unnamed one).
#[derive(Debug, Clone, Copy)]
pub struct Out;
/// A reply-publishing subscriber whose handler also takes `Out` parameters, so the
/// include site chains `.out(marker, ..)` per slot next to the (optional)
/// `.out(Reply, ..)`.
#[derive(Debug, Clone, Copy)]
pub struct PublishingOut;
/// A byte-reply subscriber whose handler also takes an `Out` parameter.
#[derive(Debug, Clone, Copy)]
pub struct RawReplyOut;
/// A batch subscriber (a handler taking `&[T]`).
#[derive(Debug, Clone, Copy)]
pub struct Batch;
/// A self-deserializing batch subscriber (a handler taking a page of
/// [`Deserialized`](crate::runtime::Deserialized) elements): a batch with no decode step.
#[derive(Debug, Clone, Copy)]
pub struct RawBatch;
/// A batch reply-publishing subscriber (a `&[T]` handler with `publish("out")`).
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishing;
/// A batch subscriber whose startup injections need a publisher attachment (an `Out`
/// parameter).
#[derive(Debug, Clone, Copy)]
pub struct BatchOut;
/// A batch reply-publishing subscriber whose handler also takes `Out` parameters, so the
/// include site chains `.out(marker, ..)` per slot next to the (optional)
/// `.out(Reply, ..)`.
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishingOut;
