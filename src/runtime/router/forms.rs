//! Form tokens for [`IncludeDef`](super::IncludeDef): which mounting machinery a definition uses.
//!
//! A definition names its form once, and both registration surfaces (a
//! [`Router`](super::Router) chain and a [`BrokerScope`](crate::runtime::BrokerScope)) dispatch
//! on that token, so `include(def)` reads the same wherever it is written.

/// A plain subscriber (`#[subscriber("in")]`).
#[derive(Debug, Clone, Copy)]
pub struct Subscribing;
/// A raw-bytes subscriber (a `#[subscriber]` handler taking `&[u8]`): no decode, no codec.
#[derive(Debug, Clone, Copy)]
pub struct RawSubscribing;
/// A byte-reply subscriber (`#[subscriber("in", publish_raw("out"))]`, on a byte or a typed
/// input): the reply bytes go out as-is through a bare publisher.
#[derive(Debug, Clone, Copy)]
pub struct RawReply;
/// A reply-publishing subscriber (`#[subscriber("in", publish("out"))]`).
#[derive(Debug, Clone, Copy)]
pub struct Publishing;
/// A subscriber whose startup injections need publisher attachments.
///
/// The signature carries `Out(out): Out<impl Publisher[, Marker]>` parameters (optionally
/// next to a `Seek` one), so the include site chains `.publisher(..)` (single slot) or
/// `.out(marker, ..)` per named slot.
#[derive(Debug, Clone, Copy)]
pub struct Out;
/// A subscriber whose startup injections need nothing from the include site.
///
/// The signature carries a `Seek(seeker): Seek<K>` parameter (and no `Out`).
#[derive(Debug, Clone, Copy)]
pub struct Seek;
/// A reply-publishing subscriber whose handler also takes `Out` parameters, so the
/// include site chains `.out(marker, ..)` per slot next to the (optional)
/// `.publisher(..)`.
#[derive(Debug, Clone, Copy)]
pub struct PublishingOut;
/// A byte-reply subscriber whose handler also takes an `Out` parameter.
#[derive(Debug, Clone, Copy)]
pub struct RawReplyOut;
/// A batch subscriber (a handler taking `&[T]`).
#[derive(Debug, Clone, Copy)]
pub struct Batch;
/// A raw batch subscriber (a handler taking `&[Payload<'_>]`): a batch with no decode step.
#[derive(Debug, Clone, Copy)]
pub struct RawBatch;
/// A batch reply-publishing subscriber (a `&[T]` handler with `publish("out")`).
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishing;
/// A batch subscriber whose startup injections need a publisher attachment (an `Out`
/// parameter, optionally next to a `Seek` one).
#[derive(Debug, Clone, Copy)]
pub struct BatchOut;
/// A batch subscriber whose startup injections need nothing from the include site (a
/// `Seek` parameter and no `Out`).
#[derive(Debug, Clone, Copy)]
pub struct BatchSeek;
/// A batch reply-publishing subscriber whose handler also takes `Out` parameters, so the
/// include site chains `.out(marker, ..)` per slot next to the (optional)
/// `.publisher(..)`.
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishingOut;
