//! The contracts behind the byte-level `#[subscriber]` forms, mounted with
//! [`BrokerScope::include`](super::BrokerScope::include): the plain raw form
//! ([`RawSubscriberDef`]), the raw reply form ([`RawPublishingDef`], from
//! `raw, publish_raw("dest")`), and the typed-input byte-reply form ([`RawReplyDef`], from
//! `publish_raw("dest")` without `raw`).

use std::any::type_name;
use std::fmt;
use std::future::Future;

use serde::de::DeserializeOwned;
use tracing::warn;

use crate::codec::Codec;
use crate::{IncomingMessage, OutgoingMessage, Publisher};

use super::context::Context;
use super::dispatch::Workers;
use super::failure::{FailurePolicies, FailurePolicy};
use super::handler::{Handler, HandlerResult, Settle};
use super::metadata::HandlerMetadata;

/// A raw handler definition produced by the `#[subscriber(.., raw)]` macro form.
///
/// The raw counterpart of [`SubscriberDef`](super::SubscriberDef): it bundles a handler consuming
/// each delivery's payload bytes with the subscription [`Source`](Self::Source) it binds to. There
/// is no input type and no codec anywhere on the path - the handler is a
/// [`Handler`](super::Handler) over the broker's message type, reading the bytes with
/// [`IncomingMessage::payload`](crate::IncomingMessage::payload) - so a raw definition mounts
/// without any codec feature enabled and ignores the scope codec.
///
/// # Examples
///
/// A hand-written definition, as the macro would generate one:
///
/// ```
/// use ruststream::runtime::{Context, Handler, HandlerResult, RawSubscriberDef, Settle};
/// use ruststream::{IncomingMessage, Name};
///
/// struct Audit;
///
/// impl<M: IncomingMessage> Handler<M> for Audit {
///     async fn handle(&self, msg: &M, _ctx: &mut Context<'_>) -> Settle {
///         let _bytes: &[u8] = msg.payload();
///         HandlerResult::Ack.into()
///     }
/// }
///
/// struct AuditDef;
///
/// impl RawSubscriberDef for AuditDef {
///     type Context = ();
///     type Handler = Audit;
///     type Source = Name;
///
///     fn source(&self) -> Name {
///         Name::new("audit")
///     }
///
///     fn into_handler(self) -> Audit {
///         Audit
///     }
/// }
/// ```
pub trait RawSubscriberDef: Sized {
    /// The broker's typed per-delivery context the handler reads by key (`()` when the handler
    /// names no context type).
    type Context;

    /// The concrete handler type, running at the raw level (`Handler<M>` over the broker's
    /// message type). Like [`SubscriberDef::Handler`](super::SubscriberDef::Handler), the handler
    /// bound is enforced where the def is mounted, not on the trait.
    type Handler;

    /// The subscription source this handler binds to. The bound to
    /// [`SubscriptionSource`](crate::SubscriptionSource) for the target broker is applied where
    /// the def is mounted, not on the trait, so a def can name any broker's descriptor.
    type Source;

    /// Builds the subscription source (fresh each call).
    fn source(&self) -> Self::Source;

    /// An optional human description (from the handler's doc comment), for `AsyncAPI`.
    fn description(&self) -> Option<&str> {
        None
    }

    /// The concurrency policy for this subscriber's dispatch loop. The macro fills this in from
    /// the `workers(..)` argument; the default is sequential dispatch.
    fn workers(&self) -> Workers {
        Workers::sequential()
    }

    /// The failure policy for a handler panic. The macro fills this in from the
    /// `on_failure(panic = ..)` argument; the default fails fast. The decode policy never fires:
    /// a raw delivery has no decode step (the macro rejects `on_failure(decode = ..)`).
    fn failure_policies(&self) -> FailurePolicies {
        FailurePolicies::default()
    }

    /// Consumes the definition, returning the handler.
    fn into_handler(self) -> Self::Handler;
}

/// Builds the registration metadata for a raw definition mounted under `name`: the raw-bytes
/// input marker plus the doc-comment description; there is no input type to describe.
pub(crate) fn raw_metadata<D: RawSubscriberDef>(name: String, def: &D) -> HandlerMetadata {
    HandlerMetadata::raw(name).with_def_details(def.description(), None, None, None)
}

/// A raw reply-publishing definition produced by `#[subscriber(.., raw, publish_raw("dest"))]`.
///
/// The raw counterpart of [`PublishingDef`](super::PublishingDef): the handler consumes each
/// delivery's payload bytes and returns the reply bytes, published as-is to
/// [`reply_name`](Self::reply_name) through the bare live [`Publisher`] paired from the source
/// attached at the include site (`b.include(def).publisher(policy)`, or the broker's default
/// publish policy without the call). There is no codec on either side and no publish transform
/// stack: what the handler returns is what goes on the wire.
///
/// # Examples
///
/// A hand-written definition, as the macro would generate one:
///
/// ```
/// use ruststream::Name;
/// use ruststream::runtime::{Context, HandlerResult, RawPublishingCall, RawPublishingDef};
///
/// struct Mirror;
///
/// impl RawPublishingDef for Mirror {
///     type Reply = Vec<u8>;
///     type Context = ();
///     type Source = Name;
///
///     fn source(&self) -> Name {
///         Name::new("frames")
///     }
///
///     fn reply_name(&self) -> &str {
///         "frames-out"
///     }
/// }
///
/// impl<S: Send + Sync> RawPublishingCall<S> for Mirror {
///     async fn call(
///         &self,
///         payload: &[u8],
///         _ctx: &mut Context<'_, (), S>,
///     ) -> Result<Vec<u8>, HandlerResult> {
///         Ok(payload.to_vec())
///     }
/// }
/// ```
pub trait RawPublishingDef: Send + Sync {
    /// The reply type the handler produces, published byte-for-byte. `Vec<u8>` is the canonical
    /// form; any owned `AsRef<[u8]>` type (for example `bytes::Bytes`) mounts the same.
    type Reply;

    /// The broker's typed per-delivery context the handler reads by key, mirroring
    /// [`SubscriberDef::Context`](super::SubscriberDef::Context) (`()` when the handler names
    /// none).
    type Context;

    /// The subscription source this handler binds to (see
    /// [`RawSubscriberDef::Source`]).
    type Source;

    /// Builds the subscription source (fresh each call).
    fn source(&self) -> Self::Source;

    /// The name (subject / channel) the reply bytes are published to.
    fn reply_name(&self) -> &str;

    /// The concurrency policy for this subscriber's dispatch loop. The macro fills this in from
    /// the `workers(..)` argument; the default is sequential dispatch.
    fn workers(&self) -> Workers {
        Workers::sequential()
    }

    /// The failure policy for a handler panic. The macro fills this in from the
    /// `on_failure(panic = ..)` argument; the default fails fast. The decode policy never fires:
    /// a raw delivery has no decode step (the macro rejects `on_failure(decode = ..)`).
    fn failure_policies(&self) -> FailurePolicies {
        FailurePolicies::default()
    }

    /// An optional human description (from the handler's doc comment), for `AsyncAPI`.
    fn description(&self) -> Option<&str> {
        None
    }
}

/// Runs a [`RawPublishingDef`]'s handler body over an app state of type `S`.
///
/// The same state-generic split as [`PublishingCall`](super::PublishingCall): a handler that
/// ignores the app state is generic over `S` (mounts on any app), one that reads it implements
/// this only for its declared state.
pub trait RawPublishingCall<S>: RawPublishingDef {
    /// Runs the handler body.
    ///
    /// `Ok(reply)` is published as-is to [`reply_name`](RawPublishingDef::reply_name), then the
    /// incoming message is acked. `Err(result)` skips publishing and the dispatcher acts on the
    /// returned [`HandlerResult`] (for example [`HandlerResult::retry`] to ask for redelivery).
    fn call(
        &self,
        payload: &[u8],
        ctx: &mut Context<'_, Self::Context, S>,
    ) -> impl Future<Output = Result<Self::Reply, HandlerResult>> + Send;
}

/// Builds the registration metadata for a raw publishing definition mounted under `name`: raw
/// bytes on both sides, plus the doc-comment description.
pub(crate) fn raw_publishing_metadata<D: RawPublishingDef>(
    name: String,
    def: &D,
) -> HandlerMetadata {
    HandlerMetadata::raw(name)
        .with_output_type("bytes")
        .with_def_details(def.description(), None, None, None)
}

/// The [`Handler`] built from a [`RawPublishingDef`] once its source paired: run the body on the
/// payload bytes, publish the returned bytes, ack.
///
/// The reply goes through the bare live publisher `P` exactly as returned - no codec, no publish
/// transforms, no pipeline ([`OutHandler`](super::OutHandler) carries the same bare `P`). A
/// handler returning `Err(result)` skips the publish; a failed reply publish nacks the incoming
/// message with `requeue = true`, so the broker redelivers it instead of silently losing the
/// reply.
pub struct RawPublishingHandler<D, P> {
    pub(crate) def: D,
    pub(crate) publisher: P,
}

impl<D, P> fmt::Debug for RawPublishingHandler<D, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawPublishingHandler")
            .finish_non_exhaustive()
    }
}

impl<M, D, P, S> Handler<M, D::Context, S> for RawPublishingHandler<D, P>
where
    M: IncomingMessage,
    D: RawPublishingCall<S>,
    D::Reply: AsRef<[u8]> + Send + Sync,
    D::Context: Send + Sync,
    P: Publisher,
    S: Send + Sync,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, D::Context, S>) -> Settle {
        // The typed publishing path minus decode and encode: run the body on the payload bytes,
        // publish the reply bytes, ack. It settles by a bare outcome (no `and_after`).
        let reply = match self.def.call(msg.payload(), ctx).await {
            Ok(reply) => reply,
            Err(result) => return result.into(),
        };
        let name = self.def.reply_name();
        let outgoing = OutgoingMessage::new(name, reply.as_ref());
        if let Err(err) = self.publisher.publish(outgoing).await {
            warn!(
                target: "ruststream::dispatch",
                subscription = %ctx.name(),
                reply = %name,
                reply_type = type_name::<D::Reply>(),
                error = %err,
                "reply publish failed",
            );
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
    }
}

/// A typed-input, byte-reply definition produced by `#[subscriber("in", publish_raw("dest"))]`
/// (no `raw` on the input side).
///
/// The bridge form between the typed and the raw worlds: the incoming payload is decoded with
/// the scope codec exactly like a [`PublishingDef`](super::PublishingDef) handler's, and the
/// returned reply bytes are published as-is through the bare live [`Publisher`] paired at the
/// include site, exactly like a [`RawPublishingDef`] handler's - the shape of a protocol
/// gateway that consumes structured messages and emits a wire format it produced itself.
///
/// # Examples
///
/// A hand-written definition, as the macro would generate one:
///
/// ```
/// use ruststream::Name;
/// use ruststream::runtime::{Context, HandlerResult, RawReplyCall, RawReplyDef};
///
/// struct Encode;
///
/// impl RawReplyDef for Encode {
///     type Input = u64;
///     type Reply = Vec<u8>;
///     type Context = ();
///     type Source = Name;
///
///     fn source(&self) -> Name {
///         Name::new("ids")
///     }
///
///     fn reply_name(&self) -> &str {
///         "ids-wire"
///     }
/// }
///
/// impl<S: Send + Sync> RawReplyCall<S> for Encode {
///     async fn call(
///         &self,
///         input: &u64,
///         _ctx: &mut Context<'_, (), S>,
///     ) -> Result<Vec<u8>, HandlerResult> {
///         Ok(input.to_be_bytes().to_vec())
///     }
/// }
/// ```
pub trait RawReplyDef: Send + Sync {
    /// The decoded message type the handler consumes.
    type Input;

    /// The reply type the handler produces, published byte-for-byte (see
    /// [`RawPublishingDef::Reply`]).
    type Reply;

    /// The broker's typed per-delivery context the handler reads by key, mirroring
    /// [`SubscriberDef::Context`](super::SubscriberDef::Context).
    type Context;

    /// The subscription source this handler binds to (see [`RawSubscriberDef::Source`]).
    type Source;

    /// Builds the subscription source (fresh each call).
    fn source(&self) -> Self::Source;

    /// The name (subject / channel) the reply bytes are published to.
    fn reply_name(&self) -> &str;

    /// The concurrency policy for this subscriber's dispatch loop. The macro fills this in from
    /// the `workers(..)` argument; the default is sequential dispatch.
    fn workers(&self) -> Workers {
        Workers::sequential()
    }

    /// The failure policy for a handler panic and a decode failure (the input side is typed, so
    /// both apply, unlike the fully raw forms).
    fn failure_policies(&self) -> FailurePolicies {
        FailurePolicies::default()
    }

    /// An optional human description (from the handler's doc comment), for `AsyncAPI`.
    fn description(&self) -> Option<&str> {
        None
    }

    /// The input type's serialized JSON Schema, when it implements [`schemars::JsonSchema`] and
    /// the `asyncapi` feature is on. The macro fills this in; the default omits it.
    fn input_schema(&self) -> Option<String> {
        None
    }

    /// The input type's [`Message`](crate::Message) name, when it implements that trait.
    fn message_name(&self) -> Option<&'static str> {
        None
    }

    /// The input type's [`Message`](crate::Message) description, when it implements that trait.
    fn message_description(&self) -> Option<&'static str> {
        None
    }
}

/// Runs a [`RawReplyDef`]'s handler body over an app state of type `S` (the same state-generic
/// split as [`PublishingCall`](super::PublishingCall)).
pub trait RawReplyCall<S>: RawReplyDef {
    /// Runs the handler body.
    ///
    /// `Ok(reply)` is published as-is to [`reply_name`](RawReplyDef::reply_name), then the
    /// incoming message is acked. `Err(result)` skips publishing and the dispatcher acts on the
    /// returned [`HandlerResult`].
    fn call(
        &self,
        input: &Self::Input,
        ctx: &mut Context<'_, Self::Context, S>,
    ) -> impl Future<Output = Result<Self::Reply, HandlerResult>> + Send;
}

/// Builds the registration metadata for a typed-input, byte-reply definition mounted under
/// `name`: a typed input (schema and message metadata when available) and raw bytes out.
pub(crate) fn raw_reply_metadata<D: RawReplyDef>(name: String, def: &D) -> HandlerMetadata {
    HandlerMetadata::typed::<D::Input>(name)
        .with_output_type("bytes")
        .with_def_details(
            def.description(),
            def.input_schema(),
            def.message_name(),
            def.message_description(),
        )
}

/// The [`Handler`] built from a [`RawReplyDef`] once its source paired: decode the input with
/// the scope codec, run the body, publish the returned bytes, ack.
///
/// The consume side is the typed publishing path (decode with `C`, the decode failure policy
/// applies); the reply side is the raw publishing path (the bare live `P`, no codec, no
/// transforms). A failed reply publish nacks with `requeue = true`, like both parents.
pub struct RawReplyHandler<D, C, P> {
    pub(crate) def: D,
    pub(crate) codec: C,
    pub(crate) publisher: P,
    pub(crate) decode: FailurePolicy,
}

impl<D, C, P> fmt::Debug for RawReplyHandler<D, C, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawReplyHandler").finish_non_exhaustive()
    }
}

impl<M, D, C, P, S> Handler<M, D::Context, S> for RawReplyHandler<D, C, P>
where
    M: IncomingMessage,
    D: RawReplyCall<S>,
    D::Input: DeserializeOwned + Send + Sync,
    D::Reply: AsRef<[u8]> + Send + Sync,
    D::Context: Send + Sync,
    C: Codec,
    P: Publisher,
    S: Send + Sync,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, D::Context, S>) -> Settle {
        let input = match self.codec.decode::<D::Input>(msg.payload()) {
            Ok(value) => value,
            Err(err) => {
                warn!(
                    target: "ruststream::dispatch",
                    subscription = %ctx.name(),
                    message_type = type_name::<D::Input>(),
                    error = %err,
                    "codec decode failed",
                );
                #[cfg(any(feature = "testing", feature = "otel"))]
                ctx.mark_decode_failed();
                return match self.decode {
                    FailurePolicy::FailFast => {
                        ctx.fail_fast(&format!("decode failed: {err}"));
                        HandlerResult::drop().into()
                    }
                    other => other
                        .settlement()
                        .unwrap_or_else(HandlerResult::drop)
                        .into(),
                };
            }
        };
        let reply = match self.def.call(&input, ctx).await {
            Ok(reply) => reply,
            Err(result) => return result.into(),
        };
        let name = self.def.reply_name();
        let outgoing = OutgoingMessage::new(name, reply.as_ref());
        if let Err(err) = self.publisher.publish(outgoing).await {
            warn!(
                target: "ruststream::dispatch",
                subscription = %ctx.name(),
                reply = %name,
                reply_type = type_name::<D::Reply>(),
                error = %err,
                "reply publish failed",
            );
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
    }
}

#[cfg(test)]
mod tests {
    use super::{RawPublishingDef, RawSubscriberDef, raw_metadata, raw_publishing_metadata};
    use crate::Name;
    use crate::runtime::dispatch::Workers;
    use crate::runtime::failure::FailurePolicies;

    /// A def overriding nothing: pins the trait's default contract, which the macro-generated
    /// defs always override.
    struct ManualDef;

    impl RawSubscriberDef for ManualDef {
        type Context = ();
        type Handler = ();
        type Source = Name;

        fn source(&self) -> Name {
            Name::new("frames")
        }

        fn into_handler(self) {}
    }

    #[test]
    fn defaults_omit_metadata_and_dispatch_sequentially() {
        let def = ManualDef;
        assert_eq!(def.workers(), Workers::sequential());
        assert_eq!(def.failure_policies(), FailurePolicies::default());
        assert!(def.description().is_none());

        let meta = raw_metadata("frames".to_owned(), &def);
        assert_eq!(meta.name, "frames");
        assert_eq!(meta.input_type, "bytes");
        assert!(meta.payload_schema.is_none());
        def.into_handler();
    }

    /// A publishing def overriding nothing optional: pins the trait's default contract, which
    /// the macro-generated defs always override.
    struct ManualPublishingDef;

    impl RawPublishingDef for ManualPublishingDef {
        type Reply = Vec<u8>;
        type Context = ();
        type Source = Name;

        fn source(&self) -> Name {
            Name::new("frames")
        }

        // The trait signature returns `&str` (tied to `&self`); the macro-generated impls do the
        // same, so this hand-written one cannot narrow to `&'static str` without diverging.
        #[allow(clippy::unnecessary_literal_bound)]
        fn reply_name(&self) -> &str {
            "frames-out"
        }
    }

    #[test]
    fn publishing_defaults_mark_bytes_on_both_sides() {
        let def = ManualPublishingDef;
        assert_eq!(def.workers(), Workers::sequential());
        assert_eq!(def.failure_policies(), FailurePolicies::default());
        assert!(def.description().is_none());
        assert_eq!(def.reply_name(), "frames-out");
        let _source = def.source();

        let meta = raw_publishing_metadata("frames".to_owned(), &def);
        assert_eq!(meta.name, "frames");
        assert_eq!(meta.input_type, "bytes");
        assert_eq!(meta.output_type, Some("bytes"));
        assert!(meta.payload_schema.is_none());
    }
}
