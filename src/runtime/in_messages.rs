//! A declared set of message models one subscription receives: the receiving counterpart of
//! [`OutMessages`](crate::runtime::OutMessages).
//!
//! `#[derive(InMessages)]` on an enum whose variants each wrap one model makes the enum a
//! handler input. Each variant decodes through its model's own path (the codec of the mount for
//! a `serde` model, [`Deserialized`] for one that reads its own bytes), the set picks the variant
//! by its rule, and the generated document lists one message per variant.

use std::any::type_name;
use std::fmt;
use std::hash::Hash;

use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::codec::{Codec, CodecError};
use crate::runtime::handle::{Deserialized, Input, Solo, SoloDeserialized};
use crate::runtime::metadata::IncomingMessageMetadata;
use crate::{IncomingMessage, MessageInfo};

/// A declared set of message models a handler receives on one subscription, as
/// `#[derive(InMessages)]` implements it on an enum whose variants each wrap one model.
///
/// The enum is the handler's input: the set decodes one delivery into the variant it picks, and
/// the generated document lists every variant's model as a message of the subscription, with
/// that model's own name, description, headers and content type. The variant is picked left to
/// right by default, by the function `#[in_messages(discriminate = path)]` names, or by the
/// header `#[in_messages(header = "..")]` names with one `#[in_messages(value = "..")]` per
/// variant.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// /// An order was placed.
/// #[derive(Deserialize, MessageInfo)]
/// struct OrderCreated {
///     id: u64,
/// }
///
/// /// An order was cancelled, sent as Protobuf by another service.
/// #[derive(Clone, PartialEq, prost::Message, Deserialized, MessageInfo)]
/// #[wire(prost)]
/// struct OrderCancelled {
///     #[prost(uint64, tag = "1")]
///     id: u64,
/// }
///
/// #[derive(InMessages)]
/// #[in_messages(header = "event")]
/// enum OrderEvent {
///     #[in_messages(value = "created")]
///     Created(OrderCreated),
///     #[in_messages(value = "cancelled")]
///     Cancelled(OrderCancelled),
/// }
///
/// #[subscriber("orders")]
/// async fn on_event(event: &OrderEvent) -> HandlerOutcome {
///     match event {
///         OrderEvent::Created(created) => tracing::info!(created.id, "created"),
///         OrderEvent::Cancelled(cancelled) => tracing::info!(cancelled.id, "cancelled"),
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(on_event);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
///
/// A set decodes by its own rule, so it is never a `serde` type as well:
///
/// ```compile_fail
/// use ruststream::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, MessageInfo)]
/// struct OrderCreated {
///     id: u64,
/// }
///
/// #[derive(Deserialize, InMessages)]
/// enum OrderEvent {
///     Created(OrderCreated),
/// }
/// ```
///
/// # Implementing by hand
///
/// Without the derive a set is two impls: this trait, with the rule written out and one
/// [`IncomingMessageMetadata`] per model, and the [`Input`] spelling that puts the enum on the
/// set lane. Each model decodes through [`InVariant::decode_variant`] and describes itself
/// through [`InVariant::describe`], named as the variant of this set: `<Model as
/// InVariant<Set>>`.
///
/// ```
/// use ruststream::codec::{Codec, CodecError};
/// use ruststream::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct OrderCreated {
///     id: u64,
/// }
///
/// impl MessageInfo for OrderCreated {
///     const NAME: &'static str = "OrderCreated";
///     const DESCRIPTION: Option<&'static str> = Some("An order was placed.");
/// }
///
/// #[derive(Deserialize)]
/// struct OrderCancelled {
///     id: u64,
/// }
///
/// impl MessageInfo for OrderCancelled {
///     const NAME: &'static str = "OrderCancelled";
///     const DESCRIPTION: Option<&'static str> = Some("An order was cancelled.");
/// }
///
/// enum OrderEvent {
///     Created(OrderCreated),
///     Cancelled(OrderCancelled),
/// }
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// enum OrderEventKind {
///     Created,
///     Cancelled,
/// }
///
/// /// The `event` header names the variant; the rule is the one `#[in_messages(header =
/// /// "event")]` writes.
/// impl InMessages for OrderEvent {
///     type Kind = OrderEventKind;
///
///     fn decode<DecodeCodec, M>(codec: &DecodeCodec, msg: &M) -> Result<Self, CodecError>
///     where
///         DecodeCodec: Codec,
///         M: IncomingMessage,
///     {
///         let kind = match msg.headers().get("event") {
///             Some(b"created") => OrderEventKind::Created,
///             Some(b"cancelled") => OrderEventKind::Cancelled,
///             Some(other) => {
///                 return Err(CodecError::Decode(
///                     format!("no variant for event `{}`", String::from_utf8_lossy(other)).into(),
///                 ));
///             }
///             None => return Err(CodecError::Decode("no `event` header".into())),
///         };
///         let payload = msg.payload();
///         match kind {
///             OrderEventKind::Created => {
///                 <OrderCreated as InVariant<Self>>::decode_variant(codec, payload)
///                     .map(OrderEvent::Created)
///             }
///             OrderEventKind::Cancelled => {
///                 <OrderCancelled as InVariant<Self>>::decode_variant(codec, payload)
///                     .map(OrderEvent::Cancelled)
///             }
///         }
///     }
///
///     fn incoming() -> Vec<IncomingMessageMetadata> {
///         vec![
///             <OrderCreated as InVariant<Self>>::describe()
///                 .with_discriminator(HeaderDiscriminator::new("event", "created")),
///             <OrderCancelled as InVariant<Self>>::describe()
///                 .with_discriminator(HeaderDiscriminator::new("event", "cancelled")),
///         ]
///     }
/// }
///
/// impl Input for OrderEvent {
///     type Axis = SoloSet<OrderEvent>;
/// }
/// ```
pub trait InMessages: Sized + Send + Sync + 'static {
    /// The kinds of the set: one unit variant per model, what a discriminator names.
    type Kind: Copy + Eq + Hash + fmt::Debug + Send + Sync + 'static;

    /// Decodes one delivery into the variant the set's rule picks.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when no variant decodes the delivery, when the rule picks none, or
    /// when the variant it picks does not decode. The subscriber's decode failure policy settles
    /// the delivery.
    fn decode<DecodeCodec, M>(codec: &DecodeCodec, msg: &M) -> Result<Self, CodecError>
    where
        DecodeCodec: Codec,
        M: IncomingMessage;

    /// The models of the set, in the order of the variants, as the generated document lists
    /// them.
    fn incoming() -> Vec<IncomingMessageMetadata>;
}

/// One model of an [`InMessages`] set: what every variant's type must be.
///
/// A model derives [`MessageInfo`] (the document needs its name) and decodes through its own
/// path: the codec of the mount for a `serde::Deserialize` type, [`Deserialized`] for a type that
/// constructs itself from the payload and owns what it constructs (a `prost` message). The
/// bound is checked where the set is declared, so a variant that fits neither path is an error
/// on that variant.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a variant of the message set `{Set}`",
    note = "a variant wraps one message model: a type deriving `MessageInfo` that decodes through \
            the codec of the mount (`serde::Deserialize`) or constructs itself from the payload \
            bytes (`Deserialized`, owning what it constructs)"
)]
pub trait InVariant<Set>: Sized + Send + Sync + 'static {
    /// True when the model reads its own bytes ([`Deserialized`]): no codec decodes it, so its
    /// message in the document carries no media type and no schema by design.
    const DESERIALIZED: bool;

    /// Decodes one payload through the model's own path.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the payload does not decode as this model.
    fn decode_variant<DecodeCodec: Codec>(
        codec: &DecodeCodec,
        payload: &[u8],
    ) -> Result<Self, CodecError>;

    /// What the document lists for this model: its [`MessageInfo`] name and description, and
    /// its lane. The set adds the model's schemas and, under the header rule, the value that
    /// names it.
    #[must_use]
    fn describe() -> IncomingMessageMetadata;
}

// The lane the model's own `Input` impl put it on is the machinery of the check, not the
// mistake: the trait's own message names the model and the set, so the impl stays out of the
// error.
#[diagnostic::do_not_recommend]
impl<Set, Model> InVariant<Set> for Model
where
    Model: MessageInfo + Input + Sized + Send + Sync + 'static,
    Model::Axis: VariantLane<Model>,
{
    const DESERIALIZED: bool = <Model::Axis as VariantLane<Model>>::DESERIALIZED;

    fn decode_variant<DecodeCodec: Codec>(
        codec: &DecodeCodec,
        payload: &[u8],
    ) -> Result<Self, CodecError> {
        <Model::Axis as VariantLane<Model>>::decode(codec, payload)
    }

    fn describe() -> IncomingMessageMetadata {
        IncomingMessageMetadata::new(type_name::<Model>())
            .with_message_name(Some(Model::NAME))
            .with_message_description(Model::DESCRIPTION)
            .with_deserialized(<Model::Axis as VariantLane<Model>>::DESERIALIZED)
    }
}

/// The decode of one lane into an owned model, for the variants of a set. Machinery behind
/// [`InVariant`]; never named in user code.
#[doc(hidden)]
pub trait VariantLane<Model> {
    /// See [`InVariant::DESERIALIZED`].
    const DESERIALIZED: bool;

    /// Decodes one payload into the model.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the payload does not decode.
    fn decode<DecodeCodec: Codec>(codec: &DecodeCodec, payload: &[u8])
    -> Result<Model, CodecError>;
}

impl<T: DeserializeOwned> VariantLane<T> for Solo<T> {
    const DESERIALIZED: bool = false;

    fn decode<DecodeCodec: Codec>(codec: &DecodeCodec, payload: &[u8]) -> Result<T, CodecError> {
        codec.decode(payload)
    }
}

// Only an owning construction fits a variant: the enum holds the model past the delivery.
impl<F> VariantLane<F> for SoloDeserialized<F>
where
    F: for<'p> Deserialized<Output<'p> = F>,
{
    const DESERIALIZED: bool = true;

    fn decode<DecodeCodec: Codec>(_codec: &DecodeCodec, payload: &[u8]) -> Result<F, CodecError> {
        F::from_payload(payload).map_err(|err| CodecError::Decode(err.to_string().into()))
    }
}

/// What a typed input declares to the generated document: one message ([`MessageInfo`]) or a
/// set of them ([`InMessages`]).
///
/// A type is one or the other. `#[derive(InMessages)]` implements this for the set, and every
/// `MessageInfo` type has it already, so deriving both on one enum is rejected as two impls of
/// this trait for the same type.
pub trait MessageDeclaration {}

impl<T: MessageInfo> MessageDeclaration for T {}

/// The failure of a set whose every variant failed to decode: each variant's own error, in the
/// order they were tried.
#[derive(Debug, Error)]
#[error(
    "no variant of the message set `{set}` decodes the delivery: {}",
    list(failures)
)]
struct EveryVariantFailed {
    set: &'static str,
    failures: Vec<(&'static str, CodecError)>,
}

fn list(failures: &[(&'static str, CodecError)]) -> String {
    failures
        .iter()
        .map(|(variant, err)| format!("`{variant}`: {err}"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Why a discriminated set did not decode: the rule picked no variant, or a header it reads is
/// missing or names no variant.
#[derive(Debug, Error)]
enum NoVariant {
    #[error("the discriminator of the message set `{set}` chose no variant for the delivery")]
    Chosen { set: &'static str },
    #[error(
        "the message set `{set}` reads its variant from the `{header}` header, and the delivery \
         carries no `{header}` header"
    )]
    HeaderMissing {
        set: &'static str,
        header: &'static str,
    },
    #[error(
        "the `{header}` header is `{seen}`, which names no variant of the message set `{set}` \
         (the values are {})",
        known.iter().map(|value| format!("`{value}`")).collect::<Vec<_>>().join(", ")
    )]
    HeaderUnknown {
        set: &'static str,
        header: &'static str,
        seen: String,
        known: &'static [&'static str],
    },
}

/// The decode error of a left-to-right set whose every variant failed. Machinery behind the
/// derive.
#[doc(hidden)]
#[must_use]
pub fn every_variant_failed(
    set: &'static str,
    failures: impl IntoIterator<Item = (&'static str, CodecError)>,
) -> CodecError {
    CodecError::Decode(Box::new(EveryVariantFailed {
        set,
        failures: failures.into_iter().collect(),
    }))
}

/// The decode error of a discriminated set whose function chose no variant. Machinery behind
/// the derive.
#[doc(hidden)]
#[must_use]
pub fn no_variant_chosen(set: &'static str) -> CodecError {
    CodecError::Decode(Box::new(NoVariant::Chosen { set }))
}

/// The decode error of a header-discriminated set whose header the delivery lacks. Machinery
/// behind the derive.
#[doc(hidden)]
#[must_use]
pub fn header_missing(set: &'static str, header: &'static str) -> CodecError {
    CodecError::Decode(Box::new(NoVariant::HeaderMissing { set, header }))
}

/// The decode error of a header-discriminated set whose header carries a value no variant
/// declares. Machinery behind the derive.
#[doc(hidden)]
#[must_use]
pub fn header_unknown(
    set: &'static str,
    header: &'static str,
    seen: &[u8],
    known: &'static [&'static str],
) -> CodecError {
    CodecError::Decode(Box::new(NoVariant::HeaderUnknown {
        set,
        header,
        seen: String::from_utf8_lossy(seen).into_owned(),
        known,
    }))
}

#[cfg(test)]
mod tests {
    use super::{every_variant_failed, header_missing, header_unknown, no_variant_chosen};
    use crate::codec::CodecError;

    fn decode_error(reason: &str) -> CodecError {
        CodecError::Decode(reason.to_owned().into())
    }

    #[test]
    fn every_variant_failed_lists_each_variant_in_order() {
        let err = every_variant_failed(
            "OrderEvent",
            [
                ("Created", decode_error("missing field `id`")),
                ("Cancelled", decode_error("invalid wire type")),
            ],
        );
        assert_eq!(
            err.to_string(),
            "decode failed: no variant of the message set `OrderEvent` decodes the delivery: \
             `Created`: decode failed: missing field `id`; `Cancelled`: decode failed: invalid \
             wire type"
        );
    }

    #[test]
    fn a_discriminator_that_chose_nothing_names_the_set() {
        assert_eq!(
            no_variant_chosen("OrderEvent").to_string(),
            "decode failed: the discriminator of the message set `OrderEvent` chose no variant \
             for the delivery"
        );
    }

    #[test]
    fn a_missing_header_names_the_header_and_the_set() {
        assert_eq!(
            header_missing("OrderEvent", "event").to_string(),
            "decode failed: the message set `OrderEvent` reads its variant from the `event` \
             header, and the delivery carries no `event` header"
        );
    }

    #[test]
    fn an_unknown_value_names_the_value_seen_and_the_known_ones() {
        let err = header_unknown(
            "OrderEvent",
            "event",
            b"returned",
            &["created", "cancelled"],
        );
        assert_eq!(
            err.to_string(),
            "decode failed: the `event` header is `returned`, which names no variant of the \
             message set `OrderEvent` (the values are `created`, `cancelled`)"
        );
    }

    #[test]
    fn a_value_that_is_not_text_is_rendered_lossily() {
        let err = header_unknown("OrderEvent", "event", &[0xff, b'a'], &["created"]);
        assert!(err.to_string().contains("is `\u{fffd}a`"), "{err}");
    }
}
