//! The imports a service writes every time, in one glob.
//!
//! `use ruststream::prelude::*;` brings in the application object and its builder, the handler
//! surface on both paths - the attribute's (with the `macros` feature) and the manual path's
//! one body trait ([`Handle`]) with its one constructor
//! ([`subscriber`](fn@crate::runtime::subscriber)) - the settlement type, the per-delivery
//! context, the extractor parameters, the subscriber settings a mount site fills in, and the
//! publishing types a handler reaches for. What a handler publishes it publishes through the
//! builder, so the outgoing message type is not here; see the note on the re-export below.
//!
//! The manual path is a first-class citizen of the glob: every impl a derive would have written
//! is spelled with names from here, so a service without the `macros` feature imports nothing
//! else. That covers the two lanes (`Deserialized` with its [`Input`] spelling over
//! [`SoloDeserialized`]; `Serialized` with [`MessageWire`] over [`SerializedWire`] for a typed
//! publish and [`ReplyShape`] over [`SerializedReply`] for the reply position), the outgoing
//! declaration ([`OutgoingDestination`] with its three forms, [`MessageHeaders`] with its two
//! contracts), the mount-site publish vocabulary ([`Reply`], [`Retry`] and [`DefaultSlot`], the
//! markers `.out(marker, policy)` binds, and [`MapPublisher`] for a broker's own publisher
//! settings), the
//! slot vocabulary ([`OutSlot`], [`PublishedThrough`], [`OutMessages`] with the
//! [`OutgoingMessageMetadata`] a dictionary reports, and [`OutEntry`], the bound a manual body
//! declares its arena with), the message-set vocabulary ([`InMessages`] over [`SoloSet`], with
//! [`InVariant`], [`IncomingMessageMetadata`] and [`HeaderDiscriminator`] a hand-written set
//! names), the state projection ([`FromRef`]) and the extractor binding ([`FromContext`]).
//!
//! Brokers, codecs and the optional feature modules (`asyncapi`, `metrics`, `logging`, `otel`,
//! `testing`) stay explicit imports.
//!
//! # Examples
//!
//! ```
//! # #[cfg(all(feature = "macros", feature = "json"))]
//! # mod demo {
//! use ruststream::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[subscriber("orders")]
//! async fn handle(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
//!     tracing::info!(order.id, subscription = ctx.name(), "handled");
//!     HandlerOutcome::ack()
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! # A broker crate's prelude
//!
//! A broker crate ships a `prelude` module of its own, because its types are named at the mount
//! site. It has three layers, in this order:
//!
//! 1. `pub use ruststream::prelude::*;`, so one glob serves the whole file;
//! 2. the crate's own surface a service names: the broker, its subscription source, its `Config`,
//!    its error, the [`ContextField`](crate::ContextField) keys a body reads, and the extension
//!    traits that carry its settings (mount-site steps, publish-builder steps);
//! 3. its publish policies under the names every broker uses, `Publish` and, where it has them,
//!    `TransactionalPublish` and `Request` (`pub use crate::KafkaTransactionalPublish as
//!    TransactionalPublish;`), plus the consumer-side capability traits a body reaches through the
//!    broker ([`Positioned`](crate::Positioned), [`Seeker`](crate::Seeker),
//!    [`Transaction`](crate::Transaction)) as a manifest.
//!
//! The core prelude exports nothing under the three policy names, so a mount site reads the same
//! on every broker and swapping brokers swaps the glob. The four publisher capabilities are
//! already here. A broker never aliases a policy to a core trait name ([`Publisher`],
//! [`TransactionalPublisher`],
//! [`OwnedTransactions`], [`RequestReply`]): an
//! explicit re-export shadows a glob silently, so the trait would vanish from every service
//! writing the glob, and the error would surface in the service's file. Two traits stay out of
//! the manifest: [`Partitioned`](crate::Partitioned), whose method collides with the defaulted
//! [`IncomingMessage::partition_key`], and
//! [`BatchSubscriber`](crate::BatchSubscriber), which the framework calls and no body names.
//!
//! The split of imports is what keeps a service portable. A handler body imports this prelude
//! alone and bounds its slot with a core capability (`Out<impl Publisher>`), so it says what it
//! needs of a publisher and never which broker provides it. A routes file imports the broker's
//! prelude, because mounting is where a broker is named. The one exception is a body that adjusts
//! a per-message setting: it imports the broker's prelude for the step and names the broker's
//! options type in its bound (`Out<impl Publisher<Options = MqttOptions>, Telemetry>`).
//!
//! [`ruststream::memory::prelude`](crate::memory::prelude) is the worked example. A probe behind
//! the crate's own glob pins both halves: a capability bound still resolves to the core trait, and
//! the mount-site name is still the crate's policy. Here it is against the in-memory broker's
//! prelude:
//!
//! ```
//! # #[cfg(feature = "memory")]
//! # mod demo {
//! use ruststream::memory::prelude::*;
//!
//! // A bound a body states: the core trait, not something of the broker's.
//! fn _bound<T: Publisher>() {}
//!
//! // A mount-site name: the broker's policy, constructible with no connection in sight.
//! fn _policy() {
//!     let _: Publish = Publish::default();
//! }
//! # }
//! # fn main() {}
//! ```

pub use crate::runtime::{
    App, AppInfo, Context, ContextKind, Ctx, DefaultSlot, Deserialized, FailurePolicies,
    FailurePolicy, ForReply, ForSlot, FromContext, FromRef, Handle, HandlerOutcome,
    HeaderDiscriminator, Headers, InMessages, InVariant, IncomingMessageMetadata, Input,
    MainRuntime, MapPublisher, Message, MessageWire, Names, Out, OutEntry, OutMessages,
    OutgoingMessageMetadata, Outs, PublishExt, PublishTransform, PublishedThrough, Reads, Reply,
    ReplyShape, Retry, Router, RouterDef, RunningApp, RustStream, Serialized, SerializedReply,
    SerializedWire, Slot, SoloDeserialized, SoloSet, State, SubscriberSettings, WireBytes, Workers,
    subscriber,
};
// `OutgoingMessage` is absent: a service on this crate publishes through the builder, which
// assembles the message itself. What still needs one - a publish transform, a middleware, or a
// broker crate used on its own without this one - names it explicitly.
//
// The publisher capability traits are here because a handler body states one of them on its
// injected slot (`Out<impl TransactionalPublisher>`) without knowing which broker runs it; the
// consumer-side capabilities (`Partitioned`, `Seekable`, `Positioned`, `Seeker`) are not, since
// a service reaches those through a broker whose prelude names the ones it implements.
// `BytesMut` is here for one reason: a hand-written `Serialized` impl names it in the signature
// the trait demands, and the manual path is meant to need nothing but this glob. `Str` is here
// for the same kind of reason: it is what a header key and a destination name are made of, and
// `Str::from_static` is how a constant one is written without paying for a copy.
pub use crate::{
    Broker, BytesMut, CallerName, FixedName, HeaderMap, IncomingMessage, MessageHeaders,
    MessageInfo, Name, NameTemplate, NoHeaders, OutSlot, OutgoingDestination, OwnedTransactions,
    PublishPolicy, Publisher, RequestReply, Str, TransactionalPublisher, Unnamed, WithHeaders,
};
// The counting macro every `workers(..)` chain writes.
pub use crate::nonzero;

// The derives sharing a name with their trait come in two ways. `MessageInfo` and `OutSlot`
// carry both halves at the crate root, so the re-exports above bring the derive along. The
// others keep the trait in `runtime` and only the derive at the root, so the derive is imported
// here on its own - never the trait, which would collide with the `runtime` import above.
// `Outgoing` is the exception: the derive lives at the crate root while the publish pipeline's
// message type of the same name lives in `runtime`, so a service that writes a publish
// transform imports that one explicitly.
#[cfg(feature = "macros")]
pub use crate::{
    Deserialized, FromRef, InMessages, OutMessages, Outgoing, Serialized, app, subscriber,
};
