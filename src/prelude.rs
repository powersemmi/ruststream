//! The imports a service writes every time, in one glob.
//!
//! `use ruststream::prelude::*;` brings in the application object and its builder, the handler
//! surface on both paths - the attribute's (with the `macros` feature) and the manual path's
//! one body trait ([`Handle`]) with its one constructor
//! ([`subscriber`](crate::runtime::subscriber)) - the settlement type, the per-delivery
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
//! contracts), the mount-site publish vocabulary ([`Reply`] and [`DefaultSlot`], the two markers
//! `.out(marker, policy)` binds, and [`MapPublisher`] for a broker's own publisher settings), the
//! slot vocabulary ([`OutSlot`], [`PublishedThrough`], [`OutMessages`] with the
//! [`OutgoingMessageMetadata`] a dictionary reports, and [`OutEntry`], the bound a manual body
//! declares its arena with), the state projection ([`FromRef`]) and the extractor binding
//! ([`FromContext`]).
//!
//! Brokers, codecs and the optional feature modules (`asyncapi`, `metrics`, `logging`, `otel`,
//! `testing`) stay explicit imports.
//!
//! # Examples
//!
//! ```
//! use ruststream::prelude::*;
//!
//! async fn handle(order: &str, ctx: &mut Context<'_>) -> HandlerOutcome {
//!     let _ = (order.len(), ctx.name());
//!     HandlerOutcome::ack()
//! }
//! ```

pub use crate::runtime::{
    App, AppInfo, Context, Ctx, DefaultSlot, Deserialized, FailurePolicies, FailurePolicy,
    FromContext, FromRef, Handle, HandlerOutcome, Headers, Input, MapPublisher, Message,
    MessageWire, Out, OutEntry, OutMessages, OutgoingMessageMetadata, Outs, PublishExt,
    PublishedThrough, Reply, ReplyShape, Router, RouterDef, RunningApp, RustStream, Serialized,
    SerializedReply, SerializedWire, Slot, SoloDeserialized, State, SubscriberSettings, Workers,
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
pub use crate::{
    Broker, CallerName, FixedName, HeaderMap, IncomingMessage, MessageHeaders, MessageInfo, Name,
    NameTemplate, NoHeaders, OutSlot, OutgoingDestination, OwnedTransactions, PublishPolicy,
    Publisher, RequestReply, TransactionalPublisher, Unnamed, WithHeaders,
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
pub use crate::{Deserialized, FromRef, OutMessages, Outgoing, Serialized, app, subscriber};
