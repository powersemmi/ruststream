//! The imports a service writes every time, in one glob.
//!
//! `use ruststream::prelude::*;` brings in the application object and its builder, the handler
//! surface on both paths - the attribute's (with the `macros` feature) and the manual path's
//! one body trait ([`Handle`]) with its one constructor
//! ([`subscriber`](crate::runtime::subscriber)) - the settlement type, the per-delivery
//! context, the extractor parameters, the subscriber settings a mount site fills in, and the
//! publishing types a handler reaches for. What a handler publishes it publishes through the
//! builder, so the outgoing message type is not here; see the note on the re-export below.
//! Brokers, codecs and the optional feature modules (`asyncapi`, `metrics`, `logging`, `otel`,
//! `testing`) stay explicit imports.
//!
//! # Examples
//!
//! ```
//! use ruststream::prelude::*;
//!
//! async fn handle(order: &[u8], ctx: &mut Context<'_>) -> HandlerOutcome {
//!     let _ = (order.len(), ctx.name());
//!     HandlerOutcome::ack()
//! }
//! ```

pub use crate::runtime::{
    App, AppInfo, Bare, Context, Ctx, DefaultSlot, FailurePolicies, FailurePolicy, FromRef, Handle,
    HandlerOutcome, Headers, Message, Out, Outs, Payload, Publish, PublishExt, Router, RouterDef,
    RunningApp, RustStream, Slot, State, SubscriberSettings, TypedPublisher, Workers, subscriber,
};
// `OutgoingMessage` is absent: a service on this crate publishes through the builder, which
// assembles the message itself. What still needs one - a publish transform, a middleware, or a
// broker crate used on its own without this one - names it explicitly.
pub use crate::{
    Broker, HeaderMap, IncomingMessage, MessageInfo, Name, OutSlot, PublishPolicy, Publisher,
    Unnamed,
};
// The counting macro every `workers(..)` chain writes.
pub use crate::nonzero;

// The derives sharing a name with their trait (`MessageInfo`, `OutSlot`, `FromRef`) come in with
// the re-exports above. `Outgoing` is the exception: the derive lives at the crate root while
// the publish pipeline's message type of the same name lives in `runtime`, so a service that
// writes a publish transform imports that one explicitly.
#[cfg(feature = "macros")]
pub use crate::{FromRef, Outgoing, app, subscriber};
