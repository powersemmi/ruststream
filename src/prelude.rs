//! The imports a service writes every time, in one glob.
//!
//! `use ruststream::prelude::*;` brings in the application object and its builder, the handler
//! surface (the settlement enum, the per-delivery context, the extractor parameters), the
//! subscriber settings a mount site fills in, the
//! publishing types a handler reaches for, and - with the `macros` feature - the attribute
//! macros and derives. What a handler publishes it publishes through the builder, so the
//! outgoing message type is not here; see the note on the re-export below.
//! It deliberately stops there: brokers, codecs and the optional feature
//! modules (`asyncapi`, `metrics`, `logging`, `otel`, `testing`) stay explicit imports, because
//! which broker and which codec a service runs on is the one thing every service states for
//! itself.
//!
//! # Examples
//!
//! ```
//! use ruststream::prelude::*;
//!
//! async fn handle(order: &[u8], ctx: &mut Context<'_>) -> HandlerResult {
//!     let _ = (order.len(), ctx.name());
//!     HandlerResult::Ack
//! }
//! ```

pub use crate::runtime::{
    App, AppInfo, Context, Ctx, FromHeaders, FromRef, HandlerResult, Out, PublishExt, Router,
    RunningApp, RustStream, Seek, State, SubscriberSettings, TypedPublisher,
};
// `OutgoingMessage` is deliberately absent: a service on this crate publishes through the
// builder, which assembles the message itself. What still needs one - a publish transform, a
// middleware, or a broker crate used on its own without this one - names it explicitly, and
// says by that import which layer it is working at.
pub use crate::{
    Broker, Headers, IncomingMessage, Message, Name, OutSlot, PublishPolicy, Publisher,
};

// The attribute macros and the derives share their names with the traits they implement (the
// `Message` trait and its derive, `OutSlot`, `FromRef`), which is why the re-exports above
// already carry the derive where the trait lives at the crate root. `Outgoing` is the exception:
// the derive lives at the root while the publish pipeline's message type of the same name lives
// in `runtime`, so a service that writes a publish transform imports that one explicitly.
#[cfg(feature = "macros")]
pub use crate::{FromRef, Outgoing, app, subscriber};
