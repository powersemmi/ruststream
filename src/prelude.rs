//! The imports a service writes every time, in one glob.
//!
//! `use ruststream::prelude::*;` brings in the application object and its builder, the handler
//! surface (the settlement enum, the per-delivery context, the extractor parameters), the
//! subscriber settings a mount site fills in, the
//! publishing types a handler reaches for, and - with the `macros` feature - the attribute
//! macros and derives. It deliberately stops there: brokers, codecs and the optional feature
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
    App, AppInfo, Context, Ctx, FromHeaders, FromRef, HandlerResult, Out, Router, RunningApp,
    RustStream, Seek, State, SubscriberSettings, TypedPublisher,
};
pub use crate::{
    Broker, Headers, IncomingMessage, Message, Name, OutSlot, OutgoingMessage, PublishPolicy,
    Publisher,
};

// The attribute macros and the derives share their names with the traits they implement (the
// `Message` trait and its derive, `OutSlot`, `FromRef`), which is why the re-exports above
// already carry the derive where the trait lives at the crate root.
#[cfg(feature = "macros")]
pub use crate::{FromRef, app, subscriber};
