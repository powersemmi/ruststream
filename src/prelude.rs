//! The imports a service writes every time, in one glob.
//!
//! `use ruststream::prelude::*;` brings in the application object and its builder, the handler
//! surface on both paths - the attribute's (with the `macros` feature) and the value
//! constructors with their body traits - the settlement types, the per-delivery context, the
//! extractor parameters, the subscriber settings a mount site fills in, and the publishing
//! types a handler reaches for. What a handler publishes it publishes through the builder, so
//! the outgoing message type is not here; see the note on the re-export below.
//! Brokers, codecs and the optional feature modules (`asyncapi`, `metrics`, `logging`, `otel`,
//! `testing`) stay explicit imports.
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
    App, AppInfo, BatchReply, BatchResult, Context, Ctx, DefaultSlot, FailurePolicies,
    FailurePolicy, FromRef, Handler, HandlerResult, Headers, Out, PublishExt, RawSliceHandler,
    Reply, Router, RouterDef, RunningApp, RustStream, Seek, Settle, SliceHandler,
    SliceHandlerWithHeaders, SlotsBatchReply, SlotsHandler, SlotsReply, SlotsSliceHandler, State,
    SubscriberSettings, TypedPublisher, Workers, batch, batch_in, batch_replying,
    batch_replying_in, batch_replying_with_slots, batch_with_headers, batch_with_headers_in,
    batch_with_seek, batch_with_slots, raw, raw_batch, raw_batch_in, raw_in, raw_replying,
    raw_replying_in, raw_replying_with_slots, replying, replying_in, replying_with_slots,
    subscriber, subscriber_in, with_seek, with_slots,
};
// `OutgoingMessage` is absent: a service on this crate publishes through the builder, which
// assembles the message itself. What still needs one - a publish transform, a middleware, or a
// broker crate used on its own without this one - names it explicitly.
pub use crate::{
    Broker, HeaderMap, IncomingMessage, Message, Name, OutSlot, PublishPolicy, Publisher, Unnamed,
};
// The counting macro every `workers(..)` chain writes.
pub use crate::nonzero;

// The derives sharing a name with their trait (`Message`, `OutSlot`, `FromRef`) come in with the
// re-exports above. `Outgoing` is the exception: the derive lives at the crate root while the
// publish pipeline's message type of the same name lives in `runtime`, so a service that writes a
// publish transform imports that one explicitly.
#[cfg(feature = "macros")]
pub use crate::{FromRef, Outgoing, app, subscriber};
