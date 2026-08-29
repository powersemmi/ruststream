//! Value-level definitions: the manual counterparts of `#[subscriber]`, built from values.
//!
//! One constructor per mount form takes the subscription source and the handler body and
//! returns a definition mounted through `include`, the same entry point the attribute path
//! uses. The registration metadata is derived from the stored source and the handler's input
//! type, so it cannot drift from the mount; decoding follows the surface's codec ladder (the
//! [`DefaultCodec`](crate::codec::DefaultCodec) unless [`with_codec`](super::Router::with_codec)
//! or [`codec`](super::SubscriberBuilder::codec) names one); the declarative settings chain on
//! the same [`SubscriberBuilder`](super::SubscriberBuilder) the attribute drives.
//!
//! Every attribute form has a constructor:
//!
//! | Attribute form | Constructor | Body trait |
//! |---|---|---|
//! | `#[subscriber("in")]` | [`subscriber`] | [`Handler<T>`](super::Handler) |
//! | `#[subscriber("in", raw)]` | [`raw`] | [`Handler<[u8]>`](super::Handler) |
//! | `#[subscriber(batch("in"))]` | [`batch`] | [`SliceHandler<T>`](super::SliceHandler) |
//! | `#[subscriber(batch("in"), raw)]` | [`raw_batch`] | [`RawSliceHandler`](super::RawSliceHandler) |
//! | batch + `Headers<Vec<H>>` | [`batch_with_headers`] | [`SliceHandlerWithHeaders`](super::SliceHandlerWithHeaders) |
//! | `publish("out")` | [`replying`] | [`Reply<T>`] (or `Reply<[u8]>` for a raw input) |
//! | `publish_raw("out")` | [`raw_replying`] | [`Reply<T>`] with a byte-shaped `Out` |
//! | batch + `publish("out")` | [`batch_replying`] | [`BatchReply<T>`] |
//! | `Out(..)` parameters | [`with_slots`] | [`SlotsHandler`] |
//! | `Seek(..)` parameter | [`with_seek`] | [`SlotsHandler`] over `(Seek<K>,)` |
//! | batch + `Out(..)` | [`batch_with_slots`] | [`SlotsSliceHandler`] |
//! | batch + `Seek(..)` | [`batch_with_seek`] | [`SlotsSliceHandler`] over `(Seek<K>,)` |
//! | `publish(..)` + `Out(..)` | [`replying_with_slots`] | [`SlotsReply`] |
//! | `publish_raw(..)` + `Out(..)` | [`raw_replying_with_slots`] | [`SlotsReply`] with a byte `Out` |
//! | batch + `publish(..)` + `Out(..)` | [`batch_replying_with_slots`] | [`SlotsBatchReply`] |
//!
//! A body implemented for one concrete app state type takes the constructor's `_in` variant
//! (`subscriber_in`, `batch_in`, ...); the slot and seek forms need none, their bodies being
//! checked at the mount. A body reading a typed broker context is inferred on the plain forms
//! and named with `.context::<K>()` on the slot and seek forms. `AsyncAPI` schemas opt in with
//! [`documented`](super::SubscriberBuilder::documented) /
//! [`documented_headers`](super::SubscriberBuilder::documented_headers) (probing cannot happen
//! inside a generic constructor body), the input type's [`Message`](crate::Message) metadata
//! with [`message`](super::SubscriberBuilder::message), and the send operation's reply side
//! with [`reply_message`](super::SubscriberBuilder::reply_message) /
//! [`reply_headers`](super::SubscriberBuilder::reply_headers) on the reply constructors.
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "json"))]
//! # mod demo {
//! use std::future::{Future, ready};
//!
//! use ruststream::memory::MemoryBroker;
//! use ruststream::prelude::*;
//! # #[derive(serde::Deserialize)]
//! # struct Order { id: u64 }
//!
//! struct Handle;
//!
//! impl Handler<Order> for Handle {
//!     fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
//!         println!("got order {}", order.id);
//!         ready(HandlerResult::ack().into())
//!     }
//! }
//!
//! fn app() -> RustStream {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
//!         b.include(subscriber("orders", Handle));
//!     })
//! }
//! # }
//! ```

mod batch_replying;
mod batch_slots;
mod replying;
mod replying_slots;
mod seek;
mod slots;
mod subscribing;

pub use batch_replying::{BatchReply, BatchReplyingValue, batch_replying, batch_replying_in};
pub use batch_slots::{
    BatchSlotsValue, BoundBatchSlots, SlotsBatchReply, SlotsSliceHandler,
    batch_replying_with_slots, batch_with_slots,
};
pub use replying::{
    DeclaredName, Reply, ReplyingBuilder, ReplyingValue, To, raw_replying, raw_replying_in,
    replying, replying_in,
};
pub use replying_slots::{
    BoundReplyingSlots, ReplyingSlotsValue, SlotsReply, raw_replying_with_slots,
    replying_with_slots,
};
pub use seek::{BatchSeekValue, SeekValue, batch_with_seek, with_seek};
pub use slots::{BoundSlotsValue, SlotsHandler, SlotsValue, with_slots};
pub use subscribing::{
    BatchValue, BatchWithHeadersValue, RawBatchValue, RawValue, SubscriberValue, batch, batch_in,
    batch_with_headers, batch_with_headers_in, raw, raw_batch, raw_batch_in, raw_in, subscriber,
    subscriber_in,
};
#[doc(hidden)]
pub use subscribing::{Docs, DocumentedValue};

use std::borrow::Cow;

use crate::runtime::settings::{AllOpen, SubscriberBuilder};
use crate::{Name, Unnamed};

/// What a value constructor returns: the settings builder over its definition, mounted on the
/// converted source with every setting open.
pub type ValueBuilder<Def, Src> = SubscriberBuilder<Def, <Src as IntoSource>::Source, AllOpen>;

/// The input kind a handler body's borrowed parameter selects: a sized `T` decodes
/// ([`Decoded<T>`](crate::runtime::Decoded)), `[u8]` takes the payload raw
/// ([`RawBytes`](crate::runtime::RawBytes)).
///
/// This is how the reply constructors follow the body instead of taking a separate spelling
/// for a raw input.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a handler input",
    note = "a body parameter is `&T` for a decoded payload (with T: DeserializeOwned) or `&[u8]` \
            for a raw one"
)]
pub trait HandledInput {
    /// The input kind the mount drives; its target is this type, which is what the body
    /// borrows.
    type Kind: crate::runtime::InputKind<Target = Self>;
}

impl<T: serde::de::DeserializeOwned + Send + Sync + 'static> HandledInput for T {
    type Kind = crate::runtime::Decoded<T>;
}

impl HandledInput for [u8] {
    type Kind = crate::runtime::RawBytes;
}

/// What a value-definition constructor accepts as its subscription source.
///
/// A subject string builds the broker-agnostic by-name source ([`Name`]); a source value passes
/// through unchanged, so a broker's own descriptor mounts the same way. Broker crates implement
/// this for their descriptors next to their
/// [`SubscriptionSource`](crate::SubscriptionSource) impls.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not name a subscription source",
    note = "pass a subject string (`subscriber(\"orders\", ..)`), a `Name`, or a broker's own \
            source descriptor"
)]
pub trait IntoSource {
    /// The source the constructor stores.
    type Source;

    /// Builds the source.
    fn into_source(self) -> Self::Source;
}

impl IntoSource for &'static str {
    type Source = Name;

    fn into_source(self) -> Name {
        Name::new(self)
    }
}

impl IntoSource for String {
    type Source = Name;

    fn into_source(self) -> Name {
        Name::new(self)
    }
}

impl IntoSource for Cow<'static, str> {
    type Source = Name;

    fn into_source(self) -> Name {
        Name::new(self)
    }
}

impl IntoSource for Name {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

// The deferred-name flow: constructing over `Unnamed<S>` keeps the mount uncompilable until
// `.name(..)` builds the source, exactly as for an unnamed attribute definition.
impl<S> IntoSource for Unnamed<S> {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

#[cfg(feature = "memory")]
impl IntoSource for crate::memory::MemorySource {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// Captures the serialized JSON Schema of `T` for the `documented` opt-in: taken as a function
/// pointer where the `JsonSchema` bound is proven, called once at registration.
#[cfg(feature = "asyncapi")]
pub(crate) fn schema_json_of<T: schemars::JsonSchema + ?Sized>() -> Option<String> {
    serde_json::to_string(&schemars::schema_for!(T)).ok()
}

#[cfg(all(test, feature = "memory", feature = "json"))]
mod parity_tests;

#[cfg(all(test, feature = "memory", feature = "json"))]
mod tests {
    use std::any::type_name;
    use std::future::{Future, ready};

    use serde::{Deserialize, Serialize};

    use super::{batch, raw, raw_batch, replying, subscriber, with_slots};
    use crate::memory::{MemoryBroker, MemoryPublish, MemorySource};
    use crate::runtime::{
        BatchResult, Context, Handler, HandlerResult, OutSlot, RawSliceHandler, Reply, Router,
        RouterDef, Settle, SliceHandler, SlotsHandler, SubscriberSettings,
    };
    use crate::{Publisher, nonzero};

    #[derive(Debug, Deserialize, Serialize)]
    struct Order {
        id: u64,
    }

    #[derive(Debug, Serialize)]
    struct Confirmation {
        id: u64,
    }

    struct Handle;

    impl Handler<Order> for Handle {
        fn handle(
            &self,
            _order: &Order,
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Settle> + Send {
            ready(HandlerResult::ack().into())
        }
    }

    struct SettlePage;

    impl SliceHandler<Order> for SettlePage {
        fn handle_slice(
            &self,
            _orders: &[Order],
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = BatchResult> + Send {
            ready(BatchResult::Uniform(HandlerResult::ack()))
        }
    }

    struct Inspect;

    impl Handler<[u8]> for Inspect {
        fn handle(
            &self,
            _payload: &[u8],
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Settle> + Send {
            ready(HandlerResult::ack().into())
        }
    }

    struct Ingest;

    impl RawSliceHandler for Ingest {
        fn handle_slice(
            &self,
            _frames: &[&[u8]],
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = BatchResult> + Send {
            ready(BatchResult::Uniform(HandlerResult::ack()))
        }
    }

    struct Confirm;

    impl Reply<Order> for Confirm {
        type Out = Confirmation;

        fn reply(
            &self,
            order: &Order,
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
            ready(Ok(Confirmation { id: order.id }))
        }
    }

    struct Audit;

    impl OutSlot for Audit {
        const NAME: &'static str = "Audit";
    }

    struct Mirror;

    impl<P, E, S> SlotsHandler<Order, (crate::runtime::Out<P, Audit, (), E>,), (), S> for Mirror
    where
        P: Publisher,
        E: Send + Sync,
        S: Send + Sync,
    {
        fn handle(
            &self,
            _order: &Order,
            _slots: &(crate::runtime::Out<P, Audit, (), E>,),
            _ctx: &mut Context<'_, (), S>,
        ) -> impl Future<Output = Settle> + Send {
            ready(HandlerResult::ack().into())
        }
    }

    /// Every constructor mounts through `include` on both surfaces; the registration metadata
    /// comes from the stored source and the input type, not from the caller.
    fn all_forms() -> impl RouterDef<MemoryBroker> + crate::runtime::RouterHandlers {
        Router::<MemoryBroker>::new()
            .include(subscriber("orders", Handle).workers(nonzero!(4)))
            .include(subscriber(MemorySource::new("orders"), Handle))
            .include(batch("orders", SettlePage))
            .include(raw("frames", Inspect))
            .include(raw_batch("frames", Ingest))
            .include(replying("orders", Confirm).to("confirmations"))
            .publisher(crate::runtime::TypedPublisher::new(MemoryPublish))
            .include(with_slots::<Order, (Audit,), _, _>("mirror", Mirror))
            .out(Audit, MemoryPublish)
            .mount()
    }

    #[test]
    fn metadata_derives_from_the_source_and_the_input_type() {
        let mut handlers = Vec::new();
        crate::runtime::RouterHandlers::collect_handlers(&all_forms(), &mut handlers);
        assert_eq!(handlers.len(), 7);
        assert!(
            handlers
                .iter()
                .any(|meta| { meta.name == "orders" && meta.input_type == type_name::<Order>() })
        );
        assert!(
            handlers
                .iter()
                .any(|meta| meta.name == "frames" && meta.input_type == "bytes")
        );
        let reply = handlers
            .iter()
            .find(|meta| meta.output_type.is_some())
            .expect("the replying registration reports its reply type");
        assert_eq!(reply.output_type, Some(type_name::<Confirmation>()));
        assert!(
            reply
                .outgoing
                .iter()
                .any(|out| out.channel == "confirmations")
        );
        let slots = handlers
            .iter()
            .find(|meta| meta.name == "mirror")
            .expect("the slot registration is mounted under its source name");
        assert_eq!(slots.input_type, type_name::<Order>());
    }

    #[tokio::test]
    async fn a_value_subscriber_dispatches_end_to_end() {
        use crate::OutgoingMessage;
        use crate::memory::MemoryBroker;
        use crate::runtime::{AppInfo, RustStream};

        let app = RustStream::new(AppInfo::new("value-defs", "0.0.0")).with_broker(
            MemoryBroker::new(),
            |b| {
                b.include(subscriber("orders", Handle).describe("Inbound orders"));
                b.after_startup(MemoryPublish, async move |publisher| {
                    publisher
                        .publish(OutgoingMessage::new("orders", br#"{"id":7}"#.as_slice()))
                        .await
                });
            },
        );
        let running = app.start().await.expect("the app starts");
        running.shutdown().await.expect("the app stops cleanly");
    }
}
