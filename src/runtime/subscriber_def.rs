//! The contract a `#[subscriber]`-generated type implements so it can be mounted with
//! [`BrokerScope::include`](super::BrokerScope::include).

use super::dispatch::Workers;
use super::failure::FailurePolicies;
// Kept in scope for the `[`Handler`]` intra-doc links; the trait no longer bounds `type Handler`.
#[allow(unused_imports)]
use super::handler::Handler;
use super::input::InputKind;
use super::metadata::HandlerMetadata;

/// A handler definition produced by the `#[subscriber]` macro (with or without `raw`).
///
/// It bundles a [`Handler`] with the subscription [`Source`](Self::Source) it binds to, so
/// [`BrokerScope::include`](super::BrokerScope::include) can subscribe and wire decoding without the
/// caller repeating anything. The source is a [`Name`](crate::Name) for `#[subscriber("topic")]`, or
/// a broker descriptor for `#[subscriber(RedisStream::new(..))]`. The generated type is itself the
/// handler, so [`Handler`](Self::Handler) is usually `Self`.
pub trait SubscriberDef: Sized {
    /// The input kind the handler consumes ([`Decoded<T>`](super::Decoded) for a typed `&T`
    /// parameter, [`RawBytes`](super::RawBytes) for a raw `&[u8]` one).
    type Input: InputKind;

    /// The broker's typed per-delivery context the handler reads by key (`()` when the handler
    /// names no context type).
    type Context;

    /// The concrete handler type over the input's borrowed target (`Handler<T>` for a
    /// `Decoded<T>` input, `Handler<[u8]>` for a raw one) and [`Context`](Self::Context).
    ///
    /// The handler bound is enforced where the def is mounted (against the app's state type `St`),
    /// not on the trait: a handler that reads typed application state is
    /// [`Handler<Target, Context, St>`](Handler) only for its declared `St`, while one that
    /// ignores state is generic over it.
    type Handler;

    /// The subscription source this handler binds to. The bound to
    /// [`SubscriptionSource`](crate::SubscriptionSource) for the target broker is applied where the
    /// def is mounted, not on the trait, so a def can name any broker's descriptor.
    type Source;

    /// Builds the subscription source (fresh each call).
    fn source(&self) -> Self::Source;

    /// An optional human description (from the handler's doc comment), for `AsyncAPI`.
    fn description(&self) -> Option<&str> {
        None
    }

    /// The input type's serialized JSON Schema, when it implements [`schemars::JsonSchema`] and the
    /// `asyncapi` feature is on. The macro fills this in; the default omits it.
    fn input_schema(&self) -> Option<String> {
        None
    }

    /// The serialized JSON Schema of the handler's typed header contract (its
    /// [`FromHeaders<T>`](super::FromHeaders) parameter), when `T` implements
    /// [`schemars::JsonSchema`] and the `asyncapi` feature is on. The macro fills this in; the
    /// default omits it.
    fn headers_schema(&self) -> Option<String> {
        None
    }

    /// The input type's [`Message`](crate::Message) name, when it implements that trait. The macro
    /// fills this in; the default omits it.
    fn message_name(&self) -> Option<&'static str> {
        None
    }

    /// The input type's [`Message`](crate::Message) description, when it implements that trait.
    /// The macro fills this in; the default omits it.
    fn message_description(&self) -> Option<&'static str> {
        None
    }

    /// The concurrency policy for this subscriber's dispatch loop. The macro fills this in from
    /// the `workers(..)` argument; the default is sequential dispatch.
    fn workers(&self) -> Workers {
        Workers::sequential()
    }

    /// The failure policy for a handler panic and a decode failure. The macro fills this in from
    /// the `on_failure(panic = .., decode = ..)` argument; the default fails fast on a panic and
    /// drops on a decode failure.
    fn failure_policies(&self) -> FailurePolicies {
        FailurePolicies::default()
    }

    /// Consumes the definition, returning the handler.
    fn into_handler(self) -> Self::Handler;
}

/// Builds the registration metadata for a subscriber definition mounted under `name`.
pub(crate) fn subscriber_metadata<D: SubscriberDef>(name: String, def: &D) -> HandlerMetadata {
    let mut meta = HandlerMetadata::raw(name).with_def_details(
        def.description(),
        def.input_schema(),
        def.headers_schema(),
        def.message_name(),
        def.message_description(),
    );
    meta.input_type = <D::Input as InputKind>::input_label();
    meta
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use super::{SubscriberDef, subscriber_metadata};
    use crate::Headers;
    use crate::Name;
    use crate::runtime::context::Context;
    use crate::runtime::dispatch::{Delivery, Workers};
    use crate::runtime::handler::{Handler, HandlerResult, Settle};
    use crate::runtime::input::{Decoded, RawBytes};

    struct Noop;

    impl Handler<u32> for Noop {
        fn handle(&self, _msg: &u32, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> {
            ready(HandlerResult::Ack.into())
        }
    }

    /// A hand-written def overriding nothing: pins the trait's default contract, which the
    /// macro-generated defs always override.
    struct ManualDef;

    impl SubscriberDef for ManualDef {
        type Input = Decoded<u32>;
        type Context = ();
        type Handler = Noop;
        type Source = Name;

        fn source(&self) -> Name {
            Name::new("manual")
        }

        fn into_handler(self) -> Noop {
            Noop
        }
    }

    /// A raw def: the same trait with the byte input kind; pins the "bytes" metadata label.
    struct ManualRawDef;

    impl SubscriberDef for ManualRawDef {
        type Input = RawBytes;
        type Context = ();
        type Handler = ();
        type Source = Name;

        fn source(&self) -> Name {
            Name::new("frames")
        }

        fn into_handler(self) {}
    }

    #[test]
    fn a_raw_def_reports_the_bytes_label() {
        let def = ManualRawDef;
        let meta = subscriber_metadata("frames".to_owned(), &def);
        assert_eq!(meta.name, "frames");
        assert_eq!(meta.input_type, "bytes");
        assert!(meta.payload_schema.is_none());
        def.into_handler();
    }

    #[tokio::test]
    async fn defaults_omit_metadata_and_dispatch_sequentially() {
        let def = ManualDef;
        assert_eq!(def.workers(), Workers::sequential());
        assert!(def.description().is_none());
        assert!(def.input_schema().is_none());
        assert!(def.message_name().is_none());
        assert!(def.message_description().is_none());
        let _source = def.source();

        let meta = subscriber_metadata("manual".to_owned(), &def);
        assert_eq!(meta.name, "manual");
        assert_eq!(meta.input_type, "u32");

        // Drive the consumed handler so source(), into_handler() and the Noop body are exercised.
        let handler = def.into_handler();
        let state = ();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("manual", &headers, &state, (), &delivery);
        assert_eq!(
            handler.handle(&7u32, &mut ctx).await.outcome(),
            HandlerResult::Ack
        );
    }
}
