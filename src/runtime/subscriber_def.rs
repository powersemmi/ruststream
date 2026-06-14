//! The contract a `#[subscriber]`-generated type implements so it can be mounted with
//! [`BrokerScope::include`](super::BrokerScope::include).

use super::dispatch::Workers;
use super::handler::Handler;
use super::metadata::HandlerMetadata;

/// A handler definition produced by the `#[subscriber]` macro.
///
/// It bundles a typed [`Handler`] with the subscription [`Source`](Self::Source) it binds to, so
/// [`BrokerScope::include`](super::BrokerScope::include) can subscribe and wire decoding without the
/// caller repeating anything. The source is a [`Name`](crate::Name) for `#[subscriber("topic")]`, or
/// a broker descriptor for `#[subscriber(RedisStream::new(..))]`. The generated type is itself the
/// handler, so [`Handler`](Self::Handler) is usually `Self`.
pub trait SubscriberDef: Sized {
    /// The decoded message type the handler consumes.
    type Input;

    /// The concrete handler type over [`Input`](Self::Input).
    type Handler: Handler<Self::Input>;

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

    /// Consumes the definition, returning the handler.
    fn into_handler(self) -> Self::Handler;
}

/// Builds the registration metadata for a subscriber definition mounted under `name`.
pub(crate) fn subscriber_metadata<D: SubscriberDef>(name: String, def: &D) -> HandlerMetadata {
    let mut meta = HandlerMetadata::typed::<D::Input>(name);
    if let Some(description) = def.description() {
        meta = meta.with_description(description.to_owned());
    }
    if let Some(schema) = def.input_schema() {
        meta = meta.with_payload_schema(schema);
    }
    if let Some(message_name) = def.message_name() {
        meta = meta.with_message_name(message_name);
    }
    if let Some(message_description) = def.message_description() {
        meta = meta.with_message_description(message_description);
    }
    meta
}

#[cfg(test)]
mod tests {
    use super::{SubscriberDef, subscriber_metadata};
    use crate::Headers;
    use crate::Name;
    use crate::runtime::context::{Context, State};
    use crate::runtime::dispatch::{Delivery, Workers};
    use crate::runtime::handler::{Handler, HandlerResult, Settle};

    struct Noop;

    impl Handler<u32> for Noop {
        async fn handle(&self, _msg: &u32, _ctx: &mut Context<'_>) -> Settle {
            HandlerResult::Ack.into()
        }
    }

    /// A hand-written def overriding nothing: pins the trait's default contract, which the
    /// macro-generated defs always override.
    struct ManualDef;

    impl SubscriberDef for ManualDef {
        type Input = u32;
        type Handler = Noop;
        type Source = Name;

        fn source(&self) -> Name {
            Name::new("manual")
        }

        fn into_handler(self) -> Noop {
            Noop
        }
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
        let state = State::default();
        let delivery = Delivery::empty();
        let headers = Headers::new();
        let mut ctx = Context::new("manual", &headers, &state, &delivery);
        assert_eq!(
            handler.handle(&7u32, &mut ctx).await.outcome(),
            HandlerResult::Ack
        );
    }
}
