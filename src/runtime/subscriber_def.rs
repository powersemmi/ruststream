//! The contract a `#[subscriber]`-generated type implements so it can be mounted with
//! [`BrokerScope::include`](super::BrokerScope::include).

use super::handler::Handler;

/// A handler definition produced by the `#[subscriber]` macro.
///
/// It bundles a typed [`Handler`] with the name it binds to, so
/// [`BrokerScope::include`](super::BrokerScope::include) can subscribe and wire decoding without
/// the caller repeating the name or message type. The generated type is itself the handler, so
/// [`Handler`](Self::Handler) is usually `Self`.
pub trait SubscriberDef: Sized {
    /// The decoded message type the handler consumes.
    type Input;

    /// The concrete handler type over [`Input`](Self::Input).
    type Handler: Handler<Self::Input>;

    /// The name (subject / channel) this handler subscribes to.
    fn name(&self) -> &str;

    /// An optional human description (from the handler's doc comment), for `AsyncAPI`.
    fn description(&self) -> Option<&str> {
        None
    }

    /// The input type's serialized JSON Schema, when it implements [`schemars::JsonSchema`] and the
    /// `asyncapi` feature is on. The macro fills this in; the default omits it.
    fn input_schema(&self) -> Option<String> {
        None
    }

    /// Consumes the definition, returning the handler.
    fn into_handler(self) -> Self::Handler;
}
