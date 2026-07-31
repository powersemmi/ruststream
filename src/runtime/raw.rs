//! The contract behind the plain byte-level `#[subscriber(.., raw)]` form, mounted with
//! [`BrokerScope::include`](super::BrokerScope::include). The byte-reply forms ride the
//! publishing machinery: `publish_raw(..)` is a [`PublishingDef`](super::PublishingDef) whose
//! reply leaves through a bare-publisher [`ReplySink`](super::ReplySink) wiring.

use super::dispatch::Workers;
use super::failure::FailurePolicies;
use super::metadata::HandlerMetadata;

/// A raw handler definition produced by the `#[subscriber(.., raw)]` macro form.
///
/// The raw counterpart of [`SubscriberDef`](super::SubscriberDef): it bundles a handler consuming
/// each delivery's payload bytes with the subscription [`Source`](Self::Source) it binds to. There
/// is no input type and no codec anywhere on the path - the handler is a
/// [`Handler`](super::Handler) over the broker's message type, reading the bytes with
/// [`IncomingMessage::payload`](crate::IncomingMessage::payload) - so a raw definition mounts
/// without any codec feature enabled and ignores the scope codec.
///
/// # Examples
///
/// A hand-written definition, as the macro would generate one:
///
/// ```
/// use ruststream::runtime::{Context, Handler, HandlerResult, RawSubscriberDef, Settle};
/// use ruststream::{IncomingMessage, Name};
///
/// struct Audit;
///
/// impl<M: IncomingMessage> Handler<M> for Audit {
///     async fn handle(&self, msg: &M, _ctx: &mut Context<'_>) -> Settle {
///         let _bytes: &[u8] = msg.payload();
///         HandlerResult::Ack.into()
///     }
/// }
///
/// struct AuditDef;
///
/// impl RawSubscriberDef for AuditDef {
///     type Context = ();
///     type Handler = Audit;
///     type Source = Name;
///
///     fn source(&self) -> Name {
///         Name::new("audit")
///     }
///
///     fn into_handler(self) -> Audit {
///         Audit
///     }
/// }
/// ```
pub trait RawSubscriberDef: Sized {
    /// The broker's typed per-delivery context the handler reads by key (`()` when the handler
    /// names no context type).
    type Context;

    /// The concrete handler type, running at the raw level (`Handler<M>` over the broker's
    /// message type). Like [`SubscriberDef::Handler`](super::SubscriberDef::Handler), the handler
    /// bound is enforced where the def is mounted, not on the trait.
    type Handler;

    /// The subscription source this handler binds to. The bound to
    /// [`SubscriptionSource`](crate::SubscriptionSource) for the target broker is applied where
    /// the def is mounted, not on the trait, so a def can name any broker's descriptor.
    type Source;

    /// Builds the subscription source (fresh each call).
    fn source(&self) -> Self::Source;

    /// An optional human description (from the handler's doc comment), for `AsyncAPI`.
    fn description(&self) -> Option<&str> {
        None
    }

    /// The concurrency policy for this subscriber's dispatch loop. The macro fills this in from
    /// the `workers(..)` argument; the default is sequential dispatch.
    fn workers(&self) -> Workers {
        Workers::sequential()
    }

    /// The failure policy for a handler panic. The macro fills this in from the
    /// `on_failure(panic = ..)` argument; the default fails fast. The decode policy never fires:
    /// a raw delivery has no decode step (the macro rejects `on_failure(decode = ..)`).
    fn failure_policies(&self) -> FailurePolicies {
        FailurePolicies::default()
    }

    /// Consumes the definition, returning the handler.
    fn into_handler(self) -> Self::Handler;
}

/// Builds the registration metadata for a raw definition mounted under `name`: the raw-bytes
/// input marker plus the doc-comment description; there is no input type to describe.
pub(crate) fn raw_metadata<D: RawSubscriberDef>(name: String, def: &D) -> HandlerMetadata {
    HandlerMetadata::raw(name).with_def_details(def.description(), None, None, None)
}

#[cfg(test)]
mod tests {
    use super::{RawSubscriberDef, raw_metadata};
    use crate::Name;
    use crate::runtime::dispatch::Workers;
    use crate::runtime::failure::FailurePolicies;

    /// A def overriding nothing: pins the trait's default contract, which the macro-generated
    /// defs always override.
    struct ManualDef;

    impl RawSubscriberDef for ManualDef {
        type Context = ();
        type Handler = ();
        type Source = Name;

        fn source(&self) -> Name {
            Name::new("frames")
        }

        fn into_handler(self) {}
    }

    #[test]
    fn defaults_omit_metadata_and_dispatch_sequentially() {
        let def = ManualDef;
        assert_eq!(def.workers(), Workers::sequential());
        assert_eq!(def.failure_policies(), FailurePolicies::default());
        assert!(def.description().is_none());

        let meta = raw_metadata("frames".to_owned(), &def);
        assert_eq!(meta.name, "frames");
        assert_eq!(meta.input_type, "bytes");
        assert!(meta.payload_schema.is_none());
        def.into_handler();
    }
}
