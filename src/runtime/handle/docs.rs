//! Documented-by-default: the typestate and the schema capture behind `.undocumented()`.
//!
//! Under the `asyncapi` feature every registration reports its message schemas unless the chain
//! opts out with [`undocumented`](crate::runtime::SubscriberBuilder::undocumented). The
//! `JsonSchema` obligation is checked where the definition mounts, and only for definitions
//! still in the documented state, so the opt-out is a compile-time exit rather than a runtime
//! switch. Without the feature the state still exists but demands nothing and produces nothing.

use std::borrow::Cow;

use crate::runtime::metadata::OutgoingMessageMetadata;

/// The documentation state a definition starts in: its message types' schemas are reported in
/// the generated document, and mounting demands they are derivable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Documented;

/// The `#[subscriber]` attribute's documentation state: schemas and message metadata were
/// captured at the expansion site by the autoref probes and ride the definition as data
/// ([`Docs`]), so the state itself demands nothing of the message types - a type without
/// `JsonSchema` simply contributes no schema, exactly as the attribute always behaved.
/// Machinery behind the macro expansion; never named in user code.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Probed;

/// The opted-out documentation state: the registration reports no schemas and demands nothing
/// of its message types.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Undocumented;

/// A type whose JSON Schema the generated document can report.
///
/// With the `asyncapi` feature this is every `schemars::JsonSchema` type; without it, every type
/// (and nothing is produced). The bound surfaces at the mount of a documented definition.
#[cfg(feature = "asyncapi")]
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no JSON Schema for the generated document",
    note = "registrations are documented by default: derive `schemars::JsonSchema` for `{Self}`, \
            or opt this registration out with `.undocumented()`"
)]
pub trait Documentable {
    /// The serialized JSON Schema, captured once at registration.
    fn schema_json() -> Option<String>;
}

#[cfg(feature = "asyncapi")]
impl<T: schemars::JsonSchema + ?Sized> Documentable for T {
    fn schema_json() -> Option<String> {
        serde_json::to_string(&schemars::schema_for!(T)).ok()
    }
}

/// See the `asyncapi`-gated definition; without the feature nothing is demanded or produced.
#[cfg(not(feature = "asyncapi"))]
pub trait Documentable {
    /// No document is generated without the `asyncapi` feature.
    fn schema_json() -> Option<String> {
        None
    }
}

#[cfg(not(feature = "asyncapi"))]
impl<T: ?Sized> Documentable for T {}

/// What one documentation state produces for a type: the schema under [`Documented`] (which is
/// where the [`Documentable`] obligation lives), nothing under [`Undocumented`].
pub trait DocState<T: ?Sized> {
    /// The serialized JSON Schema this state reports for `T`.
    fn schema() -> Option<String>;
}

impl<T: ?Sized + Documentable> DocState<T> for Documented {
    fn schema() -> Option<String> {
        T::schema_json()
    }
}

impl<T: ?Sized> DocState<T> for Undocumented {
    fn schema() -> Option<String> {
        None
    }
}

// The probed state computes nothing per type: everything it reports was captured into `Docs`
// at the expansion site.
impl<T: ?Sized> DocState<T> for Probed {
    fn schema() -> Option<String> {
        None
    }
}

/// The chain-collected documentation values of one definition: what `describe` set, plus the
/// probe-captured metadata a `#[subscriber]` expansion carries (`None` fields defer to the
/// axis-computed values, so the value path is unaffected).
#[derive(Debug, Clone, Default)]
pub struct Docs {
    pub(crate) description: Option<Cow<'static, str>>,
    pub(crate) input_schema: Option<String>,
    pub(crate) headers_schema: Option<String>,
    pub(crate) message_name: Option<&'static str>,
    pub(crate) message_description: Option<&'static str>,
    pub(crate) outgoing: Option<Vec<OutgoingMessageMetadata>>,
}

impl Docs {
    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

/// The probe-captured metadata of one `#[subscriber]` expansion, evaluated at the concrete
/// types of the handler's signature (the autoref probes only specialize there) and carried into
/// the definition as data. Machinery behind the macro expansion; not part of the public API.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct ProbedDocs {
    /// The handler's doc comment.
    pub description: Option<&'static str>,
    /// The input type's JSON Schema, when derivable.
    pub input_schema: Option<String>,
    /// The typed header contract's JSON Schema, when one applies.
    pub headers_schema: Option<String>,
    /// The input type's `MessageInfo` name.
    pub message_name: Option<&'static str>,
    /// The input type's `MessageInfo` description.
    pub message_description: Option<&'static str>,
    /// The declared outgoing messages (the reply entry, the slots' dictionaries).
    pub outgoing: Option<Vec<OutgoingMessageMetadata>>,
}

impl ProbedDocs {
    /// Lowers the capture into the definition's [`Docs`].
    pub(super) fn into_docs(self) -> Docs {
        Docs {
            description: self.description.map(Cow::Borrowed),
            input_schema: self.input_schema,
            headers_schema: self.headers_schema,
            message_name: self.message_name,
            message_description: self.message_description,
            outgoing: self.outgoing,
        }
    }
}
