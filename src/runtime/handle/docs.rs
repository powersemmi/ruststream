//! Documented-by-default: the typestate and the schema capture behind `.undocumented()`.
//!
//! Under the `asyncapi` feature every registration reports its message schemas unless the chain
//! opts out with [`undocumented`](crate::runtime::SubscriberBuilder::undocumented). The
//! `JsonSchema` obligation is checked where the definition mounts, and only for definitions
//! still in the documented state, so the opt-out is a compile-time exit rather than a runtime
//! switch. Without the feature the state still exists but demands nothing and produces nothing.

use std::borrow::Cow;

/// The documentation state a definition starts in: its message types' schemas are reported in
/// the generated document, and mounting demands they are derivable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Documented;

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

/// The chain-collected documentation values of one definition: what `describe` set.
#[derive(Debug, Clone, Default)]
pub struct Docs {
    pub(crate) description: Option<Cow<'static, str>>,
}

impl Docs {
    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}
