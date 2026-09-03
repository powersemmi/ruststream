//! The manual path's one body contract and one mounting verb.
//!
//! A handler is a type implementing [`Handle`]; every axis of the form matrix is a defaulted
//! parameter the impl pins, so the signature decides everything and nothing else is named:
//!
//! - `In` - the input spelling (see [`Input`]): a decoded `T`, a [`Deserialized`] type, a
//!   [`Message<H, P>`] pair, or a page of any of them;
//! - `R` - the verdict's `Ok` side (`()` declares no reply; a reply body names its reply type,
//!   a page reply body `Vec<Reply>`; a [`Message<H, P>`] reply carries typed headers; a
//!   [`Serialized`] reply's bytes leave as they are);
//! - `O` - the injections arena (`()` declares none; see [`Outs`]);
//! - `C` - the broker's typed per-delivery context;
//! - `S` - the typed application state the body reads via
//!   [`Context::state`](super::Context::state).
//!
//! [`subscriber`] binds a body to its subscription source and returns the definition chain;
//! `.build()` seals it and [`include`](super::Router::include) mounts it. The declarative
//! settings ([`workers`](super::SubscriberSettings::workers),
//! [`on_failure`](super::SubscriberSettings::on_failure), ...) chain between the two, exactly as
//! on the attribute path.
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "json"))]
//! # mod demo {
//! use ruststream::memory::MemoryBroker;
//! use ruststream::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize, schemars::JsonSchema)]
//! struct Order {
//!     id: u64,
//! }
//!
//! struct Audit;
//!
//! impl Handle<Order> for Audit {
//!     async fn handle(&self, order: &Order, _outs: &(), _ctx: &mut Context<'_>) -> Result<(), HandlerOutcome> {
//!         println!("order {}", order.id);
//!         Ok(())
//!     }
//! }
//!
//! fn app() -> RustStream {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
//!         b.include(subscriber("orders", Audit).build());
//!     })
//! }
//! # }
//! ```

mod axis;
mod capabilities;
mod docs;
mod eager;
mod outs;
mod reply;
mod reply_slots;
mod value;
mod verdict;

#[cfg(all(test, feature = "memory", feature = "json", feature = "asyncapi"))]
mod matrix_tests;
#[cfg(all(test, feature = "memory", feature = "json", feature = "asyncapi"))]
mod parity_tests;

#[doc(hidden)]
pub use axis::{Axis, AxisDocs, Page, PagePair, PagedAxis, Solo, SoloAxis, SoloPair};
pub use axis::{Deserialized, Input, Message, PageDeserialized, SoloDeserialized};
pub use capabilities::{
    ErrorOf, OwnedTransactionOf, OwnedTransactionalPublish, Publish, ReplyOf, RequestReplyPublish,
    ScopeOf, TransactionalPublish,
};
#[doc(hidden)]
pub use docs::{DocState, Docs, Probed, ProbedDocs};
pub use docs::{Documentable, Documented, Undocumented};
#[doc(hidden)]
pub use outs::{EntryMarkers, OutPos, SelectSlot};
pub use outs::{Outs, Slot};
#[doc(hidden)]
pub use reply::{
    ReplyDest, ReplyFormFor, ReplyHeadersSchema, ReplyRoute, SealedBatchPublishing,
    SealedPublishing, SealedRawReply, SplitAttach, WireDocs,
};
pub use reply::{ReplyShape, Serialized};
#[doc(hidden)]
pub use reply_slots::{SealedBatchPublishingOut, SealedPublishingOut, SealedRawReplyOut};
pub use value::{
    DeclaredDest, DefaultReplyAttach, EncodedReply, HandleValue, IsDocumented, NamedDest,
    ReplyAttach, ReplyValue, Sealed, SerializedReply, subscriber,
};
#[doc(hidden)]
pub use value::{ProbedReplyDef, UnbuiltDefinition, probed_def, probed_reply_def};
pub use verdict::Verdict;
#[doc(hidden)]
pub use verdict::{OneByOne, Paged, VerdictFamily};

/// What the constructor returns: the settings builder over its definition, mounted on the
/// converted source with every setting open.
pub type ValueBuilder<Def, Src> = crate::runtime::settings::SubscriberBuilder<
    Def,
    <Src as IntoSource>::Source,
    crate::runtime::settings::AllOpen,
>;

use std::borrow::Cow;
use std::future::Future;

use crate::{Name, Unnamed};

use super::context::Context;

/// The one body contract of the manual path: every axis of the form matrix is a defaulted
/// parameter the impl pins.
///
/// The lane is the type's business on both ends: a `serde` input decodes through the codec
/// while a [`Deserialized`] one constructs itself from the payload, and a `serde` reply encodes
/// through the reply codec while a [`Serialized`] one leaves byte-for-byte.
///
/// The `outs` parameter carries the injections arena for a body that declared one (`O` other
/// than `()`); a body without injections takes `&()`. The returned future's output is the
/// input family's canonical verdict ([`Verdict<In, R>`]): `Result<R, HandlerOutcome>` for one
/// message at a time, `Result<R, Vec<HandlerOutcome>>` for a page - so the body's `async fn`
/// signature spells the verdict out literally.
pub trait Handle<In: ?Sized + Input, R = (), O = (), C = (), S = ()>: Send + Sync {
    /// Handles one input (a message, or a page of them).
    fn handle(
        &self,
        input: &In,
        outs: &O,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Verdict<In, R>> + Send;
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
