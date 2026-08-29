//! The batch reply-publishing value definition: a page-settling body whose replies are encoded
//! and published, atomically when the attached wiring is transactional.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::schema::{FixedName, OutgoingDestination};
use crate::{Name, Unnamed};

use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingDef};
use crate::runtime::context::Context;
use crate::runtime::handler::HandlerResult;
use crate::runtime::input::Decoded;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::SubscriberBuilder;

use super::IntoSource;
use super::replying::{DeclaredName, To};
use super::subscribing::{Docs, DocumentedValue, docs_metadata};

/// A page-settling body producing one reply per element: the value-path counterpart of a
/// `#[subscriber(batch(..), publish(..))]` body.
///
/// `Ok(replies)` publishes every entry to the definition's destination and acks the batch;
/// `Err(result)` publishes nothing and settles the whole batch with `result`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not produce replies for batches of `{T}`",
    note = "implement `BatchReply<{T}>` on the handler type - `type Out` names the reply \
            element, and `reply` returns `Result<Vec<Self::Out>, HandlerResult>`"
)]
pub trait BatchReply<T, S = ()>: Send + Sync {
    /// The reply element type; each entry of the returned `Vec` is published.
    type Out;

    /// Produces the page's replies.
    fn reply(
        &self,
        batch: &[T],
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Vec<Self::Out>, HandlerResult>> + Send;
}

/// A batch reply-publishing definition built from a value: what
/// `batch_replying(source, handler)` returns, wrapped in the settings builder.
pub struct BatchReplyingValue<T, R, H, Dest> {
    pub(crate) handler: H,
    pub(crate) dest: Dest,
    pub(crate) docs: Docs,
    pub(crate) _types: PhantomData<fn() -> (T, R)>,
}

impl<T, R, H, Dest> fmt::Debug for BatchReplyingValue<T, R, H, Dest> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchReplyingValue").finish_non_exhaustive()
    }
}

impl<T, R, H, Dest> IncludeDef for BatchReplyingValue<T, R, H, Dest> {
    type Form = forms::BatchPublishing;
}

impl<T, R, H, Dest> DocumentedValue for BatchReplyingValue<T, R, H, Dest> {
    type Payload = T;
    type Reply = R;

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

/// The metadata methods shared by the two destination states.
macro_rules! batch_replying_def_common {
    () => {
        type Input = Decoded<T>;
        type Injections = ();
        type Reply = R;
        // The stored value never builds a source: the settings builder wrapping it carries the
        // real one (see `SubscriberValue::Source`).
        type Source = Unnamed<Name>;

        fn source(&self) -> Self::Source {
            Unnamed::new()
        }

        docs_metadata!();

        fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
            vec![
                OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R>())
                    .with_payload_schema(self.docs.reply_schema()),
            ]
        }
    };
}

impl<T, R, H> BatchPublishingDef for BatchReplyingValue<T, R, H, To>
where
    T: Send + Sync + 'static,
    R: Send + Sync,
    H: Send + Sync,
{
    batch_replying_def_common!();

    fn reply_name(&self) -> &str {
        &self.dest.0
    }
}

impl<T, R, H> BatchPublishingDef for BatchReplyingValue<T, R, H, DeclaredName>
where
    T: Send + Sync + 'static,
    R: OutgoingDestination<Form = FixedName> + Send + Sync,
    H: Send + Sync,
{
    batch_replying_def_common!();

    fn reply_name(&self) -> &str {
        R::ADDRESS
    }
}

impl<T, R, H, Dest, S> BatchPublishingCall<S> for BatchReplyingValue<T, R, H, Dest>
where
    Self: BatchPublishingDef<Input = Decoded<T>, Injections = (), Reply = R>,
    T: Send + Sync + 'static,
    H: BatchReply<T, S, Out = R>,
    S: Send + Sync,
{
    fn call(
        &self,
        batch: &[T],
        _injections: &(),
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Vec<R>, HandlerResult>> + Send {
        self.handler.reply(batch, ctx)
    }
}

impl<T, R, H, Src, State, DC>
    SubscriberBuilder<BatchReplyingValue<T, R, H, DeclaredName>, Src, State, DC>
{
    /// Names the subject the page's replies are published to. See
    /// [`to`](SubscriberBuilder::to) on the single-message reply form.
    #[must_use]
    pub fn to(
        self,
        name: impl Into<Cow<'static, str>>,
    ) -> SubscriberBuilder<BatchReplyingValue<T, R, H, To>, Src, State, DC> {
        self.map_def(|def| BatchReplyingValue {
            handler: def.handler,
            dest: To(name.into()),
            docs: def.docs,
            _types: PhantomData,
        })
    }
}

/// What the batch reply constructors return: the chain over the destination-open definition.
type BatchReplyingBuilder<Src, T, R, H> =
    super::ValueBuilder<BatchReplyingValue<T, R, H, DeclaredName>, Src>;

fn build_batch_replying<Src, T, R, H>(source: Src, handler: H) -> BatchReplyingBuilder<Src, T, R, H>
where
    Src: IntoSource,
{
    SubscriberBuilder::new(
        BatchReplyingValue {
            handler,
            dest: DeclaredName,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// Binds a page-settling, reply-producing `handler` to the batch subscription `source`: the
/// value-path counterpart of `#[subscriber(batch("in"), publish("out"))]`.
///
/// Mount it with `include`, chaining `.publisher(..)` to attach the reply publish policy (mark
/// it `.transactional()` for atomically visible pages) or nothing for the broker's default.
/// The destination follows the reply element type's `#[outgoing(name = "...")]` declaration,
/// with a mandatory [`to`](SubscriberBuilder::to) when it names none. A body implemented for
/// one concrete app state type takes [`batch_replying_in`] instead.
#[must_use]
pub fn batch_replying<Src, T, H>(source: Src, handler: H) -> BatchReplyingBuilder<Src, T, H::Out, H>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    H: BatchReply<T>,
{
    build_batch_replying(source, handler)
}

/// [`batch_replying`] for a body implemented for one concrete app state type. See
/// [`subscriber_in`](super::subscriber_in) for the split.
#[must_use]
pub fn batch_replying_in<Src, T, St, H>(
    source: Src,
    handler: H,
) -> BatchReplyingBuilder<Src, T, <H as BatchReply<T, St>>::Out, H>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    H: BatchReply<T, St>,
{
    build_batch_replying(source, handler)
}
