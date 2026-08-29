//! The seek-injection value definitions: a body that holds its subscription's own seeker.
//!
//! A `Seek<K>` injection resolves off the opened subscription itself, so the include site
//! attaches nothing: the forms mount eagerly, and the body receives the seeker through the same
//! injection tuple the slot forms use.

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::{Name, Unnamed};

use crate::runtime::batch::BatchResult;
use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::runtime::context::Context;
use crate::runtime::handler::Settle;
use crate::runtime::inject::{InjectCall, InjectDef, Seek};
use crate::runtime::input::{Decoded, InputKind};
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::{AllOpen, SubscriberBuilder};

use super::batch_slots::SlotsSliceHandler;
use super::slots::SlotsHandler;
use super::subscribing::{Docs, DocumentedValue, docs_metadata};
use super::{HandledInput, IntoSource};

/// The variance-neutral marker of a definition's carried type parameters.
type Carried<T> = PhantomData<fn() -> T>;

/// A seek-holding definition built from a value: what `with_seek(source, handler)` returns,
/// wrapped in the settings builder. `In` is the input kind the constructor resolved off the
/// body's parameter type.
pub struct SeekValue<In, H, K, C = ()> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(In, K, C)>,
}

impl<In, H, K, C> fmt::Debug for SeekValue<In, H, K, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeekValue").finish_non_exhaustive()
    }
}

impl<In, H, K, C> IncludeDef for SeekValue<In, H, K, C> {
    type Form = forms::Seek;
}

impl<In: InputKind, H, K, C> DocumentedValue for SeekValue<In, H, K, C> {
    type Payload = In::Target;
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

impl<In, H, K, C> InjectDef for SeekValue<In, H, K, C>
where
    In: InputKind,
    H: Send + Sync,
    K: Send + Sync,
    C: Send + Sync,
{
    type Input = In;
    type Context = C;
    // The stored value never builds a source: the settings builder wrapping it carries the real
    // one (see `SubscriberValue::Source`).
    type Source = Unnamed<Name>;
    type Injections = (Seek<K>,);

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    docs_metadata!();
}

impl<In, H, K, C, S> InjectCall<S> for SeekValue<In, H, K, C>
where
    In: InputKind,
    H: SlotsHandler<In::Target, (Seek<K>,), C, S>,
    K: Send + Sync,
    C: Send + Sync,
    S: Send + Sync,
{
    fn call(
        &self,
        input: &In::Target,
        injections: &(Seek<K>,),
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Settle> + Send {
        self.handler.handle(input, injections, ctx)
    }
}

impl<In, H, K, C, Src, State, DC> SubscriberBuilder<SeekValue<In, H, K, C>, Src, State, DC> {
    /// Names the broker's typed per-delivery context the body reads, replacing the unit
    /// default. The body's bound is checked at the mount.
    #[must_use]
    pub fn context<C2>(self) -> SubscriberBuilder<SeekValue<In, H, K, C2>, Src, State, DC> {
        self.map_def(|def| SeekValue {
            handler: def.handler,
            docs: def.docs,
            _types: PhantomData,
        })
    }
}

/// Binds a seeker-holding `handler` to the subscription `source`: the value-path counterpart of
/// a `#[subscriber]` body with a `Seek(seeker): Seek<K>` parameter.
///
/// The seeker is minted off the subscription right after it opens, so the body holds a live
/// handle by construction; on a broker without the [`Seekable`](crate::Seekable) capability the
/// mount does not compile. The body is the same [`SlotsHandler`] shape the `Out` forms use,
/// with the seek injection as its tuple.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySeeker, MemorySource};
/// use ruststream::prelude::*;
/// use ruststream::Seeker;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64, poisoned_until: Option<usize> }
///
/// struct Work;
///
/// impl<S: Send + Sync> SlotsHandler<Job, (Seek<MemorySeeker>,), (), S> for Work {
///     async fn handle(
///         &self,
///         job: &Job,
///         slots: &(Seek<MemorySeeker>,),
///         _ctx: &mut Context<'_, (), S>,
///     ) -> Settle {
///         let Seek(seeker) = &slots.0;
///         if let Some(resume_at) = job.poisoned_until {
///             if seeker.seek(MemoryPosition::sequence(resume_at)).await.is_err() {
///                 return HandlerResult::retry().into();
///             }
///         }
///         HandlerResult::ack().into()
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(with_seek::<Job, MemorySeeker, _, _>(MemorySource::new("jobs"), Work));
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn with_seek<T, K, Src, H>(
    source: Src,
    handler: H,
) -> super::ValueBuilder<SeekValue<T::Kind, H, K>, Src>
where
    Src: IntoSource,
    T: ?Sized + HandledInput,
{
    SubscriberBuilder::new(
        SeekValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// The batch counterpart of [`SeekValue`]: what `batch_with_seek(source, handler)` returns.
pub struct BatchSeekValue<T, H, K> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(T, K)>,
}

impl<T, H, K> fmt::Debug for BatchSeekValue<T, H, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchSeekValue").finish_non_exhaustive()
    }
}

impl<T, H, K> IncludeDef for BatchSeekValue<T, H, K> {
    type Form = forms::BatchSeek;
}

impl<T, H, K> DocumentedValue for BatchSeekValue<T, H, K> {
    type Payload = T;
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

impl<T, H, K> BatchInjectDef for BatchSeekValue<T, H, K>
where
    T: Send + Sync + 'static,
    H: Send + Sync,
    K: Send + Sync,
{
    type Input = Decoded<T>;
    type Source = Unnamed<Name>;
    type Injections = (Seek<K>,);

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    docs_metadata!();
}

impl<T, H, K, S> BatchInjectCall<S> for BatchSeekValue<T, H, K>
where
    T: Send + Sync + 'static,
    H: SlotsSliceHandler<T, (Seek<K>,), S>,
    K: Send + Sync,
    S: Send + Sync,
{
    fn call(
        &self,
        batch: &[T],
        injections: &(Seek<K>,),
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = BatchResult> + Send {
        self.handler.handle_slice(batch, injections, ctx)
    }
}

/// The batch counterpart of [`with_seek`]: the body settles whole pages while holding its
/// subscription's seeker, through the same injection tuple.
#[must_use]
pub fn batch_with_seek<T, K, Src, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<BatchSeekValue<T, H, K>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
{
    SubscriberBuilder::new(
        BatchSeekValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}
