//! What a handler reads off a retaining broker's delivery: its position in the publish log and
//! a handle to seek the subscription with.

use crate::{MemoryMessage, MemoryPosition, MemorySeeker, Retaining};
use ruststream_core::Positioned;

/// The position is the delivery's absolute sequence number in its name's publish log, assigned
/// at fanout and preserved across requeues, so a redelivered message reports the same position.
/// Only a retaining broker's deliveries have one: without a log there is nothing to be a
/// position in.
impl Positioned for MemoryMessage<Retaining> {
    type Position = MemoryPosition;

    fn position(&self) -> MemoryPosition {
        MemoryPosition::Sequence(self.delivery.as_ref().map_or(0, |d| d.seq))
    }
}

/// The in-memory broker's per-delivery context: this delivery's position in its name's publish
/// log, and the subscription's own seeker.
///
/// The runtime builds one per dispatched delivery (see [`BuildContext`](ruststream_core::BuildContext)),
/// and handlers read its fields by key - [`Position`] and [`SeekHandle`] - through the
/// [`Ctx`](ruststream_runtime::Ctx) extractor or `ctx.context(..)`. A body that manages a log
/// cursor names this type as its `C` axis and needs nothing else.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::memory::{MemoryContext, Position, SeekHandle};
/// use ruststream::prelude::*;
/// use ruststream::Seeker;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Job { id: u64 }
///
/// struct Replayer;
///
/// impl Handle<Job, (), (), MemoryContext> for Replayer {
///     async fn handle(
///         &self,
///         job: &Job,
///         _outs: &(),
///         ctx: &mut Context<'_, MemoryContext>,
///     ) -> Result<(), HandlerOutcome> {
///         let here = ctx.context(Position);
///         if job.id == u64::MAX && ctx.context(SeekHandle).seek(here).await.is_err() {
///             return Err(HandlerOutcome::retry());
///         }
///         Ok(())
///     }
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct MemoryContext {
    position: MemoryPosition,
    seeker: MemorySeeker,
}

impl ruststream_core::BuildContext<MemoryMessage<Retaining>> for MemoryContext {
    /// # Panics
    ///
    /// Panics on a request-reply inbox message, which is not a subscription delivery; no
    /// dispatch loop (and therefore no per-delivery context) ever builds off one.
    fn build(msg: &MemoryMessage<Retaining>) -> Self {
        Self {
            position: Positioned::position(msg),
            seeker: MemorySeeker::of(msg),
        }
    }
}

/// The in-memory broker's subscription-scoped batch context: the subscription's own seeker,
/// shared by every delivery of the batch.
///
/// The runtime builds one per dispatched batch from the batch's first delivery (see
/// [`BuildBatchContext`](ruststream_core::BuildBatchContext)), and a batch body reads it by key -
/// [`SeekHandle`] - through `ctx.context(..)`. Per-delivery data (a [`Position`]) has no place
/// here: a batch spans many deliveries, so the position a body reacts to rides the elements
/// themselves (a `&[Message<H, T>]` batch reads it off each element's header contract), and
/// keeping this a separate type from [`MemoryContext`] is what rejects a batch body asking for
/// per-delivery fields at compile time.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::Seeker;
/// use ruststream::memory::{MemoryBatchContext, MemoryPosition, SeekHandle};
/// use ruststream::prelude::*;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Job { id: u64 }
///
/// struct Replayer;
///
/// impl Handle<[Job], (), (), MemoryBatchContext> for Replayer {
///     async fn handle(
///         &self,
///         batch: &[Job],
///         _outs: &(),
///         ctx: &mut Context<'_, MemoryBatchContext>,
///     ) -> Result<(), Vec<HandlerOutcome>> {
///         // A batch that saw the rewind marker repositions the whole subscription once it is
///         // settled; the next batch opens at the target.
///         if batch.iter().any(|job| job.id == u64::MAX)
///             && ctx
///                 .context(SeekHandle)
///                 .seek(MemoryPosition::start())
///                 .await
///                 .is_err()
///         {
///             return Err(batch.iter().map(|_| HandlerOutcome::retry()).collect());
///         }
///         Ok(())
///     }
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct MemoryBatchContext {
    seeker: MemorySeeker,
}

impl ruststream_core::BuildBatchContext<MemoryMessage<Retaining>> for MemoryBatchContext {
    /// # Panics
    ///
    /// Panics on a request-reply inbox message, which is not a subscription delivery; no
    /// dispatch loop (and therefore no batch context) ever builds off one.
    fn build(first: &MemoryMessage<Retaining>) -> Self {
        Self {
            seeker: MemorySeeker::of(first),
        }
    }
}

impl ruststream_core::Field<MemoryBatchContext> for SeekHandle {
    type Value<'a> = &'a MemorySeeker;

    fn get(self, src: &MemoryBatchContext) -> &MemorySeeker {
        &src.seeker
    }
}

/// The key reading this delivery's [`MemoryPosition`] out of [`MemoryContext`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::memory::Position;
/// use ruststream::prelude::*;
/// use ruststream::subscriber;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// #[subscriber("jobs")]
/// async fn audit(job: &Job, Ctx(at): Ctx<Position>) -> HandlerOutcome {
///     println!("job {} sits at {at:?}", job.id);
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Position;

impl ruststream_core::ContextField for Position {
    type Context = MemoryContext;
    type Value = MemoryPosition;

    fn read(self, src: &MemoryContext) -> MemoryPosition {
        src.position
    }
}

impl ruststream_core::Field<MemoryContext> for Position {
    type Value<'a> = MemoryPosition;

    fn get(self, src: &MemoryContext) -> MemoryPosition {
        src.position
    }
}

/// The key reading the subscription's [`MemorySeeker`] out of [`MemoryContext`]: the reposition
/// handle, resolved once when the subscription opens and carried by every delivery's context.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::memory::{MemoryPosition, SeekHandle};
/// use ruststream::prelude::*;
/// use ruststream::{Seeker, subscriber};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64, poisoned_until: Option<usize> }
///
/// /// Skips forward past a region the producer marked poisoned.
/// #[subscriber("jobs")]
/// async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
///     if let Some(resume_at) = job.poisoned_until {
///         if seeker.seek(MemoryPosition::sequence(resume_at)).await.is_err() {
///             return HandlerOutcome::retry();
///         }
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SeekHandle;

impl ruststream_core::ContextField for SeekHandle {
    type Context = MemoryContext;
    type Value = MemorySeeker;

    fn read(self, src: &MemoryContext) -> MemorySeeker {
        src.seeker.clone()
    }
}

impl ruststream_core::Field<MemoryContext> for SeekHandle {
    type Value<'a> = &'a MemorySeeker;

    fn get(self, src: &MemoryContext) -> &MemorySeeker {
        &src.seeker
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use ruststream_core::Settle;

    use crate::capability::tests::replaying;
    use ruststream_core::{OutgoingMessage, Positioned, Publisher, Subscriber};

    #[tokio::test]
    async fn position_is_stable_across_a_requeue() {
        let broker = replaying();
        let mut sub = broker.subscribe("seek.requeue");
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("seek.requeue", b"a"), None)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        let position = msg.position();
        msg.nack(true).await.unwrap();

        let redelivered = stream.next().await.unwrap().unwrap();
        assert_eq!(redelivered.position(), position);
        redelivered.ack().await.unwrap();
    }
}
