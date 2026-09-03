//! Seek through the context: position values and the reposition handle ride the broker context
//! axis `C`, so a body that manages a log cursor declares `Context<'_, SeekContext<K>>` and
//! nothing else.
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "json"))]
//! # mod demo {
//! use ruststream::memory::MemorySeeker;
//! use ruststream::prelude::*;
//! use ruststream::runtime::SeekContext;
//! # #[derive(serde::Deserialize, schemars::JsonSchema)]
//! # struct Job { id: u64 }
//!
//! struct Replayer;
//!
//! impl Handle<Job, (), (), SeekContext<MemorySeeker>> for Replayer {
//!     async fn handle(
//!         &self,
//!         job: &Job,
//!         _outs: &(),
//!         ctx: &mut Context<'_, SeekContext<MemorySeeker>>,
//!     ) -> Result<(), HandlerOutcome> {
//!         let _here = ctx.position();
//!         let _ = job.id;
//!         Ok(())
//!     }
//! }
//! # }
//! ```
//!
//! A source that cannot seek (or seeks with a different kind) fails at the mount: the context
//! builds off the delivered message, and only a [`SeekableMessage`](crate::SeekableMessage)
//! with the matching [`Positioned`](crate::Positioned) position provides one.

use std::fmt;
use std::future::Future;

use crate::{BuildContext, Positioned, SeekableMessage, Seeker};

use crate::runtime::context::Context;

/// The seek-carrying broker context: the delivery's position, and the subscription's seeker.
///
/// Declare it as the body's `C` axis (`Context<'_, SeekContext<MemorySeeker>>`); the runtime
/// builds one per delivery, and [`Context::position`] / [`Context::seek`] read it.
pub struct SeekContext<K: Seeker> {
    seeker: K,
    position: K::Position,
}

impl<K: Seeker> fmt::Debug for SeekContext<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeekContext").finish_non_exhaustive()
    }
}

impl<M, K> BuildContext<M> for SeekContext<K>
where
    K: Seeker,
    M: SeekableMessage<Seeker = K> + Positioned<Position = K::Position>,
{
    fn build(msg: &M) -> Self {
        Self {
            seeker: msg.seeker(),
            position: msg.position(),
        }
    }
}

impl<'a, K: Seeker, S> Context<'a, SeekContext<K>, S> {
    /// The position of this delivery in its log; seeking to it redelivers this message.
    #[must_use]
    pub fn position(&self) -> &K::Position {
        &self.cx_ref().position
    }

    /// Repositions this subscription's cursor: the next deliveries follow `to`.
    ///
    /// # Errors
    ///
    /// Returns the broker's own error when it rejects the reposition.
    pub fn seek(
        &self,
        to: K::Position,
    ) -> impl Future<Output = Result<(), <K as Seeker>::Error>> + Send + use<'_, 'a, K, S> {
        self.cx_ref().seeker.seek(to)
    }
}
