//! What a broker keeps of what it published: the per-name log, its bound, and the two log modes.
//!
//! The log is the replay store the [`Seekable`](crate::Seekable) capability reads back. A
//! [`Discarding`] broker keeps none, so publishing costs the fanout and nothing else and memory
//! does not grow with the message count. A [`Retaining`] broker keeps the newest messages of
//! every name within its [`Retention`] bound, and only its subscriptions can be repositioned.

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
};

use bytes::Bytes;

use crate::{HeaderMap, RawMessage};

/// How much of one name's publish log a retaining broker keeps.
///
/// The bound is per name: a broker publishing under a thousand names holds up to this much for
/// each of them. The newest message always stays, so a payload wider than a byte bound is kept
/// alone rather than dropped on arrival.
///
/// # Examples
///
/// ```
/// use ruststream::memory::{MemoryBroker, Retention};
/// use ruststream::nonzero;
///
/// // The last 64 messages of every name stay replayable.
/// let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(64)));
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Retention {
    /// Keep at most this many messages per name.
    Messages(NonZeroUsize),
    /// Keep at most this many payload bytes per name; header bytes do not count.
    Bytes(NonZeroUsize),
    /// Keep within both bounds at once: whichever is reached first evicts the oldest message.
    MessagesAndBytes {
        /// The message-count bound.
        messages: NonZeroUsize,
        /// The payload-byte bound.
        bytes: NonZeroUsize,
    },
}

impl Retention {
    /// Whether a name holding `messages` messages of `bytes` payload bytes is within the bound.
    fn admits(self, messages: usize, bytes: usize) -> bool {
        match self {
            Self::Messages(limit) => messages <= limit.get(),
            Self::Bytes(limit) => bytes <= limit.get(),
            Self::MessagesAndBytes {
                messages: message_limit,
                bytes: byte_limit,
            } => messages <= message_limit.get() && bytes <= byte_limit.get(),
        }
    }
}

/// The log mode of a broker that keeps no publish log, and the default of
/// [`MemoryBroker`](super::MemoryBroker).
///
/// [`Seekable`](crate::Seekable) is not implemented for its subscriptions, so replaying one is a
/// compile error rather than a replay that silently finds nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Discarding;

/// The log mode of a [`MemoryBroker::retaining`](super::MemoryBroker::retaining) broker.
///
/// The newest messages of every name are kept within its [`Retention`] bound, and its
/// subscriptions are [`Seekable`](crate::Seekable).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Retaining;

/// The closed set of log modes: [`Discarding`] and [`Retaining`].
///
/// Sealed, so the vocabulary cannot grow outside the crate. Which mode a broker carries decides
/// at compile time whether its subscriptions can be repositioned.
pub trait LogMode: sealed::Sealed + Send + Sync + 'static {}

impl LogMode for Discarding {}
impl LogMode for Retaining {}

mod sealed {
    /// Seals [`LogMode`](super::LogMode) and carries the branch generic code takes on the mode.
    pub trait Sealed {
        /// Whether a broker in this mode records what it publishes.
        const RETAINS: bool;
    }

    impl Sealed for super::Discarding {
        const RETAINS: bool = false;
    }

    impl Sealed for super::Retaining {
        const RETAINS: bool = true;
    }
}

/// Whether `Log` records what it publishes, as a constant the compiler folds away: the seek and
/// replay work of a subscription lives behind it, and monomorphization drops it outright from a
/// discarding broker.
pub(super) const fn retains<Log: LogMode>() -> bool {
    <Log as sealed::Sealed>::RETAINS
}

/// The broker's publish log: absent in [`Discarding`] mode, a bounded per-name ring otherwise.
///
/// One value rather than a map beside a flag, so "no log" and "an empty log" cannot disagree.
pub(super) enum LogState {
    /// Nothing is recorded; a publish never reaches this value.
    Discarding,
    /// The per-name rings and what bounds them.
    Recording {
        budget: Budget,
        names: HashMap<Arc<str>, NameLog>,
    },
}

/// What bounds a recording log.
///
/// `Unbounded` is not a user's choice: the test harness installs it on a discarding broker so
/// published-message assertions have something to read. A run is short, and the broker's own code
/// cannot observe the log (seeking does not compile without a retaining broker), so the recording
/// cannot make a test pass where production would fail.
pub(super) enum Budget {
    Bounded(Retention),
    Unbounded,
}

impl LogState {
    /// The log of a broker retaining under `retention`.
    pub(super) fn recording(retention: Retention) -> Self {
        Self::Recording {
            budget: Budget::Bounded(retention),
            names: HashMap::new(),
        }
    }

    /// Starts recording without a bound, for the length of a harness run. Returns whether this
    /// call is what turned recording on; a log that already records keeps the bound it has.
    pub(super) fn record_for_harness(&mut self) -> bool {
        if matches!(self, Self::Recording { .. }) {
            return false;
        }
        *self = Self::Recording {
            budget: Budget::Unbounded,
            names: HashMap::new(),
        };
        true
    }

    /// Records `payload` under `name` and returns its absolute sequence number there.
    ///
    /// The key is cloned only for a name recorded for the first time; every later append reuses
    /// the `Arc<str>` the fanout already holds.
    pub(super) fn append(
        &mut self,
        name: &Arc<str>,
        payload: &Bytes,
        headers: &Arc<HeaderMap>,
    ) -> usize {
        let Self::Recording { budget, names } = self else {
            return 0;
        };
        let log = names.entry(Arc::clone(name)).or_default();
        log.push(
            LogEntry {
                payload: payload.clone(),
                headers: Arc::clone(headers),
            },
            budget,
        )
    }

    /// This name's log, if anything was recorded under it.
    pub(super) fn name(&self, name: &str) -> Option<&NameLog> {
        match self {
            Self::Discarding => None,
            Self::Recording { names, .. } => names.get(name),
        }
    }

    /// The names something is recorded under, for the checks that assert nothing outlives what
    /// wrote it.
    #[cfg(test)]
    pub(super) fn recorded_names(&self) -> Vec<&str> {
        match self {
            Self::Discarding => Vec::new(),
            Self::Recording { names, .. } => names.keys().map(|name| &**name).collect(),
        }
    }

    /// Drops everything recorded under `name`.
    pub(super) fn forget(&mut self, name: &str) {
        if let Self::Recording { names, .. } = self {
            names.remove(name);
        }
    }
}

/// One name's ring of retained messages.
#[derive(Default)]
pub(super) struct NameLog {
    entries: VecDeque<LogEntry>,
    /// Absolute sequence number of the oldest retained message, so positions stay absolute as
    /// the ring moves: a captured position keeps naming the same message until it is evicted.
    first_seq: usize,
    /// Retained payload bytes, tracked rather than recomputed per publish.
    bytes: usize,
}

impl NameLog {
    /// The absolute position of the oldest retained message.
    pub(super) fn first_seq(&self) -> usize {
        self.first_seq
    }

    /// The absolute position the next publish under this name will take.
    pub(super) fn next_seq(&self) -> usize {
        self.first_seq + self.entries.len()
    }

    /// The retained messages from `seq` on, oldest first, each with its absolute position.
    /// Positions below the oldest retained one start the replay at what is left.
    pub(super) fn replay_from(&self, seq: usize) -> impl Iterator<Item = (usize, &LogEntry)> {
        let skip = seq.saturating_sub(self.first_seq);
        self.entries
            .iter()
            .enumerate()
            .skip(skip)
            .map(|(offset, entry)| (self.first_seq + offset, entry))
    }

    /// The retained messages as owned values, for the harness assertions.
    pub(super) fn messages(&self, name: &str) -> Vec<RawMessage> {
        self.entries
            .iter()
            .map(|entry| {
                RawMessage::new(name, entry.payload.clone()).with_headers((*entry.headers).clone())
            })
            .collect()
    }

    /// Appends `entry`, evicts what the budget no longer admits, and returns the appended
    /// message's absolute position.
    fn push(&mut self, entry: LogEntry, budget: &Budget) -> usize {
        let seq = self.next_seq();
        self.bytes += entry.payload.len();
        self.entries.push_back(entry);
        if let Budget::Bounded(retention) = budget {
            // The newest message always stays: a payload wider than a byte bound is retained
            // alone, rather than evicted by the same publish that produced it.
            while self.entries.len() > 1 && !retention.admits(self.entries.len(), self.bytes) {
                if let Some(evicted) = self.entries.pop_front() {
                    self.bytes -= evicted.payload.len();
                    self.first_seq += 1;
                }
            }
        }
        seq
    }
}

/// One retained message. The name is the map key, so an entry does not repeat it, and both
/// fields are the shared form the fanout already built: recording a publish is reference-count
/// bumps, not allocations.
pub(super) struct LogEntry {
    payload: Bytes,
    headers: Arc<HeaderMap>,
}

impl LogEntry {
    pub(super) fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    pub(super) fn headers(&self) -> Arc<HeaderMap> {
        Arc::clone(&self.headers)
    }
}
