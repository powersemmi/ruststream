//! Research (#417): the variant a pool runs, read once from the environment when it starts.
//!
//! - `RUSTSTREAM_RESEARCH_FEED`: `handoff` (the default, #438), `queue`, `rings` or `whole`;
//! - `RUSTSTREAM_RESEARCH_PLACE`: `runtime` (the default) or `pinned`, a thread per worker;
//! - `RUSTSTREAM_RESEARCH_CAP`: the shared queue's capacity, `2n` by default;
//! - `RUSTSTREAM_RESEARCH_RING`: each ring's capacity, 2 by default;
//! - `RUSTSTREAM_RESEARCH_SPIN_US`: how long a worker that found its queue empty spins before it
//!   parks, 0 by default;
//! - `RUSTSTREAM_RESEARCH_SPIN`: `pause` (the default, a busy loop) or `yield` (`yield_now`);
//! - `RUSTSTREAM_RESEARCH_CHUNK`: `1` makes a ring worker take everything available as one chunk
//!   that it frees once the whole chunk is handled;
//! - `RUSTSTREAM_RESEARCH_PICK`: `rr` (the default) spreads deliveries over the rings round-robin
//!   and waits on the chosen ring when it is full, `least` picks the ring with the most room.

use std::env;
use std::hint::spin_loop;
use std::time::{Duration, Instant};

/// How the loop feeds its workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Feed {
    Handoff,
    Queue,
    Rings,
    Whole,
}

/// What a worker does between finding its queue empty and parking.
#[derive(Debug, Clone, Copy)]
pub(super) enum Spin {
    None,
    /// Busy-waits up to the budget.
    Pause(Duration),
    /// Yields to its runtime up to this many times.
    Yield(u32),
}

impl Spin {
    /// Waits for `ready` within the budget; `true` when it turned ready.
    pub(super) async fn wait(self, ready: impl Fn() -> bool) -> bool {
        match self {
            Self::None => false,
            Self::Pause(budget) => {
                let start = Instant::now();
                loop {
                    for _ in 0..64 {
                        if ready() {
                            return true;
                        }
                        spin_loop();
                    }
                    if start.elapsed() >= budget {
                        return ready();
                    }
                }
            }
            Self::Yield(times) => {
                for _ in 0..times {
                    tokio::task::yield_now().await;
                    if ready() {
                        return true;
                    }
                }
                false
            }
        }
    }
}

/// The variant, as the environment names it.
#[derive(Debug, Clone, Copy)]
pub(super) struct Knobs {
    pub(super) feed: Feed,
    pub(super) pinned: bool,
    capacity: Option<usize>,
    pub(super) ring: usize,
    pub(super) spin: Spin,
    pub(super) chunk: bool,
    pub(super) least: bool,
}

impl Knobs {
    pub(super) fn from_env() -> Self {
        let var = |name: &str| env::var(name).ok();
        let number = |name: &str| var(name).and_then(|v| v.parse::<u64>().ok());
        let feed = match var("RUSTSTREAM_RESEARCH_FEED").as_deref() {
            Some("queue") => Feed::Queue,
            Some("rings") => Feed::Rings,
            Some("whole") => Feed::Whole,
            _ => Feed::Handoff,
        };
        let spin_us = number("RUSTSTREAM_RESEARCH_SPIN_US").unwrap_or(0);
        let spin = match (spin_us, var("RUSTSTREAM_RESEARCH_SPIN").as_deref()) {
            (0, _) => Spin::None,
            // A yield costs about a microsecond on an idle current-thread runtime (it polls the
            // driver once), so the budget converts to a count.
            (us, Some("yield")) => Spin::Yield(u32::try_from(us).unwrap_or(u32::MAX)),
            (us, _) => Spin::Pause(Duration::from_micros(us)),
        };
        Self {
            feed,
            pinned: var("RUSTSTREAM_RESEARCH_PLACE").as_deref() == Some("pinned"),
            capacity: number("RUSTSTREAM_RESEARCH_CAP").and_then(|v| usize::try_from(v).ok()),
            ring: number("RUSTSTREAM_RESEARCH_RING")
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(2)
                .max(1),
            spin,
            chunk: var("RUSTSTREAM_RESEARCH_CHUNK").as_deref() == Some("1"),
            least: var("RUSTSTREAM_RESEARCH_PICK").as_deref() == Some("least"),
        }
    }

    /// The shared queue's capacity for `count` workers.
    pub(super) fn capacity(&self, count: usize) -> usize {
        self.capacity.unwrap_or(2 * count).max(1)
    }
}
