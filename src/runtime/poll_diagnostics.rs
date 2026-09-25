//! Poll-time diagnostics (`poll-diagnostics` feature): how long each subscription's handler
//! computes between two `.await` points, sampled on every Nth poll.

use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;
use quanta::Clock;
use tracing::warn;

/// The average poll time over which a subscription is reported: past it, the handler holds up the
/// runtime's threads long enough to delay the I/O, the timers and the other subscriptions behind
/// it.
const DEFAULT_THRESHOLD: Duration = Duration::from_micros(100);

/// One poll in 64 is timed by default.
const DEFAULT_SAMPLE_EVERY: NonZeroU32 = NonZeroU32::new(64).expect("64 is not zero");

/// The samples the moving average spans, and the samples it needs before it may warn: one slow
/// first poll (a cold cache, a lazily built client) is not a handler that computes.
const WINDOW: u64 = 16;

/// Upper bounds of the histogram buckets in nanoseconds, 1-2-5 from one microsecond to ten
/// seconds; one more bucket holds everything longer.
pub(crate) const BUCKET_BOUNDS_NS: [u64; 22] = [
    1_000,
    2_000,
    5_000,
    10_000,
    20_000,
    50_000,
    100_000,
    200_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    20_000_000,
    50_000_000,
    100_000_000,
    200_000_000,
    500_000_000,
    1_000_000_000,
    2_000_000_000,
    5_000_000_000,
    10_000_000_000,
];

const BUCKETS: usize = BUCKET_BOUNDS_NS.len() + 1;

/// Samples the time every subscription's handler spends in `poll`, keeps a moving average and a
/// histogram of it per subscription, and warns once the average says a handler computes on the
/// app's runtime.
///
/// With the `poll-diagnostics` feature on, every subscription is sampled: one poll in
/// [`sample_every`](Self::sample_every) (64 by default) is timed with two reads of the CPU's
/// cycle counter. When a subscription's average crosses the [`threshold`](Self::threshold)
/// (100 microseconds by default), the runtime logs one warning naming the subscription, its
/// average and p99 poll time, and the fix: `threads(n)`, which moves the handler onto threads of
/// its own. The average spans the last 16 samples and warns only once it has them.
///
/// A handle is cheap to clone, and every clone reads the same reports. Hand one to
/// [`RustStream::poll_diagnostics`](crate::runtime::RustStream::poll_diagnostics) to set the
/// threshold or the sampling interval, and keep another to read [`reports`](Self::reports) or to
/// export them through the `metrics` or `otel` feature.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream::nonzero;
/// use ruststream::runtime::{AppInfo, PollDiagnostics, RustStream};
///
/// let diagnostics = PollDiagnostics::new()
///     .threshold(Duration::from_micros(250))
///     .sample_every(nonzero!(16u32));
/// let app = RustStream::new(AppInfo::new("svc", "0.1.0")).poll_diagnostics(diagnostics.clone());
/// # let _ = app;
/// assert!(diagnostics.reports().is_empty());
/// ```
#[derive(Clone)]
pub struct PollDiagnostics {
    threshold: Duration,
    sample_every: NonZeroU32,
    registry: Arc<Registry>,
}

/// The subscriptions one set of diagnostics samples, in the order they opened.
#[derive(Default)]
struct Registry {
    subscriptions: Mutex<Vec<Arc<PollStats>>>,
}

impl PollDiagnostics {
    /// Diagnostics with the default threshold (100 microseconds) and sampling interval (one poll
    /// in 64).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::PollDiagnostics;
    ///
    /// let diagnostics = PollDiagnostics::new();
    /// assert_eq!(diagnostics.report("orders"), None);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            threshold: DEFAULT_THRESHOLD,
            sample_every: DEFAULT_SAMPLE_EVERY,
            registry: Arc::default(),
        }
    }

    /// The average poll time over which a subscription is reported.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use ruststream::runtime::PollDiagnostics;
    ///
    /// let diagnostics = PollDiagnostics::new().threshold(Duration::from_millis(1));
    /// # let _ = diagnostics;
    /// ```
    #[must_use]
    pub const fn threshold(mut self, threshold: Duration) -> Self {
        self.threshold = threshold;
        self
    }

    /// Times one poll in `every`. One times every poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::nonzero;
    /// use ruststream::runtime::PollDiagnostics;
    ///
    /// let diagnostics = PollDiagnostics::new().sample_every(nonzero!(8u32));
    /// # let _ = diagnostics;
    /// ```
    #[must_use]
    pub const fn sample_every(mut self, every: NonZeroU32) -> Self {
        self.sample_every = every;
        self
    }

    /// What has been measured on `subscription` so far, or `None` for a subscription that has not
    /// opened.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::PollDiagnostics;
    ///
    /// let diagnostics = PollDiagnostics::new();
    /// assert!(diagnostics.report("orders").is_none());
    /// ```
    #[must_use]
    pub fn report(&self, subscription: &str) -> Option<PollReport> {
        self.lock()
            .iter()
            .find(|stats| &*stats.name == subscription)
            .map(|stats| stats.report())
    }

    /// What has been measured on every subscription that has opened, in the order they opened.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::PollDiagnostics;
    ///
    /// for report in PollDiagnostics::new().reports() {
    ///     println!("{}: {:?} on average", report.subscription(), report.average());
    /// }
    /// ```
    #[must_use]
    pub fn reports(&self) -> Vec<PollReport> {
        self.lock().iter().map(|stats| stats.report()).collect()
    }

    /// The statistics of every subscription, for an exporter that reads the histogram itself.
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    pub(crate) fn stats(&self) -> Vec<Arc<PollStats>> {
        self.lock().clone()
    }

    /// The sampler one subscription dispatches with. A second subscription of the same name
    /// shares the first one's statistics, as it shares its label on every exported metric.
    pub(crate) fn register(&self, subscription: &str) -> PollSampler {
        let mut subscriptions = self.lock();
        let known = subscriptions
            .iter()
            .find(|stats| &*stats.name == subscription)
            .map(Arc::clone);
        let stats = known.unwrap_or_else(|| {
            let stats = Arc::new(PollStats::new(subscription, self.threshold));
            subscriptions.push(Arc::clone(&stats));
            stats
        });
        drop(subscriptions);
        PollSampler::new(stats, self.sample_every)
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Arc<PollStats>>> {
        // A panic while the list is held leaves it whole: every push is a single step.
        self.registry
            .subscriptions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for PollDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PollDiagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollDiagnostics")
            .field("threshold", &self.threshold)
            .field("sample_every", &self.sample_every)
            .field("subscriptions", &self.lock().len())
            .finish_non_exhaustive()
    }
}

/// What has been measured on one subscription: the samples taken, the moving average and the
/// 99th percentile of the time its handler spent in one poll.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::PollDiagnostics;
///
/// let diagnostics = PollDiagnostics::new();
/// if let Some(report) = diagnostics.report("orders") {
///     assert!(report.p99() >= report.average() || report.samples() < 100);
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PollReport {
    subscription: Arc<str>,
    samples: u64,
    average: Duration,
    p99: Duration,
}

impl PollReport {
    /// The subscription measured.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ruststream::runtime::PollDiagnostics;
    /// for report in PollDiagnostics::new().reports() {
    ///     println!("{}", report.subscription());
    /// }
    /// ```
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.subscription
    }

    /// How many polls were timed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ruststream::runtime::PollDiagnostics;
    /// let timed: u64 = PollDiagnostics::new().reports().iter().map(|r| r.samples()).sum();
    /// assert_eq!(timed, 0);
    /// ```
    #[must_use]
    pub const fn samples(&self) -> u64 {
        self.samples
    }

    /// The moving average of the time in one poll, over the last 16 samples.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use ruststream::runtime::PollDiagnostics;
    /// let slow = PollDiagnostics::new()
    ///     .reports()
    ///     .into_iter()
    ///     .filter(|r| r.average() > Duration::from_micros(100));
    /// assert_eq!(slow.count(), 0);
    /// ```
    #[must_use]
    pub const fn average(&self) -> Duration {
        self.average
    }

    /// The 99th percentile of the time in one poll over every sample, read off the histogram.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ruststream::runtime::PollDiagnostics;
    /// for report in PollDiagnostics::new().reports() {
    ///     println!("{}: p99 {:?}", report.subscription(), report.p99());
    /// }
    /// ```
    #[must_use]
    pub const fn p99(&self) -> Duration {
        self.p99
    }
}

/// One subscription's measurements, shared by every loop, worker and thread that dispatches it.
pub(crate) struct PollStats {
    name: Arc<str>,
    threshold_ns: u64,
    /// The counter the samples are read from. One per subscription: reading it takes no global.
    clock: Clock,
    samples: AtomicU64,
    sum_ns: AtomicU64,
    average_ns: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
    /// Set while the average stays over the threshold, so a crossing warns once.
    over: AtomicBool,
}

impl PollStats {
    fn new(name: &str, threshold: Duration) -> Self {
        Self {
            name: Arc::from(name),
            threshold_ns: saturating_nanos(threshold),
            // The first clock of the process calibrates the counter against the monotonic clock
            // (under a millisecond on an invariant TSC, never more than 200 ms); the others copy
            // that calibration. This runs when the subscription opens.
            clock: Clock::new(),
            samples: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            average_ns: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            over: AtomicBool::new(false),
        }
    }

    /// The subscription measured.
    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// The samples taken, their sum in nanoseconds, and the count in each bucket of
    /// [`BUCKET_BOUNDS_NS`] (the last one past every bound).
    pub(crate) fn histogram(&self) -> (u64, u64, [u64; BUCKETS]) {
        let buckets = std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed));
        (
            buckets.iter().sum(),
            self.sum_ns.load(Ordering::Relaxed),
            buckets,
        )
    }

    pub(crate) fn report(&self) -> PollReport {
        let (samples, _, buckets) = self.histogram();
        PollReport {
            subscription: Arc::clone(&self.name),
            samples,
            average: Duration::from_nanos(self.average_ns.load(Ordering::Relaxed)),
            p99: Duration::from_nanos(quantile_ns(&buckets, 0.99)),
        }
    }

    /// Adds one timed poll.
    fn record(&self, ns: u64) {
        let taken = self.samples.fetch_add(1, Ordering::Relaxed) + 1;
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.buckets[bucket_of(ns)].fetch_add(1, Ordering::Relaxed);
        let weight = taken.min(WINDOW);
        let step = |average: u64| {
            Some(if ns >= average {
                average + (ns - average) / weight
            } else {
                average - (average - ns) / weight
            })
        };
        // The closure always answers `Some`, so the update always lands.
        let previous = self
            .average_ns
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, step)
            .unwrap_or_else(|current| current);
        let average = step(previous).unwrap_or(previous);
        if average <= self.threshold_ns {
            if self.over.load(Ordering::Relaxed) {
                self.over.store(false, Ordering::Relaxed);
            }
        } else if taken >= WINDOW && !self.over.swap(true, Ordering::Relaxed) {
            self.warn(average);
        }
    }

    #[cold]
    fn warn(&self, average_ns: u64) {
        let (_, _, buckets) = self.histogram();
        warn!(
            target: "ruststream::dispatch",
            subscription = %self.name,
            average = ?Duration::from_nanos(average_ns),
            p99 = ?Duration::from_nanos(quantile_ns(&buckets, 0.99)),
            threshold = ?Duration::from_nanos(self.threshold_ns),
            "the handler computes between awaits for longer than the threshold, holding up the \
             app runtime's threads; mount the subscription with threads(n) to give it threads \
             of its own",
        );
    }
}

/// The sampling end of one subscription's statistics, carried in its delivery context.
pub(crate) struct PollSampler {
    stats: Arc<PollStats>,
    every: u32,
    /// Polls left until the next timed one. Pooled workers share it: a plain load and store, not
    /// a read-modify-write, so two workers that race skew which poll is timed, never the time.
    countdown: AtomicU32,
}

impl PollSampler {
    fn new(stats: Arc<PollStats>, every: NonZeroU32) -> Self {
        Self {
            stats,
            every: every.get(),
            countdown: AtomicU32::new(every.get()),
        }
    }

    /// A sampler of the same statistics with a countdown of its own, for one dedicated thread.
    pub(crate) fn for_thread(&self) -> Self {
        Self {
            stats: Arc::clone(&self.stats),
            every: self.every,
            countdown: AtomicU32::new(self.every),
        }
    }

    /// `future`, with every Nth poll of it timed.
    pub(crate) const fn observe<F>(&self, future: F) -> Sampled<'_, F> {
        Sampled {
            future,
            sampler: self,
        }
    }

    /// Whether this poll is the one to time.
    #[inline]
    fn tick(&self) -> bool {
        let left = self.countdown.load(Ordering::Relaxed);
        if left > 1 {
            self.countdown.store(left - 1, Ordering::Relaxed);
            false
        } else {
            self.countdown.store(self.every, Ordering::Relaxed);
            true
        }
    }
}

impl fmt::Debug for PollSampler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollSampler")
            .field("subscription", &self.stats.name)
            .field("every", &self.every)
            .finish_non_exhaustive()
    }
}

pin_project! {
    /// A handler's future with every Nth poll timed.
    pub(crate) struct Sampled<'a, F> {
        #[pin]
        future: F,
        sampler: &'a PollSampler,
    }
}

impl<F: Future> Future for Sampled<'_, F> {
    type Output = F::Output;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if !this.sampler.tick() {
            return this.future.poll(cx);
        }
        let stats = &*this.sampler.stats;
        let start = stats.clock.raw();
        let polled = this.future.poll(cx);
        let end = stats.clock.raw();
        stats.record(stats.clock.delta_as_nanos(start, end));
        polled
    }
}

fn saturating_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn bucket_of(ns: u64) -> usize {
    BUCKET_BOUNDS_NS.partition_point(|&bound| bound < ns)
}

/// The `q` quantile read off the histogram, interpolated linearly inside the bucket it falls in;
/// past the last bound it is that bound.
// The counts and bounds stay far below 2^52, where an `f64` holds them exactly.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn quantile_ns(buckets: &[u64; BUCKETS], q: f64) -> u64 {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return 0;
    }
    let rank = (total as f64 * q).ceil().max(1.0);
    let mut below = 0u64;
    for (i, &count) in buckets.iter().enumerate() {
        if count > 0 && (below + count) as f64 >= rank {
            let lower = if i == 0 { 0 } else { BUCKET_BOUNDS_NS[i - 1] };
            let Some(&upper) = BUCKET_BOUNDS_NS.get(i) else {
                return lower;
            };
            let within = (rank - below as f64) / count as f64;
            return lower + ((upper - lower) as f64 * within) as u64;
        }
        below += count;
    }
    BUCKET_BOUNDS_NS[BUCKET_BOUNDS_NS.len() - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_a_hundred_microseconds_and_one_poll_in_64() {
        let diagnostics = PollDiagnostics::new();
        assert_eq!(diagnostics.threshold, Duration::from_micros(100));
        assert_eq!(diagnostics.sample_every.get(), 64);
        assert_eq!(
            format!("{diagnostics:?}"),
            format!("{:?}", PollDiagnostics::default())
        );
    }

    #[test]
    fn a_sampler_times_every_nth_poll() {
        let sampler = PollDiagnostics::new()
            .sample_every(NonZeroU32::new(3).expect("three"))
            .register("s");
        let timed: Vec<bool> = (0..7).map(|_| sampler.tick()).collect();
        assert_eq!(timed, [false, false, true, false, false, true, false]);
        let own = sampler.for_thread();
        assert!(!own.tick());
        assert!(format!("{sampler:?}").contains("\"s\""));
    }

    #[test]
    fn a_name_registered_twice_shares_its_statistics() {
        let diagnostics = PollDiagnostics::new();
        let first = diagnostics.register("s");
        let second = diagnostics.register("s");
        first.stats.record(1_500);
        second.stats.record(1_500);
        assert_eq!(diagnostics.reports().len(), 1);
        assert_eq!(diagnostics.report("s").map(|r| r.samples()), Some(2));
    }

    #[test]
    fn the_average_moves_over_the_window() {
        let stats = PollStats::new("s", Duration::from_secs(1));
        stats.record(1_000);
        assert_eq!(stats.report().average(), Duration::from_nanos(1_000));
        stats.record(3_000);
        assert_eq!(stats.report().average(), Duration::from_nanos(2_000));
        for _ in 0..1_000 {
            stats.record(10_000);
        }
        // Old samples weigh less and less: the average reaches the new level.
        assert!(stats.report().average() > Duration::from_nanos(9_900));
        stats.record(0);
        assert_eq!(stats.report().average(), Duration::from_nanos(9_361));
    }

    #[test]
    fn the_quantile_is_read_off_the_buckets() {
        let mut buckets = [0u64; BUCKETS];
        assert_eq!(quantile_ns(&buckets, 0.99), 0);
        // 100 samples between 50 and 100 microseconds: the p99 is near the top of the bucket.
        buckets[bucket_of(70_000)] = 100;
        assert_eq!(bucket_of(70_000), 6);
        assert_eq!(quantile_ns(&buckets, 0.99), 99_500);
        assert_eq!(quantile_ns(&buckets, 0.5), 75_000);
        // Past the last bound, the last bound.
        let mut long = [0u64; BUCKETS];
        long[bucket_of(u64::MAX)] = 1;
        assert_eq!(bucket_of(u64::MAX), BUCKETS - 1);
        assert_eq!(quantile_ns(&long, 0.99), 10_000_000_000);
        assert_eq!(bucket_of(1_000), 0);
        assert_eq!(bucket_of(1_001), 1);
        assert_eq!(saturating_nanos(Duration::MAX), u64::MAX);
    }
}
