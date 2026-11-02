// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::future_not_send,
    // The app's future lives on the stack of the measured run, as a device keeps it; boxing it
    // would put an allocation in the number the floor holds.
    clippy::large_futures
)]
//! The embedded app without `alloc` with idle subscriptions beside a busy one: what a delivery costs
//! when fifteen other subscriptions of the app wait for theirs, and that neither the start nor a
//! delivery allocates.
//!
//! The app is a service's own: `EmbeddedApp` over the local memory bus, one task that joins every
//! subscription, run on embassy's `block_on`. One subscription receives the deliveries and the
//! others never do, so the difference between the two scenarios divided by fifteen is the price of
//! an idle subscription per delivery. A run is measured three times, over one delivery, over
//! [`MESSAGES`] and over twice as many, and the per-delivery figure is the difference of the two
//! longer totals divided by [`MESSAGES`], as in the tokio scenarios under `benches/` of the
//! facade. The bus is the board's: a `static` of fixed size, one per run, because gungraun runs
//! every benchmark in a process of its own. Nothing here is built with `testing` or `alloc`.

// The scenario exists only in a build without `alloc`; a lint leg that builds every target with
// `alloc` on gets an empty benchmark instead.
macro_rules! without_alloc {
    ($($item:item)*) => { $( #[cfg(not(ruststream_alloc))] $item )* };
}

#[cfg(ruststream_alloc)]
fn main() {}

without_alloc! {
    use std::cell::Cell;
    use std::hint::black_box;

    use embassy_futures::block_on;
    use embassy_futures::join::join;
    use embassy_futures::yield_now;
    use gungraun::{
        Callgrind, Dhat, DhatMetric, EntryPoint, LibraryBenchmarkConfig, library_benchmark,
        library_benchmark_group, main,
    };
    use ruststream::memory::local::LocalMemoryBus;
    use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
    use ruststream::runtime::{Control, EmbeddedApp, HandlerOutcome, Mounts};
    use ruststream::{Broker, Deserialized, OutgoingMessage, Publisher, subscriber};

    /// The bus: each subscription holds four messages, and there is room for sixteen subscriptions.
    type Bus = LocalMemoryBroker<4, 64, 16>;

    /// The board's bus.
    static BUS: LocalMemoryBus<4, 64, 16> = LocalMemoryBus::new();

    /// Deliveries of the longest run: `RUSTSTREAM_BENCH_MESSAGES` at build time, as in the facade's
    /// scenarios, or a thousand.
    const MESSAGES: usize = match option_env!("RUSTSTREAM_BENCH_MESSAGES") {
        None => 1_000,
        Some(text) => parse(text.as_bytes()),
    };

    /// Reads a positive decimal count at build time.
    const fn parse(digits: &[u8]) -> usize {
        assert!(
            !digits.is_empty(),
            "RUSTSTREAM_BENCH_MESSAGES must be a positive count"
        );
        let mut count = 0;
        let mut at = 0;
        while at < digits.len() {
            assert!(
                digits[at].is_ascii_digit(),
                "RUSTSTREAM_BENCH_MESSAGES must be a positive count"
            );
            count = count * 10 + (digits[at] - b'0') as usize;
            at += 1;
        }
        assert!(
            count > 0,
            "RUSTSTREAM_BENCH_MESSAGES must be a positive count"
        );
        count
    }

    thread_local! {
        /// Deliveries the busy subscription handled.
        static HANDLED: Cell<usize> = const { Cell::new(0) };
    }

    /// One reading as its bytes.
    #[derive(Deserialized)]
    struct Reading<'a>(#[allow(dead_code)] &'a [u8]);

    #[subscriber(LocalMemorySource::new("busy"))]
    async fn busy(reading: &Reading<'_>) -> HandlerOutcome {
        black_box(reading);
        HANDLED.with(|handled| handled.set(handled.get() + 1));
        HandlerOutcome::ack()
    }

    /// One handler per idle subscription: its subject is never published to.
    macro_rules! idle {
        ($($name:ident $subject:literal),* $(,)?) => {
            $(
                #[subscriber(LocalMemorySource::new($subject))]
                async fn $name(reading: &Reading<'_>) -> HandlerOutcome {
                    black_box(reading);
                    HandlerOutcome::ack()
                }
            )*
        };
    }

    idle!(
        i0 "idle/0", i1 "idle/1", i2 "idle/2", i3 "idle/3", i4 "idle/4", i5 "idle/5", i6 "idle/6",
        i7 "idle/7", i8 "idle/8", i9 "idle/9", i10 "idle/10", i11 "idle/11", i12 "idle/12",
        i13 "idle/13", i14 "idle/14",
    );

    fn alone(bus: Bus) -> EmbeddedApp<impl Mounts> {
        EmbeddedApp::new().with_broker(bus, |b| b.include(busy))
    }

    fn beside_fifteen(bus: Bus) -> EmbeddedApp<impl Mounts> {
        EmbeddedApp::new().with_broker(bus, |b| {
            b.include(busy)
                .include(i0)
                .include(i1)
                .include(i2)
                .include(i3)
                .include(i4)
                .include(i5)
                .include(i6)
                .include(i7)
                .include(i8)
                .include(i9)
                .include(i10)
                .include(i11)
                .include(i12)
                .include(i13)
                .include(i14)
        })
    }

    /// What a scenario runs: the bus and how many deliveries to send.
    struct Stand {
        bus: Bus,
        deliveries: usize,
        fifteen_idle: bool,
    }

    fn stand(deliveries: usize, fifteen_idle: bool) -> Stand {
        HANDLED.with(|handled| handled.set(0));
        Stand {
            bus: Bus::new(&BUS),
            deliveries,
            fifteen_idle,
        }
    }

    /// Starts the app, sends the deliveries one at a time and stops the app once the last is handled.
    ///
    /// The publisher waits for each delivery to be handled before it sends the next, so the busy
    /// subscription never queues and every wake of the app carries exactly one delivery.
    fn start_and_drain(stand: Stand) {
        let Stand {
            bus,
            deliveries,
            fifteen_idle,
        } = stand;
        let control = Control::new();
        block_on(async {
            let publisher = bus
                .connect()
                .await
                .expect("the local bus connects")
                .publisher();
            let drive = async {
                // Let the app start and park every subscription.
                for _ in 0..4 {
                    yield_now().await;
                }
                for done in 0..deliveries {
                    publisher
                        .publish(OutgoingMessage::with_payload("busy", &b"x"[..]), None)
                        .await
                        .expect("the bus takes the delivery");
                    while HANDLED.with(Cell::get) <= done {
                        yield_now().await;
                    }
                }
                control.stop();
            };
            let run = async {
                if fifteen_idle {
                    beside_fifteen(bus).run(&control).await
                } else {
                    alone(bus).run(&control).await
                }
            };
            let (ran, ()) = join(run, drive).await;
            ran.expect("the app stops cleanly");
        });
        assert_eq!(
            HANDLED.with(Cell::get),
            deliveries,
            "the busy subscription handled every delivery"
        );
    }

    /// The measured region: everything this runs is counted, nothing around it is.
    ///
    /// It is a frame of its own whose name appears nowhere else in the binary, and it outlives the
    /// body, as the facade's `measure` does.
    #[inline(never)]
    fn measure<T>(body: impl FnOnce() -> T) -> T {
        black_box(body())
    }

    /// The frame both tools are pointed at.
    const REGION: &str = "*embedded_idle_no_alloc::measure*";

    /// The app starts and runs without a heap: the limit is zero allocations in the measured region.
    const COLD_BLOCKS: u64 = 0;

    /// The configuration: callgrind inside the region, DHAT held to zero allocations over a run of any
    /// length.
    fn config(cold_blocks: u64) -> LibraryBenchmarkConfig {
        let mut callgrind = Callgrind::with_args([
            "--collect-atstart=no",
            &format!("--toggle-collect={REGION}"),
        ]);
        callgrind.entry_point(EntryPoint::None);
        let mut dhat = Dhat::with_args(["--num-callers=128"]);
        dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
        dhat.hard_limits([(DhatMetric::TotalBlocks, cold_blocks)]);
        let mut config = LibraryBenchmarkConfig::default();
        config.tool(callgrind).tool(dhat);
        config
    }

    #[library_benchmark(config = config(COLD_BLOCKS))]
    #[bench::first(stand(1, true))]
    #[bench::base(stand(MESSAGES, true))]
    #[bench::twice(stand(2 * MESSAGES, true))]
    fn beside_fifteen_idle(stand: Stand) {
        measure(|| start_and_drain(stand));
    }

    #[library_benchmark(config = config(COLD_BLOCKS))]
    #[bench::first(stand(1, false))]
    #[bench::base(stand(MESSAGES, false))]
    #[bench::twice(stand(2 * MESSAGES, false))]
    fn alone_on_the_app(stand: Stand) {
        measure(|| start_and_drain(stand));
    }

    library_benchmark_group!(name = embedded_idle_no_alloc; benchmarks = beside_fifteen_idle, alone_on_the_app);
    main!(library_benchmark_groups = embedded_idle_no_alloc);
}
