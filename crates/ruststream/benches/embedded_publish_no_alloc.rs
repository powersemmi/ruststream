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
//! The embedded app without `alloc` with a handler that publishes: what a publish of the
//! framework adds to a delivery, and that neither the start nor a delivery allocates.
//!
//! The app is the one `embedded_idle_no_alloc` measures alone on the app, with a handler that
//! publishes once per delivery: through an `Out` slot, through the same slot with a typed header,
//! or as its reply. Nothing subscribes to the destination, so the bus takes each publish without
//! writing it anywhere, and the difference to `embedded_idle_no_alloc/alone_on_the_app` is what
//! the framework's publish path costs. A run is measured three times, over one delivery, over
//! [`MESSAGES`] and over twice as many, and the per-delivery figure is the difference of the two
//! longer totals divided by [`MESSAGES`]. The bus is the board's: a `static` of fixed size, one
//! per run, because gungraun runs every benchmark in a process of its own. Nothing here is built
//! with `testing` or `alloc`.
//!
//! A path that published nothing would measure as a faster one, so the scenarios are also the
//! test `embedded_publish_no_alloc_check`, which runs them over a bus whose destinations have a
//! reader and holds every message to arriving; `just bench` measures the stand only after it
//! passed. The check is a binary of its own: code compiled beside the measured code changes how
//! the measured code is inlined, and with it the count.

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
    use ruststream::memory::local::{LocalMemoryBus, LocalMemoryPublish};
    use ruststream::runtime::{Control, EmbeddedApp, Mounts};
    use ruststream::{Broker, OutgoingMessage, Publisher};

    use scenario::{Bus, HANDLED, Publish, Relay, answer, relay, relay_traced};

    #[path = "embedded_publish_no_alloc/scenario.rs"]
    mod scenario;

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

    fn through_a_slot_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
        EmbeddedApp::new().with_broker(bus, |b| b.include(relay).out(Relay, LocalMemoryPublish))
    }

    fn with_a_header_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
        EmbeddedApp::new()
            .with_broker(bus, |b| b.include(relay_traced).out(Relay, LocalMemoryPublish))
    }

    fn as_a_reply_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
        EmbeddedApp::new()
            .with_broker(bus, |b| b.include(answer).out_reply(LocalMemoryPublish))
    }

    /// What a scenario runs: the bus, how many deliveries to send and how the handler publishes.
    struct Stand {
        bus: Bus,
        deliveries: usize,
        publish: Publish,
    }

    fn stand(deliveries: usize, publish: Publish) -> Stand {
        HANDLED.with(|handled| handled.set(0));
        Stand {
            bus: Bus::new(&BUS),
            deliveries,
            publish,
        }
    }

    /// Starts the app, sends the deliveries one at a time and stops the app once the last is handled.
    ///
    /// The publisher waits for each delivery to be handled before it sends the next, so the
    /// subscription never queues and every wake of the app carries exactly one delivery.
    fn start_and_drain(stand: Stand) {
        let Stand {
            bus,
            deliveries,
            publish,
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
                match publish {
                    Publish::ThroughASlot => through_a_slot_app(bus).run(&control).await,
                    Publish::WithAHeader => with_a_header_app(bus).run(&control).await,
                    Publish::AsAReply => as_a_reply_app(bus).run(&control).await,
                }
            };
            let (ran, ()) = join(run, drive).await;
            ran.expect("the app stops cleanly");
        });
        assert_eq!(
            HANDLED.with(Cell::get),
            deliveries,
            "the subscription handled every delivery"
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
    const REGION: &str = "*embedded_publish_no_alloc::measure*";

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
    #[bench::first(stand(1, Publish::ThroughASlot))]
    #[bench::base(stand(MESSAGES, Publish::ThroughASlot))]
    #[bench::twice(stand(2 * MESSAGES, Publish::ThroughASlot))]
    fn through_a_slot(stand: Stand) {
        measure(|| start_and_drain(stand));
    }

    #[library_benchmark(config = config(COLD_BLOCKS))]
    #[bench::first(stand(1, Publish::WithAHeader))]
    #[bench::base(stand(MESSAGES, Publish::WithAHeader))]
    #[bench::twice(stand(2 * MESSAGES, Publish::WithAHeader))]
    fn through_a_slot_with_a_header(stand: Stand) {
        measure(|| start_and_drain(stand));
    }

    #[library_benchmark(config = config(COLD_BLOCKS))]
    #[bench::first(stand(1, Publish::AsAReply))]
    #[bench::base(stand(MESSAGES, Publish::AsAReply))]
    #[bench::twice(stand(2 * MESSAGES, Publish::AsAReply))]
    fn as_a_reply(stand: Stand) {
        measure(|| start_and_drain(stand));
    }

    library_benchmark_group!(
        name = embedded_publish_no_alloc;
        benchmarks = through_a_slot, through_a_slot_with_a_header, as_a_reply
    );
    main!(library_benchmark_groups = embedded_publish_no_alloc);
}
