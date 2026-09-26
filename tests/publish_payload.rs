//! What a broker is handed on the publish path: the destination, the buffer the framework
//! produced (or a borrow of bytes it does not own), and the header map the publish filled.
//!
//! A transport whose client wants owned bytes or an owned map should take them rather than copy
//! them, and should pay only for the form it asks for. The tests below read the forms through the
//! public accessors, compare buffer addresses - content equality cannot tell a hand-over from a
//! copy - and count this thread's allocations around a publish.
#![cfg(all(feature = "macros", feature = "json"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use ruststream::codec::{Codec, JsonCodec};
#[cfg(feature = "memory")]
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{Outgoing as OutgoingView, PublishExt, PublishIdentity, PublishPipeline};
use ruststream::{
    BytesMut, HeaderMap, Lend, Outgoing, OutgoingMessage, Publisher, Serialized, Take,
};
use serde::Serialize;

/// Counts this thread's allocations, so the cost of one publish can be read off directly. A
/// thread-local count rather than a global one: the test harness runs other tests beside this
/// one, and their allocations are none of this measurement's business.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// A message under a destination of its own, so a publish names nothing.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "orders.created")]
struct OrderCreated {
    id: u64,
}

/// A value that already holds its bytes: the lane lends them, and no codec runs.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "orders.audit")]
struct Audit(Vec<u8>);

/// A value that computes its bytes into the buffer the publish path hands it, recording where
/// it wrote them so a test can tell a hand-over from a copy.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "ticks")]
#[wire(encode = write_tick)]
struct Tick {
    at: AtomicUsize,
}

/// The value's own encoder, in the shape `#[wire(encode = ..)]` takes. It cannot fail, so it
/// answers nothing.
fn write_tick(tick: &Tick, buf: &mut BytesMut) {
    buf.extend_from_slice(&[1, 2, 3, 4]);
    tick.at.store(buf.as_ptr() as usize, Ordering::Relaxed);
}

/// What a transport that declared [`Take`] does with the buffer it is handed.
#[derive(Clone, Copy)]
enum Claim {
    /// Reads it and forgets it, as a transport that writes into a buffer of its own does.
    Read,
    /// Takes it as the vector its client wants.
    Vec,
    /// Takes it as the `Bytes` its client wants.
    Bytes,
}

/// What the broker saw. It holds no owned buffer on purpose: the probe must not allocate, or the
/// counts below would measure the test rather than the publish.
struct Seen {
    /// Where the bytes `payload()` answered live.
    read_at: usize,
    /// Where the bytes the transport took ended up, or the read address where it took none.
    taken_at: usize,
    /// How long the payload is.
    len: usize,
}

/// A publisher whose client keeps the payload, and reports what it saw without allocating
/// anything of its own.
struct Probe {
    claim: Claim,
    seen: Mutex<Option<Seen>>,
}

impl Probe {
    fn taking(claim: Claim) -> Self {
        Self {
            claim,
            seen: Mutex::new(None),
        }
    }

    /// What the last publish handed it.
    fn seen(&self) -> Seen {
        self.seen
            .lock()
            .expect("probe mutex poisoned")
            .take()
            .expect("nothing was published")
    }
}

impl Publisher for Probe {
    // The client keeps the bytes, so the publish hands the buffer over.
    type Payload = Take;
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        let read_at = msg.payload().as_ptr() as usize;
        let len = msg.payload().len();
        let payload = msg.into_payload();
        // The taken buffer is released here rather than kept: the address is the whole of what
        // the assertions read, and holding it would outlive the count.
        let taken_at = match self.claim {
            Claim::Read => read_at,
            Claim::Vec => {
                let taken = Vec::from(payload);
                let at = taken.as_ptr() as usize;
                drop(taken);
                at
            }
            Claim::Bytes => {
                let taken = payload.freeze();
                let at = taken.as_ptr() as usize;
                drop(taken);
                at
            }
        };
        *self.seen.lock().expect("probe mutex poisoned") = Some(Seen {
            read_at,
            taken_at,
            len,
        });
        ready(Ok(()))
    }
}

/// A publisher that only reads the payload, as every transport that packs it into a frame of its
/// own does: it declares [`Lend`] and is handed the bytes where they already are.
#[derive(Default)]
struct Reading(Mutex<Option<Seen>>);

impl Reading {
    /// What the last publish handed it.
    fn seen(&self) -> Seen {
        self.0
            .lock()
            .expect("reading probe mutex poisoned")
            .take()
            .expect("nothing was published")
    }
}

impl Publisher for Reading {
    type Payload = Lend;
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        let read_at = msg.payload().as_ptr() as usize;
        let len = msg.payload().len();
        // The form itself, with no arm to match on: a lending publisher is handed a slice and
        // there is nothing else it could be handed.
        let lent: &[u8] = msg.into_payload();
        *self.0.lock().expect("reading probe mutex poisoned") = Some(Seen {
            read_at,
            taken_at: lent.as_ptr() as usize,
            len,
        });
        ready(Ok(()))
    }
}

/// The one header a publish carries below: what a transform on a publish position stamps, and
/// what every broker crate used to clone the whole map for.
const STAMP: &str = "x-stamp";

/// A publisher that keeps everything it was handed, for the assertions about contents.
#[derive(Default)]
struct Keeper(Mutex<Option<(String, Vec<u8>, HeaderMap)>>);

impl Keeper {
    /// The destination, the payload and the headers of the last publish.
    fn kept(&self) -> (String, Vec<u8>, HeaderMap) {
        self.0
            .lock()
            .expect("keeper mutex poisoned")
            .take()
            .expect("nothing was published")
    }
}

impl Publisher for Keeper {
    type Payload = Take;
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        // What a transport whose client takes owned parts does: one move for all three.
        let (name, payload, headers) = msg.into_parts();
        *self.0.lock().expect("keeper mutex poisoned") =
            Some((name.to_owned(), Vec::from(payload), headers));
        ready(Ok(()))
    }
}

/// What the broker saw of the headers. It keeps no copy of the map on purpose: the probe must
/// not allocate, or the counts below would measure the test rather than the publish.
struct SeenHeaders {
    /// How many entries arrived.
    len: usize,
    /// Where the bytes of the stamped value live.
    value_at: usize,
}

/// A publisher that takes the map and reports what it is, without allocating anything of its own.
#[derive(Default)]
struct Taker(Mutex<Option<SeenHeaders>>);

impl Taker {
    /// What the last publish handed it.
    fn seen(&self) -> SeenHeaders {
        self.0
            .lock()
            .expect("taker mutex poisoned")
            .take()
            .expect("nothing was published")
    }
}

impl Publisher for Taker {
    // The map is what this one is about, so the payload travels the free way.
    type Payload = Lend;
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        let (_, _, headers) = msg.into_parts();
        let seen = SeenHeaders {
            len: headers.len(),
            // Read, not taken: `get_shared` hands back a counted handle, and claiming one out of
            // a map nothing has cloned promotes the value - an allocation of this probe's own,
            // inside the region the test counts.
            value_at: headers
                .get(STAMP)
                .map_or(0, |value| value.as_ptr() as usize),
        };
        *self.0.lock().expect("taker mutex poisoned") = Some(seen);
        ready(Ok(()))
    }
}

#[tokio::test]
async fn a_value_that_carries_its_own_bytes_is_lent_to_a_reading_broker() {
    let audit = Audit(br#"{"seen":true}"#.to_vec());
    let publisher = Reading::default();
    publisher
        .message(&audit)
        .publish()
        .await
        .expect("the probe never refuses");

    let seen = publisher.seen();
    assert_eq!(
        seen.read_at,
        audit.0.as_ptr() as usize,
        "a transport that reads the payload is handed the value's own buffer, uncopied",
    );
}

#[tokio::test]
async fn a_value_that_carries_its_own_bytes_is_copied_for_a_taking_broker() {
    let audit = Audit(br#"{"seen":true}"#.to_vec());
    let publisher = Probe::taking(Claim::Read);
    publisher
        .message(&audit)
        .publish()
        .await
        .expect("the probe never refuses");

    let seen = publisher.seen();
    assert_ne!(
        seen.read_at,
        audit.0.as_ptr() as usize,
        "a transport that keeps the payload cannot keep bytes the value owns, so it is handed \
         a buffer of its own",
    );
    assert_eq!(seen.len, audit.0.len(), "holding the same bytes");
}

#[tokio::test]
async fn a_transport_takes_the_destination_the_payload_and_the_headers_in_one_move() {
    let mut headers = HeaderMap::new();
    headers.insert(STAMP, b"1".to_vec());

    let publisher = Keeper::default();
    publisher
        .message(&OrderCreated { id: 7 })
        .with_headers(headers)
        .publish()
        .await
        .expect("the keeper never refuses");

    let (name, payload, headers) = publisher.kept();
    assert_eq!(
        name, "orders.created",
        "the destination the message declared"
    );
    assert_eq!(
        payload,
        JsonCodec
            .encode(&OrderCreated { id: 7 })
            .expect("encodable"),
        "the payload reaches the broker byte for byte",
    );
    assert_eq!(
        headers.get(STAMP),
        Some(b"1".as_slice()),
        "and the map arrives whole, without the transport reading it out entry by entry",
    );
}

/// The framework makes no copy of the map on its way out: the terminal of the publish pipeline
/// moves what the transforms filled into the message that leaves, and the transport takes it from
/// there.
#[tokio::test]
async fn handing_the_header_map_to_the_broker_allocates_nothing() {
    let pipeline = PublishIdentity;
    let taker = Taker::default();
    let mut out = OutgoingView::new("events", b"{}".as_slice());
    out.headers_mut().insert(STAMP, b"1".to_vec());
    let stamped_at = out.headers().get(STAMP).expect("just stamped").as_ptr() as usize;

    let before = allocations();
    pipeline
        .run(&mut out, &taker, None)
        .await
        .expect("the taker never refuses");
    let spent = allocations() - before;

    let seen = taker.seen();
    assert_eq!(
        spent, 0,
        "the map travels into the broker's message; a copy of it would cost the table and a \
         reference block per entry",
    );
    assert_eq!(
        (seen.len, seen.value_at),
        (1, stamped_at),
        "and it arrives whole, over the buffer the stamp wrote",
    );
}

/// What the broker crates need before they adapt: a form costs what it costs, and only the
/// transport that asks for one pays. A publish that hands the buffer over is the floor; a vector
/// costs nothing over it, because the buffer the codec wrote is the vector; a `Bytes` costs the
/// one block that makes ownership shareable.
#[tokio::test]
async fn only_a_transport_that_takes_the_buffer_pays_for_it() {
    // One publish before the count: a run's first encode grows buffers that later ones do not.
    let counted = async |claim| {
        let publisher = Probe::taking(claim);
        let value = OrderCreated { id: 7 };
        publisher
            .message(&value)
            .publish()
            .await
            .expect("the probe never refuses");
        publisher.seen();

        let before = allocations();
        publisher
            .message(&value)
            .publish()
            .await
            .expect("the probe never refuses");
        let spent = allocations() - before;
        (spent, publisher.seen())
    };

    let (handed_over, read) = counted(Claim::Read).await;
    let (as_vec, took_vec) = counted(Claim::Vec).await;
    let (as_bytes, took_bytes) = counted(Claim::Bytes).await;

    assert_eq!(
        as_vec, handed_over,
        "the codec's buffer is already the vector, so taking it as one allocates nothing",
    );
    assert_eq!(
        took_vec.taken_at, took_vec.read_at,
        "the vector is the buffer the codec wrote, not a copy of it",
    );
    assert_eq!(
        as_bytes,
        handed_over + 1,
        "a `Bytes` needs the shared ownership block, and that block is all it costs",
    );
    assert_eq!((read.len, took_vec.len, took_bytes.len), (8, 8, 8));
}

/// A value that computes its bytes writes them once, into the buffer the publish path handed
/// it, and a transport that keeps the payload is handed that buffer rather than a copy of it.
#[tokio::test]
async fn a_computed_value_hands_over_the_buffer_it_wrote() {
    let tick = Tick {
        at: AtomicUsize::new(0),
    };
    let publisher = Probe::taking(Claim::Read);
    publisher
        .message(&tick)
        .publish()
        .await
        .expect("the probe never refuses");

    let seen = publisher.seen();
    assert_eq!(
        seen.read_at,
        tick.at.load(Ordering::Relaxed),
        "the value wrote into the publish path's buffer, and that buffer is what leaves",
    );
    assert_eq!(seen.len, 4, "holding what the value wrote");
}

/// What a broker does with the map, measured on the one broker this crate ships. A header costs
/// the publish what writing that header costs, and nothing on top: the map reaches the bus as
/// the publish filled it.
#[cfg(feature = "memory")]
#[tokio::test]
async fn a_header_costs_the_in_memory_broker_only_what_writing_it_costs() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let counted = async move |carried: usize| {
        let before = allocations();
        let mut headers = HeaderMap::new();
        for _ in 0..carried {
            headers.insert(STAMP, b"1".to_vec());
        }
        let msg = OutgoingMessage::new("orders.created", b"{}").with_headers(headers);
        publisher
            .publish(msg, None)
            .await
            .expect("the bus is up for the length of this test");
        allocations() - before
    };

    // One publish before the counts: the bus grows its own tables on the first message.
    counted(0).await;
    let bare = counted(0).await;
    let stamped = counted(1).await;

    // What the entry costs on its own, so the assertion names no magic number.
    let writing = {
        let before = allocations();
        let mut headers = HeaderMap::new();
        headers.insert(STAMP, b"1".to_vec());
        allocations() - before
    };

    assert_eq!(
        stamped - bare,
        writing,
        "the map travels into the bus; a copy of it on the way would add the table and a \
         reference block per entry",
    );
}

/// A pooled subscription keeps the encode buffers its workers use: a worker hands its buffer
/// back when it finishes, the next one takes it, and a reply through a reading transport costs
/// the pool nothing per delivery once its buffers exist. The measurement is a difference of
/// differences: what a pool adds over the sequential loop for replies may not exceed what it
/// adds for acknowledgements, so the harness's own bookkeeping of a reply and the pool's own
/// task per delivery both cancel out.
#[cfg(all(feature = "memory", feature = "testing"))]
mod pool {
    use super::*;
    use ruststream::memory::ConnectedMemoryBroker;
    use ruststream::prelude::*;
    use ruststream::testing::TestApp;
    use ruststream::{PairError, PublishPolicy};
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize, Outgoing)]
    struct Order {
        id: u64,
    }

    #[derive(Debug, Serialize, Outgoing)]
    #[outgoing(name = "confirmations")]
    struct Confirmation {
        id: u64,
    }

    #[subscriber("orders.pooled", workers(2), publish)]
    async fn confirm_pooled(order: &Order) -> Confirmation {
        Confirmation { id: order.id }
    }

    #[subscriber("orders.sequential", publish)]
    async fn confirm_sequential(order: &Order) -> Confirmation {
        Confirmation { id: order.id }
    }

    #[subscriber("plain.pooled", workers(2))]
    async fn acknowledge_pooled(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[subscriber("plain.sequential")]
    async fn acknowledge_sequential(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    /// The reading transport as a policy, so the reply position can be wired to it.
    #[derive(Debug, Clone, Copy)]
    struct SinkPublish;

    impl PublishPolicy<ConnectedMemoryBroker> for SinkPublish {
        type Live = Reading;

        fn pair(
            self,
            _connected: &ConnectedMemoryBroker,
        ) -> impl Future<Output = Result<Reading, PairError>> {
            ready(Ok(Reading::default()))
        }
    }

    /// Publishes `count` orders to `name`, each settled before the next, and answers what this
    /// thread allocated while they were handled.
    async fn cost_of(tb: &TestApp<()>, name: &str, count: u64) -> usize {
        let before = allocations();
        for id in 0..count {
            tb.message(&Order { id })
                .to(name)
                .publish()
                .await
                .expect("publish");
        }
        allocations() - before
    }

    /// The four paths compared, in the order the costs are read.
    const NAMES: [&str; 4] = [
        "orders.pooled",
        "orders.sequential",
        "plain.pooled",
        "plain.sequential",
    ];

    #[tokio::test]
    async fn a_pool_reuses_its_encode_buffers_across_deliveries() {
        let app =
            RustStream::new(AppInfo::new("pool", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
                b.include(confirm_pooled).out_reply(SinkPublish);
                b.include(confirm_sequential).out_reply(SinkPublish);
                b.include(acknowledge_pooled);
                b.include(acknowledge_sequential);
            });
        let tb = TestApp::start(app).await.expect("startup failed");

        // Warm every path: the first deliveries grow the buffers a loop keeps, and a run's
        // one-time costs are none of the comparison's business.
        for name in NAMES {
            cost_of(&tb, name, 4).await;
        }
        let mut cost = [0; 4];
        for (slot, name) in cost.iter_mut().zip(NAMES) {
            *slot = cost_of(&tb, name, 8).await;
        }
        let [reply_pooled, reply_sequential, ack_pooled, ack_sequential] = cost;
        let pool_over_replies = reply_pooled.saturating_sub(reply_sequential);
        let pool_over_acks = ack_pooled.saturating_sub(ack_sequential);
        assert!(
            pool_over_replies <= pool_over_acks,
            "over eight deliveries the pool adds {pool_over_replies} allocations to replies \
             and {pool_over_acks} to acknowledgements: a worker's buffer is not coming back \
             to the pool ({cost:?})",
        );
    }
}
