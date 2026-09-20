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

use ruststream::codec::{Codec, JsonCodec};
#[cfg(feature = "memory")]
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{Outgoing as OutgoingView, PublishExt, PublishIdentity, PublishPipeline};
use ruststream::{HeaderMap, Outgoing, OutgoingMessage, OutgoingPayload, Publisher, Serialized};
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

/// What a transport does with the payload it is handed.
#[derive(Clone, Copy)]
enum Take {
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
    /// Which arm the payload arrived in.
    arm: &'static str,
    /// Where the bytes `payload()` answered live.
    read_at: usize,
    /// Where the bytes the transport took ended up, or the read address where it took none.
    taken_at: usize,
    /// How long the payload is.
    len: usize,
}

/// A publisher that does with the payload what one of the broker crates would, and reports what
/// it saw without allocating anything of its own.
struct Probe {
    take: Take,
    seen: Mutex<Option<Seen>>,
}

impl Probe {
    fn taking(take: Take) -> Self {
        Self {
            take,
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
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        let read_at = msg.payload().as_ptr() as usize;
        let len = msg.payload().len();
        let payload = msg.into_payload();
        let arm = match payload {
            OutgoingPayload::Borrowed(_) => "borrowed",
            OutgoingPayload::Produced(_) => "produced",
            _ => "a form this test does not know",
        };
        // The taken buffer is released here rather than kept: the address is the whole of what
        // the assertions read, and holding it would outlive the count.
        let taken_at = match self.take {
            Take::Read => read_at,
            Take::Vec => {
                let taken = payload.into_vec();
                let at = taken.as_ptr() as usize;
                drop(taken);
                at
            }
            Take::Bytes => {
                let taken = payload.into_bytes();
                let at = taken.as_ptr() as usize;
                drop(taken);
                at
            }
        };
        *self.seen.lock().expect("probe mutex poisoned") = Some(Seen {
            arm,
            read_at,
            taken_at,
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
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        // What a transport whose client takes owned parts does: one move for all three.
        let (name, payload, headers) = msg.into_parts();
        *self.0.lock().expect("keeper mutex poisoned") =
            Some((name.to_owned(), payload.into_vec(), headers));
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
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
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
async fn an_encoded_publish_hands_the_codec_buffer_to_the_broker() {
    let publisher = Probe::taking(Take::Vec);
    publisher
        .message(&OrderCreated { id: 7 })
        .publish()
        .await
        .expect("the probe never refuses");

    let seen = publisher.seen();
    assert_eq!(
        seen.arm, "produced",
        "the codec's buffer travels as it was written",
    );
    assert_eq!(
        seen.taken_at, seen.read_at,
        "taking it as a vector reuses that buffer rather than copying out of it",
    );
}

#[tokio::test]
async fn a_value_that_carries_its_own_bytes_is_lent_to_the_broker() {
    let audit = Audit(br#"{"seen":true}"#.to_vec());
    let publisher = Probe::taking(Take::Vec);
    publisher
        .message(&audit)
        .publish()
        .await
        .expect("the probe never refuses");

    let seen = publisher.seen();
    assert_eq!(
        seen.arm, "borrowed",
        "bytes the value owns are lent, never claimed",
    );
    assert_eq!(
        seen.read_at,
        audit.0.as_ptr() as usize,
        "the broker reads the value's own buffer",
    );
    assert_ne!(
        seen.taken_at, seen.read_at,
        "and a transport that wants a buffer of its own gets a copy",
    );
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
/// transport that asks for one pays. A publish nobody takes from is the floor, and it is the
/// floor a lent payload already had; a vector costs nothing over it, because the buffer the codec
/// wrote is the vector; a `Bytes` costs the one block that makes ownership shareable.
#[tokio::test]
async fn only_a_transport_that_takes_the_buffer_pays_for_it() {
    // One publish before the count: a run's first encode grows buffers that later ones do not.
    let counted = async |take| {
        let publisher = Probe::taking(take);
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

    let (reading, read) = counted(Take::Read).await;
    let (as_vec, took_vec) = counted(Take::Vec).await;
    let (as_bytes, took_bytes) = counted(Take::Bytes).await;

    assert_eq!(
        as_vec, reading,
        "the codec's buffer is already the vector, so taking it as one allocates nothing",
    );
    assert_eq!(
        as_bytes,
        reading + 1,
        "a `Bytes` needs the shared ownership block, and that block is all it costs",
    );
    assert_eq!((read.len, took_vec.len, took_bytes.len), (8, 8, 8));
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
