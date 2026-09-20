//! What a broker is handed on the publish path: the buffer the framework produced, or a borrow
//! of bytes it does not own.
//!
//! A transport whose client wants owned bytes should take that buffer rather than copy it, and
//! should pay only for the form it asks for. The tests below read the form through the public
//! accessors, compare buffer addresses - content equality cannot tell a hand-over from a copy -
//! and count this thread's allocations around a publish.
#![cfg(all(feature = "macros", feature = "json"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Mutex;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::PublishExt;
use ruststream::{Outgoing, OutgoingMessage, OutgoingPayload, Publisher, Serialized};
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
            OutgoingPayload::Shared(_) => "shared",
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

/// A publisher that keeps the payload, for the one assertion about its contents.
#[derive(Default)]
struct Keeper(Mutex<Option<Vec<u8>>>);

impl Publisher for Keeper {
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        *self.0.lock().expect("keeper mutex poisoned") = Some(msg.into_payload().into_vec());
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
async fn the_payload_reaches_the_broker_byte_for_byte() {
    let publisher = Keeper::default();
    publisher
        .message(&OrderCreated { id: 7 })
        .publish()
        .await
        .expect("the keeper never refuses");

    let kept = publisher.0.lock().expect("poisoned").take().expect("sent");
    assert_eq!(
        kept,
        JsonCodec
            .encode(&OrderCreated { id: 7 })
            .expect("encodable"),
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
