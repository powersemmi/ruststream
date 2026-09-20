//! What a broker is handed on the publish path: the buffer the framework produced, or a borrow
//! of bytes it does not own.
//!
//! A transport whose client speaks `bytes` wants to keep the payload rather than copy it, and
//! the only thing that decides whether it can is which form the publish handed over. The tests
//! below read that form through the public accessors and compare buffer addresses, because
//! content equality cannot tell a hand-over from a copy.
#![cfg(all(feature = "macros", feature = "json"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Mutex;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::PublishExt;
use ruststream::{Bytes, Outgoing, OutgoingMessage, OutgoingPayload, Publisher, Serialized};
use serde::Serialize;

/// Counts this thread's allocations, so the cost of one `Bytes::clone` can be read off directly.
/// A thread-local count rather than a global one: the test harness runs other tests beside this
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

/// What the broker was handed, read the way a transport that keeps the buffer reads it.
struct Handed {
    /// Where the bytes `payload()` answers live.
    read_at: usize,
    /// Whether the publish handed a buffer over rather than lending one.
    shared: bool,
    /// The buffer itself, taken by value the way a consuming transport takes it.
    taken: Bytes,
}

/// A publisher that takes the payload by value, the way a transport that keeps the buffer does.
#[derive(Default)]
struct Taker(Mutex<Option<Handed>>);

impl Taker {
    /// What the last publish handed over, moved out so no reference count is added to it.
    fn handed(&self) -> Handed {
        self.0
            .lock()
            .expect("recorder mutex poisoned")
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
        let read_at = msg.payload().as_ptr() as usize;
        let payload = msg.into_payload();
        let shared = matches!(payload, OutgoingPayload::Shared(_));
        *self.0.lock().expect("recorder mutex poisoned") = Some(Handed {
            read_at,
            shared,
            taken: payload.into_bytes(),
        });
        ready(Ok(()))
    }
}

/// A publisher that asks for owned bytes without consuming the message, the way a transport that
/// only borrows the delivery does.
#[derive(Default)]
struct Cloner(Mutex<Option<(usize, Bytes)>>);

impl Publisher for Cloner {
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        *self.0.lock().expect("recorder mutex poisoned") =
            Some((msg.payload().as_ptr() as usize, msg.payload_bytes()));
        ready(Ok(()))
    }
}

#[tokio::test]
async fn an_encoded_publish_hands_the_codec_buffer_to_the_broker() {
    let publisher = Taker::default();
    publisher
        .message(&OrderCreated { id: 7 })
        .publish()
        .await
        .expect("the recorder never refuses");

    let handed = publisher.handed();
    assert!(handed.shared, "the codec's buffer travels as a hand-over");
    assert_eq!(
        handed.taken.as_ptr() as usize,
        handed.read_at,
        "the buffer the broker takes is the one it reads, not a copy of it",
    );
    assert_eq!(
        handed.taken,
        JsonCodec
            .encode(&OrderCreated { id: 7 })
            .expect("encodable"),
    );
}

#[tokio::test]
async fn a_value_that_carries_its_own_bytes_is_lent_to_the_broker() {
    let audit = Audit(br#"{"seen":true}"#.to_vec());
    let publisher = Taker::default();
    publisher
        .message(&audit)
        .publish()
        .await
        .expect("the recorder never refuses");

    let handed = publisher.handed();
    assert!(
        !handed.shared,
        "bytes the value owns are lent, never claimed",
    );
    assert_eq!(
        handed.read_at,
        audit.0.as_ptr() as usize,
        "the broker reads the value's own buffer",
    );
    assert_eq!(handed.taken, audit.0);
}

#[tokio::test]
async fn owned_bytes_are_free_on_a_hand_over_and_a_copy_on_a_borrow() {
    let publisher = Cloner::default();
    publisher
        .message(&OrderCreated { id: 7 })
        .publish()
        .await
        .expect("the recorder never refuses");
    let (read_at, owned) = publisher.0.lock().expect("poisoned").take().expect("sent");
    assert_eq!(
        owned.as_ptr() as usize,
        read_at,
        "a handed-over buffer is shared with the caller, not copied for it",
    );

    let audit = Audit(br#"{"seen":true}"#.to_vec());
    let publisher = Cloner::default();
    publisher
        .message(&audit)
        .publish()
        .await
        .expect("the recorder never refuses");
    let (read_at, owned) = publisher.0.lock().expect("poisoned").take().expect("sent");
    assert_ne!(
        owned.as_ptr() as usize,
        read_at,
        "lent bytes cannot be shared, so asking for owned ones copies them",
    );
    assert_eq!(owned, audit.0, "and the copy is the payload, byte for byte");
}

/// What the twelve broker crates need to know before they adapt: the buffer handed over is
/// already shareable, so a client that clones it - `async-nats` taking a `Bytes` and keeping a
/// copy of the handle - pays a reference count and no heap block. Freezing the codec's buffer
/// is what buys that, once, on the framework's side of the call.
#[tokio::test]
async fn cloning_a_handed_over_buffer_costs_no_allocation() {
    let publisher = Taker::default();
    publisher
        .message(&OrderCreated { id: 7 })
        .publish()
        .await
        .expect("the recorder never refuses");
    let taken = publisher.handed().taken;

    let before = allocations();
    let first = taken.clone();
    let second = taken.clone();
    let after = allocations();

    assert_eq!(
        after - before,
        0,
        "the handed-over buffer is shared, so taking a reference to it allocates nothing",
    );
    assert_eq!(first, taken);
    assert_eq!(second, taken);
}
