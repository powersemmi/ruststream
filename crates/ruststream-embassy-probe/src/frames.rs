//! A broker stand-in a device could run: frames in a `static` embassy-sync channel, nothing on the
//! heap between the publisher and the settlement.
//!
//! The channel locks with `NoopRawMutex`, so it is not `Sync`, and the subscriber that holds its
//! receiver is not `Send`. The subscriber and its delivery therefore implement only the `Local*`
//! contract twins, and the body reaches them through those alone. The delivery is `Sync`, as the
//! body requires of a message it lends to the handler: it holds its frame and a reference to the
//! subscription's atomic counters.

use core::convert::Infallible;
use core::future::ready;
use core::pin::Pin;
use core::sync::atomic::{AtomicU32, Ordering};
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::{Channel, Receiver};
use futures_core::Stream;
use ruststream_core::{AckError, HeaderMap, LocalIncomingMessage, LocalSubscriber};

/// The largest payload a frame holds.
pub const PAYLOAD: usize = 64;

/// How many frames the channel buffers.
pub const DEPTH: usize = 4;

/// The channel the stand-in delivers through: a device keeps it in a `static`.
pub type Frames = Channel<NoopRawMutex, Frame, DEPTH>;

/// One message on the wire, held inline.
#[derive(Clone, Debug)]
pub struct Frame(heapless::Vec<u8, PAYLOAD>);

impl Frame {
    /// A frame carrying `payload`, or `None` when it exceeds [`PAYLOAD`] bytes.
    #[must_use]
    pub fn new(payload: &[u8]) -> Option<Self> {
        heapless::Vec::from_slice(payload).ok().map(Self)
    }
}

/// How the deliveries of a subscription were settled.
#[derive(Debug, Default)]
pub struct Settlements {
    acked: AtomicU32,
    nacked: AtomicU32,
}

impl Settlements {
    /// No delivery settled yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            acked: AtomicU32::new(0),
            nacked: AtomicU32::new(0),
        }
    }

    /// How many deliveries were acknowledged.
    #[must_use]
    pub fn acked(&self) -> u32 {
        self.acked.load(Ordering::Relaxed)
    }

    /// How many deliveries were negatively acknowledged.
    #[must_use]
    pub fn nacked(&self) -> u32 {
        self.nacked.load(Ordering::Relaxed)
    }
}

/// A subscription over the channel.
#[derive(Debug)]
pub struct FrameSubscriber {
    receiver: Receiver<'static, NoopRawMutex, Frame, DEPTH>,
    settled: &'static Settlements,
}

impl FrameSubscriber {
    /// A subscription that receives from `frames` and records its settlements in `settled`.
    #[must_use]
    pub fn new(frames: &'static Frames, settled: &'static Settlements) -> Self {
        Self {
            receiver: frames.receiver(),
            settled,
        }
    }
}

impl LocalSubscriber for FrameSubscriber {
    type Message = FrameDelivery;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<FrameDelivery, Infallible>> + '_ {
        Deliveries { subscriber: self }
    }
}

/// The stream of a [`FrameSubscriber`]: never ends, parks on the channel while it is empty.
struct Deliveries<'a> {
    subscriber: &'a FrameSubscriber,
}

impl Stream for Deliveries<'_> {
    type Item = Result<FrameDelivery, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let settled = self.subscriber.settled;
        self.subscriber.receiver.poll_receive(cx).map(|frame| {
            Some(Ok(FrameDelivery {
                frame,
                headers: HeaderMap::new(),
                settled,
            }))
        })
    }
}

/// A frame as the subscription hands it out, recording how it was settled.
#[derive(Debug)]
pub struct FrameDelivery {
    frame: Frame,
    headers: HeaderMap,
    settled: &'static Settlements,
}

impl LocalIncomingMessage for FrameDelivery {
    fn payload(&self) -> &[u8] {
        &self.frame.0
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        count(&self.settled.acked);
        ready(Ok(()))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        count(&self.settled.nacked);
        ready(Ok(()))
    }
}

/// Adds one with a load and a store: a device without atomic read-modify-write has both, and one
/// executor thread settles a subscription's deliveries.
fn count(counter: &AtomicU32) {
    counter.store(counter.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
}
