//! The buffer a publish to a lending transport encodes into, kept per thread.
//!
//! A publish that has no dispatch loop to borrow a buffer from (an `Out` slot, a transaction, a
//! handle used from a background task) takes the thread's buffer, writes into it and puts it
//! back, so a steady stream of publishes allocates nothing for the encode. A second publish in
//! flight on the same thread finds the slot empty and starts a buffer of its own.

use core::ops::{Deref, DerefMut};

use crate::copied::{CopiedPayload, take_frozen};
use ruststream_core::PayloadForm;
use ruststream_core::{EncodeBuf, PayloadBuf};

#[cfg(feature = "std")]
use core::cell::Cell;
#[cfg(feature = "std")]
use core::mem;

#[cfg(feature = "std")]
std::thread_local! {
    // The largest buffer this thread has encoded into, with no cap: the same rule as the
    // buffer a dispatch loop reuses.
    static SLOT: Cell<Option<PayloadBuf>> = const { Cell::new(None) };
}

/// A buffer for one publish, on loan from the thread's slot when the transport only reads it.
pub(crate) struct Scratch {
    buf: PayloadBuf,
    /// Whether the buffer goes back to the slot when the publish is over.
    returns: bool,
}

impl Scratch {
    /// The scratch of a publish in `Form`: the thread's buffer for a lending transport, and an
    /// empty one that stays unused for a taking transport, whose buffer is the codec's own.
    #[inline]
    pub(crate) fn of<Form: PayloadForm>() -> Self {
        if Form::LENDS {
            Self {
                buf: Self::lent(),
                returns: true,
            }
        } else {
            Self {
                buf: PayloadBuf::new(),
                returns: false,
            }
        }
    }

    #[cfg(feature = "std")]
    #[inline]
    fn lent() -> PayloadBuf {
        // A slot that is gone (the thread is tearing down) hands out nothing.
        SLOT.try_with(Cell::take).ok().flatten().unwrap_or_default()
    }

    #[cfg(not(feature = "std"))]
    #[inline]
    fn lent() -> PayloadBuf {
        PayloadBuf::new()
    }

    /// Moves the bytes written so far out as a value of their own, with no copy, and keeps the
    /// rest of the buffer out of the slot: it shares storage with what left, so the next publish
    /// on this thread starts a fresh one. The refusal path only.
    pub(crate) fn split_off_frozen(&mut self) -> CopiedPayload {
        self.returns = false;
        take_frozen(&mut self.buf)
    }
}

impl Deref for Scratch {
    type Target = EncodeBuf;

    fn deref(&self) -> &EncodeBuf {
        &self.buf
    }
}

impl DerefMut for Scratch {
    fn deref_mut(&mut self) -> &mut EncodeBuf {
        &mut self.buf
    }
}

#[cfg(feature = "std")]
impl Drop for Scratch {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        if self.returns {
            let mut buf = mem::take(&mut self.buf);
            buf.clear();
            // Dropped instead of kept when the slot is gone with its thread.
            let _ = SLOT.try_with(|slot| {
                // A publish that ran beside this one may have left a buffer: the larger stays.
                match slot.take() {
                    Some(held) if held.capacity() >= buf.capacity() => slot.set(Some(held)),
                    _ => slot.set(Some(buf)),
                }
            });
        }
    }
}
