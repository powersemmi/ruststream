//! [`FixedBuf`]: the owned payload buffer of a build without `alloc`.

use core::fmt;
use core::ops::Deref;

#[cfg(feature = "alloc")]
use bytes::BytesMut;
use thiserror::Error;

/// The room of the owned payload buffer in a build without `alloc`, in bytes.
#[cfg(not(feature = "alloc"))]
pub const PAYLOAD_CAPACITY: usize = 256;

/// The owned payload buffer of the build: what a codec writes and a transport that keeps the
/// bytes is handed. `BytesMut` where `alloc` is on.
#[cfg(feature = "alloc")]
pub type PayloadBuf = BytesMut;

/// The owned payload buffer of the build: what a codec writes and a transport that keeps the
/// bytes is handed. A [`FixedBuf`] of [`PAYLOAD_CAPACITY`] bytes where `alloc` is off.
#[cfg(not(feature = "alloc"))]
pub type PayloadBuf = FixedBuf<PAYLOAD_CAPACITY>;

/// A write that does not fit what is left of a [`FixedBuf`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the buffer is full")]
pub struct BufferFull;

/// What the encode of a reply writes into: the dispatch loop's own buffer, reused for every message.
///
/// `BytesMut` where `alloc` is on, where it grows; a [`FixedBufView`] where it is off, the loop's
/// [`FixedBuf`] of whatever room its mount asked for (`.encode_capacity::<N>()`), seen without its
/// size.
#[cfg(feature = "alloc")]
pub type EncodeBuf = BytesMut;

/// What the encode of a reply writes into: the dispatch loop's own buffer, reused for every message.
///
/// A [`FixedBufView`] where `alloc` is off: the loop's [`FixedBuf`] of whatever room its mount
/// asked for (`.encode_capacity::<N>()`), seen without its size.
#[cfg(not(feature = "alloc"))]
pub type EncodeBuf = FixedBufView;

/// A byte buffer of a fixed capacity, owned and inline, named by its capacity as `FixedBuf<N>`.
///
/// It is the payload a transport that keeps the bytes is handed in a build without `alloc`, where
/// `BytesMut` does not exist; `FixedBufView` is the same buffer without its capacity.
///
/// It is the same kind of value as `BytesMut` (written once by the codec, then read by the
/// transport), with the difference that its room is decided by the type and a write that does not
/// fit is an error, never a reallocation.
///
/// # Examples
///
/// ```
/// use ruststream_core::FixedBuf;
///
/// # fn main() -> Result<(), ruststream_core::BufferFull> {
/// let mut payload = FixedBuf::<8>::new();
/// payload.extend_from_slice(b"order")?;
/// assert_eq!(payload.as_slice(), b"order");
/// // A write past the room is refused and leaves the buffer as it was.
/// assert!(payload.extend_from_slice(b"-1234").is_err());
/// assert_eq!(payload.len(), 5);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FixedBufOf<Bytes: ?Sized> {
    len: usize,
    // Last, so that `FixedBuf<N>` coerces to the sizeless `FixedBufView`: the unsizing of a
    // struct's tail is safe and built into the language.
    bytes: Bytes,
}

/// A byte buffer of a fixed capacity `N`: see [`FixedBufOf`].
pub type FixedBuf<const N: usize> = FixedBufOf<[u8; N]>;

/// A [`FixedBuf`] seen without its capacity: what the encode of a reply writes into, so the code
/// that writes does not depend on the room a mount chose.
///
/// A `&mut FixedBuf<N>` coerces to a `&mut FixedBufView`, and a view reads and writes like the
/// buffer it looks at.
///
/// # Examples
///
/// ```
/// use ruststream_core::{BufferFull, FixedBuf, FixedBufView};
///
/// /// Writes a greeting into whatever room the caller has.
/// fn greet(buf: &mut FixedBufView) -> Result<(), BufferFull> {
///     buf.extend_from_slice(b"hello")
/// }
///
/// # fn main() -> Result<(), BufferFull> {
/// let mut roomy = FixedBuf::<64>::new();
/// greet(&mut roomy)?;
/// assert_eq!(roomy.as_slice(), b"hello");
///
/// let mut tight = FixedBuf::<3>::new();
/// assert!(greet(&mut tight).is_err());
/// # Ok(())
/// # }
/// ```
pub type FixedBufView = FixedBufOf<[u8]>;

impl<const N: usize> FixedBufOf<[u8; N]> {
    /// An empty buffer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            len: 0,
            bytes: [0; N],
        }
    }
}

impl<Bytes: AsRef<[u8]> + AsMut<[u8]> + ?Sized> FixedBufOf<Bytes> {
    /// How many bytes the buffer holds at most.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.bytes.as_ref().len()
    }

    /// How many bytes it holds now.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether it holds no byte.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The bytes written so far.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes.as_ref()[..self.len]
    }

    /// Empties the buffer; the room stays.
    pub const fn clear(&mut self) {
        self.len = 0;
    }

    /// Appends `bytes`, or changes nothing when they do not fit.
    ///
    /// # Errors
    ///
    /// Returns [`BufferFull`] when fewer than `bytes.len()` bytes of room are left.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferFull> {
        let capacity = self.capacity();
        let end = self
            .len
            .checked_add(bytes.len())
            .filter(|end| *end <= capacity)
            .ok_or(BufferFull)?;
        self.bytes.as_mut()[self.len..end].copy_from_slice(bytes);
        self.len = end;
        Ok(())
    }

    /// Replaces the contents with what `write` puts into the whole room, and returns how many
    /// bytes it wrote.
    ///
    /// The shape a slice encoder has (`&mut [u8]` in, the length written out): the codec writes
    /// where the bytes will stay, with no second copy.
    ///
    /// # Errors
    ///
    /// Returns the error of `write`; the buffer is then empty.
    pub fn write_with<Error>(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<usize, Error>,
    ) -> Result<usize, Error> {
        self.len = 0;
        let room = self.bytes.as_mut();
        let capacity = room.len();
        let written = write(room)?;
        // An encoder never reports more than the slice it was given; the clamp keeps a faulty one
        // from exposing the zero bytes behind the room.
        self.len = written.min(capacity);
        Ok(self.len)
    }
}

impl<const N: usize> Default for FixedBufOf<[u8; N]> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Bytes: AsRef<[u8]> + AsMut<[u8]> + ?Sized> Deref for FixedBufOf<Bytes> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<Bytes: AsRef<[u8]> + AsMut<[u8]> + ?Sized> AsRef<[u8]> for FixedBufOf<Bytes> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Formatting into the buffer, so a name or a message is built where it will stay.
impl<Bytes: AsRef<[u8]> + AsMut<[u8]> + ?Sized> fmt::Write for FixedBufOf<Bytes> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.extend_from_slice(text.as_bytes())
            .map_err(|BufferFull| fmt::Error)
    }
}

impl<Bytes: AsRef<[u8]> + AsMut<[u8]> + ?Sized> fmt::Debug for FixedBufOf<Bytes> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBuf")
            .field("capacity", &self.capacity())
            .field("bytes", &self.as_slice())
            .finish()
    }
}

impl<Bytes: AsRef<[u8]> + AsMut<[u8]> + ?Sized> PartialEq for FixedBufOf<Bytes> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<Bytes: AsRef<[u8]> + AsMut<[u8]> + ?Sized> Eq for FixedBufOf<Bytes> {}

impl<const N: usize> TryFrom<&[u8]> for FixedBufOf<[u8; N]> {
    type Error = BufferFull;

    fn try_from(bytes: &[u8]) -> Result<Self, BufferFull> {
        let mut buf = Self::new();
        buf.extend_from_slice(bytes)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use core::fmt::Write as _;

    use super::*;

    fn fill(view: &mut FixedBufView, bytes: &[u8]) -> Result<(), BufferFull> {
        view.extend_from_slice(bytes)
    }

    #[test]
    fn buffers_of_different_rooms_are_seen_as_one_type() {
        let mut small = FixedBuf::<4>::new();
        let mut large = FixedBuf::<512>::new();
        let views: [&mut FixedBufView; 2] = [&mut small, &mut large];
        let rooms = views.map(|view| view.capacity());
        assert_eq!(rooms, [4, 512]);
    }

    #[test]
    fn a_write_through_the_view_lands_in_the_buffer() {
        let mut buf = FixedBuf::<8>::new();
        fill(&mut buf, b"ab").unwrap();
        fill(&mut buf, b"cd").unwrap();
        assert_eq!(buf.as_slice(), b"abcd");
    }

    #[test]
    fn a_view_refuses_what_its_buffer_cannot_hold() {
        let mut buf = FixedBuf::<4>::new();
        assert_eq!(fill(&mut buf, b"abcde"), Err(BufferFull));
        assert!(buf.is_empty());
    }

    #[test]
    fn an_encoder_writes_through_the_view_into_the_whole_room() {
        let mut buf = FixedBuf::<512>::new();
        let view: &mut FixedBufView = &mut buf;
        let written = view
            .write_with(|room| {
                room[..400].fill(7);
                Ok::<_, ()>(400)
            })
            .unwrap();
        assert_eq!(written, 400);
        assert_eq!(buf.len(), 400);
    }

    #[test]
    fn extend_appends_and_reads_back() {
        let mut buf = FixedBuf::<8>::new();
        buf.extend_from_slice(b"ab").unwrap();
        buf.extend_from_slice(b"cd").unwrap();
        assert_eq!(buf.as_slice(), b"abcd");
        assert_eq!(&*buf, b"abcd");
        assert_eq!(buf.len(), 4);
        assert_eq!(buf.capacity(), 8);
        assert!(!buf.is_empty());
    }

    #[test]
    fn a_write_past_the_room_is_refused_whole() {
        let mut buf = FixedBuf::<4>::new();
        buf.extend_from_slice(b"abc").unwrap();
        assert_eq!(buf.extend_from_slice(b"de"), Err(BufferFull));
        assert_eq!(buf.as_slice(), b"abc");
        buf.extend_from_slice(b"d").unwrap();
        assert_eq!(buf.as_slice(), b"abcd");
    }

    #[test]
    fn clear_keeps_the_room() {
        let mut buf = FixedBuf::<4>::try_from(&b"abcd"[..]).unwrap();
        buf.clear();
        assert!(buf.is_empty());
        buf.extend_from_slice(b"wxyz").unwrap();
        assert_eq!(buf.as_slice(), b"wxyz");
    }

    #[test]
    fn try_from_refuses_what_does_not_fit() {
        assert_eq!(FixedBuf::<2>::try_from(&b"abc"[..]), Err(BufferFull));
    }

    #[test]
    fn write_with_takes_the_length_the_encoder_reports() {
        let mut buf = FixedBuf::<8>::new();
        buf.extend_from_slice(b"old").unwrap();
        let written = buf
            .write_with(|room| {
                room[..2].copy_from_slice(b"hi");
                Ok::<_, ()>(2)
            })
            .unwrap();
        assert_eq!(written, 2);
        assert_eq!(buf.as_slice(), b"hi");
    }

    #[test]
    fn write_with_failure_leaves_the_buffer_empty() {
        let mut buf = FixedBuf::<8>::new();
        buf.extend_from_slice(b"old").unwrap();
        assert_eq!(buf.write_with(|_| Err::<usize, _>("full")), Err("full"));
        assert!(buf.is_empty());
    }

    #[test]
    fn write_with_clamps_a_length_past_the_room() {
        let mut buf = FixedBuf::<4>::new();
        assert_eq!(buf.write_with(|_| Ok::<_, ()>(99)), Ok(4));
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn formatting_writes_into_the_buffer() {
        let mut buf = FixedBuf::<16>::new();
        write!(buf, "ab-{}", 7).unwrap();
        assert_eq!(buf.as_slice(), b"ab-7");
        assert!(write!(buf, "past the room of sixteen").is_err());
    }

    #[test]
    fn a_default_buffer_is_empty_and_keeps_its_room() {
        let buf = FixedBuf::<6>::default();
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), 6);
    }

    #[test]
    fn a_buffer_reads_as_a_byte_slice() {
        fn length(bytes: impl AsRef<[u8]>) -> usize {
            bytes.as_ref().len()
        }
        let buf = FixedBuf::<8>::try_from(&b"abc"[..]).unwrap();
        assert_eq!(length(&buf), 3);
    }

    #[test]
    fn buffers_are_equal_by_their_bytes() {
        let small = FixedBuf::<4>::try_from(&b"ab"[..]).unwrap();
        let same = FixedBuf::<4>::try_from(&b"ab"[..]).unwrap();
        let other = FixedBuf::<4>::try_from(&b"ac"[..]).unwrap();
        assert_eq!(small, same);
        assert_ne!(small, other);
    }

    #[test]
    fn debug_names_capacity_and_bytes() {
        let buf = FixedBuf::<4>::try_from(&b"a"[..]).unwrap();
        let mut text = FixedBuf::<64>::new();
        write!(text, "{buf:?}").unwrap();
        assert!(str::from_utf8(&text).unwrap().contains("capacity: 4"));
    }
}
