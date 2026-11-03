//! A string or byte run of bounded size that lives in place, for the typed headers of a build
//! without an allocator.

use core::fmt;
use core::ops::Deref;
use core::str;

/// A UTF-8 run of at most `ROOM` bytes, held inline.
///
/// Writing past the room is an error for the strict writers (`fmt::Write`, `try_push`) and cuts
/// the text at a character boundary for the error texts built with `truncated`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Inline<const ROOM: usize> {
    buf: [u8; ROOM],
    len: usize,
}

/// The run did not fit its room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Overflow;

impl<const ROOM: usize> Inline<ROOM> {
    pub(super) const fn new() -> Self {
        Self {
            buf: [0; ROOM],
            len: 0,
        }
    }

    pub(super) fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    /// Appends bytes; the run is left as it was when they do not fit.
    pub(super) fn try_push(&mut self, bytes: &[u8]) -> Result<(), Overflow> {
        let end = self.len + bytes.len();
        if end > ROOM {
            return Err(Overflow);
        }
        self.buf[self.len..end].copy_from_slice(bytes);
        self.len = end;
        Ok(())
    }

    /// The text, cut at the last character boundary that fits when it is longer than the room.
    pub(super) fn truncated(text: &str) -> Self {
        let mut run = Self::new();
        run.push_truncated(text);
        run
    }

    /// The bytes as lossy UTF-8: an invalid sequence reads as U+FFFD, and the end is cut at the
    /// room.
    pub(super) fn lossy(bytes: &[u8]) -> Self {
        let mut run = Self::new();
        for chunk in bytes.utf8_chunks() {
            run.push_truncated(chunk.valid());
            if !chunk.invalid().is_empty() {
                run.push_truncated("\u{fffd}");
            }
        }
        run
    }

    /// What `value` prints as, cut at the room.
    pub(super) fn display(value: &dyn fmt::Display) -> Self {
        use fmt::Write as _;
        let mut cut = Truncating(Self::new());
        // `Truncating` never fails; a `Display` that errors on its own leaves what it wrote.
        let _ = write!(cut, "{value}");
        cut.0
    }

    fn push_truncated(&mut self, text: &str) {
        let mut take = text.len().min(ROOM - self.len);
        while !text.is_char_boundary(take) {
            take -= 1;
        }
        // `take` is within both lengths, so this cannot overflow.
        let _ = self.try_push(&text.as_bytes()[..take]);
    }
}

struct Truncating<const ROOM: usize>(Inline<ROOM>);

impl<const ROOM: usize> fmt::Write for Truncating<ROOM> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0.push_truncated(text);
        Ok(())
    }
}

impl<const ROOM: usize> fmt::Write for Inline<ROOM> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.try_push(text.as_bytes())
            .map_err(|Overflow| fmt::Error)
    }
}

impl<const ROOM: usize> Deref for Inline<ROOM> {
    type Target = str;

    fn deref(&self) -> &str {
        // Every writer appends whole UTF-8 text, or arbitrary bytes only through `try_push`
        // from a scalar the serializer built from text or bytes; a byte value is read back as
        // bytes through `as_bytes`, never as text, so a failed check reads as empty.
        str::from_utf8(self.as_bytes()).unwrap_or_default()
    }
}

impl<const ROOM: usize> fmt::Display for Inline<ROOM> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self)
    }
}

impl<const ROOM: usize> fmt::Debug for Inline<ROOM> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

#[cfg(test)]
mod tests {
    use core::fmt::Write as _;
    use std::format;

    use super::*;

    #[test]
    fn a_push_that_does_not_fit_leaves_the_run_as_it_was() {
        let mut run = Inline::<4>::new();
        assert_eq!(run.try_push(b"abc"), Ok(()));
        assert_eq!(run.try_push(b"de"), Err(Overflow));
        assert_eq!(run.as_bytes(), b"abc");
        assert_eq!(run.try_push(b"d"), Ok(()));
        assert!(write!(run, "e").is_err());
    }

    #[test]
    fn truncation_stops_at_a_character_boundary() {
        // Each letter is two bytes: three fit in seven, the fourth would cut one in half.
        let run = Inline::<7>::truncated("\u{e9}\u{e9}\u{e9}\u{e9}");
        assert_eq!(&*run, "\u{e9}\u{e9}\u{e9}");
        assert_eq!(&*Inline::<7>::truncated("short"), "short");
    }

    #[test]
    fn lossy_text_marks_invalid_bytes_and_fits_the_room() {
        assert_eq!(&*Inline::<16>::lossy(b"a\xffb"), "a\u{fffd}b");
        assert_eq!(&*Inline::<2>::lossy(b"abcd"), "ab");
    }

    #[test]
    fn display_is_cut_at_the_room() {
        assert_eq!(&*Inline::<5>::display(&123_456_789_u64), "12345");
        assert_eq!(format!("{}", Inline::<5>::display(&42)), "42");
        assert_eq!(format!("{:?}", Inline::<5>::display(&42)), "\"42\"");
    }
}
