The local form of [`Seeker`]: the handle that repositions a live subscription, for a client
whose types are not `Send`.

The handle need not be `Send` or `Sync`, and neither need its [`Position`](Self::Position).
Every [`Seeker`] is a `LocalSeeker` through a blanket impl.

# Examples

```
use std::cell::Cell;
use std::rc::Rc;

use ruststream::LocalSeeker;

#[derive(Debug, thiserror::Error)]
#[error("the log ends at offset {end}")]
pub struct SeekError {
    end: u64,
}

/// The client's read position, shared with the subscription by `Rc`.
#[derive(Clone)]
pub struct Cursor {
    next: Rc<Cell<u64>>,
    end: u64,
}

impl LocalSeeker for Cursor {
    type Position = u64;
    type Error = SeekError;

    async fn seek(&self, to: u64) -> Result<(), SeekError> {
        if to > self.end {
            return Err(SeekError { end: self.end });
        }
        self.next.set(to);
        Ok(())
    }
}
```
