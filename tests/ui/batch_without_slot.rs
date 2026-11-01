//! A batching subscriber whose batch type is not a slot the runtime can lend: the error names the
//! type and says what to implement.
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use ruststream::memory::MemoryMessage;
use ruststream::{BatchStream, BatchSubscriber, Subscriber};

/// A batch type that was never taught to be a slot.
struct Page(Vec<MemoryMessage>);

struct Pages;

impl Subscriber for Pages {
    type Message = MemoryMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<MemoryMessage, Infallible>> + Send + '_ {
        futures::stream::empty()
    }
}

impl BatchSubscriber for Pages {
    type Batch = Page;

    fn batches(
        &mut self,
        _size: NonZeroUsize,
    ) -> impl BatchStream<Page, Error = Infallible> + Send + '_ {
        Ending
    }
}

struct Ending;

impl BatchStream<Page> for Ending {
    type Error = Infallible;

    fn poll_fill(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _slot: &mut Page,
    ) -> Poll<Option<Result<(), Infallible>>> {
        Poll::Ready(None)
    }
}

fn main() {}
