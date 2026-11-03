// A handler that holds an `Rc` is refused where the compio runner is on: the error says the body
// must be `Send`, at the `include` that mounts it.
use std::future::ready;
use std::rc::Rc;

use ruststream_memory::{MemoryBroker, MemorySource};
use ruststream_runtime::{
    AppInfo, Context, Deserialized, Handle, HandlerOutcome, Input, SoloDeserialized, subscriber,
};
use ruststream_runtime_compio::RustStream;

struct Reading<'a>(&'a [u8]);

impl Deserialized for Reading<'_> {
    type Output<'a> = Reading<'a>;
    type Error = std::convert::Infallible;

    fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Self::Error> {
        Ok(Reading(payload))
    }
}

impl Input for Reading<'_> {
    type Axis = SoloDeserialized<Reading<'static>>;
}

struct Accept(Rc<u32>);

impl Handle<Reading<'_>> for Accept {
    fn handle(
        &self,
        _reading: &Reading<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

fn main() {
    let _app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| b.include(subscriber(MemorySource::new("orders"), Accept(Rc::new(1))).build()),
    );
}
