// The refused items expand to nothing, so the imports they would use stay unused.
#![allow(unused_imports)]

use ruststream::memory::local::LocalMemorySource;
use ruststream::runtime::{HandlerOutcome, Out};
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

#[derive(Outgoing, Serialized)]
#[outgoing(name = "answers")]
struct Answer(Vec<u8>);

#[derive(OutSlot)]
struct Audit;

#[subscriber(LocalMemorySource::new("readings"))]
async fn bill(readings: &[Reading<'_>], Out(_audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let _ = readings.len();
    HandlerOutcome::ack()
}

#[subscriber(LocalMemorySource::new("questions"), reply)]
async fn answer(question: &Reading<'_>, Out(_audit): Out<impl Publisher, Audit>) -> Answer {
    Answer(question.0.to_vec())
}

fn main() {}
