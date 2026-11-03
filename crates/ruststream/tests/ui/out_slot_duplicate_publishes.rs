use ruststream::{OutSlot, Outgoing};
use serde::Serialize;

#[derive(Outgoing, Serialize)]
#[outgoing(name = "reports.hourly")]
struct Report {
    id: u64,
}

// A type appears once on a slot: a second entry says nothing the first does not.
#[derive(OutSlot)]
#[publishes(Report, Report)]
struct Reports;

fn main() {}
