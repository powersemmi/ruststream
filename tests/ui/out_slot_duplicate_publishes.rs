use ruststream::{Message, OutSlot};
use serde::Serialize;

#[derive(Message, Serialize)]
struct Report {
    id: u64,
}

// One type maps to one channel per slot: a duplicate would make publish_typed's destination
// ambiguous.
#[derive(OutSlot)]
#[publishes(Report = "reports.hourly", Report = "reports.daily")]
struct Reports;

fn main() {}
