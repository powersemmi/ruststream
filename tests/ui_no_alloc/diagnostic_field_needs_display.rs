use ruststream_core::diag;

struct Opaque;

// With no sink switched on the event still checks its arguments: a field shown with `%` must be
// `Display`, so a mistake is found in a build that logs nowhere too.
fn main() {
    let subscription = Opaque;
    diag::warn!(subscription = %subscription, "a delivery was lost");
}
