use ruststream::subscriber;

// `raw` takes one delivery's payload; it does not combine with the batch source form.
#[subscriber(batch("frames"), raw)]
async fn handle(frames: &[u8]) {}

fn main() {}
