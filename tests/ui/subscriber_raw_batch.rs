use ruststream::subscriber;

// A raw batch is the typed batch without the decode step, so its handler takes the whole batch's
// payloads; `&[u8]` is one delivery's payload, which is a different shape.
#[subscriber(batch("frames"), raw)]
async fn handle(frames: &[u8]) {}

fn main() {}
