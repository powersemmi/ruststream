use ruststream::subscriber;

// There is no decode step under `raw`, so a decode failure policy is meaningless.
#[subscriber("frames", raw, on_failure(decode = drop))]
async fn handle(frame: &[u8]) {}

fn main() {}
