use ruststream::subscriber;

// A `&[u8]` payload has no decode step, so a decode failure policy is meaningless.
#[subscriber("frames", on_failure(decode = drop))]
async fn handle(frame: &[u8]) {
    let _ = frame.len();
}

fn main() {}
