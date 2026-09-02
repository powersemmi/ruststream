use ruststream::subscriber;

// The raw reply form publishes the returned bytes; without a return type there is nothing to
// publish, and the error names the accepted signatures.
#[subscriber("frames", publish_raw("out"))]
async fn handle(frame: &[u8]) {
    let _ = frame.len();
}

fn main() {}
