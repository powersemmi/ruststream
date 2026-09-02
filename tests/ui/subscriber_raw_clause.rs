use ruststream::subscriber;

// The raw clause is retired: the form is inferred from the payload type, and the error names
// that replacement.
#[subscriber("frames", raw)]
async fn handle(frame: &[u8]) {
    let _ = frame.len();
}

fn main() {}
