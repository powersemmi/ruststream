use ruststream::subscriber;

// A payload's bytes take a named type: the bare slice spelling is rejected with the derive
// that names the lane.
#[subscriber("frames")]
async fn handle(frame: &[u8]) {
    let _ = frame.len();
}

fn main() {}
