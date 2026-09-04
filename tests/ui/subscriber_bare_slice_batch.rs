use ruststream::subscriber;

// A batch of payloads takes a named element type: both bare batch spellings are rejected with
// the derive that names the lane.
#[subscriber("frames")]
async fn batches(frames: &[&[u8]]) {
    let _ = frames.len();
}

fn main() {}
