use ruststream::subscriber;

// A page of payloads takes a named element type: both bare page spellings are rejected with
// the derive that names the lane.
#[subscriber("frames")]
async fn pages(frames: &[&[u8]]) {
    let _ = frames.len();
}

fn main() {}
