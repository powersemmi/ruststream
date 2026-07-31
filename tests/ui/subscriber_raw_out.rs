use ruststream::subscriber;

// The Out injection rides the typed pipeline; it is not available on the raw form. The macro's
// Out probe is syntactic, so no import of the (never-resolved) Out type is needed here.
#[subscriber("frames", raw)]
async fn handle(frame: &[u8], out: Out<()>) {}

fn main() {}
