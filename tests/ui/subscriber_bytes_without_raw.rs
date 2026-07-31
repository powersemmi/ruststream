use ruststream::subscriber;

// A `&[u8]` parameter without the `raw` flag is ambiguous between a batch of `u8` values and the
// undecoded payload; the error points at both spellings.
#[subscriber("frames")]
async fn handle(frame: &[u8]) {}

fn main() {}
