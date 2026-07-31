use ruststream::subscriber;

// Under `raw` there is no decode step, so a typed message parameter cannot be satisfied; the
// error names the fix in both directions.
#[subscriber("frames", raw)]
async fn handle(frame: &String) {}

fn main() {}
