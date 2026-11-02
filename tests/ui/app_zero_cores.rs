use ruststream::app;

// An app runs on at least one core.
#[app(runtime = compio, cores = 0)]
fn build() -> u8 {
    0
}

fn main() {}
