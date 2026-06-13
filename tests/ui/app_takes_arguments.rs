use ruststream::app;

// `#[ruststream::app]` is a bare attribute; it accepts no arguments.
#[app(something)]
fn build() -> u8 {
    0
}

fn main() {}
