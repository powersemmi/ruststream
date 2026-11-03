use ruststream::app;

// The attribute takes the runtime arguments of `#[tokio::main]` and nothing else.
#[app(something)]
fn build() -> u8 {
    0
}

fn main() {}
