use ruststream::app;

// A compio app is sized by `cores`; a tokio flavor does not apply.
#[app(runtime = compio, flavor = "current_thread")]
fn build() -> u8 {
    0
}

fn main() {}
