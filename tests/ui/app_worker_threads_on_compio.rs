use ruststream::app;

// A compio app is sized by `cores`; a tokio worker count does not apply.
#[app(runtime = compio, worker_threads = 4)]
fn build() -> u8 {
    0
}

fn main() {}
