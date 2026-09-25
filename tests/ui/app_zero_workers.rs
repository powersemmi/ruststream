use ruststream::app;

// A runtime needs at least one worker.
#[app(worker_threads = 0)]
fn build() -> u8 {
    0
}

fn main() {}
