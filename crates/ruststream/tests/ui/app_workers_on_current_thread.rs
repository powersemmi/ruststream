use ruststream::app;

// A current-thread runtime has no workers to count.
#[app(flavor = "current_thread", worker_threads = 4)]
fn build() -> u8 {
    0
}

fn main() {}
