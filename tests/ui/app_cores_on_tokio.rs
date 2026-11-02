use ruststream::app;

// `cores` sizes a compio app; tokio sizes its pool with `worker_threads`.
#[app(cores = 4)]
fn build() -> u8 {
    0
}

fn main() {}
