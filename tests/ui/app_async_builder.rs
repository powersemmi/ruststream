use ruststream::app;

// The app builder is synchronous: the runtime calls it before the async runtime is up.
#[app]
async fn build() -> u8 {
    0
}

fn main() {}
