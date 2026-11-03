use ruststream::app;

// Tokio has two runtime flavors.
#[app(flavor = "work_stealing")]
fn build() -> u8 {
    0
}

fn main() {}
