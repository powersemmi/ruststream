use ruststream::app;

// The app builder takes no arguments; it just constructs and returns the application.
#[app]
fn build(flag: u8) -> u8 {
    flag
}

fn main() {}
