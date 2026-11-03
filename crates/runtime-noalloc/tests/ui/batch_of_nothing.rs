use ruststream_runtime_noalloc::{BatchSize, FitsBatch};

/// What `.batch::<SIZE>()` asks of its size.
fn batch<const SIZE: usize>()
where
    BatchSize<SIZE>: FitsBatch,
{
}

fn main() {
    batch::<0>();
}
