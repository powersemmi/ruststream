use ruststream_runtime_noalloc::{BatchSize, FitsBatch};

/// What `.batch::<SIZE>()` asks of its size.
fn batch<const SIZE: usize>()
where
    BatchSize<SIZE>: FitsBatch,
{
}

fn main() {
    // The runtime's storage holds 16 messages of a batch.
    batch::<17>();
}
