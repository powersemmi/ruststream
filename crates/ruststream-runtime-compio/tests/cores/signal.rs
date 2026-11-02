//! The shutdown signal of the process stops the cores.

use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::process::{self, Command};

use ruststream_runtime::ShutdownWait;
use ruststream_runtime_compio::{Cores, Stop, yield_now};

/// SIGTERM sent to the process raises the stop every core waits on.
#[cfg(unix)]
#[test]
fn a_termination_signal_raises_the_stop_and_the_cores_end() {
    let stop = Stop::default();
    let outcome = Cores::new(NonZeroUsize::new(2).expect("a core count"))
        .unpinned()
        .run(&stop, async |core, stop| {
            if core.is_main() {
                // The signal listener is a task of this runtime: let it register before the
                // signal is sent.
                for _ in 0..8 {
                    yield_now().await;
                }
                let sent = Command::new("kill")
                    .args(["-s", "TERM", &process::id().to_string()])
                    .status()
                    .expect("kill runs");
                assert!(sent.success());
            }
            stop.cancelled().await;
            Ok::<(), Infallible>(())
        });

    outcome.expect("the signal ended the run cleanly");
    assert!(stop.is_cancelled());
}
