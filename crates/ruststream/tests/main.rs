//! The facade's integration suite, one test binary: what holds only for a set of the facade's
//! features (a build with no codec, the derives off, the prelude alone) and the compile-fail
//! snapshots of the whole surface. Each file or directory under `tests/` is a module of it.

mod codec_free_lanes;
mod lane_traits_without_macros;
mod manual_raw_subscriber;
mod memory_prelude;
mod prelude;
mod raw_subscriber;
mod ui;
mod ui_codec_free;
