//! The tests of the examples, one module per example, each gated on the example's
//! `required-features`. The manifest declares the examples; this list names only the ones whose
//! tests run in a process of their own.

ruststream_example_suite::examples! {
    // Multi-file examples: their modules name each other through `crate::`, which inside this
    // suite would be the suite's root.
    guide,
    guide_http,
    manual_guide,
    manual_guide_http,
    // They install the process-global meter provider, which is what they show: a test running
    // beside them would read its instruments from theirs.
    otel_export,
    manual_otel_export,
    // They send `SIGTERM` to their own process, and every `run` in it stops: the termination
    // test of the app module would stop theirs, and theirs would stop it.
    app_running,
    manual_app_running,
}
