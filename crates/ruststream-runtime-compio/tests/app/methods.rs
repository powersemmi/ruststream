//! The metadata of a compio app known only as `impl App` reads through the runner's own import.

/// The compio runner's `App` alone.
mod compio_only {
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::compio::{App, AppInfo, RustStream};
    fn app() -> impl App {
        RustStream::new(AppInfo::new("a", "1")).with_broker(MemoryBroker::new(), |b| b)
    }
    #[test]
    fn the_metadata_reads_through_app() {
        let app = app();
        assert_eq!(app.info().title, "a");
        assert!(app.servers().is_empty());
        assert!(app.handlers().is_empty());
    }
}
