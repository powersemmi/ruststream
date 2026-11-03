//! The metadata of an app known only as `impl App` reads through the import a service already has.
#![cfg(feature = "memory")]

/// The prelude alone.
mod tokio_prelude {
    use ruststream::memory::MemoryBroker;
    use ruststream::prelude::*;
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
/// The compio runner's `App` alone.
#[cfg(feature = "compio")]
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
/// The document's module imported whole beside the prelude: nothing in it shadows the methods.
#[cfg(feature = "asyncapi")]
mod with_asyncapi_glob {
    use ruststream::asyncapi::*;
    use ruststream::memory::MemoryBroker;
    use ruststream::prelude::*;
    fn app() -> impl App {
        RustStream::new(AppInfo::new("a", "1")).with_broker(MemoryBroker::new(), |b| b)
    }
    #[test]
    fn the_metadata_reads_through_app() {
        let app = app();
        assert_eq!(app.info().title, "a");
        let _ = build_spec(&app);
    }
}
