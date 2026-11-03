//! [`Core`]: the core of the compio runner a handler runs on, for the work that belongs to one.

use core::cell::Cell;
use core::num::NonZeroUsize;
use std::thread_local;

thread_local! {
    /// The core this thread is, set by the runner that started it.
    static CURRENT: Cell<Option<Core>> = const { Cell::new(None) };
}

/// The core a handler or a hook runs on, as `Ctx(core): Ctx<Core>` binds it.
///
/// A hook that does one-time work reads its core and acts on core 0; a thread no compio runner
/// started is core 0 of 1.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "compio")]
/// # mod demo {
/// use ruststream_runtime::Core;
///
/// /// What a hook that warms a shared cache does on each core.
/// fn warm_up() -> &'static str {
///     if Core::current().is_main() { "loads the cache" } else { "reuses it" }
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Core {
    index: usize,
    count: NonZeroUsize,
}

impl Core {
    /// The core `index` of `count`.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(index: usize, count: NonZeroUsize) -> Self {
        Self { index, count }
    }

    /// The core the calling thread is.
    #[must_use]
    pub fn current() -> Self {
        CURRENT.get().unwrap_or(Self {
            index: 0,
            count: NonZeroUsize::MIN,
        })
    }

    /// Makes the calling thread this core until the guard drops.
    #[doc(hidden)]
    pub fn enter(self) -> CoreGuard {
        CoreGuard {
            previous: CURRENT.replace(Some(self)),
        }
    }

    /// This core's position, from 0 (the main core) to `count - 1`.
    #[must_use]
    pub const fn index(self) -> usize {
        self.index
    }

    /// How many cores the app runs on.
    #[must_use]
    pub const fn count(self) -> NonZeroUsize {
        self.count
    }

    /// Whether this is core 0, the one that owns the brokers and receives.
    #[must_use]
    pub const fn is_main(self) -> bool {
        self.index == 0
    }
}

/// Keeps a thread a [`Core`] until it drops.
#[doc(hidden)]
#[derive(Debug)]
#[must_use = "the thread is a core only while the guard lives"]
pub struct CoreGuard {
    previous: Option<Core>,
}

impl Drop for CoreGuard {
    fn drop(&mut self) {
        CURRENT.set(self.previous);
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroUsize;
    use std::thread;

    use super::Core;

    fn four() -> NonZeroUsize {
        NonZeroUsize::new(4).expect("four is not zero")
    }

    #[test]
    fn a_thread_no_runner_started_is_core_zero_of_one() {
        let core = Core::current();
        assert_eq!(core.index(), 0);
        assert_eq!(core.count().get(), 1);
        assert!(core.is_main());
    }

    #[test]
    fn a_thread_a_runner_entered_reports_its_core() {
        let _core = Core::new(2, four()).enter();
        let core = Core::current();
        assert_eq!(core.index(), 2);
        assert_eq!(core.count().get(), 4);
        assert!(!core.is_main());
    }

    #[test]
    fn the_core_is_per_thread() {
        let _core = Core::new(3, four()).enter();
        let other = thread::spawn(Core::current).join().expect("thread ends");
        assert_eq!(other.index(), 0);
        assert_eq!(Core::current().index(), 3);
    }

    #[test]
    fn leaving_a_core_restores_the_one_before() {
        let outer = Core::new(1, four()).enter();
        drop(Core::new(2, four()).enter());
        assert_eq!(Core::current().index(), 1);
        drop(outer);
        assert_eq!(Core::current().index(), 0);
    }
}
