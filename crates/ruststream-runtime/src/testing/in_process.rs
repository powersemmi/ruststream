//! The harness's side of an in-process run of an app that runs as one future: the brokers the app
//! connected in process, and the scope each broker's routes open under so their deliveries are
//! recorded.

use alloc::rc::{Rc, Weak};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::{Any, TypeId, type_name};
use core::cell::{Ref, RefCell};
use core::fmt;

use crate::redelivery::ScopeDelivery;

use super::broker::{LocalInProcess, LocalTestableBroker};
use super::coordinator::{Coordinator, Origin, TestHooks};

/// The record of one in-process run of an app that runs as one future, shared by the runner,
/// which notes each broker it connects, and the harness, which reads them.
///
/// The runner connects each broker through its [`LocalInProcess`] transition, hands the connected
/// form here with [`connected`](Self::connected), and opens the broker's routes under the scope
/// that returns. The harness addresses a broker by its type through [`brokers`](Self::brokers).
/// The run keeps each connected form weakly: the app owns it and shuts it down.
#[doc(hidden)]
pub struct InProcessRun {
    coordinator: Coordinator,
    hooks: Arc<TestHooks>,
    brokers: RefCell<Vec<RunBroker>>,
}

/// A broker of an in-process run: its production type, and the view of its connected form the
/// harness drives.
#[doc(hidden)]
#[derive(Clone)]
pub struct RunBroker {
    broker: TypeId,
    name: &'static str,
    scope_id: usize,
    testable: Weak<dyn LocalTestableBroker>,
    connected: Weak<dyn Any>,
}

impl RunBroker {
    /// Whether this is the broker of type `B`.
    #[must_use]
    pub fn is<B: 'static>(&self) -> bool {
        self.broker == TypeId::of::<B>()
    }

    /// The broker's type name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The broker's registration index: the scope its deliveries and publishes are recorded
    /// under.
    #[must_use]
    pub const fn scope_id(&self) -> usize {
        self.scope_id
    }

    /// The connected form, while the app holds it.
    #[must_use]
    pub fn testable(&self) -> Option<Rc<dyn LocalTestableBroker>> {
        self.testable.upgrade()
    }

    /// The connected form as its own type, while the app holds it.
    #[must_use]
    pub fn connected(&self) -> Option<Rc<dyn Any>> {
        self.connected.upgrade()
    }
}

impl fmt::Debug for RunBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunBroker")
            .field("broker", &self.name)
            .field("scope_id", &self.scope_id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for InProcessRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InProcessRun")
            .field("brokers", &self.brokers.borrow().len())
            .finish_non_exhaustive()
    }
}

impl InProcessRun {
    /// A run that records into `coordinator`.
    #[must_use]
    pub fn new(coordinator: Coordinator) -> Self {
        let hooks = Arc::new(TestHooks::detached());
        hooks.install(coordinator.clone());
        Self {
            coordinator,
            hooks,
            brokers: RefCell::new(Vec::new()),
        }
    }

    /// Notes a broker of type `B` the app connected in process, in registration order, installs
    /// the run's coordinator into it, and returns the scope its routes open under.
    pub fn connected<B>(&self, connected: &Rc<B::Connected>) -> ScopeDelivery
    where
        B: LocalInProcess + 'static,
        B::Connected: 'static,
    {
        connected.install_coordinator(self.coordinator.clone());
        let mut brokers = self.brokers.borrow_mut();
        let scope_id = brokers.len();
        // A publisher the runtime pairs against this broker names it by this address.
        self.coordinator
            .locate_local(Origin::of::<B::Connected>(connected), scope_id);
        let shared: Rc<dyn LocalTestableBroker> = connected.clone();
        let typed: Rc<dyn Any> = connected.clone();
        brokers.push(RunBroker {
            broker: TypeId::of::<B>(),
            name: type_name::<B>(),
            scope_id,
            testable: Rc::downgrade(&shared),
            connected: Rc::downgrade(&typed),
        });
        ScopeDelivery::new().with_harness(Arc::clone(&self.hooks), scope_id, 0)
    }

    /// The brokers the app connected so far, in registration order.
    #[must_use]
    pub fn brokers(&self) -> Ref<'_, [RunBroker]> {
        Ref::map(self.brokers.borrow(), Vec::as_slice)
    }

    /// The coordinator the run records into.
    #[must_use]
    pub const fn coordinator(&self) -> &Coordinator {
        &self.coordinator
    }
}
