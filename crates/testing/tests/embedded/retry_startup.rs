//! A device service whose subscription refuses its retry set-up: the start fails and says why.

// The app and its handlers are not `Send`: the harness polls them inside the test's future.
#![allow(clippy::future_not_send)]

use std::error::Error;
use std::future::ready;

use embassy_futures::block_on;
use ruststream::memory::local::prelude::{
    ConnectedLocalMemoryBroker, LocalMemoryBroker, LocalMemoryError, LocalMemorySource,
    LocalMemorySubscriber,
};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, IntoSource, MountSteps, Mounts};
use ruststream::testing::embedded::TestApp;
use ruststream::{
    AddressedCopies, DeclareRetryError, Deserialized, RedeliveryAddress, RedeliveryAddressed,
    RetryDeclaration, SubscriptionSource, nonzero, subscriber,
};

/// The board's bus.
type Bus = LocalMemoryBroker<4>;

/// A job, as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// Which step of the retry set-up the subscription refuses.
#[derive(Clone, Copy)]
enum Refuses {
    /// The declared cap and destination.
    Declaration,
    /// The address its retry copies are published to.
    Address,
}

/// The bus's own subscription, refusing one step of its retry set-up.
#[derive(Clone)]
struct Refusing(Refuses);

impl Refusing {
    const fn new(step: Refuses) -> Self {
        Self(step)
    }
}

impl IntoSource for Refusing {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedLocalMemoryBroker<4>> for Refusing {
    type Subscriber = LocalMemorySubscriber<4>;
    type Copies = AddressedCopies;

    fn name(&self) -> &'static str {
        "jobs"
    }

    async fn subscribe(
        self,
        connected: &ConnectedLocalMemoryBroker<4>,
    ) -> Result<LocalMemorySubscriber<4>, LocalMemoryError> {
        LocalMemorySource::new("jobs").subscribe(connected).await
    }

    fn declare_retry_on(
        &self,
        _connected: &ConnectedLocalMemoryBroker<4>,
        _declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        match self.0 {
            Refuses::Declaration => Err(DeclareRetryError::Broker("no such queue".into())),
            Refuses::Address => Ok(()),
        }
    }
}

impl RedeliveryAddressed<ConnectedLocalMemoryBroker<4>> for Refusing {
    fn redelivery_address(
        &self,
        _connected: &ConnectedLocalMemoryBroker<4>,
    ) -> impl Future<Output = Result<RedeliveryAddress, LocalMemoryError>> {
        ready(match self.0 {
            Refuses::Declaration => Ok(RedeliveryAddress::new("jobs")),
            Refuses::Address => Err(LocalMemoryError::ShutDown),
        })
    }
}

#[subscriber(Refusing::new(Refuses::Declaration))]
async fn on_declared(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry()
}

#[subscriber(Refusing::new(Refuses::Address))]
async fn on_addressed(job: &Job<'_>) -> HandlerOutcome {
    let _ = job.0;
    HandlerOutcome::retry()
}

#[ruststream::app(runtime = embassy)]
fn declared_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_declared)
            .max_attempts(nonzero!(3u32))
            .retry_capacity::<1>()
    })
}

#[ruststream::app(runtime = embassy)]
fn addressed_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_addressed).retry_capacity::<1>())
}

fn refusal_of(started: Result<impl Sized, impl Error>) -> String {
    let Err(error) = started else {
        panic!("the start is refused");
    };
    error.to_string()
}

#[test]
fn a_declaration_the_subscription_refuses_stops_the_start() {
    block_on(async {
        let error = refusal_of(TestApp::start(declared_app(Bus::new())).await);
        assert!(error.contains("retry declaration"), "{error}");
        assert!(error.contains("jobs"), "{error}");
        assert!(error.contains("no such queue"), "{error}");
    });
}

#[test]
fn an_address_the_subscription_refuses_to_give_stops_the_start() {
    block_on(async {
        let error = refusal_of(TestApp::start(addressed_app(Bus::new())).await);
        assert!(error.contains("shut down"), "{error}");
    });
}
