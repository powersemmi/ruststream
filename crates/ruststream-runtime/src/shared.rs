//! What the delivery loops of one app share, in the form each build holds it.
//!
//! With `alloc` a subscription's name and the app's state are reference-counted, so every loop
//! owns a handle. Without it the app future owns the state and lends it to the loops, and a name
//! is the descriptor's literal, held whole, or a fixed string the loop owns.

#[cfg(ruststream_alloc)]
use alloc::sync::Arc;
#[cfg(ruststream_alloc)]
use core::convert::Infallible;
#[cfg(not(ruststream_alloc))]
use core::{fmt, ops::Deref};

#[cfg(not(ruststream_alloc))]
use crate::copied::{CopiedName, NAME_CAPACITY, copy_name};
use ruststream_core::{ConnectedBroker, SubscriptionSource};

/// A subscription's name as the loop that serves it holds it.
#[cfg(ruststream_alloc)]
pub(crate) type SubscriptionName = Arc<str>;
/// A subscription's name as the loop that serves it holds it: the descriptor's literal, whole
/// and of any length, or a copy of a name built at run time in the fixed room.
#[cfg(not(ruststream_alloc))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub enum SubscriptionName {
    /// A literal of the program, which the loop borrows for as long as it runs.
    Static(&'static str),
    /// A name built at run time, copied into the fixed room.
    Built(CopiedName),
}

#[cfg(not(ruststream_alloc))]
impl Deref for SubscriptionName {
    type Target = str;

    fn deref(&self) -> &str {
        match self {
            Self::Static(literal) => literal,
            Self::Built(copy) => copy,
        }
    }
}

#[cfg(not(ruststream_alloc))]
impl fmt::Display for SubscriptionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self)
    }
}

/// The app's state as a route list lends it to its loops.
#[cfg(ruststream_alloc)]
pub(crate) type StateRef<'state, State> = &'state Arc<State>;
/// The app's state as a route list lends it to its loops.
#[cfg(not(ruststream_alloc))]
pub(crate) type StateRef<'state, State> = &'state State;

/// The app's state as one loop holds it.
#[cfg(ruststream_alloc)]
pub(crate) type StateHandle<'state, State> = Arc<State>;
/// The app's state as one loop holds it.
#[cfg(not(ruststream_alloc))]
pub(crate) type StateHandle<'state, State> = &'state State;

/// The handle one loop takes of the state a route list lends.
#[cfg(ruststream_alloc)]
pub(crate) fn handle_of<State>(state: StateRef<'_, State>) -> StateHandle<'_, State> {
    Arc::clone(state)
}

/// The handle one loop takes of the state a route list lends.
#[cfg(not(ruststream_alloc))]
pub(crate) const fn handle_of<State>(state: StateRef<'_, State>) -> StateHandle<'_, State> {
    state
}

/// The state one loop holds, read in place.
#[cfg(ruststream_alloc)]
pub(crate) fn state_in_place<'handle, State>(
    handle: &'handle StateHandle<'_, State>,
) -> &'handle State {
    handle
}

/// The state one loop holds, read in place.
#[cfg(not(ruststream_alloc))]
// Why a reference: the call site is the same in both builds, where the handle is not `Copy`.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) const fn state_in_place<'state, State>(
    handle: &StateHandle<'state, State>,
) -> &'state State {
    handle
}

/// The state a route list lends, read in place.
#[cfg(ruststream_alloc)]
pub(crate) fn borrowed<State>(state: StateRef<'_, State>) -> &State {
    state
}

/// The state a route list lends, read in place.
#[cfg(not(ruststream_alloc))]
pub(crate) const fn borrowed<State>(state: StateRef<'_, State>) -> &State {
    state
}

/// A subscription's name past the room a build without `alloc` holds it in.
#[cfg(not(ruststream_alloc))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the subscription name is {length} bytes; the room holds {room}")]
pub(crate) struct NameTooLong {
    /// The name's length in bytes.
    pub(crate) length: usize,
    /// The room, in bytes.
    pub(crate) room: usize,
}

/// The loop's own copy of the name the descriptor reports.
///
/// # Errors
///
/// Without `alloc`, [`NameTooLong`] when the name does not fit the fixed string.
#[cfg(ruststream_alloc)]
#[expect(clippy::unnecessary_wraps, reason = "one signature for both variants")]
pub(crate) fn subscription_name(name: &str) -> Result<SubscriptionName, Infallible> {
    Ok(Arc::from(name))
}

/// The loop's own copy of the name the descriptor reports.
///
/// # Errors
///
/// [`NameTooLong`] when the name does not fit the fixed string.
#[cfg(not(ruststream_alloc))]
pub(crate) fn subscription_name(name: &str) -> Result<SubscriptionName, NameTooLong> {
    copy_name(name)
        .map(SubscriptionName::Built)
        .ok_or(NameTooLong {
            length: name.len(),
            room: NAME_CAPACITY,
        })
}

/// The name a loop holds for the subscription `source` opens.
///
/// Without `alloc`, a descriptor that holds its name as a literal is borrowed whole; any other
/// name is copied into the fixed room, because it is built at run time and nothing here can own
/// it.
///
/// # Errors
///
/// Without `alloc`, [`NameTooLong`] when a name that is not a literal does not fit the room.
#[cfg(ruststream_alloc)]
pub(crate) fn subscription_name_of<Conn, Source>(
    source: &Source,
) -> Result<SubscriptionName, Infallible>
where
    Conn: ConnectedBroker,
    Source: SubscriptionSource<Conn>,
{
    subscription_name(source.name())
}

/// The name a loop holds for the subscription `source` opens.
///
/// A descriptor that holds its name as a literal is borrowed whole; any other name is copied into
/// the fixed room, because it is built at run time and nothing here can own it.
///
/// # Errors
///
/// [`NameTooLong`] when a name that is not a literal does not fit the room.
#[cfg(not(ruststream_alloc))]
pub(crate) fn subscription_name_of<Conn, Source>(
    source: &Source,
) -> Result<SubscriptionName, NameTooLong>
where
    Conn: ConnectedBroker,
    Source: SubscriptionSource<Conn>,
{
    source.static_name().map_or_else(
        || subscription_name(source.name()),
        |literal| Ok(SubscriptionName::Static(literal)),
    )
}

#[cfg(test)]
mod tests {
    #[cfg(ruststream_alloc)]
    use alloc::sync::Arc;

    use super::{borrowed, handle_of, subscription_name};

    #[test]
    fn a_name_that_fits_is_kept_whole() {
        let name = subscription_name("orders").expect("the name fits");
        assert_eq!(&*name, "orders");
    }

    #[test]
    fn a_loop_reads_the_state_its_route_list_lends() {
        let state = 7_u32;
        #[cfg(ruststream_alloc)]
        let lent = &Arc::new(state);
        #[cfg(not(ruststream_alloc))]
        let lent = &state;
        assert_eq!(*handle_of(lent), 7);
        assert_eq!(*borrowed(lent), 7);
    }

    #[cfg(not(ruststream_alloc))]
    #[test]
    fn a_name_past_the_room_is_refused_with_its_length() {
        let long = "x".repeat(65);
        let err = subscription_name(&long).expect_err("the name is past the room");
        assert_eq!(err.length, 65);
        assert_eq!(err.room, 64);
    }

    /// Descriptors only a build without `alloc` tells apart: with `alloc` every name is copied
    /// into a counted string, whatever its length.
    #[cfg(not(ruststream_alloc))]
    mod descriptors {
        use core::convert::Infallible;
        use core::future::ready;

        use futures::{Stream, stream};
        use ruststream_core::{
            AckError, BrokerMoves, ConnectedBroker, Headers, IncomingMessage, Settle, Subscriber,
            SubscriptionSource,
        };

        use super::super::subscription_name_of;

        /// A subject written as one literal, past the room a built name has.
        const LITERAL: &str = "plant/line-line-line-line-line-line-line-line-line-line-line-line-line-line-line-line-line-line-sensors/temperature";

        /// A connected broker that opens nothing: a name is all these tests ask about.
        struct Idle;

        impl ConnectedBroker for Idle {
            type Error = Infallible;
            type Closed = ();

            fn shutdown(self) -> impl Future<Output = Result<(), Infallible>> {
                ready(Ok(()))
            }
        }

        /// The subscriber of the descriptors below; none is ever opened.
        struct Silent;

        impl Subscriber for Silent {
            type Message = Never;
            type Error = Infallible;

            fn stream(&mut self) -> impl Stream<Item = Result<Never, Infallible>> + '_ {
                stream::pending()
            }
        }

        /// The message type of `Silent`; none is ever yielded.
        struct Never;

        impl IncomingMessage for Never {
            fn payload(&self) -> &[u8] {
                &[]
            }

            fn headers(&self) -> &Headers {
                unreachable!("no delivery is ever yielded")
            }
        }

        impl Settle for Never {
            fn ack(self) -> impl Future<Output = Result<(), AckError>> {
                ready(Ok(()))
            }

            fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
                ready(Ok(()))
            }
        }

        /// A descriptor whose name is a literal of the program and says so.
        struct Literal(&'static str);

        impl SubscriptionSource<Idle> for Literal {
            type Subscriber = Silent;
            type Copies = BrokerMoves;

            fn name(&self) -> &str {
                self.0
            }

            fn static_name(&self) -> Option<&'static str> {
                Some(self.0)
            }

            fn subscribe(
                self,
                _connected: &Idle,
            ) -> impl Future<Output = Result<Silent, Infallible>> {
                ready(Ok(Silent))
            }
        }

        /// A descriptor whose name was built at run time, so it cannot say it is static.
        struct Built(&'static str);

        impl SubscriptionSource<Idle> for Built {
            type Subscriber = Silent;
            type Copies = BrokerMoves;

            fn name(&self) -> &str {
                self.0
            }

            fn subscribe(
                self,
                _connected: &Idle,
            ) -> impl Future<Output = Result<Silent, Infallible>> {
                ready(Ok(Silent))
            }
        }

        #[test]
        fn a_static_name_past_the_room_is_held_whole() {
            let name = subscription_name_of::<Idle, _>(&Literal(LITERAL))
                .expect("a literal is held whole");
            assert_eq!(&*name, LITERAL);
        }

        #[test]
        fn a_built_name_that_fits_is_copied() {
            let name = subscription_name_of::<Idle, _>(&Built("orders")).expect("the name fits");
            assert_eq!(&*name, "orders");
        }

        #[test]
        fn a_built_name_past_the_room_is_refused_with_its_length() {
            let err =
                subscription_name_of::<Idle, _>(&Built(LITERAL)).expect_err("built, and too long");
            assert_eq!(err.length, LITERAL.len());
            assert_eq!(err.room, 64);
        }
    }
}
