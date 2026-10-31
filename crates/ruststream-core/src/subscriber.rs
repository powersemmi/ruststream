//! The [`Subscriber`] trait: a stream of broker deliveries.

use core::error::Error as StdError;

use futures_core::Stream;

use crate::{LocalSettle, Settle};

crate::__contract_twin! {
    send {
        /// A consumer attached to one or more broker names.
        ///
        /// `Subscriber` yields messages via a [`Stream`], so users get back-pressure and
        /// integration with the rest of the futures ecosystem for free. Each yielded item is a
        /// broker-specific delivery ([`IncomingMessage`](crate::IncomingMessage), settled through
        /// [`Settle`]) that must be acknowledged.
        ///
        /// # Cancel safety
        ///
        /// Polling [`stream`] is cancel-safe: dropping the returned `Stream` between polls is
        /// allowed. Implementations must not buffer partially-decoded frames in `&self` state; if
        /// buffering is required, it belongs in `&mut self`.
        ///
        /// [`stream`]: Self::stream
        pub trait Subscriber: Send
    }
    local {
        #[doc = include_str!("local/subscriber.md")]
        pub trait LocalSubscriber
    }
    blanket { impl<T: ?Sized + Subscriber> LocalSubscriber for T }
    forward { <T as Subscriber> }
    docs { Subscriber }
    types {
        /// The message type yielded by this subscriber.
        type Message: [@twin { Settle } { LocalSettle }];

        /// The error type yielded by the stream when delivery fails.
        type Error: [StdError + Send + Sync + 'static];
    }
    fns {
        /// Returns a stream of broker deliveries.
        ///
        /// The broker defines its own reconnection policy. The stream ends when the subscription
        /// is cancelled, the broker shuts down, or the broker's reconnection policy gives up.
        fn stream(&mut self)
            -> [impl Stream<Item = Result<Self::Message, Self::Error>> @send { + Send } + '_];
    }
}
