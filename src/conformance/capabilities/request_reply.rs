//! The request-reply suite.

use std::fmt;

use futures::Stream;
use tokio::sync::oneshot;

use super::{
    AFTER_SHUTDOWN_REQUEST_TIMEOUT, DEFAULT_TIMEOUT, MISS_TIMEOUT, ack_or_unsupported, promptly,
};
use crate::conformance::harness::{expect_next, on_foreign_runtime};
use crate::conformance::helpers::unique_subject;
use crate::{
    Broker, Connected, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    RequestReply, Subscriber, SubscriptionSource,
};

/// Verifies the [`RequestReply`] contract.
///
/// A request reaches a responder with a usable `reply-to` header, the correlated reply resolves
/// the request, and a request nobody answers fails once its timeout elapses. Two requests in
/// flight at once each resolve with their own reply, a reply that arrives after its request timed
/// out resolves no later request, and a request through the requester after the broker shut down
/// returns an error at once.
///
/// The first request comes from a current-thread runtime on a thread of its own, which stops once
/// the reply arrives, and a second request from the suite's runtime must still resolve: what the
/// requester starts on the first request (a reply dispatcher, a reply consumer) runs on the
/// runtime the broker connected on, not on the caller's. The requester moves to that thread, so it
/// is `'static`.
///
/// The factories mirror [`harness::lifecycle`](crate::conformance::harness::lifecycle): `make_source` opens
/// the responder's subscription, `make_requester` produces the [`RequestReply`] publisher under
/// test, and `make_publisher` produces the plain publisher the responder replies through.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::capabilities;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// capabilities::request_reply(
///     MemoryBroker::new,
///     |name| MemorySource::new(name),
///     |broker| broker.requester(),
///     |broker| broker.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message if any step violates the contract.
pub async fn request_reply<B, MkBroker, Src, MkSrc, Req, MkReq, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_requester: MkReq,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>> + Send,
    Src::Subscriber: Send,
    MkSrc: Fn(&str) -> Src,
    Req: RequestReply + 'static,
    MkReq: Fn(&Connected<B>) -> Req,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.request_reply");
    // Derived from the same run, so the "nobody answers" leg cannot collide with a responder
    // another run of this suite left listening.
    let unanswered_subject = format!("{subject}.void");

    let connected = make_broker().connect().await.expect("broker must connect");

    let mut responder = make_source(&subject)
        .subscribe(&connected)
        .await
        .expect("responder subscription must open after connect");
    let publisher = make_publisher(&connected);
    let requester = make_requester(&connected);
    let mut inbox = std::pin::pin!(responder.stream());

    let respond = async {
        // One request from another runtime, then one from this one.
        for label in [
            "request_reply responder, the request from another runtime",
            "request_reply responder, the request after that runtime stopped",
        ] {
            let msg = expect_next(&mut inbox, label).await;
            assert_eq!(
                msg.payload(),
                b"ping",
                "responder must receive the request payload"
            );
            answer(&publisher, &msg, b"pong".as_slice())
                .await
                .expect("reply publish failed");
            ack_or_unsupported(msg, label).await;
        }
    };
    let requests = async {
        // The first request comes from a runtime that stops once it is answered, the way a
        // handler on a dedicated thread requests: whatever the requester starts on it (a reply
        // dispatcher, a correlation table's reader) must run on the broker's runtime, or the
        // second request finds it gone.
        let first = subject.clone();
        let requester = on_foreign_runtime(async move || {
            let reply = requester
                .request(
                    OutgoingMessage::new(&first, b"ping".as_slice()),
                    DEFAULT_TIMEOUT,
                )
                .await
                .expect("request must resolve once the responder replies");
            assert_eq!(
                reply.payload(),
                b"pong",
                "the correlated reply must carry the responder payload"
            );
            requester
        })
        .await;
        let reply = requester
            .request(
                OutgoingMessage::new(&subject, b"ping".as_slice()),
                DEFAULT_TIMEOUT,
            )
            .await
            .expect(
                "a request must resolve after an earlier one was made from a runtime that has \
                 stopped; the broker must run its internal tasks on the runtime it connected on",
            );
        assert_eq!(
            reply.payload(),
            b"pong",
            "the correlated reply must carry the responder payload"
        );
        requester
    };

    let (requester, ()) = futures::join!(requests, respond);

    late_reply_resolves_no_later_request(&requester, &publisher, &mut inbox, &subject).await;
    concurrent_requests_get_their_own_replies(&requester, &publisher, &mut inbox, &subject).await;

    let unanswered = requester
        .request(
            OutgoingMessage::new(&unanswered_subject, b"ping".as_slice()),
            MISS_TIMEOUT,
        )
        .await;
    assert!(
        unanswered.is_err(),
        "a request nobody answers must fail once its timeout elapses",
    );

    connected
        .shutdown()
        .await
        .expect("broker must shut down cleanly");

    let after_shutdown = promptly(
        requester.request(
            OutgoingMessage::new(&subject, b"ping".as_slice()),
            AFTER_SHUTDOWN_REQUEST_TIMEOUT,
        ),
        "request_reply: a request after shutdown",
    )
    .await;
    assert!(
        after_shutdown.is_err(),
        "request_reply: a request through a requester that outlived the shutdown resolved; it \
         must return an error, never succeed against the dead connection",
    );
}

/// A request times out while the responder holds its reply back; the reply then goes out, and the
/// next request must resolve with its own reply, not with the late one.
async fn late_reply_resolves_no_later_request<Req, Pub, S, M, E>(
    requester: &Req,
    publisher: &Pub,
    inbox: &mut S,
    subject: &str,
) where
    Req: RequestReply,
    Pub: Publisher,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let (timed_out, held_back) = oneshot::channel::<()>();
    let (late_sent, late_out) = oneshot::channel::<()>();

    let requests = async move {
        let late = requester
            .request(
                OutgoingMessage::new(subject, b"late".as_slice()),
                MISS_TIMEOUT,
            )
            .await;
        assert!(
            late.is_err(),
            "request_reply: the responder held its reply back past the request's timeout, so the \
             request must fail once {MISS_TIMEOUT:?} elapse",
        );
        let _ = timed_out.send(());
        late_out
            .await
            .expect("request_reply: the responder sends the late reply before the next request");
        let fresh = requester
            .request(
                OutgoingMessage::new(subject, b"fresh".as_slice()),
                DEFAULT_TIMEOUT,
            )
            .await
            .expect("request_reply: a request after a timed-out one must resolve once answered");
        assert_eq!(
            fresh.payload(),
            b"re:fresh",
            "request_reply: a reply that arrived after its request timed out resolved the next \
             request; a late reply must be discarded, never handed to another request",
        );
    };
    let respond = async move {
        let late = expect_next(
            &mut *inbox,
            "request_reply responder, the request that times out",
        )
        .await;
        held_back
            .await
            .expect("request_reply: the requester reports the timeout");
        // Nobody waits on that reply address any more, so a transport may refuse the publish;
        // either way the reply must reach no other request.
        let _ = answer(publisher, &late, b"re:late".as_slice()).await;
        ack_or_unsupported(late, "request_reply responder, the request that timed out").await;
        let _ = late_sent.send(());

        let fresh = expect_next(
            &mut *inbox,
            "request_reply responder, the request after the timeout",
        )
        .await;
        answer(publisher, &fresh, b"re:fresh".as_slice())
            .await
            .expect("reply publish failed");
        ack_or_unsupported(
            fresh,
            "request_reply responder, the request after the timeout",
        )
        .await;
    };
    futures::join!(requests, respond);
}

/// Two requests in flight at once, answered in the reverse of their arrival order: each must
/// resolve with the reply to itself, so a requester that hands out replies in arrival order, or
/// that shares one reply address between requests, is caught.
async fn concurrent_requests_get_their_own_replies<Req, Pub, S, M, E>(
    requester: &Req,
    publisher: &Pub,
    inbox: &mut S,
    subject: &str,
) where
    Req: RequestReply,
    Pub: Publisher,
    S: Stream<Item = Result<M, E>> + Unpin,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let requests = async {
        let (alpha, beta) = futures::join!(
            requester.request(
                OutgoingMessage::new(subject, b"alpha".as_slice()),
                DEFAULT_TIMEOUT,
            ),
            requester.request(
                OutgoingMessage::new(subject, b"beta".as_slice()),
                DEFAULT_TIMEOUT,
            ),
        );
        for (reply, expected) in [(alpha, b"re:alpha".as_slice()), (beta, b"re:beta")] {
            let reply = reply.expect(
                "request_reply: each of two concurrent requests must resolve once answered",
            );
            assert_eq!(
                reply.payload(),
                expected,
                "request_reply: two requests in flight at once must each resolve with the reply \
                 to itself",
            );
        }
    };
    let respond = async {
        let first = expect_next(
            &mut *inbox,
            "request_reply responder, the first concurrent request",
        )
        .await;
        let second = expect_next(
            &mut *inbox,
            "request_reply responder, the second concurrent request",
        )
        .await;
        for request in [&second, &first] {
            let echoed = [b"re:".as_slice(), request.payload()].concat();
            answer(publisher, request, &echoed)
                .await
                .expect("reply publish failed");
        }
        ack_or_unsupported(
            first,
            "request_reply responder, the first concurrent request",
        )
        .await;
        ack_or_unsupported(
            second,
            "request_reply responder, the second concurrent request",
        )
        .await;
    };
    futures::join!(requests, respond);
}

/// Replies to `request` with `payload` at its `reply-to` address, echoing its correlation id when
/// the requester set one.
async fn answer<Pub, M>(publisher: &Pub, request: &M, payload: &[u8]) -> Result<(), Pub::Error>
where
    Pub: Publisher,
    M: IncomingMessage,
{
    let reply_to = request
        .headers()
        .reply_to()
        .expect("a request must carry a usable reply-to header")
        .to_owned();
    // Replies must at minimum go to the reply-to destination; the correlation id is echoed for
    // the requesters that match on it.
    let mut headers = HeaderMap::new();
    if let Some(correlation_id) = request.headers().correlation_id() {
        headers.insert("correlation-id", correlation_id.to_owned());
    }
    publisher
        .publish(
            OutgoingMessage::new(&reply_to, payload).with_headers(headers),
            None,
        )
        .await
}
