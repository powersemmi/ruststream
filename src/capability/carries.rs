//! Typed deliveries: a broker whose deliveries already hold a decoded value lends it to the
//! handler, with no codec between the two, one delivery at a time or a batch as one slice.

/// A delivery that lends a typed value it already holds.
///
/// Some brokers hand over a value rather than bytes: a database queue reads a row through its
/// driver, a client decodes a frame of its own format. A handler taking `&T` reads that value
/// straight from the delivery, with no codec and no copy. The broker implements this on its
/// delivery type, and its crate's derive puts `T` on the carried input lane by writing
/// `impl Input for T { type Axis = SoloCarried<T>; }` (both from [`runtime`](crate::runtime));
/// a `T` that derives `Deserialize` rides the codec instead. The lane asks of `T`
/// `Clone + Send + Sync + 'static`; `Clone` because the test harness keeps a clone of each value.
///
/// `None` is a delivery whose value is gone, such as a claimed id whose row was deleted. The
/// runtime settles it by the subscription's decode-failure policy
/// ([`on_failure(decode = ..)`](crate::runtime::FailurePolicies)), as it settles a payload that
/// does not decode.
///
/// The delivery answers [`payload`](crate::IncomingMessage::payload) with the message's bytes,
/// the ones the value is read from. A copy the runtime publishes of it, a retry copy or a dead
/// letter, carries those bytes and the headers, and the broker reads the same value from that
/// copy; `conformance::capabilities::carries` checks it.
///
/// # Examples
///
/// A queue table's delivery: the row the driver read, kept beside the claim that settles it.
///
/// ```
/// use ruststream::{AckError, Carries, HeaderMap, IncomingMessage};
///
/// /// One row of the queue table.
/// struct SendEmail {
///     to: String,
/// }
///
/// /// A claimed row: the bytes the table stores, and the row read from them while it is still
/// /// there.
/// struct Claimed {
///     bytes: Vec<u8>,
///     headers: HeaderMap,
///     row: Option<SendEmail>,
/// }
///
/// impl Carries<SendEmail> for Claimed {
///     fn carried(&self) -> Option<&SendEmail> {
///         self.row.as_ref()
///     }
/// }
///
/// impl IncomingMessage for Claimed {
///     fn payload(&self) -> &[u8] {
///         &self.bytes
///     }
///     fn headers(&self) -> &HeaderMap {
///         &self.headers
///     }
///     async fn ack(self) -> Result<(), AckError> {
///         Ok(())
///     }
///     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
///         Ok(())
///     }
/// }
///
/// let claimed = Claimed {
///     bytes: b"ops@example.com".to_vec(),
///     headers: HeaderMap::new(),
///     row: Some(SendEmail { to: "ops@example.com".to_owned() }),
/// };
/// assert_eq!(claimed.carried().map(|row| row.to.as_str()), Some("ops@example.com"));
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not carry `{T}`",
    note = "a broker implements `Carries<{T}>` on its delivery type for the value its deliveries \
            hold"
)]
pub trait Carries<T> {
    /// The value this delivery holds, or `None` where it has none.
    fn carried(&self) -> Option<&T>;
}

/// A batch that lends the values its deliveries carry, as one slice it already holds.
///
/// A broker that fetches a page of values (a page of rows read through a database driver) keeps
/// them in one buffer beside the deliveries that settle them. The broker implements this on its
/// [`BatchSubscriber::Batch`](crate::BatchSubscriber::Batch), and a handler taking `&[T]` reads
/// the slice where it lies: no codec, no copy, and no vector of the runtime's own.
///
/// The value at index `i` belongs to the `i`-th delivery the batch yields when it is iterated.
/// A delivery past the end of the slice carries no value: the runtime settles it by the
/// subscription's decode-failure policy
/// ([`on_failure(decode = ..)`](crate::runtime::FailurePolicies)), as it settles a single
/// delivery whose [`Carries::carried`] answers `None`. A broker puts such deliveries last.
///
/// The batch's context ([`BuildBatchContext`](crate::BuildBatchContext)) is built from the batch
/// itself on this lane, because the batch stays whole while the handler reads its slice; the
/// copies a retry publishes still build theirs from each delivery.
///
/// # Examples
///
/// A page of rows, the rows in one vector and the claims that settle them in another:
///
/// ```
/// use ruststream::{AckError, CarriesBatch, HeaderMap, IncomingMessage};
///
/// /// One row of the queue table.
/// struct SendEmail {
///     to: String,
/// }
///
/// /// The claim that settles one row, with the bytes the table stores for it.
/// struct Claim {
///     bytes: Vec<u8>,
///     headers: HeaderMap,
/// }
///
/// impl IncomingMessage for Claim {
///     fn payload(&self) -> &[u8] {
///         &self.bytes
///     }
///     fn headers(&self) -> &HeaderMap {
///         &self.headers
///     }
///     async fn ack(self) -> Result<(), AckError> {
///         Ok(())
///     }
///     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
///         Ok(())
///     }
/// }
///
/// /// One fetched page.
/// struct Page {
///     rows: Vec<SendEmail>,
///     claims: Vec<Claim>,
/// }
///
/// impl CarriesBatch<SendEmail> for Page {
///     fn carried(&self) -> &[SendEmail] {
///         &self.rows
///     }
/// }
///
/// impl IntoIterator for Page {
///     type Item = Claim;
///     type IntoIter = std::vec::IntoIter<Claim>;
///
///     fn into_iter(self) -> Self::IntoIter {
///         self.claims.into_iter()
///     }
/// }
///
/// let page = Page {
///     rows: vec![SendEmail { to: "ops@example.com".to_owned() }],
///     claims: vec![Claim {
///         bytes: b"ops@example.com".to_vec(),
///         headers: HeaderMap::new(),
///     }],
/// };
/// assert_eq!(page.carried()[0].to, "ops@example.com");
/// assert_eq!(page.into_iter().count(), 1);
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not lend a batch of `{T}`",
    note = "a broker implements `CarriesBatch<{T}>` on its batch type, lending the values its \
            deliveries carry as one slice"
)]
pub trait CarriesBatch<T> {
    /// The values the batch's deliveries carry, in the order the batch yields its deliveries.
    fn carried(&self) -> &[T];
}
