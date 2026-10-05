//! Typed deliveries: a broker whose deliveries already hold a decoded value lends it to the
//! handler, with no codec between the two.

/// A delivery that lends a typed value it already holds.
///
/// Some brokers hand over a value rather than bytes: a database queue reads a row through its
/// driver, a client decodes a frame of its own format. A handler taking `&T` reads that value
/// straight from the delivery, with no codec and no copy. The broker implements this on its
/// delivery type, and its crate's derive puts `T` on the carried input lane; a `T` that derives
/// `Deserialize` rides the codec instead.
///
/// `None` is a delivery whose value is gone, such as a claimed id whose row was deleted. The
/// runtime settles it by the subscription's decode-failure policy
/// ([`on_failure(decode = ..)`](crate::runtime::FailurePolicies)), as it settles a payload that
/// does not decode.
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
/// /// A claimed row: the row itself while it is still there.
/// struct Claimed {
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
///         &[]
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
