//! What a broker's client does with the message of a publish it refuses, the declaration the
//! refused-publish checks of both families take.

/// What a broker's client does with the message of a publish it refuses.
///
/// The answer is the broker's: a client that hands the buffer back in its error lets the crate
/// return the unsent message, and one that consumes it cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsent {
    /// The refusal carries the message that was not sent, destination, payload and headers as
    /// they were published.
    Returned,
    /// The client consumed the message: the refusal may carry it, and carries nothing wrong when
    /// it does.
    Consumed,
}
