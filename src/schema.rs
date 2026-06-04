//! Message-type metadata for `AsyncAPI` generation.

/// Metadata about a message type, used to name and describe it in an `AsyncAPI` document.
///
/// Derive with `#[derive(Message)]` (requires the `macros` feature): the derive uses the type's
/// name as [`NAME`](Self::NAME) and its doc comment as [`DESCRIPTION`](Self::DESCRIPTION).
///
/// # Examples
///
/// ```
/// use ruststream::Message;
///
/// struct Order;
/// impl Message for Order {
///     const NAME: &'static str = "Order";
/// }
///
/// assert_eq!(Order::NAME, "Order");
/// ```
pub trait Message {
    /// The message name, used as the `AsyncAPI` component name.
    const NAME: &'static str;

    /// An optional description, typically from the type's doc comment.
    const DESCRIPTION: Option<&'static str> = None;
}
