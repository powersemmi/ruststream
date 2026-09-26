//! Pattern subscriptions: the [`MemoryPattern`] descriptor, the pattern it parses, and the
//! [`Routing`] rule that decides which subscriptions a publish reaches.
//!
//! A name is split into tokens at `.`. A pattern token `*` matches exactly one token, and `>` as
//! the last token matches one or more tokens: the NATS subject syntax, so a service that moves
//! between this broker and NATS keeps its subscriptions as written.

use std::{
    cmp::Ordering,
    future::{Future, ready},
};

use thiserror::Error;
use tokio::sync::mpsc;

use super::{ConnectedMemoryBroker, Discarding, LogMode, MemoryError, MemorySubscriber, Sender};
use crate::{AddressedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource};

/// How the broker picks the subscriptions a publish reaches when patterns are subscribed.
///
/// Set once, on the unconnected broker, with [`MemoryBroker::routing`](super::MemoryBroker::routing).
/// Subscriptions of one exact name receive every publish to that name under both rules; the rule
/// decides what happens when patterns match as well.
///
/// # Examples
///
/// ```
/// use ruststream::memory::{MemoryBroker, Routing};
///
/// // A pattern subscription is a fallback: it receives what no exact name, and no more
/// // specific pattern, took.
/// let broker = MemoryBroker::new().routing(Routing::MostSpecific);
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Routing {
    /// Every subscription whose name or pattern matches receives the publish, as NATS delivers.
    /// The default.
    #[default]
    EveryMatch,
    /// Only the most specific match receives the publish.
    ///
    /// The subscriptions of the exact name come first. Where there are none, the most specific
    /// matching pattern wins: two patterns are compared token by token from the left, and at the
    /// first position where they differ a literal token beats `*`, and `*` beats `>`. So for a
    /// publish to `orders.eu.created`, `orders.eu.*` beats `orders.*.created`, which beats
    /// `orders.>`, which beats `*.eu.created`. Two different patterns that match the same name
    /// always differ somewhere, so there is exactly one winner; every subscription of the
    /// winning pattern receives the publish.
    MostSpecific,
}

/// Why a subscription pattern was refused.
///
/// Reported inside [`MemoryError::InvalidPattern`], which names the pattern.
///
/// # Examples
///
/// ```
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use ruststream::memory::{MemoryBroker, MemoryError, MemoryPattern, PatternError};
/// use ruststream::{Broker, SubscriptionSource};
///
/// let connected = MemoryBroker::new().connect().await?;
/// let refused = MemoryPattern::new("orders.>.eu").subscribe(&connected).await;
/// assert!(matches!(
///     refused,
///     Err(MemoryError::InvalidPattern { reason: PatternError::TailNotLast { token: 1 }, .. })
/// ));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PatternError {
    /// A token between two dots, or before the first or after the last one, is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use ruststream::memory::{MemoryBroker, MemoryError, MemoryPattern, PatternError};
    /// use ruststream::{Broker, SubscriptionSource};
    ///
    /// let connected = MemoryBroker::new().connect().await?;
    /// let refused = MemoryPattern::new("orders..eu").subscribe(&connected).await;
    /// assert!(matches!(
    ///     refused,
    ///     Err(MemoryError::InvalidPattern { reason: PatternError::EmptyToken { token: 1 }, .. })
    /// ));
    /// # Ok(())
    /// # }
    /// ```
    #[error("token {token} is empty; tokens are separated by single dots")]
    EmptyToken {
        /// The zero-based index of the empty token.
        token: usize,
    },
    /// `>` stands before the last token. It matches the rest of a name, so it ends a pattern.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use ruststream::memory::{MemoryBroker, MemoryError, MemoryPattern, PatternError};
    /// use ruststream::{Broker, SubscriptionSource};
    ///
    /// let connected = MemoryBroker::new().connect().await?;
    /// let refused = MemoryPattern::new("orders.>.eu").subscribe(&connected).await;
    /// assert!(matches!(
    ///     refused,
    ///     Err(MemoryError::InvalidPattern { reason: PatternError::TailNotLast { token: 1 }, .. })
    /// ));
    /// # Ok(())
    /// # }
    /// ```
    #[error("`>` at token {token} is not the last token; it matches the rest of a name")]
    TailNotLast {
        /// The zero-based index of the misplaced `>`.
        token: usize,
    },
    /// The pattern has no `*` or `>` token, so it matches one name only.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use ruststream::memory::{MemoryBroker, MemoryError, MemoryPattern, PatternError};
    /// use ruststream::{Broker, SubscriptionSource};
    ///
    /// let connected = MemoryBroker::new().connect().await?;
    /// let refused = MemoryPattern::new("orders.eu").subscribe(&connected).await;
    /// assert!(matches!(
    ///     refused,
    ///     Err(MemoryError::InvalidPattern { reason: PatternError::NoWildcard, .. })
    /// ));
    /// # Ok(())
    /// # }
    /// ```
    #[error("it has no `*` or `>` token; subscribe to one name with `MemorySource`")]
    NoWildcard,
}

/// Whether `name` carries a wildcard token, `*` or `>`, which makes it a pattern rather than a
/// name.
///
/// A wildcard is a whole token: `orders.eu*` is the name it reads as.
pub(super) fn has_wildcard(name: &str) -> bool {
    name.split('.').any(|token| token == "*" || token == ">")
}

/// One token of a parsed pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Literal(Box<str>),
    /// `*`: exactly one token.
    One,
    /// `>`: one or more tokens, and the end of the pattern.
    Tail,
}

impl Token {
    /// The token's place in the specificity order: a literal beats `*`, and `*` beats `>`.
    const fn rank(&self) -> u8 {
        match self {
            Self::Literal(_) => 2,
            Self::One => 1,
            Self::Tail => 0,
        }
    }
}

/// A validated subscription pattern: parsed once, when the subscription opens, and matched on
/// every publish without allocating.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Pattern {
    tokens: Box<[Token]>,
}

impl Pattern {
    /// Parses `pattern`.
    ///
    /// # Errors
    ///
    /// Returns the [`PatternError`] of the first fault from the left, or
    /// [`PatternError::NoWildcard`] for a pattern with no wildcard token.
    pub(super) fn parse(pattern: &str) -> Result<Self, PatternError> {
        let count = pattern.split('.').count();
        let mut wildcard = false;
        let tokens = pattern
            .split('.')
            .enumerate()
            .map(|(index, token)| match token {
                "" => Err(PatternError::EmptyToken { token: index }),
                ">" if index + 1 < count => Err(PatternError::TailNotLast { token: index }),
                ">" => {
                    wildcard = true;
                    Ok(Token::Tail)
                }
                "*" => {
                    wildcard = true;
                    Ok(Token::One)
                }
                literal => Ok(Token::Literal(literal.into())),
            })
            .collect::<Result<Box<[Token]>, PatternError>>()?;
        if !wildcard {
            return Err(PatternError::NoWildcard);
        }
        Ok(Self { tokens })
    }

    /// Whether a publish to `name` matches this pattern.
    ///
    /// A wildcard stands for a token, and an empty token (a leading, trailing or doubled dot) is
    /// none: the parser refuses one in a pattern, so it matches none in a name either.
    pub(super) fn matches(&self, name: &str) -> bool {
        let mut parts = name.split('.');
        for token in &*self.tokens {
            match token {
                Token::Tail => {
                    return parts.next().is_some_and(|part| !part.is_empty())
                        && parts.all(|part| !part.is_empty());
                }
                Token::One => {
                    if parts.next().is_none_or(str::is_empty) {
                        return false;
                    }
                }
                Token::Literal(literal) => {
                    if parts.next() != Some(&**literal) {
                        return false;
                    }
                }
            }
        }
        parts.next().is_none()
    }

    /// Orders two patterns by specificity: `Greater` when `self` is the more specific one.
    pub(super) fn specificity(&self, other: &Self) -> Ordering {
        self.tokens
            .iter()
            .map(Token::rank)
            .cmp(other.tokens.iter().map(Token::rank))
    }
}

/// The subscriptions of one pattern: every subscription opened under the same pattern text
/// joins one group, so the most specific match hands the publish to all of them.
pub(super) struct PatternGroup {
    pub(super) text: Box<str>,
    pub(super) pattern: Pattern,
    pub(super) senders: Vec<Sender>,
}

impl PatternGroup {
    /// Whether any subscription of this pattern still reads.
    pub(super) fn is_live(&self) -> bool {
        self.senders.iter().any(|sender| !sender.is_closed())
    }
}

/// Which pattern groups a publish reaches, found in one scan of the pattern table.
pub(super) enum PatternReach {
    /// None of them.
    Nobody,
    /// Every matching group, the first of which sits at this index.
    EveryFrom(usize),
    /// The group at this index alone.
    Only(usize),
}

impl Routing {
    /// Which of `groups` a publish to `name` reaches, given whether an exact name took it.
    pub(super) fn reach(self, groups: &[PatternGroup], name: &str, exact: bool) -> PatternReach {
        match self {
            Self::EveryMatch => groups
                .iter()
                .position(|group| group.pattern.matches(name))
                .map_or(PatternReach::Nobody, PatternReach::EveryFrom),
            Self::MostSpecific if exact => PatternReach::Nobody,
            // A group whose subscriptions have all gone takes nothing, so it cannot shadow a
            // less specific one that is still listening.
            Self::MostSpecific => groups
                .iter()
                .enumerate()
                .filter(|(_, group)| group.pattern.matches(name) && group.is_live())
                .max_by(|(_, left), (_, right)| left.pattern.specificity(&right.pattern))
                .map_or(PatternReach::Nobody, |(index, _)| PatternReach::Only(index)),
        }
    }

    /// Which of `subscriptions` (names and patterns, as an app subscribed them) a publish to
    /// `destination` reaches: the positions, in order.
    ///
    /// The same rule as the fanout's, over the names alone. A name with a wildcard token is a
    /// pattern, since a name subscription refuses one; a pattern that does not parse opened no
    /// subscription, so it reaches nothing.
    pub(super) fn routes(self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let exact: Vec<usize> = subscriptions
            .iter()
            .enumerate()
            .filter(|(_, name)| !has_wildcard(name) && **name == destination)
            .map(|(position, _)| position)
            .collect();
        let patterns: Vec<(usize, Pattern)> = subscriptions
            .iter()
            .enumerate()
            .filter(|(_, name)| has_wildcard(name))
            .filter_map(|(position, name)| Some((position, Pattern::parse(name).ok()?)))
            .filter(|(_, pattern)| pattern.matches(destination))
            .collect();
        match self {
            Self::EveryMatch => {
                let mut every: Vec<usize> = exact
                    .into_iter()
                    .chain(patterns.into_iter().map(|(position, _)| position))
                    .collect();
                every.sort_unstable();
                every
            }
            Self::MostSpecific if !exact.is_empty() => exact,
            Self::MostSpecific => {
                let Some((_, best)) = patterns
                    .iter()
                    .max_by(|(_, left), (_, right)| left.specificity(right))
                else {
                    return Vec::new();
                };
                patterns
                    .iter()
                    .filter(|(_, pattern)| pattern == best)
                    .map(|(position, _)| *position)
                    .collect()
            }
        }
    }
}

/// A pattern subscription descriptor for [`MemoryBroker`](super::MemoryBroker): one subscription
/// reading every name the pattern matches.
///
/// `*` matches one token and `>` as the last token matches the rest of the name, as NATS
/// subjects do: `orders.*` reads `orders.eu` and `orders.us`, `orders.>` reads `orders.eu.created`
/// as well. The pattern is checked when the subscription opens, at startup, and a bad one
/// refuses to start with [`MemoryError::InvalidPattern`]. Which subscriptions a publish reaches
/// when several match is the broker's [`Routing`].
///
/// A `retry_after` outcome comes back to the same subscriber on the broker's own timer, as on
/// every subscription of this broker, so a pattern needs no destination for its retries. A
/// pattern subscription has no replay position: it reads many names, and a retaining broker keeps
/// a log per name, so its subscriber is
/// never [`Seekable`](crate::Seekable) and a handler on it cannot read a seek handle.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[subscriber(MemoryPattern::new("orders.*"))]
/// async fn audit(order: &Order) -> HandlerOutcome {
///     let _ = order.id;
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(audit);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct MemoryPattern {
    pattern: String,
}

impl MemoryPattern {
    /// Creates a source reading every name `pattern` matches.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::memory::MemoryPattern;
    ///
    /// let every_region = MemoryPattern::new("orders.*");
    /// let everything_below = MemoryPattern::new("orders.>");
    /// # let _ = (every_region, everything_below);
    /// ```
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
        }
    }
}

impl<Log: LogMode> SubscriptionSource<ConnectedMemoryBroker<Log>> for MemoryPattern {
    // A pattern reads many names, and a retaining broker keeps a log per name, so there is no
    // one log a seek could reposition it in: the subscriber is the non-seekable form on either
    // broker.
    type Subscriber = MemorySubscriber<Discarding>;
    // This broker redelivers a `retry_after` on its own timer, so the runtime publishes no copy
    // to this address; what it reports is the pattern text, which every pattern reads as itself.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.pattern
    }

    fn subscribe(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<Self::Subscriber, MemoryError>> + Send {
        let subscribed = match Pattern::parse(&self.pattern) {
            Ok(pattern) => {
                let (tx, rx) = mpsc::unbounded_channel();
                connected
                    .state
                    .register_pattern(&self.pattern, pattern, tx.clone())
                    .map(|()| {
                        MemorySubscriber::new(
                            self.pattern,
                            rx,
                            tx,
                            &connected.state,
                            Some(connected.runtime.clone()),
                        )
                    })
            }
            Err(reason) => Err(MemoryError::InvalidPattern {
                pattern: self.pattern,
                reason,
            }),
        };
        ready(subscribed)
    }
}

impl<Log: LogMode> RedeliveryAddressed<ConnectedMemoryBroker<Log>> for MemoryPattern {
    /// The pattern text. A publish to it matches the pattern token for token (`*` matches the
    /// token `*`, `>` the token `>`), so under [`Routing::EveryMatch`] it reaches this
    /// subscription.
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.pattern.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(pattern: &str) -> Pattern {
        Pattern::parse(pattern).expect("a valid pattern")
    }

    #[test]
    fn one_matches_exactly_one_token() {
        let pattern = parse("orders.*");
        assert!(pattern.matches("orders.eu"));
        assert!(!pattern.matches("orders"));
        assert!(!pattern.matches("orders.eu.created"));
        assert!(!pattern.matches("invoices.eu"));
    }

    #[test]
    fn tail_matches_one_or_more_tokens() {
        let pattern = parse("orders.>");
        assert!(pattern.matches("orders.eu"));
        assert!(pattern.matches("orders.eu.created"));
        assert!(!pattern.matches("orders"));
        assert!(parse(">").matches("anything.at.all"));
    }

    #[test]
    fn an_empty_token_matches_no_wildcard() {
        assert!(!parse("orders.*").matches("orders."));
        assert!(!parse("orders.*").matches(".eu"));
        assert!(!parse("orders.>").matches("orders."));
        assert!(!parse("orders.>").matches("orders..eu"));
        assert!(!parse("orders.>").matches("orders.eu."));
        assert!(!parse("*.eu").matches(".eu"));
        assert!(parse("orders.>").matches("orders.eu.created"));
    }

    #[test]
    fn faults_are_reported_from_the_left() {
        assert_eq!(
            Pattern::parse("orders..*"),
            Err(PatternError::EmptyToken { token: 1 })
        );
        assert_eq!(
            Pattern::parse("orders.>.eu"),
            Err(PatternError::TailNotLast { token: 1 })
        );
        assert_eq!(
            Pattern::parse("*."),
            Err(PatternError::EmptyToken { token: 1 })
        );
        assert_eq!(Pattern::parse("orders.eu"), Err(PatternError::NoWildcard));
        // A wildcard is a whole token, so this is a name, not a pattern.
        assert_eq!(Pattern::parse("orders.eu*"), Err(PatternError::NoWildcard));
        assert!(!has_wildcard("orders.eu*"));
    }

    #[test]
    fn a_literal_beats_one_and_one_beats_tail_from_the_left() {
        let ordered = [
            "orders.eu.*",
            "orders.*.created",
            "orders.>",
            "*.eu.created",
        ];
        for pair in ordered.windows(2) {
            assert_eq!(
                parse(pair[0]).specificity(&parse(pair[1])),
                Ordering::Greater,
                "{} is more specific than {}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn routes_follow_the_rule() {
        let subscriptions = ["orders.eu", "orders.*", "orders.>", "orders.*"];
        assert_eq!(
            Routing::EveryMatch.routes("orders.eu", &subscriptions),
            [0, 1, 2, 3]
        );
        assert_eq!(
            Routing::MostSpecific.routes("orders.eu", &subscriptions),
            [0]
        );
        assert_eq!(
            Routing::MostSpecific.routes("orders.us", &subscriptions),
            [1, 3]
        );
        assert_eq!(
            Routing::MostSpecific.routes("orders.us.created", &subscriptions),
            [2]
        );
        assert!(
            Routing::MostSpecific
                .routes("invoices", &subscriptions)
                .is_empty()
        );
    }
}
