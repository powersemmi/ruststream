//! Which per-message options each published message carried: what the harness records with a
//! publish, and how a broker's publish log is matched back to those records.
//!
//! The options are typed only where the runtime hands a message to the broker's publisher: the
//! publisher resolves them into its protocol, so the publish log underneath never sees them. The
//! harness therefore copies them at that hand-over, next to the message, and the assertions read
//! them back per message.

use std::any::{Any, type_name};
use std::fmt;
use std::sync::Arc;

use crate::RawMessage;

/// The broker's per-message options one publish carried, copied and type-erased so the
/// assertions can hand them back to a test as the broker's own type.
#[derive(Clone)]
pub(crate) struct RecordedOptions {
    /// The broker's options type, so a `with_options` naming another one fails by name.
    pub(crate) type_name: &'static str,
    /// `None` when the publish carried no options: no step ran, the policy's defaults applied.
    pub(crate) value: Option<Arc<dyn Any + Send + Sync>>,
}

impl RecordedOptions {
    pub(crate) fn capture<Options: Clone + Send + Sync + 'static>(
        options: Option<&Options>,
    ) -> Self {
        Self {
            type_name: type_name::<Options>(),
            value: options.map(|options| Arc::new(options.clone()) as Arc<dyn Any + Send + Sync>),
        }
    }

    /// The recorded options as `Options`, or `None` when the publish carried none.
    ///
    /// # Panics
    ///
    /// Panics when the publish carried options of another type: the test named the wrong
    /// broker's type, and the message says which one was recorded.
    pub(crate) fn downcast<Options: 'static>(&self, channel: &str) -> Option<&Options> {
        let value = self.value.as_ref()?;
        Some(value.as_ref().downcast_ref::<Options>().unwrap_or_else(|| {
            panic!(
                "channel {channel:?} published with per-message options of type `{}`, not `{}`",
                self.type_name,
                type_name::<Options>(),
            )
        }))
    }

    /// Whether the two records certainly hold the same options. A value cannot be compared
    /// through the erasure, so only two publishes that both took the policy's defaults are.
    fn certainly_same_as(&self, other: &Self) -> bool {
        self.type_name == other.type_name && self.value.is_none() && other.value.is_none()
    }
}

impl fmt::Debug for RecordedOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordedOptions")
            .field("type_name", &self.type_name)
            .field("set", &self.value.is_some())
            .finish()
    }
}

/// One publish the runtime handed a broker, as the harness recorded it: the message, the options
/// it went with, and when the hand-over began and ended on the harness's clock.
#[derive(Clone, Debug)]
pub(crate) struct RuntimePublish {
    pub(crate) message: RawMessage,
    pub(crate) options: RecordedOptions,
    /// The clock when the message was captured, before the broker's publisher ran.
    pub(crate) started: u64,
    /// The clock once the broker took it.
    pub(crate) finished: u64,
}

impl RuntimePublish {
    /// Whether the two hand-overs ran at the same time, so the order their messages reached a
    /// broker's log is not the order they were recorded in.
    const fn overlaps(&self, other: &Self) -> bool {
        self.started < other.finished && other.started < self.finished
    }
}

/// What the harness knows about the per-message options of one message on a broker's view.
#[derive(Clone, Debug)]
pub(crate) enum MessageOptions {
    /// The runtime published the message with these options.
    Recorded(RecordedOptions),
    /// The harness has no record of the message: the test published it itself, a publisher the
    /// runtime did not pair made it, or the broker changed its payload on the way.
    Unrecorded,
    /// The message has the same bytes as other publishes to the destination, and the harness
    /// cannot tell which of them carried which options.
    Ambiguous,
}

/// The options of each message in a broker's publish log for one destination, read off the
/// runtime's publishes the harness recorded for the same broker and destination.
///
/// The broker's own publisher may add headers, so messages are matched by payload, which it hands
/// on as it was given. Within one payload, the log and the records are paired in order when every
/// message is recorded and no two of them were in flight at once; otherwise a message is
/// attributed only when every record it could belong to certainly holds the same options, and is
/// [`MessageOptions::Ambiguous`] where a guess could name the options of another publish.
pub(crate) fn attribute(log: &[RawMessage], records: &[RuntimePublish]) -> Vec<MessageOptions> {
    let mut attributed = vec![MessageOptions::Unrecorded; log.len()];
    let mut seen = vec![false; log.len()];
    for first in 0..log.len() {
        if seen[first] {
            continue;
        }
        let payload = log[first].payload();
        let in_log: Vec<usize> = (first..log.len())
            .filter(|&index| log[index].payload() == payload)
            .collect();
        for &index in &in_log {
            seen[index] = true;
        }
        let candidates: Vec<&RuntimePublish> = records
            .iter()
            .filter(|record| record.message.payload() == payload)
            .collect();
        let Some(earliest) = candidates.first() else {
            continue;
        };
        let concurrent = candidates
            .iter()
            .enumerate()
            .any(|(at, one)| candidates[at + 1..].iter().any(|other| one.overlaps(other)));
        if candidates.len() == in_log.len() && !concurrent {
            for (&index, record) in in_log.iter().zip(&candidates) {
                attributed[index] = MessageOptions::Recorded(record.options.clone());
            }
        } else if candidates.len() >= in_log.len()
            && candidates
                .iter()
                .all(|record| record.options.certainly_same_as(&earliest.options))
        {
            for &index in &in_log {
                attributed[index] = MessageOptions::Recorded(earliest.options.clone());
            }
        } else {
            for &index in &in_log {
                attributed[index] = MessageOptions::Ambiguous;
            }
        }
    }
    attributed
}

#[cfg(test)]
mod tests {
    use super::{MessageOptions, RecordedOptions, RuntimePublish, attribute};
    use crate::RawMessage;

    fn message(payload: &[u8]) -> RawMessage {
        RawMessage::new("orders".to_owned(), payload.to_vec())
    }

    fn sent(payload: &[u8], options: Option<u8>, started: u64, finished: u64) -> RuntimePublish {
        RuntimePublish {
            message: message(payload),
            options: RecordedOptions::capture(options.as_ref()),
            started,
            finished,
        }
    }

    /// Each message as the assertions see it: the recorded value, or why there is none.
    fn read(attributed: &[MessageOptions]) -> Vec<Result<Option<u8>, &'static str>> {
        attributed
            .iter()
            .map(|one| match one {
                MessageOptions::Recorded(options) => Ok(options.downcast::<u8>("orders").copied()),
                MessageOptions::Unrecorded => Err("unrecorded"),
                MessageOptions::Ambiguous => Err("ambiguous"),
            })
            .collect()
    }

    #[test]
    fn a_message_outside_the_record_has_no_options_and_the_rest_keep_theirs() {
        let log = [message(b"reply"), message(b"own"), message(b"slot")];
        let records = [sent(b"reply", Some(1), 0, 1), sent(b"slot", None, 2, 3)];

        assert_eq!(
            read(&attribute(&log, &records)),
            [Ok(Some(1)), Err("unrecorded"), Ok(None)],
        );
    }

    /// Identical bytes published one after another keep their order, so each keeps its options.
    #[test]
    fn identical_payloads_published_in_turn_pair_in_order() {
        let log = [message(b"order"), message(b"order")];
        let records = [sent(b"order", None, 0, 1), sent(b"order", Some(9), 2, 3)];

        assert_eq!(read(&attribute(&log, &records)), [Ok(None), Ok(Some(9))]);
    }

    /// Two publishes of the same bytes in flight at once may reach the log in either order.
    #[test]
    fn identical_payloads_in_flight_at_once_are_ambiguous() {
        let log = [message(b"order"), message(b"order")];
        let records = [sent(b"order", None, 0, 3), sent(b"order", Some(9), 1, 2)];

        assert_eq!(
            read(&attribute(&log, &records)),
            [Err("ambiguous"), Err("ambiguous")],
        );
    }

    /// A message the harness did not record, with the bytes of one it did: either could be last.
    #[test]
    fn an_unrecorded_copy_of_a_recorded_payload_is_ambiguous() {
        let log = [message(b"order"), message(b"order")];
        let records = [sent(b"order", Some(9), 0, 1)];

        assert_eq!(
            read(&attribute(&log, &records)),
            [Err("ambiguous"), Err("ambiguous")],
        );
    }

    /// Publishes that all took the policy's defaults leave nothing to tell apart.
    #[test]
    fn identical_payloads_with_default_options_are_not_ambiguous() {
        let log = [message(b"order"), message(b"order")];
        let records = [sent(b"order", None, 0, 3), sent(b"order", None, 1, 2)];

        assert_eq!(read(&attribute(&log, &records)), [Ok(None), Ok(None)]);
    }
}
