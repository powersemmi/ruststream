//! The post-settle registrations of one delivery on a device without a heap.
//!
//! A delivery holds a fixed number of `ctx.after*` registrations. A registration that does not fit
//! is refused where it is made, so nothing registered is ever dropped: [`Registrations::vacancy`]
//! returns the room for one more or [`RegistrationsFull`], and a [`Vacancy`] is the proof that
//! the entry put into it fits.

use core::array;
use core::error::Error;
use core::fmt;
use core::mem;

use heapless::Vec;

/// How many `ctx.after*` registrations one delivery holds.
pub(crate) const REGISTRATIONS: usize = 4;

/// The delivery's registrations are all taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationsFull;

impl fmt::Display for RegistrationsFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the delivery holds no more post-settle registrations")
    }
}

impl Error for RegistrationsFull {}

/// One registration: the future to run, the outcome kind it is gated on (`None` runs it
/// regardless of how the message settled) and the `.error` registered beside it.
#[derive(Debug)]
pub(crate) struct Entry<Gate, Work, Error> {
    pub(crate) gate: Option<Gate>,
    pub(crate) fut: Option<Work>,
    pub(crate) error: Option<Error>,
}

/// The registrations of one delivery, in the order they were made.
pub(crate) struct Registrations<Gate, Work, Error, const SLOTS: usize> {
    slots: [Option<Entry<Gate, Work, Error>>; SLOTS],
    len: usize,
}

/// Room for one more registration, held until the entry is put into it.
pub(crate) struct Vacancy<'registrations, Gate, Work, Error, const SLOTS: usize> {
    registrations: &'registrations mut Registrations<Gate, Work, Error, SLOTS>,
}

impl<Gate, Work, Error, const SLOTS: usize> Registrations<Gate, Work, Error, SLOTS> {
    /// No registrations.
    pub(crate) fn new() -> Self {
        Self {
            slots: array::from_fn(|_| None),
            len: 0,
        }
    }

    /// Whether the delivery registered nothing.
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// How many registrations the delivery holds.
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// Room for one more registration, or [`RegistrationsFull`].
    pub(crate) fn vacancy(
        &mut self,
    ) -> Result<Vacancy<'_, Gate, Work, Error, SLOTS>, RegistrationsFull> {
        if self.len == SLOTS {
            return Err(RegistrationsFull);
        }
        Ok(Vacancy {
            registrations: self,
        })
    }

    /// The registration at `at`, as [`Vacancy::put`] returned it.
    pub(crate) fn entry_mut(&mut self, at: usize) -> &mut Entry<Gate, Work, Error> {
        self.slots[at]
            .as_mut()
            .expect("a registration index comes from `Vacancy::put`")
    }

    /// Runs every entry through `keep`, in order, and keeps what it returns.
    fn rebuild(
        &mut self,
        mut keep: impl FnMut(Entry<Gate, Work, Error>) -> Option<Entry<Gate, Work, Error>>,
    ) {
        let old = mem::replace(self, Self::new());
        for entry in old.slots.into_iter().flatten() {
            if let Some(kept) = keep(entry) {
                self.place(kept);
            }
        }
    }

    fn place(&mut self, entry: Entry<Gate, Work, Error>) -> usize {
        let at = self.len;
        self.slots[at] = Some(entry);
        self.len += 1;
        at
    }
}

impl<Gate: Copy + PartialEq, Work, Error, const SLOTS: usize>
    Registrations<Gate, Work, Error, SLOTS>
{
    /// Takes the works whose gate is `kind` or none, in registration order; the rest stay.
    pub(crate) fn take_for(&mut self, kind: Gate) -> Vec<Work, SLOTS> {
        self.take_where(|gate| gate.is_none_or(|gate| gate == kind))
    }

    /// Takes the works with no gate, in registration order; the rest stay.
    pub(crate) fn take_ungated(&mut self) -> Vec<Work, SLOTS> {
        self.take_where(|gate| gate.is_none())
    }

    fn take_where(&mut self, runs: impl Fn(Option<Gate>) -> bool) -> Vec<Work, SLOTS> {
        let mut runnable = Vec::new();
        self.rebuild(|entry| {
            if !runs(entry.gate) {
                return Some(entry);
            }
            if let Some(fut) = entry.fut {
                // At most `SLOTS` entries exist, so the list never overflows.
                let _ = runnable.push(fut);
            }
            None
        });
        runnable
    }

    /// Takes the reaction registered for a delivery that settles as `kind`: the first `.error`
    /// registered under it, with the work beside it. Every other registration of the kind keeps
    /// its work as a plain post-settle hook and loses its `.error`.
    pub(crate) fn take_reaction(&mut self, kind: Gate) -> Option<(Error, Option<Work>)> {
        let mut chosen = None;
        self.rebuild(|mut entry| {
            let Some(error) = entry.error.take() else {
                return Some(entry);
            };
            if chosen.is_none() && entry.gate == Some(kind) {
                chosen = Some((error, entry.fut.take()));
                return None;
            }
            entry.fut.is_some().then_some(entry)
        });
        chosen
    }

    /// Keeps the work of every registration as a plain post-settle hook and drops the errors.
    pub(crate) fn demote_errors(&mut self) {
        self.rebuild(|mut entry| {
            entry.error = None;
            entry.fut.is_some().then_some(entry)
        });
    }
}

impl<'registrations, Gate, Work, Error, const SLOTS: usize>
    Vacancy<'registrations, Gate, Work, Error, SLOTS>
{
    /// Puts `entry` into the room; the index is how the builder finds it again.
    pub(crate) fn put(self, entry: Entry<Gate, Work, Error>) -> usize {
        self.registrations.place(entry)
    }

    /// Puts `entry` into the room and lends it back, for a builder that completes it.
    pub(crate) fn put_mut(
        self,
        entry: Entry<Gate, Work, Error>,
    ) -> &'registrations mut Entry<Gate, Work, Error> {
        let at = self.registrations.place(entry);
        self.registrations.entry_mut(at)
    }
}

#[cfg(test)]
mod tests {
    use std::format;

    use super::{Entry, REGISTRATIONS, Registrations, RegistrationsFull};

    #[test]
    fn a_delivery_holds_four_registrations() {
        let mut table = Registrations::<u8, (), (), REGISTRATIONS>::new();
        for _ in 0..REGISTRATIONS {
            table.vacancy().expect("room").put(Entry {
                gate: None,
                fut: Some(()),
                error: None,
            });
        }
        assert_eq!(table.len(), 4);
        assert!(table.vacancy().is_err());
    }

    const ACK: u8 = 1;
    const RETRY: u8 = 2;

    type Table = Registrations<u8, &'static str, &'static str, 3>;

    fn entry(
        gate: Option<u8>,
        fut: Option<&'static str>,
        error: Option<&'static str>,
    ) -> Entry<u8, &'static str, &'static str> {
        Entry { gate, fut, error }
    }

    fn fill(
        table: &mut Table,
        entries: impl IntoIterator<Item = Entry<u8, &'static str, &'static str>>,
    ) {
        for entry in entries {
            table.vacancy().expect("room").put(entry);
        }
    }

    #[test]
    fn a_registration_beyond_the_capacity_is_refused_and_the_held_ones_stay() {
        let mut table = Table::new();
        fill(
            &mut table,
            [
                entry(Some(ACK), Some("a"), None),
                entry(Some(ACK), Some("b"), None),
                entry(None, Some("c"), None),
            ],
        );

        let full = table.vacancy().err().expect("the table is full");
        assert_eq!(full, RegistrationsFull);
        assert_eq!(
            format!("{full}"),
            "the delivery holds no more post-settle registrations"
        );
        assert_eq!(table.len(), 3);
        assert_eq!(table.take_for(ACK).as_slice(), ["a", "b", "c"]);
    }

    #[test]
    fn the_index_of_a_put_entry_finds_it_again() {
        let mut table = Table::new();
        fill(&mut table, [entry(Some(ACK), Some("a"), None)]);
        let at = table
            .vacancy()
            .expect("room")
            .put(entry(Some(RETRY), None, None));
        assert_eq!(at, 1);

        table.entry_mut(at).fut = Some("later");

        assert_eq!(table.take_for(RETRY).as_slice(), ["later"]);
    }

    #[test]
    fn a_builder_completes_the_entry_it_was_lent() {
        let mut table = Table::new();

        let lent = table
            .vacancy()
            .expect("room")
            .put_mut(entry(Some(ACK), Some("then"), None));
        lent.error = Some("error");

        assert_eq!(table.take_reaction(ACK), Some(("error", Some("then"))));
    }

    #[test]
    fn take_for_returns_the_matching_and_ungated_in_order_and_keeps_the_rest() {
        let mut table = Table::new();
        fill(
            &mut table,
            [
                entry(Some(ACK), Some("ack"), None),
                entry(Some(RETRY), Some("retry"), None),
                entry(None, Some("always"), None),
            ],
        );

        assert_eq!(table.take_for(ACK).as_slice(), ["ack", "always"]);
        assert_eq!(table.len(), 1);
        assert_eq!(table.take_for(RETRY).as_slice(), ["retry"]);
        assert!(table.is_empty());
    }

    #[test]
    fn take_ungated_leaves_the_gated_ones() {
        let mut table = Table::new();
        fill(
            &mut table,
            [
                entry(Some(ACK), Some("ack"), None),
                entry(None, Some("always"), None),
            ],
        );

        assert_eq!(table.take_ungated().as_slice(), ["always"]);
        assert_eq!(table.take_for(ACK).as_slice(), ["ack"]);
    }

    #[test]
    fn the_reaction_is_the_first_error_of_the_kind_and_the_others_keep_only_their_work() {
        let mut table = Table::new();
        fill(
            &mut table,
            [
                entry(Some(ACK), Some("first then"), Some("first error")),
                entry(Some(ACK), Some("second then"), Some("second error")),
                entry(Some(ACK), None, Some("error alone")),
            ],
        );

        assert_eq!(
            table.take_reaction(ACK),
            Some(("first error", Some("first then")))
        );
        assert_eq!(table.len(), 1);
        assert_eq!(table.take_for(ACK).as_slice(), ["second then"]);
    }

    #[test]
    fn no_reaction_when_no_error_is_registered_under_the_kind() {
        let mut table = Table::new();
        fill(
            &mut table,
            [
                entry(Some(RETRY), Some("retry then"), Some("retry error")),
                entry(Some(ACK), Some("ack then"), None),
            ],
        );

        assert_eq!(table.take_reaction(ACK), None);
        // The retry registration lost its `.error` and kept its work for a retry settlement.
        assert_eq!(table.len(), 2);
        assert_eq!(table.take_for(ACK).as_slice(), ["ack then"]);
        assert_eq!(table.take_for(RETRY).as_slice(), ["retry then"]);
    }

    #[test]
    fn demoting_errors_keeps_the_works_and_drops_the_registrations_with_none() {
        let mut table = Table::new();
        fill(
            &mut table,
            [
                entry(Some(ACK), Some("then"), Some("error")),
                entry(Some(ACK), None, Some("error alone")),
            ],
        );

        table.demote_errors();

        assert_eq!(table.len(), 1);
        assert_eq!(table.take_reaction(ACK), None);
        assert_eq!(table.take_for(ACK).as_slice(), ["then"]);
    }
}
