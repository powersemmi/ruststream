---
name: ruststream-core
description: "Use when changing the RustStream core repository itself (powersemmi/ruststream: `src/`, `crates/ruststream-macros`, `tests/`, `examples/`, the site), as a contributor: public items, errors, features, doctests, compile-fail snapshots, conformance suites, the local gates and the pull request."
---

# Contributing to the RustStream core

The core is one published crate, `ruststream`, plus the proc-macro crate
`crates/ruststream-macros`, released in lockstep. It is broker-agnostic, 100% safe Rust
(`#![forbid(unsafe_code)]`), and its surface sits behind strictly additive cargo features. The
environment, the gates and the pull request flow are in `CONTRIBUTING.md`; read it first.

## A public item, the way the crate writes it

```rust
use thiserror::Error;

/// Why a string was refused as a subject.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum SubjectError {
    /// The subject is empty.
    #[error("a subject must not be empty")]
    Empty,
}

impl Subject {
    /// Builds a subject, refusing an empty one.
    ///
    /// # Errors
    ///
    /// Returns [`SubjectError::Empty`] for an empty string.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
    /// # mod demo {
    /// // A full example in the service's own syntax: the item where a service uses it.
    /// # }
    /// # fn main() {}
    /// ```
    pub fn try_new(subject: impl Into<String>) -> Result<Self, SubjectError> {
        let subject = subject.into();
        if subject.is_empty() {
            return Err(SubjectError::Empty);
        }
        Ok(Self(subject))
    }
}
```

## Rules

- **Visibility.** Default to `pub(crate)`; `pub` is what `lib.rs` re-exports. The crate denies
  `missing_docs`: every public item has a one-line summary, and every one outside `#[doc(hidden)]`
  a compiling `# Examples` doctest that shows the item used where a caller uses it (a handler, a
  mount chain, a broker author's impl), with `?` rather than `unwrap()`. A fallible function has
  `# Errors`, a panicking one `# Panics`.
- **Errors.** `thiserror` in the library (`anyhow` only in bins and examples), public error enums
  `#[non_exhaustive]`, no panics on user or network input.
- **Broker-agnostic.** No broker client and no broker-specific default in the core. A field with no
  sane default gets no `Default`. Settlement is honest: every settling site accepts
  `AckError::Unsupported`.
- **Async.** Native `async fn` in traits (RPITIT), `+ Send` on the returned future, no forced object
  safety; erasure lives in `runtime`. Destructors never panic or block.
- **Compile-time first.** A misuse is rejected by the compiler where the design allows: an enum
  instead of sibling `bool` / `Option` fields, a newtype or `NonZero*` instead of adjacent
  same-typed parameters, a consuming `self` transition or a phase type instead of a "call X before
  Y" rule. Bounds users hit carry `#[diagnostic::on_unimplemented]`. A runtime check stays only
  where the force is external, with a comment naming it.
- **Zero cost.** No allocation, `dyn` call, extra atomic or copy per message on the delivery and
  publish paths. A change there cites `just bench --baseline=main`.
- **A broker-facing rule is a conformance check.** A behaviour a broker must follow ships with its
  check in `conformance` in the same pull request, exercised the way a service reaches it.
- **Tests.** Unit tests in the module's own `#[cfg(test)] mod tests`; public API tests in `tests/`
  through the `TestApp` harness; no mocks; no `sleep` for synchronization. Compile-fail snapshots
  change only after reading the diff.
- **Docs move with the code.** Rustdoc says what an item is; a component page on the site says how
  a topic is done, with a Macros and a Manual tab of compiled regions from `examples/`; the agent
  skills in `.agents/skills/` and the rules in `context7.json` name the recommended path. A page
  in Guides or Architecture changes in English, Russian and Chinese together.
- **Text.** English only in files, no emoji, no em-dashes or en-dashes.

## Before review

```bash
just ci                    # just check (fmt, clippy, feature edges, rustdoc, compile-fail) + just test
just brokers               # when the change touches what broker crates use
```

The site gates run in `website/` (`npm ci && npm run typecheck && npm run test && npm run build`,
after `cargo doc --all-features --no-deps`), with `scripts/lint_doc_fences.py`,
`scripts/check_doc_parity.py` and `scripts/check_doc_i18n.py`. Pre-1.0 a minor bump is breaking
and a patch additive. A pull request opens as a draft and stays one until the maintainer marks it
ready; commits are signed. Releases are the maintainer's.
