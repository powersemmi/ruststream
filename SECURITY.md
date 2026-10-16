# Security policy

## Supported versions

Security fixes ship as patch releases of the newest minor line of `ruststream` and
`ruststream-macros` (today 0.7.x). Before 1.0 a minor release may break the API, so older minor
lines receive no fixes; upgrading to the newest line is the fix.

Each broker crate (`ruststream-nats`, `ruststream-rdkafka` and the others) tracks the same minor
line and follows this policy in its own repository.

## Reporting a vulnerability

Report it privately through GitHub:
[open a security advisory](https://github.com/powersemmi/ruststream/security/advisories/new).
Do not open a public issue, pull request or discussion for it.

A useful report names the affected version and features, the broker crate if one is involved,
the impact, and the shortest code or configuration that reproduces it.

## What happens next

- The report is acknowledged within 7 days.
- The fix is prepared in a private fork of the advisory and released as a patch of the supported
  line.
- The advisory is published on GitHub with the fixed version, and submitted to the
  [RustSec advisory database](https://github.com/rustsec/advisory-db), so `cargo audit` and
  `cargo deny` report the affected versions.
- The reporter is credited in the advisory unless they ask otherwise.

## Scope

The published crates `ruststream` and `ruststream-macros`. The documentation site under
`website/` and its build tools are not shipped to users; a problem there is still welcome through
the same channel.
