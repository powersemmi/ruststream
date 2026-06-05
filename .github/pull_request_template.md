# Description

Please include a summary of the change and specify which issue is being addressed. Additionally,
provide relevant motivation and context.

Fixes # (issue number)

## Type of change

Please delete options that are not relevant.

- [ ] Documentation (typos, code examples, or any documentation updates)
- [ ] Bug fix (a non-breaking change that resolves an issue)
- [ ] New feature (a non-breaking change that adds functionality)
- [ ] Breaking change (removing or renaming public API, changing a signature, tightening bounds, or
      raising the MSRV - a minor version bump pre-1.0)
- [ ] This change requires a documentation update

## Checklist

- [ ] My code follows the project's style guidelines (`just check` passes: rustfmt, clippy, and
      `cargo check` with all features and with `--no-default-features`)
- [ ] I have performed a self-review of my own code
- [ ] I have made the necessary changes to the documentation (rustdoc and/or the docs site)
- [ ] My changes generate no new warnings (clippy runs with `-D warnings`)
- [ ] I have added tests that validate my fix or new feature
- [ ] New and existing tests pass locally (`just test`)
- [ ] Public items carry a compiling `# Examples` doctest, and user-facing changes are reflected in
      `examples/` where applicable

> `just ci` runs the full gate (`just check` + `just test`) in one go.
