#!/usr/bin/env python3
"""Forbid inline Rust fences in docs/.

Every ```rust fence must either embed a compiled snippet (a --8<-- include) or be
explicitly exempted by an HTML comment on the line directly above it:

    <!-- inline-rust: <why this cannot come from a compiled example> -->
    ```rust

The justification is mandatory. Exemptions are for code that has no compilable home in
this repository: simplified trait sketches (the real signatures are RPITIT with long doc
comments) and walk-throughs of another crate's internals.
"""

import re
import sys
from pathlib import Path

FENCE = re.compile(r"^```rust\b")
SNIPPET = re.compile(r"--8<--")
EXEMPT = re.compile(r"<!--\s*inline-rust:\s*\S")

def lint(path: Path) -> list[str]:
    errors = []
    lines = path.read_text().splitlines()
    inside = False
    fence_line = 0
    exempt = False
    has_snippet = False
    for n, line in enumerate(lines, 1):
        if not inside and FENCE.match(line.strip()):
            inside = True
            fence_line = n
            exempt = n > 1 and bool(EXEMPT.search(lines[n - 2]))
            has_snippet = False
        elif inside:
            if SNIPPET.search(line):
                has_snippet = True
            if line.strip() == "```":
                inside = False
                if not has_snippet and not exempt:
                    errors.append(
                        f"{path}:{fence_line}: inline rust fence - embed a compiled"
                        " snippet (--8<--) or add an `<!-- inline-rust: why -->`"
                        " justification on the previous line"
                    )
    return errors

def main() -> int:
    docs = Path(__file__).resolve().parent.parent / "docs"
    errors = []
    for path in sorted(docs.rglob("*.md")):
        errors.extend(lint(path))
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        print(f"{len(errors)} inline rust fence(s)", file=sys.stderr)
    return 1 if errors else 0

if __name__ == "__main__":
    raise SystemExit(main())
