#!/usr/bin/env python3
"""Forbid inline Rust fences in the documentation pages, in every language.

Every ```rust fence must either embed a compiled snippet (a `reference="<path>#<region>"`
on the fence) or be explicitly exempted by an MDX comment on the line directly above it:

    {/* inline-rust: <why this cannot come from a compiled example> */}
    ```rust

The justification is mandatory. Exemptions are for code that has no compilable home in
this repository: simplified trait sketches (the real signatures are RPITIT with long doc
comments) and walk-throughs of another crate's internals.
"""

import re
import sys
from pathlib import Path

FENCE = re.compile(r"^```rust\b")
SNIPPET = re.compile(r'(?:^|\s)reference="[^"]+"')
EXEMPT = re.compile(r"\{/\*\s*inline-rust:\s*\S")
ROOT = Path(__file__).resolve().parent.parent
PAGES = [ROOT / "docs", *sorted((ROOT / "website" / "i18n").glob("*/docusaurus-plugin-content-docs"))]

def lint(path: Path) -> list[str]:
    errors = []
    lines = path.read_text().splitlines()
    inside = False
    fence_line = 0
    allowed = False
    for n, line in enumerate(lines, 1):
        if not inside and FENCE.match(line.strip()):
            inside = True
            fence_line = n
            exempt = n > 1 and bool(EXEMPT.search(lines[n - 2]))
            allowed = exempt or bool(SNIPPET.search(line))
        elif inside and line.strip() == "```":
            inside = False
            if not allowed:
                errors.append(
                    f"{path}:{fence_line}: inline rust fence - embed a compiled"
                    ' snippet (reference="<path>#<region>") or add an'
                    " `{/* inline-rust: why */}` justification on the previous line"
                )
    return errors

def main() -> int:
    errors = []
    for root in PAGES:
        for path in sorted(root.rglob("*.md")):
            errors.extend(lint(path))
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        print(f"{len(errors)} inline rust fence(s)", file=sys.stderr)
    return 1 if errors else 0

if __name__ == "__main__":
    raise SystemExit(main())
