#!/usr/bin/env python3
"""Hold the Rust fences of the documentation pages, in every language, to two rules.

Every ```rust fence must either embed a compiled snippet (a `reference="<path>#<region>"`
on the fence) or be explicitly exempted by an MDX comment on the line directly above it:

    {/* inline-rust: <why this cannot come from a compiled example> */}
    ```rust

The justification is mandatory. Exemptions are for code that has no compilable home in
this repository: simplified trait sketches (the real signatures are RPITIT with long doc
comments) and walk-throughs of another crate's internals.

A service's code is shown in two forms, with the macros and without them, as two tabs of one
`<Tabs groupId="api">` block: `<TabItem value="macros">` and `<TabItem value="manual">`. So a
fence that references `examples/manual/` sits in a `manual` tab, and a fence that references
any other file under `examples/` sits in a `macros` tab whose block also has a `manual` tab.
A region that has one form only (a broker constructor, a test driver, no macro in reach) says
why on the line above the fence:

    {/* single-form: <why this code has no macro form> */}
    ```rust reference="examples/seek.rs#retaining"
"""

import re
import sys
from pathlib import Path

FENCE = re.compile(r"^```rust\b")
SNIPPET = re.compile(r'(?:^|\s)reference="([^"#]+)')
EXEMPT = re.compile(r"\{/\*\s*inline-rust:\s*\S")
SINGLE_FORM = re.compile(r"\{/\*\s*single-form:\s*\S")
TABS_OPEN = re.compile(r"<Tabs\b")
TABS_CLOSE = re.compile(r"</Tabs>")
TAB_OPEN = re.compile(r'<TabItem\b[^>]*\bvalue="([^"]+)"')
TAB_CLOSE = re.compile(r"</TabItem>")
EXAMPLES = "examples/"
MANUAL = "examples/manual/"
ROOT = Path(__file__).resolve().parent.parent
PAGES = [ROOT / "docs", *sorted((ROOT / "website" / "i18n").glob("*/docusaurus-plugin-content-docs"))]

def lint(path: Path) -> list[str]:
    errors = []
    lines = path.read_text().splitlines()
    inside = False
    fence_line = 0
    allowed = False
    tab = None
    # Per open `<Tabs>` block: whether it has a manual tab, and the macro fences it holds.
    block_has_manual = False
    block_macro_fences: list[int] = []
    for n, line in enumerate(lines, 1):
        stripped = line.strip()
        if inside:
            if stripped == "```":
                inside = False
                if not allowed:
                    errors.append(
                        f"{path}:{fence_line}: inline rust fence - embed a compiled"
                        ' snippet (reference="<path>#<region>") or add an'
                        " `{/* inline-rust: why */}` justification on the previous line"
                    )
            continue
        if FENCE.match(stripped):
            inside = True
            fence_line = n
            previous = lines[n - 2] if n > 1 else ""
            snippet = SNIPPET.search(line)
            allowed = bool(EXEMPT.search(previous)) or bool(snippet)
            if snippet:
                target = snippet.group(1)
                if target.startswith(MANUAL):
                    if tab != "manual":
                        errors.append(
                            f"{path}:{n}: {target} is the manual form - show it in a"
                            ' `<TabItem value="manual">` beside its macro form'
                        )
                elif target.startswith(EXAMPLES) and not SINGLE_FORM.search(previous):
                    if tab == "macros":
                        block_macro_fences.append(n)
                    else:
                        errors.append(
                            f"{path}:{n}: {target} is shown outside a macros tab - put it in"
                            ' `<TabItem value="macros">` with its manual twin in'
                            ' `<TabItem value="manual">`, or say why it has one form in'
                            " `{/* single-form: why */}` on the previous line"
                        )
            continue
        if TABS_OPEN.search(line):
            block_has_manual = False
            block_macro_fences = []
        tab_open = TAB_OPEN.search(line)
        if tab_open:
            tab = tab_open.group(1)
            block_has_manual = block_has_manual or tab == "manual"
        if TAB_CLOSE.search(line):
            tab = None
        if TABS_CLOSE.search(line):
            if not block_has_manual:
                errors.extend(
                    f'{path}:{fence}: a macro example with no `<TabItem value="manual">`'
                    " beside it - add the manual form as the second tab"
                    for fence in block_macro_fences
                )
            block_macro_fences = []
    return errors

def main() -> int:
    errors = []
    for root in PAGES:
        for path in sorted(root.rglob("*.md")):
            errors.extend(lint(path))
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        print(f"{len(errors)} rust fence problem(s)", file=sys.stderr)
    return 1 if errors else 0

if __name__ == "__main__":
    raise SystemExit(main())
