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

An example region shown outside the Testing pages is what a service writes, so it holds no test (the
tests that prove it sit in the same file, after the region's end marker), no `timeout` or `sleep`
and no stream polled by hand, and opens with an item
or an attribute, so the reader sees the signature of the code it shows, never a statement taken
out of the middle of a function. An inline fence outside the Testing pages holds no test either. A region on a Testing page is
the test that calls the documented function, without the handler and `fn app` above it.

A component page reads in one order: the title, a one-sentence summary, the description, then
`## Example` with the code. So under `components/` no code (a rust fence or a `<Tabs>` block)
comes before `## Example`, the summary is followed by at least one paragraph of description, and
the page has no "How it works" or "See also" heading: links to API items and related pages sit
in the text at their first mention. A category page lists none of the pages below it, under a
"Pages in this category" heading or as a list of links: the sidebar lists them, and it is rendered
into every page's HTML.
"""

import re
import sys
from pathlib import Path

FENCE = re.compile(r"^```rust\b")
SNIPPET = re.compile(r'(?:^|\s)reference="([^"#]+)')
REGION = re.compile(r'(?:^|\s)reference="[^"#]+#([^"]+)"')
MARKER = re.compile(r"^\s*// --8<-- \[(start|end):([^\]]+)\]\s*$")
TEST_ATTR = re.compile(r"^\s*#\[(?:tokio::)?test\b")
# Waiting and polling a stream by hand are test machinery; a service shows neither.
TEST_MACHINERY = re.compile(r"\b(?:timeout|sleep)\s*\(|\.next\(\)\s*\.await")
SERVICE = re.compile(r"^\s*(?:#\[subscriber\b|(?:pub(?:\([^)]*\))?\s+)?fn app\b)")
COMMENT = re.compile(r"^\s*//")
ITEM = re.compile(
    r"^\s*(?:#!?\[|(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:const\s+)?(?:unsafe\s+)?"
    r"(?:fn|use|struct|enum|impl|mod|const|static|type|trait|macro_rules!)\b)"
)
EXEMPT = re.compile(r"\{/\*\s*inline-rust:\s*\S")
SINGLE_FORM = re.compile(r"\{/\*\s*single-form:\s*\S")
TABS_OPEN = re.compile(r"<Tabs\b")
TABS_CLOSE = re.compile(r"</Tabs>")
TAB_OPEN = re.compile(r'<TabItem\b[^>]*\bvalue="([^"]+)"')
TAB_CLOSE = re.compile(r"</TabItem>")
EXAMPLES = "examples/"
MANUAL = "examples/manual/"
ROOT = Path(__file__).resolve().parent.parent
# Paths within a documentation tree: the English one under `docs/` and each translation under
# `website/i18n/<locale>/docusaurus-plugin-content-docs/current/` hold the same pages.
TESTING_PAGES = ("components/application/testing/",)
COMPONENT_PAGES = ("components/",)
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*$")
BANNED_HEADINGS = ("how it works", "see also", "pages in this category")
# A list item that opens with a link to a page beside or below this one.
PAGE_LIST_ITEM = re.compile(r"^\s*[-*+]\s+\[[^\]]+\]\(\./")


def region_lines(source: Path, name: str) -> list[str] | None:
    """The lines of region `name` in `source`, markers dropped; None when it is not there."""
    lines = None
    for line in source.read_text().splitlines():
        marker = MARKER.match(line)
        if marker:
            kind, region = marker.groups()
            if region == name:
                if kind == "start" and lines is None:
                    lines = []
                elif kind == "end" and lines is not None:
                    return lines
            continue
        if lines is not None:
            lines.append(line)
    return lines


def testing_region_errors(path: Path, n: int, target: str, region: str) -> list[str]:
    """A Testing page shows the test that calls its function; the service sits above it."""
    source = ROOT / target
    lines = region_lines(source, region) if source.is_file() else None
    if lines is None:
        return []
    where = f"{path}:{n}: {target}#{region}"
    errors = []
    if not any(TEST_ATTR.match(line) for line in lines):
        errors.append(f"{where} holds no test - a Testing page shows the test it documents")
    if any(SERVICE.match(line) for line in lines):
        errors.append(
            f"{where} shows the service - move the region's start marker below the handler and"
            " `fn app`, so the region holds only the test"
        )
    return errors


def service_region_errors(path: Path, n: int, target: str, region: str) -> list[str]:
    source = ROOT / target
    if not source.is_file():
        return []
    lines = region_lines(source, region)
    if lines is None:
        return []
    errors = []
    where = f"{path}:{n}: {target}#{region}"
    if any(TEST_ATTR.match(line) for line in lines):
        errors.append(
            f"{where} holds a test - a page outside Testing shows the service only; move the"
            " test after the region's end marker"
        )
    if any(TEST_MACHINERY.search(line) for line in lines if not COMMENT.match(line)):
        errors.append(
            f"{where} waits or polls a stream by hand (`timeout`, `sleep`, `.next().await`) -"
            " that is test machinery; keep it after the region's end marker"
        )
    first = next((line for line in lines if line.strip() and not COMMENT.match(line)), None)
    if first is not None and not ITEM.match(first):
        errors.append(
            f"{where} opens with `{first.strip()}` - start the region at the item it belongs to"
            " (its `fn`, `struct`, `impl`, attribute or `use`) so the reader sees its signature"
        )
    return errors
PAGES = [ROOT / "docs", *sorted((ROOT / "website" / "i18n").glob("*/docusaurus-plugin-content-docs"))]


def page_path(path: Path) -> str:
    """The page's path within its documentation tree, the same for a page and its translations."""
    resolved = path.resolve()
    for tree in PAGES:
        if resolved.is_relative_to(tree.resolve()):
            inner = resolved.relative_to(tree.resolve()).parts
            # A translation sits one level deeper, under the docs version (`current/`).
            if tree.name == "docusaurus-plugin-content-docs" and inner:
                inner = inner[1:]
            return Path(*inner).as_posix()
    return resolved.relative_to(ROOT).as_posix()

def component_order_errors(path: Path, lines: list[str]) -> list[str]:
    """A component page: summary, description, then `## Example`; no How it works or See also."""
    errors = []
    title = example = None
    paragraphs = 0
    in_paragraph = False
    in_fence = False
    for n, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("```"):
            if not in_fence and example is None:
                errors.append(f"{path}:{n}: code before `## Example` - the description comes first")
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        heading = HEADING.match(line)
        if heading:
            level, text = len(heading.group(1)), heading.group(2)
            if text.lower() in BANNED_HEADINGS:
                errors.append(
                    f"{path}:{n}: `{line.strip()}` - a component page has no such section; the"
                    " description follows the summary, and links sit in the text"
                )
            if level == 1 and title is None:
                title = n
            elif level == 2 and text == "Example" and example is None:
                example = n
                if paragraphs < 2:
                    errors.append(
                        f"{path}:{n}: `## Example` right after the summary - the description"
                        " comes between them"
                    )
            in_paragraph = False
            continue
        if PAGE_LIST_ITEM.match(line):
            errors.append(
                f"{path}:{n}: a list of the pages below - the sidebar lists them; a link to a"
                " page sits in the text at its first mention"
            )
        if TABS_OPEN.search(line) and example is None:
            errors.append(f"{path}:{n}: tabs before `## Example` - the description comes first")
        if title is not None and example is None:
            if stripped and not stripped.startswith(("{/*", "<", "import ")):
                if not in_paragraph:
                    paragraphs += 1
                in_paragraph = True
            elif not stripped:
                in_paragraph = False
    return errors


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
    relative = page_path(path)
    # The Testing pages and the broker contract's pages document test suites: tests are their
    # subject.
    testing_page = relative.startswith(TESTING_PAGES)
    if relative.startswith(COMPONENT_PAGES):
        errors.extend(component_order_errors(path, lines))
    snippet_fence = False
    for n, line in enumerate(lines, 1):
        stripped = line.strip()
        if inside:
            if not snippet_fence and not testing_page and TEST_ATTR.match(line):
                errors.append(
                    f"{path}:{n}: a test inside an inline rust fence - a page outside Testing"
                    " shows the service only; keep the test beside the code, off the page"
                )
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
            snippet_fence = bool(snippet)
            allowed = bool(EXEMPT.search(previous)) or bool(snippet)
            if snippet:
                target = snippet.group(1)
                region = REGION.search(line)
                if region and target.startswith(EXAMPLES) and relative.startswith(TESTING_PAGES):
                    errors.extend(testing_region_errors(path, n, target, region.group(1)))
                # The rule is about a service's code, which lives under `examples/`; a page that
                # documents a test suite (the broker contract) shows tests on purpose.
                if (
                    region
                    and target.startswith(EXAMPLES)
                    and not testing_page
                ):
                    errors.extend(service_region_errors(path, n, target, region.group(1)))
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
