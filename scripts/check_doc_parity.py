#!/usr/bin/env python3
"""Require an edit to a documentation page to reach that page's other languages.

The rule is symmetric: it takes its input from changed pages in any language, not from
English alone.

Three properties bound what it enforces:

- A page with no counterpart yet is skipped, not failed. Demanding a translation that has
  not been written would block every edit until the whole site is translated. The skip is
  temporary: once every page exists in every language, a missing counterpart is a failure
  like any other.
- Adding a page does not demand an edit elsewhere: only pages that were modified, deleted or
  renamed put the rule in force.
- It checks that a file changed, not that it changed correctly, and one character satisfies
  it. It catches "forgot the translation entirely", nothing more, and is paired with a
  review of the translation itself.

The declared way out is a label on the pull request, handled in the workflow rather than
here. Without one the way out becomes a token edit to the translated file, which marks a
page as revisited when nobody read it.
"""

import argparse
import subprocess
import sys
from pathlib import Path

# Keep in sync with the `i18n` plugin's language list in `properdocs.yml`.
LOCALES = ("ru", "zh")

DOCS_PREFIX = "docs/"
# Statuses that put the rule in force: the page existed and its content moved. A rename that
# changed nothing (`R100`) is excluded, because relocating a file says nothing about what it
# says: moving the whole tree from one layout to another would otherwise demand an edit to
# every page in every language to prove that none of them changed.
TRIGGERING = ("M", "D", "R")

REPO = Path(__file__).resolve().parent.parent


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout


def family(path: str) -> tuple[str, dict[str, str]]:
    """The English original of a page and every locale counterpart it could have."""
    relative = path[len(DOCS_PREFIX) :]
    head, _, rest = relative.partition("/")
    # A path that starts with a locale directory is a translation; anything else is the original.
    if head in LOCALES:
        relative = rest
    english = f"{DOCS_PREFIX}{relative}"
    return english, {loc: f"{DOCS_PREFIX}{loc}/{relative}" for loc in LOCALES}


def changed_pages(base: str) -> dict[str, str]:
    """Documentation pages in the diff against `base`, mapped to their change status."""
    merge_base = git("merge-base", base, "HEAD").strip()
    raw = git("diff", "--name-status", "-M", merge_base, "HEAD")
    pages: dict[str, str] = {}
    for line in raw.splitlines():
        fields = line.split("\t")
        code = fields[0]
        status = code[0]
        if status == "R" and code[1:] == "100":
            continue
        # A rename reports the old path and the new one; both sides are part of the diff.
        for path in fields[1:]:
            if path.startswith(DOCS_PREFIX) and path.endswith(".md"):
                pages[path] = status
    return pages


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base",
        default="origin/main",
        help="branch, tag or commit the pull request targets (default: origin/main)",
    )
    args = parser.parse_args()

    pages = changed_pages(args.base)
    if not pages:
        print("no documentation pages changed")
        return 0

    errors: list[str] = []
    for path, status in sorted(pages.items()):
        if status not in TRIGGERING:
            continue
        english, translations = family(path)
        for member in (english, *translations.values()):
            if member == path or member in pages:
                continue
            if not (REPO / member).exists():
                continue
            errors.append(f"{path} changed, but its counterpart {member} did not")

    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        print(
            "\nA change to what a page says belongs in every language that page exists in."
            "\nIf this edit is confined to one language (spelling, grammar, or a word choice"
            "\nthat was already right elsewhere), label the pull request and this job is"
            "\nskipped; see .github/workflows/docs.yml for the label name.",
            file=sys.stderr,
        )
        return 1
    print(f"checked {len(pages)} changed documentation page(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
