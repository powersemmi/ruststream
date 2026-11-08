#!/usr/bin/env python3
"""Structural and terminological parity between a documentation page and its translations.

Three properties are checked, none of which needs a reviewer who reads the target language:

- The same snippet includes, in the same order. Code is never translated, so a difference
  here means the translation dropped or duplicated a block of code the original shows.
- The same heading levels, in the same order. Heading text is translated; the shape of the
  page is not.
- The locked rendering for every term of art the English page uses, per
  `scripts/doc_glossary.toml`.

Pages with no translation yet are skipped, not failed: translations land page by page, and
demanding all of them at once would block every edit until the site is fully translated.

The project README and its translations under `docs/readme/` are held to the same properties and
to one more: their code blocks are byte-identical, in the same order, because GitHub shows each
file as it is written. A README translation is never skipped, since the language line under the
title of every README links to it.

Run with no arguments; it exits non-zero on the first tree that violates any of the above.
"""

import difflib
import re
import sys
import tomllib
from itertools import zip_longest
from pathlib import Path

# Locales with a translation tree. Keep in sync with `i18n.locales` in
# `website/docusaurus.config.ts`.
LOCALES = ("ru", "zh")

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs"
# Each locale mirrors the English page paths in a tree of its own.
TRANSLATIONS = {
    locale: REPO / "website" / "i18n" / locale / "docusaurus-plugin-content-docs" / "current"
    for locale in LOCALES
}
GLOSSARY = REPO / "scripts" / "doc_glossary.toml"
# The README GitHub and crates.io render, and its translations: `README.<locale>.md` in a
# directory of `docs/` that the site leaves out.
README = REPO / "README.md"
README_TRANSLATIONS = DOCS / "readme"

FENCE = re.compile(r"^(?P<indent>\s*)(?P<ticks>```+|~~~+).*?^(?P=indent)(?P=ticks)\s*$", re.M | re.S)
HEADING = re.compile(r"^(#{1,6})\s+\S", re.M)
INCLUDE = re.compile(r'^\s*```.*?(?:^|\s)reference="(?P<target>[^"]+)"', re.M)
INLINE_CODE = re.compile(r"`[^`\n]+`")
COMMENT = re.compile(r"\{/\*.*?\*/\}", re.S)
LINK_TARGET = re.compile(r"\]\([^)]*\)")


def english_pages() -> list[Path]:
    """Every original page: a markdown file under `docs/`, outside the README translations."""
    return sorted(
        page for page in DOCS.rglob("*.md") if not page.is_relative_to(README_TRANSLATIONS)
    )


def translation_of(page: Path, locale: str) -> Path:
    return TRANSLATIONS[locale] / page.relative_to(DOCS)


def includes(text: str) -> list[str]:
    return [m.group("target") for m in INCLUDE.finditer(text)]


def headings(text: str) -> list[int]:
    return [len(m.group(1)) for m in HEADING.finditer(FENCE.sub("", text))]


def code_blocks(text: str) -> list[str]:
    """Every fenced block of a page, its fences and info string included, in page order."""
    return [m.group(0).strip("\n") for m in FENCE.finditer(text)]


def prose(text: str) -> str:
    """The page with everything that is not translated prose removed.

    Code, inline identifiers and link targets are the same in every language, so matching a
    glossary term against them would report a term as used where nothing was translated, and
    would accept a term as rendered where the translator only left a symbol name in place.
    """
    text = FENCE.sub("", text)
    text = COMMENT.sub("", text)
    text = INLINE_CODE.sub("", text)
    return LINK_TARGET.sub("]()", text)


def load_glossary() -> list[dict]:
    terms = tomllib.loads(GLOSSARY.read_text(encoding="utf-8"))["term"]
    for term in terms:
        term["_en"] = re.compile(rf"\b(?:{term.get('en_match', re.escape(term['en']))})\b", re.I)
        for locale in LOCALES:
            pattern = term.get(f"{locale}_match", re.escape(term[locale]))
            term[f"_{locale}"] = re.compile(pattern, re.I)
    return terms


def check_pair(page: Path, translated: Path, locale: str, terms: list[dict]) -> list[str]:
    original_text = page.read_text(encoding="utf-8")
    translated_text = translated.read_text(encoding="utf-8")
    rel = translated.relative_to(REPO)
    errors = []

    if includes(original_text) != includes(translated_text):
        errors.append(
            f"{rel}: snippet includes differ from {page.relative_to(REPO)}\n"
            f"    original:   {includes(original_text)}\n"
            f"    translated: {includes(translated_text)}"
        )
    if headings(original_text) != headings(translated_text):
        errors.append(
            f"{rel}: heading structure differs from {page.relative_to(REPO)}\n"
            f"    original:   {headings(original_text)}\n"
            f"    translated: {headings(translated_text)}"
        )

    original_prose, translated_prose = prose(original_text), prose(translated_text)
    for term in terms:
        if term["_en"].search(original_prose) and not term[f"_{locale}"].search(translated_prose):
            errors.append(
                f"{rel}: the original uses the term \"{term['en']}\"; the {locale} page has to"
                f" use its locked rendering \"{term[locale]}\" (scripts/doc_glossary.toml)"
            )
    return errors


def check_readme(locale: str, terms: list[dict]) -> list[str]:
    """The README translation of `locale` against the README: the page checks, then its code."""
    translated = README_TRANSLATIONS / f"README.{locale}.md"
    rel = translated.relative_to(REPO)
    if not translated.exists():
        return [f"{rel}: missing, while the language line of README.md links to it"]
    errors = check_pair(README, translated, locale, terms)
    original = code_blocks(README.read_text(encoding="utf-8"))
    translation = code_blocks(translated.read_text(encoding="utf-8"))
    for number, (block, translated_block) in enumerate(zip_longest(original, translation), 1):
        if block != translated_block:
            diff = difflib.unified_diff(
                (block or "").splitlines(),
                (translated_block or "").splitlines(),
                "README.md",
                str(rel),
                lineterm="",
                n=0,
            )
            errors.append(
                f"{rel}: code block {number} differs from README.md; code is the same in every"
                " language\n" + "\n".join(f"    {line}" for line in diff)
            )
            break
    return errors


def main() -> int:
    terms = load_glossary()
    errors: list[str] = []
    pairs = missing = 0

    originals = {page.relative_to(DOCS).as_posix(): page for page in english_pages()}
    for locale, tree in TRANSLATIONS.items():
        for path in sorted(tree.rglob("*.md")):
            relative = path.relative_to(tree).as_posix()
            if relative not in originals:
                errors.append(
                    f"{path.relative_to(REPO)}: translated page with no original at"
                    f" {(DOCS / relative).relative_to(REPO)}"
                )

    for page in originals.values():
        for locale in LOCALES:
            translated = translation_of(page, locale)
            if not translated.exists():
                missing += 1
                continue
            pairs += 1
            errors.extend(check_pair(page, translated, locale, terms))

    for locale in LOCALES:
        errors.extend(check_readme(locale, terms))

    for error in errors:
        print(error, file=sys.stderr)
    print(
        f"checked {pairs} translated page(s), {missing} page(s) not translated yet,"
        f" and {len(LOCALES)} README translation(s)"
    )
    if errors:
        print(f"{len(errors)} i18n parity problem(s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
