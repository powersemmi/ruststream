#!/usr/bin/env python3
"""Structural and terminological parity between a documentation page and its translations.

A translated page decays silently: the reader who would notice that a section is missing is
the one relying on the page. Three properties are cheap to check and cover the failure modes
that matter, without needing a reviewer who reads the target language.

- The same snippet includes, in the same order. Code is never translated - every fence pulls
  the same lines out of the same compiling example - so a difference here means the
  translation dropped or duplicated a block of code the original shows.
- The same heading levels, in the same order. Heading text is translated; the shape of the
  page is not. This is what turns "the translation missed the section added last week" into
  a failing build.
- The locked rendering for every term of art the English page uses, per
  `scripts/doc_glossary.toml`.

Pages with no translation yet are skipped, not failed: translations land page by page, and
demanding all of them at once would block every edit until the site is fully translated.

Run with no arguments; it exits non-zero on the first tree that violates any of the above.
"""

import re
import sys
import tomllib
from pathlib import Path

# Locales that get a translated page next to the original. Keep in sync with the `i18n`
# plugin's language list in `properdocs.yml`.
LOCALES = ("ru", "zh")

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / "docs"
GLOSSARY = REPO / "scripts" / "doc_glossary.toml"

FENCE = re.compile(r"^(?P<indent>\s*)(?P<ticks>```+|~~~+).*?^(?P=indent)(?P=ticks)\s*$", re.M | re.S)
HEADING = re.compile(r"^(#{1,6})\s+\S", re.M)
INCLUDE = re.compile(r'--8<--\s+"(?P<target>[^"]+)"')
INLINE_CODE = re.compile(r"`[^`\n]+`")
HTML_COMMENT = re.compile(r"<!--.*?-->", re.S)
LINK_TARGET = re.compile(r"\]\([^)]*\)")


def english_pages() -> list[Path]:
    """Every original page: a markdown file whose name carries no locale suffix."""
    return sorted(
        path
        for path in DOCS.rglob("*.md")
        if path.name.split(".")[-2] not in LOCALES or path.name.count(".") < 2
    )


def translation_of(page: Path, locale: str) -> Path:
    return page.with_name(f"{page.name[: -len('.md')]}.{locale}.md")


def includes(text: str) -> list[str]:
    return [m.group("target") for m in INCLUDE.finditer(text)]


def headings(text: str) -> list[int]:
    return [len(m.group(1)) for m in HEADING.finditer(FENCE.sub("", text))]


def prose(text: str) -> str:
    """The page with everything that is not translated prose removed.

    Code, inline identifiers and link targets are the same in every language, so matching a
    glossary term against them would report a term as used where nothing was translated, and
    would accept a term as rendered where the translator only left a symbol name in place.
    """
    text = FENCE.sub("", text)
    text = HTML_COMMENT.sub("", text)
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


def main() -> int:
    terms = load_glossary()
    errors: list[str] = []
    pairs = missing = 0

    originals = {page.relative_to(DOCS).as_posix(): page for page in english_pages()}
    for path in sorted(DOCS.rglob("*.md")):
        name = path.name
        if name.count(".") < 2 or name.split(".")[-2] not in LOCALES:
            continue
        locale = name.split(".")[-2]
        original = path.with_name(name.replace(f".{locale}.md", ".md"))
        if original.relative_to(DOCS).as_posix() not in originals:
            errors.append(
                f"{path.relative_to(REPO)}: translated page with no original at"
                f" {original.relative_to(REPO)}"
            )

    for page in originals.values():
        for locale in LOCALES:
            translated = translation_of(page, locale)
            if not translated.exists():
                missing += 1
                continue
            pairs += 1
            errors.extend(check_pair(page, translated, locale, terms))

    for error in errors:
        print(error, file=sys.stderr)
    print(f"checked {pairs} translated page(s), {missing} page(s) not translated yet")
    if errors:
        print(f"{len(errors)} i18n parity problem(s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
