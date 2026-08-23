"""Normative-source banner on every translated page.

English is the source of truth, and that has to be written on the page rather than assumed:
a reader who lands on a translated page has no way to tell whether it is current. Every page
whose source file lives under a locale directory therefore opens with a banner naming English
as normative and linking to the original; the Chinese pages add that the translation was
produced by a model, which is a statement about where the text came from, not a disclaimer
about its quality.

Injecting the banner from a hook rather than writing it into each file keeps it off the list
of things a translator can forget, and keeps the wording in one place. It also keeps the
banner off pages that only *appear* in a localized site: with `fallback_to_default` an
untranslated page is served in the localized tree with its English text, and telling the
reader that page is a translation would be false.

The link is built as a relative URL and injected after markdown rendering, for two reasons:
the site is deployed under a version prefix by `mike`, so a root-relative path would point
outside the version being read, and a relative link written in markdown would be resolved
against the translated file's own path by the link checker.
"""

# The default locale, whose pages are the originals. Keep in sync with `properdocs.yml`.
DEFAULT_LOCALE = "en"

BANNERS = {
    "ru": (
        "Нормативная версия документации - английская",
        'Эта страница переведена с английского. При любом расхождении верен '
        '<a href="{original}">английский оригинал</a>.',
    ),
    "zh": (
        "英文版本为准",
        '本页译自英文，由模型翻译。若与<a href="{original}">英文原文</a>存在差异，'
        "以英文原文为准。",
    ),
}

TEMPLATE = (
    '<div class="admonition info i18n-banner">\n'
    '<p class="admonition-title">{title}</p>\n'
    "<p>{body}</p>\n"
    "</div>\n"
)


def _original_url(page_url: str, locale: str) -> str:
    """Relative URL from a translated page to its English original.

    `page_url` is the localized page's URL relative to the site root, so it starts with the
    locale segment (`ru/guides/routing/`) and the English original is the same URL without
    that segment. Both are relative to the same root, so climbing out of the localized page's
    directory and walking back down gets there under any deployment prefix.
    """
    segments = [s for s in page_url.split("/") if s]
    # With `use_directory_urls` a page URL is a directory and every segment is one level
    # deep; without it the last segment is the file itself and does not add a level.
    depth = len(segments) if page_url.endswith("/") or not segments else len(segments) - 1
    if segments and segments[0] == locale:
        segments = segments[1:]
    up = "../" * depth if depth else "./"
    tail = "/".join(segments)
    if not tail:
        return up
    return f"{up}{tail}/" if page_url.endswith("/") else f"{up}{tail}"


def on_page_content(html, page, **_kwargs):
    locale = getattr(page.file, "locale", DEFAULT_LOCALE)
    banner = BANNERS.get(locale) if locale != DEFAULT_LOCALE else None
    if banner is None:
        return html
    title, body = banner
    block = TEMPLATE.format(title=title, body=body.format(original=_original_url(page.url, locale)))
    # Under the page title rather than above it, so the reader sees what the page is before
    # being told how to read it.
    head, sep, tail = html.partition("</h1>")
    return head + sep + "\n" + block + tail if sep else block + html
