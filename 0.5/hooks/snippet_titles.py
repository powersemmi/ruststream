"""Filename headers on embedded snippets.

Every code fence that embeds a compiled snippet (a ``--8<--`` include) gets a Material
``title="<path>"`` header naming the source file, and the rendered header links to the file on
GitHub - so a reader can jump from any excerpt to the full compiled example. Fences that already
carry an explicit ``title=`` (for example the scaffold walkthrough titling a block
``src/main.rs``) are left alone.

The title is injected in the markdown phase, before pymdownx.snippets resolves the include; the
link is wrapped in the HTML phase, because Material renders the title as a plain
``<span class="filename">``.
"""

import re

REPO_BLOB = "https://github.com/powersemmi/ruststream/blob/main/"

FENCE = re.compile(r"^```(?P<info>[^\n`]*)\n(?P<body>.*?)^```$", re.M | re.S)
INCLUDE = re.compile(r'--8<--\s+"(?P<path>[^":]+)(?::[^"]*)?"')

# Paths whose filename headers get linkified in the HTML phase. Collected across pages; the
# build is single-process, so a plain module-level set is enough.
_linked_paths: set[str] = set()


def on_page_markdown(markdown, **_kwargs):
    def add_title(match):
        info, body = match.group("info"), match.group("body")
        include = INCLUDE.search(body)
        if include is None or "title=" in info:
            return match.group(0)
        path = include.group("path")
        _linked_paths.add(path)
        return f'```{info} title="{path}"\n{body}```'

    return FENCE.sub(add_title, markdown)


def on_page_content(html, **_kwargs):
    for path in _linked_paths:
        html = html.replace(
            f'<span class="filename">{path}</span>',
            f'<span class="filename"><a href="{REPO_BLOB}{path}">{path}</a></span>',
        )
    return html
