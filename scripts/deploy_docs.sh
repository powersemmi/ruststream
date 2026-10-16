#!/usr/bin/env bash
# Builds the site for one minor line and places it in a checkout of the gh-pages branch.
#
#   scripts/deploy_docs.sh <version> [--alias latest]
#   scripts/deploy_docs.sh next
#   scripts/deploy_docs.sh <version> --content <archive checkout> [--source-ref <ref>]
#
# GH_PAGES_DIR names the checkout (default: gh-pages, beside the repository root). The version
# is built into <version>/ with its own base URL; an alias (latest) gets a second build with the
# alias's base URL, because every page carries its base URL in its links and assets.
#
# The kind of version decides the banner on its pages: `next` is the documentation of the
# current branch, ahead of every release; a version deployed with the latest alias is the newest
# release; any other version is an older one. `--content` builds an older line from its archive
# checkout (its pages, the sources they include, its sidebar) with this repository's site code;
# `--source-ref` is the ref the excerpts link to. An archive never takes an alias.
#
# The script keeps versions.json in mike's format (a development version such as next above the
# releases, releases newest first, an alias on one version only), the root redirect to latest/,
# and the root copy of llms.txt. Committing and pushing the checkout is the caller's job.
set -euo pipefail

usage() {
  echo "usage: $0 <version> [--alias latest | --content <dir> [--source-ref <ref>]]" >&2
  exit 2
}

[ $# -ge 1 ] || usage
version="$1"
shift
alias_name=""
content=""
source_ref=""
while [ $# -gt 0 ]; do
  case "$1" in
    --alias | --content | --source-ref)
      [ $# -ge 2 ] || usage
      case "$1" in
        --alias) alias_name="$2" ;;
        --content) content="$(cd "$2" && pwd)" ;;
        --source-ref) source_ref="$2" ;;
      esac
      shift 2
      ;;
    *) usage ;;
  esac
done

if [ "$version" = "next" ]; then
  if [ -n "$alias_name$content" ]; then
    echo "next is built from this repository and takes no alias" >&2
    exit 2
  fi
  kind="next"
  title="next (unreleased)"
  line=""
elif [[ "$version" =~ ^[0-9]+\.[0-9]+$ ]]; then
  title="$version"
  # A release links its pages to the rustdoc of its own line, under the alias too.
  line="$version"
  if [ -n "$alias_name" ]; then
    if [ "$alias_name" != "latest" ] || [ -n "$content" ]; then
      echo "only the newest release takes an alias, and it is latest" >&2
      exit 2
    fi
    kind="latest"
  else
    kind="archive"
  fi
else
  echo "version must be next or a minor line such as 0.7, got '$version'" >&2
  exit 2
fi
if [ -z "$content" ] && [ -n "$source_ref" ]; then
  echo "--source-ref names the ref of an archive checkout given with --content" >&2
  exit 2
fi

repo="$(cd "$(dirname "$0")/.." && pwd)"
site="$(cd "${GH_PAGES_DIR:-$repo/gh-pages}" && pwd)"
website="$repo/website"

build_into() {
  local directory="$1"
  echo "Building /ruststream/$directory/"
  (cd "$website" &&
    DOCS_BASE_URL="/ruststream/$directory/" DOCS_VERSION_KIND="$kind" DOCS_LINE="$line" \
      DOCS_CONTENT_ROOT="$content" DOCS_SOURCE_REF="${source_ref:-main}" \
      npm run build -- --out-dir "build/$directory")
  # mike published an alias as a symlink; removing the path drops the link, not its target.
  rm -rf "${site:?}/$directory"
  mv "$website/build/$directory" "$site/$directory"
  # The site's llms.txt indexes the current documentation; an archive line has none of its own.
  if [ -n "$content" ]; then
    rm -f "$site/$directory/llms.txt"
  fi
}

build_into "$version"
if [ -n "$alias_name" ]; then
  build_into "$alias_name"
fi

VERSION="$version" TITLE="$title" ALIAS="$alias_name" VERSIONS="$site/versions.json" node <<'NODE'
const fs = require('node:fs');
const {VERSION: version, TITLE: title, ALIAS: alias, VERSIONS: file} = process.env;
const versions = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : [];
const others = versions
  .filter((entry) => entry.version !== version)
  .map((entry) => ({...entry, aliases: entry.aliases.filter((name) => name !== alias)}));
const previous = versions.find((entry) => entry.version === version);
const aliases = new Set(previous ? previous.aliases : []);
if (alias) aliases.add(alias);
// mike's order: a version that does not start with a digit is a development version and sits
// above the releases; releases go newest first.
const release = (entry) => /^v?\d/.test(entry.version);
const minor = (entry) => entry.version.split('.').map(Number);
const updated = [...others, {version, title, aliases: [...aliases]}].sort((a, b) => {
  if (release(a) !== release(b)) return release(a) ? 1 : -1;
  if (!release(a)) return a.version < b.version ? 1 : a.version > b.version ? -1 : 0;
  const [aMajor, aMinor] = minor(a);
  const [bMajor, bMinor] = minor(b);
  return bMajor - aMajor || bMinor - aMinor;
});
fs.writeFileSync(file, `${JSON.stringify(updated, null, 2)}\n`);
NODE

if [ "$alias_name" = "latest" ]; then
  cat >"$site/index.html" <<'HTML'
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Redirecting</title>
  <noscript>
    <meta http-equiv="refresh" content="1; url=latest/" />
  </noscript>
  <script>
    window.location.replace(
      "latest/" + window.location.search + window.location.hash
    );
  </script>
</head>
<body>
  Redirecting to <a href="latest/">latest/</a>...
</body>
</html>
HTML
  # The llms.txt convention looks for the file at the site root.
  cp "$site/latest/llms.txt" "$site/llms.txt"
fi
touch "$site/.nojekyll"
echo "Deployed $version${alias_name:+ as $alias_name} into $site"
