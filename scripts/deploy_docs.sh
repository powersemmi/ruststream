#!/usr/bin/env bash
# Builds the site for one minor line and places it in a checkout of the gh-pages branch.
#
#   scripts/deploy_docs.sh <version> [--alias <name>]
#
# GH_PAGES_DIR names the checkout (default: gh-pages, beside the repository root). The version
# is built into <version>/ with its own base URL; an alias (latest) gets a second build with the
# alias's base URL, because every page carries its base URL in its links and assets. The script
# keeps versions.json in the format the version dropdown reads (newest first, an alias on one
# version only), the root redirect to latest/, and the root copy of llms.txt. Committing and
# pushing the checkout is the caller's job.
set -euo pipefail

usage() {
  echo "usage: $0 <version> [--alias <name>]" >&2
  exit 2
}

[ $# -ge 1 ] || usage
version="$1"
shift
alias_name=""
while [ $# -gt 0 ]; do
  case "$1" in
    --alias)
      [ $# -ge 2 ] || usage
      alias_name="$2"
      shift 2
      ;;
    *) usage ;;
  esac
done

if ! [[ "$version" =~ ^[0-9]+\.[0-9]+$ ]]; then
  echo "version must be a minor line such as 0.7, got '$version'" >&2
  exit 2
fi
if [ -n "$alias_name" ] && ! [[ "$alias_name" =~ ^[a-z]+$ ]]; then
  echo "alias must be a lowercase word such as latest, got '$alias_name'" >&2
  exit 2
fi

repo="$(cd "$(dirname "$0")/.." && pwd)"
site="$(cd "${GH_PAGES_DIR:-$repo/gh-pages}" && pwd)"
website="$repo/website"

build_into() {
  local directory="$1"
  echo "Building /ruststream/$directory/"
  (cd "$website" && DOCS_BASE_URL="/ruststream/$directory/" npm run build -- --out-dir "build/$directory")
  # mike published an alias as a symlink; removing the path drops the link, not its target.
  rm -rf "${site:?}/$directory"
  mv "$website/build/$directory" "$site/$directory"
}

build_into "$version"
if [ -n "$alias_name" ]; then
  build_into "$alias_name"
fi

VERSION="$version" ALIAS="$alias_name" VERSIONS="$site/versions.json" node <<'NODE'
const fs = require('node:fs');
const {VERSION: version, ALIAS: alias, VERSIONS: file} = process.env;
const versions = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : [];
const others = versions
  .filter((entry) => entry.version !== version)
  .map((entry) => ({...entry, aliases: entry.aliases.filter((name) => name !== alias)}));
const previous = versions.find((entry) => entry.version === version);
const aliases = new Set(previous ? previous.aliases : []);
if (alias) aliases.add(alias);
const minor = (entry) => entry.version.split('.').map(Number);
const updated = [...others, {version, title: version, aliases: [...aliases]}].sort((a, b) => {
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
