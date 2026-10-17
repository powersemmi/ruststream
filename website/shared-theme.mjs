// Writes the theme the broker sites share into a directory: `theme.css`, the font files it names
// in `fonts/`, and the logos and favicon in `img/`.
//
//   node shared-theme.mjs <output directory>
//
// The stylesheet is this site's own `src/css/custom.css`, so the core and the broker sites have
// one source. The `@fontsource` imports become `@font-face` rules over the woff2 files copied
// beside the stylesheet: a broker page loads nothing from node_modules or a third-party host.
//
// A broker site loads the file through the Docusaurus `stylesheets` option, which places it in
// `<head>` before the site's bundled CSS. The rules stay as written: the bundle puts Infima and the
// Docusaurus themes into cascade layers (`future.v4`), and a rule outside any layer wins over them
// wherever it loads.
import {copyFileSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import postcss from 'postcss';

const website = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

const [output] = process.argv.slice(2);
if (!output) {
  console.error('usage: node shared-theme.mjs <output directory>');
  process.exit(2);
}
const fontsDir = path.join(output, 'fonts');
const imgDir = path.join(output, 'img');
rmSync(output, {recursive: true, force: true});
mkdirSync(fontsDir, {recursive: true});
mkdirSync(imgDir, {recursive: true});

// Each `@font-face` keeps its woff2 source only, rewritten to the copy in `fonts/`; every browser
// a Docusaurus site supports reads woff2.
function fontFaces(specifier) {
  const file = require.resolve(specifier);
  const root = postcss.parse(readFileSync(file, 'utf8'), {from: file});
  root.walkDecls('src', (decl) => {
    const woff2 = decl.value.match(/url\(([^)]+\.woff2)\)/);
    if (!woff2) {
      throw new Error(`${file}: an @font-face without a woff2 source`);
    }
    const name = path.basename(woff2[1]);
    copyFileSync(path.resolve(path.dirname(file), woff2[1]), path.join(fontsDir, name));
    decl.value = `url(fonts/${name}) format('woff2')`;
  });
  return root.nodes;
}

const source = path.join(website, 'src/css/custom.css');
const css = postcss.parse(readFileSync(source, 'utf8'), {from: source});
css.walkAtRules('import', (rule) => {
  rule.replaceWith(fontFaces(rule.params.replace(/^['"]|['"]$/g, '')));
});
css.prepend(
  postcss.comment({
    text: 'The RustStream sites theme, generated from the core site src/css/custom.css by shared-theme.mjs.',
  }),
);
writeFileSync(path.join(output, 'theme.css'), css.toString());

for (const name of ['logo.svg', 'logo-dark.svg', 'favicon.svg']) {
  copyFileSync(path.join(website, 'static/img', name), path.join(imgDir, name));
}
console.log(`Wrote the shared theme into ${output}`);
