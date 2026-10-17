// Builds the site one language at a time, each in a process of its own, passing every argument
// on to `docusaurus build`.
//
// The local search plugin sets its index builder up once per process, for the languages of the
// first build it indexes, while each language build indexes in languages of its own (English
// beside the build's language). In one process the Russian and Chinese builds would reuse the
// English set-up. The default language goes first: its build empties the output directory the
// others are written into.
//
// An archive line is built in English only, as `docusaurus.config.ts` decides from the same
// variable.
import {spawnSync} from 'node:child_process';

const locales = process.env.DOCS_CONTENT_ROOT ? ['en'] : ['en', 'ru', 'zh'];
for (const locale of locales) {
  const {status} = spawnSync(
    'docusaurus',
    ['build', '--locale', locale, ...process.argv.slice(2)],
    {stdio: 'inherit', shell: process.platform === 'win32'},
  );
  if (status !== 0) process.exit(status ?? 1);
}
