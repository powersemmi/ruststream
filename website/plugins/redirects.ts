import fs from 'node:fs/promises';
import path from 'node:path';
import type {LoadContext, Plugin} from '@docusaurus/types';

/**
 * Keeps old addresses alive: each one becomes a static page that sends the reader on.
 *
 * `@docusaurus/plugin-client-redirects` applies the site's trailing slash to every target, an
 * external one included, which turns `index.html#routing` into `index.html/#routing`; the
 * targets here are mostly docs.rs pages, so the pages are written directly.
 */
export interface Options {
  /** Old address (relative to the build's base URL, no slashes around it) to its target. */
  redirects: Record<string, string>;
}

const escapeHtml = (text: string): string =>
  text.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/** The pages for `moved`; a target starting with `/` is a page of this build. */
export function redirectPages(
  moved: Record<string, string>,
  baseUrl: string,
): Array<{file: string; html: string}> {
  return Object.entries(moved).map(([from, to]) => {
    const url = to.startsWith('/') ? `${baseUrl}${to.slice(1)}` : to;
    const attribute = escapeHtml(url);
    // JSON quoting plus `<` escaped keeps the target a string inside the script element.
    const script = JSON.stringify(url).replace(/</g, '\\u003c');
    const html = [
      '<!DOCTYPE html>',
      '<html>',
      '<head>',
      '<meta charset="utf-8">',
      `<meta http-equiv="refresh" content="0; url=${attribute}">`,
      `<link rel="canonical" href="${attribute}">`,
      '<meta name="robots" content="noindex">',
      `<script>location.replace(${script})</script>`,
      '</head>',
      `<body><a href="${attribute}">${attribute}</a></body>`,
      '</html>',
      '',
    ].join('\n');
    return {file: `${from}/index.html`, html};
  });
}

export default function redirectsPlugin(context: LoadContext, options: Options): Plugin {
  return {
    name: 'ruststream-redirects',
    async postBuild({outDir}) {
      for (const {file, html} of redirectPages(options.redirects, context.baseUrl)) {
        const target = path.join(outDir, file);
        await fs.mkdir(path.dirname(target), {recursive: true});
        await fs.writeFile(target, html);
      }
    },
  };
}
