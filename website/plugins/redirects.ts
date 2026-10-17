import fs from 'node:fs/promises';
import path from 'node:path';
import type {LoadContext, Plugin} from '@docusaurus/types';
import {canonicalFor} from '../src/theme/SiteMetadata/canonical';

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

/** Where the pages of this build are indexed; see `canonicalFor`. */
export interface Canonical {
  siteUrl: string;
  /** The build's base URL, without the language. */
  baseUrl: string;
  canonicalBaseUrl: string;
}

/**
 * The pages for `moved`; a target starting with `/` is a page of this build, served from
 * `baseUrl` (the language's base URL), and its canonical address is the one `canonical` gives.
 */
export function redirectPages(
  moved: Record<string, string>,
  baseUrl: string,
  canonical?: Canonical,
): Array<{file: string; html: string}> {
  return Object.entries(moved).map(([from, to]) => {
    const url = to.startsWith('/') ? `${baseUrl}${to.slice(1)}` : to;
    const attribute = escapeHtml(url);
    const canonicalUrl =
      canonical && url.startsWith('/')
        ? canonicalFor(
            `${canonical.siteUrl}${url}`,
            canonical.siteUrl,
            canonical.baseUrl,
            canonical.canonicalBaseUrl,
          )
        : url;
    // JSON quoting plus `<` escaped keeps the target a string inside the script element.
    const quoted = JSON.stringify(url).replace(/</g, '\\u003c');
    // A link to a section of the old page keeps its anchor, so a section that moved on with
    // its page (or to a page of its own) is still found; a target that names an anchor keeps it.
    const script = url.includes('#')
      ? `location.replace(${quoted})`
      : `location.replace(${quoted} + location.hash)`;
    const html = [
      '<!DOCTYPE html>',
      '<html>',
      '<head>',
      '<meta charset="utf-8">',
      `<meta http-equiv="refresh" content="0; url=${attribute}">`,
      // An instant refresh is a permanent redirect to search engines, and the canonical link
      // says the same; a `noindex` beside them would contradict the canonical.
      `<link rel="canonical" href="${escapeHtml(canonicalUrl)}">`,
      `<script>${script}</script>`,
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
      // A language's build sees its own base URL; the version's one travels beside it.
      const {url: siteUrl, baseUrl: localeBaseUrl, customFields} = context.siteConfig;
      const baseUrl = (customFields?.buildBaseUrl as string | undefined) ?? localeBaseUrl;
      const canonical = {
        siteUrl,
        baseUrl,
        canonicalBaseUrl: (customFields?.canonicalBaseUrl as string | undefined) ?? baseUrl,
      };
      for (const {file, html} of redirectPages(options.redirects, context.baseUrl, canonical)) {
        const target = path.join(outDir, file);
        await fs.mkdir(path.dirname(target), {recursive: true});
        await fs.writeFile(target, html);
      }
    },
  };
}
