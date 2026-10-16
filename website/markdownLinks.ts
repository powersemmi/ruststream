import fs from 'node:fs';
import path from 'node:path';
import type {MarkdownConfig} from '@docusaurus/types';

const docsRoot = path.resolve(__dirname, '../docs');
const translationRoot = path.resolve(__dirname, 'i18n');
const translationTree = /^(ru|zh)\/docusaurus-plugin-content-docs\/current\/(.*)$/;

/**
 * Resolves the Markdown links Docusaurus cannot.
 *
 * A translated page links to its neighbours by relative path, and Docusaurus looks for them
 * only in the translation's own folder. A neighbour with no translation yet is served from the
 * English file, so the link goes to that page's address in the same language, where the
 * build's link check still verifies it. Any other unresolved link fails the build.
 */
export function resolveMarkdownLinks(): MarkdownConfig['hooks']['onBrokenMarkdownLinks'] {
  return ({url, sourceFilePath}) => {
    const source = path.resolve(__dirname, sourceFilePath);
    const translated = translationTree.exec(path.relative(translationRoot, source));
    const original = translated ? path.join(docsRoot, translated[2]!) : source;
    const [file, hash] = url.split('#');
    const target = path.resolve(path.dirname(original), file!);
    const page = path.relative(docsRoot, target).replace(/\.md$/, '');
    const address = page.replace(/(^|\/)index$/, '');
    const suffix = `${address}${address ? '/' : ''}${hash ? `#${hash}` : ''}`;
    if (translated && !page.startsWith('..') && fs.existsSync(target)) {
      return `/${suffix}`;
    }
    throw new Error(`Markdown link '${url}' in '${sourceFilePath}' does not resolve`);
  };
}
