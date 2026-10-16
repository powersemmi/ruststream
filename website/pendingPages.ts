import path from 'node:path';
import type {MarkdownConfig} from '@docusaurus/types';

// Pages still written in the previous site generator's syntax that MDX cannot parse. They stay
// out of the build until their conversion lands; the conversion deletes this file.
export const pendingPages = [
  'broker-authors/conformance',
  'broker-authors/example-nats',
  'broker-authors/index',
  'brokers/index',
  'brokers/memory',
];

const docsRoot = path.resolve(__dirname, '../docs');
const translationRoot = path.resolve(__dirname, 'i18n');
const translationTree = /^(ru|zh)\/docusaurus-plugin-content-docs\/current\/(.*)$/;

// A link into a pending page leaves the build unchecked and points at the page's address;
// every other unresolved link still fails the build.
export function linkToPendingPage(
  baseUrl: string,
): MarkdownConfig['hooks']['onBrokenMarkdownLinks'] {
  return ({url, sourceFilePath}) => {
    const source = path.resolve(__dirname, sourceFilePath);
    const translated = translationTree.exec(path.relative(translationRoot, source));
    const locale = translated?.[1];
    const original = translated ? path.join(docsRoot, translated[2]) : source;
    const target = path
      .relative(docsRoot, path.resolve(path.dirname(original), url.split('#')[0]))
      .replace(/\.md$/, '');
    if (!pendingPages.includes(target)) {
      throw new Error(`Markdown link '${url}' in '${sourceFilePath}' does not resolve`);
    }
    const page = target.replace(/(^|\/)index$/, '');
    const hash = url.includes('#') ? `#${url.split('#')[1]}` : '';
    return `pathname://${baseUrl}${locale ? `${locale}/` : ''}${page}${page ? '/' : ''}${hash}`;
  };
}
