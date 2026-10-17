/*
 * The sections that left the Benchmarks page for pages of their own, by the anchor links to
 * them still carry. A static site cannot redirect an anchor, so the page forwards one itself.
 */

const MOVED: Record<string, string[]> = {
  'methodology/': [
    'methodology',
    'the-three-loops',
    'the-run',
    'the-report',
    'the-code-measurement',
  ],
  'interpretation/': [
    'what-the-numbers-are',
    'the-comparison-against-a-raw-client',
    'the-cost-of-the-code',
  ],
  'publishing/': ['publishing-results', 'the-stable-path', 'the-document'],
};

/**
 * The page, relative to the Benchmarks page, that holds the section `hash` named there, or
 * `undefined` for an anchor the page still has. The first anchor of each list is the section
 * the whole page replaced; the others keep their name on the new page.
 */
export function movedSection(hash: string): string | undefined {
  const anchor = hash.replace(/^#/, '');
  for (const [page, anchors] of Object.entries(MOVED)) {
    const index = anchors.indexOf(anchor);
    if (index === 0) {
      return page;
    }
    if (index > 0) {
      return `${page}#${anchor}`;
    }
  }
  return undefined;
}
