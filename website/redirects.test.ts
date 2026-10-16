import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {redirects} from './redirects';

const docsRs = 'https://docs.rs/ruststream/0.8/';
const docsRoot = path.resolve(__dirname, '../docs');
const moved = redirects(docsRs);

// The page a site address is built from, if there is one: `a/b/` comes from `a/b.md` or
// `a/b/index.md`. A page no locale translated is served from the English file, so an address
// with an English page exists in every language.
function page(address: string): string | undefined {
  const relative = address.replace(/^\/|\/$/g, '');
  return [`${relative}.md`, `${relative}/index.md`, `${relative}.mdx`, `${relative}/index.mdx`]
    .map((file) => path.join(docsRoot, file))
    .find((file) => fs.existsSync(file));
}

describe('redirects', () => {
  it('sends the entry pages to their homes in Guides and Architecture', () => {
    expect(moved['getting-started/installation']).toBe('/guides/getting-started/installation/');
    expect(moved['getting-started/quickstart']).toBe('/guides/getting-started/quickstart/');
    expect(moved['getting-started/tutorial']).toBe('/guides/getting-started/tutorial/');
    expect(moved.benchmarks).toBe('/architecture/benchmarks/');
  });

  it('sends the API reference page to the rustdoc of the version built', () => {
    expect(moved.reference).toBe('https://docs.rs/ruststream/0.8/ruststream/');
  });

  it('keeps the guides that moved to docs.rs where they went', () => {
    expect(moved['guides/routing']).toBe(
      'https://docs.rs/ruststream/latest/ruststream/runtime/index.html#routing',
    );
    expect(moved['guides/templates']).toBe('/broker-authors/templates/');
  });

  it('points every address on the site at a page the build has', () => {
    const internal = Object.entries(moved).filter(([, to]) => to.startsWith('/'));
    expect(internal.length).toBeGreaterThan(0);
    for (const [from, to] of internal) {
      expect(page(to), `${from} -> ${to}`).toBeDefined();
    }
  });

  it('never writes over a page: an old address has no page of its own', () => {
    for (const from of Object.keys(moved)) {
      expect(page(from), from).toBeUndefined();
    }
  });
});
