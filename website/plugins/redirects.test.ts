import {describe, expect, it} from 'vitest';
import {redirectPages} from './redirects';

const target = (html: string) => /http-equiv="refresh" content="0; url=([^"]+)"/.exec(html)?.[1];

describe('redirectPages', () => {
  it('writes a page at the old address that sends the reader to an external target', () => {
    const [page] = redirectPages(
      {'guides/routing': 'https://docs.rs/ruststream/latest/ruststream/runtime/index.html#routing'},
      '/ruststream/latest/',
    );
    expect(page!.file).toBe('guides/routing/index.html');
    expect(target(page!.html)).toBe(
      'https://docs.rs/ruststream/latest/ruststream/runtime/index.html#routing',
    );
    expect(page!.html).toContain(
      'location.replace("https://docs.rs/ruststream/latest/ruststream/runtime/index.html#routing")',
    );
  });

  it('resolves a target on the site against the base URL of the build', () => {
    const [page] = redirectPages(
      {'guides/templates': '/broker-authors/templates/'},
      '/ruststream/0.7/ru/',
    );
    expect(target(page!.html)).toBe('/ruststream/0.7/ru/broker-authors/templates/');
  });

  it('escapes a target so it cannot leave the attribute or the script', () => {
    const [page] = redirectPages({'guides/x': 'https://example.com/?a="b"&c=<d>'}, '/');
    expect(page!.html).not.toContain('"b"&c=<d>');
  });

  it('carries the anchor of the old address to a target that names none', () => {
    const [page] = redirectPages({benchmarks: '/architecture/benchmarks/'}, '/ruststream/latest/');
    const script = /<script>([^<]*)<\/script>/.exec(page!.html)![1]!;
    const opened: string[] = [];
    const replace = (url: string) => opened.push(url);
    new Function('location', script)({hash: '#methodology', replace});
    expect(opened).toEqual(['/ruststream/latest/architecture/benchmarks/#methodology']);
  });

  it('keeps the anchor a target names itself', () => {
    const [page] = redirectPages({'guides/routing': 'https://docs.rs/r/index.html#routing'}, '/');
    const script = /<script>([^<]*)<\/script>/.exec(page!.html)![1]!;
    const opened: string[] = [];
    const replace = (url: string) => opened.push(url);
    new Function('location', script)({hash: '#old', replace});
    expect(opened).toEqual(['https://docs.rs/r/index.html#routing']);
  });

  it('leaves the choice of the indexed address to the refresh and the canonical link', () => {
    const [page] = redirectPages({'guides/x': 'https://docs.rs/r/'}, '/ruststream/latest/');
    expect(page!.html).not.toContain('name="robots"');
    expect(page!.html).toContain('<link rel="canonical" href="https://docs.rs/r/">');
  });

  it('names the indexed copy of a target on the site as the canonical address', () => {
    const [page] = redirectPages(
      {'getting-started/tutorial': '/guides/getting-started/tutorial/'},
      '/ruststream/0.7/ru/',
      {
        siteUrl: 'https://powersemmi.github.io',
        baseUrl: '/ruststream/0.7/',
        canonicalBaseUrl: '/ruststream/latest/',
      },
    );
    expect(target(page!.html)).toBe('/ruststream/0.7/ru/guides/getting-started/tutorial/');
    expect(page!.html).toContain(
      '<link rel="canonical" href="https://powersemmi.github.io/ruststream/latest/ru/guides/getting-started/tutorial/">',
    );
  });
});
