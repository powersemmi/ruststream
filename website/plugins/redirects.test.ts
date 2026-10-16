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
});
