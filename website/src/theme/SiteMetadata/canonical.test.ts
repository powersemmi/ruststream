import {describe, expect, it} from 'vitest';
import {canonicalFor} from './canonical';

const site = 'https://powersemmi.github.io';

describe('canonicalFor', () => {
  it('moves a page of the numbered copy of the newest line to latest', () => {
    expect(
      canonicalFor(`${site}/ruststream/0.7/ru/guides/`, site, '/ruststream/0.7/', '/ruststream/latest/'),
    ).toBe(`${site}/ruststream/latest/ru/guides/`);
  });

  it('keeps the address of a build that is its own canonical copy', () => {
    expect(
      canonicalFor(`${site}/ruststream/next/`, site, '/ruststream/next/', '/ruststream/next/'),
    ).toBe(`${site}/ruststream/next/`);
  });

  it('keeps an address outside the build', () => {
    expect(
      canonicalFor('https://docs.rs/ruststream/', site, '/ruststream/0.7/', '/ruststream/latest/'),
    ).toBe('https://docs.rs/ruststream/');
  });
});
