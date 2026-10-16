import {describe, expect, it} from 'vitest';
import {currentEntry, latestEntry, parseVersions, samePageIn, versionRoot} from './versions';

const published = [
  {version: 'next', title: 'next (unreleased)', aliases: []},
  {version: '0.7', title: '0.7', aliases: ['latest']},
  {version: '0.6', title: '0.6', aliases: []},
];

describe('parseVersions', () => {
  it('reads the versions file the deploy writes', () => {
    expect(parseVersions(published)).toEqual(published);
  });

  it('drops entries that are not versions and a file that is not a list', () => {
    expect(parseVersions([{version: '0.5'}, {title: 'x'}, 3])).toEqual([
      {version: '0.5', title: '0.5', aliases: []},
    ]);
    expect(parseVersions({version: '0.7'})).toEqual([]);
  });
});

describe('currentEntry', () => {
  it('finds the version a build serves, by its directory or by an alias', () => {
    const versions = parseVersions(published);
    expect(currentEntry(versions, '/ruststream/', '/ruststream/0.6/')?.version).toBe('0.6');
    expect(currentEntry(versions, '/ruststream/', '/ruststream/latest/')?.version).toBe('0.7');
    expect(currentEntry(versions, '/ruststream/', '/ruststream/next/')?.title).toBe(
      'next (unreleased)',
    );
    expect(currentEntry(versions, '/ruststream/', '/ruststream/0.1/')).toBeUndefined();
  });

  it('finds the version of a translated build, whose base URL ends in its language', () => {
    const versions = parseVersions(published);
    expect(currentEntry(versions, '/ruststream/', '/ruststream/next/ru/')?.title).toBe(
      'next (unreleased)',
    );
    expect(currentEntry(versions, '/ruststream/', '/ruststream/latest/zh/')?.version).toBe('0.7');
  });
});

describe('latestEntry', () => {
  it('is the version the latest alias points at, wherever it sits in the list', () => {
    expect(latestEntry(parseVersions(published))?.version).toBe('0.7');
    expect(latestEntry(parseVersions([{version: 'next', aliases: []}]))).toBeUndefined();
  });
});

describe('samePageIn', () => {
  it('keeps the page and the language when switching versions', () => {
    expect(
      samePageIn('/ruststream/', '/ruststream/latest/', '/ruststream/latest/ru/getting-started/installation/', '0.6'),
    ).toBe('/ruststream/0.6/ru/getting-started/installation/');
  });

  it('maps a page of next to the same page of latest', () => {
    expect(
      samePageIn('/ruststream/', '/ruststream/next/', '/ruststream/next/zh/benchmarks/', 'latest'),
    ).toBe('/ruststream/latest/zh/benchmarks/');
  });

  it('falls back to the version root for a path outside the build', () => {
    expect(samePageIn('/ruststream/', '/ruststream/0.7/', '/elsewhere/', '0.6')).toBe(
      '/ruststream/0.6/',
    );
  });
});

describe('versionRoot', () => {
  it('is the version directory under the site root', () => {
    expect(versionRoot('/ruststream/', '0.5')).toBe('/ruststream/0.5/');
  });
});
