import {describe, expect, it} from 'vitest';
import {currentEntry, parseVersions, samePageIn, versionRoot} from './versions';

const published = [
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
    expect(currentEntry(versions, '/ruststream/', '/ruststream/0.1/')).toBeUndefined();
  });
});

describe('samePageIn', () => {
  it('keeps the page and the language when switching versions', () => {
    expect(
      samePageIn('/ruststream/', '/ruststream/latest/', '/ruststream/latest/ru/getting-started/installation/', '0.6'),
    ).toBe('/ruststream/0.6/ru/getting-started/installation/');
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
