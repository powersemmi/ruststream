import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {readBuildVersion} from './versioning';

const repoRoot = '/work/ruststream';

describe('readBuildVersion', () => {
  it('builds the repository itself as the latest release by default', () => {
    expect(readBuildVersion({}, repoRoot)).toEqual({
      baseUrl: '/ruststream/latest/',
      directory: 'latest',
      kind: 'latest',
      archive: false,
      contentRoot: repoRoot,
      sourceRef: 'main',
      noIndex: false,
      docsVersion: {label: 'latest', banner: 'none'},
    });
  });

  it('marks next as unreleased and keeps it out of search engines', () => {
    const version = readBuildVersion(
      {DOCS_BASE_URL: '/ruststream/next/', DOCS_VERSION_KIND: 'next'},
      repoRoot,
    );
    expect(version.directory).toBe('next');
    expect(version.archive).toBe(false);
    expect(version.noIndex).toBe(true);
    expect(version.docsVersion).toEqual({label: 'next', banner: 'unreleased'});
  });

  it('marks an older release as unmaintained', () => {
    const version = readBuildVersion(
      {DOCS_BASE_URL: '/ruststream/0.6/', DOCS_VERSION_KIND: 'archive'},
      repoRoot,
    );
    expect(version.noIndex).toBe(false);
    expect(version.docsVersion).toEqual({label: '0.6', banner: 'unmaintained'});
    expect(version.contentRoot).toBe(repoRoot);
  });

  it('reads an archive from its own checkout, relative to the repository', () => {
    const version = readBuildVersion(
      {
        DOCS_BASE_URL: '/ruststream/0.5/',
        DOCS_CONTENT_ROOT: '../archive',
        DOCS_SOURCE_REF: 'docs/archive/v0.5',
      },
      repoRoot,
    );
    expect(version.archive).toBe(true);
    expect(version.kind).toBe('archive');
    expect(version.contentRoot).toBe(path.resolve(repoRoot, '../archive'));
    expect(version.sourceRef).toBe('docs/archive/v0.5');
    expect(version.docsVersion.banner).toBe('unmaintained');
  });

  it('refuses an archive checkout built as latest or next', () => {
    for (const kind of ['latest', 'next']) {
      expect(() =>
        readBuildVersion(
          {DOCS_BASE_URL: '/ruststream/0.5/', DOCS_CONTENT_ROOT: '/a', DOCS_VERSION_KIND: kind},
          repoRoot,
        ),
      ).toThrow(/archive/);
    }
  });

  it('refuses an unknown kind and a base URL outside the site', () => {
    expect(() => readBuildVersion({DOCS_VERSION_KIND: 'beta'}, repoRoot)).toThrow(/beta/);
    expect(() => readBuildVersion({DOCS_BASE_URL: '/other/0.7/'}, repoRoot)).toThrow(
      /DOCS_BASE_URL/,
    );
  });
});
