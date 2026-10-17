import path from 'node:path';

/**
 * What a build documents, read from the environment the deploy script sets.
 *
 * - `latest`: the newest release, the default; it carries no banner.
 * - `next`: the documentation of `main`, ahead of every release; its pages say so and are
 *   kept out of search engines.
 * - `archive`: an older release; its pages point the reader at `latest` and are kept out of
 *   search engines, so a search lands on the release a reader should use.
 */
export type VersionKind = 'latest' | 'next' | 'archive';

const kinds: readonly VersionKind[] = ['latest', 'next', 'archive'];

export interface BuildVersion {
  /** `/ruststream/<directory>/`. */
  baseUrl: string;
  /** The directory of the site root the build is served from: `0.7`, `latest`, `next`. */
  directory: string;
  kind: VersionKind;
  /**
   * The pages come from an archive checkout: one language, no landing page, the archive's own
   * sidebar and redirects.
   */
  archive: boolean;
  /** The checkout holding `docs/`, the sources the pages include, and an archive's sidebar. */
  contentRoot: string;
  /** The git ref the excerpts' source links point at. */
  sourceRef: string;
  /** Every page asks search engines to leave it out of their results. */
  noIndex: boolean;
  /**
   * The base URL of the copy search engines should index: `/ruststream/latest/` for the newest
   * line, which is built twice (under its number and under `latest`), the build's own base URL
   * otherwise. A page's canonical URL and its language alternates are written against it.
   */
  canonicalBaseUrl: string;
  /** The build publishes a sitemap: only the copy that is indexed under its own address does. */
  sitemap: boolean;
  /** The docs plugin's options for the one version a build holds. */
  docsVersion: {label: string; banner: 'none' | 'unreleased' | 'unmaintained'};
  /**
   * The crate's rustdoc on docs.rs for the code this build documents, with a trailing slash:
   * the minor line of a release (docs.rs serves its newest patch), and the newest release for
   * `next`, whose code is not published yet.
   */
  docsRs: string;
}

export const siteRoot = '/ruststream/';

const banners = {latest: 'none', next: 'unreleased', archive: 'unmaintained'} as const;

export function readBuildVersion(
  env: Record<string, string | undefined>,
  repoRoot: string,
): BuildVersion {
  const baseUrl = env.DOCS_BASE_URL ?? `${siteRoot}latest/`;
  const directory = /^\/ruststream\/([^/]+)\/$/.exec(baseUrl)?.[1];
  if (!directory) {
    throw new Error(`DOCS_BASE_URL must be ${siteRoot}<version>/, got '${baseUrl}'`);
  }
  const archive = Boolean(env.DOCS_CONTENT_ROOT);
  const requested = env.DOCS_VERSION_KIND ?? (archive ? 'archive' : 'latest');
  if (!kinds.includes(requested as VersionKind)) {
    throw new Error(`DOCS_VERSION_KIND must be one of ${kinds.join(', ')}, got '${requested}'`);
  }
  const kind = requested as VersionKind;
  // The deploy script names the minor line of a release; a build without it (a local one, or
  // CI's check) documents the newest release.
  const line = env.DOCS_LINE || (/^\d+\.\d+$/.test(directory) ? directory : undefined);
  if (line && !/^\d+\.\d+$/.test(line)) {
    throw new Error(`DOCS_LINE must be a minor line such as 0.7, got '${line}'`);
  }
  // An archive checkout holds an old line's pages; it never becomes `latest` or `next`.
  if (archive && kind !== 'archive') {
    throw new Error(`DOCS_CONTENT_ROOT names an archive, which builds as archive, not '${kind}'`);
  }
  const noIndex = kind !== 'latest';
  const canonicalBaseUrl = kind === 'latest' ? `${siteRoot}latest/` : baseUrl;
  return {
    baseUrl,
    directory,
    kind,
    archive,
    contentRoot: archive ? path.resolve(repoRoot, env.DOCS_CONTENT_ROOT!) : repoRoot,
    sourceRef: env.DOCS_SOURCE_REF ?? 'main',
    noIndex,
    canonicalBaseUrl,
    sitemap: !noIndex && canonicalBaseUrl === baseUrl,
    docsVersion: {label: directory, banner: banners[kind]},
    docsRs: `https://docs.rs/ruststream/${kind === 'next' ? 'latest' : (line ?? 'latest')}/`,
  };
}
