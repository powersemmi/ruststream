import path from 'node:path';
import type {VcsConfig} from '@docusaurus/types';

/**
 * `inner`, asked for a file by its absolute path.
 *
 * The sitemap asks the Git history for a page's date with the page's path relative to the site
 * directory, while the eager Git reader of Docusaurus Faster knows files by absolute path, so
 * every `lastmod` comes out empty. Docusaurus fixed this after 3.10.2
 * (facebook/docusaurus#12329); this wrapper can go with the release that ships the fix.
 */
export function siteRelativeVcs(inner: VcsConfig): VcsConfig {
  let siteDir = process.cwd();
  const resolve = (filePath: string) => path.resolve(siteDir, filePath);
  return {
    initialize(params) {
      siteDir = params.siteDir;
      inner.initialize(params);
    },
    getFileCreationInfo: (filePath) => inner.getFileCreationInfo(resolve(filePath)),
    getFileLastUpdateInfo: (filePath) => inner.getFileLastUpdateInfo(resolve(filePath)),
  };
}
