import {describe, expect, it} from 'vitest';
import type {VcsConfig} from '@docusaurus/types';
import {siteRelativeVcs} from './siteRelativeVcs';

function recording(): VcsConfig & {asked: string[]} {
  const asked: string[] = [];
  const info = async (filePath: string) => {
    asked.push(filePath);
    return {timestamp: 1, author: 'a'};
  };
  return {asked, initialize: () => {}, getFileCreationInfo: info, getFileLastUpdateInfo: info};
}

describe('siteRelativeVcs', () => {
  it('reads a path relative to the site directory as the file it names', async () => {
    const inner = recording();
    const vcs = siteRelativeVcs(inner);
    vcs.initialize({siteDir: '/repo/website'});
    await vcs.getFileLastUpdateInfo('../docs/brokers/index.md');
    await vcs.getFileCreationInfo('src/pages/index.tsx');
    await vcs.getFileLastUpdateInfo('/repo/docs/a.md');
    expect(inner.asked).toEqual([
      '/repo/docs/brokers/index.md',
      '/repo/website/src/pages/index.tsx',
      '/repo/docs/a.md',
    ]);
  });
});
